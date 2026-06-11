//! Stage 3 native physical plan and lowering.
//!
//! `lower_query` translates the BGP subset of a (statically substituted)
//! Spargebra query into a small physical operator tree (`NativeQueryPlan`) that
//! the engine's native executor runs directly over the `FrozenIndexedDataset`.
//! Anything outside the subset returns `Err(reason)` so the caller falls back
//! to Spareval — the routing decision is made here, once, at planning time
//! (`docs/05-sparql-execution.md §46`, §263): a query is never half-evaluated by
//! both engines.
//!
//! ## Subset (stage 3)
//!
//! - `SELECT` / `ASK`;
//! - basic graph patterns and fixed `GRAPH <iri> {…}` blocks;
//! - `JOIN`, `UNION`, `PROJECT`, `DISTINCT`;
//! - `FILTER` over the provably-safe boolean subset (`BOUND`, `&&`, `||`, `!`,
//!   `sameTerm`) and `BIND` of a variable/constant.
//!
//! Property paths, `EXISTS`, ordered/value comparisons (`=`, `<`, `IN`),
//! arithmetic, function calls, aggregates, `OPTIONAL`, `MINUS`, `VALUES`,
//! `ORDER BY`, `LIMIT`, and variable graph names all fall back. These are added
//! in later stages with their exact Spareval semantics — stage 3 only lowers
//! constructs it can evaluate identically to the oracle.
//!
//! ## Execution model
//!
//! Operators form a left-deep pipeline rooted at `InputFocus`, which emits one
//! seed solution per focus node (binding `$this`). Each `Scan` extends its input
//! solutions by indexed-nested-loop matching against the dataset; `$this` is a
//! batched input column rather than a substituted constant (doc §88), so one
//! plan evaluates many focus nodes together. `Join(A, B)` is lowered by threading
//! `A`'s pipeline as the input of `B` (sound because BGP join only *adds* bound
//! variables to the evaluation context).

use super::property_path_to_algebra;
use shacl_algebra::Path;
use spargebra::Query;
use spargebra::algebra::{Expression, GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNode, NamedNodePattern, Term, TermPattern, TriplePattern};
use std::collections::HashMap;

/// Index into [`NativeQueryPlan::nodes`].
pub type OpId = u32;
/// Index into [`NativeQueryPlan::var_names`].
pub type VarId = u32;

/// An operand in a triple-scan position: a bound constant or a (possibly free)
/// variable. Query blank nodes are treated as fresh variables.
#[derive(Debug, Clone)]
pub enum ScanTerm {
    Var(VarId),
    Const(Term),
}

/// Which graph a `Scan` reads. Variable graph names are unsupported (fallback),
/// so only the default graph and fixed named graphs appear here.
#[derive(Debug, Clone)]
pub enum GraphScan {
    Default,
    Named(NamedNode),
}

/// A single triple pattern to match against the dataset.
#[derive(Debug, Clone)]
pub struct TripleScan {
    pub subject: ScanTerm,
    pub predicate: ScanTerm,
    pub object: ScanTerm,
    pub graph: GraphScan,
}

/// The arbitrary-length repetition wrapping a [`PathScan`]'s step. These three
/// SPARQL operators use *distinct* (set) semantics, unlike the translatable
/// connectives (sequence, alternative, inverse, predicate) which are lowered to
/// `Scan`/`Join`/`Union` and keep multiset semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClosureKind {
    /// `p*` — reflexive-transitive closure (includes the start node).
    Star,
    /// `p+` — transitive closure (one or more steps).
    Plus,
    /// `p?` — zero or one step.
    Opt,
}

/// An arbitrary-length property-path match. `step` is the (possibly compound)
/// inner path evaluated set-at-a-time per node; `kind` is the repetition. Both
/// endpoint binding modes are supported at execution: a bound endpoint drives a
/// forward (subject bound) or reverse (object bound) closure, both bound is a
/// membership test, and both free is a relation scan over the node domain.
#[derive(Debug, Clone)]
pub struct PathScan {
    pub subject: ScanTerm,
    pub object: ScanTerm,
    pub step: Path,
    pub kind: ClosureKind,
    pub graph: GraphScan,
}

/// A compiled expression in the safe stage-3 subset. Evaluated in boolean
/// (effective-boolean-value) context by `Filter`, and in value context by
/// `Extend`. Every node here has semantics that match Spareval exactly for all
/// inputs — that is the admission criterion for lowering an expression natively.
#[derive(Debug, Clone)]
pub enum ExprPlan {
    Var(VarId),
    Const(Term),
    Bound(VarId),
    Not(Box<ExprPlan>),
    And(Box<ExprPlan>, Box<ExprPlan>),
    Or(Box<ExprPlan>, Box<ExprPlan>),
    SameTerm(Box<ExprPlan>, Box<ExprPlan>),
    /// Correlated `EXISTS { … }`: true iff the sub-plan rooted at this `OpId`,
    /// seeded with the current solution, yields at least one row. `NOT EXISTS`
    /// is `Not(Exists(..))`. The sub-plan lives in the same `nodes` arena.
    Exists(OpId),
}

/// A physical operator. Children are referenced by [`OpId`] into the owning
/// plan's `nodes` arena.
#[derive(Debug, Clone)]
pub enum NativeOp {
    /// Leaf: emit one solution per focus node, binding `$this`.
    InputFocus,
    /// Extend input solutions by indexed-nested-loop matching `pattern`.
    Scan { input: OpId, pattern: TripleScan },
    /// Extend input solutions by an arbitrary-length property path (`*`/`+`/`?`).
    PathScan { input: OpId, scan: PathScan },
    /// Union of two sub-plans evaluated over the same input.
    Union { left: OpId, right: OpId },
    /// Keep input solutions whose `expr` has effective boolean value `true`.
    Filter { input: OpId, expr: ExprPlan },
    /// Bind `var` to `expr` (left unbound if `expr` errors), keeping all rows.
    Extend {
        input: OpId,
        var: VarId,
        expr: ExprPlan,
    },
    /// Restrict each solution's bindings to `vars` (the focus tag is implicit).
    Project { input: OpId, vars: Vec<VarId> },
    /// Deduplicate solutions (per focus node).
    Distinct { input: OpId },
}

/// Whether the query yields solution rows (`Select`) or a single boolean
/// (`Ask` — any solution means the constraint matched).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryForm {
    Select,
    Ask,
}

/// A lowered native query plan over a `FrozenIndexedDataset`.
#[derive(Debug, Clone)]
pub struct NativeQueryPlan {
    pub nodes: Vec<NativeOp>,
    pub root: OpId,
    pub form: QueryForm,
    /// `VarId` → variable name (used to read out `?value` / `?path` results).
    pub var_names: Vec<String>,
    /// The `VarId` of `$this`, bound per focus node by `InputFocus`.
    pub focus_var: VarId,
}

impl NativeQueryPlan {
    /// Look up a variable's id by name, if it occurs in the plan.
    pub fn var_id(&self, name: &str) -> Option<VarId> {
        self.var_names
            .iter()
            .position(|n| n == name)
            .map(|i| i as VarId)
    }
}

// ── Lowering ─────────────────────────────────────────────────────────────────

/// The name of the SHACL focus-node variable, bound by `InputFocus`.
const FOCUS_VAR: &str = "this";

struct Builder {
    nodes: Vec<NativeOp>,
    var_ids: HashMap<String, VarId>,
    var_names: Vec<String>,
    fresh: u32,
}

impl Builder {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            var_ids: HashMap::new(),
            var_names: Vec::new(),
            fresh: 0,
        }
    }

    fn var(&mut self, name: &str) -> VarId {
        if let Some(&id) = self.var_ids.get(name) {
            return id;
        }
        let id = self.var_names.len() as VarId;
        self.var_names.push(name.to_string());
        self.var_ids.insert(name.to_string(), id);
        id
    }

    /// A fresh internal variable, used as the join midpoint when decomposing a
    /// path sequence. The name can't collide with a query variable (`?` is not a
    /// legal SPARQL variable character).
    fn fresh_var(&mut self) -> VarId {
        let name = format!("?path{}", self.fresh);
        self.fresh += 1;
        self.var(&name)
    }

    fn push(&mut self, op: NativeOp) -> OpId {
        let id = self.nodes.len() as OpId;
        self.nodes.push(op);
        id
    }
}

/// Lower a statically substituted query into a native plan, or return a fallback
/// reason. `$this` must remain a free variable in `query` (it is bound per focus
/// by the executor); all other SHACL parameters should already be substituted.
pub fn lower_query(query: &Query) -> Result<NativeQueryPlan, String> {
    let (pattern, form) = match query {
        Query::Select { pattern, .. } => (pattern, QueryForm::Select),
        Query::Ask { pattern, .. } => (pattern, QueryForm::Ask),
        Query::Construct { .. } => return Err("CONSTRUCT query".into()),
        Query::Describe { .. } => return Err("DESCRIBE query".into()),
    };

    let mut b = Builder::new();
    let focus_var = b.var(FOCUS_VAR);
    let input = b.push(NativeOp::InputFocus);
    let root = lower_pattern(&mut b, pattern, input, &GraphScan::Default)?;

    Ok(NativeQueryPlan {
        nodes: b.nodes,
        root,
        form,
        var_names: b.var_names,
        focus_var,
    })
}

/// Lower a graph pattern, threading `input` (the pipeline producing the
/// already-bound context) and the current `graph` scope through it.
fn lower_pattern(
    b: &mut Builder,
    pattern: &GraphPattern,
    input: OpId,
    graph: &GraphScan,
) -> Result<OpId, String> {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            let mut current = input;
            for tp in patterns {
                let scan = lower_triple(b, tp, graph)?;
                current = b.push(NativeOp::Scan {
                    input: current,
                    pattern: scan,
                });
            }
            Ok(current)
        }
        GraphPattern::Join { left, right } => {
            let l = lower_pattern(b, left, input, graph)?;
            lower_pattern(b, right, l, graph)
        }
        GraphPattern::Union { left, right } => {
            let l = lower_pattern(b, left, input, graph)?;
            let r = lower_pattern(b, right, input, graph)?;
            Ok(b.push(NativeOp::Union { left: l, right: r }))
        }
        GraphPattern::Filter { expr, inner } => {
            let i = lower_pattern(b, inner, input, graph)?;
            let e = lower_expr(b, expr, graph)?;
            Ok(b.push(NativeOp::Filter { input: i, expr: e }))
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            let i = lower_pattern(b, inner, input, graph)?;
            let e = lower_expr(b, expression, graph)?;
            let var = b.var(variable.as_str());
            Ok(b.push(NativeOp::Extend {
                input: i,
                var,
                expr: e,
            }))
        }
        GraphPattern::Project { inner, variables } => {
            let i = lower_pattern(b, inner, input, graph)?;
            let vars = variables.iter().map(|v| b.var(v.as_str())).collect();
            Ok(b.push(NativeOp::Project { input: i, vars }))
        }
        GraphPattern::Distinct { inner } => {
            let i = lower_pattern(b, inner, input, graph)?;
            Ok(b.push(NativeOp::Distinct { input: i }))
        }
        GraphPattern::Graph { name, inner } => match name {
            NamedNodePattern::NamedNode(nn) => {
                lower_pattern(b, inner, input, &GraphScan::Named(nn.clone()))
            }
            NamedNodePattern::Variable(_) => Err("variable GRAPH name".into()),
        },
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            let s = lower_term_pattern(b, subject)?;
            let o = lower_term_pattern(b, object)?;
            lower_path(b, s, path, o, input, graph)
        }
        GraphPattern::Reduced { .. } => Err("REDUCED".into()),
        GraphPattern::Values { .. } => Err("inline VALUES".into()),
        GraphPattern::OrderBy { .. } => Err("ORDER BY".into()),
        GraphPattern::Slice { .. } => Err("LIMIT/OFFSET".into()),
        GraphPattern::Group { .. } => Err("aggregates (GROUP BY)".into()),
        GraphPattern::Service { .. } => Err("SERVICE".into()),
        GraphPattern::LeftJoin { .. } => Err("OPTIONAL".into()),
        GraphPattern::Minus { .. } => Err("MINUS".into()),
        GraphPattern::Lateral { .. } => Err("LATERAL".into()),
    }
}

fn lower_triple(
    b: &mut Builder,
    tp: &TriplePattern,
    graph: &GraphScan,
) -> Result<TripleScan, String> {
    Ok(TripleScan {
        subject: lower_term_pattern(b, &tp.subject)?,
        predicate: lower_named_node_pattern(b, &tp.predicate)?,
        object: lower_term_pattern(b, &tp.object)?,
        graph: graph.clone(),
    })
}

fn lower_term_pattern(b: &mut Builder, tp: &TermPattern) -> Result<ScanTerm, String> {
    match tp {
        TermPattern::Variable(v) => Ok(ScanTerm::Var(b.var(v.as_str()))),
        // A query blank node behaves as a non-distinguished join variable.
        TermPattern::BlankNode(bn) => Ok(ScanTerm::Var(b.var(&format!("_:{}", bn.as_str())))),
        TermPattern::NamedNode(n) => Ok(ScanTerm::Const(Term::NamedNode(n.clone()))),
        TermPattern::Literal(l) => Ok(ScanTerm::Const(Term::Literal(l.clone()))),
        #[allow(unreachable_patterns)]
        _ => Err("rdf-star triple term".into()),
    }
}

fn lower_named_node_pattern(b: &mut Builder, np: &NamedNodePattern) -> Result<ScanTerm, String> {
    match np {
        NamedNodePattern::NamedNode(n) => Ok(ScanTerm::Const(Term::NamedNode(n.clone()))),
        NamedNodePattern::Variable(v) => Ok(ScanTerm::Var(b.var(v.as_str()))),
    }
}

/// Lower a top-level property-path pattern between two endpoints, threading
/// `input`. The translatable connectives (predicate, inverse, sequence,
/// alternative) decompose into `Scan`/`Join`/`Union` — matching SPARQL's own
/// translation, which keeps *multiset* semantics. Only the arbitrary-length
/// operators (`*`/`+`/`?`) become a `PathScan` with distinct semantics.
fn lower_path(
    b: &mut Builder,
    subject: ScanTerm,
    path: &PropertyPathExpression,
    object: ScanTerm,
    input: OpId,
    graph: &GraphScan,
) -> Result<OpId, String> {
    match path {
        PropertyPathExpression::NamedNode(p) => {
            let pattern = TripleScan {
                subject,
                predicate: ScanTerm::Const(Term::NamedNode(p.clone())),
                object,
                graph: graph.clone(),
            };
            Ok(b.push(NativeOp::Scan { input, pattern }))
        }
        // ^p : evaluate p with the endpoints swapped.
        PropertyPathExpression::Reverse(p) => lower_path(b, object, p, subject, input, graph),
        // p1/p2 : join through a fresh midpoint (preserves duplicates).
        PropertyPathExpression::Sequence(p1, p2) => {
            let mid = ScanTerm::Var(b.fresh_var());
            let first = lower_path(b, subject, p1, mid.clone(), input, graph)?;
            lower_path(b, mid, p2, object, first, graph)
        }
        // p1|p2 : union of both translations over the same input.
        PropertyPathExpression::Alternative(p1, p2) => {
            let left = lower_path(b, subject.clone(), p1, object.clone(), input, graph)?;
            let right = lower_path(b, subject, p2, object, input, graph)?;
            Ok(b.push(NativeOp::Union { left, right }))
        }
        PropertyPathExpression::ZeroOrMore(p) => {
            lower_closure(b, subject, p, object, ClosureKind::Star, input, graph)
        }
        PropertyPathExpression::OneOrMore(p) => {
            lower_closure(b, subject, p, object, ClosureKind::Plus, input, graph)
        }
        PropertyPathExpression::ZeroOrOne(p) => {
            lower_closure(b, subject, p, object, ClosureKind::Opt, input, graph)
        }
        PropertyPathExpression::NegatedPropertySet(_) => Err("negated property set".into()),
    }
}

/// Lower an arbitrary-length path into a `PathScan`. The repeated step `p` is
/// converted to the `shacl_algebra::Path` algebra (evaluated set-at-a-time by
/// the executor); a step with no algebra equivalent forces fallback.
fn lower_closure(
    b: &mut Builder,
    subject: ScanTerm,
    p: &PropertyPathExpression,
    object: ScanTerm,
    kind: ClosureKind,
    input: OpId,
    graph: &GraphScan,
) -> Result<OpId, String> {
    let step = property_path_to_algebra(p).ok_or("negated property set in closure")?;
    let scan = PathScan {
        subject,
        object,
        step,
        kind,
        graph: graph.clone(),
    };
    Ok(b.push(NativeOp::PathScan { input, scan }))
}

/// Lower an expression. The safe boolean subset (`BOUND`, `&&`, `||`, `!`,
/// `sameTerm`) and correlated `EXISTS` are evaluated natively; everything whose
/// Spareval semantics we don't yet reproduce exactly (value equality, ordered
/// comparison, arithmetic, `IN`, functions) forces whole-query fallback rather
/// than risk a silent disagreement. `graph` is the enclosing graph scope, used
/// for the `EXISTS` sub-pattern.
fn lower_expr(b: &mut Builder, expr: &Expression, graph: &GraphScan) -> Result<ExprPlan, String> {
    match expr {
        Expression::Variable(v) => Ok(ExprPlan::Var(b.var(v.as_str()))),
        Expression::NamedNode(n) => Ok(ExprPlan::Const(Term::NamedNode(n.clone()))),
        Expression::Literal(l) => Ok(ExprPlan::Const(Term::Literal(l.clone()))),
        Expression::Bound(v) => Ok(ExprPlan::Bound(b.var(v.as_str()))),
        Expression::Not(a) => Ok(ExprPlan::Not(Box::new(lower_expr(b, a, graph)?))),
        Expression::And(a, c) => Ok(ExprPlan::And(
            Box::new(lower_expr(b, a, graph)?),
            Box::new(lower_expr(b, c, graph)?),
        )),
        Expression::Or(a, c) => Ok(ExprPlan::Or(
            Box::new(lower_expr(b, a, graph)?),
            Box::new(lower_expr(b, c, graph)?),
        )),
        Expression::SameTerm(a, c) => Ok(ExprPlan::SameTerm(
            Box::new(lower_expr(b, a, graph)?),
            Box::new(lower_expr(b, c, graph)?),
        )),
        // Correlated EXISTS / NOT EXISTS: lower the sub-pattern into the same
        // arena, rooted at its own InputFocus (seeded with the current solution
        // at evaluation time). NOT EXISTS arrives as Not(Exists(..)).
        Expression::Exists(pattern) => {
            let leaf = b.push(NativeOp::InputFocus);
            let root = lower_pattern(b, pattern, leaf, graph)?;
            Ok(ExprPlan::Exists(root))
        }
        Expression::Equal(..) => Err("= (value equality)".into()),
        Expression::Greater(..)
        | Expression::GreaterOrEqual(..)
        | Expression::Less(..)
        | Expression::LessOrEqual(..) => Err("ordered comparison".into()),
        Expression::In(..) => Err("IN".into()),
        Expression::Add(..)
        | Expression::Subtract(..)
        | Expression::Multiply(..)
        | Expression::Divide(..)
        | Expression::UnaryPlus(_)
        | Expression::UnaryMinus(_) => Err("arithmetic".into()),
        Expression::If(..) => Err("IF".into()),
        Expression::Coalesce(_) => Err("COALESCE".into()),
        Expression::FunctionCall(..) => Err("function call".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spargebra::SparqlParser;

    fn parse(q: &str) -> Query {
        SparqlParser::new().parse_query(q).unwrap()
    }

    #[test]
    fn lowers_simple_bgp_select() {
        let q = parse("SELECT ?value WHERE { ?this <http://ex/p> ?value }");
        let plan = lower_query(&q).expect("should lower");
        assert_eq!(plan.form, QueryForm::Select);
        // InputFocus + one Scan + Project
        assert!(matches!(plan.nodes[0], NativeOp::InputFocus));
        assert!(
            plan.nodes
                .iter()
                .any(|op| matches!(op, NativeOp::Scan { .. }))
        );
        assert!(plan.var_names.iter().any(|n| n == "value"));
    }

    #[test]
    fn lowers_ask() {
        let q = parse("ASK { ?this <http://ex/p> ?o }");
        let plan = lower_query(&q).expect("should lower");
        assert_eq!(plan.form, QueryForm::Ask);
    }

    #[test]
    fn lowers_union() {
        let q = parse(
            "SELECT ?o WHERE { { ?this <http://ex/p> ?o } UNION { ?this <http://ex/q> ?o } }",
        );
        let plan = lower_query(&q).expect("should lower");
        assert!(
            plan.nodes
                .iter()
                .any(|op| matches!(op, NativeOp::Union { .. }))
        );
    }

    #[test]
    fn lowers_safe_filter() {
        let q = parse("ASK { ?this <http://ex/p> ?o FILTER (bound(?o) && !sameTerm(?o, ?this)) }");
        let plan = lower_query(&q).expect("should lower");
        assert!(
            plan.nodes
                .iter()
                .any(|op| matches!(op, NativeOp::Filter { .. }))
        );
    }

    #[test]
    fn arbitrary_length_path_lowers_to_pathscan() {
        let q = parse("ASK { ?this <http://ex/p>* ?o }");
        let plan = lower_query(&q).expect("should lower");
        assert!(plan.nodes.iter().any(|op| matches!(
            op,
            NativeOp::PathScan { scan, .. } if scan.kind == ClosureKind::Star
        )));
    }

    #[test]
    fn sequence_path_decomposes_to_scans() {
        // p/q is multiset-translated to two joined scans, not a PathScan.
        let q = parse("SELECT ?o WHERE { ?this <http://ex/p>/<http://ex/q> ?o }");
        let plan = lower_query(&q).expect("should lower");
        assert_eq!(
            plan.nodes
                .iter()
                .filter(|op| matches!(op, NativeOp::Scan { .. }))
                .count(),
            2
        );
        assert!(
            !plan
                .nodes
                .iter()
                .any(|op| matches!(op, NativeOp::PathScan { .. }))
        );
    }

    #[test]
    fn not_exists_filter_lowers() {
        let q = parse("ASK { ?this <http://ex/p> ?o FILTER NOT EXISTS { ?o <http://ex/q> ?w } }");
        let plan = lower_query(&q).expect("should lower");
        assert!(
            plan.nodes
                .iter()
                .any(|op| matches!(op, NativeOp::Filter { .. }))
        );
    }

    #[test]
    fn negated_property_set_falls_back() {
        let q = parse("ASK { ?this !<http://ex/p> ?o }");
        assert!(lower_query(&q).is_err());
    }

    #[test]
    fn value_equality_falls_back() {
        let q = parse("ASK { ?this <http://ex/p> ?o FILTER (?o = 3) }");
        assert_eq!(lower_query(&q).unwrap_err(), "= (value equality)");
    }

    #[test]
    fn optional_falls_back() {
        let q =
            parse("SELECT ?o WHERE { ?this <http://ex/p> ?o OPTIONAL { ?o <http://ex/q> ?w } }");
        assert!(lower_query(&q).is_err());
    }

    #[test]
    fn fixed_graph_block_lowers() {
        let q = parse("ASK { GRAPH <urn:g> { ?this <http://ex/p> ?o } }");
        let plan = lower_query(&q).expect("should lower");
        assert!(plan.nodes.iter().any(
            |op| matches!(op, NativeOp::Scan { pattern, .. } if matches!(pattern.graph, GraphScan::Named(_)))
        ));
    }
}
