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
//! ## Native subset
//!
//! - `SELECT` / `ASK`;
//! - basic graph patterns and fixed `GRAPH <iri> {…}` blocks;
//! - `JOIN`, `UNION`, `PROJECT`, `DISTINCT`;
//! - `FILTER` over the provably-safe boolean subset (`BOUND`, `&&`, `||`, `!`,
//!   `sameTerm`, safe equality, `STR`, `STRSTARTS`);
//! - property paths and correlated `EXISTS` / `NOT EXISTS`;
//! - `BIND` of supported expressions.
//!
//! Ordered comparisons, `IN`, arithmetic, most function calls, aggregates,
//! `OPTIONAL`, `MINUS`, `VALUES`, `ORDER BY`, `LIMIT`, and variable graph names
//! all fall back. New constructs are lowered only when they can be evaluated
//! identically to the Spareval oracle.
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
use shifty_algebra::Path;
use spargebra::Query;
use spargebra::algebra::{Expression, Function, GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNode, NamedNodePattern, Term, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    /// SPARQL `STR`, producing the lexical form of an IRI or literal.
    Str(Box<ExprPlan>),
    /// SPARQL `STRSTARTS` over argument-compatible strings.
    StrStarts(Box<ExprPlan>, Box<ExprPlan>),
    /// SPARQL value equality (`=`). Uses term-id identity as the fast path.
    /// Returns `None` (type error) for cross-type numeric comparisons rather
    /// than implementing full numeric promotion — those queries fall through
    /// the FILTER just as SPARQL type errors do.
    Equal(Box<ExprPlan>, Box<ExprPlan>),
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

/// Dataset statistics passed from `FrozenIndexedDataset` to guide BGP
/// scan reordering at plan-compilation time. Keyed by RDF `Term` so the
/// planner can look up constants without touching the frozen dictionary.
#[derive(Debug, Clone, Default)]
pub struct PlanStats {
    pub total_triples: u64,
    pub distinct_subjects: u64,
    pub distinct_objects: u64,
    pub distinct_predicates: u64,
    /// Triples per predicate IRI.
    pub predicate_cardinality: HashMap<Term, u64>,
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

    fn focus_var(&self) -> VarId {
        *self
            .var_ids
            .get(FOCUS_VAR)
            .expect("focus var registered first")
    }
}

/// Lower a statically substituted query into a native plan, or return a fallback
/// reason. `$this` must remain a free variable in `query` (it is bound per focus
/// by the executor); all other SHACL parameters should already be substituted.
pub fn lower_query(query: &Query) -> Result<NativeQueryPlan, String> {
    lower_query_with_stats(query, None)
}

/// Like [`lower_query`] but with optional dataset statistics. When `stats` is
/// `Some`, BGP triple patterns within each `GraphPattern::Bgp` block are
/// reordered greedily by estimated output cardinality before lowering: the
/// most selective scan (smallest expected row count given currently bound
/// variables) is placed first. The plan's semantics are unchanged — BGP join
/// is commutative — but the left-deep pipeline evaluates fewer intermediate
/// rows.
pub fn lower_query_with_stats(
    query: &Query,
    stats: Option<&PlanStats>,
) -> Result<NativeQueryPlan, String> {
    let (pattern, form) = match query {
        Query::Select { pattern, .. } => (pattern, QueryForm::Select),
        Query::Ask { pattern, .. } => (pattern, QueryForm::Ask),
        Query::Construct { .. } => return Err("CONSTRUCT query".into()),
        Query::Describe { .. } => return Err("DESCRIBE query".into()),
    };

    let mut b = Builder::new();
    let focus_var = b.var(FOCUS_VAR);
    let input = b.push(NativeOp::InputFocus);
    let root = lower_pattern(&mut b, pattern, input, &GraphScan::Default, stats)?;

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
    stats: Option<&PlanStats>,
) -> Result<OpId, String> {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            let mut scans: Vec<TripleScan> = patterns
                .iter()
                .map(|tp| lower_triple(b, tp, graph))
                .collect::<Result<_, _>>()?;
            if let Some(s) = stats {
                reorder_bgp(&mut scans, b.focus_var(), s);
            }
            let mut current = input;
            for scan in scans {
                current = b.push(NativeOp::Scan {
                    input: current,
                    pattern: scan,
                });
            }
            Ok(current)
        }
        GraphPattern::Join { left, right } => {
            // Flatten the entire join tree into BGP/Path leaves and apply
            // greedy cost-based ordering across all of them at once.
            // Join(Join(A, B), C) is treated as three independent leaves, not
            // two nested binary decisions — this generalises the two-arm case.
            if let Some(s) = stats {
                let mut leaves = Vec::new();
                if collect_join_leaves(pattern, &mut leaves) && leaves.len() >= 2 {
                    return lower_join_leaves(b, leaves, input, graph, s);
                }
            }
            let l = lower_pattern(b, left, input, graph, stats)?;
            lower_pattern(b, right, l, graph, stats)
        }
        GraphPattern::Union { left, right } => {
            let l = lower_pattern(b, left, input, graph, stats)?;
            let r = lower_pattern(b, right, input, graph, stats)?;
            Ok(b.push(NativeOp::Union { left: l, right: r }))
        }
        GraphPattern::Filter { expr, inner } => {
            let i = lower_pattern(b, inner, input, graph, stats)?;
            let e = lower_expr(b, expr, graph, stats)?;
            Ok(b.push(NativeOp::Filter { input: i, expr: e }))
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            let i = lower_pattern(b, inner, input, graph, stats)?;
            let e = lower_expr(b, expression, graph, stats)?;
            let var = b.var(variable.as_str());
            Ok(b.push(NativeOp::Extend {
                input: i,
                var,
                expr: e,
            }))
        }
        GraphPattern::Project { inner, variables } => {
            let i = lower_pattern(b, inner, input, graph, stats)?;
            let vars = variables.iter().map(|v| b.var(v.as_str())).collect();
            Ok(b.push(NativeOp::Project { input: i, vars }))
        }
        GraphPattern::Distinct { inner } => {
            let i = lower_pattern(b, inner, input, graph, stats)?;
            Ok(b.push(NativeOp::Distinct { input: i }))
        }
        GraphPattern::Graph { name, inner } => match name {
            NamedNodePattern::NamedNode(nn) => {
                lower_pattern(b, inner, input, &GraphScan::Named(nn.clone()), stats)
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

// ── BGP scan reordering ───────────────────────────────────────────────────────

/// Reorder `scans` in-place using a greedy minimum-cost algorithm.
///
/// Starting from the set of variables bound by `InputFocus` (just `?this`),
/// at each step we pick the scan with the lowest estimated output cardinality
/// given the variables bound so far, then add that scan's output variables to
/// the bound set. BGP join is commutative so the final result set is unchanged.
fn reorder_bgp(scans: &mut Vec<TripleScan>, focus_var: VarId, stats: &PlanStats) {
    if scans.len() < 2 {
        return;
    }
    let mut bound: HashSet<VarId> = std::iter::once(focus_var).collect();
    let mut ordered: Vec<TripleScan> = Vec::with_capacity(scans.len());
    let mut remaining: Vec<TripleScan> = std::mem::take(scans);

    while !remaining.is_empty() {
        let best = remaining
            .iter()
            .enumerate()
            .min_by_key(|(_, s)| estimate_scan_cost(s, &bound, stats))
            .map(|(i, _)| i)
            .unwrap();
        let scan = remaining.remove(best);
        for v in scan_free_vars(&scan, &bound) {
            bound.insert(v);
        }
        ordered.push(scan);
    }
    *scans = ordered;
}

/// Variables in `scan` that are not yet in `bound` (they become bound after
/// this scan executes).
fn scan_free_vars(scan: &TripleScan, bound: &HashSet<VarId>) -> Vec<VarId> {
    [&scan.subject, &scan.predicate, &scan.object]
        .into_iter()
        .filter_map(|t| {
            if let ScanTerm::Var(v) = t {
                (!bound.contains(v)).then_some(*v)
            } else {
                None
            }
        })
        .collect()
}

/// Estimated output row count for `scan` given `bound`. Uses a tiered model:
/// membership check → SP/PO range → predicate-only → subject/object range
/// → full scan. Constant predicates are looked up in `stats.predicate_cardinality`;
/// bound-variable predicates use the average cardinality.
fn estimate_scan_cost(scan: &TripleScan, bound: &HashSet<VarId>, stats: &PlanStats) -> u64 {
    let s = is_bound(&scan.subject, bound);
    let p = is_bound(&scan.predicate, bound);
    let o = is_bound(&scan.object, bound);
    let total = stats.total_triples.max(1);
    let ds = stats.distinct_subjects.max(1);
    let dp = stats.distinct_predicates.max(1);
    let dobj = stats.distinct_objects.max(1);

    match (s, p, o) {
        (true, true, true) => 1,
        (true, true, false) => {
            // SP range: predicate_card / distinct_subjects per predicate group
            pred_card(&scan.predicate, stats).unwrap_or(total / dp) / ds + 1
        }
        (false, true, true) => pred_card(&scan.predicate, stats).unwrap_or(total / dp) / dobj + 1,
        (false, true, false) => pred_card(&scan.predicate, stats).unwrap_or(total / dp),
        (true, false, false) => total / ds,
        (false, false, true) => total / dobj,
        // S+O with free P, or fully free: treat as full scan
        (true, false, true) | (false, false, false) => total,
    }
}

fn is_bound(term: &ScanTerm, bound: &HashSet<VarId>) -> bool {
    match term {
        ScanTerm::Const(_) => true,
        ScanTerm::Var(v) => bound.contains(v),
    }
}

/// Predicate cardinality for a constant-predicate scan term, `None` for
/// variable predicates (caller uses average) or unknown predicates (cost = 0,
/// the scan will immediately produce no rows — pick it first).
fn pred_card(term: &ScanTerm, stats: &PlanStats) -> Option<u64> {
    match term {
        ScanTerm::Const(t) => Some(*stats.predicate_cardinality.get(t).unwrap_or(&0)),
        ScanTerm::Var(_) => None,
    }
}

// ── Join tree flattening and reordering ──────────────────────────────────────

/// Collect all BGP/Path leaves from a tree of Join nodes into `out`.
/// Returns `false` if any leaf is not a BGP or Path — non-monotone nodes
/// (Filter, Union, …) break commutativity so we can't freely reorder them.
fn collect_join_leaves<'a>(pattern: &'a GraphPattern, out: &mut Vec<&'a GraphPattern>) -> bool {
    match pattern {
        GraphPattern::Join { left, right } => {
            collect_join_leaves(left, out) && collect_join_leaves(right, out)
        }
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } => {
            out.push(pattern);
            true
        }
        _ => false,
    }
}

/// Lower a flat list of BGP/Path leaves in greedy least-cost order.
///
/// Starting from `{?this}` as the initially bound set, at each step the
/// cheapest remaining leaf is lowered first and its output variables added to
/// the bound set before costing the next leaf.  This is the join-level
/// generalisation of `reorder_bgp` and subsumes the earlier two-arm case.
fn lower_join_leaves(
    b: &mut Builder,
    mut leaves: Vec<&GraphPattern>,
    input: OpId,
    graph: &GraphScan,
    stats: &PlanStats,
) -> Result<OpId, String> {
    let mut bound: HashSet<String> = std::iter::once(FOCUS_VAR.to_string()).collect();
    let mut current = input;
    while !leaves.is_empty() {
        let best = leaves
            .iter()
            .enumerate()
            .min_by_key(|(_, p)| estimate_pattern_cost(p, &bound, stats))
            .map(|(i, _)| i)
            .unwrap();
        let pat = leaves.remove(best);
        bound.extend(pattern_new_vars(pat, &bound));
        current = lower_pattern(b, pat, current, graph, Some(stats))?;
    }
    Ok(current)
}

/// Variables that `pattern` will newly bind (not already present in `bound`).
fn pattern_new_vars(pattern: &GraphPattern, bound: &HashSet<String>) -> Vec<String> {
    let mut vars: HashSet<String> = HashSet::new();
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for tp in patterns {
                if let TermPattern::Variable(v) = &tp.subject {
                    if !bound.contains(v.as_str()) {
                        vars.insert(v.as_str().to_string());
                    }
                }
                if let NamedNodePattern::Variable(v) = &tp.predicate {
                    if !bound.contains(v.as_str()) {
                        vars.insert(v.as_str().to_string());
                    }
                }
                if let TermPattern::Variable(v) = &tp.object {
                    if !bound.contains(v.as_str()) {
                        vars.insert(v.as_str().to_string());
                    }
                }
            }
        }
        GraphPattern::Path { subject, object, .. } => {
            if let TermPattern::Variable(v) = subject {
                if !bound.contains(v.as_str()) {
                    vars.insert(v.as_str().to_string());
                }
            }
            if let TermPattern::Variable(v) = object {
                if !bound.contains(v.as_str()) {
                    vars.insert(v.as_str().to_string());
                }
            }
        }
        _ => {}
    }
    vars.into_iter().collect()
}

/// Estimated cost of evaluating `pattern` given `bound` variable names.
/// For BGPs this is the minimum single-triple cost across all patterns — the
/// bound set propagates so the cheapest entry point drives the estimate.
fn estimate_pattern_cost(pattern: &GraphPattern, bound: &HashSet<String>, stats: &PlanStats) -> u64 {
    let total = stats.total_triples.max(1);
    let ds = stats.distinct_subjects.max(1);
    let dp = stats.distinct_predicates.max(1);
    let dobj = stats.distinct_objects.max(1);

    match pattern {
        GraphPattern::Bgp { patterns } => patterns
            .iter()
            .map(|tp| {
                let s = tp_name_is_bound(&tp.subject, bound);
                let p = nnp_name_is_bound(&tp.predicate, bound);
                let o = tp_name_is_bound(&tp.object, bound);
                let pred_est = match &tp.predicate {
                    NamedNodePattern::NamedNode(nn) => stats
                        .predicate_cardinality
                        .get(&Term::NamedNode(nn.clone()))
                        .copied()
                        .unwrap_or(total / dp),
                    NamedNodePattern::Variable(_) => total / dp,
                };
                match (s, p, o) {
                    (true, true, true) => 1,
                    (true, true, false) => pred_est / ds + 1,
                    (false, true, true) => pred_est / dobj + 1,
                    (false, true, false) => pred_est,
                    (true, false, false) => total / ds,
                    (false, false, true) => total / dobj,
                    _ => total,
                }
            })
            .min()
            .unwrap_or(total),

        GraphPattern::Path { subject, object, .. } => {
            let s = tp_name_is_bound(subject, bound);
            let o = tp_name_is_bound(object, bound);
            match (s, o) {
                (true, true) => 1,
                (true, false) | (false, true) => total / ds / 2 + 1,
                (false, false) => total,
            }
        }

        _ => total,
    }
}

fn tp_name_is_bound(tp: &TermPattern, bound: &HashSet<String>) -> bool {
    match tp {
        TermPattern::Variable(v) => bound.contains(v.as_str()),
        _ => true,
    }
}

fn nnp_name_is_bound(nnp: &NamedNodePattern, bound: &HashSet<String>) -> bool {
    match nnp {
        NamedNodePattern::NamedNode(_) => true,
        NamedNodePattern::Variable(v) => bound.contains(v.as_str()),
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
/// converted to the `shifty_algebra::Path` algebra (evaluated set-at-a-time by
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
fn lower_expr(
    b: &mut Builder,
    expr: &Expression,
    graph: &GraphScan,
    stats: Option<&PlanStats>,
) -> Result<ExprPlan, String> {
    match expr {
        Expression::Variable(v) => Ok(ExprPlan::Var(b.var(v.as_str()))),
        Expression::NamedNode(n) => Ok(ExprPlan::Const(Term::NamedNode(n.clone()))),
        Expression::Literal(l) => Ok(ExprPlan::Const(Term::Literal(l.clone()))),
        Expression::Bound(v) => Ok(ExprPlan::Bound(b.var(v.as_str()))),
        Expression::Not(a) => Ok(ExprPlan::Not(Box::new(lower_expr(b, a, graph, stats)?))),
        Expression::And(a, c) => Ok(ExprPlan::And(
            Box::new(lower_expr(b, a, graph, stats)?),
            Box::new(lower_expr(b, c, graph, stats)?),
        )),
        Expression::Or(a, c) => Ok(ExprPlan::Or(
            Box::new(lower_expr(b, a, graph, stats)?),
            Box::new(lower_expr(b, c, graph, stats)?),
        )),
        Expression::SameTerm(a, c) => Ok(ExprPlan::SameTerm(
            Box::new(lower_expr(b, a, graph, stats)?),
            Box::new(lower_expr(b, c, graph, stats)?),
        )),
        Expression::Equal(a, c) => Ok(ExprPlan::Equal(
            Box::new(lower_expr(b, a, graph, stats)?),
            Box::new(lower_expr(b, c, graph, stats)?),
        )),
        // Correlated EXISTS / NOT EXISTS: lower the sub-pattern into the same
        // arena, rooted at its own InputFocus (seeded with the current solution
        // at evaluation time). NOT EXISTS arrives as Not(Exists(..)).
        Expression::Exists(pattern) => {
            let leaf = b.push(NativeOp::InputFocus);
            let root = lower_pattern(b, pattern, leaf, graph, stats)?;
            Ok(ExprPlan::Exists(root))
        }
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
        Expression::FunctionCall(function, args) => match (function, args.as_slice()) {
            (Function::Str, [arg]) => {
                Ok(ExprPlan::Str(Box::new(lower_expr(b, arg, graph, stats)?)))
            }
            (Function::StrStarts, [text, prefix]) => Ok(ExprPlan::StrStarts(
                Box::new(lower_expr(b, text, graph, stats)?),
                Box::new(lower_expr(b, prefix, graph, stats)?),
            )),
            _ => Err("function call".into()),
        },
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
    fn lowers_strstarts_over_str() {
        let q = parse("ASK { ?this ?p ?o FILTER (STRSTARTS(STR(?p), \"http://ex/\")) }");
        lower_query(&q).expect("STRSTARTS(STR(...), literal) should lower");
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
    fn value_equality_lowers() {
        let q = parse("ASK { ?this <http://ex/p> ?o FILTER (?o = <http://ex/target>) }");
        lower_query(&q).expect("= over IRIs should lower");
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

    /// A two-triple BGP where the second pattern has a rare predicate should be
    /// reordered first when statistics say so.
    #[test]
    fn bgp_reorder_moves_selective_scan_first() {
        // ?x ?y ?z . ?this <rare> ?x — without stats: $this-scan is first,
        // rare-predicate-scan is second. With stats that give <rare> cardinality
        // 1 (vs. the first scan which has an unbound predicate, cost = total),
        // the planner should move the rare scan first.
        let q = parse("SELECT ?x WHERE { ?x ?y ?z . ?this <http://ex/rare> ?x }");
        let rare = Term::NamedNode(NamedNode::new_unchecked("http://ex/rare"));
        let mut pcard = HashMap::new();
        pcard.insert(rare.clone(), 1u64);
        let stats = PlanStats {
            total_triples: 100_000,
            distinct_subjects: 10_000,
            distinct_objects: 10_000,
            distinct_predicates: 50,
            predicate_cardinality: pcard,
        };
        let plan = lower_query_with_stats(&q, Some(&stats)).expect("should lower");
        // Collect scans in pipeline order (InputFocus → first Scan → second Scan …)
        let scans: Vec<&TripleScan> = plan
            .nodes
            .iter()
            .filter_map(|op| {
                if let NativeOp::Scan { pattern, .. } = op {
                    Some(pattern)
                } else {
                    None
                }
            })
            .collect();
        // The rare predicate scan must come before the free-predicate scan.
        let rare_pos = scans
            .iter()
            .position(|s| matches!(&s.predicate, ScanTerm::Const(t) if t == &rare));
        let free_pos = scans
            .iter()
            .position(|s| matches!(&s.predicate, ScanTerm::Var(_)));
        assert!(
            rare_pos < free_pos,
            "expected rare-predicate scan first; got rare={rare_pos:?} free={free_pos:?}",
        );
    }

    /// `Join(Join(Bgp1, Bgp2), Bgp3)` — a nested join where the outer left arm
    /// is itself a Join — should be flattened into three leaves and the rare-
    /// predicate leaf placed first, even though two-arm reordering of the outer
    /// join would leave Bgp3 stranded at the end.
    #[test]
    fn join_flattens_three_way_nested() {
        use spargebra::term::{TriplePattern, Variable};

        let nn = |s: &str| NamedNode::new_unchecked(s);
        let var = |s: &str| Variable::new_unchecked(s);
        let bgp = |s: &str, p: &str, o: &str| GraphPattern::Bgp {
            patterns: vec![TriplePattern {
                subject: TermPattern::Variable(var(s)),
                predicate: NamedNodePattern::NamedNode(nn(p)),
                object: TermPattern::Variable(var(o)),
            }],
        };

        // Bgp1: ?this :common ?x  — ?this bound → SP range, cost ≈ 2
        // Bgp2: ?x :medium ?y    — nothing bound, cost = medium_card = 10_000
        // Bgp3: ?x :rare ?y      — nothing bound, cost = rare_card = 1
        // Naive order: Bgp1, Bgp2, Bgp3 (outer Join sees a Join on the left,
        //   not a Bgp/Path, so two-arm reordering doesn't fire for the outer).
        // Flat greedy order: Bgp3 (cost 1), then Bgp1 and Bgp2 (both cost 1
        //   once ?x and ?y are bound).
        let bgp1 = bgp("this", "http://ex/common", "x");
        let bgp2 = bgp("x", "http://ex/medium", "y");
        let bgp3 = bgp("x", "http://ex/rare", "y");

        let query = Query::Ask {
            pattern: GraphPattern::Join {
                left: Box::new(GraphPattern::Join {
                    left: Box::new(bgp1),
                    right: Box::new(bgp2),
                }),
                right: Box::new(bgp3),
            },
            dataset: None,
            base_iri: None,
        };

        let mut pcard = HashMap::new();
        pcard.insert(Term::NamedNode(nn("http://ex/common")), 10_000u64);
        pcard.insert(Term::NamedNode(nn("http://ex/medium")), 10_000u64);
        pcard.insert(Term::NamedNode(nn("http://ex/rare")), 1u64);
        let stats = PlanStats {
            total_triples: 100_000,
            distinct_subjects: 10_000,
            distinct_objects: 10_000,
            distinct_predicates: 50,
            predicate_cardinality: pcard,
        };

        let plan = lower_query_with_stats(&query, Some(&stats)).expect("should lower");

        let rare = Term::NamedNode(nn("http://ex/rare"));
        let scans: Vec<&TripleScan> = plan
            .nodes
            .iter()
            .filter_map(|op| {
                if let NativeOp::Scan { pattern, .. } = op {
                    Some(pattern)
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(
            scans.len(),
            3,
            "expected 3 scans; got {}",
            scans.len()
        );
        assert!(
            matches!(&scans[0].predicate, ScanTerm::Const(t) if t == &rare),
            "rare-predicate scan should be first; got predicates: {:?}",
            scans.iter().map(|s| &s.predicate).collect::<Vec<_>>()
        );
    }

    /// A `Join(Bgp, Path)` where the path arm has a constant endpoint should be
    /// reordered to `Join(Path, Bgp)` when stats are present, because the path
    /// closure is cheaper than the SP range scan for the BGP arm.
    #[test]
    fn join_reorders_path_before_bgp_when_cheaper() {
        // "?this ?p ?v . ?p <rdf:type>* <ex:X>" — BGP has free predicate (cost
        // ≈ total/subjects) while Path has a constant object (cost ≈ total/subjects/2).
        let q = parse(
            "ASK { ?this ?p ?v \
             . ?p <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>* <http://ex/X> }",
        );
        let stats = PlanStats {
            total_triples: 100_000,
            distinct_subjects: 10_000,
            distinct_objects: 10_000,
            distinct_predicates: 50,
            predicate_cardinality: HashMap::new(),
        };
        let plan = lower_query_with_stats(&q, Some(&stats)).expect("should lower");
        // After swap the PathScan should feed directly from InputFocus (node 0).
        let path_inputs_focus = plan.nodes.iter().any(|op| {
            matches!(op, NativeOp::PathScan { input, .. } if *input == 0)
        });
        assert!(
            path_inputs_focus,
            "PathScan should be evaluated first (input = InputFocus)"
        );
    }
}
