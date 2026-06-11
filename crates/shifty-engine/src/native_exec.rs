//! Stage 3 native BGP executor.
//!
//! Evaluates a [`NativeQueryPlan`] (lowered in `shifty-opt`) directly over a
//! [`FrozenIndexedDataset`], batched across focus nodes. This is the fast path
//! for constraint/target queries in the BGP subset; everything else stays on the
//! Spareval fallback. The executor must agree with Spareval-over-frozen for every
//! query it runs — `sparql.rs` cross-checks that with a differential assertion
//! under `debug_assertions` (`docs/05-sparql-execution.md §254`).
//!
//! ## Model
//!
//! Operators are evaluated bottom-up into `Vec<Solution>`. `InputFocus` emits one
//! seed per focus node (binding `$this`); every solution carries a `FocusId` so a
//! single batched run keeps each focus node's results separate — `DISTINCT`,
//! projection, and result bucketing are all per focus, exactly as if the query
//! had been run once per node with `$this` pre-bound. `Scan` is indexed
//! nested-loop matching: positions already bound (constants or bound variables)
//! are pushed into the index lookup; free variables are bound from each matching
//! triple, with repeated variables checked for consistency.

use crate::frozen::{FrozenIndexedDataset, GraphSel, TermId};
use crate::path_plan::{ReachStep, apply_closure, compile_bwd, compile_fwd};
use oxrdf::vocab::xsd;
use oxrdf::{Literal, Term};
use shifty_opt::{
    ExprPlan, GraphScan, NativeOp, NativeQueryPlan, OpId, PathScan, QueryForm, ScanTerm,
    TripleScan, VarId,
};
use std::collections::{BTreeMap, HashSet};

/// Index of a focus node within the batch passed to [`execute`].
type FocusId = u32;

/// A partial solution: variable bindings (by `VarId`, as term ids) tagged with
/// the focus node they descend from. `BTreeMap` keeps bindings in a canonical
/// order so `DISTINCT` can key on them directly.
#[derive(Clone, PartialEq, Eq)]
struct Solution {
    focus: FocusId,
    bindings: BTreeMap<VarId, TermId>,
}

/// A solution exposed to the caller: variable name → RDF term.
pub(crate) type NativeBindings = BTreeMap<String, Term>;
pub(crate) type NativeIdBindings = BTreeMap<VarId, TermId>;

/// Result of a batched native run, grouped per input focus node (same order as
/// the `foci` slice passed to [`execute`]).
pub(crate) struct NativeExecResult {
    pub form: QueryForm,
    /// `solutions[i]` are the solutions for `foci[i]`.
    pub solutions: Vec<Vec<NativeBindings>>,
}

pub(crate) struct NativeIdExecResult {
    pub form: QueryForm,
    pub solutions: Vec<Vec<NativeIdBindings>>,
}

/// Run `plan` over `ds` for the given focus nodes.
pub(crate) fn execute(
    plan: &NativeQueryPlan,
    ds: &FrozenIndexedDataset,
    foci: &[Term],
) -> NativeExecResult {
    let result = execute_ids(plan, ds, foci);
    let solutions = result
        .solutions
        .into_iter()
        .map(|bucket| {
            bucket
                .into_iter()
                .map(|bindings| {
                    bindings
                        .into_iter()
                        .filter_map(|(v, t)| {
                            let name = plan.var_names.get(v as usize)?.clone();
                            let term = ds.externalize(t)?;
                            Some((name, term))
                        })
                        .collect()
                })
                .collect()
        })
        .collect();

    NativeExecResult {
        form: result.form,
        solutions,
    }
}

/// Execute without externalizing term IDs or variable names.
pub(crate) fn execute_ids(
    plan: &NativeQueryPlan,
    ds: &FrozenIndexedDataset,
    foci: &[Term],
) -> NativeIdExecResult {
    let exec = NativeExecutor::new(plan, ds);
    let seeds: Vec<Solution> = foci
        .iter()
        .enumerate()
        .map(|(i, term)| {
            let mut bindings = BTreeMap::new();
            bindings.insert(plan.focus_var, ds.intern(term));
            Solution {
                focus: i as FocusId,
                bindings,
            }
        })
        .collect();

    let solutions = exec.eval(plan.root, &seeds);

    let mut buckets: Vec<Vec<NativeIdBindings>> = vec![Vec::new(); foci.len()];
    for sol in solutions {
        buckets[sol.focus as usize].push(sol.bindings);
    }

    NativeIdExecResult {
        form: plan.form,
        solutions: buckets,
    }
}

/// Derive focus nodes whose query solutions involve at least one triple from
/// `delta`. This is the differential of the supported positive relational
/// subset: each default-graph scan is restricted to the delta in turn while
/// all other scans see the current full dataset.
///
/// `None` means the plan contains an operator for which this differential
/// execution has not been proven sound; callers must evaluate all focus nodes.
pub(crate) fn delta_focus_ids(
    plan: &NativeQueryPlan,
    ds: &FrozenIndexedDataset,
    delta: &[oxrdf::Triple],
) -> Option<HashSet<TermId>> {
    let scans = linear_bgp_scans(plan)?;
    if !scans
        .iter()
        .any(|scan| scan_variables(scan).contains(&plan.focus_var))
    {
        // A WHERE clause independent of `$this` can gain a solution that must
        // be applied to every focus node.
        return None;
    }
    if delta.is_empty() {
        return Some(HashSet::new());
    }

    let encoded: Vec<_> = delta
        .iter()
        .map(|triple| ds.encode_triple(triple))
        .collect();
    let exec = NativeExecutor::new(plan, ds);
    let mut foci = HashSet::new();

    for (delta_index, delta_scan) in scans.iter().enumerate() {
        if !matches!(delta_scan.graph, GraphScan::Default) {
            continue;
        }

        let seed = Solution {
            focus: 0,
            bindings: BTreeMap::new(),
        };
        let mut solutions = exec.extend_encoded(&[seed], delta_scan, &encoded);
        if solutions.is_empty() {
            continue;
        }

        let mut bound = scan_variables(delta_scan);
        let mut remaining: Vec<usize> = (0..scans.len())
            .filter(|&index| index != delta_index)
            .collect();
        while !remaining.is_empty() {
            let Some((position, _)) = remaining
                .iter()
                .enumerate()
                .filter_map(|(position, &index)| {
                    let variables = scan_variables(scans[index]);
                    let shared = variables.intersection(&bound).count();
                    (shared > 0).then_some((position, shared))
                })
                .max_by_key(|&(_, shared)| shared)
            else {
                // A disconnected BGP component can turn a local delta into
                // results for every focus node; narrowing would be unsound.
                return None;
            };
            let index = remaining.swap_remove(position);
            solutions = exec.extend_scan(&solutions, scans[index]);
            if solutions.is_empty() {
                break;
            }
            bound.extend(scan_variables(scans[index]));
        }

        for solution in solutions {
            if let Some(&focus) = solution.bindings.get(&plan.focus_var) {
                foci.insert(focus);
            }
        }
    }
    Some(foci)
}

fn linear_bgp_scans(plan: &NativeQueryPlan) -> Option<Vec<&TripleScan>> {
    let mut op = plan.root;
    let mut scans = Vec::new();
    loop {
        match &plan.nodes[op as usize] {
            NativeOp::InputFocus => return Some(scans),
            NativeOp::Scan { input, pattern } => {
                scans.push(pattern);
                op = *input;
            }
            NativeOp::Project { input, vars } if vars.contains(&plan.focus_var) => {
                op = *input;
            }
            NativeOp::Distinct { input } => op = *input,
            NativeOp::PathScan { .. }
            | NativeOp::Union { .. }
            | NativeOp::Filter { .. }
            | NativeOp::Extend { .. }
            | NativeOp::Project { .. } => return None,
        }
    }
}

fn scan_variables(scan: &TripleScan) -> HashSet<VarId> {
    [&scan.subject, &scan.predicate, &scan.object]
        .into_iter()
        .filter_map(|term| match term {
            ScanTerm::Var(variable) => Some(*variable),
            ScanTerm::Const(_) => None,
        })
        .collect()
}

impl NativeExecutor<'_> {
    fn extend_scan(&self, input: &[Solution], pattern: &TripleScan) -> Vec<Solution> {
        let graph = self.graph_sel(&pattern.graph);
        let mut out = Vec::new();
        for sol in input {
            let s = self.resolve(&pattern.subject, sol);
            let p = self.resolve(&pattern.predicate, sol);
            let o = self.resolve(&pattern.object, sol);
            for [ts, tp, to] in self.ds.scan(s.probe(), p.probe(), o.probe(), graph) {
                let mut next = sol.clone();
                if bind(&mut next, &s, ts) && bind(&mut next, &p, tp) && bind(&mut next, &o, to) {
                    out.push(next);
                }
            }
        }
        out
    }

    fn extend_encoded(
        &self,
        input: &[Solution],
        pattern: &TripleScan,
        triples: &[[TermId; 3]],
    ) -> Vec<Solution> {
        let mut out = Vec::new();
        for sol in input {
            let s = self.resolve(&pattern.subject, sol);
            let p = self.resolve(&pattern.predicate, sol);
            let o = self.resolve(&pattern.object, sol);
            for &[ts, tp, to] in triples {
                if s.probe().is_some_and(|bound| bound != ts)
                    || p.probe().is_some_and(|bound| bound != tp)
                    || o.probe().is_some_and(|bound| bound != to)
                {
                    continue;
                }
                let mut next = sol.clone();
                if bind(&mut next, &s, ts) && bind(&mut next, &p, tp) && bind(&mut next, &o, to) {
                    out.push(next);
                }
            }
        }
        out
    }
}

struct NativeExecutor<'a> {
    plan: &'a NativeQueryPlan,
    ds: &'a FrozenIndexedDataset,
    /// Pre-compiled (fwd, bwd) reach steps for each `PathScan` op, indexed by
    /// `OpId`. `None` for non-path ops.
    path_plans: Vec<Option<(ReachStep, ReachStep)>>,
}

impl<'a> NativeExecutor<'a> {
    fn new(plan: &'a NativeQueryPlan, ds: &'a FrozenIndexedDataset) -> Self {
        let path_plans = plan
            .nodes
            .iter()
            .map(|op| {
                if let NativeOp::PathScan { scan, .. } = op {
                    Some((compile_fwd(&scan.step, ds), compile_bwd(&scan.step, ds)))
                } else {
                    None
                }
            })
            .collect();
        Self {
            plan,
            ds,
            path_plans,
        }
    }
}

/// A scan operand resolved against the current solution: either bound to a term
/// id (constant, or an already-bound variable) or a free variable to bind.
#[derive(Clone, Copy)]
enum Resolved {
    Bound(TermId),
    Free(VarId),
}

impl Resolved {
    /// The term id to push into the index lookup (`None` for a free variable).
    fn probe(&self) -> Option<TermId> {
        match self {
            Resolved::Bound(t) => Some(*t),
            Resolved::Free(_) => None,
        }
    }
}

impl NativeExecutor<'_> {
    fn eval(&self, op: OpId, seeds: &[Solution]) -> Vec<Solution> {
        match &self.plan.nodes[op as usize] {
            NativeOp::InputFocus => seeds.to_vec(),
            NativeOp::Scan { input, pattern } => {
                let input = self.eval(*input, seeds);
                let graph = self.graph_sel(&pattern.graph);
                let mut out = Vec::new();
                for sol in &input {
                    let s = self.resolve(&pattern.subject, sol);
                    let p = self.resolve(&pattern.predicate, sol);
                    let o = self.resolve(&pattern.object, sol);
                    for [ts, tp, to] in self.ds.scan(s.probe(), p.probe(), o.probe(), graph) {
                        let mut next = sol.clone();
                        if bind(&mut next, &s, ts)
                            && bind(&mut next, &p, tp)
                            && bind(&mut next, &o, to)
                        {
                            out.push(next);
                        }
                    }
                }
                out
            }
            NativeOp::PathScan { input, scan } => {
                let input = self.eval(*input, seeds);
                let graph = self.graph_sel(&scan.graph);
                let (fwd, bwd) = self.path_plans[op as usize].as_ref().unwrap();
                let mut out = Vec::new();
                for sol in &input {
                    self.path_scan_extend(sol, scan, graph, fwd, bwd, &mut out);
                }
                out
            }
            NativeOp::Union { left, right } => {
                let mut out = self.eval(*left, seeds);
                out.extend(self.eval(*right, seeds));
                out
            }
            NativeOp::Filter { input, expr } => {
                let input = self.eval(*input, seeds);
                input
                    .into_iter()
                    .filter(|sol| self.ebv(expr, sol) == Some(true))
                    .collect()
            }
            NativeOp::Extend { input, var, expr } => {
                let input = self.eval(*input, seeds);
                input
                    .into_iter()
                    .map(|mut sol| {
                        // BIND of an existing variable is illegal in SPARQL, so a
                        // present binding never happens here; an erroring
                        // expression leaves the variable unbound (row retained).
                        if let Some(t) = self.eval_value(expr, &sol) {
                            sol.bindings.insert(*var, t);
                        }
                        sol
                    })
                    .collect()
            }
            NativeOp::Project { input, vars } => {
                let input = self.eval(*input, seeds);
                input
                    .into_iter()
                    .map(|sol| {
                        let bindings = vars
                            .iter()
                            .filter_map(|v| sol.bindings.get(v).map(|t| (*v, *t)))
                            .collect();
                        Solution {
                            focus: sol.focus,
                            bindings,
                        }
                    })
                    .collect()
            }
            NativeOp::Distinct { input } => {
                let input = self.eval(*input, seeds);
                let mut seen = HashSet::new();
                input
                    .into_iter()
                    .filter(|sol| seen.insert((sol.focus, sol.bindings.clone())))
                    .collect()
            }
        }
    }

    fn resolve(&self, t: &ScanTerm, sol: &Solution) -> Resolved {
        match t {
            ScanTerm::Const(term) => Resolved::Bound(self.ds.intern(term)),
            ScanTerm::Var(v) => match sol.bindings.get(v) {
                Some(&id) => Resolved::Bound(id),
                None => Resolved::Free(*v),
            },
        }
    }

    fn graph_sel(&self, graph: &GraphScan) -> GraphSel {
        match graph {
            GraphScan::Default => GraphSel::Default,
            GraphScan::Named(nn) => GraphSel::Named(self.ds.intern(&Term::NamedNode(nn.clone()))),
        }
    }

    // ── property paths ────────────────────────────────────────────────────────

    /// Extend one input solution by an arbitrary-length path, appending the
    /// resulting solutions to `out`. Distinct (set) endpoint semantics, per
    /// SPARQL `*`/`+`/`?`.
    fn path_scan_extend(
        &self,
        sol: &Solution,
        scan: &PathScan,
        graph: GraphSel,
        fwd: &ReachStep,
        bwd: &ReachStep,
        out: &mut Vec<Solution>,
    ) {
        let s = self.resolve(&scan.subject, sol);
        let o = self.resolve(&scan.object, sol);
        match (s, o) {
            (Resolved::Bound(start), Resolved::Bound(end)) => {
                if apply_closure(start, fwd, scan.kind, self.ds, graph).contains(&end) {
                    out.push(sol.clone());
                }
            }
            (Resolved::Bound(start), Resolved::Free(ov)) => {
                for end in apply_closure(start, fwd, scan.kind, self.ds, graph) {
                    let mut next = sol.clone();
                    next.bindings.insert(ov, end);
                    out.push(next);
                }
            }
            (Resolved::Free(sv), Resolved::Bound(end)) => {
                for start in apply_closure(end, bwd, scan.kind, self.ds, graph) {
                    let mut next = sol.clone();
                    next.bindings.insert(sv, start);
                    out.push(next);
                }
            }
            (Resolved::Free(sv), Resolved::Free(ov)) => {
                // O(|triples|): enumerates all nodes. Rare in SHACL (both endpoints
                // free only for unconstrained ?s p* ?o). If this becomes common,
                // precompute a node-domain index in FrozenIndexedDataset at build time.
                for start in self.node_domain(graph) {
                    for end in apply_closure(start, fwd, scan.kind, self.ds, graph) {
                        let mut next = sol.clone();
                        // sv and ov may be the same variable ⇒ require start == end.
                        if sv == ov {
                            if start == end {
                                next.bindings.insert(sv, start);
                                out.push(next);
                            }
                        } else {
                            next.bindings.insert(sv, start);
                            next.bindings.insert(ov, end);
                            out.push(next);
                        }
                    }
                }
            }
        }
    }

    /// All terms that occur as a subject or object in graph `g` — the node domain
    /// for a both-endpoints-free path scan (SPARQL ALP over an unbound pair).
    fn node_domain(&self, g: GraphSel) -> HashSet<TermId> {
        let mut nodes = HashSet::new();
        for [s, _, o] in self.ds.scan(None, None, None, g) {
            nodes.insert(s);
            nodes.insert(o);
        }
        nodes
    }

    // ── expression evaluation ────────────────────────────────────────────────

    /// Effective boolean value, 3-valued: `None` is a SPARQL type error. Only the
    /// provably-safe subset admitted by `lower_expr` reaches here.
    fn ebv(&self, expr: &ExprPlan, sol: &Solution) -> Option<bool> {
        match expr {
            ExprPlan::Bound(v) => Some(sol.bindings.contains_key(v)),
            ExprPlan::Not(a) => self.ebv(a, sol).map(|b| !b),
            ExprPlan::And(a, b) => match (self.ebv(a, sol), self.ebv(b, sol)) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            ExprPlan::Or(a, b) => match (self.ebv(a, sol), self.ebv(b, sol)) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
            // sameTerm: term identity ⇔ term-id equality (the dictionary interns
            // by RDF-term equality). Both operands must evaluate (be bound).
            ExprPlan::SameTerm(a, b) => {
                let ta = self.eval_value(a, sol)?;
                let tb = self.eval_value(b, sol)?;
                Some(ta == tb)
            }
            // value equality (=): fast path is term-id identity, which covers
            // IRIs, blank nodes, and same-type same-lexical-form literals.
            // For different-id literals, we return None (type error) whenever
            // numeric promotion could apply, rather than risk a wrong answer.
            ExprPlan::Equal(a, b) => {
                let ta = self.eval_value(a, sol)?;
                let tb = self.eval_value(b, sol)?;
                if ta == tb {
                    return Some(true);
                }
                let ta_term = self.ds.externalize(ta)?;
                let tb_term = self.ds.externalize(tb)?;
                match (&ta_term, &tb_term) {
                    (Term::NamedNode(_), Term::NamedNode(_)) => Some(false),
                    (Term::BlankNode(_), Term::BlankNode(_)) => Some(false),
                    (Term::Literal(la), Term::Literal(lb)) => {
                        if is_numeric_datatype(la.datatype().as_str())
                            || is_numeric_datatype(lb.datatype().as_str())
                        {
                            // Cross-type or non-canonical same-type numeric: value
                            // promotion needed. Return None (type error) rather
                            // than produce a wrong result; Spareval handles these.
                            None
                        } else if la.datatype() == lb.datatype() {
                            // Same non-numeric type, different lexical form → not equal.
                            Some(false)
                        } else {
                            // Incompatible literal types → type error per SPARQL spec.
                            None
                        }
                    }
                    // IRI vs. literal or blank node vs. literal → type error.
                    _ => None,
                }
            }
            // Correlated EXISTS: run the sub-plan seeded with this solution and
            // test for any result. Never a type error (NOT EXISTS is Not(Exists)).
            ExprPlan::Exists(op) => {
                let sub = self.eval(*op, std::slice::from_ref(sol));
                Some(!sub.is_empty())
            }
            ExprPlan::Var(v) => {
                let id = sol.bindings.get(v)?;
                term_ebv(&self.ds.externalize(*id)?)
            }
            ExprPlan::Const(term) => term_ebv(term),
        }
    }

    /// Value of an expression as a term id, or `None` on error/unbound.
    fn eval_value(&self, expr: &ExprPlan, sol: &Solution) -> Option<TermId> {
        match expr {
            ExprPlan::Var(v) => sol.bindings.get(v).copied(),
            ExprPlan::Const(term) => Some(self.ds.intern(term)),
            // Boolean-valued forms: materialize their EBV as an xsd:boolean term.
            ExprPlan::Bound(_)
            | ExprPlan::Not(_)
            | ExprPlan::And(..)
            | ExprPlan::Or(..)
            | ExprPlan::SameTerm(..)
            | ExprPlan::Equal(..)
            | ExprPlan::Exists(_) => {
                let b = self.ebv(expr, sol)?;
                Some(self.ds.intern(&Term::Literal(Literal::from(b))))
            }
        }
    }
}

/// Bind a free variable from a matched triple position, or verify an
/// already-fixed position. Returns `false` only when a repeated free variable
/// disagrees across positions of the same triple (so the row is rejected).
/// Bound positions were already enforced by the index lookup.
fn bind(sol: &mut Solution, r: &Resolved, term: TermId) -> bool {
    match r {
        Resolved::Bound(_) => true,
        Resolved::Free(v) => match sol.bindings.get(v) {
            Some(&existing) => existing == term,
            None => {
                sol.bindings.insert(*v, term);
                true
            }
        },
    }
}

/// SPARQL effective boolean value of a concrete term. `None` is a type error
/// (IRIs, blank nodes, lang-tagged or non-EBV-typed literals).
fn term_ebv(term: &Term) -> Option<bool> {
    let Term::Literal(l) = term else {
        return None;
    };
    let dt = l.datatype();
    if dt == xsd::BOOLEAN {
        match l.value() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        }
    } else if dt == xsd::STRING {
        Some(!l.value().is_empty())
    } else if is_numeric_datatype(dt.as_str()) {
        // Ill-formed numeric lexical forms are a type error.
        let v: f64 = l.value().trim().parse().ok()?;
        Some(v != 0.0 && !v.is_nan())
    } else {
        None
    }
}

fn is_numeric_datatype(dt: &str) -> bool {
    const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
    let Some(local) = dt.strip_prefix(XSD) else {
        return false;
    };
    matches!(
        local,
        "integer"
            | "decimal"
            | "float"
            | "double"
            | "long"
            | "int"
            | "short"
            | "byte"
            | "nonNegativeInteger"
            | "nonPositiveInteger"
            | "negativeInteger"
            | "positiveInteger"
            | "unsignedLong"
            | "unsignedInt"
            | "unsignedShort"
            | "unsignedByte"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frozen::FrozenIndexedDataset;
    use oxigraph::sparql::{QueryResults, SparqlEvaluator};
    use oxrdf::{Graph, Literal, NamedNode, Triple};
    use shifty_opt::lower_query;
    use spargebra::SparqlParser;
    use std::collections::BTreeSet;

    fn nn(iri: &str) -> NamedNode {
        NamedNode::new(iri).unwrap()
    }

    fn t(s: &str, p: &str, o: &str) -> Triple {
        Triple::new(nn(s), nn(p), nn(o))
    }

    fn sample() -> FrozenIndexedDataset {
        let mut g = Graph::new();
        for tr in [
            t("http://ex/a", "http://ex/p", "http://ex/b"),
            t("http://ex/a", "http://ex/p", "http://ex/c"),
            t("http://ex/b", "http://ex/q", "http://ex/d"),
            t("http://ex/c", "http://ex/q", "http://ex/d"),
            t("http://ex/c", "http://ex/flag", "http://ex/bad"),
        ] {
            g.insert(tr.as_ref());
        }
        FrozenIndexedDataset::from_graph(&g)
    }

    /// One solution rendered canonically (`name=term`, sorted, comma-joined).
    fn render(bindings: &NativeBindings) -> String {
        bindings
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Native solutions for `query` and a single focus, as a multiset of rows.
    fn native_rows(ds: &FrozenIndexedDataset, query: &str, focus: &str) -> Vec<String> {
        let parsed = SparqlParser::new().parse_query(query).unwrap();
        let plan = lower_query(&parsed).expect("query should lower to native");
        let foci = [Term::NamedNode(nn(focus))];
        let result = execute(&plan, ds, &foci);
        let mut rows: Vec<String> = result.solutions[0].iter().map(render).collect();
        rows.sort();
        rows
    }

    /// Spareval-over-frozen solutions for the same query with `$this` textually
    /// replaced by the focus IRI — the differential oracle.
    fn spareval_rows(ds: &FrozenIndexedDataset, query: &str, focus: &str) -> Vec<String> {
        let grounded = query.replace("$this", &format!("<{focus}>"));
        let prepared = SparqlEvaluator::new().parse_query(&grounded).unwrap();
        let QueryResults::Solutions(solutions) =
            prepared.on_queryable_dataset(ds).execute().unwrap()
        else {
            panic!("expected SELECT solutions");
        };
        let mut rows: Vec<String> = solutions
            .map(|s| {
                let s = s.unwrap();
                let mut pairs: Vec<String> = s
                    .iter()
                    .map(|(var, term)| format!("{}={}", var.as_str(), term))
                    .collect();
                pairs.sort();
                pairs.join(",")
            })
            .collect();
        rows.sort();
        rows
    }

    /// Assert native and Spareval agree on `query` for `focus`.
    fn assert_agrees(query: &str, focus: &str) {
        let ds = sample();
        let native = native_rows(&ds, query, focus);
        let spareval = spareval_rows(&ds, query, focus);
        assert_eq!(native, spareval, "disagreement on `{query}` @ {focus}");
    }

    #[test]
    fn single_scan_matches_spareval() {
        assert_agrees(
            "SELECT ?value WHERE { <http://ex/a> <http://ex/p> ?value }",
            "http://ex/a",
        );
    }

    #[test]
    fn join_matches_spareval() {
        assert_agrees(
            "SELECT ?value ?w WHERE { $this <http://ex/p> ?value . ?value <http://ex/q> ?w }",
            "http://ex/a",
        );
    }

    #[test]
    fn union_matches_spareval() {
        assert_agrees(
            "SELECT ?o WHERE { { $this <http://ex/p> ?o } UNION { ?o <http://ex/q> <http://ex/d> } }",
            "http://ex/a",
        );
    }

    #[test]
    fn distinct_matches_spareval() {
        assert_agrees(
            "SELECT DISTINCT ?o WHERE { { $this <http://ex/p> ?o } UNION { $this <http://ex/p> ?o } }",
            "http://ex/a",
        );
    }

    #[test]
    fn safe_filter_matches_spareval() {
        assert_agrees(
            "SELECT ?value WHERE { $this <http://ex/p> ?value FILTER (!sameTerm(?value, <http://ex/b>)) }",
            "http://ex/a",
        );
    }

    #[test]
    fn focus_seeds_this_variable() {
        // `$this` flows through as the focus binding; the inner scan finds the
        // flagged value only for focus ex:c.
        let ds = sample();
        let q = "SELECT ?value WHERE { $this <http://ex/p> ?value . ?value <http://ex/flag> <http://ex/bad> }";
        let parsed = SparqlParser::new().parse_query(q).unwrap();
        let plan = lower_query(&parsed).unwrap();
        let foci = [Term::NamedNode(nn("http://ex/a"))];
        let result = execute(&plan, &ds, &foci);
        // ex:a --p--> {b, c}; only c is flagged ⇒ one solution, ?value = ex:c.
        assert_eq!(result.solutions[0].len(), 1);
        assert_eq!(
            result.solutions[0][0].get("value"),
            Some(&Term::NamedNode(nn("http://ex/c"))),
        );
    }

    #[test]
    fn batches_keep_foci_separate() {
        let ds = sample();
        let q = "SELECT ?value WHERE { $this <http://ex/p> ?value }";
        let parsed = SparqlParser::new().parse_query(q).unwrap();
        let plan = lower_query(&parsed).unwrap();
        let foci = [
            Term::NamedNode(nn("http://ex/a")),
            Term::NamedNode(nn("http://ex/b")), // no ex:p edges
        ];
        let result = execute(&plan, &ds, &foci);
        assert_eq!(result.solutions[0].len(), 2); // a -> b, c
        assert_eq!(result.solutions[1].len(), 0); // b has none
    }

    #[test]
    fn delta_join_derives_focus_through_another_scan() {
        let delta = t("http://ex/p", "http://ex/inverseOf", "http://ex/inverseP");
        let mut graph = Graph::new();
        graph.insert(t("http://ex/a", "http://ex/p", "http://ex/b").as_ref());
        graph.insert(delta.as_ref());
        let ds = FrozenIndexedDataset::from_graph(&graph);
        let query = "SELECT * WHERE {
            $this ?predicate ?object .
            ?predicate <http://ex/inverseOf> ?inverse .
        }";
        let parsed = SparqlParser::new().parse_query(query).unwrap();
        let plan = lower_query(&parsed).unwrap();

        let actual = delta_focus_ids(&plan, &ds, &[delta]).unwrap();

        assert_eq!(
            actual,
            HashSet::from([ds.intern(&Term::NamedNode(nn("http://ex/a")))])
        );
    }

    #[test]
    fn delta_focus_rejects_non_bgp_plans() {
        let ds = sample();
        let query = "SELECT * WHERE {
            { $this <http://ex/p> ?object }
            UNION
            { $this <http://ex/q> ?object }
        }";
        let parsed = SparqlParser::new().parse_query(query).unwrap();
        let plan = lower_query(&parsed).unwrap();
        let delta = t("http://ex/a", "http://ex/p", "http://ex/new");

        assert!(delta_focus_ids(&plan, &ds, &[delta]).is_none());
    }

    #[test]
    fn delta_focus_rejects_queries_independent_of_focus() {
        let ds = sample();
        let query = "SELECT * WHERE {
            ?subject <http://ex/p> ?object .
        }";
        let parsed = SparqlParser::new().parse_query(query).unwrap();
        let plan = lower_query(&parsed).unwrap();
        let delta = t("http://ex/a", "http://ex/p", "http://ex/new");

        assert!(delta_focus_ids(&plan, &ds, &[delta]).is_none());
    }

    #[test]
    fn term_ebv_basics() {
        assert_eq!(term_ebv(&Term::Literal(Literal::from(true))), Some(true));
        assert_eq!(term_ebv(&Term::Literal(Literal::from(false))), Some(false));
        assert_eq!(term_ebv(&Term::Literal(Literal::from(0_i64))), Some(false));
        assert_eq!(term_ebv(&Term::Literal(Literal::from(3_i64))), Some(true));
        assert_eq!(term_ebv(&Term::Literal(Literal::from(""))), Some(false));
        assert_eq!(term_ebv(&Term::Literal(Literal::from("x"))), Some(true));
        // IRIs have no EBV (type error).
        assert_eq!(term_ebv(&Term::NamedNode(nn("http://ex/a"))), None);
    }

    #[test]
    fn star_path_matches_spareval() {
        assert_agrees("SELECT ?o WHERE { $this <http://ex/p>* ?o }", "http://ex/a");
    }

    #[test]
    fn plus_path_matches_spareval() {
        assert_agrees("SELECT ?o WHERE { $this <http://ex/q>+ ?o }", "http://ex/b");
    }

    #[test]
    fn opt_path_matches_spareval() {
        assert_agrees("SELECT ?o WHERE { $this <http://ex/p>? ?o }", "http://ex/a");
    }

    #[test]
    fn inverse_star_path_matches_spareval() {
        // start free, end bound (reverse closure).
        assert_agrees(
            "SELECT ?s WHERE { ?s <http://ex/p>* <http://ex/c> }",
            "http://ex/a",
        );
    }

    #[test]
    fn sequence_path_preserves_multiset() {
        // a -p-> {b,c} -q-> d : ?o = d appears TWICE. The sequence is translated
        // to joined scans, so duplicates are preserved exactly like Spareval.
        let ds = sample();
        let q = "SELECT ?o WHERE { $this <http://ex/p>/<http://ex/q> ?o }";
        let native = native_rows(&ds, q, "http://ex/a");
        let spareval = spareval_rows(&ds, q, "http://ex/a");
        assert_eq!(native.len(), 2, "expected 2 (multiset) rows: {native:?}");
        assert_eq!(native, spareval);
    }

    #[test]
    fn nested_closure_in_sequence_matches_spareval() {
        assert_agrees(
            "SELECT ?o WHERE { $this <http://ex/p>/<http://ex/q>* ?o }",
            "http://ex/a",
        );
    }

    #[test]
    fn exists_filter_matches_spareval() {
        assert_agrees(
            "SELECT ?value WHERE { $this <http://ex/p> ?value FILTER EXISTS { ?value <http://ex/flag> <http://ex/bad> } }",
            "http://ex/a",
        );
    }

    #[test]
    fn not_exists_filter_matches_spareval() {
        assert_agrees(
            "SELECT ?value WHERE { $this <http://ex/p> ?value FILTER NOT EXISTS { ?value <http://ex/flag> <http://ex/bad> } }",
            "http://ex/a",
        );
    }

    #[test]
    fn equal_iri_constant_matches_spareval() {
        // FILTER(?value = <iri>): term-id fast path keeps the matching row.
        assert_agrees(
            "SELECT ?value WHERE { $this <http://ex/p> ?value FILTER (?value = <http://ex/b>) }",
            "http://ex/a",
        );
    }

    #[test]
    fn equal_variable_to_variable_matches_spareval() {
        // FILTER(?x = ?y): rows where two paths converge on the same node.
        assert_agrees(
            "SELECT ?o WHERE { $this <http://ex/p> ?o . $this <http://ex/p> ?o FILTER (?o = ?o) }",
            "http://ex/a",
        );
    }

    #[test]
    fn equal_unequal_iris_matches_spareval() {
        // FILTER(?value = <iri>) where ?value never equals the constant.
        assert_agrees(
            "SELECT ?value WHERE { $this <http://ex/p> ?value FILTER (?value = <http://ex/nonexistent>) }",
            "http://ex/a",
        );
    }

    #[test]
    fn repeated_variable_is_consistency_checked() {
        // `?x ?x ?x` matches only triples whose three positions coincide; the
        // sample graph has none, so the result is empty (and must not panic).
        let ds = sample();
        let rows: BTreeSet<String> =
            native_rows(&ds, "SELECT ?x WHERE { ?x ?x ?x }", "http://ex/a")
                .into_iter()
                .collect();
        assert!(rows.is_empty());
    }
}
