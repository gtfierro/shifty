//! Reference shape satisfaction `G, v ⊨ φ` and schema validation `G ⊨ S`
//! (doc 00 §3–§4, Table 2). This is the conformance *oracle*: the optimized
//! engines in later layers must agree with it.
//!
//! Two evaluators share the logic: [`holds`] returns a bare bool (used for
//! target selection and counting), while [`explain`] returns the specific
//! atomic constraints that failed, with the value node and path at which they
//! failed — enough for per-constraint reporting. The `∀π = ∃≤0 π.¬φ` encoding
//! lets a failed universal drill straight into the offending value node's inner
//! constraint.

use crate::frozen::FrozenIndexedDataset;
use crate::path::{PathBackend, node_of, pred, succ};
use crate::profile::ShapeCacheSample;
use crate::sparql::{SparqlExecutor, SparqlViolation};
use crate::value::{compare_terms, value_type_holds};
use oxrdf::{Graph, NamedNode, Term};
use regex::Regex;
use serde::{Deserialize, Serialize};
use shifty_algebra::render::{path_to_string, shape_to_string};
use shifty_algebra::{
    Path, Schema, Selector, Severity, Shape, ShapeArena, ShapeId, SparqlConstraint,
};
use shifty_opt::{FocusSource, PhysicalPlan, analyze};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::OnceLock;

#[derive(Debug, Clone, Copy)]
struct EvalResult {
    holds: bool,
    cacheable: bool,
}

#[derive(Default)]
struct EvalState {
    memo: HashMap<(ShapeId, Term), bool>,
    active: HashSet<(ShapeId, Term)>,
    telemetry: Option<ShapeCacheSample>,
}

/// Per-graph-snapshot shape evaluator.
///
/// Completed checks are shared across statements and focus nodes. Results that
/// depend on the coinductive recursion back-edge are deliberately not cached:
/// their provisional `true` may only be valid in the active call context.
pub(crate) struct ShapeEvaluator<'a> {
    g: &'a dyn PathBackend,
    arena: &'a ShapeArena,
    sparql: &'a SparqlExecutor,
    state: EvalState,
}

impl<'a> ShapeEvaluator<'a> {
    pub(crate) fn new(
        g: &'a dyn PathBackend,
        arena: &'a ShapeArena,
        sparql: &'a SparqlExecutor,
    ) -> Self {
        Self {
            g,
            arena,
            sparql,
            state: EvalState {
                telemetry: crate::profile::is_enabled().then(ShapeCacheSample::default),
                ..EvalState::default()
            },
        }
    }

    pub(crate) fn holds(&mut self, node: &Term, id: ShapeId) -> bool {
        holds_memoized(self.g, self.arena, node, id, self.sparql, &mut self.state).holds
    }

    pub(crate) fn sparql(&self) -> &SparqlExecutor {
        self.sparql
    }

    /// The path-evaluation backend, for sibling folds (e.g. witnessing).
    pub(crate) fn backend(&self) -> &dyn PathBackend {
        self.g
    }

    /// The shape arena, for sibling folds (e.g. witnessing).
    pub(crate) fn arena(&self) -> &ShapeArena {
        self.arena
    }
}

impl Drop for ShapeEvaluator<'_> {
    fn drop(&mut self) {
        let Some(mut sample) = self.state.telemetry else {
            return;
        };
        sample.entries = self.state.memo.len();
        sample.estimated_bytes = estimated_memo_bytes(&self.state.memo);
        crate::profile::record_shape_cache(sample);
    }
}

/// A single failed atomic constraint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reason {
    /// The node at which the constraint failed (a value node, or the focus).
    pub value: Term,
    /// The path from the enclosing focus to `value`, if the failure is
    /// value-scoped (rendered in `π` notation).
    pub path: Option<String>,
    /// The failing constraint's arena slot (cross-references the algebra dump).
    pub shape: ShapeId,
    /// `sh:severity` on the source shape, defaulting to `sh:Violation`.
    #[serde(default)]
    pub severity: Severity,
    pub message: String,
    /// Non-empty when this reason is an `sh:or` group: one entry per OR branch
    /// that failed, so the caller can tell "fix any one of these."
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sub_reasons: Vec<Reason>,
}

/// One focus node that failed its statement's shape, with the reasons why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Violation {
    pub focus: Term,
    /// Index of the violated `(selector, shape)` statement in the schema.
    pub statement: usize,
    /// The most severe top-level reason in this grouped finding.
    pub severity: Severity,
    pub reasons: Vec<Reason>,
}

/// The outcome of validating a data graph against a schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationOutcome {
    pub conforms: bool,
    pub violations: Vec<Violation>,
}

/// Controls which retained findings make a validation outcome non-conforming.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationOptions {
    /// Lowest severity that makes `conforms` false. Defaults to `sh:Info`, so
    /// all findings fail validation as they did before severity was retained.
    pub minimum_severity: Severity,
    /// Whether to sort violations by severity, focus node, and statement.
    /// Defaults to `true` for deterministic output.
    pub sort_results: bool,
}

impl Default for ValidationOptions {
    fn default() -> Self {
        Self {
            minimum_severity: Severity::Info,
            sort_results: true,
        }
    }
}

fn most_severe(reasons: &[Reason]) -> Severity {
    reasons
        .iter()
        .max_by_key(|reason| reason.severity.rank())
        .map(|reason| reason.severity.clone())
        .unwrap_or(Severity::Violation)
}

fn conforms_at_threshold(violations: &[Violation], minimum: &Severity) -> bool {
    !violations
        .iter()
        .flat_map(|violation| &violation.reasons)
        .any(|reason| reason.severity.meets(minimum))
}

fn sort_violations(violations: &mut [Violation], enabled: bool) {
    if enabled {
        violations.sort_by(|left, right| {
            right
                .severity
                .rank()
                .cmp(&left.severity.rank())
                .then_with(|| left.focus.to_string().cmp(&right.focus.to_string()))
                .then_with(|| left.statement.cmp(&right.statement))
        });
    }
}

/// Which RDF graph(s) validation uses for focus discovery and evaluation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ValidationGraphMode {
    /// Focus nodes and evaluation both use only the data graph.
    Data,
    /// Focus nodes come from data; paths, class hierarchy, and SPARQL use the
    /// union of data and shapes. This is the default for split graphs.
    #[default]
    Union,
    /// Focus discovery and evaluation both use the full data/shapes union.
    UnionAll,
}

/// The schema is not stratifiable: it recurses through genuine negation, so it
/// has no defined 2-valued semantics (`docs/03-recursion-semantics.md`). We
/// diagnose rather than guess. Carries the offending shape components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonStratifiable {
    pub components: Vec<Vec<ShapeId>>,
}

impl fmt::Display for NonStratifiable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "non-stratifiable schema (recursion through negation): ")?;
        for (i, c) in self.components.iter().enumerate() {
            if i > 0 {
                write!(f, "; ")?;
            }
            let ids: Vec<String> = c.iter().map(|s| format!("@{}", s.0)).collect();
            write!(f, "{{{}}}", ids.join(" "))?;
        }
        Ok(())
    }
}

impl std::error::Error for NonStratifiable {}

/// Validate `data` against `schema`.
///
/// Honors the decided recursion semantics (`docs/03-recursion-semantics.md`):
/// the schema must be **stratifiable** (no recursion through net negation), else
/// we return [`NonStratifiable`]. For a stratifiable schema all recursion is
/// net-positive (monotone), and [`explain`]/[`holds`]'s "assume conforming on a
/// back-edge" cycle guard computes exactly the **greatest fixpoint** — the
/// coinductive validation reading we chose.
pub fn validate(data: &Graph, schema: &Schema) -> Result<ValidationOutcome, NonStratifiable> {
    validate_with_options(data, schema, &ValidationOptions::default())
}

/// Validate `data` against `schema` using an explicit severity policy.
pub fn validate_with_options(
    data: &Graph,
    schema: &Schema,
    options: &ValidationOptions,
) -> Result<ValidationOutcome, NonStratifiable> {
    validate_with_context_and_options(data, data, schema, options)
}

/// Validate split data and shapes graphs using the selected graph mode.
pub fn validate_graphs(
    data: &Graph,
    shapes: &Graph,
    schema: &Schema,
) -> Result<ValidationOutcome, NonStratifiable> {
    validate_graphs_with_mode_and_options(
        data,
        shapes,
        schema,
        ValidationGraphMode::default(),
        &ValidationOptions::default(),
    )
}

/// Validate split data and shapes graphs using an explicit graph mode.
pub fn validate_graphs_with_mode(
    data: &Graph,
    shapes: &Graph,
    schema: &Schema,
    mode: ValidationGraphMode,
) -> Result<ValidationOutcome, NonStratifiable> {
    validate_graphs_with_mode_and_options(data, shapes, schema, mode, &ValidationOptions::default())
}

/// Validate split graphs using an explicit graph mode and severity policy.
pub fn validate_graphs_with_mode_and_options(
    data: &Graph,
    shapes: &Graph,
    schema: &Schema,
    mode: ValidationGraphMode,
    options: &ValidationOptions,
) -> Result<ValidationOutcome, NonStratifiable> {
    match mode {
        ValidationGraphMode::Data => {
            let uses_shapes = uses_shapes_graph(&schema.arena);
            let frozen = if uses_shapes {
                FrozenIndexedDataset::from_graphs(data, shapes)
            } else {
                FrozenIndexedDataset::from_graph(data)
            };
            validate_with_frozen(data, schema, frozen, uses_shapes, options)
        }
        ValidationGraphMode::Union => {
            let uses_shapes = uses_shapes_graph(&schema.arena);
            let frozen = if uses_shapes {
                FrozenIndexedDataset::from_graph_union_with_shapes(data, shapes)
            } else {
                FrozenIndexedDataset::from_graph_union(data, shapes)
            };
            validate_with_frozen(data, schema, frozen, uses_shapes, options)
        }
        ValidationGraphMode::UnionAll => {
            let union = graph_union(data, shapes);
            validate_with_context_and_options(&union, &union, schema, options)
        }
    }
}

/// Validate focus nodes from `data` while evaluating paths, class hierarchy,
/// and SPARQL against `context`. For split data/shapes inputs, `context` should
/// be their RDF union.
pub fn validate_with_context(
    data: &Graph,
    context: &Graph,
    schema: &Schema,
) -> Result<ValidationOutcome, NonStratifiable> {
    validate_with_context_and_options(data, context, schema, &ValidationOptions::default())
}

/// Validate with separate focus/context graphs and an explicit severity policy.
pub fn validate_with_context_and_options(
    data: &Graph,
    context: &Graph,
    schema: &Schema,
    options: &ValidationOptions,
) -> Result<ValidationOutcome, NonStratifiable> {
    let uses_shapes = uses_shapes_graph(&schema.arena);
    let frozen = if uses_shapes {
        FrozenIndexedDataset::from_graphs(context, context)
    } else {
        FrozenIndexedDataset::from_graph(context)
    };
    validate_with_frozen(data, schema, frozen, uses_shapes, options)
}

fn validate_with_frozen(
    data: &Graph,
    schema: &Schema,
    frozen: FrozenIndexedDataset,
    has_shapes_graph: bool,
    options: &ValidationOptions,
) -> Result<ValidationOutcome, NonStratifiable> {
    let strat = analyze(&schema.arena);
    if !strat.stratifiable {
        let components = strat
            .strata
            .iter()
            .filter(|s| !s.stratifiable)
            .map(|s| s.shapes.clone())
            .collect();
        return Err(NonStratifiable { components });
    }

    let sparql = SparqlExecutor::from_frozen(frozen, has_shapes_graph);
    let backend = sparql
        .frozen()
        .expect("validation executor always has a frozen dataset");
    let mut evaluator = ShapeEvaluator::new(backend, &schema.arena, &sparql);
    let mut violations = Vec::new();
    for (i, st) in schema.statements.iter().enumerate() {
        let label = schema
            .names
            .get(&st.shape)
            .cloned()
            .unwrap_or_else(|| format!("@{}", st.shape.0));
        let foci = focus_nodes_with_evaluator(data, &st.selector, &mut evaluator);
        prefetch_sparql_constraints(&schema.arena, st.shape, &foci, &sparql);
        for v in foci {
            let t = web_time::Instant::now();
            let mut stack = HashSet::new();
            let mut reasons = explain(
                &mut evaluator,
                &v,
                st.shape,
                None,
                &Severity::Violation,
                &mut stack,
            );
            crate::profile::record_shape(&label, t.elapsed().as_micros() as u64);
            dedup_reasons(&mut reasons);
            if !reasons.is_empty() {
                let severity = most_severe(&reasons);
                violations.push(Violation {
                    focus: v,
                    statement: i,
                    severity,
                    reasons,
                });
            }
        }
    }
    sort_violations(&mut violations, options.sort_results);
    Ok(ValidationOutcome {
        conforms: conforms_at_threshold(&violations, &options.minimum_severity),
        violations,
    })
}

/// Whether any `sh:sparql` constraint references `$shapesGraph`, requiring the
/// shapes graph to be mirrored into a named graph for evaluation.
pub(crate) fn uses_shapes_graph(arena: &ShapeArena) -> bool {
    (0..arena.len()).any(|i| {
        matches!(arena.get(ShapeId(i as u32)), Shape::Sparql(c) if c.query.contains("shapesGraph"))
    })
}

/// The RDF merge of two graphs (`left ∪ right`). The standard way to build the
/// `context` graph the `*_with_context` repair entry points expect: the union of
/// a data graph and a shapes/ontology graph.
pub fn graph_union(left: &Graph, right: &Graph) -> Graph {
    let mut union = left.clone();
    for triple in right.iter() {
        union.insert(triple);
    }
    union
}

/// Validate using a [`PhysicalPlan`] (Layer 5): focus nodes come from compiled
/// [`FocusSource`]s (so class targets seed backward from the constant instead of
/// scanning every node) and checks run over the plan's cost-ordered arena. The
/// result is identical to [`validate`] on the same schema — the W3C harness
/// cross-checks this.
pub fn validate_plan(
    data: &Graph,
    plan: &PhysicalPlan,
) -> Result<ValidationOutcome, NonStratifiable> {
    validate_plan_with_options(data, plan, &ValidationOptions::default())
}

/// Validate a physical plan using an explicit severity policy.
pub fn validate_plan_with_options(
    data: &Graph,
    plan: &PhysicalPlan,
    options: &ValidationOptions,
) -> Result<ValidationOutcome, NonStratifiable> {
    validate_plan_with_context_and_options(data, data, plan, options)
}

/// Validate a physical plan over split graphs using the default graph mode.
pub fn validate_plan_graphs(
    data: &Graph,
    shapes: &Graph,
    plan: &PhysicalPlan,
) -> Result<ValidationOutcome, NonStratifiable> {
    validate_plan_graphs_with_mode_and_options(
        data,
        shapes,
        plan,
        ValidationGraphMode::default(),
        &ValidationOptions::default(),
    )
}

/// Validate a physical plan over split graphs using an explicit graph mode.
pub fn validate_plan_graphs_with_mode(
    data: &Graph,
    shapes: &Graph,
    plan: &PhysicalPlan,
    mode: ValidationGraphMode,
) -> Result<ValidationOutcome, NonStratifiable> {
    validate_plan_graphs_with_mode_and_options(
        data,
        shapes,
        plan,
        mode,
        &ValidationOptions::default(),
    )
}

/// Validate a physical plan over split graphs with an explicit severity policy.
pub fn validate_plan_graphs_with_mode_and_options(
    data: &Graph,
    shapes: &Graph,
    plan: &PhysicalPlan,
    mode: ValidationGraphMode,
    options: &ValidationOptions,
) -> Result<ValidationOutcome, NonStratifiable> {
    match mode {
        ValidationGraphMode::Data => {
            let uses_shapes = uses_shapes_graph(&plan.arena);
            let frozen = if uses_shapes {
                FrozenIndexedDataset::from_graphs(data, shapes)
            } else {
                FrozenIndexedDataset::from_graph(data)
            };
            validate_plan_with_frozen(data, plan, frozen, uses_shapes, options)
        }
        ValidationGraphMode::Union => {
            let uses_shapes = uses_shapes_graph(&plan.arena);
            let frozen = if uses_shapes {
                FrozenIndexedDataset::from_graph_union_with_shapes(data, shapes)
            } else {
                FrozenIndexedDataset::from_graph_union(data, shapes)
            };
            validate_plan_with_frozen(data, plan, frozen, uses_shapes, options)
        }
        ValidationGraphMode::UnionAll => {
            let union = graph_union(data, shapes);
            validate_plan_with_context_and_options(&union, &union, plan, options)
        }
    }
}

/// Validate plan focus nodes from `data` against the supplied execution context.
pub fn validate_plan_with_context(
    data: &Graph,
    context: &Graph,
    plan: &PhysicalPlan,
) -> Result<ValidationOutcome, NonStratifiable> {
    validate_plan_with_context_and_options(data, context, plan, &ValidationOptions::default())
}

/// Validate plan focus nodes against a context with a severity policy.
pub fn validate_plan_with_context_and_options(
    data: &Graph,
    context: &Graph,
    plan: &PhysicalPlan,
    options: &ValidationOptions,
) -> Result<ValidationOutcome, NonStratifiable> {
    let uses_shapes = uses_shapes_graph(&plan.arena);
    let frozen = if uses_shapes {
        FrozenIndexedDataset::from_graphs(context, context)
    } else {
        FrozenIndexedDataset::from_graph(context)
    };
    validate_plan_with_frozen(data, plan, frozen, uses_shapes, options)
}

fn validate_plan_with_frozen(
    data: &Graph,
    plan: &PhysicalPlan,
    frozen: FrozenIndexedDataset,
    has_shapes_graph: bool,
    options: &ValidationOptions,
) -> Result<ValidationOutcome, NonStratifiable> {
    let strat = analyze(&plan.arena);
    if !strat.stratifiable {
        let components = strat
            .strata
            .iter()
            .filter(|s| !s.stratifiable)
            .map(|s| s.shapes.clone())
            .collect();
        return Err(NonStratifiable { components });
    }

    let sparql = SparqlExecutor::from_frozen(frozen, has_shapes_graph);
    let backend = sparql
        .frozen()
        .expect("validation executor always has a frozen dataset");
    let mut evaluator = ShapeEvaluator::new(backend, &plan.arena, &sparql);
    let mut violations = Vec::new();
    for (i, sp) in plan.statements.iter().enumerate() {
        let label = plan
            .names
            .get(&sp.shape)
            .cloned()
            .unwrap_or_else(|| format!("@{}", sp.shape.0));
        let foci = focus_for_source(data, &sp.source, &mut evaluator);
        prefetch_sparql_constraints(&plan.arena, sp.shape, &foci, &sparql);
        for v in foci {
            let t = web_time::Instant::now();
            let mut stack = HashSet::new();
            let mut reasons = explain(
                &mut evaluator,
                &v,
                sp.shape,
                None,
                &Severity::Violation,
                &mut stack,
            );
            crate::profile::record_shape(&label, t.elapsed().as_micros() as u64);
            dedup_reasons(&mut reasons);
            if !reasons.is_empty() {
                let severity = most_severe(&reasons);
                violations.push(Violation {
                    focus: v,
                    statement: i,
                    severity,
                    reasons,
                });
            }
        }
    }
    sort_violations(&mut violations, options.sort_results);
    Ok(ValidationOutcome {
        conforms: conforms_at_threshold(&violations, &options.minimum_severity),
        violations,
    })
}

/// Focus nodes for a compiled [`FocusSource`].
fn focus_for_source(
    data: &Graph,
    source: &FocusSource,
    evaluator: &mut ShapeEvaluator<'_>,
) -> Vec<Term> {
    match source {
        FocusSource::SubjectsOf(p) => subjects_of(data, p),
        FocusSource::ObjectsOf(p) => objects_of(data, p),
        FocusSource::Node(c) => vec![c.clone()],
        // the optimization: seed backward from the constant, no full scan
        FocusSource::PathToConst { path, target } => pred(evaluator.g, target, path)
            .into_iter()
            .filter(|node| graph_contains_term(data, node))
            .collect(),
        FocusSource::ScanFilter { path, qualifier } => all_nodes(data)
            .into_iter()
            .filter(|v| {
                succ(evaluator.g, v, path)
                    .iter()
                    .any(|u| evaluator.holds(u, *qualifier))
            })
            .collect(),
        FocusSource::Sparql(target) => {
            let candidates = all_nodes(data);
            evaluator
                .sparql
                .target_nodes(&target.query)
                .unwrap_or_default()
                .into_iter()
                .filter(|node| candidates.contains(node))
                .collect()
        }
    }
}

/// The focus nodes selected by a selector.
pub fn focus_nodes(data: &Graph, sel: &Selector, arena: &ShapeArena) -> Vec<Term> {
    let sparql =
        SparqlExecutor::new(data).expect("building an in-memory Oxigraph store should succeed");
    let mut evaluator = ShapeEvaluator::new(data, arena, &sparql);
    focus_nodes_with_evaluator(data, sel, &mut evaluator)
}

pub(crate) fn focus_nodes_with(
    data: &Graph,
    backend: &dyn PathBackend,
    sel: &Selector,
    arena: &ShapeArena,
    sparql: &SparqlExecutor,
) -> Vec<Term> {
    let mut evaluator = ShapeEvaluator::new(backend, arena, sparql);
    focus_nodes_with_evaluator(data, sel, &mut evaluator)
}

fn focus_nodes_with_evaluator(
    data: &Graph,
    sel: &Selector,
    evaluator: &mut ShapeEvaluator<'_>,
) -> Vec<Term> {
    match sel {
        Selector::HasOut(q) => subjects_of(data, q),
        Selector::HasIn(q) => objects_of(data, q),
        Selector::IsConst(c) => vec![c.clone()],
        Selector::HasPath(path, qual) => match evaluator.arena.get(*qual) {
            // Class targets are lowered to
            // rdf:type/rdfs:subClassOf* ending at a constant. Searching
            // backward from that constant avoids traversing the hierarchy once
            // for every node in the data graph.
            Shape::TestConst(target) => pred(evaluator.g, target, path)
                .into_iter()
                .filter(|node| graph_contains_term(data, node))
                .collect(),
            _ => all_nodes(data)
                .into_iter()
                .filter(|v| {
                    succ(evaluator.g, v, path)
                        .iter()
                        .any(|u| evaluator.holds(u, *qual))
                })
                .collect(),
        },
        Selector::Sparql(target) => {
            let candidates = all_nodes(data);
            evaluator
                .sparql
                .target_nodes(&target.query)
                .unwrap_or_default()
                .into_iter()
                .filter(|node| candidates.contains(node))
                .collect()
        }
    }
}

fn holds_memoized(
    g: &dyn PathBackend,
    arena: &ShapeArena,
    v: &Term,
    id: ShapeId,
    sparql: &SparqlExecutor,
    state: &mut EvalState,
) -> EvalResult {
    let key = (id, v.clone());
    if let Some(&holds) = state.memo.get(&key) {
        if let Some(telemetry) = state.telemetry.as_mut() {
            telemetry.hits += 1;
        }
        return EvalResult {
            holds,
            cacheable: true,
        };
    }
    if let Some(telemetry) = state.telemetry.as_mut() {
        telemetry.misses += 1;
    }
    if !state.active.insert(key.clone()) {
        if let Some(telemetry) = state.telemetry.as_mut() {
            telemetry.recursion_back_edges += 1;
        }
        return EvalResult {
            holds: true,
            cacheable: false,
        }; // back-edge ⇒ assume conforming: the gfp choice (doc 03)
    }
    let result = match arena.get(id) {
        Shape::Annotated { shape, .. } => holds_memoized(g, arena, v, *shape, sparql, state),
        Shape::Top | Shape::Pending => EvalResult {
            holds: true,
            cacheable: true,
        },
        Shape::Sparql(constraint) => EvalResult {
            holds: sparql
                .constraint_violations(constraint, v)
                .is_ok_and(|violations| violations.is_empty()),
            cacheable: true,
        },
        Shape::TestConst(c) => EvalResult {
            holds: v == c,
            cacheable: true,
        },
        Shape::TestType(t) => EvalResult {
            holds: value_type_holds(t, v),
            cacheable: true,
        },
        Shape::TestKind(k) => EvalResult {
            holds: k.matches(v),
            cacheable: true,
        },
        Shape::Closed(q) => EvalResult {
            holds: closed_offenders(g, v, q).is_empty(),
            cacheable: true,
        },
        Shape::Eq(path, p) => EvalResult {
            holds: succ(g, v, path) == objects(g, v, p),
            cacheable: true,
        },
        Shape::Disj(path, p) => EvalResult {
            holds: succ(g, v, path).is_disjoint(&objects(g, v, p)),
            cacheable: true,
        },
        Shape::Lt(path, p) => EvalResult {
            holds: all_pairs_ordered(g, v, path, p, false),
            cacheable: true,
        },
        Shape::Le(path, p) => EvalResult {
            holds: all_pairs_ordered(g, v, path, p, true),
            cacheable: true,
        },
        Shape::UniqueLang(path) => EvalResult {
            holds: unique_lang(&succ(g, v, path)),
            cacheable: true,
        },
        Shape::Not(c) => {
            let child = holds_memoized(g, arena, v, *c, sparql, state);
            EvalResult {
                holds: !child.holds,
                cacheable: child.cacheable,
            }
        }
        Shape::And(cs) => {
            let mut result = EvalResult {
                holds: true,
                cacheable: true,
            };
            for child in cs {
                let child = holds_memoized(g, arena, v, *child, sparql, state);
                result.cacheable &= child.cacheable;
                if !child.holds {
                    result.holds = false;
                    break;
                }
            }
            result
        }
        Shape::Or(cs) => {
            let mut result = EvalResult {
                holds: false,
                cacheable: true,
            };
            for child in cs {
                let child = holds_memoized(g, arena, v, *child, sparql, state);
                result.cacheable &= child.cacheable;
                if child.holds {
                    result.holds = true;
                    break;
                }
            }
            result
        }
        Shape::Count {
            path,
            min,
            max,
            qualifier,
        } => {
            let mut n = 0;
            let mut cacheable = true;
            for value in succ(g, v, path) {
                let qualified = holds_memoized(g, arena, &value, *qualifier, sparql, state);
                cacheable &= qualified.cacheable;
                n += u64::from(qualified.holds);
            }
            EvalResult {
                holds: min.is_none_or(|m| n >= m) && max.is_none_or(|m| n <= m),
                cacheable,
            }
        }
    };
    state.active.remove(&key);
    if result.cacheable {
        state.memo.insert(key, result.holds);
        if let Some(telemetry) = state.telemetry.as_mut() {
            telemetry.insertions += 1;
        }
    } else if let Some(telemetry) = state.telemetry.as_mut() {
        telemetry.non_cacheable_results += 1;
    }
    result
}

fn estimated_memo_bytes(memo: &HashMap<(ShapeId, Term), bool>) -> usize {
    const CONTROL_BYTE_ESTIMATE: usize = 1;
    let bucket_bytes =
        memo.capacity() * (std::mem::size_of::<((ShapeId, Term), bool)>() + CONTROL_BYTE_ESTIMATE);
    bucket_bytes
        + memo
            .keys()
            .map(|(_, term)| estimated_term_heap_bytes(term))
            .sum::<usize>()
}

fn estimated_term_heap_bytes(term: &Term) -> usize {
    match term {
        Term::NamedNode(node) => node.as_str().len(),
        Term::BlankNode(node) => node.as_str().len(),
        Term::Literal(literal) => {
            literal.value().len()
                + literal.language().map_or_else(
                    || {
                        let datatype = literal.datatype();
                        if datatype.as_str() == "http://www.w3.org/2001/XMLSchema#string" {
                            0
                        } else {
                            datatype.as_str().len()
                        }
                    },
                    str::len,
                )
        }
    }
}

/// Batch-evaluate the SPARQL constraints reachable from `root` at the focus set
/// before the per-node walk, so their fallback queries run once for the whole
/// focus set instead of once per focus (doc §189). Only constraints reached through
/// focus-preserving operators (`∧`/`∨`/`¬`) are evaluated at `foci`; constraints
/// under a `Count` path apply to value nodes, not the statement focus, so they
/// are skipped here and fall back to per-focus execution. Prefetching is a pure
/// memo (constraint violations depend only on focus + immutable dataset), so it
/// is sound regardless of the operator context the constraint is reached in.
fn prefetch_sparql_constraints(
    arena: &ShapeArena,
    root: ShapeId,
    foci: &[Term],
    sparql: &SparqlExecutor,
) {
    if foci.len() < 2 {
        return;
    }
    let mut constraints = Vec::new();
    let mut seen = HashSet::new();
    collect_focus_sparql(arena, root, &mut seen, &mut constraints);
    for constraint in constraints {
        let _ = sparql.prefetch_constraint(constraint, foci);
    }
}

fn collect_focus_sparql<'a>(
    arena: &'a ShapeArena,
    id: ShapeId,
    seen: &mut HashSet<ShapeId>,
    out: &mut Vec<&'a SparqlConstraint>,
) {
    if !seen.insert(id) {
        return; // cyclic (recursive) shape: stop at the back-edge
    }
    match arena.get(id) {
        Shape::Annotated { shape, .. } => collect_focus_sparql(arena, *shape, seen, out),
        Shape::Sparql(constraint) => out.push(constraint),
        Shape::Not(inner) => collect_focus_sparql(arena, *inner, seen, out),
        Shape::And(ids) | Shape::Or(ids) => {
            for &child in ids {
                collect_focus_sparql(arena, child, seen, out);
            }
        }
        // `Count` crosses a path (different focus); all other variants are leaves.
        _ => {}
    }
}

/// The reasons `φ` (slot `id`) fails at `node`. Empty iff it holds. `path_ctx`
/// is the rendered path by which `node` was reached from the enclosing focus.
fn explain(
    evaluator: &mut ShapeEvaluator<'_>,
    node: &Term,
    id: ShapeId,
    path_ctx: Option<&str>,
    severity: &Severity,
    stack: &mut HashSet<(ShapeId, Term)>,
) -> Vec<Reason> {
    let key = (id, node.clone());
    if !stack.insert(key.clone()) {
        return Vec::new(); // back-edge ⇒ assume conforming (gfp, doc 03)
    }
    if evaluator.holds(node, id) {
        stack.remove(&key);
        return Vec::new();
    }
    let reasons = match evaluator.arena.get(id).clone() {
        Shape::Annotated {
            severity: source_severity,
            shape,
        } => explain(evaluator, node, shape, path_ctx, &source_severity, stack),
        Shape::Top | Shape::Pending => Vec::new(),
        Shape::Sparql(constraint) => {
            match evaluator.sparql.constraint_violations(&constraint, node) {
                Ok(violations) => violations
                    .into_iter()
                    .map(|violation| {
                        // Compute the message before the value/path fields are moved
                        // out of `violation`.
                        let message = sparql_violation_message(&violation, &constraint, node);
                        Reason {
                            value: violation.value.unwrap_or_else(|| node.clone()),
                            path: violation
                                .path
                                .map(|path| path.to_string())
                                .or_else(|| path_ctx.map(str::to_string))
                                .or_else(|| constraint.path.as_ref().map(path_to_string)),
                            message,
                            shape: id,
                            severity: severity.clone(),
                            sub_reasons: Vec::new(),
                        }
                    })
                    .collect(),
                Err(error) => vec![Reason {
                    value: node.clone(),
                    path: path_ctx.map(str::to_string),
                    shape: id,
                    severity: severity.clone(),
                    message: format!("SPARQL constraint evaluation failed: {error}"),
                    sub_reasons: Vec::new(),
                }],
            }
        }
        Shape::TestConst(_)
        | Shape::TestType(_)
        | Shape::TestKind(_)
        | Shape::Eq(..)
        | Shape::Disj(..)
        | Shape::Lt(..)
        | Shape::Le(..)
        | Shape::UniqueLang(_) => leaf(
            evaluator.holds(node, id),
            node,
            id,
            path_ctx,
            severity,
            format!("{} not satisfied", shape_to_string(evaluator.arena, id)),
        ),
        Shape::Closed(q) => {
            let bad = closed_offenders(evaluator.g, node, &q);
            if bad.is_empty() {
                Vec::new()
            } else {
                let preds: Vec<String> = bad.iter().map(|p| p.to_string()).collect();
                vec![Reason {
                    value: node.clone(),
                    path: path_ctx.map(str::to_string),
                    shape: id,
                    severity: severity.clone(),
                    message: format!("closed: unexpected predicate(s) {}", preds.join(", ")),
                    sub_reasons: Vec::new(),
                }]
            }
        }
        Shape::Not(c) => {
            if explain(evaluator, node, c, path_ctx, severity, stack).is_empty() {
                vec![Reason {
                    value: node.clone(),
                    path: path_ctx.map(str::to_string),
                    shape: id,
                    severity: severity.clone(),
                    message: "negated shape unexpectedly held".to_string(),
                    sub_reasons: Vec::new(),
                }]
            } else {
                Vec::new()
            }
        }
        Shape::And(cs) => cs
            .iter()
            .flat_map(|c| explain(evaluator, node, *c, path_ctx, severity, stack))
            .collect(),
        Shape::Or(cs) => {
            let mut sub_reasons = Vec::new();
            let mut satisfied = false;
            for c in &cs {
                let sub = explain(evaluator, node, *c, path_ctx, severity, stack);
                if sub.is_empty() {
                    satisfied = true;
                    break;
                }
                sub_reasons.extend(sub);
            }
            if satisfied {
                Vec::new()
            } else {
                vec![Reason {
                    value: node.clone(),
                    path: path_ctx.map(str::to_string),
                    shape: id,
                    severity: severity.clone(),
                    message: format!("none of {} alternative(s) satisfied", cs.len()),
                    sub_reasons,
                }]
            }
        }
        Shape::Count {
            path,
            min,
            max,
            qualifier,
        } => explain_count(
            evaluator, node, id, &path, min, max, qualifier, severity, stack,
        ),
    };
    stack.remove(&key);
    reasons
}

#[allow(clippy::too_many_arguments)]
fn explain_count(
    evaluator: &mut ShapeEvaluator<'_>,
    node: &Term,
    id: ShapeId,
    path: &Path,
    min: Option<u64>,
    max: Option<u64>,
    qualifier: ShapeId,
    severity: &Severity,
    stack: &mut HashSet<(ShapeId, Term)>,
) -> Vec<Reason> {
    let path_str = path_to_string(path);
    let matched: Vec<Term> = succ(evaluator.g, node, path)
        .into_iter()
        .filter(|u| evaluator.holds(u, qualifier))
        .collect();
    let n = matched.len() as u64;
    let mut reasons = Vec::new();

    if let Some(mx) = max
        && n > mx
    {
        match evaluator.arena.get(qualifier).clone() {
            // ∀path.inner encoded as ∃≤0 path.¬inner: drill into the offenders.
            Shape::Not(inner) if mx == 0 => {
                for u in &matched {
                    reasons.extend(explain(
                        evaluator,
                        u,
                        inner,
                        Some(&path_str),
                        severity,
                        stack,
                    ));
                }
            }
            _ => reasons.push(Reason {
                value: node.clone(),
                path: Some(path_str.clone()),
                shape: id,
                severity: severity.clone(),
                message: format!("at most {mx} value(s) may match along {path_str}, found {n}"),
                sub_reasons: Vec::new(),
            }),
        }
    }

    if let Some(mn) = min
        && n < mn
    {
        reasons.push(Reason {
            value: node.clone(),
            path: Some(path_str.clone()),
            shape: id,
            severity: severity.clone(),
            message: format!("at least {mn} value(s) required along {path_str}, found {n}"),
            sub_reasons: Vec::new(),
        });
    }

    reasons
}

/// The message for a `sh:sparql` violation, by SHACL §5.2.1 precedence: the
/// result's own `?message` binding, then the constraint's (or shape's)
/// `sh:message` (with `{$this}`/`{?var}` substitution), then a constructed
/// description naming the shape and value.
fn sparql_violation_message(
    violation: &SparqlViolation,
    constraint: &SparqlConstraint,
    node: &Term,
) -> String {
    if let Some(message) = &violation.message {
        return term_text(message);
    }
    if !constraint.messages.is_empty() {
        return constraint
            .messages
            .iter()
            .map(|m| apply_message_template(&term_text(m), node, &violation.bindings))
            .collect::<Vec<_>>()
            .join("; ");
    }
    let mut message = match &constraint.shape {
        Some(shape) => format!("SPARQL constraint at {shape} not satisfied"),
        None => "SPARQL constraint not satisfied".to_string(),
    };
    if let Some(value) = &violation.value {
        message.push_str(&format!(" (value: {value})"));
    }
    message
}

/// Substitute `{$varName}` / `{?varName}` placeholders in a message template.
///
/// `$this` resolves to `focus`; all other names are looked up in `bindings`
/// (keyed without the `$`/`?` sigil). Unresolved placeholders are left as-is.
pub(crate) fn apply_message_template(
    template: &str,
    focus: &Term,
    bindings: &HashMap<String, Term>,
) -> String {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE
        .get_or_init(|| Regex::new(r"\{(\$[A-Za-z_]\w*|\?[A-Za-z_]\w*)\}").expect("static regex"));
    re.replace_all(template, |caps: &regex::Captures| {
        let placeholder = &caps[1];
        let name = &placeholder[1..]; // strip leading `$` or `?`
        let term = if name == "this" {
            Some(focus)
        } else {
            bindings.get(name)
        };
        term.map(|t| match t {
            Term::NamedNode(n) => format!("<{}>", n.as_str()),
            Term::BlankNode(b) => format!("_:{}", b.as_str()),
            Term::Literal(l) => l.value().to_string(),
        })
        .unwrap_or_else(|| placeholder.to_string())
    })
    .to_string()
}

/// A term's human-facing text: a literal's lexical value, otherwise its RDF
/// rendering (`<iri>` / `_:id`).
fn term_text(term: &Term) -> String {
    match term {
        Term::Literal(literal) => literal.value().to_string(),
        other => other.to_string(),
    }
}

fn leaf(
    ok: bool,
    node: &Term,
    id: ShapeId,
    path_ctx: Option<&str>,
    severity: &Severity,
    message: String,
) -> Vec<Reason> {
    if ok {
        Vec::new()
    } else {
        vec![Reason {
            value: node.clone(),
            path: path_ctx.map(str::to_string),
            shape: id,
            severity: severity.clone(),
            message,
            sub_reasons: Vec::new(),
        }]
    }
}

fn all_pairs_ordered(
    g: &dyn PathBackend,
    v: &Term,
    path: &Path,
    p: &NamedNode,
    allow_eq: bool,
) -> bool {
    let lhs = succ(g, v, path);
    let rhs = objects(g, v, p);
    for a in &lhs {
        for b in &rhs {
            match compare_terms(a, b) {
                Some(Ordering::Less) => {}
                Some(Ordering::Equal) if allow_eq => {}
                _ => return false,
            }
        }
    }
    true
}

fn objects(g: &dyn PathBackend, v: &Term, p: &NamedNode) -> HashSet<Term> {
    succ(g, v, &Path::Pred(p.clone()))
}

/// Predicates on `node` not allowed by a closed shape's set `q`.
fn closed_offenders(
    g: &dyn PathBackend,
    node: &Term,
    q: &BTreeSet<NamedNode>,
) -> BTreeSet<NamedNode> {
    g.out_predicates(node)
        .into_iter()
        .filter(|p| !q.contains(p))
        .collect()
}

fn unique_lang(values: &HashSet<Term>) -> bool {
    let mut seen = HashSet::new();
    for term in values {
        if let Term::Literal(l) = term
            && let Some(lang) = l.language()
            && !seen.insert(lang.to_ascii_lowercase())
        {
            return false;
        }
    }
    true
}

fn dedup_reasons(reasons: &mut Vec<Reason>) {
    let mut seen = HashSet::new();
    reasons.retain(|r| {
        seen.insert((
            r.value.to_string(),
            r.message.clone(),
            r.severity.as_str().to_string(),
        ))
    });
}

fn subject_term(s: oxrdf::NamedOrBlankNodeRef) -> Term {
    crate::path::term_of(s.into_owned())
}

/// Distinct subjects of triples with predicate `p`.
fn subjects_of(data: &Graph, p: &NamedNode) -> Vec<Term> {
    let mut seen = HashSet::new();
    data.triples_for_predicate(p.as_ref())
        .filter_map(|t| {
            let term = subject_term(t.subject);
            seen.insert(term.clone()).then_some(term)
        })
        .collect()
}

/// Distinct objects of triples with predicate `p`.
fn objects_of(data: &Graph, p: &NamedNode) -> Vec<Term> {
    let mut seen = HashSet::new();
    data.triples_for_predicate(p.as_ref())
        .filter_map(|t| {
            let term = t.object.into_owned();
            seen.insert(term.clone()).then_some(term)
        })
        .collect()
}

/// All distinct terms appearing as a subject or object in the graph.
fn all_nodes(g: &Graph) -> HashSet<Term> {
    let mut nodes = HashSet::new();
    for t in g.iter() {
        nodes.insert(subject_term(t.subject));
        nodes.insert(t.object.into_owned());
    }
    nodes
}

/// Whether `term` appears in the graph's node domain.
fn graph_contains_term(g: &Graph, term: &Term) -> bool {
    node_of(term).is_some_and(|node| g.triples_for_subject(&node).next().is_some())
        || g.triples_for_object(term).next().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{NamedNode, Triple};

    fn iri(local: &str) -> NamedNode {
        NamedNode::new(format!("http://ex/{local}")).unwrap()
    }

    fn term(local: &str) -> Term {
        Term::NamedNode(iri(local))
    }

    #[test]
    fn memoizes_shared_value_checks_across_focus_nodes() {
        let p = iri("p");
        let shared = term("shared");
        let mut graph = Graph::new();
        graph.insert(&Triple::new(iri("a"), p.clone(), shared.clone()));
        graph.insert(&Triple::new(iri("b"), p.clone(), shared.clone()));

        let mut arena = ShapeArena::new();
        let qualifier = arena.insert(Shape::TestConst(shared));
        let root = arena.insert(Shape::Count {
            path: Path::Pred(p),
            min: Some(1),
            max: None,
            qualifier,
        });
        let sparql = SparqlExecutor::new(&graph).unwrap();
        crate::profile::enable();
        {
            let mut evaluator = ShapeEvaluator::new(&graph, &arena, &sparql);
            assert!(evaluator.holds(&term("a"), root));
            assert!(evaluator.holds(&term("b"), root));
        }
        let profile = crate::profile::take().unwrap();
        let cache = profile.shape_cache();
        assert_eq!(cache.evaluators, 1);
        assert!(cache.hits >= 1, "shared qualifier should hit the cache");
        assert_eq!(cache.peak_entries, 3);
        assert!(cache.estimated_peak_bytes > 0);
    }

    #[test]
    fn does_not_cache_cycle_dependent_results() {
        // A := B ∧ false; B := A. While evaluating A, the B result is
        // provisionally true through the A back-edge, but the gfp solution is
        // A=false, B=false. Caching that provisional B=true would be unsound.
        let mut arena = ShapeArena::new();
        let a = arena.reserve();
        let b = arena.reserve();
        let bottom = arena.insert(Shape::Or(Vec::new()));
        arena.set(a, Shape::And(vec![b, bottom]));
        arena.set(b, Shape::And(vec![a]));

        let graph = Graph::new();
        let sparql = SparqlExecutor::new(&graph).unwrap();
        let node = term("x");

        crate::profile::enable();
        {
            let mut evaluator = ShapeEvaluator::new(&graph, &arena, &sparql);
            assert!(!evaluator.holds(&node, a));
            assert!(!evaluator.holds(&node, b));
        }
        let profile = crate::profile::take().unwrap();
        let cache = profile.shape_cache();
        assert!(cache.recursion_back_edges > 0);
        assert!(cache.non_cacheable_results > 0);
    }
}
