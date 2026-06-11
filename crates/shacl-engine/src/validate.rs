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
use crate::sparql::{SparqlExecutor, SparqlViolation};
use crate::value::{compare_terms, value_type_holds};
use oxrdf::{Graph, NamedNode, Term};
use regex::Regex;
use serde::{Deserialize, Serialize};
use shacl_algebra::render::{path_to_string, shape_to_string};
use shacl_algebra::{Path, Schema, Selector, Shape, ShapeArena, ShapeId, SparqlConstraint};
use shacl_opt::{FocusSource, PhysicalPlan, analyze};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::OnceLock;

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
    pub reasons: Vec<Reason>,
}

/// The outcome of validating a data graph against a schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationOutcome {
    pub conforms: bool,
    pub violations: Vec<Violation>,
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
    validate_with_context(data, data, schema)
}

/// Validate split data and shapes graphs using the selected graph mode.
pub fn validate_graphs(
    data: &Graph,
    shapes: &Graph,
    schema: &Schema,
) -> Result<ValidationOutcome, NonStratifiable> {
    validate_graphs_with_mode(data, shapes, schema, ValidationGraphMode::default())
}

/// Validate split data and shapes graphs using an explicit graph mode.
pub fn validate_graphs_with_mode(
    data: &Graph,
    shapes: &Graph,
    schema: &Schema,
    mode: ValidationGraphMode,
) -> Result<ValidationOutcome, NonStratifiable> {
    match mode {
        ValidationGraphMode::Data => validate_with_context(data, data, schema),
        ValidationGraphMode::Union => {
            let union = graph_union(data, shapes);
            validate_with_context(data, &union, schema)
        }
        ValidationGraphMode::UnionAll => {
            let union = graph_union(data, shapes);
            validate_with_context(&union, &union, schema)
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

    // Mirror the context into a named shapes graph only when a `sh:sparql`
    // query references `$shapesGraph` (rare), so the common path stays cheap.
    let sparql = if uses_shapes_graph(&schema.arena) {
        SparqlExecutor::new_with_shapes(context, context)
            .expect("building an in-memory Oxigraph store should succeed")
            .with_frozen(FrozenIndexedDataset::from_graphs(context, context))
    } else {
        SparqlExecutor::new(context)
            .expect("building an in-memory Oxigraph store should succeed")
            .with_frozen(FrozenIndexedDataset::from_graph(context))
    };
    // Path / shape evaluation runs over the indexed frozen snapshot built above;
    // `context` (the mutable-free Graph) is only the fallback for callers without
    // a snapshot (none here — `with_frozen` always ran).
    let backend = path_backend(&sparql, context);
    let mut violations = Vec::new();
    for (i, st) in schema.statements.iter().enumerate() {
        for v in focus_nodes_with(data, backend, &st.selector, &schema.arena, &sparql) {
            let mut stack = HashSet::new();
            let mut reasons = explain(
                backend,
                &schema.arena,
                &v,
                st.shape,
                None,
                &mut stack,
                &sparql,
            );
            dedup_reasons(&mut reasons);
            if !reasons.is_empty() {
                violations.push(Violation {
                    focus: v,
                    statement: i,
                    reasons,
                });
            }
        }
    }
    Ok(ValidationOutcome {
        conforms: violations.is_empty(),
        violations,
    })
}

/// Whether any `sh:sparql` constraint references `$shapesGraph`, requiring the
/// shapes graph to be mirrored into a named graph for evaluation.
fn uses_shapes_graph(arena: &ShapeArena) -> bool {
    (0..arena.len()).any(|i| {
        matches!(arena.get(ShapeId(i as u32)), Shape::Sparql(c) if c.query.contains("shapesGraph"))
    })
}

pub(crate) fn graph_union(left: &Graph, right: &Graph) -> Graph {
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
    validate_plan_with_context(data, data, plan)
}

/// Validate a physical plan over split graphs using the default graph mode.
pub fn validate_plan_graphs(
    data: &Graph,
    shapes: &Graph,
    plan: &PhysicalPlan,
) -> Result<ValidationOutcome, NonStratifiable> {
    validate_plan_graphs_with_mode(data, shapes, plan, ValidationGraphMode::default())
}

/// Validate a physical plan over split graphs using an explicit graph mode.
pub fn validate_plan_graphs_with_mode(
    data: &Graph,
    shapes: &Graph,
    plan: &PhysicalPlan,
    mode: ValidationGraphMode,
) -> Result<ValidationOutcome, NonStratifiable> {
    match mode {
        ValidationGraphMode::Data => validate_plan_with_context(data, data, plan),
        ValidationGraphMode::Union => {
            let union = graph_union(data, shapes);
            validate_plan_with_context(data, &union, plan)
        }
        ValidationGraphMode::UnionAll => {
            let union = graph_union(data, shapes);
            validate_plan_with_context(&union, &union, plan)
        }
    }
}

/// Validate plan focus nodes from `data` against the supplied execution context.
pub fn validate_plan_with_context(
    data: &Graph,
    context: &Graph,
    plan: &PhysicalPlan,
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

    let sparql = SparqlExecutor::new(context)
        .expect("building an in-memory Oxigraph store should succeed")
        .with_frozen(FrozenIndexedDataset::from_graph(context));
    let backend = path_backend(&sparql, context);
    let mut violations = Vec::new();
    for (i, sp) in plan.statements.iter().enumerate() {
        for v in focus_for_source(data, backend, &sp.source, &plan.arena, &sparql) {
            let mut stack = HashSet::new();
            let mut reasons = explain(
                backend,
                &plan.arena,
                &v,
                sp.shape,
                None,
                &mut stack,
                &sparql,
            );
            dedup_reasons(&mut reasons);
            if !reasons.is_empty() {
                violations.push(Violation {
                    focus: v,
                    statement: i,
                    reasons,
                });
            }
        }
    }
    Ok(ValidationOutcome {
        conforms: violations.is_empty(),
        violations,
    })
}

/// The path-evaluation backend for a validation run: the indexed frozen snapshot
/// when present (always so on the validation paths), else the context `Graph`
/// (the inference / standalone-target fallback). See [`crate::path::PathBackend`].
fn path_backend<'a>(sparql: &'a SparqlExecutor, context: &'a Graph) -> &'a dyn PathBackend {
    match sparql.frozen() {
        Some(frozen) => frozen,
        None => context,
    }
}

/// Focus nodes for a compiled [`FocusSource`].
fn focus_for_source(
    data: &Graph,
    backend: &dyn PathBackend,
    source: &FocusSource,
    arena: &ShapeArena,
    sparql: &SparqlExecutor,
) -> Vec<Term> {
    match source {
        FocusSource::SubjectsOf(p) => subjects_of(data, p),
        FocusSource::ObjectsOf(p) => objects_of(data, p),
        FocusSource::Node(c) => vec![c.clone()],
        // the optimization: seed backward from the constant, no full scan
        FocusSource::PathToConst { path, target } => pred(backend, target, path)
            .into_iter()
            .filter(|node| graph_contains_term(data, node))
            .collect(),
        FocusSource::ScanFilter { path, qualifier } => all_nodes(data)
            .into_iter()
            .filter(|v| {
                let mut stack = HashSet::new();
                succ(backend, v, path)
                    .iter()
                    .any(|u| holds(backend, arena, u, *qualifier, &mut stack, sparql))
            })
            .collect(),
        FocusSource::Sparql(target) => {
            let candidates = all_nodes(data);
            sparql
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
    focus_nodes_with(data, data, sel, arena, &sparql)
}

pub(crate) fn focus_nodes_with(
    data: &Graph,
    backend: &dyn PathBackend,
    sel: &Selector,
    arena: &ShapeArena,
    sparql: &SparqlExecutor,
) -> Vec<Term> {
    match sel {
        Selector::HasOut(q) => subjects_of(data, q),
        Selector::HasIn(q) => objects_of(data, q),
        Selector::IsConst(c) => vec![c.clone()],
        Selector::HasPath(path, qual) => match arena.get(*qual) {
            // Class targets are lowered to
            // rdf:type/rdfs:subClassOf* ending at a constant. Searching
            // backward from that constant avoids traversing the hierarchy once
            // for every node in the data graph.
            Shape::TestConst(target) => pred(backend, target, path)
                .into_iter()
                .filter(|node| graph_contains_term(data, node))
                .collect(),
            _ => all_nodes(data)
                .into_iter()
                .filter(|v| {
                    let mut stack = HashSet::new();
                    succ(backend, v, path)
                        .iter()
                        .any(|u| holds(backend, arena, u, *qual, &mut stack, sparql))
                })
                .collect(),
        },
        Selector::Sparql(target) => {
            let candidates = all_nodes(data);
            sparql
                .target_nodes(&target.query)
                .unwrap_or_default()
                .into_iter()
                .filter(|node| candidates.contains(node))
                .collect()
        }
    }
}

/// `G, v ⊨ φ` (bare bool). Used for target selection, qualifier counting, and
/// rule conditions (`crate::infer`).
pub(crate) fn holds(
    g: &dyn PathBackend,
    arena: &ShapeArena,
    v: &Term,
    id: ShapeId,
    stack: &mut HashSet<(ShapeId, Term)>,
    sparql: &SparqlExecutor,
) -> bool {
    let key = (id, v.clone());
    if !stack.insert(key.clone()) {
        return true; // back-edge ⇒ assume conforming: the gfp choice (doc 03)
    }
    let result = match arena.get(id) {
        Shape::Top | Shape::Pending => true,
        Shape::Sparql(constraint) => sparql
            .constraint_violations(constraint, v)
            .is_ok_and(|violations| violations.is_empty()),
        Shape::TestConst(c) => v == c,
        Shape::TestType(t) => value_type_holds(t, v),
        Shape::TestKind(k) => k.matches(v),
        Shape::Closed(q) => closed_offenders(g, v, q).is_empty(),
        Shape::Eq(path, p) => succ(g, v, path) == objects(g, v, p),
        Shape::Disj(path, p) => succ(g, v, path).is_disjoint(&objects(g, v, p)),
        Shape::Lt(path, p) => all_pairs_ordered(g, v, path, p, false),
        Shape::Le(path, p) => all_pairs_ordered(g, v, path, p, true),
        Shape::UniqueLang(path) => unique_lang(&succ(g, v, path)),
        Shape::Not(c) => !holds(g, arena, v, *c, stack, sparql),
        Shape::And(cs) => cs.iter().all(|c| holds(g, arena, v, *c, stack, sparql)),
        Shape::Or(cs) => cs.iter().any(|c| holds(g, arena, v, *c, stack, sparql)),
        Shape::Count {
            path,
            min,
            max,
            qualifier,
        } => {
            let n = succ(g, v, path)
                .iter()
                .filter(|u| holds(g, arena, u, *qualifier, stack, sparql))
                .count() as u64;
            min.is_none_or(|m| n >= m) && max.is_none_or(|m| n <= m)
        }
    };
    stack.remove(&key);
    result
}

/// The reasons `φ` (slot `id`) fails at `node`. Empty iff it holds. `path_ctx`
/// is the rendered path by which `node` was reached from the enclosing focus.
fn explain(
    g: &dyn PathBackend,
    arena: &ShapeArena,
    node: &Term,
    id: ShapeId,
    path_ctx: Option<&str>,
    stack: &mut HashSet<(ShapeId, Term)>,
    sparql: &SparqlExecutor,
) -> Vec<Reason> {
    let key = (id, node.clone());
    if !stack.insert(key.clone()) {
        return Vec::new(); // back-edge ⇒ assume conforming (gfp, doc 03)
    }
    let reasons = match arena.get(id) {
        Shape::Top | Shape::Pending => Vec::new(),
        Shape::Sparql(constraint) => match sparql.constraint_violations(constraint, node) {
            Ok(violations) => violations
                .into_iter()
                .map(|violation| {
                    // Compute the message before the value/path fields are moved
                    // out of `violation`.
                    let message = sparql_violation_message(&violation, constraint, node);
                    Reason {
                        value: violation.value.unwrap_or_else(|| node.clone()),
                        path: violation
                            .path
                            .map(|path| path.to_string())
                            .or_else(|| path_ctx.map(str::to_string))
                            .or_else(|| constraint.path.as_ref().map(path_to_string)),
                        message,
                        shape: id,
                        sub_reasons: Vec::new(),
                    }
                })
                .collect(),
            Err(error) => vec![Reason {
                value: node.clone(),
                path: path_ctx.map(str::to_string),
                shape: id,
                message: format!("SPARQL constraint evaluation failed: {error}"),
                sub_reasons: Vec::new(),
            }],
        },
        Shape::TestConst(_)
        | Shape::TestType(_)
        | Shape::TestKind(_)
        | Shape::Eq(..)
        | Shape::Disj(..)
        | Shape::Lt(..)
        | Shape::Le(..)
        | Shape::UniqueLang(_) => {
            let mut s = HashSet::new();
            leaf(
                holds(g, arena, node, id, &mut s, sparql),
                node,
                id,
                path_ctx,
                format!("{} not satisfied", shape_to_string(arena, id)),
            )
        }
        Shape::Closed(q) => {
            let bad = closed_offenders(g, node, q);
            if bad.is_empty() {
                Vec::new()
            } else {
                let preds: Vec<String> = bad.iter().map(|p| p.to_string()).collect();
                vec![Reason {
                    value: node.clone(),
                    path: path_ctx.map(str::to_string),
                    shape: id,
                    message: format!("closed: unexpected predicate(s) {}", preds.join(", ")),
                    sub_reasons: Vec::new(),
                }]
            }
        }
        Shape::Not(c) => {
            if explain(g, arena, node, *c, path_ctx, stack, sparql).is_empty() {
                vec![Reason {
                    value: node.clone(),
                    path: path_ctx.map(str::to_string),
                    shape: id,
                    message: "negated shape unexpectedly held".to_string(),
                    sub_reasons: Vec::new(),
                }]
            } else {
                Vec::new()
            }
        }
        Shape::And(cs) => cs
            .iter()
            .flat_map(|c| explain(g, arena, node, *c, path_ctx, stack, sparql))
            .collect(),
        Shape::Or(cs) => {
            let mut sub_reasons = Vec::new();
            let mut satisfied = false;
            for c in cs {
                let sub = explain(g, arena, node, *c, path_ctx, stack, sparql);
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
            g, arena, node, id, path, *min, *max, *qualifier, stack, sparql,
        ),
    };
    stack.remove(&key);
    reasons
}

#[allow(clippy::too_many_arguments)]
fn explain_count(
    g: &dyn PathBackend,
    arena: &ShapeArena,
    node: &Term,
    id: ShapeId,
    path: &Path,
    min: Option<u64>,
    max: Option<u64>,
    qualifier: ShapeId,
    stack: &mut HashSet<(ShapeId, Term)>,
    sparql: &SparqlExecutor,
) -> Vec<Reason> {
    let path_str = path_to_string(path);
    let matched: Vec<Term> = succ(g, node, path)
        .into_iter()
        .filter(|u| holds(g, arena, u, qualifier, stack, sparql))
        .collect();
    let n = matched.len() as u64;
    let mut reasons = Vec::new();

    if let Some(mx) = max
        && n > mx
    {
        match arena.get(qualifier) {
            // ∀path.inner encoded as ∃≤0 path.¬inner: drill into the offenders.
            Shape::Not(inner) if mx == 0 => {
                for u in &matched {
                    reasons.extend(explain(g, arena, u, *inner, Some(&path_str), stack, sparql));
                }
            }
            _ => reasons.push(Reason {
                value: node.clone(),
                path: Some(path_str.clone()),
                shape: id,
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
    let re = RE.get_or_init(|| {
        Regex::new(r"\{(\$[A-Za-z_]\w*|\?[A-Za-z_]\w*)\}").expect("static regex")
    });
    re.replace_all(template, |caps: &regex::Captures| {
        let placeholder = &caps[1];
        let name = &placeholder[1..]; // strip leading `$` or `?`
        let term = if name == "this" { Some(focus) } else { bindings.get(name) };
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
    message: String,
) -> Vec<Reason> {
    if ok {
        Vec::new()
    } else {
        vec![Reason {
            value: node.clone(),
            path: path_ctx.map(str::to_string),
            shape: id,
            message,
            sub_reasons: Vec::new(),
        }]
    }
}

fn all_pairs_ordered(g: &dyn PathBackend, v: &Term, path: &Path, p: &NamedNode, allow_eq: bool) -> bool {
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
fn closed_offenders(g: &dyn PathBackend, node: &Term, q: &BTreeSet<NamedNode>) -> BTreeSet<NamedNode> {
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
    reasons.retain(|r| seen.insert((r.value.to_string(), r.message.clone())));
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
