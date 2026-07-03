//! W3C `sh:ValidationReport` generation (component-granular, RDF-driven).
//!
//! Producing a spec-faithful report needs provenance the optimized algebra
//! discards: each result carries `sh:sourceConstraintComponent`,
//! `sh:sourceShape`, and `sh:resultPath`, and the granularity is one result per
//! (focus, value node, component) — `sh:and`/`sh:or`/`sh:not`/`sh:node` report
//! as a *unit* (they do not drill into sub-failures), while `sh:property`
//! delegates to the nested shape. So this validator walks the shapes graph
//! directly, reusing only the leaf evaluation primitives (`succ`,
//! `value_type_holds`). It is separate from the algebra path used for fast
//! conformance.
//!
//! Coverage is a growing subset of SHACL Core (see `docs/BACKLOG.md`).

use crate::frozen::FrozenIndexedDataset;
use crate::path::succ;
use crate::sparql::{FunctionDef, SparqlExecutor};
use crate::validate::{
    UnsupportedPolicy, ValidationGraphMode, ValidationOptions, apply_message_template, graph_union,
    is_boolean_true,
};
use crate::value::{compare_terms, value_type_holds};
use oxrdf::{BlankNode, Graph, Literal, NamedNode, NamedNodeRef, NamedOrBlankNode, Term, Triple};
use shifty_algebra::value_type::{Bound, ValueType};
use shifty_algebra::{NodeKindSet, Path, Severity, SparqlConstraint, SparqlQueryKind};
use shifty_parse::graph::{Loaded, term_to_node};
use shifty_parse::lower::canonical_sparql_query;
use shifty_parse::path::parse_path;
use shifty_parse::vocab;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

/// One `sh:ValidationResult`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ValidationResult {
    pub focus: Term,
    /// `sh:resultPath` as the original RDF node (predicate IRI for simple paths).
    pub path: Option<Term>,
    pub value: Option<Term>,
    pub component: NamedNode,
    pub source_shape: Term,
    /// `sh:resultSeverity` — the `sh:severity` declared on the source shape,
    /// defaulting to `sh:Violation`.
    pub severity: NamedNode,
    /// `sh:resultMessage` — copied from `sh:message` on the source shape.
    pub messages: Vec<Term>,
}

#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub conforms: bool,
    pub results: Vec<ValidationResult>,
}

/// The observed binding of one `sh:property` shape at one *conforming* focus
/// node — the inverse of a violation: not what failed, but what a passing
/// property shape's `sh:path` actually resolved to.
///
/// `key` identifies the property shape stably: the (deterministically first,
/// when several) value reached by evaluating `key_path` from the property
/// shape's own node over the shapes graph (e.g. a path to a
/// `zea:roleName "outsideAirTemp"`-style annotation) when `key_path` is given
/// and resolves to at least one value, otherwise the property shape's own
/// source node (so callers can still join on it by IRI/blank-node id).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyWitness {
    pub focus: Term,
    /// The node shape (application profile) `focus` conforms to.
    pub shape: Term,
    pub key: Term,
    /// The `sh:path` value nodes, deduped. When the property shape carries a
    /// `sh:qualifiedValueShape`, this is filtered to the values that satisfy
    /// the qualifier (and, under `sh:qualifiedValueShapesDisjoint`, not any
    /// sibling qualifier) — the disambiguated binding rather than every raw
    /// path value.
    pub values: Vec<Term>,
}

/// Validate `data` against the shapes in `shapes`, producing a W3C report.
pub fn validate_report(shapes: &Loaded, data: &Graph) -> ValidationReport {
    validate_report_with_options(shapes, data, &ValidationOptions::default())
}

/// Evaluate a SPARQL expression (a `dash:expression` function-call string such
/// as `ex:fn("A", "B")`) against the `sh:SPARQLFunction`s declared in `shapes`,
/// returning the result term. Drives `dash:FunctionTestCase`s and exposes SHACL
/// functions as a standalone capability. The document's prefixes and base form
/// the query prologue so prefixed function names resolve.
pub fn evaluate_function_expression(shapes: &Loaded, expr: &str) -> Result<Option<Term>, String> {
    let frozen = FrozenIndexedDataset::from_graph(&shapes.graph);
    let mut sparql = SparqlExecutor::from_frozen(frozen, false);
    // Expression evaluation is the function's own dataset-free path; register
    // every function (Ignore) so pure dash:expressions resolve.
    sparql.set_functions(collect_functions(shapes), UnsupportedPolicy::Ignore);
    let mut prologue = String::new();
    if let Some(base) = &shapes.base {
        prologue.push_str(&format!("BASE <{base}>\n"));
    }
    for (prefix, namespace) in &shapes.prefixes {
        prologue.push_str(&format!("PREFIX {prefix}: <{namespace}>\n"));
    }
    sparql.evaluate_expression(&prologue, expr)
}

/// Validate and build a W3C report using an explicit severity policy.
pub fn validate_report_with_options(
    shapes: &Loaded,
    data: &Graph,
    options: &ValidationOptions,
) -> ValidationReport {
    let has_shapes_graph = shapes_reference_shapes_graph(shapes);
    let frozen = if has_shapes_graph {
        FrozenIndexedDataset::from_graphs(data, &shapes.graph)
    } else {
        FrozenIndexedDataset::from_graph(data)
    };
    validate_report_context(shapes, data, frozen, has_shapes_graph, options)
}

/// Validate split data and shapes graphs using the selected graph mode.
pub fn validate_report_graphs(shapes: &Loaded, data: &Graph) -> ValidationReport {
    validate_report_graphs_with_mode_and_options(
        shapes,
        data,
        ValidationGraphMode::default(),
        &ValidationOptions::default(),
    )
}

/// Validate split data and shapes graphs using an explicit graph mode.
pub fn validate_report_graphs_with_mode(
    shapes: &Loaded,
    data: &Graph,
    mode: ValidationGraphMode,
) -> ValidationReport {
    validate_report_graphs_with_mode_and_options(shapes, data, mode, &ValidationOptions::default())
}

/// Validate split graphs with an explicit graph mode and severity policy.
pub fn validate_report_graphs_with_mode_and_options(
    shapes: &Loaded,
    data: &Graph,
    mode: ValidationGraphMode,
    options: &ValidationOptions,
) -> ValidationReport {
    let has_shapes_graph = shapes_reference_shapes_graph(shapes);
    match mode {
        ValidationGraphMode::Data => {
            let frozen = if has_shapes_graph {
                FrozenIndexedDataset::from_graphs(data, &shapes.graph)
            } else {
                FrozenIndexedDataset::from_graph(data)
            };
            validate_report_context(shapes, data, frozen, has_shapes_graph, options)
        }
        ValidationGraphMode::Union => {
            let frozen = if has_shapes_graph {
                FrozenIndexedDataset::from_graph_union_with_shapes(data, &shapes.graph)
            } else {
                FrozenIndexedDataset::from_graph_union(data, &shapes.graph)
            };
            validate_report_context(shapes, data, frozen, has_shapes_graph, options)
        }
        ValidationGraphMode::UnionAll => {
            let union = graph_union(data, &shapes.graph);
            let frozen = if has_shapes_graph {
                FrozenIndexedDataset::from_graphs(&union, &shapes.graph)
            } else {
                FrozenIndexedDataset::from_graph(&union)
            };
            validate_report_context(shapes, &union, frozen, has_shapes_graph, options)
        }
    }
}

/// Collect [`PropertyWitness`]es for every `sh:property` shape (reached
/// through `sh:property`, `sh:and`, and `sh:node` from a target/profile node
/// shape) at every focus node that *conforms* to that node shape — the
/// inverse of [`validate_report_graphs_with_mode`]. `key_path`, when given, is
/// evaluated from each property shape's own node *over the shapes graph* to
/// produce a stable `PropertyWitness::key`; property shapes where it resolves
/// to no value fall back to their own source node as the key.
pub fn property_witnesses_graphs_with_mode(
    shapes: &Loaded,
    data: &Graph,
    mode: ValidationGraphMode,
    key_path: Option<&Path>,
) -> Vec<PropertyWitness> {
    property_witnesses_graphs_with_mode_and_options(
        shapes,
        data,
        mode,
        key_path,
        &ValidationOptions::default(),
    )
}

/// [`property_witnesses_graphs_with_mode`] with an explicit severity policy,
/// so conformance agrees exactly with [`validate_report_graphs_with_mode_and_options`]
/// under the same options.
pub fn property_witnesses_graphs_with_mode_and_options(
    shapes: &Loaded,
    data: &Graph,
    mode: ValidationGraphMode,
    key_path: Option<&Path>,
    options: &ValidationOptions,
) -> Vec<PropertyWitness> {
    let has_shapes_graph = shapes_reference_shapes_graph(shapes);
    let (focus_data, frozen, union_owner);
    match mode {
        ValidationGraphMode::Data => {
            frozen = if has_shapes_graph {
                FrozenIndexedDataset::from_graphs(data, &shapes.graph)
            } else {
                FrozenIndexedDataset::from_graph(data)
            };
            focus_data = data;
        }
        ValidationGraphMode::Union => {
            frozen = if has_shapes_graph {
                FrozenIndexedDataset::from_graph_union_with_shapes(data, &shapes.graph)
            } else {
                FrozenIndexedDataset::from_graph_union(data, &shapes.graph)
            };
            focus_data = data;
        }
        ValidationGraphMode::UnionAll => {
            union_owner = graph_union(data, &shapes.graph);
            frozen = if has_shapes_graph {
                FrozenIndexedDataset::from_graphs(&union_owner, &shapes.graph)
            } else {
                FrozenIndexedDataset::from_graph(&union_owner)
            };
            focus_data = &union_owner;
        }
    }
    let r = build_reporter(shapes, focus_data, frozen, has_shapes_graph, options);
    let mut out = Vec::new();
    for shape in r.target_shapes() {
        let foci = r.focus_nodes(&shape);
        r.prefetch_sparql(&shape, &foci);
        for focus in &foci {
            let mut check = HashSet::new();
            let mut results = Vec::new();
            r.collect(&shape, focus, &mut results, &mut check, &[]);
            let conforms = !results.iter().any(|result| {
                Severity::from_named_node(result.severity.clone()).meets(&options.minimum_severity)
            });
            if !conforms {
                continue;
            }
            let mut visited = HashSet::new();
            r.collect_property_witnesses(&shape, focus, &shape, key_path, &mut visited, &mut out);
        }
    }
    out
}

/// Build the shared [`Reporter`] setup (SPARQL executor, class index, custom
/// components) used by both violation reporting and property witnessing, so
/// the two traversals stay in lockstep on what counts as "the shape holds".
fn build_reporter<'a>(
    shapes: &'a Loaded,
    focus_data: &'a Graph,
    frozen: FrozenIndexedDataset,
    has_shapes_graph: bool,
    options: &ValidationOptions,
) -> Reporter<'a> {
    // Only execute SPARQL target/constraint work when the shapes graph contains
    // those features. Query execution shares the frozen validation dataset.
    let needs_sparql = shapes
        .graph
        .triples_for_predicate(vocab::SH_SPARQL)
        .next()
        .is_some()
        || shapes
            .graph
            .triples_for_predicate(vocab::SH_TARGET)
            .next()
            .is_some();
    let mut sparql = SparqlExecutor::from_frozen(frozen, needs_sparql && has_shapes_graph);
    sparql.set_functions(collect_functions(shapes), options.engine.unsupported);
    // Index class membership once (instead of a forward scan over every node per
    // class-target shape): this is the report path's analogue of the plan's
    // backward `PathToConst` focus source, amortized across all shapes.
    let has_explicit_class_target = shapes
        .graph
        .triples_for_predicate(vocab::SH_TARGET_CLASS)
        .next()
        .is_some();
    let has_implicit_class_target = shapes.graph.iter().any(|triple| {
        let subject = triple.subject.into_owned();
        is_shape_node(shapes, &subject)
            && (shapes.is_instance_of(&subject, vocab::RDFS_CLASS)
                || shapes.is_instance_of(&subject, vocab::OWL_CLASS))
    });
    let needs_class_index = has_explicit_class_target || has_implicit_class_target;
    let class_index = if needs_class_index {
        build_class_index(
            focus_data,
            sparql
                .frozen()
                .expect("report validation always has a frozen dataset"),
        )
    } else {
        HashMap::new()
    };
    Reporter {
        shapes,
        focus_data,
        sparql,
        needs_sparql,
        class_index,
        path_cache: RefCell::new(HashMap::new()),
        components: build_components(shapes, options.engine.unsupported),
    }
}

fn validate_report_context(
    shapes: &Loaded,
    focus_data: &Graph,
    frozen: FrozenIndexedDataset,
    has_shapes_graph: bool,
    options: &ValidationOptions,
) -> ValidationReport {
    let r = build_reporter(shapes, focus_data, frozen, has_shapes_graph, options);
    let mut results = Vec::new();
    for shape in r.target_shapes() {
        let foci = r.focus_nodes(&shape);
        r.prefetch_sparql(&shape, &foci);
        for focus in &foci {
            let mut visited = HashSet::new();
            r.collect(&shape, focus, &mut results, &mut visited, &[]);
        }
    }
    if options.sort_results {
        results.sort_by(|left, right| {
            Severity::from_named_node(right.severity.clone())
                .rank()
                .cmp(&Severity::from_named_node(left.severity.clone()).rank())
                .then_with(|| left.focus.to_string().cmp(&right.focus.to_string()))
                .then_with(|| {
                    left.source_shape
                        .to_string()
                        .cmp(&right.source_shape.to_string())
                })
                .then_with(|| left.component.as_str().cmp(right.component.as_str()))
        });
    }
    ValidationReport {
        conforms: !results.iter().any(|result| {
            Severity::from_named_node(result.severity.clone()).meets(&options.minimum_severity)
        }),
        results,
    }
}

/// Serialize a report as an RDF `sh:ValidationReport` graph (W3C shape).
pub fn report_to_graph(report: &ValidationReport) -> Graph {
    let mut g = Graph::new();
    let root = BlankNode::default();
    let t = |s: NamedOrBlankNode, p: NamedNodeRef, o: Term| Triple::new(s, p.into_owned(), o);

    g.insert(&t(
        root.clone().into(),
        vocab::RDF_TYPE,
        vocab::SH_VALIDATION_REPORT.into_owned().into(),
    ));
    g.insert(&t(
        root.clone().into(),
        vocab::SH_CONFORMS,
        Literal::from(report.conforms).into(),
    ));

    for r in &report.results {
        let rn = BlankNode::default();
        g.insert(&t(root.clone().into(), vocab::SH_RESULT, rn.clone().into()));
        g.insert(&t(
            rn.clone().into(),
            vocab::RDF_TYPE,
            vocab::SH_VALIDATION_RESULT.into_owned().into(),
        ));
        g.insert(&t(rn.clone().into(), vocab::SH_FOCUS_NODE, r.focus.clone()));
        if let Some(path) = &r.path {
            g.insert(&t(rn.clone().into(), vocab::SH_RESULT_PATH, path.clone()));
        }
        if let Some(value) = &r.value {
            g.insert(&t(rn.clone().into(), vocab::SH_VALUE, value.clone()));
        }
        g.insert(&t(
            rn.clone().into(),
            vocab::SH_RESULT_SEVERITY,
            r.severity.clone().into(),
        ));
        g.insert(&t(
            rn.clone().into(),
            vocab::SH_SOURCE_CONSTRAINT_COMPONENT,
            r.component.clone().into(),
        ));
        for msg in &r.messages {
            g.insert(&t(rn.clone().into(), vocab::SH_RESULT_MESSAGE, msg.clone()));
        }
        g.insert(&t(
            rn.into(),
            vocab::SH_SOURCE_SHAPE,
            r.source_shape.clone(),
        ));
    }
    g
}

/// Substitute `{$varName}` / `{?varName}` placeholders in `sh:message` literals.
///
/// `$this` is resolved from `focus`; all other names are looked up in
/// `bindings` (keyed without the `$`/`?` sigil). Unresolved placeholders are
/// left as-is. Only `sh:Literal` messages are processed; IRI/blank-node
/// message terms pass through unchanged.
fn substitute_messages(
    messages: &[Term],
    focus: &Term,
    bindings: &HashMap<String, Term>,
) -> Vec<Term> {
    messages
        .iter()
        .map(|msg| {
            let Term::Literal(lit) = msg else {
                return msg.clone();
            };
            let text = lit.value();
            let substituted = apply_message_template(text, focus, bindings);
            if substituted == text {
                msg.clone()
            } else {
                Term::Literal(Literal::new_simple_literal(&substituted))
            }
        })
        .collect()
}

/// A SPARQL-based custom constraint component (SHACL §6.2–6.3): a named
/// component IRI, its parameters, and the validators that apply to node shapes,
/// property shapes, or both.
struct CustomComponent {
    /// The component IRI, reported as `sh:sourceConstraintComponent`.
    iri: NamedNode,
    params: Vec<ComponentParam>,
    /// `sh:nodeValidator` — used when the component is applied to a node shape.
    node_validator: Option<ComponentValidator>,
    /// `sh:propertyValidator` — used when applied to a property shape.
    property_validator: Option<ComponentValidator>,
    /// `sh:validator` — an ASK validator usable for either shape kind.
    generic_validator: Option<ComponentValidator>,
}

struct ComponentParam {
    /// The parameter's `sh:path` predicate; the shape supplies its value here.
    path: NamedNode,
    /// The pre-bound SPARQL variable name (the local name of `path`).
    var: String,
    optional: bool,
}

struct ComponentValidator {
    kind: SparqlQueryKind,
    /// Prefix-expanded query text (`sh:ask` / `sh:select`).
    query: String,
    messages: Vec<Term>,
}

fn resolve_validator(
    shapes: &Loaded,
    node: Term,
    component_iri: &NamedNode,
    policy: UnsupportedPolicy,
) -> Option<ComponentValidator> {
    match parse_validator(shapes, &node) {
        Ok(v) => Some(v),
        Err(e) => {
            assert!(
                policy != UnsupportedPolicy::Error,
                "invalid SPARQL in custom constraint component <{component_iri}>: {e}"
            );
            None
        }
    }
}

/// Discover every SPARQL-based custom constraint component in the shapes graph:
/// a named subject carrying `sh:parameter`(s) and at least one validator. (A
/// `sh:SPARQLFunction` also has `sh:parameter` but no validator, so it is
/// excluded.)
///
/// Under [`UnsupportedPolicy::Error`], a component whose validator query is
/// invalid SPARQL causes a panic with a diagnostic message so the problem
/// surfaces immediately rather than producing a silent wrong answer.
/// Under [`UnsupportedPolicy::Ignore`], such components are silently skipped
/// (the constraint is not enforced, which is the historical default behaviour).
fn build_components(shapes: &Loaded, policy: UnsupportedPolicy) -> Vec<CustomComponent> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for triple in shapes.graph.triples_for_predicate(vocab::SH_PARAMETER) {
        let subject = triple.subject.into_owned();
        if !seen.insert(subject.clone()) {
            continue;
        }
        let NamedOrBlankNode::NamedNode(iri) = &subject else {
            continue; // a component must be named to be a sourceConstraintComponent
        };
        if vocab::NATIVE_CONSTRAINT_COMPONENTS.contains(&iri.as_ref()) {
            continue; // SHACL Core component; already implemented natively
        }

        let node_validator = shapes
            .object(&subject, vocab::SH_NODE_VALIDATOR)
            .and_then(|v| resolve_validator(shapes, v, iri, policy));
        let property_validator = shapes
            .object(&subject, vocab::SH_PROPERTY_VALIDATOR)
            .and_then(|v| resolve_validator(shapes, v, iri, policy));
        let generic_validator = shapes
            .object(&subject, vocab::SH_VALIDATOR)
            .and_then(|v| resolve_validator(shapes, v, iri, policy));
        if node_validator.is_none() && property_validator.is_none() && generic_validator.is_none() {
            continue; // not a constraint component (e.g. a sh:SPARQLFunction)
        }
        let mut params = Vec::new();
        for p in shapes.objects(&subject, vocab::SH_PARAMETER) {
            let Some(pn) = term_to_node(&p) else { continue };
            let Some(Term::NamedNode(path)) = shapes.object(&pn, vocab::SH_PATH) else {
                continue;
            };
            let var = local_name(path.as_str()).to_string();
            let optional = matches!(
                shapes.object(&pn, vocab::SH_OPTIONAL),
                Some(Term::Literal(ref l)) if l.value() == "true"
            );
            params.push(ComponentParam {
                path,
                var,
                optional,
            });
        }
        if params.is_empty() {
            continue;
        }
        out.push(CustomComponent {
            iri: iri.clone(),
            params,
            node_validator,
            property_validator,
            generic_validator,
        });
    }
    out.sort_by(|a, b| a.iri.as_str().cmp(b.iri.as_str()));
    out
}

/// Parse a validator node (`sh:SPARQLAskValidator` / `sh:SPARQLSelectValidator`
/// or a subclass), resolving its `sh:prefixes` into a canonical query string.
/// Returns `Err` (with the parse error message) when the query is invalid SPARQL.
fn parse_validator(shapes: &Loaded, node: &Term) -> Result<ComponentValidator, String> {
    let node = term_to_node(node)
        .ok_or_else(|| "validator node is not an IRI or blank node".to_string())?;
    let (kind, raw) = if let Some(Term::Literal(q)) = shapes.object(&node, vocab::SH_ASK) {
        (SparqlQueryKind::Ask, q.value().to_string())
    } else if let Some(Term::Literal(q)) = shapes.object(&node, vocab::SH_SELECT) {
        (SparqlQueryKind::Select, q.value().to_string())
    } else {
        return Err("validator has neither sh:ask nor sh:select".to_string());
    };
    let (_, query) = canonical_sparql_query(shapes, &node, &raw)
        .map_err(|e| format!("invalid SPARQL in {node}: {e}"))?;
    let messages = shapes.objects(&node, vocab::SH_MESSAGE);
    Ok(ComponentValidator {
        kind,
        query,
        messages,
    })
}

/// The local name of an IRI (after the last `#` or `/`) — the SHACL rule for a
/// parameter's pre-bound variable name (SHACL §6.2.1).
fn local_name(iri: &str) -> &str {
    iri.rsplit(['#', '/']).next().unwrap_or(iri)
}

/// Discover `sh:SPARQLFunction`s in the shapes graph (SHACL-AF §5) and build
/// their registrable [`FunctionDef`]s: the function IRI, its parameter variable
/// names in positional order (`sh:order`, then local name), and its prefix-
/// expanded `sh:select`/`sh:ask` body.
pub(crate) fn collect_functions(shapes: &Loaded) -> Vec<FunctionDef> {
    let mut out = Vec::new();
    for func in shapes
        .graph
        .subjects_for_predicate_object(vocab::RDF_TYPE, vocab::SH_SPARQL_FUNCTION)
        .map(|s| s.into_owned())
        .collect::<Vec<_>>()
    {
        let NamedOrBlankNode::NamedNode(iri) = &func else {
            continue;
        };
        let raw = match shapes
            .object(&func, vocab::SH_SELECT)
            .or_else(|| shapes.object(&func, vocab::SH_ASK))
        {
            Some(Term::Literal(q)) => q.value().to_string(),
            _ => continue,
        };
        let Ok((_, query)) = canonical_sparql_query(shapes, &func, &raw) else {
            continue;
        };
        out.push(FunctionDef {
            iri: iri.clone(),
            params: function_param_names(shapes, &func),
            reads_graph: crate::sparql::query_reads_graph(&query),
            query,
        });
    }
    out
}

/// Parameter variable names of a function, ordered by `sh:order` then by the
/// local name of `sh:path` (matching the node-expression evaluator).
fn function_param_names(shapes: &Loaded, func: &NamedOrBlankNode) -> Vec<String> {
    let mut params: Vec<(i64, String)> = shapes
        .objects(func, vocab::SH_PARAMETER)
        .iter()
        .filter_map(|p| {
            let pn = term_to_node(p)?;
            let order = match shapes.object(&pn, vocab::SH_ORDER) {
                Some(Term::Literal(l)) => l.value().parse::<i64>().unwrap_or(0),
                _ => 0,
            };
            let name = match shapes.object(&pn, vocab::SH_NAME) {
                Some(Term::Literal(l)) => l.value().to_string(),
                _ => match shapes.object(&pn, vocab::SH_PATH) {
                    Some(Term::NamedNode(n)) => local_name(n.as_str()).to_string(),
                    _ => return None,
                },
            };
            Some((order, name))
        })
        .collect();
    params.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    params.into_iter().map(|(_, name)| name).collect()
}

struct Reporter<'a> {
    shapes: &'a Loaded,
    focus_data: &'a Graph,
    sparql: SparqlExecutor,
    needs_sparql: bool,
    /// `class → focus-data instances` under `rdf:type / rdfs:subClassOf*`, built
    /// once and shared by every `sh:targetClass` / implicit-class lookup.
    class_index: HashMap<Term, Vec<Term>>,
    /// Parsed `sh:path` per shape node, so `collect` does not re-parse the path
    /// RDF on every (shape, focus) visit. `None` = shape has no/invalid path.
    path_cache: RefCell<HashMap<NamedOrBlankNode, PathCacheEntry>>,
    /// SPARQL-based custom constraint components declared in the shapes graph
    /// (empty for the common case of no custom components).
    components: Vec<CustomComponent>,
}

type Visited = HashSet<(NamedOrBlankNode, Term)>;

/// Cached parsed path and its term representation for sh:path expressions
type PathCacheEntry = (Option<Term>, Option<Path>);

impl Reporter<'_> {
    fn frozen(&self) -> &FrozenIndexedDataset {
        self.sparql
            .frozen()
            .expect("report validation always has a frozen dataset")
    }

    fn target_shapes(&self) -> Vec<NamedOrBlankNode> {
        let mut found: HashSet<NamedOrBlankNode> = HashSet::new();
        for t in self.shapes.graph.iter() {
            let p = t.predicate;
            if p == vocab::SH_TARGET_NODE
                || p == vocab::SH_TARGET_CLASS
                || p == vocab::SH_TARGET_SUBJECTS_OF
                || p == vocab::SH_TARGET_OBJECTS_OF
            {
                found.insert(t.subject.into_owned());
            }
            // SPARQL-based target: sh:target [ sh:select "…" ]
            if p == vocab::SH_TARGET
                && let Some(target) = term_to_node(&t.object.into_owned())
                && self.shapes.object(&target, vocab::SH_SELECT).is_some()
            {
                found.insert(t.subject.into_owned());
            }
            // implicit class target: a shape that is also an rdfs:Class / owl:Class
            if p == vocab::RDF_TYPE {
                let s = t.subject.into_owned();
                if self.is_class(&s) && self.is_shape(&s) {
                    found.insert(s);
                }
            }
        }
        let mut v: Vec<_> = found.into_iter().collect();
        v.sort_by_key(|n| n.to_string());
        v
    }

    /// Does this node look like a SHACL shape (so its class-ness implies a target)?
    fn is_shape(&self, n: &NamedOrBlankNode) -> bool {
        is_shape_node(self.shapes, n)
    }

    fn is_class(&self, n: &NamedOrBlankNode) -> bool {
        self.shapes.is_instance_of(n, vocab::RDFS_CLASS)
            || self.shapes.is_instance_of(n, vocab::OWL_CLASS)
    }

    fn deactivated(&self, n: &NamedOrBlankNode) -> bool {
        matches!(self.shapes.object(n, vocab::SH_DEACTIVATED),
            Some(Term::Literal(ref l)) if l.value() == "true")
    }

    fn focus_nodes(&self, shape: &NamedOrBlankNode) -> Vec<Term> {
        let mut nodes = Vec::new();
        nodes.extend(self.shapes.objects(shape, vocab::SH_TARGET_NODE));
        for c in self.shapes.objects(shape, vocab::SH_TARGET_CLASS) {
            if let Some(instances) = self.class_index.get(&c) {
                nodes.extend(instances.iter().cloned());
            }
        }
        for p in self.shapes.objects(shape, vocab::SH_TARGET_SUBJECTS_OF) {
            if let Term::NamedNode(n) = p {
                nodes.extend(
                    self.focus_data
                        .triples_for_predicate(n.as_ref())
                        .map(|t| node_term(t.subject)),
                );
            }
        }
        for p in self.shapes.objects(shape, vocab::SH_TARGET_OBJECTS_OF) {
            if let Term::NamedNode(n) = p {
                nodes.extend(
                    self.focus_data
                        .triples_for_predicate(n.as_ref())
                        .map(|t| t.object.into_owned()),
                );
            }
        }
        // SPARQL-based targets: sh:target [ sh:select "…" ]. The query selects
        // `?this` focus nodes from the context store.
        if self.needs_sparql {
            let exec = &self.sparql;
            for target in self.shapes.objects(shape, vocab::SH_TARGET) {
                let Some(target_node) = term_to_node(&target) else {
                    continue;
                };
                let Some(Term::Literal(query)) = self.shapes.object(&target_node, vocab::SH_SELECT)
                else {
                    continue;
                };
                // Drop targets that fail to canonicalize, matching the lowering path.
                let Ok((_, canonical)) =
                    canonical_sparql_query(self.shapes, &target_node, query.value())
                else {
                    continue;
                };
                if let Ok(found) = exec.target_nodes(&canonical) {
                    nodes.extend(found);
                }
            }
        }
        // implicit class target: instances of the shape (which is also a class)
        if let NamedOrBlankNode::NamedNode(n) = shape
            && self.is_class(shape)
        {
            let class = Term::NamedNode(n.clone());
            if let Some(instances) = self.class_index.get(&class) {
                nodes.extend(instances.iter().cloned());
            }
        }
        let mut seen = HashSet::new();
        nodes.retain(|t| seen.insert(t.clone()));
        nodes
    }

    /// The shape's `sh:path` as both its raw RDF node (for `sh:resultPath`) and
    /// the parsed path algebra, memoized so repeated visits don't re-parse it.
    fn shape_path(&self, shape: &NamedOrBlankNode) -> (Option<Term>, Option<Path>) {
        if let Some(cached) = self.path_cache.borrow().get(shape) {
            return cached.clone();
        }
        let path_term = self.shapes.object(shape, vocab::SH_PATH);
        let parsed = path_term
            .as_ref()
            .and_then(|t| parse_path(self.shapes, t).ok());
        let entry = (path_term, parsed);
        self.path_cache
            .borrow_mut()
            .insert(shape.clone(), entry.clone());
        entry
    }

    /// `shape`'s own `sh:message`, falling back to `inherited` — the nearest
    /// enclosing shape's `sh:message` — when `shape` declares none. Mirrors the
    /// algebra path's "nearest-enclosing shape" resolution (see `explain` in
    /// `lib.rs`) so a message authored on an outer node shape still surfaces on
    /// violations from an unlabeled nested property shape.
    fn messages_or_inherited(&self, shape: &NamedOrBlankNode, inherited: &[Term]) -> Vec<Term> {
        let own = self.messages(shape);
        if own.is_empty() {
            inherited.to_vec()
        } else {
            own
        }
    }

    /// Collect the results of validating `focus` against `shape`.
    ///
    /// `inherited` is the nearest enclosing shape's `sh:message` (empty at the
    /// top-level target shapes), used when `shape` itself has none.
    fn collect(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        out: &mut Vec<ValidationResult>,
        visited: &mut Visited,
        inherited: &[Term],
    ) {
        if self.deactivated(shape) {
            return; // deactivated shapes produce no results
        }
        let key = (shape.clone(), focus.clone());
        if !visited.insert(key.clone()) {
            return; // recursion: conform on the back-edge (gfp)
        }

        let (path_term, parsed) = self.shape_path(shape);
        let value_nodes: Vec<Term> = match &parsed {
            Some(p) => succ(self.frozen(), focus, p).into_iter().collect(),
            None => vec![focus.clone()],
        };
        let severity = self.severity(shape);
        let messages = self.messages_or_inherited(shape, inherited);
        let push = |out: &mut Vec<ValidationResult>, value, component| {
            out.push(ValidationResult {
                focus: focus.clone(),
                path: path_term.clone(),
                value,
                component,
                source_shape: node_term_ref(shape),
                severity: severity.clone(),
                messages: messages.clone(),
            });
        };

        // cardinality (only meaningful with a path)
        if parsed.is_some() {
            if let Some(min) = self.int(shape, vocab::SH_MIN_COUNT)
                && (value_nodes.len() as u64) < min
            {
                push(out, None, vocab::SH_CC_MIN_COUNT.into_owned());
            }
            if let Some(max) = self.int(shape, vocab::SH_MAX_COUNT)
                && (value_nodes.len() as u64) > max
            {
                push(out, None, vocab::SH_CC_MAX_COUNT.into_owned());
            }
        }

        // sh:hasValue — one of the value nodes must equal the constant
        for hv in self.shapes.objects(shape, vocab::SH_HAS_VALUE) {
            if !value_nodes.contains(&hv) {
                push(out, None, vocab::SH_CC_HAS_VALUE.into_owned());
            }
        }

        self.collect_closed(shape, focus, &value_nodes, out, &messages);
        self.collect_property_pairs(shape, focus, &path_term, &value_nodes, out, &messages);
        self.collect_unique_lang(shape, focus, &path_term, &value_nodes, out, &messages);
        self.collect_qualified_counts(
            shape,
            focus,
            &path_term,
            &value_nodes,
            out,
            visited,
            &messages,
        );

        // value-scoped components
        for u in &value_nodes {
            for (component, ok) in self.value_checks(shape, u, visited) {
                if !ok {
                    push(out, Some(u.clone()), component);
                }
            }
        }

        // nested property shapes: delegate (each value node is a focus for P),
        // passing this shape's resolved message down as the inherited fallback.
        for prop in self.shapes.objects(shape, vocab::SH_PROPERTY) {
            if let Some(pn) = term_to_node(&prop) {
                for u in &value_nodes {
                    self.collect(&pn, u, out, visited, &messages);
                }
            }
        }

        self.collect_sparql(shape, focus, &path_term, &parsed, out, &messages);
        self.collect_expression(shape, focus, out, visited, &messages);
        self.collect_components(
            shape,
            focus,
            &path_term,
            &parsed,
            &value_nodes,
            out,
            &messages,
        );

        visited.remove(&key);
    }

    /// Walk from a (conforming) target shape down through `sh:property`,
    /// `sh:and`, and `sh:node` — the shape forms that keep `focus` as the
    /// same node — collecting one [`PropertyWitness`] per `sh:property` shape
    /// reached. `sh:or`/`sh:xone`/`sh:not` are not descended: which branch
    /// applies is not a fixed set of roles, so there is no single binding to
    /// report.
    fn collect_property_witnesses(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        profile: &NamedOrBlankNode,
        key_path: Option<&Path>,
        visited: &mut Visited,
        out: &mut Vec<PropertyWitness>,
    ) {
        if self.deactivated(shape) {
            return;
        }
        let key = (shape.clone(), focus.clone());
        if !visited.insert(key.clone()) {
            return;
        }

        for prop in self.shapes.objects(shape, vocab::SH_PROPERTY) {
            if let Some(pn) = term_to_node(&prop) {
                self.collect_property_binding(&pn, focus, profile, key_path, visited, out);
            }
        }
        for list in self.shapes.objects(shape, vocab::SH_AND) {
            for member in self.shapes.read_list(&list) {
                if let Some(mn) = term_to_node(&member) {
                    self.collect_property_witnesses(&mn, focus, profile, key_path, visited, out);
                }
            }
        }
        for n in self.shapes.objects(shape, vocab::SH_NODE) {
            if let Some(nn) = term_to_node(&n) {
                self.collect_property_witnesses(&nn, focus, profile, key_path, visited, out);
            }
        }

        visited.remove(&key);
    }

    /// The single [`PropertyWitness`] for one `sh:property` shape at `focus`:
    /// its `sh:path` value nodes, narrowed to the `sh:qualifiedValueShape`
    /// matches when the property shape declares one (mirroring the *counted*
    /// set in [`Reporter::collect_qualified_counts`], but keeping the values
    /// themselves rather than just their count). Property shapes without a
    /// `sh:path` are not addressable and are skipped.
    fn collect_property_binding(
        &self,
        pn: &NamedOrBlankNode,
        focus: &Term,
        profile: &NamedOrBlankNode,
        key_path: Option<&Path>,
        visited: &mut Visited,
        out: &mut Vec<PropertyWitness>,
    ) {
        if self.deactivated(pn) {
            return;
        }
        let (_, parsed_path) = self.shape_path(pn);
        let Some(path) = parsed_path else { return };

        // `key_path` is evaluated over the *shapes* graph, from the property
        // shape's own node — a different graph and starting point than the
        // `sh:path` evaluation below (which reads the data, from `focus`).
        // When it resolves to several values, the first in string order is
        // used, for a deterministic result independent of set iteration order.
        let key = key_path
            .and_then(|kp| {
                let mut matches: Vec<Term> = succ(&self.shapes.graph, &node_term_ref(pn), kp)
                    .into_iter()
                    .collect();
                matches.sort_by_key(ToString::to_string);
                matches.into_iter().next()
            })
            .unwrap_or_else(|| node_term_ref(pn));

        let value_nodes: Vec<Term> = succ(self.frozen(), focus, &path).into_iter().collect();
        let values = match self.shapes.object(pn, vocab::SH_QUALIFIED_VALUE_SHAPE) {
            Some(qualifier_term) => {
                let Some(qualifier) = term_to_node(&qualifier_term) else {
                    return;
                };
                let siblings = if self.bool(pn, vocab::SH_QUALIFIED_VALUE_SHAPES_DISJOINT) {
                    self.sibling_qualified_shapes(pn, &qualifier)
                } else {
                    Vec::new()
                };
                value_nodes
                    .into_iter()
                    .filter(|v| {
                        self.conforms(&qualifier, v, visited)
                            && siblings
                                .iter()
                                .all(|sibling| !self.conforms(sibling, v, visited))
                    })
                    .collect()
            }
            None => value_nodes,
        };

        out.push(PropertyWitness {
            focus: focus.clone(),
            shape: node_term_ref(profile),
            key,
            values,
        });
    }

    /// SPARQL-based custom constraint components (SHACL §6.3). A component is
    /// *activated* for `shape` iff the shape supplies a value for each of its
    /// mandatory parameters; those values (plus `$this`, `$value`, `$PATH`,
    /// `$currentShape`) are pre-bound into the validator query. ASK validators
    /// run per value node (violation iff they return `false`); SELECT validators
    /// run once per focus (each solution row is a violation).
    #[allow(clippy::too_many_arguments)]
    fn collect_components(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        path_term: &Option<Term>,
        parsed_path: &Option<Path>,
        value_nodes: &[Term],
        out: &mut Vec<ValidationResult>,
        inherited: &[Term],
    ) {
        if self.components.is_empty() {
            return;
        }
        let is_property_shape = parsed_path.is_some();
        for component in &self.components {
            // Activation: every mandatory parameter must have a value on `shape`.
            let mut params: Vec<(String, Term)> = Vec::new();
            let mut activated = true;
            for p in &component.params {
                if let Some(value) = self.shapes.object(shape, p.path.as_ref()) {
                    params.push((p.var.clone(), value));
                } else if !p.optional {
                    activated = false;
                    break;
                }
            }
            if !activated {
                continue;
            }

            let validator = if is_property_shape {
                component
                    .property_validator
                    .as_ref()
                    .or(component.generic_validator.as_ref())
            } else {
                component
                    .node_validator
                    .as_ref()
                    .or(component.generic_validator.as_ref())
            };
            let Some(validator) = validator else { continue };

            // Bindings shared across value nodes: parameters and $currentShape.
            // For property shapes, `$PATH` is pre-bound to the shape's path
            // (simple predicate or complex property path) inside the executor.
            let mut base = params;
            base.push(("currentShape".to_string(), node_term_ref(shape)));
            let path = parsed_path.as_ref();

            match validator.kind {
                SparqlQueryKind::Ask => {
                    for value in value_nodes {
                        let mut bindings = base.clone();
                        bindings.push(("this".to_string(), focus.clone()));
                        bindings.push(("value".to_string(), value.clone()));
                        // Conform iff ASK is true; a runtime error fails closed.
                        let violates = match self.sparql.eval_ask(&validator.query, path, &bindings)
                        {
                            Ok(conforms) => !conforms,
                            Err(_) => true,
                        };
                        if violates {
                            self.push_component_result(
                                out,
                                component,
                                shape,
                                focus,
                                path_term.clone(),
                                Some(value.clone()),
                                &bindings,
                                &validator.messages,
                                inherited,
                            );
                        }
                    }
                }
                SparqlQueryKind::Select => {
                    let mut bindings = base.clone();
                    bindings.push(("this".to_string(), focus.clone()));
                    match self.sparql.eval_select(&validator.query, path, &bindings) {
                        Ok(rows) => {
                            for row in rows {
                                // ?value projected; for node validators it is the
                                // focus node itself when not projected.
                                let value = row
                                    .get("value")
                                    .cloned()
                                    .or_else(|| (!is_property_shape).then(|| focus.clone()));
                                let path = row.get("path").cloned().or_else(|| path_term.clone());
                                let mut binds = bindings.clone();
                                binds.extend(row);
                                self.push_component_result(
                                    out,
                                    component,
                                    shape,
                                    focus,
                                    path,
                                    value,
                                    &binds,
                                    &validator.messages,
                                    inherited,
                                );
                            }
                        }
                        Err(_) => self.push_component_result(
                            out,
                            component,
                            shape,
                            focus,
                            path_term.clone(),
                            None,
                            &bindings,
                            &validator.messages,
                            inherited,
                        ),
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn push_component_result(
        &self,
        out: &mut Vec<ValidationResult>,
        component: &CustomComponent,
        shape: &NamedOrBlankNode,
        focus: &Term,
        path: Option<Term>,
        value: Option<Term>,
        bindings: &[(String, Term)],
        validator_messages: &[Term],
        inherited: &[Term],
    ) {
        let raw = if validator_messages.is_empty() {
            self.messages_or_inherited(shape, inherited)
        } else {
            validator_messages.to_vec()
        };
        let bind_map: HashMap<String, Term> = bindings.iter().cloned().collect();
        let messages = substitute_messages(&raw, focus, &bind_map);
        out.push(ValidationResult {
            focus: focus.clone(),
            path,
            value,
            component: component.iri.clone(),
            source_shape: node_term_ref(shape),
            severity: self.severity(shape),
            messages,
        });
    }

    /// `sh:expression` constraints (SHACL-AF §5). The node expression is
    /// evaluated with the focus node as `?this`; every produced value that is
    /// not the boolean `true` yields one `sh:ExpressionConstraintComponent`
    /// result whose `sh:value` is that value. Expression forms the report path
    /// cannot evaluate (function applications) are skipped, matching the
    /// lowering path which diagnoses them rather than under-constraining.
    fn collect_expression(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        out: &mut Vec<ValidationResult>,
        visited: &mut Visited,
        inherited: &[Term],
    ) {
        for expr_term in self.shapes.objects(shape, vocab::SH_EXPRESSION) {
            let Some(results) = self.eval_node_expr(&expr_term, focus, visited) else {
                continue;
            };
            for value in results {
                if is_boolean_true(&value) {
                    continue;
                }
                out.push(ValidationResult {
                    focus: focus.clone(),
                    path: None,
                    value: Some(value),
                    component: vocab::SH_CC_EXPRESSION.into_owned(),
                    source_shape: node_term_ref(shape),
                    severity: self.severity(shape),
                    messages: self.messages_or_inherited(shape, inherited),
                });
            }
        }
    }

    /// Evaluate a SHACL-AF node expression term (read straight from the shapes
    /// graph) with `focus` as `?this`. `None` means the expression uses a form
    /// the report path cannot evaluate (e.g. a function application), so the
    /// caller skips the owning constraint.
    fn eval_node_expr(
        &self,
        term: &Term,
        focus: &Term,
        visited: &mut Visited,
    ) -> Option<Vec<Term>> {
        match term {
            Term::NamedNode(n) if n.as_ref() == vocab::SH_THIS => Some(vec![focus.clone()]),
            Term::NamedNode(_) | Term::Literal(_) => Some(vec![term.clone()]),
            Term::BlankNode(_) => {
                let node = term_to_node(term)?;
                if let Some(path_term) = self.shapes.object(&node, vocab::SH_PATH) {
                    let path = parse_path(self.shapes, &path_term).ok()?;
                    Some(succ(self.frozen(), focus, &path).into_iter().collect())
                } else if let Some(filter_shape) = self.shapes.object(&node, vocab::SH_FILTER_SHAPE)
                {
                    let filter_shape = term_to_node(&filter_shape)?;
                    let nodes_term = self.shapes.object(&node, vocab::SH_NODES)?;
                    let inputs = self.eval_node_expr(&nodes_term, focus, visited)?;
                    Some(
                        inputs
                            .into_iter()
                            .filter(|x| self.conforms(&filter_shape, x, visited))
                            .collect(),
                    )
                } else if let Some(list) = self.shapes.object(&node, vocab::SH_INTERSECTION) {
                    self.eval_node_expr_set(&list, focus, visited, true)
                } else if let Some(list) = self.shapes.object(&node, vocab::SH_UNION) {
                    self.eval_node_expr_set(&list, focus, visited, false)
                } else {
                    // Function application or unrecognized form: unsupported here.
                    None
                }
            }
        }
    }

    /// Evaluate the members of an `sh:intersection` / `sh:union` list and combine
    /// them (`intersect = true` for intersection, set union otherwise),
    /// preserving each member's order while deduplicating.
    fn eval_node_expr_set(
        &self,
        list_head: &Term,
        focus: &Term,
        visited: &mut Visited,
        intersect: bool,
    ) -> Option<Vec<Term>> {
        let members = self.shapes.read_list(list_head);
        if members.is_empty() {
            return None;
        }
        let mut iter = members.iter();
        let mut acc = self.eval_node_expr(iter.next().unwrap(), focus, visited)?;
        for member in iter {
            let next = self.eval_node_expr(member, focus, visited)?;
            if intersect {
                acc.retain(|x| next.contains(x));
            } else {
                for t in next {
                    if !acc.contains(&t) {
                        acc.push(t);
                    }
                }
            }
        }
        Some(acc)
    }

    /// `sh:sparql` constraints (SHACL-SPARQL). Each `SELECT`/`ASK` query runs for
    /// the focus node against the context store; every solution (or a `true`
    /// `ASK`) is one `sh:SPARQLConstraintComponent` result. A `value`/`path`
    /// projected by the query overrides the value node / `sh:resultPath`.
    /// Build the [`SparqlConstraint`] for a `sh:sparql` constraint node, applying
    /// the same canonicalization the lowering path uses. `None` when the node has
    /// neither `sh:select` nor `sh:ask`, or when canonicalization fails (matching
    /// the lowering path, which omits such constraints with a diagnostic).
    fn build_sparql_constraint(
        &self,
        shape: &NamedOrBlankNode,
        constraint_node: &NamedOrBlankNode,
        parsed_path: &Option<Path>,
    ) -> Option<SparqlConstraint> {
        let (kind, raw) = if let Some(Term::Literal(query)) =
            self.shapes.object(constraint_node, vocab::SH_SELECT)
        {
            (SparqlQueryKind::Select, query.value().to_string())
        } else if let Some(Term::Literal(query)) =
            self.shapes.object(constraint_node, vocab::SH_ASK)
        {
            (SparqlQueryKind::Ask, query.value().to_string())
        } else {
            return None;
        };
        let (_, query) = canonical_sparql_query(self.shapes, constraint_node, &raw).ok()?;
        Some(SparqlConstraint {
            kind,
            query,
            path: parsed_path.clone(),
            shape: Some(node_term_ref(shape)),
            // The report path resolves messages itself, so the constraint's own
            // message slot is left empty here.
            messages: Vec::new(),
            extra_bindings: Vec::new(),
            bind_value_to_this: false,
        })
    }

    /// Batch-evaluate a shape's direct `sh:sparql` constraints over its whole
    /// focus set before the per-focus walk, so fallback queries run once over a
    /// `VALUES` table (doc §189) rather than once per focus.
    fn prefetch_sparql(&self, shape: &NamedOrBlankNode, foci: &[Term]) {
        if !self.needs_sparql || foci.len() < 2 {
            return;
        }
        let (_, parsed_path) = self.shape_path(shape);
        for constraint_term in self.shapes.objects(shape, vocab::SH_SPARQL) {
            let Some(constraint_node) = term_to_node(&constraint_term) else {
                continue;
            };
            if let Some(constraint) =
                self.build_sparql_constraint(shape, &constraint_node, &parsed_path)
            {
                let _ = self.sparql.prefetch_constraint(&constraint, foci);
            }
        }
    }

    fn collect_sparql(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        path_term: &Option<Term>,
        parsed_path: &Option<Path>,
        out: &mut Vec<ValidationResult>,
        inherited: &[Term],
    ) {
        if !self.needs_sparql {
            return;
        }
        let sparql = &self.sparql;
        let severity = self.severity(shape);
        for constraint_term in self.shapes.objects(shape, vocab::SH_SPARQL) {
            let Some(constraint_node) = term_to_node(&constraint_term) else {
                continue;
            };
            let Some(constraint) =
                self.build_sparql_constraint(shape, &constraint_node, parsed_path)
            else {
                continue;
            };
            // Mirror lower.rs §179-184: constraint-node sh:message takes
            // precedence; absent that, fall back to the owning shape's
            // sh:message, then to the nearest enclosing shape's.
            let raw_messages = {
                let on_constraint = self.shapes.objects(&constraint_node, vocab::SH_MESSAGE);
                if on_constraint.is_empty() {
                    self.messages_or_inherited(shape, inherited)
                } else {
                    on_constraint
                }
            };
            match sparql.constraint_violations(&constraint, focus) {
                Ok(violations) => {
                    for violation in violations {
                        let messages =
                            substitute_messages(&raw_messages, focus, &violation.bindings);
                        // SHACL-AF §8.4.1: for SELECT constraints, when ?value is
                        // not projected, the focus node itself is used as sh:value.
                        let value = violation.value.or_else(|| match constraint.kind {
                            SparqlQueryKind::Select => Some(focus.clone()),
                            SparqlQueryKind::Ask => None,
                        });
                        out.push(ValidationResult {
                            focus: focus.clone(),
                            path: violation.path.or_else(|| path_term.clone()),
                            value,
                            component: vocab::SH_CC_SPARQL.into_owned(),
                            source_shape: node_term_ref(shape),
                            severity: severity.clone(),
                            messages,
                        });
                    }
                }
                // Runtime failure (e.g. an unsupported graph-reading function
                // under UnsupportedPolicy::Error, or complex-path prebinding):
                // fail closed, surfacing the error so it is not a silent miss.
                Err(error) => {
                    let mut messages = raw_messages;
                    messages.push(Term::Literal(Literal::new_simple_literal(format!(
                        "SPARQL constraint evaluation failed: {error}"
                    ))));
                    out.push(ValidationResult {
                        focus: focus.clone(),
                        path: path_term.clone(),
                        value: None,
                        component: vocab::SH_CC_SPARQL.into_owned(),
                        source_shape: node_term_ref(shape),
                        severity: severity.clone(),
                        messages,
                    });
                }
            }
        }
    }

    fn collect_closed(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        value_nodes: &[Term],
        out: &mut Vec<ValidationResult>,
        inherited: &[Term],
    ) {
        if !self.bool(shape, vocab::SH_CLOSED) {
            return;
        }
        let mut allowed = HashSet::new();
        for prop in self.shapes.objects(shape, vocab::SH_PROPERTY) {
            let Some(prop) = term_to_node(&prop) else {
                continue;
            };
            if let Some(Term::NamedNode(path)) = self.shapes.object(&prop, vocab::SH_PATH) {
                allowed.insert(path);
            }
        }
        for list in self.shapes.objects(shape, vocab::SH_IGNORED_PROPERTIES) {
            for term in self.shapes.read_list(&list) {
                if let Term::NamedNode(predicate) = term {
                    allowed.insert(predicate);
                }
            }
        }
        for value_node in value_nodes {
            for (predicate, object) in self.frozen().outgoing(value_node) {
                if allowed.contains(&predicate) {
                    continue;
                }
                out.push(ValidationResult {
                    focus: focus.clone(),
                    path: Some(Term::NamedNode(predicate)),
                    value: Some(object),
                    component: vocab::SH_CC_CLOSED.into_owned(),
                    source_shape: node_term_ref(shape),
                    severity: self.severity(shape),
                    messages: self.messages_or_inherited(shape, inherited),
                });
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_property_pairs(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        path: &Option<Term>,
        value_nodes: &[Term],
        out: &mut Vec<ValidationResult>,
        inherited: &[Term],
    ) {
        for predicate in self.shapes.objects(shape, vocab::SH_EQUALS) {
            let Term::NamedNode(predicate) = predicate else {
                continue;
            };
            let other = succ(self.frozen(), focus, &Path::Pred(predicate));
            for value in value_nodes.iter().filter(|value| !other.contains(*value)) {
                self.push(
                    out,
                    shape,
                    focus,
                    path.clone(),
                    Some((*value).clone()),
                    vocab::SH_CC_EQUALS,
                    inherited,
                );
            }
            for value in other.iter().filter(|value| !value_nodes.contains(*value)) {
                self.push(
                    out,
                    shape,
                    focus,
                    path.clone(),
                    Some(value.clone()),
                    vocab::SH_CC_EQUALS,
                    inherited,
                );
            }
        }
        for predicate in self.shapes.objects(shape, vocab::SH_DISJOINT) {
            let Term::NamedNode(predicate) = predicate else {
                continue;
            };
            let other = succ(self.frozen(), focus, &Path::Pred(predicate));
            for value in value_nodes.iter().filter(|value| other.contains(*value)) {
                self.push(
                    out,
                    shape,
                    focus,
                    path.clone(),
                    Some((*value).clone()),
                    vocab::SH_CC_DISJOINT,
                    inherited,
                );
            }
        }
        for (constraint, component, inclusive) in [
            (vocab::SH_LESS_THAN, vocab::SH_CC_LESS_THAN, false),
            (
                vocab::SH_LESS_THAN_OR_EQUALS,
                vocab::SH_CC_LESS_THAN_OR_EQUALS,
                true,
            ),
        ] {
            for predicate in self.shapes.objects(shape, constraint) {
                let Term::NamedNode(predicate) = predicate else {
                    continue;
                };
                for left in value_nodes {
                    for right in succ(self.frozen(), focus, &Path::Pred(predicate.clone())) {
                        let ordering = compare_terms(left, &right);
                        let passes = ordering == Some(Ordering::Less)
                            || inclusive && ordering == Some(Ordering::Equal);
                        if !passes {
                            self.push(
                                out,
                                shape,
                                focus,
                                path.clone(),
                                Some(left.clone()),
                                component,
                                inherited,
                            );
                        }
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_unique_lang(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        path: &Option<Term>,
        value_nodes: &[Term],
        out: &mut Vec<ValidationResult>,
        inherited: &[Term],
    ) {
        if !self.bool(shape, vocab::SH_UNIQUE_LANG) {
            return;
        }
        let mut counts = HashMap::new();
        for value in value_nodes {
            if let Term::Literal(literal) = value
                && let Some(language) = literal.language()
            {
                *counts
                    .entry(language.to_ascii_lowercase())
                    .or_insert(0usize) += 1;
            }
        }
        for _ in counts.values().filter(|count| **count > 1) {
            self.push(
                out,
                shape,
                focus,
                path.clone(),
                None,
                vocab::SH_CC_UNIQUE_LANG,
                inherited,
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_qualified_counts(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        path: &Option<Term>,
        value_nodes: &[Term],
        out: &mut Vec<ValidationResult>,
        visited: &mut Visited,
        inherited: &[Term],
    ) {
        for qualifier in self.shapes.objects(shape, vocab::SH_QUALIFIED_VALUE_SHAPE) {
            let Some(qualifier) = term_to_node(&qualifier) else {
                continue;
            };
            let siblings = if self.bool(shape, vocab::SH_QUALIFIED_VALUE_SHAPES_DISJOINT) {
                self.sibling_qualified_shapes(shape, &qualifier)
            } else {
                Vec::new()
            };
            let count = value_nodes
                .iter()
                .filter(|value| {
                    self.conforms(&qualifier, value, visited)
                        && siblings
                            .iter()
                            .all(|sibling| !self.conforms(sibling, value, visited))
                })
                .count() as u64;
            if let Some(min) = self.int(shape, vocab::SH_QUALIFIED_MIN_COUNT)
                && count < min
            {
                self.push(
                    out,
                    shape,
                    focus,
                    path.clone(),
                    None,
                    vocab::SH_CC_QUALIFIED_MIN_COUNT,
                    inherited,
                );
            }
            if let Some(max) = self.int(shape, vocab::SH_QUALIFIED_MAX_COUNT)
                && count > max
            {
                self.push(
                    out,
                    shape,
                    focus,
                    path.clone(),
                    None,
                    vocab::SH_CC_QUALIFIED_MAX_COUNT,
                    inherited,
                );
            }
        }
    }

    fn sibling_qualified_shapes(
        &self,
        shape: &NamedOrBlankNode,
        qualifier: &NamedOrBlankNode,
    ) -> Vec<NamedOrBlankNode> {
        let shape_term = node_term_ref(shape);
        let mut siblings = HashSet::new();
        for triple in self.shapes.graph.triples_for_predicate(vocab::SH_PROPERTY) {
            if triple.object != shape_term.as_ref() {
                continue;
            }
            let parent = triple.subject.into_owned();
            for property in self.shapes.objects(&parent, vocab::SH_PROPERTY) {
                let Some(property) = term_to_node(&property) else {
                    continue;
                };
                for qualifier in self
                    .shapes
                    .objects(&property, vocab::SH_QUALIFIED_VALUE_SHAPE)
                {
                    if let Some(qualifier) = term_to_node(&qualifier) {
                        siblings.insert(qualifier);
                    }
                }
            }
        }
        siblings.remove(qualifier);
        siblings.into_iter().collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn push(
        &self,
        out: &mut Vec<ValidationResult>,
        shape: &NamedOrBlankNode,
        focus: &Term,
        path: Option<Term>,
        value: Option<Term>,
        component: NamedNodeRef<'static>,
        inherited: &[Term],
    ) {
        let mut bindings = HashMap::new();
        if let Some(v) = &value {
            bindings.insert("value".to_string(), v.clone());
        }
        if let Some(p) = &path {
            bindings.insert("path".to_string(), p.clone());
        }
        let raw = self.messages_or_inherited(shape, inherited);
        let messages = substitute_messages(&raw, focus, &bindings);
        out.push(ValidationResult {
            focus: focus.clone(),
            path,
            value,
            component: component.into_owned(),
            source_shape: node_term_ref(shape),
            severity: self.severity(shape),
            messages,
        });
    }

    /// Read `sh:message` values from `shape` to propagate as `sh:resultMessage`.
    fn messages(&self, shape: &NamedOrBlankNode) -> Vec<Term> {
        self.shapes.objects(shape, vocab::SH_MESSAGE)
    }

    fn conforms(&self, shape: &NamedOrBlankNode, focus: &Term, visited: &mut Visited) -> bool {
        let mut scratch = Vec::new();
        self.collect(shape, focus, &mut scratch, visited, &[]);
        scratch.is_empty()
    }

    /// Each value-scoped constraint component on `shape` and whether it holds at
    /// value node `u`. `sh:and`/`or`/`not`/`node` report as a unit.
    fn value_checks(
        &self,
        shape: &NamedOrBlankNode,
        u: &Term,
        visited: &mut Visited,
    ) -> Vec<(NamedNode, bool)> {
        let mut checks = Vec::new();

        for c in self.shapes.objects(shape, vocab::SH_CLASS) {
            checks.push((vocab::SH_CC_CLASS.into_owned(), self.is_instance(u, &c)));
        }
        for d in self.shapes.objects(shape, vocab::SH_DATATYPE) {
            if let Term::NamedNode(dt) = d {
                let ok = value_type_holds(&ValueType::Datatype(dt), u);
                checks.push((vocab::SH_CC_DATATYPE.into_owned(), ok));
            }
        }
        for k in self.shapes.objects(shape, vocab::SH_NODE_KIND) {
            if let Some(set) = map_node_kind(&k) {
                checks.push((vocab::SH_CC_NODE_KIND.into_owned(), set.matches(u)));
            }
        }
        // numeric ranges (each bound is its own component)
        for (pred_iri, comp, inclusive) in [
            (vocab::SH_MIN_INCLUSIVE, vocab::SH_CC_MIN_INCLUSIVE, true),
            (vocab::SH_MIN_EXCLUSIVE, vocab::SH_CC_MIN_EXCLUSIVE, false),
        ] {
            if let Some(Term::Literal(b)) = self.shapes.object(shape, pred_iri) {
                let vt = ValueType::NumericRange {
                    lo: Some(Bound {
                        value: b,
                        inclusive,
                    }),
                    hi: None,
                };
                checks.push((comp.into_owned(), value_type_holds(&vt, u)));
            }
        }
        for (pred_iri, comp, inclusive) in [
            (vocab::SH_MAX_INCLUSIVE, vocab::SH_CC_MAX_INCLUSIVE, true),
            (vocab::SH_MAX_EXCLUSIVE, vocab::SH_CC_MAX_EXCLUSIVE, false),
        ] {
            if let Some(Term::Literal(b)) = self.shapes.object(shape, pred_iri) {
                let vt = ValueType::NumericRange {
                    lo: None,
                    hi: Some(Bound {
                        value: b,
                        inclusive,
                    }),
                };
                checks.push((comp.into_owned(), value_type_holds(&vt, u)));
            }
        }
        // length / pattern
        let min_len = self.int(shape, vocab::SH_MIN_LENGTH);
        let max_len = self.int(shape, vocab::SH_MAX_LENGTH);
        if let Some(m) = min_len {
            let vt = ValueType::Length {
                min: Some(m),
                max: None,
            };
            checks.push((
                vocab::SH_CC_MIN_LENGTH.into_owned(),
                value_type_holds(&vt, u),
            ));
        }
        if let Some(m) = max_len {
            let vt = ValueType::Length {
                min: None,
                max: Some(m),
            };
            checks.push((
                vocab::SH_CC_MAX_LENGTH.into_owned(),
                value_type_holds(&vt, u),
            ));
        }
        if let Some(Term::Literal(re)) = self.shapes.object(shape, vocab::SH_PATTERN) {
            let flags = match self.shapes.object(shape, vocab::SH_FLAGS) {
                Some(Term::Literal(f)) => f.value().to_string(),
                _ => String::new(),
            };
            let vt = ValueType::Pattern {
                regex: re.value().to_string(),
                flags,
            };
            checks.push((vocab::SH_CC_PATTERN.into_owned(), value_type_holds(&vt, u)));
        }
        // sh:in
        for list in self.shapes.objects(shape, vocab::SH_IN) {
            let members = self.shapes.read_list(&list);
            checks.push((vocab::SH_CC_IN.into_owned(), members.contains(u)));
        }
        for list in self.shapes.objects(shape, vocab::SH_LANGUAGE_IN) {
            let languages = self
                .shapes
                .read_list(&list)
                .into_iter()
                .filter_map(|term| match term {
                    Term::Literal(literal) => Some(literal.value().to_string()),
                    _ => None,
                })
                .collect();
            checks.push((
                vocab::SH_CC_LANGUAGE_IN.into_owned(),
                value_type_holds(&ValueType::LangIn(languages), u),
            ));
        }

        // logical (unit results)
        for list in self.shapes.objects(shape, vocab::SH_AND) {
            let ok = self
                .shapes
                .read_list(&list)
                .iter()
                .filter_map(term_to_node)
                .all(|m| self.conforms(&m, u, visited));
            checks.push((vocab::SH_CC_AND.into_owned(), ok));
        }
        for list in self.shapes.objects(shape, vocab::SH_OR) {
            let ok = self
                .shapes
                .read_list(&list)
                .iter()
                .filter_map(term_to_node)
                .any(|m| self.conforms(&m, u, visited));
            checks.push((vocab::SH_CC_OR.into_owned(), ok));
        }
        for list in self.shapes.objects(shape, vocab::SH_XONE) {
            let count = self
                .shapes
                .read_list(&list)
                .iter()
                .filter_map(term_to_node)
                .filter(|m| self.conforms(m, u, visited))
                .count();
            checks.push((vocab::SH_CC_XONE.into_owned(), count == 1));
        }
        for n in self.shapes.objects(shape, vocab::SH_NOT) {
            if let Some(nn) = term_to_node(&n) {
                checks.push((
                    vocab::SH_CC_NOT.into_owned(),
                    !self.conforms(&nn, u, visited),
                ));
            }
        }
        for n in self.shapes.objects(shape, vocab::SH_NODE) {
            if let Some(nn) = term_to_node(&n) {
                checks.push((
                    vocab::SH_CC_NODE.into_owned(),
                    self.conforms(&nn, u, visited),
                ));
            }
        }

        checks
    }

    fn is_instance(&self, u: &Term, class: &Term) -> bool {
        succ(self.frozen(), u, &class_path()).contains(class)
    }

    fn int(&self, s: &NamedOrBlankNode, p: NamedNodeRef) -> Option<u64> {
        match self.shapes.object(s, p) {
            Some(Term::Literal(l)) => l.value().parse().ok(),
            _ => None,
        }
    }

    fn bool(&self, s: &NamedOrBlankNode, p: NamedNodeRef) -> bool {
        matches!(
            self.shapes.object(s, p),
            Some(Term::Literal(ref literal)) if matches!(literal.value(), "true" | "1")
        )
    }

    /// `sh:resultSeverity` for results from `shape`: its declared `sh:severity`
    /// (an IRI such as `sh:Warning`/`sh:Info`), defaulting to `sh:Violation`.
    fn severity(&self, shape: &NamedOrBlankNode) -> NamedNode {
        match self.shapes.object(shape, vocab::SH_SEVERITY) {
            Some(Term::NamedNode(n)) => n,
            _ => vocab::SH_VIOLATION.into_owned(),
        }
    }
}

fn is_shape_node(shapes: &Loaded, node: &NamedOrBlankNode) -> bool {
    shapes.has_type(node, vocab::SH_NODE_SHAPE)
        || shapes.has_type(node, vocab::SH_PROPERTY_SHAPE)
        || [
            vocab::SH_PROPERTY,
            vocab::SH_NODE,
            vocab::SH_AND,
            vocab::SH_OR,
            vocab::SH_NOT,
            vocab::SH_XONE,
            vocab::SH_DATATYPE,
            vocab::SH_CLASS,
            vocab::SH_NODE_KIND,
            vocab::SH_IN,
            vocab::SH_HAS_VALUE,
            vocab::SH_PROPERTY,
        ]
        .iter()
        .any(|predicate| shapes.object(node, *predicate).is_some())
}

/// Whether any `sh:select` / `sh:ask` query references `$shapesGraph`, so the
/// shapes graph must be mirrored into a named graph for evaluation.
fn shapes_reference_shapes_graph(shapes: &Loaded) -> bool {
    [vocab::SH_SELECT, vocab::SH_ASK].iter().any(|predicate| {
        shapes.graph.triples_for_predicate(*predicate).any(
            |t| matches!(t.object, oxrdf::TermRef::Literal(l) if l.value().contains("shapesGraph")),
        )
    })
}

fn class_path() -> Path {
    Path::seq(vec![
        Path::Pred(vocab::rdf_type()),
        Path::star(Path::Pred(vocab::rdfs_subclassof())),
    ])
}

/// Index `class → focus-data instances` under `rdf:type / rdfs:subClassOf*`.
///
/// One pass over the `rdf:type` triples replaces the per-shape forward scan
/// (`graph_nodes(data).filter(node is instance of c)`), which was
/// `O(shapes × nodes × type-closure)`. Each instance is attributed to every
/// superclass of its declared type, and the reflexive `subClassOf*` closure of
/// each distinct type is computed at most once. Only nodes present in the focus
/// (data) graph are indexed, matching the original target-selection semantics.
fn build_class_index(
    focus_data: &Graph,
    frozen: &FrozenIndexedDataset,
) -> HashMap<Term, Vec<Term>> {
    let focus_nodes = graph_nodes(focus_data);
    let subclass_star = Path::star(Path::Pred(vocab::rdfs_subclassof()));
    let mut supers: HashMap<Term, Vec<Term>> = HashMap::new();
    let mut index: HashMap<Term, Vec<Term>> = HashMap::new();
    let mut seen: HashSet<(Term, Term)> = HashSet::new();
    for (node, ty) in frozen.triples_for_predicate(&vocab::rdf_type()) {
        if !focus_nodes.contains(&node) {
            continue;
        }
        let classes = supers
            .entry(ty.clone())
            .or_insert_with(|| succ(frozen, &ty, &subclass_star).into_iter().collect());
        for class in classes.iter() {
            if seen.insert((class.clone(), node.clone())) {
                index.entry(class.clone()).or_default().push(node.clone());
            }
        }
    }
    index
}

fn graph_nodes(graph: &Graph) -> HashSet<Term> {
    let mut nodes = HashSet::new();
    for triple in graph.iter() {
        nodes.insert(node_term(triple.subject));
        nodes.insert(triple.object.into_owned());
    }
    nodes
}

fn node_term(s: oxrdf::NamedOrBlankNodeRef) -> Term {
    crate::path::term_of(s.into_owned())
}

fn node_term_ref(s: &NamedOrBlankNode) -> Term {
    match s {
        NamedOrBlankNode::NamedNode(n) => Term::NamedNode(n.clone()),
        NamedOrBlankNode::BlankNode(b) => Term::BlankNode(b.clone()),
    }
}

fn map_node_kind(term: &Term) -> Option<NodeKindSet> {
    let Term::NamedNode(n) = term else {
        return None;
    };
    let r = n.as_ref();
    Some(if r == vocab::SH_IRI {
        NodeKindSet::IRI
    } else if r == vocab::SH_BLANK_NODE {
        NodeKindSet::BLANK_NODE
    } else if r == vocab::SH_LITERAL {
        NodeKindSet::LITERAL
    } else if r == vocab::SH_BLANK_NODE_OR_IRI {
        NodeKindSet::BLANK_NODE_OR_IRI
    } else if r == vocab::SH_BLANK_NODE_OR_LITERAL {
        NodeKindSet::BLANK_NODE_OR_LITERAL
    } else if r == vocab::SH_IRI_OR_LITERAL {
        NodeKindSet::IRI_OR_LITERAL
    } else {
        return None;
    })
}
