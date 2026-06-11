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
use crate::sparql::SparqlExecutor;
use crate::validate::{ValidationGraphMode, apply_message_template, graph_union};
use crate::value::{compare_terms, value_type_holds};
use oxrdf::{BlankNode, Graph, Literal, NamedNode, NamedNodeRef, NamedOrBlankNode, Term, Triple};
use shifty_algebra::value_type::{Bound, ValueType};
use shifty_algebra::{NodeKindSet, Path, SparqlConstraint, SparqlQueryKind};
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

/// Validate `data` against the shapes in `shapes`, producing a W3C report.
pub fn validate_report(shapes: &Loaded, data: &Graph) -> ValidationReport {
    validate_report_context(shapes, data, data)
}

/// Validate split data and shapes graphs using the selected graph mode.
pub fn validate_report_graphs(shapes: &Loaded, data: &Graph) -> ValidationReport {
    validate_report_graphs_with_mode(shapes, data, ValidationGraphMode::default())
}

/// Validate split data and shapes graphs using an explicit graph mode.
pub fn validate_report_graphs_with_mode(
    shapes: &Loaded,
    data: &Graph,
    mode: ValidationGraphMode,
) -> ValidationReport {
    match mode {
        ValidationGraphMode::Data => validate_report_context(shapes, data, data),
        ValidationGraphMode::Union => {
            let union = graph_union(data, &shapes.graph);
            validate_report_context(shapes, data, &union)
        }
        ValidationGraphMode::UnionAll => {
            let union = graph_union(data, &shapes.graph);
            validate_report_context(shapes, &union, &union)
        }
    }
}

fn validate_report_context(
    shapes: &Loaded,
    focus_data: &Graph,
    context: &Graph,
) -> ValidationReport {
    // SHACL-SPARQL constraints and SPARQL-based targets execute against an
    // in-memory store over the context graph. Build it once, and only when some
    // shape actually carries a `sh:sparql` constraint or a `sh:target`, so the
    // common case pays nothing.
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
    let sparql = if needs_sparql {
        // Only mirror the shapes into a named graph (for `$shapesGraph`) when a
        // query actually references it — otherwise skip the extra copy.
        if shapes_reference_shapes_graph(shapes) {
            SparqlExecutor::new_with_shapes(context, &shapes.graph)
                .ok()
                .map(|s| s.with_frozen(FrozenIndexedDataset::from_graphs(context, &shapes.graph)))
        } else {
            SparqlExecutor::new(context)
                .ok()
                .map(|s| s.with_frozen(FrozenIndexedDataset::from_graph(context)))
        }
    } else {
        None
    };
    // Dictionary-encoded snapshot of the context, used as the indexed backend
    // for every `sh:path` / `rdfs:subClassOf*` traversal below. Built once and
    // shared across all (shape, focus) visits — the report-path analogue of the
    // algebra path's `FrozenIndexedDataset` reachability backend.
    let frozen = FrozenIndexedDataset::from_graph(context);
    // Index class membership once (instead of a forward scan over every node per
    // class-target shape): this is the report path's analogue of the plan's
    // backward `PathToConst` focus source, amortized across all shapes.
    let class_index = build_class_index(context, focus_data, &frozen);
    let r = Reporter {
        shapes,
        focus_data,
        context,
        frozen,
        sparql,
        class_index,
        path_cache: RefCell::new(HashMap::new()),
    };
    let mut results = Vec::new();
    for shape in r.target_shapes() {
        for focus in r.focus_nodes(&shape) {
            let mut visited = HashSet::new();
            r.collect(&shape, &focus, &mut results, &mut visited);
        }
    }
    ValidationReport {
        conforms: results.is_empty(),
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

struct Reporter<'a> {
    shapes: &'a Loaded,
    focus_data: &'a Graph,
    context: &'a Graph,
    /// Indexed snapshot of `context` for path / class-hierarchy traversal.
    frozen: FrozenIndexedDataset,
    sparql: Option<SparqlExecutor>,
    /// `class → focus-data instances` under `rdf:type / rdfs:subClassOf*`, built
    /// once and shared by every `sh:targetClass` / implicit-class lookup.
    class_index: HashMap<Term, Vec<Term>>,
    /// Parsed `sh:path` per shape node, so `collect` does not re-parse the path
    /// RDF on every (shape, focus) visit. `None` = shape has no/invalid path.
    path_cache: RefCell<HashMap<NamedOrBlankNode, PathCacheEntry>>,
}

type Visited = HashSet<(NamedOrBlankNode, Term)>;

/// Cached parsed path and its term representation for sh:path expressions
type PathCacheEntry = (Option<Term>, Option<Path>);

impl Reporter<'_> {
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
        self.shapes.has_type(n, vocab::SH_NODE_SHAPE)
            || self.shapes.has_type(n, vocab::SH_PROPERTY_SHAPE)
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
            .any(|p| self.shapes.object(n, *p).is_some())
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
        if let Some(exec) = &self.sparql {
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

    /// Collect the results of validating `focus` against `shape`.
    fn collect(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        out: &mut Vec<ValidationResult>,
        visited: &mut Visited,
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
            Some(p) => succ(&self.frozen, focus, p).into_iter().collect(),
            None => vec![focus.clone()],
        };
        let severity = self.severity(shape);
        let messages = self.messages(shape);
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

        self.collect_closed(shape, focus, &value_nodes, out);
        self.collect_property_pairs(shape, focus, &path_term, &value_nodes, out);
        self.collect_unique_lang(shape, focus, &path_term, &value_nodes, out);
        self.collect_qualified_counts(shape, focus, &path_term, &value_nodes, out, visited);

        // value-scoped components
        for u in &value_nodes {
            for (component, ok) in self.value_checks(shape, u, visited) {
                if !ok {
                    push(out, Some(u.clone()), component);
                }
            }
        }

        // nested property shapes: delegate (each value node is a focus for P)
        for prop in self.shapes.objects(shape, vocab::SH_PROPERTY) {
            if let Some(pn) = term_to_node(&prop) {
                for u in &value_nodes {
                    self.collect(&pn, u, out, visited);
                }
            }
        }

        self.collect_sparql(shape, focus, &path_term, &parsed, out);

        visited.remove(&key);
    }

    /// `sh:sparql` constraints (SHACL-SPARQL). Each `SELECT`/`ASK` query runs for
    /// the focus node against the context store; every solution (or a `true`
    /// `ASK`) is one `sh:SPARQLConstraintComponent` result. A `value`/`path`
    /// projected by the query overrides the value node / `sh:resultPath`.
    fn collect_sparql(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        path_term: &Option<Term>,
        parsed_path: &Option<Path>,
        out: &mut Vec<ValidationResult>,
    ) {
        let Some(sparql) = &self.sparql else { return };
        let severity = self.severity(shape);
        for constraint_term in self.shapes.objects(shape, vocab::SH_SPARQL) {
            let Some(constraint_node) = term_to_node(&constraint_term) else {
                continue;
            };
            let raw = if let Some(Term::Literal(query)) =
                self.shapes.object(&constraint_node, vocab::SH_SELECT)
            {
                Some((SparqlQueryKind::Select, query.value().to_string()))
            } else if let Some(Term::Literal(query)) =
                self.shapes.object(&constraint_node, vocab::SH_ASK)
            {
                Some((SparqlQueryKind::Ask, query.value().to_string()))
            } else {
                None
            };
            let Some((kind, raw)) = raw else { continue };
            // Drop constraints that fail to canonicalize, matching the lowering
            // path (which emits the diagnostic and omits the constraint).
            let Ok((_, query)) = canonical_sparql_query(self.shapes, &constraint_node, &raw) else {
                continue;
            };
            let constraint = SparqlConstraint {
                kind,
                query,
                path: parsed_path.clone(),
                shape: Some(node_term_ref(shape)),
                // The report path resolves messages itself, so the constraint's
                // own message slot is left empty here.
                messages: Vec::new(),
            };
            // Mirror lower.rs §179-184: constraint-node sh:message takes
            // precedence; absent that, fall back to the owning shape's sh:message.
            let raw_messages = {
                let on_constraint = self.shapes.objects(&constraint_node, vocab::SH_MESSAGE);
                if on_constraint.is_empty() {
                    self.messages(shape)
                } else {
                    on_constraint
                }
            };
            match sparql.constraint_violations(&constraint, focus) {
                Ok(violations) => {
                    for violation in violations {
                        let messages =
                            substitute_messages(&raw_messages, focus, &violation.bindings);
                        out.push(ValidationResult {
                            focus: focus.clone(),
                            path: violation.path.or_else(|| path_term.clone()),
                            value: violation.value,
                            component: vocab::SH_CC_SPARQL.into_owned(),
                            source_shape: node_term_ref(shape),
                            severity: severity.clone(),
                            messages,
                        });
                    }
                }
                // Runtime failure (e.g. complex-path prebinding is unsupported):
                // fail closed, matching the algebra validator.
                Err(_) => out.push(ValidationResult {
                    focus: focus.clone(),
                    path: path_term.clone(),
                    value: None,
                    component: vocab::SH_CC_SPARQL.into_owned(),
                    source_shape: node_term_ref(shape),
                    severity: severity.clone(),
                    messages: raw_messages,
                }),
            }
        }
    }

    fn collect_closed(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        value_nodes: &[Term],
        out: &mut Vec<ValidationResult>,
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
            let Some(node) = term_to_node(value_node) else {
                continue;
            };
            for triple in self.context.triples_for_subject(node.as_ref()) {
                if allowed.contains(&triple.predicate.into_owned()) {
                    continue;
                }
                out.push(ValidationResult {
                    focus: focus.clone(),
                    path: Some(Term::NamedNode(triple.predicate.into_owned())),
                    value: Some(triple.object.into_owned()),
                    component: vocab::SH_CC_CLOSED.into_owned(),
                    source_shape: node_term_ref(shape),
                    severity: self.severity(shape),
                    messages: self.messages(shape),
                });
            }
        }
    }

    fn collect_property_pairs(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        path: &Option<Term>,
        value_nodes: &[Term],
        out: &mut Vec<ValidationResult>,
    ) {
        for predicate in self.shapes.objects(shape, vocab::SH_EQUALS) {
            let Term::NamedNode(predicate) = predicate else {
                continue;
            };
            let other = succ(&self.frozen, focus, &Path::Pred(predicate));
            for value in value_nodes.iter().filter(|value| !other.contains(*value)) {
                self.push(
                    out,
                    shape,
                    focus,
                    path.clone(),
                    Some((*value).clone()),
                    vocab::SH_CC_EQUALS,
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
                );
            }
        }
        for predicate in self.shapes.objects(shape, vocab::SH_DISJOINT) {
            let Term::NamedNode(predicate) = predicate else {
                continue;
            };
            let other = succ(&self.frozen, focus, &Path::Pred(predicate));
            for value in value_nodes.iter().filter(|value| other.contains(*value)) {
                self.push(
                    out,
                    shape,
                    focus,
                    path.clone(),
                    Some((*value).clone()),
                    vocab::SH_CC_DISJOINT,
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
                    for right in succ(&self.frozen, focus, &Path::Pred(predicate.clone())) {
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
                            );
                        }
                    }
                }
            }
        }
    }

    fn collect_unique_lang(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        path: &Option<Term>,
        value_nodes: &[Term],
        out: &mut Vec<ValidationResult>,
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
            );
        }
    }

    fn collect_qualified_counts(
        &self,
        shape: &NamedOrBlankNode,
        focus: &Term,
        path: &Option<Term>,
        value_nodes: &[Term],
        out: &mut Vec<ValidationResult>,
        visited: &mut Visited,
    ) {
        for qualifier in self.shapes.objects(shape, vocab::SH_QUALIFIED_VALUE_SHAPE) {
            let Some(qualifier) = term_to_node(&qualifier) else {
                continue;
            };
            let siblings = if self.bool(shape, vocab::SH_QUALIFIED_VALUE_SHAPES_DISJOINT) {
                self.sibling_qualified_shapes(shape, path.as_ref())
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
                );
            }
        }
    }

    fn sibling_qualified_shapes(
        &self,
        shape: &NamedOrBlankNode,
        path: Option<&Term>,
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
                if property == *shape
                    || self.shapes.object(&property, vocab::SH_PATH).as_ref() != path
                {
                    continue;
                }
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
        siblings.into_iter().collect()
    }

    fn push(
        &self,
        out: &mut Vec<ValidationResult>,
        shape: &NamedOrBlankNode,
        focus: &Term,
        path: Option<Term>,
        value: Option<Term>,
        component: NamedNodeRef<'static>,
    ) {
        let mut bindings = HashMap::new();
        if let Some(v) = &value {
            bindings.insert("value".to_string(), v.clone());
        }
        if let Some(p) = &path {
            bindings.insert("path".to_string(), p.clone());
        }
        let raw = self.messages(shape);
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
        self.collect(shape, focus, &mut scratch, visited);
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
        succ(&self.frozen, u, &class_path()).contains(class)
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
    context: &Graph,
    focus_data: &Graph,
    frozen: &FrozenIndexedDataset,
) -> HashMap<Term, Vec<Term>> {
    let focus_nodes = graph_nodes(focus_data);
    let subclass_star = Path::star(Path::Pred(vocab::rdfs_subclassof()));
    let mut supers: HashMap<Term, Vec<Term>> = HashMap::new();
    let mut index: HashMap<Term, Vec<Term>> = HashMap::new();
    let mut seen: HashSet<(Term, Term)> = HashSet::new();
    for triple in context.triples_for_predicate(vocab::RDF_TYPE) {
        let node = node_term(triple.subject);
        if !focus_nodes.contains(&node) {
            continue;
        }
        let ty = triple.object.into_owned();
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
