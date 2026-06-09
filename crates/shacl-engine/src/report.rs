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

use crate::path::pred;
use crate::path::succ;
use crate::value::value_type_holds;
use oxrdf::{BlankNode, Graph, Literal, NamedNode, NamedNodeRef, NamedOrBlankNode, Term, Triple};
use shacl_algebra::value_type::{Bound, ValueType};
use shacl_algebra::{NodeKindSet, Path};
use shacl_parse::graph::{term_to_node, Loaded};
use shacl_parse::path::parse_path;
use shacl_parse::vocab;
use std::collections::HashSet;

/// One `sh:ValidationResult`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ValidationResult {
    pub focus: Term,
    /// `sh:resultPath` as the original RDF node (predicate IRI for simple paths).
    pub path: Option<Term>,
    pub value: Option<Term>,
    pub component: NamedNode,
    pub source_shape: Term,
}

#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub conforms: bool,
    pub results: Vec<ValidationResult>,
}

/// Validate `data` against the shapes in `shapes`, producing a W3C report.
pub fn validate_report(shapes: &Loaded, data: &Graph) -> ValidationReport {
    let r = Reporter { shapes, data };
    let mut results = Vec::new();
    for shape in r.target_shapes() {
        for focus in r.focus_nodes(&shape) {
            let mut visited = HashSet::new();
            r.collect(&shape, &focus, &mut results, &mut visited);
        }
    }
    ValidationReport { conforms: results.is_empty(), results }
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
        g.insert(&t(rn.clone().into(), vocab::RDF_TYPE, vocab::SH_VALIDATION_RESULT.into_owned().into()));
        g.insert(&t(rn.clone().into(), vocab::SH_FOCUS_NODE, r.focus.clone()));
        if let Some(path) = &r.path {
            g.insert(&t(rn.clone().into(), vocab::SH_RESULT_PATH, path.clone()));
        }
        if let Some(value) = &r.value {
            g.insert(&t(rn.clone().into(), vocab::SH_VALUE, value.clone()));
        }
        g.insert(&t(rn.clone().into(), vocab::SH_RESULT_SEVERITY, vocab::SH_VIOLATION.into_owned().into()));
        g.insert(&t(
            rn.clone().into(),
            vocab::SH_SOURCE_CONSTRAINT_COMPONENT,
            r.component.clone().into(),
        ));
        g.insert(&t(rn.into(), vocab::SH_SOURCE_SHAPE, r.source_shape.clone()));
    }
    g
}

struct Reporter<'a> {
    shapes: &'a Loaded,
    data: &'a Graph,
}

type Visited = HashSet<(NamedOrBlankNode, Term)>;

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
            // implicit class target: a shape that is also an rdfs:Class / owl:Class
            if p == vocab::RDF_TYPE {
                let is_class = matches!(&t.object, oxrdf::TermRef::NamedNode(c)
                    if *c == vocab::RDFS_CLASS || *c == vocab::OWL_CLASS);
                let s = t.subject.into_owned();
                if is_class && self.is_shape(&s) {
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
        self.shapes.has_type(n, vocab::RDFS_CLASS) || self.shapes.has_type(n, vocab::OWL_CLASS)
    }

    fn deactivated(&self, n: &NamedOrBlankNode) -> bool {
        matches!(self.shapes.object(n, vocab::SH_DEACTIVATED),
            Some(Term::Literal(ref l)) if l.value() == "true")
    }

    fn focus_nodes(&self, shape: &NamedOrBlankNode) -> Vec<Term> {
        let mut nodes = Vec::new();
        nodes.extend(self.shapes.objects(shape, vocab::SH_TARGET_NODE));
        for c in self.shapes.objects(shape, vocab::SH_TARGET_CLASS) {
            nodes.extend(pred(self.data, &c, &class_path()));
        }
        for p in self.shapes.objects(shape, vocab::SH_TARGET_SUBJECTS_OF) {
            if let Term::NamedNode(n) = p {
                nodes.extend(
                    self.data.triples_for_predicate(n.as_ref()).map(|t| node_term(t.subject)),
                );
            }
        }
        for p in self.shapes.objects(shape, vocab::SH_TARGET_OBJECTS_OF) {
            if let Term::NamedNode(n) = p {
                nodes.extend(self.data.triples_for_predicate(n.as_ref()).map(|t| t.object.into_owned()));
            }
        }
        // implicit class target: instances of the shape (which is also a class)
        if let NamedOrBlankNode::NamedNode(n) = shape
            && self.is_class(shape)
        {
            nodes.extend(pred(self.data, &Term::NamedNode(n.clone()), &class_path()));
        }
        let mut seen = HashSet::new();
        nodes.retain(|t| seen.insert(t.clone()));
        nodes
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

        let path_term = self.shapes.object(shape, vocab::SH_PATH);
        let parsed = path_term.as_ref().and_then(|t| parse_path(self.shapes, t).ok());
        let value_nodes: Vec<Term> = match &parsed {
            Some(p) => succ(self.data, focus, p).into_iter().collect(),
            None => vec![focus.clone()],
        };
        let push = |out: &mut Vec<ValidationResult>, value, component| {
            out.push(ValidationResult {
                focus: focus.clone(),
                path: path_term.clone(),
                value,
                component,
                source_shape: node_term_ref(shape),
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

        visited.remove(&key);
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
                    lo: Some(Bound { value: b, inclusive }),
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
                    hi: Some(Bound { value: b, inclusive }),
                };
                checks.push((comp.into_owned(), value_type_holds(&vt, u)));
            }
        }
        // length / pattern
        let min_len = self.int(shape, vocab::SH_MIN_LENGTH);
        let max_len = self.int(shape, vocab::SH_MAX_LENGTH);
        if let Some(m) = min_len {
            let vt = ValueType::Length { min: Some(m), max: None };
            checks.push((vocab::SH_CC_MIN_LENGTH.into_owned(), value_type_holds(&vt, u)));
        }
        if let Some(m) = max_len {
            let vt = ValueType::Length { min: None, max: Some(m) };
            checks.push((vocab::SH_CC_MAX_LENGTH.into_owned(), value_type_holds(&vt, u)));
        }
        if let Some(Term::Literal(re)) = self.shapes.object(shape, vocab::SH_PATTERN) {
            let flags = match self.shapes.object(shape, vocab::SH_FLAGS) {
                Some(Term::Literal(f)) => f.value().to_string(),
                _ => String::new(),
            };
            let vt = ValueType::Pattern { regex: re.value().to_string(), flags };
            checks.push((vocab::SH_CC_PATTERN.into_owned(), value_type_holds(&vt, u)));
        }
        // sh:in
        for list in self.shapes.objects(shape, vocab::SH_IN) {
            let members = self.shapes.read_list(&list);
            checks.push((vocab::SH_CC_IN.into_owned(), members.contains(u)));
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
                checks.push((vocab::SH_CC_NOT.into_owned(), !self.conforms(&nn, u, visited)));
            }
        }
        for n in self.shapes.objects(shape, vocab::SH_NODE) {
            if let Some(nn) = term_to_node(&n) {
                checks.push((vocab::SH_CC_NODE.into_owned(), self.conforms(&nn, u, visited)));
            }
        }

        checks
    }

    fn is_instance(&self, u: &Term, class: &Term) -> bool {
        succ(self.data, u, &class_path()).contains(class)
    }

    fn int(&self, s: &NamedOrBlankNode, p: NamedNodeRef) -> Option<u64> {
        match self.shapes.object(s, p) {
            Some(Term::Literal(l)) => l.value().parse().ok(),
            _ => None,
        }
    }
}

fn class_path() -> Path {
    Path::seq(vec![
        Path::Pred(vocab::rdf_type()),
        Path::star(Path::Pred(vocab::rdfs_subclassof())),
    ])
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
    let Term::NamedNode(n) = term else { return None };
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
