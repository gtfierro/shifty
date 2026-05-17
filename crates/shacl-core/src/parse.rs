use crate::diagnostics::{Diagnostic, DiagnosticSeverity, SourceRef};
use crate::source::{ResolvedShapeSet, ShapeSource, SourceLoadOptions, load_with_ontoenv};
use crate::syntax::{
    ConstraintSyntax, PredicateObjects, RuleSyntax, RuleSyntaxKind, ShapeSyntax, ShapeSyntaxDocument,
    ShapeSyntaxKind, TargetSyntax,
};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use std::collections::{HashMap, HashSet};
use std::error::Error;

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const SH_NODE_SHAPE: &str = "http://www.w3.org/ns/shacl#NodeShape";
const SH_PROPERTY_SHAPE: &str = "http://www.w3.org/ns/shacl#PropertyShape";
const SH_TRIPLE_RULE: &str = "http://www.w3.org/ns/shacl#TripleRule";
const SH_SPARQL_RULE: &str = "http://www.w3.org/ns/shacl#SPARQLRule";
const SH_TARGET: &str = "http://www.w3.org/ns/shacl#target";
const SH_TARGET_CLASS: &str = "http://www.w3.org/ns/shacl#targetClass";
const SH_TARGET_NODE: &str = "http://www.w3.org/ns/shacl#targetNode";
const SH_TARGET_SUBJECTS_OF: &str = "http://www.w3.org/ns/shacl#targetSubjectsOf";
const SH_TARGET_OBJECTS_OF: &str = "http://www.w3.org/ns/shacl#targetObjectsOf";
const SH_PATH: &str = "http://www.w3.org/ns/shacl#path";
const SH_PROPERTY: &str = "http://www.w3.org/ns/shacl#property";
const SH_RULE: &str = "http://www.w3.org/ns/shacl#rule";
const SH_SEVERITY: &str = "http://www.w3.org/ns/shacl#severity";
const SH_DEACTIVATED: &str = "http://www.w3.org/ns/shacl#deactivated";
const SH_ORDER: &str = "http://www.w3.org/ns/shacl#order";

pub fn load_and_parse_with_ontoenv(
    sources: &[ShapeSource],
    options: &SourceLoadOptions,
) -> Result<ShapeSyntaxDocument, Box<dyn Error>> {
    let resolved = load_with_ontoenv(sources, options)?;
    Ok(parse_resolved(&resolved))
}

pub fn parse_quads<I>(quads: I) -> ShapeSyntaxDocument
where
    I: IntoIterator<Item = Quad>,
{
    parse_resolved(&ResolvedShapeSet {
        root_graphs: Vec::new(),
        imported_graphs: Vec::new(),
        quads: quads.into_iter().collect(),
    })
}

pub fn parse_resolved(resolved: &ResolvedShapeSet) -> ShapeSyntaxDocument {
    let quads = resolved.quads.clone();
    let provenance_by_graph = provenance_map(resolved);
    let mut by_subject: HashMap<Term, Vec<Quad>> = HashMap::new();
    let mut candidate_shapes = HashSet::new();
    let mut candidate_rules = HashSet::new();

    for quad in &quads {
        let subject_term = subject_to_term(&quad.subject);
        by_subject
            .entry(subject_term.clone())
            .or_default()
            .push(quad.clone());

        let pred = quad.predicate.as_str();
        if pred == RDF_TYPE {
            if matches_named(&quad.object, SH_NODE_SHAPE) || matches_named(&quad.object, SH_PROPERTY_SHAPE)
            {
                candidate_shapes.insert(subject_term.clone());
            }
            if matches_named(&quad.object, SH_TRIPLE_RULE) || matches_named(&quad.object, SH_SPARQL_RULE)
            {
                candidate_rules.insert(subject_term.clone());
            }
        }
        if matches!(
            pred,
            SH_TARGET
                | SH_TARGET_CLASS
                | SH_TARGET_NODE
                | SH_TARGET_SUBJECTS_OF
                | SH_TARGET_OBJECTS_OF
                | SH_PATH
                | SH_PROPERTY
                | SH_RULE
        ) {
            candidate_shapes.insert(subject_term);
        }
        if pred == SH_RULE {
            candidate_rules.insert(quad.object.clone());
        }
    }

    let mut shapes: Vec<ShapeSyntax> = candidate_shapes
        .into_iter()
        .map(|subject| build_shape(&subject, by_subject.get(&subject), &provenance_by_graph))
        .collect();
    shapes.sort_by_key(|shape| shape.subject.to_string());

    let mut rules: Vec<RuleSyntax> = candidate_rules
        .into_iter()
        .map(|subject| build_rule(&subject, by_subject.get(&subject), &provenance_by_graph))
        .collect();
    rules.sort_by_key(|rule| rule.subject.to_string());

    let diagnostics = shapes
        .iter()
        .filter(|shape| matches!(shape.kind, ShapeSyntaxKind::PropertyShape) && shape.path.is_none())
        .map(|shape| Diagnostic {
            severity: DiagnosticSeverity::Warning,
            message: format!("property shape {} has no sh:path", shape.subject),
            source: shape.provenance.first().cloned(),
        })
        .collect();

    ShapeSyntaxDocument {
        shapes,
        rules,
        quads,
        sources: resolved
            .all_graphs()
            .into_iter()
            .map(|graph| graph.source.clone())
            .collect(),
        diagnostics,
    }
}

fn build_shape(
    subject: &Term,
    quads: Option<&Vec<Quad>>,
    provenance_by_graph: &HashMap<Option<String>, SourceRef>,
) -> ShapeSyntax {
    let quads = quads.cloned().unwrap_or_default();
    let kind = if quads.iter().any(|q| matches_named(&q.object, SH_PROPERTY_SHAPE))
        || quads.iter().any(|q| q.predicate.as_str() == SH_PATH)
    {
        ShapeSyntaxKind::PropertyShape
    } else {
        ShapeSyntaxKind::NodeShape
    };

    let mut targets = Vec::new();
    let mut property_shapes = Vec::new();
    let mut path = None;
    let mut severity = None;
    let mut deactivated = false;
    let mut constraints = Vec::new();
    let extras = Vec::new();
    let mut rule_nodes = Vec::new();

    let grouped = group_predicates(&quads);
    for (predicate, objects) in grouped {
        match predicate.as_str() {
            SH_TARGET_CLASS => targets.extend(objects.into_iter().map(TargetSyntax::Class)),
            SH_TARGET_NODE => targets.extend(objects.into_iter().map(TargetSyntax::Node)),
            SH_TARGET_SUBJECTS_OF => {
                targets.extend(objects.into_iter().map(TargetSyntax::SubjectsOf))
            }
            SH_TARGET_OBJECTS_OF => targets.extend(objects.into_iter().map(TargetSyntax::ObjectsOf)),
            SH_TARGET => targets.extend(objects.into_iter().map(TargetSyntax::Advanced)),
            SH_PROPERTY => property_shapes.extend(objects),
            SH_PATH => path = objects.into_iter().next(),
            SH_SEVERITY => severity = objects.into_iter().next(),
            SH_DEACTIVATED => {
                deactivated = objects.iter().any(is_true_literal);
            }
            SH_RULE => rule_nodes.extend(objects),
            RDF_TYPE => {}
            _ => {
                if is_constraint_predicate(predicate.as_str()) {
                    constraints.push(ConstraintSyntax { predicate, objects });
                } else {
                    constraints.push(ConstraintSyntax { predicate, objects });
                }
            }
        }
    }

    ShapeSyntax {
        subject: subject.clone(),
        kind,
        targets,
        property_shapes,
        path,
        severity,
        deactivated,
        constraints,
        rule_nodes,
        extras,
        provenance: provenance_for_quads(&quads, provenance_by_graph),
    }
}

fn build_rule(
    subject: &Term,
    quads: Option<&Vec<Quad>>,
    provenance_by_graph: &HashMap<Option<String>, SourceRef>,
) -> RuleSyntax {
    let quads = quads.cloned().unwrap_or_default();
    let kind = if quads.iter().any(|q| matches_named(&q.object, SH_TRIPLE_RULE)) {
        RuleSyntaxKind::Triple
    } else if quads.iter().any(|q| matches_named(&q.object, SH_SPARQL_RULE)) {
        RuleSyntaxKind::Sparql
    } else {
        RuleSyntaxKind::Generic
    };

    let grouped = group_predicates(&quads);
    let mut order = None;
    let mut deactivated = false;
    let mut properties = Vec::new();
    for (predicate, objects) in grouped {
        match predicate.as_str() {
            RDF_TYPE => {}
            SH_ORDER => {
                order = objects.first().and_then(parse_numeric_literal);
            }
            SH_DEACTIVATED => {
                deactivated = objects.iter().any(is_true_literal);
            }
            _ => properties.push(PredicateObjects { predicate, objects }),
        }
    }

    RuleSyntax {
        subject: subject.clone(),
        kind,
        order,
        deactivated,
        properties,
        provenance: provenance_for_quads(&quads, provenance_by_graph),
    }
}

fn provenance_map(resolved: &ResolvedShapeSet) -> HashMap<Option<String>, SourceRef> {
    let mut map = HashMap::new();
    for graph in resolved
        .root_graphs
        .iter()
        .chain(resolved.imported_graphs.iter())
    {
        map.insert(
            Some(graph.graph_iri.as_str().to_string()),
            graph.source.clone(),
        );
    }
    map
}

fn provenance_for_quads(
    quads: &[Quad],
    provenance_by_graph: &HashMap<Option<String>, SourceRef>,
) -> Vec<SourceRef> {
    let mut refs = HashSet::new();
    let mut out = Vec::new();
    for quad in quads {
        let key = match &quad.graph_name {
            GraphName::NamedNode(node) => Some(node.as_str().to_string()),
            _ => None,
        };
        if let Some(source) = provenance_by_graph.get(&key)
            && refs.insert(source.clone())
        {
            out.push(source.clone());
        }
    }
    out.sort_by_key(|source| source.graph_iri.clone());
    out
}

fn group_predicates(quads: &[Quad]) -> Vec<(NamedNode, Vec<Term>)> {
    let mut grouped: HashMap<NamedNode, Vec<Term>> = HashMap::new();
    for quad in quads {
        grouped
            .entry(quad.predicate.clone())
            .or_default()
            .push(quad.object.clone());
    }
    let mut entries: Vec<_> = grouped.into_iter().collect();
    entries.sort_by_key(|(predicate, _)| predicate.as_str().to_string());
    entries
}

fn is_constraint_predicate(predicate: &str) -> bool {
    predicate.starts_with("http://www.w3.org/ns/shacl#")
        && !matches!(
            predicate,
            SH_TARGET
                | SH_TARGET_CLASS
                | SH_TARGET_NODE
                | SH_TARGET_SUBJECTS_OF
                | SH_TARGET_OBJECTS_OF
                | SH_PATH
                | SH_PROPERTY
                | SH_RULE
                | SH_SEVERITY
                | SH_DEACTIVATED
                | SH_ORDER
        )
}

fn subject_to_term(subject: &NamedOrBlankNode) -> Term {
    match subject {
        NamedOrBlankNode::NamedNode(node) => Term::NamedNode(node.clone()),
        NamedOrBlankNode::BlankNode(node) => Term::BlankNode(node.clone()),
    }
}

fn matches_named(term: &Term, iri: &str) -> bool {
    matches!(term, Term::NamedNode(node) if node.as_str() == iri)
}

fn parse_numeric_literal(term: &Term) -> Option<f64> {
    match term {
        Term::Literal(lit) => lit.value().parse().ok(),
        _ => None,
    }
}

fn is_true_literal(term: &Term) -> bool {
    matches!(term, Term::Literal(lit) if lit.value().eq_ignore_ascii_case("true") || lit.value() == "1")
}
