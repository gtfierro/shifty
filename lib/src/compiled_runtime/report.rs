use crate::compiled_runtime::program::CompiledProgram;
use crate::named_nodes::SHACL;
use oxigraph::io::{RdfFormat, RdfSerializer};
use oxigraph::model::vocab::rdf;
use oxigraph::model::{Graph, NamedOrBlankNode, Term};
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct CompactReport {
    pub conforms: bool,
    pub violations: Vec<CompactViolation>,
}

#[derive(Debug, Clone)]
pub struct CompactViolation {
    pub shape_id: u64,
    pub component_id: u64,
    pub focus: Term,
    pub value: Option<Term>,
    pub path: Option<CompactResultPath>,
}

#[derive(Debug, Clone)]
pub enum CompactResultPath {
    Term(Term),
    PathId(u64),
}

pub fn compact_from_graph(graph: &Graph, program: &CompiledProgram) -> CompactReport {
    let sh = SHACL::new();

    let shape_lookup: HashMap<Term, u64> = program
        .static_hints
        .shape_id_to_term
        .iter()
        .filter_map(|row| program.term(row.term).cloned().map(|term| (term, row.id)))
        .collect();

    let component_lookup: HashMap<Term, u64> = program
        .static_hints
        .component_id_to_iri_term
        .iter()
        .filter_map(|row| program.term(row.term).cloned().map(|term| (term, row.id)))
        .collect();

    let path_lookup: HashMap<Term, u64> = program
        .static_hints
        .path_id_to_term
        .iter()
        .filter_map(|row| program.term(row.term).cloned().map(|term| (term, row.id)))
        .collect();

    let Some(report_node) = graph
        .subjects_for_predicate_object(rdf::TYPE, Term::from(sh.validation_report).as_ref())
        .next()
        .map(|subject| subject.into_owned())
    else {
        return CompactReport::default();
    };

    let conforms = graph
        .objects_for_subject_predicate(report_node.as_ref(), sh.conforms)
        .next()
        .and_then(|term| match term {
            oxigraph::model::TermRef::Literal(lit) => lit.value().parse::<bool>().ok(),
            _ => None,
        })
        .unwrap_or(true);

    let mut violations = Vec::new();
    for result_term in graph.objects_for_subject_predicate(report_node.as_ref(), sh.result) {
        let result_term = result_term.into_owned();
        let result_subject = match &result_term {
            Term::NamedNode(node) => NamedOrBlankNode::NamedNode(node.clone()),
            Term::BlankNode(node) => NamedOrBlankNode::BlankNode(node.clone()),
            _ => continue,
        };

        let focus = graph
            .objects_for_subject_predicate(result_subject.as_ref(), sh.focus_node)
            .next()
            .map(|term| term.into_owned())
            .unwrap_or_else(|| result_term.clone());

        let source_shape = graph
            .objects_for_subject_predicate(result_subject.as_ref(), sh.source_shape)
            .next()
            .map(|term| term.into_owned());

        let source_component = graph
            .objects_for_subject_predicate(result_subject.as_ref(), sh.source_constraint_component)
            .next()
            .map(|term| term.into_owned());

        let value = graph
            .objects_for_subject_predicate(result_subject.as_ref(), sh.value)
            .next()
            .map(|term| term.into_owned());

        let path = graph
            .objects_for_subject_predicate(result_subject.as_ref(), sh.result_path)
            .next()
            .map(|term| term.into_owned())
            .map(|term| match path_lookup.get(&term).copied() {
                Some(path_id) => CompactResultPath::PathId(path_id),
                None => CompactResultPath::Term(term),
            });

        let shape_id = source_shape
            .as_ref()
            .and_then(|term| shape_lookup.get(term).copied())
            .unwrap_or(0);
        let component_id = source_component
            .as_ref()
            .and_then(|term| component_lookup.get(term).copied())
            .unwrap_or(0);

        violations.push(CompactViolation {
            shape_id,
            component_id,
            focus,
            value,
            path,
        });
    }

    CompactReport {
        conforms,
        violations,
    }
}

pub fn graph_to_turtle(graph: &Graph) -> Result<String, String> {
    let mut serializer = RdfSerializer::from_format(RdfFormat::Turtle).for_writer(Vec::new());
    for triple in graph {
        serializer
            .serialize_triple(triple)
            .map_err(|err| format!("failed to serialize report triple: {err}"))?;
    }
    let bytes = serializer
        .finish()
        .map_err(|err| format!("failed to finish report serialization: {err}"))?;
    String::from_utf8(bytes).map_err(|err| format!("report is not valid utf-8: {err}"))
}
