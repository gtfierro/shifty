use crate::algebra::{PropertyPath, Severity, ShapeProgram};
use crate::execute::ValidationResult;
use oxigraph::io::{RdfFormat, RdfSerializer};
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term, TripleRef};
use serde::{Deserialize, Serialize};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
const SH_VALIDATION_REPORT: &str = "http://www.w3.org/ns/shacl#ValidationReport";
const SH_VALIDATION_RESULT: &str = "http://www.w3.org/ns/shacl#ValidationResult";
const SH_CONFORMS: &str = "http://www.w3.org/ns/shacl#conforms";
const SH_RESULT: &str = "http://www.w3.org/ns/shacl#result";
const SH_FOCUS_NODE: &str = "http://www.w3.org/ns/shacl#focusNode";
const SH_VALUE: &str = "http://www.w3.org/ns/shacl#value";
const SH_RESULT_MESSAGE: &str = "http://www.w3.org/ns/shacl#resultMessage";
const SH_RESULT_PATH: &str = "http://www.w3.org/ns/shacl#resultPath";
const SH_RESULT_SEVERITY: &str = "http://www.w3.org/ns/shacl#resultSeverity";
const SH_SOURCE_SHAPE: &str = "http://www.w3.org/ns/shacl#sourceShape";
const SH_SOURCE_CONSTRAINT: &str = "http://www.w3.org/ns/shacl#sourceConstraint";
const SH_SOURCE_CONSTRAINT_COMPONENT: &str = "http://www.w3.org/ns/shacl#sourceConstraintComponent";
const SH_INVERSE_PATH: &str = "http://www.w3.org/ns/shacl#inversePath";
const SH_ALTERNATIVE_PATH: &str = "http://www.w3.org/ns/shacl#alternativePath";
const SH_ZERO_OR_MORE_PATH: &str = "http://www.w3.org/ns/shacl#zeroOrMorePath";
const SH_ONE_OR_MORE_PATH: &str = "http://www.w3.org/ns/shacl#oneOrMorePath";
const SH_ZERO_OR_ONE_PATH: &str = "http://www.w3.org/ns/shacl#zeroOrOnePath";
const SH_INFO: &str = "http://www.w3.org/ns/shacl#Info";
const SH_WARNING: &str = "http://www.w3.org/ns/shacl#Warning";
const SH_VIOLATION: &str = "http://www.w3.org/ns/shacl#Violation";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReportGraph {
    pub report_node: Term,
    pub quads: Vec<Quad>,
}

impl ValidationReportGraph {
    pub fn serialize(&self, format: RdfFormat) -> Result<String, String> {
        let mut writer = Vec::new();
        let mut serializer = RdfSerializer::from_format(format)
            .with_prefix("sh", "http://www.w3.org/ns/shacl#")
            .map_err(|error| error.to_string())?
            .with_prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
            .map_err(|error| error.to_string())?
            .with_prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
            .map_err(|error| error.to_string())?
            .for_writer(&mut writer);
        for quad in &self.quads {
            serializer
                .serialize_triple(TripleRef::new(
                    quad.subject.as_ref(),
                    quad.predicate.as_ref(),
                    quad.object.as_ref(),
                ))
                .map_err(|error| error.to_string())?;
        }
        serializer.finish().map_err(|error| error.to_string())?;
        String::from_utf8(writer).map_err(|error| error.to_string())
    }
}

pub fn build_validation_report(
    result: &ValidationResult,
    program: &ShapeProgram,
) -> ValidationReportGraph {
    let mut quads = Vec::new();
    let report_node = fresh_blank_term(&quads);
    let report_subject = term_to_subject(&report_node).expect("report node must be a subject");
    push_quad(
        &mut quads,
        report_subject.clone(),
        RDF_TYPE,
        Term::NamedNode(named_node(SH_VALIDATION_REPORT)),
    );
    push_quad(
        &mut quads,
        report_subject.clone(),
        SH_CONFORMS,
        Term::Literal(Literal::from(result.conforms)),
    );
    for violation in &result.violations {
        let result_node = fresh_blank_term(&quads);
        let result_subject = term_to_subject(&result_node).expect("result node must be a subject");
        push_quad(
            &mut quads,
            report_subject.clone(),
            SH_RESULT,
            result_node.clone(),
        );
        push_quad(
            &mut quads,
            result_subject.clone(),
            RDF_TYPE,
            Term::NamedNode(named_node(SH_VALIDATION_RESULT)),
        );
        push_quad(
            &mut quads,
            result_subject.clone(),
            SH_FOCUS_NODE,
            violation.focus.clone(),
        );
        if let Some(value) = &violation.value {
            push_quad(&mut quads, result_subject.clone(), SH_VALUE, value.clone());
        }
        push_quad(
            &mut quads,
            result_subject.clone(),
            SH_RESULT_MESSAGE,
            Term::Literal(Literal::new_simple_literal(violation.message.clone())),
        );
        push_quad(
            &mut quads,
            result_subject.clone(),
            SH_RESULT_SEVERITY,
            severity_term(&violation.severity),
        );
        if let Some(shape) = violation
            .source_shape
            .as_ref()
            .and_then(term_to_subject_term)
        {
            push_quad(&mut quads, result_subject.clone(), SH_SOURCE_SHAPE, shape);
        }
        if let Some(constraint) = violation
            .source_constraint
            .as_ref()
            .and_then(term_to_subject_term)
        {
            push_quad(
                &mut quads,
                result_subject.clone(),
                SH_SOURCE_CONSTRAINT,
                constraint,
            );
        }
        if let Some(component) = violation
            .source_constraint_component
            .as_ref()
            .and_then(term_to_subject_term)
        {
            push_quad(
                &mut quads,
                result_subject.clone(),
                SH_SOURCE_CONSTRAINT_COMPONENT,
                component,
            );
        }
        if let Some(path_term) = violation
            .result_path
            .clone()
            .or_else(|| report_path_for_shape(program, violation.shape, &mut quads))
        {
            push_quad(&mut quads, result_subject, SH_RESULT_PATH, path_term);
        }
    }
    ValidationReportGraph { report_node, quads }
}

fn report_path_for_shape(
    program: &ShapeProgram,
    shape_id: crate::algebra::ShapeId,
    quads: &mut Vec<Quad>,
) -> Option<Term> {
    let shape = program.shapes.iter().find(|shape| shape.id == shape_id)?;
    let path = shape.path.as_ref()?;
    Some(property_path_term(path, quads))
}

fn property_path_term(path: &PropertyPath, quads: &mut Vec<Quad>) -> Term {
    match path {
        PropertyPath::Predicate(predicate) => Term::NamedNode(predicate.clone()),
        PropertyPath::Inverse(inner) => {
            let node = fresh_blank_subject(quads);
            let inner_term = property_path_term(inner, quads);
            push_quad(quads, node.clone(), SH_INVERSE_PATH, inner_term);
            Term::BlankNode(blank_from_subject(&node))
        }
        PropertyPath::Sequence(parts) => rdf_list(
            parts
                .iter()
                .map(|part| property_path_term(part, quads))
                .collect(),
            quads,
        ),
        PropertyPath::Alternative(parts) => {
            let node = fresh_blank_subject(quads);
            let list = rdf_list(
                parts
                    .iter()
                    .map(|part| property_path_term(part, quads))
                    .collect(),
                quads,
            );
            push_quad(quads, node.clone(), SH_ALTERNATIVE_PATH, list);
            Term::BlankNode(blank_from_subject(&node))
        }
        PropertyPath::ZeroOrMore(inner) => {
            let node = fresh_blank_subject(quads);
            let inner_term = property_path_term(inner, quads);
            push_quad(quads, node.clone(), SH_ZERO_OR_MORE_PATH, inner_term);
            Term::BlankNode(blank_from_subject(&node))
        }
        PropertyPath::OneOrMore(inner) => {
            let node = fresh_blank_subject(quads);
            let inner_term = property_path_term(inner, quads);
            push_quad(quads, node.clone(), SH_ONE_OR_MORE_PATH, inner_term);
            Term::BlankNode(blank_from_subject(&node))
        }
        PropertyPath::ZeroOrOne(inner) => {
            let node = fresh_blank_subject(quads);
            let inner_term = property_path_term(inner, quads);
            push_quad(quads, node.clone(), SH_ZERO_OR_ONE_PATH, inner_term);
            Term::BlankNode(blank_from_subject(&node))
        }
        PropertyPath::Unsupported(term) => term.clone(),
    }
}

fn rdf_list(values: Vec<Term>, quads: &mut Vec<Quad>) -> Term {
    if values.is_empty() {
        return Term::NamedNode(named_node(RDF_NIL));
    }
    let head = fresh_blank_subject(quads);
    let mut current = head.clone();
    let len = values.len();
    for (index, value) in values.into_iter().enumerate() {
        push_quad(quads, current.clone(), RDF_FIRST, value);
        if index + 1 == len {
            push_quad(
                quads,
                current.clone(),
                RDF_REST,
                Term::NamedNode(named_node(RDF_NIL)),
            );
        } else {
            let next = fresh_blank_subject(quads);
            push_quad(
                quads,
                current.clone(),
                RDF_REST,
                Term::BlankNode(blank_from_subject(&next)),
            );
            current = next;
        }
    }
    Term::BlankNode(blank_from_subject(&head))
}

fn severity_term(severity: &Severity) -> Term {
    match severity {
        Severity::Info => Term::NamedNode(named_node(SH_INFO)),
        Severity::Warning => Term::NamedNode(named_node(SH_WARNING)),
        Severity::Violation => Term::NamedNode(named_node(SH_VIOLATION)),
        Severity::Custom(term) => term.clone(),
    }
}

fn push_quad(quads: &mut Vec<Quad>, subject: NamedOrBlankNode, predicate: &str, object: Term) {
    quads.push(Quad::new(
        subject,
        named_node(predicate),
        object,
        GraphName::DefaultGraph,
    ));
}

fn named_node(iri: &str) -> NamedNode {
    NamedNode::new(iri).expect("static IRI must be valid")
}

fn fresh_blank_term(quads: &[Quad]) -> Term {
    Term::BlankNode(fresh_blank_node(quads))
}

fn fresh_blank_subject(quads: &[Quad]) -> NamedOrBlankNode {
    NamedOrBlankNode::BlankNode(fresh_blank_node(quads))
}

fn fresh_blank_node(quads: &[Quad]) -> BlankNode {
    BlankNode::new(format!("b{}", quads.len())).expect("generated blank node id must be valid")
}

fn blank_from_subject(subject: &NamedOrBlankNode) -> BlankNode {
    match subject {
        NamedOrBlankNode::BlankNode(blank) => blank.clone(),
        NamedOrBlankNode::NamedNode(_) => panic!("expected blank-node subject"),
    }
}

fn term_to_subject(term: &Term) -> Option<NamedOrBlankNode> {
    match term {
        Term::NamedNode(node) => Some(NamedOrBlankNode::NamedNode(node.clone())),
        Term::BlankNode(node) => Some(NamedOrBlankNode::BlankNode(node.clone())),
        _ => None,
    }
}

fn term_to_subject_term(term: &Term) -> Option<Term> {
    match term {
        Term::NamedNode(_) | Term::BlankNode(_) => Some(term.clone()),
        _ => None,
    }
}
