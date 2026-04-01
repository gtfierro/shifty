use crate::compiled_runtime::program::CompiledProgram;
use oxigraph::model::{NamedNode, NamedOrBlankNode, Term};

pub fn term_as_named_node(program: &CompiledProgram, term_id: u64) -> Result<NamedNode, String> {
    let term = program
        .term(term_id)
        .ok_or_else(|| format!("missing term id {}", term_id))?;
    match term {
        Term::NamedNode(node) => Ok(node.clone()),
        _ => Err(format!("term id {} is not an IRI", term_id)),
    }
}

pub fn term_as_subject(
    program: &CompiledProgram,
    term_id: u64,
) -> Result<NamedOrBlankNode, String> {
    let term = program
        .term(term_id)
        .ok_or_else(|| format!("missing term id {}", term_id))?;
    match term {
        Term::NamedNode(node) => Ok(NamedOrBlankNode::NamedNode(node.clone())),
        Term::BlankNode(node) => Ok(NamedOrBlankNode::BlankNode(node.clone())),
        _ => Err(format!("term id {} cannot be used as RDF subject", term_id)),
    }
}

pub fn term(program: &CompiledProgram, term_id: u64) -> Result<Term, String> {
    program
        .term(term_id)
        .cloned()
        .ok_or_else(|| format!("missing term id {}", term_id))
}
