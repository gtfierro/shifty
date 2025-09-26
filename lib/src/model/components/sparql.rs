use std::collections::HashMap;

use oxigraph::model::{NamedNode, Term};

/// Parameter definition for a custom SPARQL-based constraint component.
#[derive(Debug, Clone)]
pub struct Parameter {
    pub path: NamedNode,
    pub optional: bool,
}

/// Represents a SPARQL validator (either ASK or SELECT) defined on a custom constraint component.
#[derive(Debug, Clone)]
pub struct SPARQLValidator {
    pub query: String,
    pub is_ask: bool,
    pub messages: Vec<Term>,
    pub prefixes: String,
}

/// Metadata that defines a SHACL custom constraint component.
#[derive(Debug, Clone)]
pub struct CustomConstraintComponentDefinition {
    pub iri: NamedNode,
    pub parameters: Vec<Parameter>,
    pub validator: Option<SPARQLValidator>,
    pub node_validator: Option<SPARQLValidator>,
    pub property_validator: Option<SPARQLValidator>,
}

/// Concrete values bound to a parameterized SPARQL validator during parsing time.
pub type ParameterBindings = HashMap<NamedNode, Vec<Term>>;
