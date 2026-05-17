use crate::diagnostics::{Diagnostic, SourceRef};
use oxrdf::{NamedNode, Term};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShapeSyntaxKind {
    NodeShape,
    PropertyShape,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetSyntax {
    Class(Term),
    Node(Term),
    SubjectsOf(Term),
    ObjectsOf(Term),
    Advanced(Term),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredicateObjects {
    pub predicate: NamedNode,
    pub objects: Vec<Term>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintSyntax {
    pub predicate: NamedNode,
    pub objects: Vec<Term>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleSyntaxKind {
    Triple,
    Sparql,
    Generic,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuleSyntax {
    pub subject: Term,
    pub kind: RuleSyntaxKind,
    pub order: Option<f64>,
    pub deactivated: bool,
    pub properties: Vec<PredicateObjects>,
    pub provenance: Vec<SourceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShapeSyntax {
    pub subject: Term,
    pub kind: ShapeSyntaxKind,
    pub targets: Vec<TargetSyntax>,
    pub property_shapes: Vec<Term>,
    pub path: Option<Term>,
    pub severity: Option<Term>,
    pub deactivated: bool,
    pub constraints: Vec<ConstraintSyntax>,
    pub rule_nodes: Vec<Term>,
    pub extras: Vec<PredicateObjects>,
    pub provenance: Vec<SourceRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeSyntaxDocument {
    pub shapes: Vec<ShapeSyntax>,
    pub rules: Vec<RuleSyntax>,
    pub quads: Vec<oxrdf::Quad>,
    pub sources: Vec<SourceRef>,
    pub diagnostics: Vec<Diagnostic>,
}
