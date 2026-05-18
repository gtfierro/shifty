use crate::diagnostics::{Diagnostic, SourceRef};
use oxrdf::{NamedNode, Term};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShapeSyntaxKind {
    NodeShape,
    PropertyShape,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefixDeclarationSyntax {
    pub node: Term,
    pub prefix: Option<String>,
    pub namespace: Option<Term>,
    pub extras: Vec<PredicateObjects>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdvancedTargetSyntax {
    pub node: Term,
    pub select: Option<String>,
    pub ask: Option<String>,
    pub target_shape: Option<Term>,
    pub filter_shape: Option<Term>,
    pub prefixes: Vec<Term>,
    pub declarations: Vec<PrefixDeclarationSyntax>,
    pub extras: Vec<PredicateObjects>,
    pub provenance: Vec<SourceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetSyntax {
    Class(Term),
    Node(Term),
    SubjectsOf(Term),
    ObjectsOf(Term),
    Advanced(AdvancedTargetSyntax),
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
pub struct ParameterSyntax {
    pub node: Term,
    pub path: Option<Term>,
    pub datatype: Option<Term>,
    pub var_name: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub optional: bool,
    pub default_values: Vec<Term>,
    pub extras: Vec<PredicateObjects>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SparqlValidatorSyntax {
    pub node: Term,
    pub kind: Option<Term>,
    pub select: Option<String>,
    pub ask: Option<String>,
    pub messages: Vec<Term>,
    pub prefixes: Vec<Term>,
    pub declarations: Vec<PrefixDeclarationSyntax>,
    pub extras: Vec<PredicateObjects>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintComponentSyntax {
    pub subject: Term,
    pub parameters: Vec<ParameterSyntax>,
    pub validators: Vec<SparqlValidatorSyntax>,
    pub messages: Vec<Term>,
    pub prefixes: Vec<Term>,
    pub declarations: Vec<PrefixDeclarationSyntax>,
    pub label: Option<String>,
    pub label_template: Option<String>,
    pub comment: Option<String>,
    pub extras: Vec<PredicateObjects>,
    pub provenance: Vec<SourceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SparqlConstraintSyntax {
    pub node: Term,
    pub kind: Option<Term>,
    pub select: Option<String>,
    pub ask: Option<String>,
    pub messages: Vec<Term>,
    pub prefixes: Vec<Term>,
    pub declarations: Vec<PrefixDeclarationSyntax>,
    pub extras: Vec<PredicateObjects>,
    pub provenance: Vec<SourceRef>,
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
    pub sparql_constraints: Vec<SparqlConstraintSyntax>,
    pub rule_nodes: Vec<Term>,
    pub extras: Vec<PredicateObjects>,
    pub provenance: Vec<SourceRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeSyntaxDocument {
    pub shapes: Vec<ShapeSyntax>,
    pub rules: Vec<RuleSyntax>,
    pub constraint_components: Vec<ConstraintComponentSyntax>,
    pub quads: Vec<oxrdf::Quad>,
    pub sources: Vec<SourceRef>,
    pub diagnostics: Vec<Diagnostic>,
}
