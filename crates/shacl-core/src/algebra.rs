use crate::diagnostics::{Diagnostic, InspectionGraph, SourceRef};
use oxrdf::{NamedNode, Term};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ShapeId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ConstraintId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct RuleId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct TargetId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShapeKind {
    Node,
    Property,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Severity {
    Info,
    Warning,
    Violation,
    Custom(Term),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetExpr {
    Class(Term),
    Node(Term),
    SubjectsOf(Term),
    ObjectsOf(Term),
    Advanced {
        node: Term,
        select: Option<String>,
        ask: Option<String>,
        target_shape: Option<Term>,
        filter_shape: Option<Term>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PropertyPath {
    Predicate(NamedNode),
    Inverse(Box<PropertyPath>),
    Sequence(Vec<PropertyPath>),
    Alternative(Vec<PropertyPath>),
    ZeroOrMore(Box<PropertyPath>),
    OneOrMore(Box<PropertyPath>),
    ZeroOrOne(Box<PropertyPath>),
    Unsupported(Term),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalKind {
    And,
    Or,
    Xone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConstraintExpr {
    NodeRef {
        shape: Option<ShapeId>,
        source: Term,
    },
    PropertyRef {
        shape: Option<ShapeId>,
        source: Term,
    },
    QualifiedValueShape {
        shape: Option<ShapeId>,
        source: Term,
        min_count: Option<u64>,
        max_count: Option<u64>,
        disjoint: Option<bool>,
    },
    Logical {
        kind: LogicalKind,
        shapes: Vec<ShapeId>,
    },
    Not {
        shape: Option<ShapeId>,
        source: Term,
    },
    Class(Term),
    Datatype(Term),
    NodeKind(Term),
    Cardinality {
        predicate: NamedNode,
        min: Option<u64>,
        max: Option<u64>,
    },
    NumericRange {
        predicate: NamedNode,
        values: Vec<Term>,
    },
    StringConstraint {
        predicate: NamedNode,
        values: Vec<Term>,
    },
    PropertyComparison {
        predicate: NamedNode,
        values: Vec<Term>,
    },
    Closed {
        ignored_properties: Vec<Term>,
    },
    HasValue(Term),
    In(Vec<Term>),
    Sparql {
        node: Term,
    },
    GenericPredicate {
        predicate: NamedNode,
        values: Vec<Term>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriplePatternTerm {
    This,
    Constant(Term),
    Path(PropertyPath),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleExpr {
    Triple {
        node: Term,
        subject: Option<TriplePatternTerm>,
        predicate: Option<NamedNode>,
        object: Option<TriplePatternTerm>,
        conditions: Vec<ShapeId>,
        order: Option<String>,
    },
    Sparql {
        node: Term,
        query: Option<String>,
        conditions: Vec<ShapeId>,
        order: Option<String>,
    },
    Generic {
        node: Term,
        conditions: Vec<ShapeId>,
        order: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FeatureUse {
    Core,
    AdvancedTargets,
    Rules,
    Sparql,
    Templates,
    CustomComponents,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shape {
    pub id: ShapeId,
    pub source: Term,
    pub kind: ShapeKind,
    pub targets: Vec<TargetId>,
    pub constraints: Vec<ConstraintId>,
    pub property_shapes: Vec<ShapeId>,
    pub path: Option<PropertyPath>,
    pub severity: Severity,
    pub deactivated: bool,
    pub provenance: Vec<SourceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Constraint {
    pub id: ConstraintId,
    pub owner: ShapeId,
    pub expr: ConstraintExpr,
    pub provenance: Vec<SourceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
    pub id: TargetId,
    pub owner: ShapeId,
    pub expr: TargetExpr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    pub id: RuleId,
    pub owner: ShapeId,
    pub expr: RuleExpr,
    pub deactivated: bool,
    pub provenance: Vec<SourceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyEdge {
    pub from: ShapeId,
    pub to: ShapeId,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeProgram {
    pub shapes: Vec<Shape>,
    pub constraints: Vec<Constraint>,
    pub targets: Vec<Target>,
    pub rules: Vec<Rule>,
    pub dependencies: Vec<DependencyEdge>,
    pub source_inventory: Vec<SourceRef>,
    pub features: Vec<FeatureUse>,
    pub diagnostics: Vec<Diagnostic>,
    pub inspection: InspectionGraph,
    pub shape_index: HashMap<String, ShapeId>,
}
