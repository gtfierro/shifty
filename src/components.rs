use oxigraph::model::{Term, NamedNode, BlankNode, Literal};
use oxigraph::model::{Dataset, Graph, GraphName, Quad, Triple};
use crate::types::ID;

pub fn parse_components(start: Term, shape_graph: &Graph) -> Vec<Component> {
    vec![]
}

pub enum Component {
    ClassConstraint(ClassConstraintComponent),
    NodeConstraint(NodeConstraintComponent),
    PropertyConstraint(PropertyConstraintComponent),
    QualifiedValueShape(QualifiedValueShapeComponent),
}

pub struct ClassConstraintComponent {
    identifier: ID,
    class: Term,
}

pub struct NodeConstraintComponent {
    identifier: ID,
    shape: ID,
}

pub struct PropertyConstraintComponent {
    identifier: ID,
    shape: ID,
}

pub struct QualifiedValueShapeComponent {
    identifier: ID,
    shape: ID,
    min_count: Option<u64>,
    max_count: Option<u64>,
    disjoint: Option<bool>,
}
