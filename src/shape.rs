use crate::types::{ID, ComponentID};
use crate::types::{Path, Target};
use crate::named_nodes::SHACL;
use oxigraph::model::{Term, NamedNode, TermRef};

pub enum Shape {
    NodeShape(NodeShape),
    PropertyShape(PropertyShape),
}

pub struct NodeShape {
    identifier: ID,
    targets: Vec<Target>,
    property_shapes: Vec<ID>,
    constraints: Vec<ComponentID>,
    // TODO severity
    // TODO message
}

pub struct PropertyShape {
    identifier: ID,
    path: Path,
    constraints: Vec<ID>,
    // TODO severity
    // TODO message
}
