use crate::types::{ComponentID, PropShapeID, ID};
use crate::types::{Path, Target};
// SHACL, Term, NamedNode, TermRef were unused

#[derive(Debug)]
pub enum Shape {
    NodeShape(NodeShape),
    PropertyShape(PropertyShape),
}

#[derive(Debug)]
pub struct NodeShape {
    identifier: ID,
    targets: Vec<Target>,
    constraints: Vec<ComponentID>,
    // TODO severity
    // TODO message
}

impl NodeShape {
    pub fn new(
        identifier: ID,
        targets: Vec<Target>,
        constraints: Vec<ComponentID>,
    ) -> Self {
        NodeShape {
            identifier,
            targets,
            constraints,
        }
    }

    pub fn identifier(&self) -> &ID {
        &self.identifier
    }

    pub fn constraints(&self) -> &[ComponentID] {
        &self.constraints
    }
}

#[derive(Debug)]
pub struct PropertyShape {
    identifier: PropShapeID,
    path: Path,
    constraints: Vec<ComponentID>,
    // TODO severity
    // TODO message
}

impl PropertyShape {
    pub fn new(identifier: PropShapeID, path: Path, constraints: Vec<ComponentID>) -> Self {
        PropertyShape {
            identifier,
            path,
            constraints,
        }
    }
    pub fn identifier(&self) -> &PropShapeID {
        &self.identifier
    }
    pub fn path(&self) -> String {
        match &self.path {
            Path::Simple(t) => format!("{}", t),
        }
    }
    pub fn constraints(&self) -> &[ComponentID] {
        &self.constraints
    }
}
