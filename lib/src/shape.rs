use crate::context::{Context, ValidationContext};
use crate::report::ValidationReportBuilder;
use crate::types::{ComponentID, PropShapeID, ID};
use crate::types::{Path, Target};
// SHACL, Term, NamedNode, TermRef were unused

pub trait ValidateShape {
    fn validate(
        &self,
        context: &ValidationContext,
        rb: &mut ValidationReportBuilder,
    ) -> Result<(), String>;
}

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
    pub fn new(identifier: ID, targets: Vec<Target>, constraints: Vec<ComponentID>) -> Self {
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

    pub fn targets(&self) -> &[Target] {
        &self.targets
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
    pub fn sparql_path(&self) -> String {
        self.path.to_sparql_path().unwrap()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn constraints(&self) -> &[ComponentID] {
        &self.constraints
    }
}
