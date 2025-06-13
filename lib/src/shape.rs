use crate::context::{Context, ValidationContext};
use crate::report::ValidationReportBuilder;
use crate::types::{ComponentID, PropShapeID, Severity, ID};
use crate::types::{Path, Target};

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
    pub targets: Vec<Target>,
    constraints: Vec<ComponentID>,
    severity: Severity,
    // TODO message
}

impl NodeShape {
    pub fn new(
        identifier: ID,
        targets: Vec<Target>,
        constraints: Vec<ComponentID>,
        severity: Option<Severity>,
    ) -> Self {
        NodeShape {
            identifier,
            targets,
            constraints,
            severity: severity.unwrap_or_default(),
        }
    }

    pub fn identifier(&self) -> &ID {
        &self.identifier
    }

    pub fn constraints(&self) -> &[ComponentID] {
        &self.constraints
    }

    pub fn severity(&self) -> Severity {
        self.severity
    }
}

#[derive(Debug)]
pub struct PropertyShape {
    identifier: PropShapeID,
    path: Path,
    constraints: Vec<ComponentID>,
    severity: Severity,
    // TODO message
}

impl PropertyShape {
    pub fn new(
        identifier: PropShapeID,
        path: Path,
        constraints: Vec<ComponentID>,
        severity: Option<Severity>,
    ) -> Self {
        PropertyShape {
            identifier,
            path,
            constraints,
            severity: severity.unwrap_or_default(),
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

    pub fn severity(&self) -> Severity {
        self.severity
    }
}
