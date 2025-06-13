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

    /// Validates a focus node against this property shape.
    /// This is assumed to be called from a component validator (e.g. sh:property).
    /// The `parent_context` is the context of the shape containing the component.
    pub fn validate(
        &self,
        parent_context: &mut Context,
        _validation_context: &ValidationContext,
        _rb: &mut ValidationReportBuilder,
    ) -> Result<(), String> {
        // When validating a property shape, we create a new context for its constraints.
        // This new context has a `result_path` which is the path of this property shape.
        let mut prop_context = parent_context.clone();
        prop_context.result_path = Some(self.path.clone());

        // A full implementation would then:
        // 1. Calculate value nodes based on `self.path` and `parent_context.focus_node()`.
        // 2. Set `prop_context.value_nodes` with the calculated value nodes.
        // 3. Iterate over `self.constraints()` and validate each component using `prop_context`.
        // This logic is omitted as it is beyond the scope of the request.

        Ok(())
    }
}
