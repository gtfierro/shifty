use crate::context::ValidationContext;
use crate::report::ValidationReportBuilder;
use crate::types::{ComponentID, PropShapeID, Severity, ID};
use crate::types::{Path, Target};

/// A trait for shapes that can be validated.
pub(crate) trait ValidateShape {
    /// Performs validation of the shape against the data in the `ValidationContext`.
    /// found by the shape's targets.
    ///
    /// # Arguments
    ///
    /// * `context` - The current `ValidationContext`.
    /// * `rb` - A mutable `ValidationReportBuilder` to collect validation results.
    fn process_targets(
        &self,
        context: &ValidationContext,
        rb: &mut ValidationReportBuilder,
    ) -> Result<(), String>;
}

/// An enum that can hold either a `NodeShape` or a `PropertyShape`.
#[derive(Debug)]
pub(crate) enum Shape {
    /// A node shape.
    NodeShape(NodeShape),
    /// A property shape.
    PropertyShape(PropertyShape),
}

/// Represents a SHACL Node Shape.
#[derive(Debug)]
pub struct NodeShape {
    identifier: ID,
    /// The targets that specify which nodes this shape applies to.
    pub targets: Vec<Target>,
    constraints: Vec<ComponentID>,
    severity: Severity,
    // TODO message
}

impl NodeShape {
    /// Creates a new `NodeShape`.
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

    /// Returns the unique identifier of the node shape.
    pub fn identifier(&self) -> &ID {
        &self.identifier
    }

    /// Returns a slice of the component IDs that are constraints on this node shape.
    pub fn constraints(&self) -> &[ComponentID] {
        &self.constraints
    }

    /// Returns the severity level for this node shape.
    pub fn severity(&self) -> Severity {
        self.severity
    }

    /// Retrieves the name (Term) of the node shape from the validation context.
    pub(crate) fn name(&self, context: &ValidationContext) -> String {
        context
            .nodeshape_id_lookup()
            .borrow()
            .get_term(*self.identifier())
            .unwrap()
            .to_string()
    }
}

/// Represents a SHACL Property Shape.
#[derive(Debug)]
pub struct PropertyShape {
    identifier: PropShapeID,
    path: Path,
    constraints: Vec<ComponentID>,
    severity: Severity,
    // TODO message
}

impl PropertyShape {
    /// Creates a new `PropertyShape`.
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
    /// Returns the unique identifier of the property shape.
    pub fn identifier(&self) -> &PropShapeID {
        &self.identifier
    }
    /// Returns the SPARQL property path representation of the shape's `sh:path`.
    pub fn sparql_path(&self) -> String {
        self.path.to_sparql_path().unwrap()
    }

    /// Returns a reference to the `Path` of this property shape.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns a slice of the component IDs that are constraints on this property shape.
    pub fn constraints(&self) -> &[ComponentID] {
        &self.constraints
    }

    /// Returns the severity level for this property shape.
    pub fn severity(&self) -> Severity {
        self.severity
    }
    /// Retrieves the name (Term) of the property shape from the validation context.
    pub(crate) fn name(&self, context: &ValidationContext) -> String {
        context
            .propshape_id_lookup()
            .borrow()
            .get_term(*self.identifier())
            .unwrap()
            .to_string()
    }
}
