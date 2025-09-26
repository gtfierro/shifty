use crate::types::{ComponentID, Path, PropShapeID, Severity, Target, ID};

/// Immutable description of a SHACL node shape.
#[derive(Debug)]
pub struct NodeShape {
    identifier: ID,
    /// Target selectors identifying candidate focus nodes.
    pub targets: Vec<Target>,
    constraints: Vec<ComponentID>,
    severity: Severity,
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

/// Immutable description of a SHACL property shape.
#[derive(Debug)]
pub struct PropertyShape {
    identifier: PropShapeID,
    /// Target selectors identifying candidate focus nodes.
    pub targets: Vec<Target>,
    path: Path,
    constraints: Vec<ComponentID>,
    severity: Severity,
}

impl PropertyShape {
    pub fn new(
        identifier: PropShapeID,
        targets: Vec<Target>,
        path: Path,
        constraints: Vec<ComponentID>,
        severity: Option<Severity>,
    ) -> Self {
        PropertyShape {
            identifier,
            targets,
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
