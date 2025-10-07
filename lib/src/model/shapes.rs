use crate::types::{ComponentID, Path, PropShapeID, Severity, Target, ID};
use oxigraph::model::Term;

/// Immutable description of a SHACL node shape.
#[derive(Debug)]
pub struct NodeShape {
    identifier: ID,
    /// Target selectors identifying candidate focus nodes.
    pub targets: Vec<Target>,
    constraints: Vec<ComponentID>,
    severity: Severity,
    deactivated: bool,
}

impl NodeShape {
    pub fn new(
        identifier: ID,
        targets: Vec<Target>,
        constraints: Vec<ComponentID>,
        severity: Option<Severity>,
        deactivated: bool,
    ) -> Self {
        NodeShape {
            identifier,
            targets,
            constraints,
            severity: severity.unwrap_or_default(),
            deactivated,
        }
    }

    pub fn identifier(&self) -> &ID {
        &self.identifier
    }

    pub fn constraints(&self) -> &[ComponentID] {
        &self.constraints
    }

    pub fn severity(&self) -> &Severity {
        &self.severity
    }

    pub fn is_deactivated(&self) -> bool {
        self.deactivated
    }
}

/// Immutable description of a SHACL property shape.
#[derive(Debug)]
pub struct PropertyShape {
    identifier: PropShapeID,
    /// Target selectors identifying candidate focus nodes.
    pub targets: Vec<Target>,
    path: Path,
    path_term: Term,
    constraints: Vec<ComponentID>,
    severity: Severity,
    deactivated: bool,
}

impl PropertyShape {
    pub fn new(
        identifier: PropShapeID,
        targets: Vec<Target>,
        path: Path,
        path_term: Term,
        constraints: Vec<ComponentID>,
        severity: Option<Severity>,
        deactivated: bool,
    ) -> Self {
        PropertyShape {
            identifier,
            targets,
            path,
            path_term,
            constraints,
            severity: severity.unwrap_or_default(),
            deactivated,
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

    pub fn path_term(&self) -> &Term {
        &self.path_term
    }

    pub fn constraints(&self) -> &[ComponentID] {
        &self.constraints
    }

    pub fn severity(&self) -> &Severity {
        &self.severity
    }

    pub fn is_deactivated(&self) -> bool {
        self.deactivated
    }
}
