use crate::types::{ComponentID, Path, PropShapeID, Severity, Target, ID};
use oxigraph::model::Term;

/// Immutable description of a SHACL node shape.
///
/// This is the parsed, data-only representation used by the runtime. It does not
/// execute validation itself; instead it provides the identifiers, targets, and
/// constraint IDs needed by the validation engine.
#[derive(Debug)]
pub struct NodeShape {
    identifier: ID,
    /// Target selectors identifying candidate focus nodes.
    pub targets: Vec<Target>,
    constraints: Vec<ComponentID>,
    property_shapes: Vec<PropShapeID>,
    severity: Severity,
    deactivated: bool,
}

impl NodeShape {
    /// Create a node shape descriptor.
    ///
    /// `property_shapes` captures the `sh:property` links so the association can be
    /// round-tripped through ShapeIR and surfaced in diagnostics. Validation of those
    /// property shapes happens through the PropertyShape constraint component.
    pub fn new(
        identifier: ID,
        targets: Vec<Target>,
        constraints: Vec<ComponentID>,
        property_shapes: Vec<PropShapeID>,
        severity: Option<Severity>,
        deactivated: bool,
    ) -> Self {
        NodeShape {
            identifier,
            targets,
            constraints,
            property_shapes,
            severity: severity.unwrap_or_default(),
            deactivated,
        }
    }

    pub fn identifier(&self) -> &ID {
        &self.identifier
    }

    /// Constraint component IDs attached to this node shape.
    pub fn constraints(&self) -> &[ComponentID] {
        &self.constraints
    }

    /// Property shape IDs referenced via `sh:property`.
    pub fn property_shapes(&self) -> &[PropShapeID] {
        &self.property_shapes
    }

    /// Severity applied to violations originating from this shape.
    pub fn severity(&self) -> &Severity {
        &self.severity
    }

    /// Whether this shape is deactivated (`sh:deactivated true`).
    pub fn is_deactivated(&self) -> bool {
        self.deactivated
    }
}

/// Immutable description of a SHACL property shape.
///
/// Property shapes may be evaluated either as standalone shapes with explicit targets
/// or via `sh:property` constraints when attached to a node shape.
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
    /// Create a property shape descriptor.
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

    /// Returns the SPARQL path expression used for `sh:path`-based queries.
    pub fn sparql_path(&self) -> String {
        self.path.to_sparql_path().unwrap()
    }

    /// The parsed path representation used by path evaluators.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The raw RDF term for the path (useful for diagnostics/graphviz).
    pub fn path_term(&self) -> &Term {
        &self.path_term
    }

    /// Constraint component IDs attached to this property shape.
    pub fn constraints(&self) -> &[ComponentID] {
        &self.constraints
    }

    /// Severity applied to violations originating from this shape.
    pub fn severity(&self) -> &Severity {
        &self.severity
    }

    /// Whether this shape is deactivated (`sh:deactivated true`).
    pub fn is_deactivated(&self) -> bool {
        self.deactivated
    }
}
