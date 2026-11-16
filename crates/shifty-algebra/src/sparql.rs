//! Opaque SPARQL escape-hatch leaves (gap-analysis **AF-C / AF-T / AF-R**).
//!
//! These carry parsed-and-canonicalized query text the algebra does not yet
//! reason about. Prefixes are resolved so the stored query is self-contained;
//! later passes may rewrite its Spargebra AST or recognize simple BGPs and lift
//! them back into the algebra.

use crate::path::Path;
use oxrdf::{NamedNode, Term};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SparqlQueryKind {
    Ask,
    Select,
}

/// One SHACL-AF result annotation declaration.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResultAnnotation {
    pub property: NamedNode,
    pub variable: Option<String>,
    pub defaults: Vec<Term>,
}

impl ResultAnnotation {
    /// Resolve one solution row, preserving all distinct property/value pairs.
    pub fn resolve(
        declarations: &[Self],
        bindings: &std::collections::HashMap<String, Term>,
    ) -> Vec<(NamedNode, Term)> {
        let mut values = std::collections::HashSet::new();
        for annotation in declarations {
            let name = annotation.variable.as_deref().unwrap_or_else(|| {
                annotation
                    .property
                    .as_str()
                    .rsplit(['#', '/'])
                    .next()
                    .unwrap_or(annotation.property.as_str())
            });
            if let Some(value) = bindings.get(name) {
                values.insert((annotation.property.clone(), value.clone()));
            } else {
                values.extend(
                    annotation
                        .defaults
                        .iter()
                        .map(|value| (annotation.property.clone(), value.clone())),
                );
            }
        }
        let mut values: Vec<_> = values.into_iter().collect();
        values.sort_by(|a, b| {
            a.0.as_str()
                .cmp(b.0.as_str())
                .then_with(|| a.1.to_string().cmp(&b.1.to_string()))
        });
        values
    }
}

/// `sh:sparql` constraint (an `ASK` or `SELECT` producing violations).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SparqlConstraint {
    #[serde(default)]
    pub annotations: Vec<ResultAnnotation>,
    pub kind: SparqlQueryKind,
    pub query: String,
    /// The owning property shape's path. Simple predicate paths are prebound
    /// to `$PATH`; complex paths require an AST rewrite before execution.
    pub path: Option<Path>,
    /// The RDF node of the shape that declares this constraint, pre-bound to
    /// `$currentShape`. `None` when provenance is unavailable.
    pub shape: Option<Term>,
    /// `sh:message` literals declared on the SPARQL constraint (falling back to
    /// the owning shape). Used as the violation message when a `SELECT` result
    /// does not bind its own `?message`. Empty when none is declared.
    pub messages: Vec<Term>,
    /// Extra pre-bound variables injected before `$this`. Used by custom
    /// constraint component lowering to substitute parameter values (e.g.
    /// `$exactCount`, `$class`) that are static per activating shape.
    /// Empty for plain `sh:sparql` constraints.
    pub extra_bindings: Vec<(String, Term)>,
    /// When `true`, rename `?value` to `?this` in the compiled query before
    /// binding `$this` per focus. Set for ASK validators activated by node
    /// shapes (no `sh:path`), where `$value` equals the focus node (`$this`).
    pub bind_value_to_this: bool,
}

/// `sh:target` + `sh:select` — a SPARQL-based target.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SparqlTarget {
    pub query: String,
    #[serde(default)]
    pub extra_bindings: Vec<(String, Term)>,
}

/// `sh:SPARQLRule` — a `CONSTRUCT` rule head.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SparqlConstruct {
    pub query: String,
}

/// Canonical definition of a document-authored `sh:SPARQLFunction`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionDef {
    pub iri: NamedNode,
    /// Parameter variable names in positional call order.
    pub params: Vec<String>,
    /// Optional flags in the same positional order as `params`.
    pub optional_params: Vec<bool>,
    /// Prefix-expanded `sh:select` or `sh:ask` body.
    pub query: String,
    /// Whether the body reads RDF data rather than only computing a value.
    pub reads_graph: bool,
}
