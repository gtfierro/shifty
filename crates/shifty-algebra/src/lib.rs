//! Core SHACL formalism IR (Layer 1).
//!
//! This crate is the formal heart of the engine: the path algebra `π`, the
//! shape grammar `φ`, value types `T`, selectors, schemas, node expressions,
//! and the rule skeleton. It is the SHACL fragment of arXiv:2502.01295
//! specialized to RDF (see `docs/00-formalism.md`), plus the extensions
//! catalogued in `docs/01-gap-analysis.md`.
//!
//! Nothing here depends on a graph store or a parser; downstream crates
//! (`shifty-parse`, `shifty-opt`, `shifty-engine`) build on these types.
//!
//! # Layout
//! - [`term`] — RDF term aliases and node-kind sets.
//! - [`path`] — the path algebra `π` (Def. 3).
//! - [`value_type`] — value types `T` / `test(τ)` facets.
//! - [`shape`] — the shape grammar `φ` (Def. 4) and the cyclic-capable arena.
//! - [`selector`] — selectors (Def. 5).
//! - [`expr`] — SHACL-AF node expressions (gap-analysis AF-E).
//! - [`rule`] — SHACL-AF rule skeleton (gap-analysis AF-R).
//! - [`sparql`] — opaque SPARQL escape-hatch leaves.
//! - [`schema`] — a SHACL schema `S = { (sel, φ) }` plus rules.

pub mod expr;
pub mod path;
pub mod render;
pub mod rule;
pub mod schema;
pub mod selector;
pub mod severity;
pub mod shape;
pub mod sparql;
pub mod term;
pub mod value_type;

pub use expr::NodeExpr;
pub use path::Path;
pub use rule::{Rule, RuleHead};
pub use schema::{Schema, Statement};
pub use selector::Selector;
pub use severity::Severity;
pub use shape::{Shape, ShapeArena, ShapeId};
pub use sparql::{SparqlConstraint, SparqlConstruct, SparqlQueryKind, SparqlTarget};
pub use term::{BlankNode, Literal, NamedNode, NodeKindSet, Term};
pub use value_type::{Bound, ValueType};
