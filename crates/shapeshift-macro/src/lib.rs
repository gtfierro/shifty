//! Shapeshift macro layer: the kernel builder.
//!
//! Shapeshift is a small text language for RDF validation rules that compiles
//! to shifty's shape algebra. This crate owns everything between the AST and
//! the arena:
//!
//! - [`ast`] — the serializable AST, the interchange format between layers.
//! - [`build`] — two-phase construction of named shapes, template expansion
//!   with inferred parameter sorts, provenance, and build-time admission.
//! - [`display`] — AST back to canonical text (the formatter's back half).
//! - [`diagnostic`] — build-time diagnostics shared by every layer.
//! - [`vocab`] — vocabulary index from imported ontologies.
//!
//! Nothing here defines a constraint of its own: every construct bottoms out
//! in an arena `Shape` built with shifty's smart constructors.

pub mod ast;
pub mod build;
pub mod diagnostic;
pub mod display;
pub mod vocab;

pub use build::{
    Binding, BuildOutput, Expansion, ModuleUnit, Portability, Program, RdfUnit, Sort, build,
    build_module, class_path,
};
pub use diagnostic::{Diagnostic, Level};
pub use vocab::Vocabulary;
