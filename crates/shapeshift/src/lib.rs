//! Shapeshift: a macro language over the shifty shape algebra.
//!
//! This crate is the facade the interfaces (`shifty shape`, the REPL, and
//! later the Python and WASM bindings) build on:
//!
//! - [`session`] — definitions, data, and imports that persist across
//!   commands, transactional redefinition, and save/load of `.shm` files and
//!   session images.
//! - [`report`] — validation reports in the author's terms, with hints.
//!
//! The layers underneath are re-exported: [`parse`] (text front end),
//! [`macros`] (AST, builder, diagnostics), [`raise`] (SHACL export), and
//! [`import`] (resolution).

pub mod report;
pub mod session;

pub use shapeshift_import as import;
pub use shapeshift_macro as macros;
pub use shapeshift_parse as parse;
pub use shapeshift_raise as raise;

pub use report::Report;
pub use session::{Applied, Compiled, Image, Session, validate_compiled};
