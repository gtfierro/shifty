//! Experimental interfaces of shifty.
//!
//! **Everything in this crate may change in any release, without a
//! deprecation period.** It is kept apart from shifty's stable crates
//! (`shifty-algebra`, `shifty-parse`, `shifty-engine`, ...) so that depending
//! on it is a deliberate choice. Pin an exact version (`=0.6.0-alpha.1`) if
//! you build on it.
//!
//! - [`shapeshift`]: Shapeshift, a text language (`.shm`) for writing shapes,
//!   which compiles to shifty's shape algebra and exports readable SHACL. The
//!   `shifty shape` commands and Python's `shifty.experimental.shapeshift`
//!   are built on it.
//! - [`vio`]: a generator of SHACL violations with known repairs, for
//!   knowledge-graph repair benchmarks. Python's `shifty.experimental.vio`
//!   is built on it.
//!
//! This crate is the entry point. The crates it re-exports (`shapeshift`,
//! `shapeshift-parse`, `shapeshift-macro`, `shapeshift-import`,
//! `shapeshift-raise`, `tree-sitter-shapeshift`, and `shifty-vio`) are its
//! implementation, published only so this one can be.

/// Shapeshift, a text language for shapes. **Experimental**: it may change
/// in any release.
///
/// ```
/// use shifty_experimental::shapeshift::{macros, parse};
///
/// let src = "prefix ex: <http://example.org/>\nshape S for every ex:C { ex:p required }\n";
/// let parsed = parse::parse(src, "s.shm");
/// assert!(parsed.diagnostics.is_empty());
/// let built = macros::build::build(&macros::build::Program::single("s.shm", parsed.module));
/// assert!(!built.has_errors());
/// ```
///
/// - [`session`](shapeshift::session): definitions, data, and imports that
///   persist across commands; `.shm` files and session images.
/// - [`report`](shapeshift::report): validation reports in the author's
///   terms.
/// - [`parse`](shapeshift::parse): the parser and formatter.
/// - [`macros`](shapeshift::macros): the AST, the builder, and diagnostics.
/// - [`import`](shapeshift::import): import resolution.
/// - [`raise`](shapeshift::raise): SHACL export.
/// - `grammar`, with the `tree-sitter` feature: the tree-sitter grammar, for
///   highlighting and as-you-type feedback.
pub mod shapeshift {
    pub use ::shapeshift::*;

    /// The tree-sitter grammar for `.shm`: highlighting and error-tolerant
    /// parsing for editors and REPLs.
    #[cfg(feature = "tree-sitter")]
    pub use tree_sitter_shapeshift as grammar;
}

/// Violation generation for repair benchmarks. **Experimental**: it may
/// change in any release.
///
/// A [`Universe`](vio::Universe) holds every way to break a conforming
/// reference graph that the evidence explains, as
/// [`Injection`](vio::Injection)s. [`Universe::draw`](vio::Universe::draw)
/// composes a [`Case`](vio::Case) of compatible injections whose
/// ground-truth repair is the inverse of the injected delta. It does not
/// repair graphs.
pub mod vio {
    pub use ::shifty_vio::*;
}
