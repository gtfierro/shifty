//! Shapeshift text front end.
//!
//! **Experimental**: this crate may change in any release, without a
//! deprecation period. It is part of the implementation of
//! [`shifty-experimental`](https://docs.rs/shifty-experimental), the entry
//! point for Shapeshift (`shifty_experimental::shapeshift`): depend on that
//! instead.
//!
//! - [`parse`] turns `.shm` text into the AST, with line-level error
//!   recovery and diagnostics written for people new to RDF.
//! - [`format`] parses and re-renders a file canonically, preserving comments
//!   and docstrings.
//!
//! The parser is hand-written rather than generated so its error messages can
//! name what was expected in the language's own words. Editors and the REPL use
//! the tree-sitter grammar in `tree-sitter-shapeshift` for highlighting and
//! as-you-type feedback; a test keeps the two grammars in agreement.

pub mod lexer;
pub mod parser;

pub use parser::{KEYWORDS, ParseOutput, parse};
use shapeshift_macro::Diagnostic;

/// Parse and re-render `src` canonically. Fails with the parse diagnostics
/// if the file has syntax errors (formatting a broken file would lose text).
pub fn format(src: &str, label: &str) -> Result<String, Vec<Diagnostic>> {
    let out = parse(src, label);
    if out.has_errors() {
        return Err(out.diagnostics);
    }
    Ok(shapeshift_macro::display::format_module(&out.module))
}
