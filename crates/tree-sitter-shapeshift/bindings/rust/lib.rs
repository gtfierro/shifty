//! Shapeshift grammar for [tree-sitter](https://tree-sitter.github.io/).
//!
//! Editors use it for highlighting and folding; the `shifty shape repl` uses
//! it for as-you-type feedback (colors, error markers, and deciding when a
//! multi-line definition is complete). The compiler parses with
//! `shapeshift-parse`; a test keeps the two grammars in agreement.
//!
//! ```ignore
//! let mut parser = tree_sitter::Parser::new();
//! parser.set_language(&tree_sitter_shapeshift::LANGUAGE.into()).unwrap();
//! let tree = parser.parse("shape S { ex:p required }", None).unwrap();
//! assert!(!tree.root_node().has_error());
//! ```
//!
//! Regenerate `src/parser.c` after editing `grammar.js` with
//! `tree-sitter generate` (tree-sitter-cli 0.25).

use tree_sitter_language::LanguageFn;

unsafe extern "C" {
    fn tree_sitter_shapeshift() -> *const ();
}

/// The tree-sitter [`LanguageFn`] for Shapeshift.
pub const LANGUAGE: LanguageFn = unsafe { LanguageFn::from_raw(tree_sitter_shapeshift) };

/// The content of the `node-types.json` file for this grammar.
pub const NODE_TYPES: &str = include_str!("../../src/node-types.json");

/// Syntax highlighting query.
pub const HIGHLIGHTS_QUERY: &str = include_str!("../../queries/highlights.scm");

#[cfg(test)]
mod tests {
    #[test]
    fn can_load_grammar() {
        let mut parser = tree_sitter::Parser::new();
        parser
            .set_language(&super::LANGUAGE.into())
            .expect("Error loading Shapeshift parser");
    }
}
