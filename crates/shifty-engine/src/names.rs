//! Authored names for shapes, read from the shapes graph through `name_path`.
//!
//! Shape maps (Python and C++) and the result database all name things this
//! way, so the rules live here once: paths are tried in order, each path's
//! values are sorted by their text, and a name already found is not repeated.

use std::collections::HashSet;

use oxrdf::{Graph, Term};
use shifty_algebra::Path;
use shifty_parse::Loaded;

/// The `name_path` used when a caller does not choose one.
pub const DEFAULT_NAME_PATHS: [&str; 2] = ["sh:name", "rdfs:label"];

/// Parse `name_path` entries against the shapes graph's prefixes. `sh:` and
/// `rdfs:` resolve without a declaration.
pub fn parse_name_paths<S: AsRef<str>>(exprs: &[S], shapes: &Loaded) -> Result<Vec<Path>, String> {
    exprs
        .iter()
        .map(|expr| {
            let expr = expr.as_ref();
            shifty_parse::parse_property_path(expr, shapes)
                .map_err(|error| format!("invalid name_path {expr:?}: {error}"))
        })
        .collect()
}

/// Every name `paths` reach from `node`, earlier paths first.
pub fn authored_names(graph: &Graph, node: &Term, paths: &[Path]) -> Vec<Term> {
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    for path in paths {
        let mut matches: Vec<Term> = crate::path::succ(graph, node, path).into_iter().collect();
        matches.sort_by_cached_key(name_text);
        for name in matches {
            if seen.insert(name_text(&name)) {
                names.push(name);
            }
        }
    }
    names
}

/// A name as text: a literal's lexical form, otherwise its N-Triples form.
pub fn name_text(term: &Term) -> String {
    match term {
        Term::Literal(literal) => literal.value().to_owned(),
        other => other.to_string(),
    }
}

/// The local name of an IRI: the last non-empty segment after `#`, `/`, or `:`.
pub fn local_name(iri: &str) -> Option<&str> {
    iri.rsplit(['#', '/', ':']).find(|part| !part.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn earlier_paths_come_first_without_repeats() {
        let shapes = shifty_parse::load_turtle(
            br#"@prefix ex: <urn:ex:> .
                ex:p ex:label "b", "a" ; ex:title "a", "c" ."#,
            None,
        )
        .unwrap();
        let paths = parse_name_paths(&["ex:label", "ex:title"], &shapes).unwrap();
        let node = Term::from(oxrdf::NamedNode::new_unchecked("urn:ex:p"));
        let names: Vec<String> = authored_names(&shapes.graph, &node, &paths)
            .iter()
            .map(name_text)
            .collect();
        assert_eq!(names, ["a", "b", "c"]);
    }

    #[test]
    fn default_paths_need_no_prefix_declarations() {
        let shapes = shifty_parse::load_turtle(b"", None).unwrap();
        assert!(parse_name_paths(&DEFAULT_NAME_PATHS, &shapes).is_ok());
        assert!(parse_name_paths(&["ex:undeclared"], &shapes).is_err());
    }

    #[test]
    fn local_name_uses_last_nonempty_segment() {
        assert_eq!(local_name("http://ex/hasPoint"), Some("hasPoint"));
        assert_eq!(local_name("http://ex#hasPoint"), Some("hasPoint"));
        assert_eq!(
            local_name("urn:zonepac-app/zonepac-zone"),
            Some("zonepac-zone")
        );
        assert_eq!(local_name("http://ex/a#b/c"), Some("c"));
        assert_eq!(local_name("http://ex/a#"), Some("a"));
        assert_eq!(local_name("#/"), None);
    }
}
