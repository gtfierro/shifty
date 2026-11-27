//! Authored names for shapes, read from the shapes graph through `name_path`.
//!
//! Shape maps (Python and C++) and the result database all name things this
//! way, so the rules live here once: paths are tried in order, each path's
//! values are sorted by their text, and a name already found is not repeated.

use std::collections::{HashMap, HashSet};

use oxrdf::{Graph, NamedOrBlankNode, Term};
use shifty_algebra::{Path, Schema, Shape, ShapeId};
use shifty_parse::{Loaded, graph::term_to_node, vocab};

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

/// The suffix after the last `#`, `/`, or `:`, tried in that order.
pub fn local_name(iri: &str) -> &str {
    ['#', '/', ':']
        .into_iter()
        .filter_map(|separator| iri.rsplit_once(separator))
        .map(|(_, tail)| tail)
        .find(|tail| !tail.is_empty())
        .unwrap_or(iri)
}

/// Follow transparent `Annotated` wrappers from `id` to an authored property
/// shape and return its authored `sh:path`.
pub fn unwrap_property_shape(
    schema: &Schema,
    shapes: &Loaded,
    id: ShapeId,
) -> Option<(ShapeId, NamedOrBlankNode, Term)> {
    annotated_chain(schema, id).find_map(|id| {
        let node = term_to_node(schema.sources.get(&id)?)?;
        let path = shapes.object(&node, vocab::SH_PATH)?;
        Some((id, node, path))
    })
}

fn annotated_chain(schema: &Schema, id: ShapeId) -> impl Iterator<Item = ShapeId> + '_ {
    let mut next = Some(id);
    let mut seen = HashSet::new();
    std::iter::from_fn(move || {
        let id = next.take()?;
        if !seen.insert(id) {
            return None;
        }
        if let Shape::Annotated { shape, .. } = schema.arena.get(id) {
            next = Some(*shape);
        }
        Some(id)
    })
}

/// Resolve names once per authored RDF node, then inherit the first names
/// available through transparent annotations for every raw constraint.
pub fn binding_names(
    schema: &Schema,
    graph: &Graph,
    paths: &[Path],
    source_owners: &HashMap<u32, u32>,
) -> HashMap<u32, Vec<String>> {
    let mut by_source = HashMap::new();
    let mut result = HashMap::new();
    for index in 0..schema.arena.len() as u32 {
        for id in annotated_chain(schema, ShapeId(index)) {
            let owner = ShapeId(*source_owners.get(&id.0).unwrap_or(&id.0));
            let Some(source) = schema.sources.get(&owner) else {
                continue;
            };
            let names: &mut Vec<String> = by_source.entry(source).or_insert_with(|| {
                authored_names(graph, source, paths)
                    .iter()
                    .map(name_text)
                    .collect()
            });
            if !names.is_empty() {
                result.insert(index, names.clone());
                break;
            }
        }
    }
    result
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
    fn local_name_prefers_a_nonempty_fragment() {
        assert_eq!(local_name("http://ex/hasPoint"), "hasPoint");
        assert_eq!(local_name("http://ex#hasPoint"), "hasPoint");
        assert_eq!(local_name("urn:zonepac-app/zonepac-zone"), "zonepac-zone");
        assert_eq!(local_name("http://ex/a#b/c"), "b/c");
        assert_eq!(local_name("urn:ex:value"), "value");
        assert_eq!(local_name("http://ex/a#"), "a#");
    }
}
