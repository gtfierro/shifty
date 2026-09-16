//! The vocabulary an IRI is *displayed* in.
//!
//! The algebra stores absolute IRIs, which is the only unambiguous form — but a
//! validation message built from them is mostly namespace. A single s223 term is
//! 55 characters absolute and 8 compacted, and a report message names several,
//! so carrying the source document's `@prefix` declarations alongside the schema
//! is the difference between a message a person reads and one they scroll.
//!
//! This is a *display* concern only: nothing in evaluation consults it, and a
//! [`Prefixes`] that is empty or wrong costs legibility, never correctness.

use serde::{Deserialize, Serialize};

/// Namespaces every RDF reader knows, compacted even when a document declares
/// nothing. Kept separate from the document's own declarations so that a
/// document is free to bind these prefixes to something else.
const WELL_KNOWN: &[(&str, &str)] = &[
    ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    ("sh", "http://www.w3.org/ns/shacl#"),
    ("xsd", "http://www.w3.org/2001/XMLSchema#"),
    ("owl", "http://www.w3.org/2002/07/owl#"),
];

/// Prefix declarations used to compact IRIs for display, in the order they are
/// tried. Empty is a valid, useful value: the well-known namespaces still apply.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Prefixes {
    /// `(prefix, namespace)`, longest namespace first — see [`Prefixes::new`].
    decls: Vec<(String, String)>,
}

impl Prefixes {
    /// Build from a document's declarations.
    ///
    /// Two normalizations matter for a stable rendering. Longer namespaces are
    /// tried first, so a document declaring both `ex:` for `http://ex/` and
    /// `exs:` for `http://ex/sub/` compacts `http://ex/sub/a` as `exs:a` rather
    /// than as the technically-correct-but-useless `ex:sub/a`. Ties break on the
    /// prefix name, so the choice never depends on the parser's iteration order.
    /// The empty prefix (`@prefix : <…>`) is kept: `:local` is valid Turtle.
    pub fn new(decls: impl IntoIterator<Item = (String, String)>) -> Self {
        let mut decls: Vec<(String, String)> = decls.into_iter().collect();
        decls.sort_by(|(ap, ans), (bp, bns)| bns.len().cmp(&ans.len()).then_with(|| ap.cmp(bp)));
        decls.dedup_by(|a, b| a.1 == b.1);
        Self { decls }
    }

    /// The empty table — well-known namespaces only — behind a `'static`
    /// reference, for call sites that must hand out a long-lived borrow.
    pub fn empty() -> &'static Prefixes {
        static EMPTY: Prefixes = Prefixes { decls: Vec::new() };
        &EMPTY
    }

    /// One table from several documents' declarations, earlier ones winning a
    /// tie. A report names terms from both the data and the shapes graph, and a
    /// reader wants each spelled the way its own document spelled it.
    pub fn merged<I>(documents: impl IntoIterator<Item = I>) -> Self
    where
        I: IntoIterator<Item = (String, String)>,
    {
        Self::new(documents.into_iter().flatten())
    }

    pub fn is_empty(&self) -> bool {
        self.decls.is_empty()
    }

    /// The declarations, longest namespace first.
    pub fn declarations(&self) -> &[(String, String)] {
        &self.decls
    }

    /// Compact `iri` to `prefix:local`, or return it as `<iri>` when no
    /// declaration applies.
    ///
    /// A namespace only matches when what follows it is a plausible Turtle local
    /// name. Without that check `<http://ex/a/b>` under `ex: <http://ex/>` would
    /// render as `ex:a/b`, which reads as a compacted name but does not parse
    /// back as one — a report a reader can copy into a query is worth more than a
    /// few saved characters.
    pub fn compact(&self, iri: &str) -> String {
        for (prefix, ns) in self.decls.iter().map(|(p, n)| (p.as_str(), n.as_str())) {
            if let Some(local) = local_name(iri, ns) {
                return format!("{prefix}:{local}");
            }
        }
        for (prefix, ns) in WELL_KNOWN {
            if let Some(local) = local_name(iri, ns) {
                return format!("{prefix}:{local}");
            }
        }
        format!("<{iri}>")
    }
}

/// The local part of `iri` under `ns`, if `ns` is a prefix of it and the
/// remainder is a non-empty, delimiter-free Turtle local name.
fn local_name<'a>(iri: &'a str, ns: &str) -> Option<&'a str> {
    let local = iri.strip_prefix(ns)?;
    let usable = !local.is_empty()
        && !local.starts_with(['-', '.'])
        && local
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'));
    usable.then_some(local)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decls() -> Prefixes {
        Prefixes::new([
            ("ex".to_string(), "http://ex/".to_string()),
            ("exs".to_string(), "http://ex/sub/".to_string()),
            ("".to_string(), "http://default/".to_string()),
        ])
    }

    #[test]
    fn compacts_against_declared_and_well_known_namespaces() {
        let p = decls();
        assert_eq!(p.compact("http://ex/Thing"), "ex:Thing");
        assert_eq!(p.compact("http://default/Thing"), ":Thing");
        assert_eq!(
            p.compact("http://www.w3.org/ns/shacl#NodeShape"),
            "sh:NodeShape"
        );
        assert_eq!(p.compact("http://other/Thing"), "<http://other/Thing>");
    }

    #[test]
    fn the_most_specific_namespace_wins() {
        // Both `ex:` and `exs:` are prefixes of the IRI; `ex:sub/a` would not
        // parse back, so the longer declaration has to be tried first.
        assert_eq!(decls().compact("http://ex/sub/a"), "exs:a");
    }

    #[test]
    fn a_local_name_that_would_not_parse_is_left_absolute() {
        let p = decls();
        // Path separators, fragments and an empty local name all disqualify.
        assert_eq!(p.compact("http://ex/a/b"), "<http://ex/a/b>");
        assert_eq!(p.compact("http://ex/a#b"), "<http://ex/a#b>");
        assert_eq!(p.compact("http://ex/"), "<http://ex/>");
    }

    #[test]
    fn well_known_namespaces_apply_without_any_declarations() {
        let p = Prefixes::default();
        assert!(p.is_empty());
        assert_eq!(
            p.compact("http://www.w3.org/2001/XMLSchema#integer"),
            "xsd:integer"
        );
        assert_eq!(p.compact("http://ex/Thing"), "<http://ex/Thing>");
    }

    #[test]
    fn merging_documents_keeps_every_vocabulary() {
        // A report names data-graph and shapes-graph terms side by side; both
        // documents' prefixes have to apply.
        let data = [("bdg1".to_string(), "https://ex/models#".to_string())];
        let shapes = [("s223".to_string(), "http://ex/223#".to_string())];
        let px = Prefixes::merged([data, shapes]);
        assert_eq!(px.compact("https://ex/models#abc"), "bdg1:abc");
        assert_eq!(px.compact("http://ex/223#Thing"), "s223:Thing");
    }

    #[test]
    fn duplicate_namespaces_collapse_to_one_prefix() {
        // Two names for the same namespace must not make the rendering depend on
        // which one the parser happened to report first.
        let p = Prefixes::new([
            ("b".to_string(), "http://ex/".to_string()),
            ("a".to_string(), "http://ex/".to_string()),
        ]);
        assert_eq!(p.declarations().len(), 1);
        assert_eq!(p.compact("http://ex/Thing"), "a:Thing");
    }
}
