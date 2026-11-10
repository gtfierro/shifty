//! Terms as N-Triples strings.
//!
//! The generator names every term by its N-Triples spelling (`<iri>`,
//! `_:label`, `"lex"^^<dt>`). Strings order totally, so every set the
//! expansion iterates has one order on every run, and expressions print and
//! deduplicate without a term table. Terms are parsed back only to probe the
//! engine or to build a [`GraphDelta`](shifty_repair::GraphDelta).

use oxrdf::{Literal, NamedNode, NamedOrBlankNode, Term};
use std::str::FromStr;

/// A triple of N-Triples terms: subject, predicate, object.
pub type Triple = (String, String, String);

pub const RDF_TYPE: &str = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
pub const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
pub const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
pub const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

const INTEGRAL: &[&str] = &[
    "integer",
    "int",
    "long",
    "short",
    "byte",
    "nonNegativeInteger",
    "positiveInteger",
    "negativeInteger",
    "nonPositiveInteger",
    "unsignedInt",
    "unsignedLong",
    "unsignedShort",
    "unsignedByte",
];

fn xsd_local(dt: &str) -> Option<&str> {
    dt.strip_prefix(XSD)
}

pub fn is_integral(dt: &str) -> bool {
    xsd_local(dt).is_some_and(|name| INTEGRAL.contains(&name))
}

pub fn is_numeric(dt: &str) -> bool {
    is_integral(dt) || matches!(xsd_local(dt), Some("decimal" | "double" | "float"))
}

pub fn is_literal(term: &str) -> bool {
    term.starts_with('"')
}

pub fn term(t: &Term) -> String {
    t.to_string()
}

pub fn triple(t: &oxrdf::Triple) -> Triple {
    (
        t.subject.to_string(),
        t.predicate.to_string(),
        t.object.to_string(),
    )
}

pub fn parse_term(s: &str) -> Option<Term> {
    Term::from_str(s).ok()
}

/// The engine form of a triple, or `None` when a position holds a term it
/// cannot (a literal subject, say).
pub fn parse_triple(t: &Triple) -> Option<oxrdf::Triple> {
    let subject = match parse_term(&t.0)? {
        Term::NamedNode(n) => NamedOrBlankNode::NamedNode(n),
        Term::BlankNode(b) => NamedOrBlankNode::BlankNode(b),
        _ => return None,
    };
    let predicate = match parse_term(&t.1)? {
        Term::NamedNode(n) => n,
        _ => return None,
    };
    Some(oxrdf::Triple::new(subject, predicate, parse_term(&t.2)?))
}

/// `"s p o ."`
pub fn line(t: &Triple) -> String {
    format!("{} {} {} .", t.0, t.1, t.2)
}

/// A literal in N-Triples, as the graph itself would print it. An
/// `xsd:string` datatype is left implicit.
pub fn literal(lex: &str, dt: Option<&str>, lang: Option<&str>) -> String {
    if let Some(lang) = lang.filter(|l| !l.is_empty()) {
        return Literal::new_language_tagged_literal_unchecked(lex, lang).to_string();
    }
    match dt {
        Some(dt) if dt != XSD_STRING => {
            Literal::new_typed_literal(lex, NamedNode::new_unchecked(dt)).to_string()
        }
        _ => Literal::new_simple_literal(lex).to_string(),
    }
}

/// The lexical form, explicit datatype, and language of a literal. For an
/// IRI or blank node, its local name and no datatype or language.
pub fn literal_parts(term: &str) -> (String, Option<String>, Option<String>) {
    match parse_term(term) {
        Some(Term::Literal(lit)) => {
            if let Some(lang) = lit.language() {
                return (lit.value().to_owned(), None, Some(lang.to_owned()));
            }
            let dt = lit.datatype().as_str();
            let dt = (dt != XSD_STRING).then(|| dt.to_owned());
            (lit.value().to_owned(), dt, None)
        }
        _ => (local_name(term), None, None),
    }
}

/// The part of an IRI after its last `/`, `#`, or `:`.
pub fn local_name(term: &str) -> String {
    let iri = term.trim_start_matches('<').trim_end_matches('>');
    let iri = iri.strip_prefix("_:").unwrap_or(iri);
    let cut = iri.rfind(['/', '#', ':']).map_or(0, |i| i + 1);
    let name = &iri[cut..];
    if name.is_empty() { iri } else { name }.to_owned()
}

/// A short label for operation details: local names for IRIs, the term for
/// anything else.
pub fn short(term: &str) -> String {
    if term.starts_with('<') {
        local_name(term)
    } else {
        term.to_owned()
    }
}

/// `<iri>` for a bare IRI or one already in angle brackets.
pub fn iri_term(iri: &str) -> String {
    if iri.starts_with('<') {
        iri.to_owned()
    } else {
        format!("<{iri}>")
    }
}

/// A number shifted by `delta`, in the lexical space of `dt`.
pub fn shift_number(lex: &str, dt: &str, delta: i64) -> Option<String> {
    if is_integral(dt) {
        let v: i128 = lex.trim().parse().ok()?;
        return Some((v + i128::from(delta)).to_string());
    }
    let v: f64 = lex.trim().parse().ok()?;
    let shifted = v + delta as f64;
    shifted.is_finite().then(|| format!("{shifted:?}"))
}

/// The first `n` characters.
pub fn prefix(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// `s` padded with `x` to at least `n` characters.
pub fn pad(s: &str, n: usize) -> String {
    let mut out = s.to_owned();
    let len = s.chars().count();
    out.extend(std::iter::repeat_n('x', n.saturating_sub(len)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_print_as_the_graph_does() {
        assert_eq!(literal("RA", None, None), "\"RA\"");
        assert_eq!(literal("RA", Some(XSD_STRING), None), "\"RA\"");
        assert_eq!(
            literal("1", Some(XSD_INTEGER), None),
            "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        );
        assert_eq!(literal("x", None, Some("en")), "\"x\"@en");
        assert_eq!(literal("a\"b", None, None), "\"a\\\"b\"");
    }

    #[test]
    fn parts_and_names() {
        let (lex, dt, lang) = literal_parts("\"12\"^^<http://www.w3.org/2001/XMLSchema#integer>");
        assert_eq!(
            (lex.as_str(), dt.as_deref(), lang),
            ("12", Some(XSD_INTEGER), None)
        );
        assert_eq!(local_name("<http://ex/a#RAT_01>"), "RAT_01");
        assert_eq!(local_name("<urn:x:y>"), "y");
        assert_eq!(shift_number("3", XSD_INTEGER, -1).as_deref(), Some("2"));
        assert_eq!(
            shift_number("1.5", "http://www.w3.org/2001/XMLSchema#decimal", 1).as_deref(),
            Some("2.5")
        );
        let t = (
            "<http://ex/a>".into(),
            "<http://ex/p>".into(),
            "\"v\"@en".into(),
        );
        assert_eq!(triple(&parse_triple(&t).unwrap()), t);
        assert!(
            parse_triple(&(
                "\"x\"".into(),
                "<http://ex/p>".into(),
                "<http://ex/o>".into()
            ))
            .is_none()
        );
    }
}
