//! Evaluation of value types `test(τ)` and the term ordering used by
//! `sh:lessThan`, ranges, etc. Naive reference semantics.

use oxrdf::vocab::xsd;
use oxrdf::{Literal, Term};
use regex::Regex;
use shacl_algebra::value_type::{Bound, ValueType};
use std::cmp::Ordering;

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// Does `term` satisfy value type `τ`?
pub fn value_type_holds(vt: &ValueType, term: &Term) -> bool {
    match vt {
        ValueType::Any => true,
        ValueType::Datatype(dt) => {
            matches!(term, Term::Literal(l) if l.datatype() == dt.as_ref())
        }
        ValueType::NumericRange { lo, hi } => {
            lo.as_ref().is_none_or(|b| satisfies_lower(term, b))
                && hi.as_ref().is_none_or(|b| satisfies_upper(term, b))
        }
        ValueType::Length { min, max } => match lexical(term) {
            Some(s) => {
                let len = s.chars().count() as u64;
                min.is_none_or(|m| len >= m) && max.is_none_or(|m| len <= m)
            }
            None => false,
        },
        ValueType::Pattern { regex, flags } => match lexical(term) {
            Some(s) => compile(regex, flags).is_some_and(|re| re.is_match(&s)),
            None => false,
        },
        ValueType::LangIn(langs) => match term {
            Term::Literal(l) => l
                .language()
                .is_some_and(|tag| langs.iter().any(|range| lang_matches(tag, range))),
            _ => false,
        },
        ValueType::And(parts) => parts.iter().all(|p| value_type_holds(p, term)),
    }
}

fn satisfies_lower(term: &Term, b: &Bound) -> bool {
    match compare_terms(term, &Term::Literal(b.value.clone())) {
        Some(Ordering::Greater) => true,
        Some(Ordering::Equal) => b.inclusive,
        _ => false,
    }
}

fn satisfies_upper(term: &Term, b: &Bound) -> bool {
    match compare_terms(term, &Term::Literal(b.value.clone())) {
        Some(Ordering::Less) => true,
        Some(Ordering::Equal) => b.inclusive,
        _ => false,
    }
}

/// Partial ordering over terms (SPARQL-flavored, reference subset): numeric
/// literals compare numerically, string-typed literals lexicographically;
/// everything else is incomparable (`None`).
pub fn compare_terms(a: &Term, b: &Term) -> Option<Ordering> {
    if let (Term::Literal(la), Term::Literal(lb)) = (a, b) {
        if let (Some(na), Some(nb)) = (numeric(la), numeric(lb)) {
            return na.partial_cmp(&nb);
        }
        if is_string(la) && is_string(lb) {
            return Some(la.value().cmp(lb.value()));
        }
    }
    None
}

fn numeric(l: &Literal) -> Option<f64> {
    is_numeric_datatype(l).then(|| l.value().parse::<f64>().ok()).flatten()
}

fn is_numeric_datatype(l: &Literal) -> bool {
    let dt = l.datatype().as_str();
    let Some(local) = dt.strip_prefix(XSD) else {
        return false;
    };
    matches!(
        local,
        "integer"
            | "decimal"
            | "float"
            | "double"
            | "long"
            | "int"
            | "short"
            | "byte"
            | "nonNegativeInteger"
            | "nonPositiveInteger"
            | "negativeInteger"
            | "positiveInteger"
            | "unsignedLong"
            | "unsignedInt"
            | "unsignedShort"
            | "unsignedByte"
    )
}

fn is_string(l: &Literal) -> bool {
    l.datatype() == xsd::STRING
}

/// The lexical form to which string facets apply: a literal's value or an IRI;
/// blank nodes have none.
fn lexical(term: &Term) -> Option<String> {
    match term {
        Term::Literal(l) => Some(l.value().to_string()),
        Term::NamedNode(n) => Some(n.as_str().to_string()),
        Term::BlankNode(_) => None,
    }
}

/// BCP47 basic language-range match (case-insensitive prefix), plus `*`.
fn lang_matches(tag: &str, range: &str) -> bool {
    range == "*"
        || tag.eq_ignore_ascii_case(range)
        || tag
            .to_ascii_lowercase()
            .starts_with(&format!("{}-", range.to_ascii_lowercase()))
}

/// Compile a SHACL `sh:pattern` with the supported `sh:flags` subset (i,s,m,x).
fn compile(regex: &str, flags: &str) -> Option<Regex> {
    let active: String = flags.chars().filter(|c| "ismx".contains(*c)).collect();
    let pattern = if active.is_empty() {
        regex.to_string()
    } else {
        format!("(?{active}){regex}")
    };
    Regex::new(&pattern).ok()
}
