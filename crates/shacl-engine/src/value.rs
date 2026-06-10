//! Evaluation of value types `test(τ)` and the term ordering used by
//! `sh:lessThan`, ranges, etc. Naive reference semantics.

use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNodeRef, Term};
use oxsdatatypes::{Boolean, DateTime, Decimal, Double, Float};
use regex::Regex;
use shacl_algebra::value_type::{Bound, ValueType};
use std::cmp::Ordering;
use std::str::FromStr;

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

/// Does `term` satisfy value type `τ`?
pub fn value_type_holds(vt: &ValueType, term: &Term) -> bool {
    match vt {
        ValueType::Any => true,
        ValueType::Datatype(dt) => {
            matches!(term, Term::Literal(l) if l.datatype() == dt.as_ref() && valid_lexical_form(l))
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
        if la.datatype() == xsd::DATE_TIME && lb.datatype() == xsd::DATE_TIME {
            let left = DateTime::from_str(la.value()).ok()?;
            let right = DateTime::from_str(lb.value()).ok()?;
            return left.partial_cmp(&right);
        }
    }
    None
}

fn valid_lexical_form(literal: &Literal) -> bool {
    let datatype = literal.datatype();
    let value = literal.value();
    if datatype == xsd::BOOLEAN {
        Boolean::from_str(value).is_ok()
    } else if datatype == xsd::DECIMAL {
        Decimal::from_str(value).is_ok()
    } else if datatype == xsd::FLOAT {
        Float::from_str(value).is_ok()
    } else if datatype == xsd::DOUBLE {
        Double::from_str(value).is_ok()
    } else if datatype == xsd::DATE_TIME {
        DateTime::from_str(value).is_ok()
    } else if is_integer_datatype(datatype) {
        integer_in_datatype_range(value, datatype)
    } else {
        true
    }
}

fn is_integer_datatype(datatype: NamedNodeRef<'_>) -> bool {
    matches!(
        datatype,
        xsd::INTEGER
            | xsd::LONG
            | xsd::INT
            | xsd::SHORT
            | xsd::BYTE
            | xsd::NON_NEGATIVE_INTEGER
            | xsd::NON_POSITIVE_INTEGER
            | xsd::NEGATIVE_INTEGER
            | xsd::POSITIVE_INTEGER
            | xsd::UNSIGNED_LONG
            | xsd::UNSIGNED_INT
            | xsd::UNSIGNED_SHORT
            | xsd::UNSIGNED_BYTE
    )
}

fn integer_in_datatype_range(value: &str, datatype: NamedNodeRef<'_>) -> bool {
    let value = value.trim();
    let digits = value.strip_prefix(['+', '-']).unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    if datatype == xsd::INTEGER {
        return true;
    }
    let Ok(number) = value.parse::<i128>() else {
        return false;
    };
    match datatype {
        xsd::LONG => number >= i64::MIN.into() && number <= i64::MAX.into(),
        xsd::INT => number >= i32::MIN.into() && number <= i32::MAX.into(),
        xsd::SHORT => number >= i16::MIN.into() && number <= i16::MAX.into(),
        xsd::BYTE => number >= i8::MIN.into() && number <= i8::MAX.into(),
        xsd::NON_NEGATIVE_INTEGER => number >= 0,
        xsd::NON_POSITIVE_INTEGER => number <= 0,
        xsd::NEGATIVE_INTEGER => number < 0,
        xsd::POSITIVE_INTEGER => number > 0,
        xsd::UNSIGNED_LONG => number >= 0 && number <= u64::MAX.into(),
        xsd::UNSIGNED_INT => number >= 0 && number <= u32::MAX.into(),
        xsd::UNSIGNED_SHORT => number >= 0 && number <= u16::MAX.into(),
        xsd::UNSIGNED_BYTE => number >= 0 && number <= u8::MAX.into(),
        _ => false,
    }
}

fn numeric(l: &Literal) -> Option<f64> {
    is_numeric_datatype(l)
        .then(|| l.value().parse::<f64>().ok())
        .flatten()
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
