//! Candidate terms derived from value-type facets and from a current value.
//!
//! These only *propose*. The engine decides whether a proposal has the
//! qualifier status an edit needs.

use crate::nt::{self, XSD, XSD_INTEGER, XSD_STRING, literal};
use shifty_algebra::ValueType;

pub(crate) fn flatten_facets(vt: &ValueType) -> Vec<ValueType> {
    match vt {
        ValueType::And(parts) => parts.iter().flat_map(flatten_facets).collect(),
        other => vec![other.clone()],
    }
}

/// Literals on either side of a facet's boundary, built from `lex`.
pub(crate) fn facet_proposals(
    facet: &ValueType,
    lex: &str,
    dt: Option<&str>,
    lang: Option<&str>,
) -> Vec<String> {
    let mut out = Vec::new();
    match facet {
        ValueType::Datatype(target) => {
            let target = target.as_str();
            out.push(literal(lex, Some(target), None));
            out.extend(
                samples(target)
                    .iter()
                    .map(|sample| literal(sample, Some(target), None)),
            );
            if target == XSD_STRING {
                out.push(literal(lex, None, Some("en")));
                out.push(literal("1", Some(XSD_INTEGER), None));
            } else {
                out.push(literal(lex, None, None));
            }
        }
        ValueType::Length { min, max } => {
            if let Some(lo) = min.filter(|lo| *lo > 0) {
                let lo = lo as usize;
                out.push(literal(&nt::prefix(lex, lo - 1), dt, lang));
                let base = if lex.is_empty() { "x" } else { lex };
                out.push(literal(&nt::pad(base, lo), dt, lang));
            }
            if let Some(hi) = max {
                let hi = *hi as usize;
                out.push(literal(&nt::pad(lex, hi + 1), dt, lang));
                out.push(literal(&nt::prefix(lex, hi), dt, lang));
            }
        }
        ValueType::Pattern { .. } => {
            out.push(literal("", dt, lang));
            out.push(literal(&format!("{lex} ~"), dt, lang));
        }
        ValueType::NumericRange { lo, hi } => {
            for bound in [lo, hi].into_iter().flatten() {
                let bdt = bound.value.datatype().as_str();
                let bdt = if bdt == XSD_STRING { XSD_INTEGER } else { bdt };
                for delta in [-1, 0, 1] {
                    if let Some(shifted) = nt::shift_number(bound.value.value(), bdt, delta) {
                        out.push(literal(&shifted, Some(bdt), None));
                    }
                }
            }
        }
        ValueType::LangIn(tags) => {
            for tag in tags {
                out.push(literal(lex, None, Some(tag)));
            }
            out.push(literal(lex, None, Some("zz")));
            out.push(literal(lex, None, None));
        }
        ValueType::Any | ValueType::And(_) => {}
    }
    out
}

/// Well-formed lexical forms for common XSD datatypes, so a proposal can
/// satisfy `sh:datatype` when no existing value has that type.
pub(crate) fn samples(datatype: &str) -> &'static [&'static str] {
    if nt::is_integral(datatype) {
        return &["0", "1"];
    }
    match datatype.strip_prefix(XSD) {
        Some("boolean") => &["true", "false"],
        Some("decimal") => &["1.0"],
        Some("double" | "float") => &["1.0E0"],
        Some("dateTime") => &["2024-01-01T00:00:00"],
        Some("date") => &["2024-01-01"],
        Some("time") => &["00:00:00"],
        Some("duration") => &["PT1H"],
        Some("anyURI") => &["http://example.org/x"],
        Some("gYear") => &["2024"],
        _ => &[],
    }
}

/// Small edits of the current value: a shifted number, a suffix, a
/// truncation, or an IRI's local name as a literal.
pub(crate) fn mutations(
    current: Option<&str>,
    lex: &str,
    dt: Option<&str>,
    lang: Option<&str>,
) -> Vec<String> {
    let Some(current) = current else {
        return vec![literal("x", None, None)];
    };
    if !nt::is_literal(current) {
        return vec![literal(lex, None, None)];
    }
    let mut out = Vec::new();
    if let Some(dt) = dt.filter(|dt| nt::is_numeric(dt))
        && let Some(shifted) = nt::shift_number(lex, dt, 1)
    {
        out.push(literal(&shifted, Some(dt), None));
    }
    out.push(literal(&format!("{lex}_x"), dt, lang));
    out.push(literal(&nt::prefix(lex, lex.chars().count() / 2), dt, lang));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn length_facets_straddle_the_bound() {
        let props = facet_proposals(
            &ValueType::Length {
                min: Some(3),
                max: None,
            },
            "RAT_01",
            None,
            None,
        );
        assert_eq!(props, vec!["\"RA\"".to_owned(), "\"RAT_01\"".to_owned()]);
    }

    #[test]
    fn mutations_of_a_number() {
        let props = mutations(
            Some("\"12\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            "12",
            Some(XSD_INTEGER),
            None,
        );
        assert_eq!(
            props[0],
            "\"13\"^^<http://www.w3.org/2001/XMLSchema#integer>"
        );
    }
}
