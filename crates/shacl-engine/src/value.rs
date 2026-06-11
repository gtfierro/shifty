//! Evaluation of value types `test(τ)` and the term ordering used by
//! `sh:lessThan`, ranges, etc. Naive reference semantics.

use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNodeRef, Term};
use oxsdatatypes::{Boolean, Date, DateTime, DayTimeDuration, Decimal, Double, Duration, Float,
    Time, YearMonthDuration};
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

/// Partial ordering over terms: numeric literals compare numerically,
/// string-typed literals lexicographically, and the XSD date/time/duration
/// family via their `oxsdatatypes` implementations (which correctly model the
/// partial order for timezone-unaware comparisons and incomparable durations).
pub fn compare_terms(a: &Term, b: &Term) -> Option<Ordering> {
    if let (Term::Literal(la), Term::Literal(lb)) = (a, b) {
        if let (Some(na), Some(nb)) = (numeric(la), numeric(lb)) {
            return na.partial_cmp(&nb);
        }
        if is_string(la) && is_string(lb) {
            return Some(la.value().cmp(lb.value()));
        }
        let (dta, dtb) = (la.datatype(), lb.datatype());
        if dta == xsd::DATE_TIME && dtb == xsd::DATE_TIME {
            let left = DateTime::from_str(la.value()).ok()?;
            let right = DateTime::from_str(lb.value()).ok()?;
            return left.partial_cmp(&right);
        }
        if dta == xsd::DATE && dtb == xsd::DATE {
            let left = Date::from_str(la.value()).ok()?;
            let right = Date::from_str(lb.value()).ok()?;
            return left.partial_cmp(&right);
        }
        if dta == xsd::TIME && dtb == xsd::TIME {
            let left = Time::from_str(la.value()).ok()?;
            let right = Time::from_str(lb.value()).ok()?;
            return left.partial_cmp(&right);
        }
        if is_duration_datatype(dta) && is_duration_datatype(dtb) {
            // Parse via Duration for all three subtypes; Duration::partial_cmp
            // correctly returns None for incomparable year-month vs day-time pairs.
            let left = Duration::from_str(la.value()).ok()?;
            let right = Duration::from_str(lb.value()).ok()?;
            return left.partial_cmp(&right);
        }
    }
    None
}

fn is_duration_datatype(dt: NamedNodeRef<'_>) -> bool {
    dt == xsd::DURATION || dt == xsd::YEAR_MONTH_DURATION || dt == xsd::DAY_TIME_DURATION
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
    } else if datatype == xsd::DATE {
        Date::from_str(value).is_ok()
    } else if datatype == xsd::TIME {
        Time::from_str(value).is_ok()
    } else if datatype == xsd::DURATION {
        Duration::from_str(value).is_ok()
    } else if datatype == xsd::YEAR_MONTH_DURATION {
        YearMonthDuration::from_str(value).is_ok()
    } else if datatype == xsd::DAY_TIME_DURATION {
        DayTimeDuration::from_str(value).is_ok()
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

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::Literal;
    use std::cmp::Ordering;

    fn lit(value: &str, datatype: &str) -> Term {
        Term::Literal(Literal::new_typed_literal(
            value,
            oxrdf::NamedNode::new(datatype).unwrap(),
        ))
    }
    fn date(v: &str) -> Term { lit(v, "http://www.w3.org/2001/XMLSchema#date") }
    fn time(v: &str) -> Term { lit(v, "http://www.w3.org/2001/XMLSchema#time") }
    fn dur(v: &str) -> Term { lit(v, "http://www.w3.org/2001/XMLSchema#duration") }
    fn ymd(v: &str) -> Term { lit(v, "http://www.w3.org/2001/XMLSchema#yearMonthDuration") }
    fn dtd(v: &str) -> Term { lit(v, "http://www.w3.org/2001/XMLSchema#dayTimeDuration") }

    #[test]
    fn date_ordering() {
        assert_eq!(compare_terms(&date("2020-01-01"), &date("2020-06-01")), Some(Ordering::Less));
        assert_eq!(compare_terms(&date("2020-06-01"), &date("2020-01-01")), Some(Ordering::Greater));
        assert_eq!(compare_terms(&date("2020-03-15"), &date("2020-03-15")), Some(Ordering::Equal));
    }

    #[test]
    fn time_ordering() {
        assert_eq!(compare_terms(&time("08:00:00"), &time("17:30:00")), Some(Ordering::Less));
        assert_eq!(compare_terms(&time("23:59:59"), &time("00:00:00")), Some(Ordering::Greater));
        assert_eq!(compare_terms(&time("12:00:00"), &time("12:00:00")), Some(Ordering::Equal));
    }

    #[test]
    fn duration_ordering() {
        assert_eq!(compare_terms(&dur("P1Y"), &dur("P2Y")), Some(Ordering::Less));
        assert_eq!(compare_terms(&dur("P30D"), &dur("P1D")), Some(Ordering::Greater));
        assert_eq!(compare_terms(&dur("P1Y"), &dur("P1Y")), Some(Ordering::Equal));
    }

    #[test]
    fn year_month_duration_ordering() {
        assert_eq!(compare_terms(&ymd("P1Y"), &ymd("P13M")), Some(Ordering::Less));
        assert_eq!(compare_terms(&ymd("P2Y"), &ymd("P1Y")), Some(Ordering::Greater));
    }

    #[test]
    fn day_time_duration_ordering() {
        assert_eq!(compare_terms(&dtd("PT1H"), &dtd("PT2H")), Some(Ordering::Less));
        assert_eq!(compare_terms(&dtd("P2D"), &dtd("P1D")), Some(Ordering::Greater));
    }

    #[test]
    fn genuinely_incomparable_durations() {
        // P1M and P30D are incomparable: a month is 28–31 days so no consistent ordering
        assert_eq!(compare_terms(&dur("P1M"), &dur("P30D")), None);
    }

    #[test]
    fn cross_type_incomparable() {
        assert_eq!(compare_terms(&date("2020-01-01"), &time("12:00:00")), None);
        assert_eq!(compare_terms(&date("2020-01-01"), &dur("P1Y")), None);
    }

    #[test]
    fn valid_lexical_form_date() {
        let good = Literal::new_typed_literal("2020-01-01",
            oxrdf::NamedNode::new("http://www.w3.org/2001/XMLSchema#date").unwrap());
        let bad = Literal::new_typed_literal("not-a-date",
            oxrdf::NamedNode::new("http://www.w3.org/2001/XMLSchema#date").unwrap());
        assert!(valid_lexical_form(&good));
        assert!(!valid_lexical_form(&bad));
    }

    #[test]
    fn valid_lexical_form_duration() {
        let good = Literal::new_typed_literal("P1Y2M3DT4H",
            oxrdf::NamedNode::new("http://www.w3.org/2001/XMLSchema#duration").unwrap());
        let bad = Literal::new_typed_literal("not-a-duration",
            oxrdf::NamedNode::new("http://www.w3.org/2001/XMLSchema#duration").unwrap());
        assert!(valid_lexical_form(&good));
        assert!(!valid_lexical_form(&bad));
    }
}
