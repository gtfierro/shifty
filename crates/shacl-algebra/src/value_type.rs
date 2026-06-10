//! Value types `T` — the `test(τ)` facets (doc 00 §1).
//!
//! A `ValueType` is a decidable predicate on literal values, `⟦τ⟧ ⊆ V`. The
//! paper folds datatype, numeric ranges, string length, and regex into this one
//! abstraction (Appendix B); we add language membership (gap-analysis **L1**).

use crate::term::{Literal, NamedNode};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// A numeric/ordered bound for `sh:min/maxInclusive` / `sh:min/maxExclusive`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Bound {
    pub value: Literal,
    pub inclusive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ValueType {
    /// `any` — `⟦any⟧ = V`.
    Any,
    /// `sh:datatype`.
    Datatype(NamedNode),
    /// `sh:minInclusive` / `sh:minExclusive` / `sh:maxInclusive` / `sh:maxExclusive`.
    NumericRange { lo: Option<Bound>, hi: Option<Bound> },
    /// `sh:minLength` / `sh:maxLength`.
    Length { min: Option<u64>, max: Option<u64> },
    /// `sh:pattern` (+ `sh:flags`).
    Pattern { regex: String, flags: String },
    /// `sh:languageIn` (gap-analysis **L1**).
    LangIn(Vec<String>),
    /// Intersection of facets (e.g. a datatype stacked with a length bound).
    And(Vec<ValueType>),
}

impl ValueType {
    /// Intersect facets, flattening nested `And` and dropping `Any` (the unit of
    /// intersection). An empty intersection is `Any`.
    pub fn and(parts: Vec<ValueType>) -> ValueType {
        let mut flat = Vec::with_capacity(parts.len());
        for p in parts {
            match p {
                ValueType::Any => {}
                ValueType::And(inner) => flat.extend(inner),
                other => flat.push(other),
            }
        }
        match flat.len() {
            0 => ValueType::Any,
            1 => flat.pop().unwrap(),
            _ => ValueType::And(flat),
        }
    }

    /// Tighten an intersection of facets and detect same-family unsat
    /// (`docs/04-normalization.md` §4).
    ///
    /// Returns `None` when the facet set is unsatisfiable (⊥), `Some(Any)` when
    /// it imposes no constraint (⊤), else the tightened facet. Each rewrite is
    /// per-value truth-functional, hence sound under the gfp semantics.
    ///
    /// Scope: merges same-family bounds (numeric ranges, lengths), folds an
    /// empty range / `len.min > max` / two distinct datatypes to ⊥. Numeric
    /// ordering covers xsd numeric only; incomparable bounds (mixed/non-numeric
    /// datatypes) are *kept*, never merged or declared unsat. Cross-facet unsat
    /// (`datatype(xsd:string) ∧ NumericRange`) is deferred (§4 [todo]).
    pub fn normalize(&self) -> Option<ValueType> {
        let flat = match ValueType::and(vec![self.clone()]) {
            ValueType::And(parts) => parts,
            ValueType::Any => return Some(ValueType::Any),
            single => vec![single],
        };

        let mut lo: Option<Bound> = None;
        let mut hi: Option<Bound> = None;
        let mut len_min: Option<u64> = None;
        let mut len_max: Option<u64> = None;
        let mut has_range = false;
        let mut has_length = false;
        let mut datatype: Option<NamedNode> = None;
        // Facets we don't merge here (Pattern, LangIn) plus range bounds that
        // were incomparable with the running bound and so couldn't be folded.
        let mut rest: Vec<ValueType> = Vec::new();

        for f in flat {
            match f {
                ValueType::Any => {}
                ValueType::NumericRange { lo: ilo, hi: ihi } => {
                    has_range = true;
                    if let Some(b) = ilo {
                        lo = fold_bound(lo, b, Side::Lo, &mut rest);
                    }
                    if let Some(b) = ihi {
                        hi = fold_bound(hi, b, Side::Hi, &mut rest);
                    }
                }
                ValueType::Length { min, max } => {
                    has_length = true;
                    len_min = tighter_lower(len_min, min);
                    len_max = tighter_upper(len_max, max);
                }
                ValueType::Datatype(d) => match &datatype {
                    Some(prev) if *prev != d => return None, // two distinct datatypes ⇒ ⊥
                    _ => datatype = Some(d),
                },
                other => rest.push(other),
            }
        }

        // Same-family unsat.
        if range_is_empty(&lo, &hi) {
            return None;
        }
        if let (Some(mn), Some(mx)) = (len_min, len_max)
            && mn > mx
        {
            return None;
        }

        // Reassemble in a canonical order; `and` re-flattens and drops `Any`.
        let mut parts = Vec::new();
        if let Some(d) = datatype {
            parts.push(ValueType::Datatype(d));
        }
        if has_range && !(lo.is_none() && hi.is_none()) {
            parts.push(ValueType::NumericRange { lo, hi });
        }
        if has_length && !(len_min.is_none() && len_max.is_none()) {
            parts.push(ValueType::Length { min: len_min, max: len_max });
        }
        parts.extend(rest);
        Some(ValueType::and(parts))
    }
}

/// Which end of a `NumericRange` a bound constrains.
#[derive(Clone, Copy)]
enum Side {
    Lo,
    Hi,
}

/// The tighter lower length bound (larger min); `None` is no bound.
fn tighter_lower(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (s, None) | (None, s) => s,
    }
}

/// The tighter upper length bound (smaller max); `None` is no bound.
fn tighter_upper(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (s, None) | (None, s) => s,
    }
}

/// Fold an incoming bound into the running bound on one side, keeping the
/// tighter of the two. If the two are not numerically comparable we cannot
/// merge them, so the incoming bound is preserved as its own range facet in
/// `rest` and the running bound is left unchanged (conjunction is order- and
/// grouping-independent, so this stays sound — we just tighten less).
fn fold_bound(cur: Option<Bound>, new: Bound, side: Side, rest: &mut Vec<ValueType>) -> Option<Bound> {
    let Some(c) = cur else { return Some(new) };
    match compare_literals(&c.value, &new.value) {
        Some(Ordering::Equal) => {
            // same value: the exclusive bound is tighter
            Some(Bound { value: c.value, inclusive: c.inclusive && new.inclusive })
        }
        Some(ord) => {
            let cur_tighter = match side {
                Side::Lo => ord == Ordering::Greater, // larger lower bound is tighter
                Side::Hi => ord == Ordering::Less,    // smaller upper bound is tighter
            };
            if cur_tighter { Some(c) } else { Some(new) }
        }
        None => {
            rest.push(match side {
                Side::Lo => ValueType::NumericRange { lo: Some(new), hi: None },
                Side::Hi => ValueType::NumericRange { lo: None, hi: Some(new) },
            });
            Some(c)
        }
    }
}

/// Is `[lo, hi]` empty? `lo > hi`, or `lo == hi` with either end exclusive.
/// Only decides when the two bounds are numerically comparable.
fn range_is_empty(lo: &Option<Bound>, hi: &Option<Bound>) -> bool {
    let (Some(l), Some(h)) = (lo, hi) else { return false };
    match compare_literals(&l.value, &h.value) {
        Some(Ordering::Greater) => true,
        Some(Ordering::Equal) => !(l.inclusive && h.inclusive),
        _ => false,
    }
}

/// Numeric comparison of two literals; `None` when not comparable in this slice
/// (non-numeric datatypes or unparseable lexical forms). Covers xsd numeric
/// only — dateTime/duration ordering is deferred (see `docs/BACKLOG.md`).
fn compare_literals(a: &Literal, b: &Literal) -> Option<Ordering> {
    let x: f64 = a.value().parse().ok()?;
    let y: f64 = b.value().parse().ok()?;
    x.partial_cmp(&y)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn and_flattens_and_drops_any() {
        let xsd_string = NamedNode::new("http://www.w3.org/2001/XMLSchema#string").unwrap();
        let dt = ValueType::Datatype(xsd_string);
        let len = ValueType::Length { min: Some(1), max: None };
        let combined = ValueType::and(vec![
            ValueType::Any,
            dt.clone(),
            ValueType::and(vec![len.clone(), ValueType::Any]),
        ]);
        assert_eq!(combined, ValueType::And(vec![dt, len]));
    }

    #[test]
    fn and_units() {
        assert_eq!(ValueType::and(vec![]), ValueType::Any);
        assert_eq!(
            ValueType::and(vec![ValueType::Any, ValueType::Any]),
            ValueType::Any
        );
    }

    fn int(n: i64) -> Literal {
        Literal::new_typed_literal(
            n.to_string(),
            NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap(),
        )
    }

    fn incl(n: i64) -> Bound {
        Bound { value: int(n), inclusive: true }
    }

    fn excl(n: i64) -> Bound {
        Bound { value: int(n), inclusive: false }
    }

    #[test]
    fn empty_range_is_unsat() {
        // [5, 3] is empty
        let vt = ValueType::NumericRange { lo: Some(incl(5)), hi: Some(incl(3)) };
        assert_eq!(vt.normalize(), None);
    }

    #[test]
    fn point_range_exclusive_end_is_unsat() {
        // [5, 5) is empty
        let vt = ValueType::NumericRange { lo: Some(incl(5)), hi: Some(excl(5)) };
        assert_eq!(vt.normalize(), None);
        // but [5, 5] is a single point, satisfiable
        let pt = ValueType::NumericRange { lo: Some(incl(5)), hi: Some(incl(5)) };
        assert!(pt.normalize().is_some());
    }

    #[test]
    fn merges_numeric_ranges() {
        // (≥1) ∧ (≤10) → [1, 10]
        let vt = ValueType::and(vec![
            ValueType::NumericRange { lo: Some(incl(1)), hi: None },
            ValueType::NumericRange { lo: None, hi: Some(incl(10)) },
        ]);
        assert_eq!(
            vt.normalize(),
            Some(ValueType::NumericRange { lo: Some(incl(1)), hi: Some(incl(10)) })
        );
    }

    #[test]
    fn merges_to_tighter_bounds() {
        // (≥1) ∧ (≥5) → ≥5 ; (≤10) ∧ (≤3) → ≤3 ⇒ [5, 3] ⇒ ⊥
        let vt = ValueType::and(vec![
            ValueType::NumericRange { lo: Some(incl(1)), hi: Some(incl(10)) },
            ValueType::NumericRange { lo: Some(incl(5)), hi: Some(incl(3)) },
        ]);
        assert_eq!(vt.normalize(), None);
    }

    #[test]
    fn exclusive_wins_on_tie() {
        // (≥5 incl) ∧ (≥5 excl) → ≥5 excl
        let vt = ValueType::and(vec![
            ValueType::NumericRange { lo: Some(incl(5)), hi: None },
            ValueType::NumericRange { lo: Some(excl(5)), hi: None },
        ]);
        assert_eq!(
            vt.normalize(),
            Some(ValueType::NumericRange { lo: Some(excl(5)), hi: None })
        );
    }

    #[test]
    fn distinct_datatypes_are_unsat() {
        let xsd_int = NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap();
        let xsd_string = NamedNode::new("http://www.w3.org/2001/XMLSchema#string").unwrap();
        let vt = ValueType::and(vec![
            ValueType::Datatype(xsd_int),
            ValueType::Datatype(xsd_string),
        ]);
        assert_eq!(vt.normalize(), None);
    }

    #[test]
    fn merges_length_bounds_and_unsat() {
        // minLength 2 ∧ maxLength 5 → {2..5}
        let ok = ValueType::and(vec![
            ValueType::Length { min: Some(2), max: None },
            ValueType::Length { min: None, max: Some(5) },
        ]);
        assert_eq!(ok.normalize(), Some(ValueType::Length { min: Some(2), max: Some(5) }));
        // minLength 5 ∧ maxLength 2 → ⊥
        let bad = ValueType::and(vec![
            ValueType::Length { min: Some(5), max: None },
            ValueType::Length { min: None, max: Some(2) },
        ]);
        assert_eq!(bad.normalize(), None);
    }

    #[test]
    fn incomparable_bounds_are_kept_not_dropped() {
        // An integer lower bound and a non-numeric "date" upper bound can't be
        // compared in this slice: keep both, declare neither unsat.
        let date = Literal::new_typed_literal(
            "2020-01-01",
            NamedNode::new("http://www.w3.org/2001/XMLSchema#date").unwrap(),
        );
        let vt = ValueType::and(vec![
            ValueType::NumericRange { lo: Some(incl(1)), hi: None },
            ValueType::NumericRange { lo: None, hi: Some(Bound { value: date, inclusive: true }) },
        ]);
        let n = vt.normalize().expect("not unsat");
        // both bounds still present somewhere in the result
        let s = format!("{n:?}");
        assert!(s.contains("date"), "date bound dropped: {s}");
    }

    #[test]
    fn any_normalizes_to_any() {
        assert_eq!(ValueType::Any.normalize(), Some(ValueType::Any));
    }
}
