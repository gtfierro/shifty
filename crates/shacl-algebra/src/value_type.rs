//! Value types `T` — the `test(τ)` facets (doc 00 §1).
//!
//! A `ValueType` is a decidable predicate on literal values, `⟦τ⟧ ⊆ V`. The
//! paper folds datatype, numeric ranges, string length, and regex into this one
//! abstraction (Appendix B); we add language membership (gap-analysis **L1**).

use crate::term::{Literal, NamedNode};
use serde::{Deserialize, Serialize};

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
}
