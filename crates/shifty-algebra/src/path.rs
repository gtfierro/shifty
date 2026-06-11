//! The path algebra `π` (doc 00 §2, Def. 3).
//!
//! ```text
//! π ::= id | q | π⁻ | π · π′ | π ∪ π′ | π*        (q ∈ P)
//! ```
//!
//! Paths are finite trees (no cycles), so plain `Box`/`Vec` nesting suffices.
//! The smart constructors apply the *light* Kleene-with-converse laws that are
//! always sound and cheap; heavier canonicalization is Layer 4 (`shifty-opt`).

use crate::term::NamedNode;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Path {
    /// `id` — the identity relation / empty word.
    Id,
    /// `q` — a single predicate step.
    Pred(NamedNode),
    /// `π⁻` — inverse.
    Inverse(Box<Path>),
    /// `π · π′ · …` — sequential composition.
    Seq(Vec<Path>),
    /// `π ∪ π′ ∪ …` — alternation (relational union).
    Alt(Vec<Path>),
    /// `π*` — reflexive-transitive closure.
    Star(Box<Path>),
}

impl Path {
    /// Compose paths, flattening nested `Seq` and dropping `id` (the unit of
    /// composition, `π · id = π`). An empty sequence is `id`.
    pub fn seq(parts: Vec<Path>) -> Path {
        let mut flat = Vec::with_capacity(parts.len());
        for p in parts {
            match p {
                Path::Id => {}
                Path::Seq(inner) => flat.extend(inner),
                other => flat.push(other),
            }
        }
        match flat.len() {
            0 => Path::Id,
            1 => flat.pop().unwrap(),
            _ => Path::Seq(flat),
        }
    }

    /// Alternate paths, flattening nested `Alt`. An empty alternation is the
    /// empty relation (kept explicit as `Alt([])` — it matches nothing).
    pub fn alt(parts: Vec<Path>) -> Path {
        let mut flat = Vec::with_capacity(parts.len());
        for p in parts {
            match p {
                Path::Alt(inner) => flat.extend(inner),
                other => flat.push(other),
            }
        }
        if flat.len() == 1 {
            flat.pop().unwrap()
        } else {
            Path::Alt(flat)
        }
    }

    /// Invert, using `id⁻ = id` and the involution `(π⁻)⁻ = π`.
    pub fn inverse(self) -> Path {
        match self {
            Path::Id => Path::Id,
            Path::Inverse(inner) => *inner,
            other => Path::Inverse(Box::new(other)),
        }
    }

    /// Reflexive-transitive closure, using `id* = id` and `(π*)* = π*`.
    pub fn star(self) -> Path {
        match self {
            Path::Id => Path::Id,
            Path::Star(_) => self,
            other => Path::Star(Box::new(other)),
        }
    }

    /// `π⁺ = π · π*` — one-or-more sugar (gap-analysis **P1**).
    pub fn one_or_more(self) -> Path {
        let star = self.clone().star();
        Path::seq(vec![self, star])
    }

    /// `π? = π ∪ id` — zero-or-one sugar (gap-analysis **P1**).
    pub fn zero_or_one(self) -> Path {
        Path::alt(vec![self, Path::Id])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pred(s: &str) -> Path {
        Path::Pred(NamedNode::new(format!("http://ex/{s}")).unwrap())
    }

    #[test]
    fn seq_flattens_and_drops_id() {
        let p = Path::seq(vec![
            pred("a"),
            Path::Id,
            Path::seq(vec![pred("b"), pred("c")]),
        ]);
        assert_eq!(p, Path::Seq(vec![pred("a"), pred("b"), pred("c")]));
    }

    #[test]
    fn seq_units() {
        assert_eq!(Path::seq(vec![]), Path::Id);
        assert_eq!(Path::seq(vec![Path::Id, Path::Id]), Path::Id);
        assert_eq!(Path::seq(vec![pred("a")]), pred("a"));
    }

    #[test]
    fn alt_flattens_and_collapses_singleton() {
        let p = Path::alt(vec![pred("a"), Path::alt(vec![pred("b"), pred("c")])]);
        assert_eq!(p, Path::Alt(vec![pred("a"), pred("b"), pred("c")]));
        assert_eq!(Path::alt(vec![pred("a")]), pred("a"));
    }

    #[test]
    fn inverse_is_involution() {
        assert_eq!(pred("a").inverse().inverse(), pred("a"));
        assert_eq!(Path::Id.inverse(), Path::Id);
    }

    #[test]
    fn star_is_idempotent() {
        assert_eq!(pred("a").star().star(), pred("a").star());
        assert_eq!(Path::Id.star(), Path::Id);
    }

    #[test]
    fn sugar_expansions() {
        assert_eq!(
            pred("a").one_or_more(),
            Path::Seq(vec![pred("a"), Path::Star(Box::new(pred("a")))])
        );
        assert_eq!(
            pred("a").zero_or_one(),
            Path::Alt(vec![pred("a"), Path::Id])
        );
    }
}
