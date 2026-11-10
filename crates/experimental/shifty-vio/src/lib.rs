//! Violation generation for knowledge-graph repair benchmarks.
//!
//! **Experimental**: this crate may change in any release. Depend on it
//! through `shifty-experimental` (`shifty_experimental::vio`).
//!
//! The generator injects SHACL violations with known ground-truth repairs
//! into a *reference* graph that conforms to a shapes graph. It follows Lin
//! et al., "Systematic Evaluation of Knowledge Graph Repair with Large
//! Language Models" (arXiv 2507.22419), extended to multiple interacting
//! violations as in "Selective Retrieval Routing for Multi-Violation
//! Knowledge Graph Repair" (arXiv 2611.07342). Shifty's evidence engine and
//! shape algebra do the work the original implementation wrote per SHACL
//! component.
//!
//! The mental model has three nouns:
//!
//! - A [`Universe`] is every way to break the reference graph. For every
//!   passing `(focus, statement)` pair, the satisfaction
//!   evidence explains *why* the shape holds. `VIO(x, φ)` rewrites it into
//!   alternative edit sets that make `φ` fail at `x`. A failure inside `¬φ`
//!   flips to the dual `SAT(x, φ)`, which makes a failing sub-shape hold.
//!   The rules run over the normalized algebra (`∧ ∨ ¬ ∃≥n ∃≤n ∀ test
//!   closed`), so they cover every SHACL component that lowers to it.
//!   The expansion streams: draws sample it as it runs, and listing the
//!   universe caches it. [`Universe::filter`] and [`Universe::weighted`]
//!   derive views that share the session and caches.
//! - An [`Injection`] is one member: [`Operation`]s that together make one
//!   pair fail, identified by its net delta ([`Injection::key`]).
//! - A [`Case`] is what [`Universe::draw`] returns for a budget `k` and a
//!   seed: up to `k` compatible injections, the broken graph, and the
//!   ground-truth repair. A weighted reservoir with Efraimidis–Spirakis keys
//!   `-ln h(v) / w(v)` picks candidates, where `h` is a stable hash. Every
//!   injection is equally likely unless [`Weights`] weighs predicates or
//!   operation kinds. A candidate is admitted only if its edits can be
//!   applied with those already chosen and it makes its target fail
//!   alone. The composed delta is revalidated, so every
//!   [`CaseFailure`] is one the engine reports.
//!
//! ```
//! use shifty_vio::{Options, Universe};
//!
//! let shapes = r#"
//!     @prefix sh: <http://www.w3.org/ns/shacl#> .
//!     @prefix ex: <http://ex/> .
//!     ex:S a sh:NodeShape ; sh:targetClass ex:C ;
//!         sh:property [ sh:path ex:p ; sh:minCount 1 ; sh:maxCount 1 ] .
//! "#;
//! let data = "<http://ex/a> a <http://ex/C> ; <http://ex/p> 1 .";
//! let shapes = shifty_parse::load_turtle(shapes.as_bytes(), None).unwrap();
//! let data = shifty_parse::load_turtle(data.as_bytes(), None).unwrap().graph;
//! let universe = Universe::new(shapes, data, Options::default()).unwrap();
//! assert!(!universe.is_empty());
//! let case = universe.draw(1, 0).unwrap();
//! assert_eq!(case.injections.len(), 1);
//! assert_eq!(case.failures[0].injections, [0]);
//! ```
//!
//! Terms are N-Triples strings throughout (see [`nt`]): they order totally,
//! so a universe and its draws reproduce in every process.

mod expand;
pub mod hash;
mod index;
mod model;
pub mod nt;
mod propose;
mod universe;

pub use model::{Case, CaseFailure, Injection, Operation, OperationKind, Polarity};
pub use nt::Triple;
pub use universe::{Options, Universe, Weights};

use std::fmt;

/// Bounds on the expansion. Each caps a branching factor of the rewrite
/// system: without them a large graph has an astronomical universe, since
/// every value can be replaced in many ways.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpansionLimits {
    /// Alternatives kept at any one choice point.
    pub max_alternatives: usize,
    /// Values sampled per count or universal before branching.
    pub max_values: usize,
    /// Replacement or new terms tried per value.
    pub max_candidates: usize,
    /// Joint choices kept when several values must change together.
    pub max_combinations: usize,
    /// Nesting depth across `sh:node` and `sh:qualifiedValueShape`.
    pub max_depth: usize,
    /// Injections emitted per `(focus, statement)` pair.
    pub max_per_target: usize,
    /// Total injections before the traversal stops.
    pub max_injections: usize,
}

impl Default for ExpansionLimits {
    fn default() -> Self {
        Self {
            max_alternatives: 24,
            max_values: 6,
            max_candidates: 3,
            max_combinations: 12,
            max_depth: 12,
            max_per_target: 64,
            max_injections: 200_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// The shapes graph does not compile.
    Compile(String),
    /// The engine rejected the session, for example over unsupported
    /// features.
    Session(String),
    Evaluation(String),
    /// A sampling weight that is not positive and finite.
    Weight {
        name: String,
        weight: f64,
    },
    /// A draw with a budget below one.
    Budget,
    /// A draw from a universe with no injections.
    Empty,
    /// An edit with a term the engine cannot hold in that position.
    Term(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compile(e) => write!(f, "shapes do not compile: {e}"),
            Self::Session(e) => f.write_str(e),
            Self::Evaluation(e) => write!(f, "evaluation failed: {e}"),
            Self::Weight { name, weight } => {
                write!(f, "weight for {name} must be positive, got {weight}")
            }
            Self::Budget => f.write_str("k must be at least 1"),
            Self::Empty => f.write_str("the universe has no injections to draw from"),
            Self::Term(t) => write!(f, "edit is not a valid triple: {t}"),
        }
    }
}

impl std::error::Error for Error {}
