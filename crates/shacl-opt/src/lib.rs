//! Static analysis, normalization, and planning (Layers 4-5).
//!
//! Layer 4 starts here: the polarity-aware shape [`deps`]endency graph and
//! [`strata`]ification analysis that realize the recursion semantics decided in
//! `docs/03-recursion-semantics.md` (stratified; diagnose non-stratifiable).
//! Normalization and logical→physical planning follow.

pub mod deps;
pub mod strata;

pub use deps::{dependency_edges, DepEdge, Polarity};
pub use strata::{analyze, Stratification, Stratum};
