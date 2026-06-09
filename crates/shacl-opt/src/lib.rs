//! Static analysis, normalization, and planning (Layers 4-5).
//!
//! Layer 4 starts here: the polarity-aware shape [`deps`]endency graph and
//! [`strata`]ification analysis that realize the recursion semantics decided in
//! `docs/03-recursion-semantics.md` (stratified; diagnose non-stratifiable).
//! Normalization and logical→physical planning follow.

pub mod deps;
pub mod normalize;
pub mod plan;
pub mod strata;

pub use deps::{dependency_edges, DepEdge, Polarity};
pub use normalize::normalize;
pub use plan::{plan, FocusSource, PhysicalPlan, StatementPlan};
pub use strata::{analyze, Stratification, Stratum};
