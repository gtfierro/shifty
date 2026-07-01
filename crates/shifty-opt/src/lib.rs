//! Static analysis, normalization, and planning (Layers 4-5).
//!
//! Layer 4 starts here: the polarity-aware shape [`deps`]endency graph and
//! [`strata`]ification analysis that realize the recursion semantics decided in
//! `docs/03-recursion-semantics.md` (stratified; diagnose non-stratifiable).
//! Normalization and logical→physical planning follow.

pub mod deps;
pub mod normalize;
pub mod plan;
pub mod rule_deps;
pub mod sparql_native;
pub mod strata;

pub use deps::{DepEdge, Polarity, dependency_edges};
pub use normalize::normalize;
pub use plan::{FocusSource, PhysicalPlan, StatementPlan, plan, plan_no_seeding, plan_no_sort};
pub use rule_deps::{
    RuleDependencies, rule_dependencies, rule_guard_dependencies, selector_dependencies,
};
pub use sparql_native::{
    ClosureKind, ExprPlan, GraphScan, NativeOp, NativeQueryPlan, OpId, PathScan, PlanStats,
    QueryForm, ScanTerm, TripleScan, VarId, lower_query, lower_query_with_stats,
};
pub use strata::{Stratification, Stratum, analyze};
