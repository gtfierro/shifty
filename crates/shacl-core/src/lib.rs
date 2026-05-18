#![deny(clippy::all)]

pub mod algebra;
pub mod analysis;
pub mod backend_views;
pub mod diagnostics;
pub mod parse;
pub mod passes;
pub mod plan;
pub mod render;
pub mod rewrite;
pub mod source;
pub mod static_analysis;
pub mod syntax;

pub use analysis::{AnalysisSummary, analyze_program};
pub use backend_views::{
    BackendBucket, BackendClosureMode, BackendShapePartition, BackendViewOptions, BackendViews,
    ClassifiedDependency, DependencyClass, InferenceView, InferenceWorkInventory, SharedWorkUnit,
    SharedWorkUnitKind, ValidationView, ValidationWorkInventory, derive_backend_views,
    derive_inference_view, derive_validation_view,
};
pub use parse::{load_and_parse_with_ontoenv, parse_quads, parse_resolved};
pub use passes::{
    NormalizeOptions, canonicalize_program, lower_to_program, normalize_program,
    prune_deactivated_program,
};
pub use plan::{
    BackendPlans, InferencePlan, InferencePlanNode, LogicalPlanSummary, ValidationPlan,
    ValidationPlanNode, derive_backend_logical_plans, derive_inference_logical_plan,
    derive_inference_logical_plan_from_view, derive_validation_logical_plan,
    derive_validation_logical_plan_from_view,
};
pub use render::render_shape_program_dot;
pub use rewrite::{
    RecursiveRegion, RewriteOptions, RewritePassRecord, RewriteSummary, RewrittenProgram,
    rewrite_program,
};
pub use static_analysis::{
    ContextFootprint, ContextFootprintReport, FingerprintReport, ProgramSlice, SharedWorkReport,
    SliceReason, SliceRoots, StaticAnalysisSummary, StaticCostHint, StaticCostHintReport,
    analyze_static, analyze_static_with_roots, context_requirements, fingerprint_program,
    shared_work_candidates, slice_program, static_cost_hints,
};
