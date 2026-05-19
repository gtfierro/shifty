#![deny(clippy::all)]

pub mod algebra;
pub mod analysis;
pub mod backend_views;
pub mod data_graph;
pub mod diagnostics;
pub mod execute;
pub mod parse;
pub mod passes;
pub mod plan;
pub mod render;
pub mod report;
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
pub use data_graph::{
    ClassDataStats, DEFAULT_SUBJECT_SAMPLE_BUDGET, DataGraphSummary, DataSummaryMetadata,
    DataSummaryOptions, FanoutClass, PlanningEstimationMode, PlanningIndex, PredicateDataStats,
    SelectivityClass, ShapeDataSummary, TargetEstimate, summarize_data_graph,
    summarize_data_graph_with_options,
};
pub use execute::{
    ValidationBackend, ValidationCoverage, ValidationHeatmap, ValidationResult,
    ValidationRuleProfile, ValidationTraceEvent, ValidationUnsupported, ValidationViolation,
};
pub use parse::{load_and_parse_with_ontoenv, parse_quads, parse_resolved};
pub use passes::{
    NormalizeOptions, canonicalize_program, lower_to_program, normalize_program,
    prune_deactivated_program,
};
pub use plan::{
    BackendPlans, ConstraintBatchAnnotation as ValidationConstraintBatchAnnotation, InferencePlan,
    InferencePlanNode, LogicalPlanSummary, PlanningStageTiming,
    TargetScanAnnotation as ValidationTargetScanAnnotation, ValidationPlan,
    ValidationPlanAnnotations, ValidationPlanNode, ValidationPlanningBenchmark,
    ValidationPlanningMetadata, ValidationPlanningOptions, ValidationPlanningResult,
    derive_backend_logical_plans, derive_inference_logical_plan,
    derive_inference_logical_plan_from_view, derive_validation_logical_plan,
    derive_validation_logical_plan_detailed, derive_validation_logical_plan_from_view,
    derive_validation_logical_plan_with_data, derive_validation_logical_plan_with_options,
    derive_validation_logical_plan_with_options_detailed,
};
pub use render::render_shape_program_dot;
pub use report::{ValidationReportGraph, build_validation_report};
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
