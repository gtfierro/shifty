#![deny(clippy::all)]

pub mod algebra;
pub mod analysis;
pub mod diagnostics;
pub mod parse;
pub mod passes;
pub mod render;
pub mod source;
pub mod static_analysis;
pub mod syntax;

pub use analysis::{AnalysisSummary, analyze_program};
pub use parse::{load_and_parse_with_ontoenv, parse_quads, parse_resolved};
pub use passes::{
    NormalizeOptions, canonicalize_program, lower_to_program, normalize_program,
    prune_deactivated_program,
};
pub use render::render_shape_program_dot;
pub use static_analysis::{
    ContextFootprint, ContextFootprintReport, FingerprintReport, ProgramSlice, SharedWorkReport,
    SliceReason, SliceRoots, StaticAnalysisSummary, StaticCostHint, StaticCostHintReport,
    analyze_static, analyze_static_with_roots, context_requirements, fingerprint_program,
    shared_work_candidates, slice_program, static_cost_hints,
};
