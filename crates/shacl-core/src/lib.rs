#![deny(clippy::all)]

pub mod algebra;
pub mod analysis;
pub mod diagnostics;
pub mod parse;
pub mod passes;
pub mod render;
pub mod source;
pub mod syntax;

pub use analysis::{AnalysisSummary, analyze_program};
pub use parse::{load_and_parse_with_ontoenv, parse_quads, parse_resolved};
pub use render::render_shape_program_dot;
pub use passes::lower_to_program;
