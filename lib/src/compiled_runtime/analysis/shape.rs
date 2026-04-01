use crate::compiled_runtime::analysis::{AnalysisContext, AnalysisPhase, AnalysisState, Analyzer};

pub struct ShapeStructureAnalyzer;

impl Analyzer for ShapeStructureAnalyzer {
    fn id(&self) -> &'static str {
        "shape-structure"
    }

    fn phase(&self) -> AnalysisPhase {
        AnalysisPhase::Static
    }

    fn run(&self, ctx: &AnalysisContext<'_>, state: &mut AnalysisState) -> Result<(), String> {
        state
            .counters
            .insert("shape_count".to_string(), ctx.program.shapes.len() as u64);
        state.counters.insert(
            "component_count".to_string(),
            ctx.program.components.len() as u64,
        );
        state.notes.insert(
            self.id().to_string(),
            "indexed shape/component table sizes".to_string(),
        );
        Ok(())
    }
}
