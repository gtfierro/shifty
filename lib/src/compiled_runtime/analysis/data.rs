use crate::compiled_runtime::analysis::{AnalysisContext, AnalysisPhase, AnalysisState, Analyzer};

pub struct DataGraphProfileAnalyzer;

impl Analyzer for DataGraphProfileAnalyzer {
    fn id(&self) -> &'static str {
        "data-profile"
    }

    fn phase(&self) -> AnalysisPhase {
        AnalysisPhase::Runtime
    }

    fn run(&self, ctx: &AnalysisContext<'_>, state: &mut AnalysisState) -> Result<(), String> {
        let quad_count = ctx
            .store
            .quads_for_pattern(None, None, None, ctx.data_graph)
            .filter_map(Result::ok)
            .count() as u64;
        state
            .counters
            .insert("runtime_quad_count".to_string(), quad_count);
        state.notes.insert(
            self.id().to_string(),
            "counted quads in selected runtime data graph".to_string(),
        );
        Ok(())
    }
}
