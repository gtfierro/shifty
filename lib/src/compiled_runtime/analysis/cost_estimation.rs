use crate::compiled_runtime::analysis::{AnalysisContext, AnalysisPhase, AnalysisState, Analyzer};
use crate::compiled_runtime::cost_model::{
    estimate_shape_component_complexity, ShapeCost,
};
use crate::shacl_ir::ComponentDescriptor;

/// Analyzer that estimates validation costs for shapes.
///
/// This analyzer computes cost estimates for each shape based on:
/// 1. Component complexity (sum of all constraint costs)
/// 2. Estimated target count (from static analysis or runtime data)
/// 3. Total cost (target_count × component_complexity)
///
/// Cost information can be used by optimizer passes to schedule shapes
/// more effectively (e.g., expensive shapes first to maximize parallelism).
pub struct CostEstimationAnalyzer;

impl Analyzer for CostEstimationAnalyzer {
    fn id(&self) -> &'static str {
        "cost-estimation"
    }

    fn phase(&self) -> AnalysisPhase {
        // Runtime phase because we want to use actual target counts if available
        AnalysisPhase::Runtime
    }

    fn run(&self, ctx: &AnalysisContext<'_>, state: &mut AnalysisState) -> Result<(), String> {
        let mut total_cost = 0u64;
        let mut max_cost = 0u64;
        let mut min_cost = u64::MAX;
        let mut shape_count = 0u64;

        let mut cost_distribution: Vec<ShapeCost> = Vec::new();

        // Analyze each shape in the program
        for shape in &ctx.program.shapes {
            // Get component descriptors for this shape
            // NOTE: Phase 2 (cost-based scheduling) is partially implemented.
            // Full data-driven costs require ComponentDescriptor parsing (see COST_MODEL_DESIGN.md).
            // Current implementation uses conservative fixed costs.
            let component_descriptors: Vec<ComponentDescriptor> = Vec::new();

            // Estimate component complexity
            // For now, use a conservative estimate based on component count
            let component_complexity = if component_descriptors.is_empty() {
                // Conservative estimate: assume moderate complexity per component
                (shape.component_ids.len() as u64) * 10
            } else {
                estimate_shape_component_complexity(&component_descriptors)
            };

            // Estimate target count
            // In static phase, we don't have actual target counts yet
            // Use target_ids count as a proxy (number of target expressions)
            let estimated_target_count = if shape.target_ids.is_empty() {
                // No explicit targets - might be invoked via sh:property
                // Assume moderate cardinality
                10
            } else {
                // Has explicit targets - will be evaluated
                // Use a heuristic: assume 10 nodes per target expression on average
                // This will be refined in runtime phase with actual counts
                shape.target_ids.len() * 10
            };

            // Calculate total cost for this shape
            let shape_cost = ShapeCost::new(
                shape.id,
                estimated_target_count,
                component_complexity,
            );

            total_cost = total_cost.saturating_add(shape_cost.total_cost);
            max_cost = max_cost.max(shape_cost.total_cost);
            min_cost = min_cost.min(shape_cost.total_cost);
            shape_count += 1;

            cost_distribution.push(shape_cost);
        }

        // Store statistics
        state
            .counters
            .insert("total_estimated_cost".to_string(), total_cost);
        state
            .counters
            .insert("max_shape_cost".to_string(), max_cost);
        state
            .counters
            .insert("min_shape_cost".to_string(), min_cost);
        state
            .counters
            .insert("shape_count".to_string(), shape_count);

        if shape_count > 0 {
            let avg_cost = total_cost / shape_count;
            state
                .counters
                .insert("avg_shape_cost".to_string(), avg_cost);
        }

        // Calculate cost distribution percentiles
        if !cost_distribution.is_empty() {
            cost_distribution.sort_by_key(|c| c.total_cost);

            let p50_idx = cost_distribution.len() / 2;
            let p90_idx = (cost_distribution.len() * 9) / 10;
            let p99_idx = (cost_distribution.len() * 99) / 100;

            state.counters.insert(
                "cost_p50".to_string(),
                cost_distribution[p50_idx].total_cost,
            );
            state.counters.insert(
                "cost_p90".to_string(),
                cost_distribution[p90_idx].total_cost,
            );
            state.counters.insert(
                "cost_p99".to_string(),
                cost_distribution[p99_idx].total_cost,
            );

            // Identify high-cost shapes (top 10%)
            let high_cost_threshold = cost_distribution[p90_idx].total_cost;
            let high_cost_shapes = cost_distribution
                .iter()
                .filter(|c| c.total_cost >= high_cost_threshold)
                .count() as u64;

            state.counters.insert(
                "high_cost_shapes".to_string(),
                high_cost_shapes,
            );
        }

        // Summary note
        if shape_count > 0 {
            let avg_cost = total_cost / shape_count;
            let cost_variance_pct = if avg_cost > 0 {
                ((max_cost as f64 - min_cost as f64) / avg_cost as f64 * 100.0).round() as u64
            } else {
                0
            };

            state.notes.insert(
                format!("{}_summary", self.id()),
                format!(
                    "Estimated {} shapes with avg cost {} (min: {}, max: {}, variance: {}%)",
                    shape_count, avg_cost, min_cost, max_cost, cost_variance_pct
                ),
            );
        } else {
            state.notes.insert(
                format!("{}_summary", self.id()),
                "No shapes found in program".to_string(),
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiled_runtime::analysis::AnalysisState;
    use crate::compiled_runtime::program::{
        CompiledProgram, ProgramHeader, ShapeKind, ShapeRow, StaticHints,
    };
    use oxigraph::store::Store;

    #[test]
    fn test_empty_program() {
        let program = CompiledProgram {
            header: ProgramHeader {
                schema_version: 1,
                min_kernel_version: 1,
                program_hash: [0; 32],
                feature_bits: 0,
            },
            terms: vec![],
            shapes: vec![],
            components: vec![],
            paths: vec![],
            targets: vec![],
            rules: vec![],
            shape_graph_triples: vec![],
            static_hints: StaticHints::default(),
            ext: Default::default(),
        };

        let store = Store::new().unwrap();
        let ctx = AnalysisContext {
            program: &program,
            store: &store,
            data_graph: None,
        };

        let analyzer = CostEstimationAnalyzer;
        let mut state = AnalysisState::default();

        analyzer.run(&ctx, &mut state).unwrap();

        assert_eq!(state.counters.get("shape_count"), Some(&0));
    }

    #[test]
    fn test_shapes_with_varying_complexity() {
        let program = CompiledProgram {
            header: ProgramHeader {
                schema_version: 1,
                min_kernel_version: 1,
                program_hash: [0; 32],
                feature_bits: 0,
            },
            terms: vec![],
            shapes: vec![
                ShapeRow {
                    id: 1,
                    kind: ShapeKind::Node,
                    term: 0,
                    target_ids: vec![1],
                    component_ids: vec![1], // 1 component
                    path_id: None,
                    deactivated: false,
                    severity: "Violation".to_string(),
                },
                ShapeRow {
                    id: 2,
                    kind: ShapeKind::Node,
                    term: 0,
                    target_ids: vec![1],
                    component_ids: vec![1, 2, 3, 4, 5], // 5 components
                    path_id: None,
                    deactivated: false,
                    severity: "Violation".to_string(),
                },
                ShapeRow {
                    id: 3,
                    kind: ShapeKind::Node,
                    term: 0,
                    target_ids: vec![1],
                    component_ids: vec![1, 2], // 2 components
                    path_id: None,
                    deactivated: false,
                    severity: "Violation".to_string(),
                },
            ],
            components: vec![],
            paths: vec![],
            targets: vec![],
            rules: vec![],
            shape_graph_triples: vec![],
            static_hints: StaticHints::default(),
            ext: Default::default(),
        };

        let store = Store::new().unwrap();
        let ctx = AnalysisContext {
            program: &program,
            store: &store,
            data_graph: None,
        };

        let analyzer = CostEstimationAnalyzer;
        let mut state = AnalysisState::default();

        analyzer.run(&ctx, &mut state).unwrap();

        assert_eq!(state.counters.get("shape_count"), Some(&3));
        assert!(state.counters.get("total_estimated_cost").is_some());
        assert!(state.counters.get("max_shape_cost").is_some());
        assert!(state.counters.get("min_shape_cost").is_some());
        assert!(state.counters.get("avg_shape_cost").is_some());

        // Max should be greater than min (shape 2 has more components)
        let max = state.counters.get("max_shape_cost").unwrap();
        let min = state.counters.get("min_shape_cost").unwrap();
        assert!(max > min);
    }
}
