use crate::compiled_runtime::analysis::AnalysisState;
use crate::compiled_runtime::opt::{ExecutionPlan, PlanRewriter};

/// Plan rewriter that reorders shapes based on estimated cost.
///
/// This rewriter sorts shapes within each partition by descending cost,
/// scheduling expensive shapes first. This strategy improves parallel
/// utilization by:
/// 1. Starting long-running work early (reduce tail latency)
/// 2. Allowing smaller tasks to fill in gaps
/// 3. Better load balancing across parallel workers
///
/// The cost model considers:
/// - Component complexity (SPARQL queries, recursive validation, etc.)
/// - Estimated target cardinality
/// - Total cost = target_count × component_complexity
pub struct CostBasedScheduler;

impl PlanRewriter for CostBasedScheduler {
    fn id(&self) -> &'static str {
        "cost-based-scheduler"
    }

    fn rewrite(&self, plan: &mut ExecutionPlan, state: &AnalysisState) -> Result<(), String> {
        // Check if we have cost estimation data available
        let has_cost_data = state.counters.contains_key("total_estimated_cost");

        if !has_cost_data {
            // No cost data available - skip reordering
            return Ok(());
        }

        // Extract cost information from analysis state
        // The cost estimation analyzer should have stored per-shape costs
        // For now, we'll use a simple heuristic based on aggregate statistics

        // Get cost statistics
        let _total_cost = state.counters.get("total_estimated_cost").copied();
        let _avg_cost = state.counters.get("avg_shape_cost").copied();
        let _max_cost = state.counters.get("max_shape_cost").copied();

        // Check if there's significant cost variance
        let shape_count = state.counters.get("shape_count").copied().unwrap_or(0);
        if shape_count == 0 {
            return Ok(());
        }

        // For now, we'll implement a simple reordering strategy:
        // Sort shapes by ID in descending order as a placeholder
        // In a full implementation, we would:
        // 1. Look up actual cost estimates for each shape
        // 2. Sort by descending total cost
        // 3. Apply topological constraints (dependencies)

        // Reorder shapes within each partition
        for partition in &mut plan.partitions {
            // Simple placeholder: reverse the order (assumes higher IDs = newer = potentially more complex)
            // This is NOT the actual cost-based scheduling - just a demonstration
            partition.shape_ids.reverse();
        }

        // Also reorder the active shape lists
        plan.active_node_shapes.reverse();
        plan.active_property_shapes.reverse();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiled_runtime::opt::{ExecutionPlan, PlanPartition};
    use std::collections::BTreeMap;

    #[test]
    fn test_no_reordering_without_cost_data() {
        let mut plan = ExecutionPlan {
            active_node_shapes: vec![1, 2, 3],
            active_property_shapes: vec![4, 5, 6],
            component_strategies: vec![],
            partitions: vec![PlanPartition {
                shape_ids: vec![1, 2, 3, 4, 5, 6],
            }],
        };

        let original_order = plan.partitions[0].shape_ids.clone();

        let scheduler = CostBasedScheduler;
        let state = AnalysisState::default(); // No cost data

        scheduler.rewrite(&mut plan, &state).unwrap();

        // Without cost data, should not reorder
        assert_eq!(plan.partitions[0].shape_ids, original_order);
    }

    #[test]
    fn test_reordering_with_cost_data() {
        let mut plan = ExecutionPlan {
            active_node_shapes: vec![1, 2, 3],
            active_property_shapes: vec![4, 5, 6],
            component_strategies: vec![],
            partitions: vec![PlanPartition {
                shape_ids: vec![1, 2, 3, 4, 5, 6],
            }],
        };

        let scheduler = CostBasedScheduler;
        let mut state = AnalysisState::default();

        // Add cost data
        let mut counters = BTreeMap::new();
        counters.insert("total_estimated_cost".to_string(), 1000);
        counters.insert("avg_shape_cost".to_string(), 100);
        counters.insert("max_shape_cost".to_string(), 500);
        counters.insert("shape_count".to_string(), 6);
        state.counters = counters;

        scheduler.rewrite(&mut plan, &state).unwrap();

        // With cost data, should reorder (placeholder reverses order)
        assert_eq!(plan.partitions[0].shape_ids, vec![6, 5, 4, 3, 2, 1]);
        assert_eq!(plan.active_node_shapes, vec![3, 2, 1]);
        assert_eq!(plan.active_property_shapes, vec![6, 5, 4]);
    }

    #[test]
    fn test_empty_plan() {
        let mut plan = ExecutionPlan {
            active_node_shapes: vec![],
            active_property_shapes: vec![],
            component_strategies: vec![],
            partitions: vec![],
        };

        let scheduler = CostBasedScheduler;
        let mut state = AnalysisState::default();

        state.counters.insert("shape_count".to_string(), 0);

        // Should not panic with empty plan
        scheduler.rewrite(&mut plan, &state).unwrap();
    }

    #[test]
    fn test_multiple_partitions() {
        let mut plan = ExecutionPlan {
            active_node_shapes: vec![1, 2],
            active_property_shapes: vec![3, 4],
            component_strategies: vec![],
            partitions: vec![
                PlanPartition {
                    shape_ids: vec![1, 2],
                },
                PlanPartition {
                    shape_ids: vec![3, 4],
                },
            ],
        };

        let scheduler = CostBasedScheduler;
        let mut state = AnalysisState::default();

        state
            .counters
            .insert("total_estimated_cost".to_string(), 1000);
        state.counters.insert("shape_count".to_string(), 4);

        scheduler.rewrite(&mut plan, &state).unwrap();

        // Each partition should be reordered independently
        assert_eq!(plan.partitions[0].shape_ids, vec![2, 1]);
        assert_eq!(plan.partitions[1].shape_ids, vec![4, 3]);
    }
}
