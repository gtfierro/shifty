use crate::algebra::{ConstraintId, RuleId, ShapeId, TargetId};
use crate::backend_views::{
    BackendBucket, BackendViewOptions, InferenceView, SharedWorkUnit, ValidationView,
    derive_inference_view, derive_validation_view,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationPlanNode {
    TargetScan {
        shape: ShapeId,
        targets: Vec<TargetId>,
    },
    ConstraintBatch {
        shape: ShapeId,
        bucket: BackendBucket,
        constraints: Vec<ConstraintId>,
    },
    SharedWorkUnit {
        unit_id: String,
    },
    RecursiveRegion {
        region_id: u64,
        shapes: Vec<ShapeId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InferencePlanNode {
    SeedScan {
        shape: ShapeId,
        targets: Vec<TargetId>,
    },
    RuleBatch {
        shape: ShapeId,
        bucket: BackendBucket,
        rules: Vec<RuleId>,
        condition_shapes: Vec<ShapeId>,
    },
    SharedWorkUnit {
        unit_id: String,
    },
    RecursiveRegion {
        region_id: u64,
        shapes: Vec<ShapeId>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalPlanSummary {
    pub node_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationPlan {
    pub view: ValidationView,
    pub deferred_rules: Vec<(ShapeId, RuleId)>,
    pub nodes: Vec<ValidationPlanNode>,
    pub summary: LogicalPlanSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferencePlan {
    pub view: InferenceView,
    pub nodes: Vec<InferencePlanNode>,
    pub summary: LogicalPlanSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendPlans {
    pub validation: ValidationPlan,
    pub inference: InferencePlan,
}

pub fn derive_backend_logical_plans(
    program: &crate::algebra::ShapeProgram,
    options: BackendViewOptions,
) -> BackendPlans {
    BackendPlans {
        validation: derive_validation_logical_plan(program, options.clone()),
        inference: derive_inference_logical_plan(program, options),
    }
}

pub fn derive_validation_logical_plan(
    program: &crate::algebra::ShapeProgram,
    options: BackendViewOptions,
) -> ValidationPlan {
    let view = derive_validation_view(program, options);
    let retained_shapes = view
        .program
        .shapes
        .iter()
        .map(|shape| shape.id)
        .collect::<std::collections::BTreeSet<_>>();
    let deferred_rules = program
        .rules
        .iter()
        .filter(|rule| retained_shapes.contains(&rule.owner))
        .map(|rule| (rule.owner, rule.id))
        .collect::<Vec<_>>();
    derive_validation_logical_plan_from_view_with_rules(view, deferred_rules)
}

pub fn derive_inference_logical_plan(
    program: &crate::algebra::ShapeProgram,
    options: BackendViewOptions,
) -> InferencePlan {
    let view = derive_inference_view(program, options);
    derive_inference_logical_plan_from_view(view)
}

pub fn derive_validation_logical_plan_from_view(view: ValidationView) -> ValidationPlan {
    derive_validation_logical_plan_from_view_with_rules(view, Vec::new())
}

fn derive_validation_logical_plan_from_view_with_rules(
    view: ValidationView,
    deferred_rules: Vec<(ShapeId, RuleId)>,
) -> ValidationPlan {
    let mut nodes = Vec::new();
    for shape in &view.program.shapes {
        if !shape.targets.is_empty() {
            nodes.push(ValidationPlanNode::TargetScan {
                shape: shape.id,
                targets: shape.targets.clone(),
            });
        }
    }
    for shape in &view.program.shapes {
        let constraints = view
            .program
            .constraints
            .iter()
            .filter(|constraint| constraint.owner == shape.id)
            .map(|constraint| constraint.id)
            .collect::<Vec<_>>();
        if constraints.is_empty() {
            continue;
        }
        let bucket = view
            .partition
            .by_shape
            .get(&shape.id)
            .cloned()
            .unwrap_or(BackendBucket::LocalOnly);
        nodes.push(ValidationPlanNode::ConstraintBatch {
            shape: shape.id,
            bucket,
            constraints,
        });
    }
    append_shared_work_nodes(&mut nodes, &view.shared_work_units);
    for region in &view.recursive_regions {
        nodes.push(ValidationPlanNode::RecursiveRegion {
            region_id: region.id,
            shapes: region.shapes.clone(),
        });
    }
    let summary = validation_plan_summary(&nodes);
    ValidationPlan {
        view,
        deferred_rules,
        nodes,
        summary,
    }
}

pub fn derive_inference_logical_plan_from_view(view: InferenceView) -> InferencePlan {
    let mut nodes = Vec::new();
    for shape in &view.program.shapes {
        if !shape.targets.is_empty() {
            nodes.push(InferencePlanNode::SeedScan {
                shape: shape.id,
                targets: shape.targets.clone(),
            });
        }
    }
    for shape in &view.program.shapes {
        let rules = view
            .program
            .rules
            .iter()
            .filter(|rule| rule.owner == shape.id)
            .map(|rule| rule.id)
            .collect::<Vec<_>>();
        if rules.is_empty() {
            continue;
        }
        let bucket = rules
            .first()
            .and_then(|id| view.rule_buckets.get(id))
            .cloned()
            .unwrap_or(BackendBucket::LocalOnly);
        let condition_shapes = view
            .program
            .rules
            .iter()
            .filter(|rule| rule.owner == shape.id)
            .flat_map(|rule| match &rule.expr {
                crate::algebra::RuleExpr::Triple { conditions, .. }
                | crate::algebra::RuleExpr::Sparql { conditions, .. }
                | crate::algebra::RuleExpr::Generic { conditions, .. } => conditions.clone(),
            })
            .collect::<Vec<_>>();
        nodes.push(InferencePlanNode::RuleBatch {
            shape: shape.id,
            bucket,
            rules,
            condition_shapes,
        });
    }
    append_shared_work_nodes_inference(&mut nodes, &view.shared_work_units);
    for region in &view.recursive_regions {
        nodes.push(InferencePlanNode::RecursiveRegion {
            region_id: region.id,
            shapes: region.shapes.clone(),
        });
    }
    let summary = inference_plan_summary(&nodes);
    InferencePlan {
        view,
        nodes,
        summary,
    }
}

fn append_shared_work_nodes(nodes: &mut Vec<ValidationPlanNode>, units: &[SharedWorkUnit]) {
    for unit in units {
        nodes.push(ValidationPlanNode::SharedWorkUnit {
            unit_id: unit.id.clone(),
        });
    }
}

fn append_shared_work_nodes_inference(
    nodes: &mut Vec<InferencePlanNode>,
    units: &[SharedWorkUnit],
) {
    for unit in units {
        nodes.push(InferencePlanNode::SharedWorkUnit {
            unit_id: unit.id.clone(),
        });
    }
}

fn validation_plan_summary(nodes: &[ValidationPlanNode]) -> LogicalPlanSummary {
    let mut node_counts = BTreeMap::new();
    for node in nodes {
        *node_counts
            .entry(
                match node {
                    ValidationPlanNode::TargetScan { .. } => "target_scan",
                    ValidationPlanNode::ConstraintBatch { .. } => "constraint_batch",
                    ValidationPlanNode::SharedWorkUnit { .. } => "shared_work_unit",
                    ValidationPlanNode::RecursiveRegion { .. } => "recursive_region",
                }
                .to_string(),
            )
            .or_insert(0) += 1;
    }
    LogicalPlanSummary { node_counts }
}

fn inference_plan_summary(nodes: &[InferencePlanNode]) -> LogicalPlanSummary {
    let mut node_counts = BTreeMap::new();
    for node in nodes {
        *node_counts
            .entry(
                match node {
                    InferencePlanNode::SeedScan { .. } => "seed_scan",
                    InferencePlanNode::RuleBatch { .. } => "rule_batch",
                    InferencePlanNode::SharedWorkUnit { .. } => "shared_work_unit",
                    InferencePlanNode::RecursiveRegion { .. } => "recursive_region",
                }
                .to_string(),
            )
            .or_insert(0) += 1;
    }
    LogicalPlanSummary { node_counts }
}
