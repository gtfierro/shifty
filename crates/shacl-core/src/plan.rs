use crate::algebra::{ConstraintId, Rule, RuleId, ShapeId, TargetId};
use crate::backend_views::{
    BackendBucket, BackendViewOptions, InferenceView, SharedWorkUnit, ValidationView,
    derive_inference_view, derive_validation_view,
};
use crate::data_graph::{DataGraphSummary, FanoutClass, summarize_data_graph};
use crate::source::ResolvedShapeSet;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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
pub struct TargetScanAnnotation {
    pub shape: ShapeId,
    pub estimated_focus_nodes: Option<usize>,
    pub empty_scan: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintBatchAnnotation {
    pub shape: ShapeId,
    pub estimated_focus_nodes: Option<usize>,
    pub fanout_hints: BTreeMap<String, FanoutClass>,
    pub dead_constraint_candidates: Vec<ConstraintId>,
    pub vacuous_constraint_candidates: Vec<ConstraintId>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidationPlanAnnotations {
    pub target_scans: BTreeMap<u64, TargetScanAnnotation>,
    pub constraint_batches: BTreeMap<u64, ConstraintBatchAnnotation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationPlan {
    pub view: ValidationView,
    pub executable_rules: Vec<Rule>,
    pub nodes: Vec<ValidationPlanNode>,
    pub summary: LogicalPlanSummary,
    pub data_summary: Option<DataGraphSummary>,
    pub annotations: ValidationPlanAnnotations,
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
    let executable_rules = program
        .rules
        .iter()
        .filter(|rule| retained_shapes.contains(&rule.owner) && !rule.deactivated)
        .cloned()
        .collect::<Vec<_>>();
    derive_validation_logical_plan_from_view_with_rules(view, executable_rules, None)
}

pub fn derive_validation_logical_plan_with_data(
    program: &crate::algebra::ShapeProgram,
    options: BackendViewOptions,
    data: &ResolvedShapeSet,
) -> ValidationPlan {
    let view = derive_validation_view(program, options);
    let retained_shapes = view
        .program
        .shapes
        .iter()
        .map(|shape| shape.id)
        .collect::<BTreeSet<_>>();
    let executable_rules = program
        .rules
        .iter()
        .filter(|rule| retained_shapes.contains(&rule.owner) && !rule.deactivated)
        .cloned()
        .collect::<Vec<_>>();
    let summary = summarize_data_graph(&view.program, data);
    derive_validation_logical_plan_from_view_with_rules(view, executable_rules, Some(summary))
}

pub fn derive_inference_logical_plan(
    program: &crate::algebra::ShapeProgram,
    options: BackendViewOptions,
) -> InferencePlan {
    let view = derive_inference_view(program, options);
    derive_inference_logical_plan_from_view(view)
}

pub fn derive_validation_logical_plan_from_view(view: ValidationView) -> ValidationPlan {
    derive_validation_logical_plan_from_view_with_rules(view, Vec::new(), None)
}

fn derive_validation_logical_plan_from_view_with_rules(
    view: ValidationView,
    executable_rules: Vec<Rule>,
    data_summary: Option<DataGraphSummary>,
) -> ValidationPlan {
    let mut nodes = Vec::new();
    let mut target_shapes = view
        .program
        .shapes
        .iter()
        .filter(|shape| !shape.targets.is_empty())
        .collect::<Vec<_>>();
    if let Some(summary) = data_summary.as_ref() {
        target_shapes.sort_by_key(|shape| {
            summary
                .shape_summaries
                .iter()
                .find(|candidate| candidate.shape == shape.id)
                .and_then(|candidate| candidate.estimated_focus_nodes)
                .unwrap_or(usize::MAX)
        });
    }
    for shape in target_shapes {
        nodes.push(ValidationPlanNode::TargetScan {
            shape: shape.id,
            targets: shape.targets.clone(),
        });
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
    let annotations = data_summary
        .as_ref()
        .map(build_validation_plan_annotations)
        .unwrap_or_default();
    ValidationPlan {
        view,
        executable_rules,
        nodes,
        summary,
        data_summary,
        annotations,
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

fn build_validation_plan_annotations(summary: &DataGraphSummary) -> ValidationPlanAnnotations {
    let mut annotations = ValidationPlanAnnotations::default();
    for shape in &summary.shape_summaries {
        annotations.target_scans.insert(
            shape.shape.0,
            TargetScanAnnotation {
                shape: shape.shape,
                estimated_focus_nodes: shape.estimated_focus_nodes,
                empty_scan: shape.empty_target_scan,
            },
        );
        annotations.constraint_batches.insert(
            shape.shape.0,
            ConstraintBatchAnnotation {
                shape: shape.shape,
                estimated_focus_nodes: shape.estimated_focus_nodes,
                fanout_hints: shape.fanout_hints.clone(),
                dead_constraint_candidates: shape
                    .dead_constraint_candidates
                    .iter()
                    .copied()
                    .map(ConstraintId)
                    .collect(),
                vacuous_constraint_candidates: shape
                    .vacuous_constraint_candidates
                    .iter()
                    .copied()
                    .map(ConstraintId)
                    .collect(),
            },
        );
    }
    annotations
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
