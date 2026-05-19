use crate::algebra::{ConstraintId, Rule, RuleId, ShapeId, TargetId};
use crate::backend_views::{
    BackendBucket, BackendViewOptions, InferenceView, SharedWorkUnit, ValidationView,
    derive_inference_view, derive_validation_view,
};
use crate::data_graph::{
    DEFAULT_SUBJECT_SAMPLE_BUDGET, DataGraphSummary, DataSummaryMetadata, DataSummaryOptions,
    FanoutClass, PlanningEstimationMode, PlanningIndex, build_data_index,
    summarize_data_graph_from_index,
};
use crate::source::ResolvedShapeSet;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

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
    pub planning: ValidationPlanningMetadata,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationPlanningOptions {
    pub estimation_mode: PlanningEstimationMode,
    pub subject_sample_budget: usize,
}

impl Default for ValidationPlanningOptions {
    fn default() -> Self {
        Self {
            estimation_mode: PlanningEstimationMode::Exact,
            subject_sample_budget: DEFAULT_SUBJECT_SAMPLE_BUDGET,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationPlanningMetadata {
    pub estimation_mode: PlanningEstimationMode,
    pub sample_subject_budget: Option<usize>,
    pub sampled_subjects: Option<usize>,
    pub sampled_quads: Option<usize>,
    pub exact_target_estimates: usize,
    pub sampled_target_estimates: usize,
    pub indexed_shapes: usize,
    pub indexed_constraints: usize,
    pub indexed_targets: usize,
    pub indexed_direct_path_shapes: usize,
}

impl Default for ValidationPlanningMetadata {
    fn default() -> Self {
        Self {
            estimation_mode: PlanningEstimationMode::Exact,
            sample_subject_budget: None,
            sampled_subjects: None,
            sampled_quads: None,
            exact_target_estimates: 0,
            sampled_target_estimates: 0,
            indexed_shapes: 0,
            indexed_constraints: 0,
            indexed_targets: 0,
            indexed_direct_path_shapes: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanningStageTiming {
    pub name: String,
    pub elapsed_ms: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidationPlanningBenchmark {
    pub stages: Vec<PlanningStageTiming>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationPlanningResult {
    pub plan: ValidationPlan,
    pub benchmark: ValidationPlanningBenchmark,
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
    derive_validation_logical_plan_detailed(program, options).plan
}

pub fn derive_validation_logical_plan_with_data(
    program: &crate::algebra::ShapeProgram,
    options: BackendViewOptions,
    data: &ResolvedShapeSet,
) -> ValidationPlan {
    derive_validation_logical_plan_with_options(
        program,
        options,
        data,
        ValidationPlanningOptions::default(),
    )
}

pub fn derive_validation_logical_plan_with_options(
    program: &crate::algebra::ShapeProgram,
    options: BackendViewOptions,
    data: &ResolvedShapeSet,
    planning_options: ValidationPlanningOptions,
) -> ValidationPlan {
    derive_validation_logical_plan_with_options_detailed(program, options, data, planning_options)
        .plan
}

pub fn derive_validation_logical_plan_detailed(
    program: &crate::algebra::ShapeProgram,
    options: BackendViewOptions,
) -> ValidationPlanningResult {
    let started = Instant::now();
    let view = derive_validation_view(program, options);
    let view_elapsed = elapsed_ms(started.elapsed());

    let stage_started = Instant::now();
    let executable_rules = executable_validation_rules(program, &view);
    let index = PlanningIndex::build(&view.program);
    let index_elapsed = elapsed_ms(stage_started.elapsed());

    let stage_started = Instant::now();
    let plan = derive_validation_logical_plan_from_view_with_rules(
        view,
        executable_rules,
        &index,
        None,
        ValidationPlanningMetadata {
            indexed_shapes: index.shape_positions.len(),
            indexed_constraints: index.constraint_positions.len(),
            indexed_targets: index.targets_by_owner.values().map(Vec::len).sum(),
            indexed_direct_path_shapes: index.direct_path_predicate_by_shape.len(),
            ..ValidationPlanningMetadata::default()
        },
    );
    let plan_elapsed = elapsed_ms(stage_started.elapsed());

    ValidationPlanningResult {
        plan,
        benchmark: ValidationPlanningBenchmark {
            stages: vec![
                PlanningStageTiming {
                    name: "plan.view".to_string(),
                    elapsed_ms: view_elapsed,
                },
                PlanningStageTiming {
                    name: "plan.index".to_string(),
                    elapsed_ms: index_elapsed,
                },
                PlanningStageTiming {
                    name: "plan.constraint_batches".to_string(),
                    elapsed_ms: plan_elapsed,
                },
            ],
        },
    }
}

pub fn derive_validation_logical_plan_with_options_detailed(
    program: &crate::algebra::ShapeProgram,
    options: BackendViewOptions,
    data: &ResolvedShapeSet,
    planning_options: ValidationPlanningOptions,
) -> ValidationPlanningResult {
    let mut stages = Vec::new();

    let stage_started = Instant::now();
    let view = derive_validation_view(program, options);
    stages.push(PlanningStageTiming {
        name: "plan.view".to_string(),
        elapsed_ms: elapsed_ms(stage_started.elapsed()),
    });

    let stage_started = Instant::now();
    let executable_rules = executable_validation_rules(program, &view);
    let index = PlanningIndex::build(&view.program);
    let planning_metadata = ValidationPlanningMetadata {
        estimation_mode: planning_options.estimation_mode,
        indexed_shapes: index.shape_positions.len(),
        indexed_constraints: index.constraint_positions.len(),
        indexed_targets: index.targets_by_owner.values().map(Vec::len).sum(),
        indexed_direct_path_shapes: index.direct_path_predicate_by_shape.len(),
        ..ValidationPlanningMetadata::default()
    };
    stages.push(PlanningStageTiming {
        name: "plan.index".to_string(),
        elapsed_ms: elapsed_ms(stage_started.elapsed()),
    });

    let summary_options = DataSummaryOptions {
        estimation_mode: planning_options.estimation_mode,
        full_statistics: false,
        subject_sample_budget: planning_options.subject_sample_budget,
    };
    let stage_started = Instant::now();
    let data_index = build_data_index(data, &summary_options);
    stages.push(PlanningStageTiming {
        name: "plan.data_index".to_string(),
        elapsed_ms: elapsed_ms(stage_started.elapsed()),
    });

    let stage_started = Instant::now();
    let data_summary =
        summarize_data_graph_from_index(&view.program, &index, data, &summary_options, &data_index);
    stages.push(PlanningStageTiming {
        name: "plan.data_summary".to_string(),
        elapsed_ms: elapsed_ms(stage_started.elapsed()),
    });

    let stage_started = Instant::now();
    let planning_metadata = merge_planning_metadata(planning_metadata, &data_summary.metadata);
    let data_summary = Some(data_summary);
    let target_nodes = build_target_scan_nodes(&view.program, data_summary.as_ref());
    stages.push(PlanningStageTiming {
        name: "plan.target_order".to_string(),
        elapsed_ms: elapsed_ms(stage_started.elapsed()),
    });

    let stage_started = Instant::now();
    let constraint_nodes = build_constraint_batch_nodes(&view, &index);
    stages.push(PlanningStageTiming {
        name: "plan.constraint_batches".to_string(),
        elapsed_ms: elapsed_ms(stage_started.elapsed()),
    });

    let stage_started = Instant::now();
    let plan = assemble_validation_plan(
        view,
        executable_rules,
        target_nodes,
        constraint_nodes,
        data_summary,
        planning_metadata,
    );
    stages.push(PlanningStageTiming {
        name: "plan.annotations".to_string(),
        elapsed_ms: elapsed_ms(stage_started.elapsed()),
    });

    ValidationPlanningResult {
        plan,
        benchmark: ValidationPlanningBenchmark { stages },
    }
}

pub fn derive_inference_logical_plan(
    program: &crate::algebra::ShapeProgram,
    options: BackendViewOptions,
) -> InferencePlan {
    let view = derive_inference_view(program, options);
    derive_inference_logical_plan_from_view(view)
}

pub fn derive_validation_logical_plan_from_view(view: ValidationView) -> ValidationPlan {
    let executable_rules = Vec::new();
    let index = PlanningIndex::build(&view.program);
    derive_validation_logical_plan_from_view_with_rules(
        view,
        executable_rules,
        &index,
        None,
        ValidationPlanningMetadata {
            indexed_shapes: index.shape_positions.len(),
            indexed_constraints: index.constraint_positions.len(),
            indexed_targets: index.targets_by_owner.values().map(Vec::len).sum(),
            indexed_direct_path_shapes: index.direct_path_predicate_by_shape.len(),
            ..ValidationPlanningMetadata::default()
        },
    )
}

fn derive_validation_logical_plan_from_view_with_rules(
    view: ValidationView,
    executable_rules: Vec<Rule>,
    planning_index: &PlanningIndex,
    data_summary: Option<DataGraphSummary>,
    planning: ValidationPlanningMetadata,
) -> ValidationPlan {
    let target_nodes = build_target_scan_nodes(&view.program, data_summary.as_ref());
    let constraint_nodes = build_constraint_batch_nodes(&view, planning_index);
    assemble_validation_plan(
        view,
        executable_rules,
        target_nodes,
        constraint_nodes,
        data_summary,
        planning,
    )
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

fn build_target_scan_nodes(
    program: &crate::algebra::ShapeProgram,
    data_summary: Option<&DataGraphSummary>,
) -> Vec<ValidationPlanNode> {
    let mut target_shapes = program
        .shapes
        .iter()
        .filter(|shape| !shape.targets.is_empty())
        .collect::<Vec<_>>();
    if let Some(summary) = data_summary {
        let shape_estimates = summary
            .shape_summaries
            .iter()
            .map(|candidate| (candidate.shape, candidate.estimated_focus_nodes))
            .collect::<BTreeMap<_, _>>();
        target_shapes.sort_by_key(|shape| {
            shape_estimates
                .get(&shape.id)
                .and_then(|candidate| *candidate)
                .unwrap_or(usize::MAX)
        });
    }
    target_shapes
        .into_iter()
        .map(|shape| ValidationPlanNode::TargetScan {
            shape: shape.id,
            targets: shape.targets.clone(),
        })
        .collect()
}

fn build_constraint_batch_nodes(
    view: &ValidationView,
    planning_index: &PlanningIndex,
) -> Vec<ValidationPlanNode> {
    let mut nodes = Vec::new();
    for shape in &view.program.shapes {
        let constraints = planning_index
            .constraints_by_owner
            .get(&shape.id)
            .cloned()
            .unwrap_or_default();
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
    nodes
}

fn assemble_validation_plan(
    view: ValidationView,
    executable_rules: Vec<Rule>,
    mut target_nodes: Vec<ValidationPlanNode>,
    constraint_nodes: Vec<ValidationPlanNode>,
    data_summary: Option<DataGraphSummary>,
    planning: ValidationPlanningMetadata,
) -> ValidationPlan {
    let mut nodes = Vec::new();
    nodes.append(&mut target_nodes);
    nodes.extend(constraint_nodes);
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
        planning,
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

fn executable_validation_rules(
    program: &crate::algebra::ShapeProgram,
    view: &ValidationView,
) -> Vec<Rule> {
    let retained_shapes = view
        .program
        .shapes
        .iter()
        .map(|shape| shape.id)
        .collect::<BTreeSet<_>>();
    program
        .rules
        .iter()
        .filter(|rule| retained_shapes.contains(&rule.owner) && !rule.deactivated)
        .cloned()
        .collect()
}

fn merge_planning_metadata(
    mut planning: ValidationPlanningMetadata,
    summary: &DataSummaryMetadata,
) -> ValidationPlanningMetadata {
    planning.estimation_mode = summary.estimation_mode;
    planning.sampled_subjects = summary.sampled_subjects;
    planning.sampled_quads = summary.sampled_quads;
    planning.sample_subject_budget = summary.sample_subject_budget;
    planning.exact_target_estimates = summary.exact_target_estimates;
    planning.sampled_target_estimates = summary.sampled_target_estimates;
    planning
}

fn elapsed_ms(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
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
