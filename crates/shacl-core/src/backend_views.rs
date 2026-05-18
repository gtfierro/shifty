use crate::algebra::{RuleExpr, RuleId, ShapeId, ShapeProgram};
use crate::analysis::{AnalysisSummary, analyze_program};
use crate::rewrite::{
    RecursiveRegion, RewriteOptions, RewriteSummary, RewrittenProgram, rewrite_program,
};
use crate::static_analysis::{ContextFootprint, SliceRoots, context_requirements};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendBucket {
    LocalOnly,
    BoundedTraversal,
    ShapeReference,
    GlobalSparql,
    Recursive,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendShapePartition {
    pub by_shape: BTreeMap<ShapeId, BackendBucket>,
    pub histogram: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendViewOptions {
    pub rewrite: RewriteOptions,
}

impl Default for BackendViewOptions {
    fn default() -> Self {
        Self {
            rewrite: RewriteOptions {
                roots: Some(SliceRoots::TargetShapes),
                prune_unreachable: true,
                ..RewriteOptions::default()
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationView {
    pub program: ShapeProgram,
    pub rewrite_summary: RewriteSummary,
    pub analysis: AnalysisSummary,
    pub entry_shapes: Vec<ShapeId>,
    pub helper_shapes: Vec<ShapeId>,
    pub partition: BackendShapePartition,
    pub recursive_regions: Vec<RecursiveRegion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceView {
    pub program: ShapeProgram,
    pub rewrite_summary: RewriteSummary,
    pub analysis: AnalysisSummary,
    pub rule_owner_shapes: Vec<ShapeId>,
    pub condition_shapes: Vec<ShapeId>,
    pub target_seed_shapes: Vec<ShapeId>,
    pub partition: BackendShapePartition,
    pub recursive_regions: Vec<RecursiveRegion>,
    pub rule_buckets: BTreeMap<RuleId, BackendBucket>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendViews {
    pub validation: ValidationView,
    pub inference: InferenceView,
}

pub fn derive_backend_views(program: &ShapeProgram, options: BackendViewOptions) -> BackendViews {
    let rewritten = rewrite_program(program, options.rewrite);
    BackendViews {
        validation: derive_validation_view_from_rewritten(&rewritten),
        inference: derive_inference_view_from_rewritten(&rewritten),
    }
}

pub fn derive_validation_view(
    program: &ShapeProgram,
    options: BackendViewOptions,
) -> ValidationView {
    let rewritten = rewrite_program(program, options.rewrite);
    derive_validation_view_from_rewritten(&rewritten)
}

pub fn derive_inference_view(program: &ShapeProgram, options: BackendViewOptions) -> InferenceView {
    let rewritten = rewrite_program(program, options.rewrite);
    derive_inference_view_from_rewritten(&rewritten)
}

pub fn derive_validation_view_from_rewritten(rewritten: &RewrittenProgram) -> ValidationView {
    let mut program = rewritten.program.clone();
    program.rules.clear();
    let program = crate::passes::canonicalize_program(&program);
    let analysis = analyze_program(&program);
    let partition = partition_shapes(&program, &rewritten.summary.recursive_regions);
    let entry_shapes = program
        .shapes
        .iter()
        .filter(|shape| !shape.targets.is_empty())
        .map(|shape| shape.id)
        .collect();
    let helper_shapes = program
        .shapes
        .iter()
        .filter(|shape| shape.targets.is_empty())
        .map(|shape| shape.id)
        .collect();
    ValidationView {
        program,
        rewrite_summary: rewritten.summary.clone(),
        analysis,
        entry_shapes,
        helper_shapes,
        partition,
        recursive_regions: rewritten.summary.recursive_regions.clone(),
    }
}

pub fn derive_inference_view_from_rewritten(rewritten: &RewrittenProgram) -> InferenceView {
    let program = rewritten.program.clone();
    let analysis = analyze_program(&program);
    let partition = partition_shapes(&program, &rewritten.summary.recursive_regions);
    let rule_owner_shapes = program
        .rules
        .iter()
        .map(|rule| rule.owner)
        .collect::<Vec<_>>();
    let mut condition_shapes = program
        .rules
        .iter()
        .flat_map(rule_condition_shapes)
        .collect::<Vec<_>>();
    condition_shapes.sort();
    condition_shapes.dedup();
    let target_seed_shapes = program
        .shapes
        .iter()
        .filter(|shape| !shape.targets.is_empty())
        .map(|shape| shape.id)
        .collect::<Vec<_>>();
    let rule_buckets = program
        .rules
        .iter()
        .map(|rule| {
            (
                rule.id,
                classify_rule_bucket(rule, &rewritten.summary.recursive_regions),
            )
        })
        .collect();
    InferenceView {
        program,
        rewrite_summary: rewritten.summary.clone(),
        analysis,
        rule_owner_shapes,
        condition_shapes,
        target_seed_shapes,
        partition,
        recursive_regions: rewritten.summary.recursive_regions.clone(),
        rule_buckets,
    }
}

fn partition_shapes(
    program: &ShapeProgram,
    recursive_regions: &[RecursiveRegion],
) -> BackendShapePartition {
    let context = context_requirements(program);
    let recursive_shapes = recursive_regions
        .iter()
        .flat_map(|region| region.shapes.iter().copied())
        .collect::<Vec<_>>();
    let mut by_shape = BTreeMap::new();
    let mut histogram = BTreeMap::new();
    for shape in &program.shapes {
        let bucket = if recursive_shapes.contains(&shape.id) {
            BackendBucket::Recursive
        } else {
            match context.shape_footprints.get(&shape.id) {
                Some(ContextFootprint::GlobalSparql) => BackendBucket::GlobalSparql,
                Some(ContextFootprint::ShapeReferenceTraversal) => BackendBucket::ShapeReference,
                Some(ContextFootprint::BoundedTraversal) => BackendBucket::BoundedTraversal,
                Some(ContextFootprint::RecursiveNeighborhood) => BackendBucket::Recursive,
                Some(ContextFootprint::SingleHopPath)
                | Some(ContextFootprint::NodeLocal)
                | Some(ContextFootprint::TargetOnly)
                | None => BackendBucket::LocalOnly,
            }
        };
        *histogram
            .entry(bucket_name(&bucket).to_string())
            .or_insert(0) += 1;
        by_shape.insert(shape.id, bucket);
    }
    BackendShapePartition {
        by_shape,
        histogram,
    }
}

fn classify_rule_bucket(
    rule: &crate::algebra::Rule,
    recursive_regions: &[RecursiveRegion],
) -> BackendBucket {
    if recursive_regions
        .iter()
        .any(|region| region.shapes.contains(&rule.owner))
    {
        return BackendBucket::Recursive;
    }
    match &rule.expr {
        RuleExpr::Sparql { .. } => BackendBucket::GlobalSparql,
        RuleExpr::Generic { conditions, .. } | RuleExpr::Triple { conditions, .. } => {
            if conditions.is_empty() {
                BackendBucket::LocalOnly
            } else {
                BackendBucket::ShapeReference
            }
        }
    }
}

fn rule_condition_shapes(rule: &crate::algebra::Rule) -> Vec<ShapeId> {
    match &rule.expr {
        RuleExpr::Triple { conditions, .. }
        | RuleExpr::Sparql { conditions, .. }
        | RuleExpr::Generic { conditions, .. } => conditions.clone(),
    }
}

fn bucket_name(bucket: &BackendBucket) -> &'static str {
    match bucket {
        BackendBucket::LocalOnly => "local_only",
        BackendBucket::BoundedTraversal => "bounded_traversal",
        BackendBucket::ShapeReference => "shape_reference",
        BackendBucket::GlobalSparql => "global_sparql",
        BackendBucket::Recursive => "recursive",
    }
}
