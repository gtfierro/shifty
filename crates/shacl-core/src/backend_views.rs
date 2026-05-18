use crate::algebra::{RuleExpr, RuleId, ShapeId, ShapeProgram};
use crate::analysis::{AnalysisSummary, analyze_program};
use crate::rewrite::{
    RecursiveRegion, RewriteOptions, RewriteSummary, RewrittenProgram, rewrite_program,
};
use crate::static_analysis::{
    ContextFootprint, SharedWorkReport, SliceRoots, StaticAnalysisSummary,
    analyze_static_with_roots, context_requirements,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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
pub enum BackendClosureMode {
    TargetRoots,
    ValidationClosure,
    InferenceClosure,
    MixedClosure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DependencyClass {
    ValidationOnly,
    InferenceOnly,
    Shared,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassifiedDependency {
    pub from: ShapeId,
    pub to: ShapeId,
    pub kind: String,
    pub class: DependencyClass,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SharedWorkUnitKind {
    CustomComponentConstraint,
    SparqlConstraint,
    RuleBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedWorkUnit {
    pub id: String,
    pub kind: SharedWorkUnitKind,
    pub fingerprint: String,
    pub owner_shapes: Vec<ShapeId>,
    pub member_ids: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationWorkInventory {
    pub local_checks: usize,
    pub traversal_checks: usize,
    pub global_checks: usize,
    pub shared_work_units: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceWorkInventory {
    pub seed_shapes: usize,
    pub rule_clusters: usize,
    pub recursive_regions: usize,
    pub local_rules: usize,
    pub traversal_rules: usize,
    pub global_rules: usize,
    pub shared_work_units: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendViewOptions {
    pub rewrite: RewriteOptions,
    pub closure_mode: BackendClosureMode,
}

impl Default for BackendViewOptions {
    fn default() -> Self {
        Self {
            rewrite: RewriteOptions {
                roots: Some(SliceRoots::TargetShapes),
                prune_unreachable: false,
                ..RewriteOptions::default()
            },
            closure_mode: BackendClosureMode::TargetRoots,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationView {
    pub program: ShapeProgram,
    pub rewrite_summary: RewriteSummary,
    pub analysis: AnalysisSummary,
    pub static_analysis: StaticAnalysisSummary,
    pub closure_mode: BackendClosureMode,
    pub entry_shapes: Vec<ShapeId>,
    pub helper_shapes: Vec<ShapeId>,
    pub partition: BackendShapePartition,
    pub recursive_regions: Vec<RecursiveRegion>,
    pub dependencies: Vec<ClassifiedDependency>,
    pub shared_work_units: Vec<SharedWorkUnit>,
    pub work_inventory: ValidationWorkInventory,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceView {
    pub program: ShapeProgram,
    pub rewrite_summary: RewriteSummary,
    pub analysis: AnalysisSummary,
    pub static_analysis: StaticAnalysisSummary,
    pub closure_mode: BackendClosureMode,
    pub rule_owner_shapes: Vec<ShapeId>,
    pub condition_shapes: Vec<ShapeId>,
    pub target_seed_shapes: Vec<ShapeId>,
    pub partition: BackendShapePartition,
    pub recursive_regions: Vec<RecursiveRegion>,
    pub rule_buckets: BTreeMap<RuleId, BackendBucket>,
    pub dependencies: Vec<ClassifiedDependency>,
    pub shared_work_units: Vec<SharedWorkUnit>,
    pub work_inventory: InferenceWorkInventory,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendViews {
    pub validation: ValidationView,
    pub inference: InferenceView,
}

pub fn derive_backend_views(program: &ShapeProgram, options: BackendViewOptions) -> BackendViews {
    BackendViews {
        validation: derive_validation_view(program, options.clone()),
        inference: derive_inference_view(program, options),
    }
}

pub fn derive_validation_view(
    program: &ShapeProgram,
    options: BackendViewOptions,
) -> ValidationView {
    let rewritten = rewrite_with_closure(program, &options, true);
    derive_validation_view_from_rewritten(&rewritten, options.closure_mode)
}

pub fn derive_inference_view(program: &ShapeProgram, options: BackendViewOptions) -> InferenceView {
    let rewritten = rewrite_with_closure(program, &options, false);
    derive_inference_view_from_rewritten(&rewritten, options.closure_mode)
}

pub fn derive_validation_view_from_rewritten(
    rewritten: &RewrittenProgram,
    closure_mode: BackendClosureMode,
) -> ValidationView {
    let mut program = rewritten.program.clone();
    program.rules.clear();
    let program = crate::passes::canonicalize_program(&program);
    let analysis = analyze_program(&program);
    let static_analysis = analyze_static_with_roots(&program, SliceRoots::TargetShapes);
    let partition = partition_shapes(&program, &rewritten.summary.recursive_regions);
    let entry_shapes = program
        .shapes
        .iter()
        .filter(|shape| !shape.targets.is_empty())
        .map(|shape| shape.id)
        .collect::<Vec<_>>();
    let helper_shapes = program
        .shapes
        .iter()
        .filter(|shape| shape.targets.is_empty())
        .map(|shape| shape.id)
        .collect::<Vec<_>>();
    let dependencies = classify_dependencies(&program);
    let shared_work_units = shared_work_units(&static_analysis.shared_work);
    let work_inventory = ValidationWorkInventory {
        local_checks: program
            .constraints
            .iter()
            .filter(|constraint| {
                matches!(
                    partition.by_shape.get(&constraint.owner),
                    Some(BackendBucket::LocalOnly)
                )
            })
            .count(),
        traversal_checks: program
            .constraints
            .iter()
            .filter(|constraint| {
                matches!(
                    partition.by_shape.get(&constraint.owner),
                    Some(BackendBucket::BoundedTraversal | BackendBucket::ShapeReference)
                )
            })
            .count(),
        global_checks: program
            .constraints
            .iter()
            .filter(|constraint| {
                matches!(
                    partition.by_shape.get(&constraint.owner),
                    Some(BackendBucket::GlobalSparql | BackendBucket::Recursive)
                )
            })
            .count(),
        shared_work_units: shared_work_units.len(),
    };
    ValidationView {
        program,
        rewrite_summary: rewritten.summary.clone(),
        analysis,
        static_analysis,
        closure_mode,
        entry_shapes,
        helper_shapes,
        partition,
        recursive_regions: rewritten.summary.recursive_regions.clone(),
        dependencies,
        shared_work_units,
        work_inventory,
    }
}

pub fn derive_inference_view_from_rewritten(
    rewritten: &RewrittenProgram,
    closure_mode: BackendClosureMode,
) -> InferenceView {
    let program = rewritten.program.clone();
    let analysis = analyze_program(&program);
    let static_analysis = analyze_static_with_roots(&program, SliceRoots::TargetShapes);
    let partition = partition_shapes(&program, &rewritten.summary.recursive_regions);
    let mut rule_owner_shapes = program
        .rules
        .iter()
        .map(|rule| rule.owner)
        .collect::<Vec<_>>();
    rule_owner_shapes.sort();
    rule_owner_shapes.dedup();
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
        .collect::<BTreeMap<_, _>>();
    let dependencies = classify_dependencies(&program);
    let shared_work_units = shared_work_units(&static_analysis.shared_work);
    let work_inventory = {
        let mut histogram = BTreeMap::<&'static str, usize>::new();
        for bucket in rule_buckets.values() {
            *histogram.entry(bucket_name(bucket)).or_insert(0) += 1;
        }
        InferenceWorkInventory {
            seed_shapes: target_seed_shapes.len(),
            rule_clusters: count_rule_clusters(&program),
            recursive_regions: rewritten.summary.recursive_regions.len(),
            local_rules: *histogram.get("local_only").unwrap_or(&0),
            traversal_rules: histogram.get("bounded_traversal").copied().unwrap_or(0)
                + histogram.get("shape_reference").copied().unwrap_or(0),
            global_rules: histogram.get("global_sparql").copied().unwrap_or(0)
                + histogram.get("recursive").copied().unwrap_or(0),
            shared_work_units: shared_work_units.len(),
        }
    };
    InferenceView {
        program,
        rewrite_summary: rewritten.summary.clone(),
        analysis,
        static_analysis,
        closure_mode,
        rule_owner_shapes,
        condition_shapes,
        target_seed_shapes,
        partition,
        recursive_regions: rewritten.summary.recursive_regions.clone(),
        rule_buckets,
        dependencies,
        shared_work_units,
        work_inventory,
    }
}

fn rewrite_with_closure(
    program: &ShapeProgram,
    options: &BackendViewOptions,
    validation_view: bool,
) -> RewrittenProgram {
    let base_roots = options
        .rewrite
        .roots
        .clone()
        .unwrap_or(SliceRoots::TargetShapes);
    let root_ids = resolve_roots(program, base_roots, validation_view);
    let closure_program = program_for_closure(program, &options.closure_mode, &root_ids);
    let mut rewrite_options = options.rewrite.clone();
    rewrite_options.roots = Some(SliceRoots::ExplicitShapes(root_ids));
    rewrite_program(&closure_program, rewrite_options)
}

fn resolve_roots(program: &ShapeProgram, roots: SliceRoots, validation_view: bool) -> Vec<ShapeId> {
    match roots {
        SliceRoots::TargetShapes => {
            let mut ids = program
                .shapes
                .iter()
                .filter(|shape| {
                    !shape.targets.is_empty()
                        || (!validation_view && shape_has_rules(program, shape.id))
                })
                .map(|shape| shape.id)
                .collect::<Vec<_>>();
            ids.sort();
            ids.dedup();
            ids
        }
        SliceRoots::ExplicitShapes(mut ids) => {
            if !validation_view {
                for rule_owner in program.rules.iter().map(|rule| rule.owner) {
                    if !ids.contains(&rule_owner)
                        && program
                            .shapes
                            .iter()
                            .any(|shape| shape.id == rule_owner && !shape.targets.is_empty())
                    {
                        continue;
                    }
                }
            }
            ids.sort();
            ids.dedup();
            ids
        }
        SliceRoots::ExplicitSelectors(selectors) => {
            let mut ids = selectors
                .iter()
                .filter_map(|selector| {
                    program
                        .normalized_shape_index
                        .get(selector)
                        .or_else(|| program.shape_index.get(selector))
                        .copied()
                })
                .collect::<Vec<_>>();
            ids.sort();
            ids.dedup();
            ids
        }
    }
}

fn program_for_closure(
    program: &ShapeProgram,
    closure_mode: &BackendClosureMode,
    roots: &[ShapeId],
) -> ShapeProgram {
    let allowed_edges = match closure_mode {
        BackendClosureMode::TargetRoots | BackendClosureMode::MixedClosure => None,
        BackendClosureMode::ValidationClosure => Some(
            ["property", "node", "qualified", "not", "logical", "target"]
                .into_iter()
                .collect::<BTreeSet<_>>(),
        ),
        BackendClosureMode::InferenceClosure => Some(
            ["target", "rule_condition"]
                .into_iter()
                .collect::<BTreeSet<_>>(),
        ),
    };
    let reachable = reachable_shapes(program, roots, allowed_edges.as_ref());
    let kept_shapes = reachable.iter().copied().collect::<BTreeSet<_>>();
    let mut filtered = program.clone();
    filtered.shapes = program
        .shapes
        .iter()
        .filter(|shape| kept_shapes.contains(&shape.id))
        .cloned()
        .map(|mut shape| {
            shape.property_shapes.retain(|id| kept_shapes.contains(id));
            shape
        })
        .collect();
    filtered.constraints = program
        .constraints
        .iter()
        .filter(|constraint| kept_shapes.contains(&constraint.owner))
        .cloned()
        .collect();
    filtered.targets = program
        .targets
        .iter()
        .filter(|target| kept_shapes.contains(&target.owner))
        .cloned()
        .collect();
    filtered.rules = program
        .rules
        .iter()
        .filter(|rule| kept_shapes.contains(&rule.owner))
        .cloned()
        .collect();
    filtered.dependencies = program
        .dependencies
        .iter()
        .filter(|edge| {
            kept_shapes.contains(&edge.from)
                && kept_shapes.contains(&edge.to)
                && allowed_edges
                    .as_ref()
                    .map(|allowed| allowed.contains(edge.kind.as_str()))
                    .unwrap_or(true)
        })
        .cloned()
        .collect();
    crate::passes::canonicalize_program(&filtered)
}

fn reachable_shapes(
    program: &ShapeProgram,
    roots: &[ShapeId],
    allowed_edges: Option<&BTreeSet<&str>>,
) -> Vec<ShapeId> {
    let mut adjacency = BTreeMap::<ShapeId, Vec<ShapeId>>::new();
    for shape in &program.shapes {
        adjacency.entry(shape.id).or_default();
    }
    for edge in &program.dependencies {
        if allowed_edges
            .map(|allowed| allowed.contains(edge.kind.as_str()))
            .unwrap_or(true)
        {
            adjacency.entry(edge.from).or_default().push(edge.to);
        }
    }
    let mut visited = BTreeSet::new();
    let mut stack = roots.to_vec();
    while let Some(shape) = stack.pop() {
        if !visited.insert(shape) {
            continue;
        }
        if let Some(next) = adjacency.get(&shape) {
            stack.extend(next.iter().copied());
        }
    }
    visited.into_iter().collect()
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

fn classify_dependencies(program: &ShapeProgram) -> Vec<ClassifiedDependency> {
    let mut dependencies = program
        .dependencies
        .iter()
        .map(|edge| ClassifiedDependency {
            from: edge.from,
            to: edge.to,
            kind: edge.kind.clone(),
            class: dependency_class(edge.kind.as_str()),
        })
        .collect::<Vec<_>>();
    dependencies.sort_by_key(|edge| (edge.from, edge.to, edge.kind.clone()));
    dependencies
}

fn dependency_class(kind: &str) -> DependencyClass {
    match kind {
        "rule_condition" => DependencyClass::InferenceOnly,
        "property" | "node" | "qualified" | "not" | "logical" => DependencyClass::ValidationOnly,
        "target" => DependencyClass::Shared,
        _ => DependencyClass::Shared,
    }
}

fn shared_work_units(report: &SharedWorkReport) -> Vec<SharedWorkUnit> {
    let mut units = Vec::new();
    for (index, group) in report
        .duplicate_custom_component_constraints
        .iter()
        .enumerate()
    {
        units.push(SharedWorkUnit {
            id: format!("custom_component:{index}"),
            kind: SharedWorkUnitKind::CustomComponentConstraint,
            fingerprint: group.fingerprint.clone(),
            owner_shapes: group.owner_shapes.clone(),
            member_ids: group.ids.iter().map(|id| id.0).collect(),
        });
    }
    for (index, group) in report.duplicate_sparql_constraints.iter().enumerate() {
        units.push(SharedWorkUnit {
            id: format!("sparql_constraint:{index}"),
            kind: SharedWorkUnitKind::SparqlConstraint,
            fingerprint: group.fingerprint.clone(),
            owner_shapes: group.owner_shapes.clone(),
            member_ids: group.ids.iter().map(|id| id.0).collect(),
        });
    }
    for (index, group) in report.duplicate_rules.iter().enumerate() {
        units.push(SharedWorkUnit {
            id: format!("rule_body:{index}"),
            kind: SharedWorkUnitKind::RuleBody,
            fingerprint: group.fingerprint.clone(),
            owner_shapes: group.owner_shapes.clone(),
            member_ids: group.ids.iter().map(|id| id.0).collect(),
        });
    }
    units
}

fn count_rule_clusters(program: &ShapeProgram) -> usize {
    let mut groups = BTreeSet::new();
    for rule in &program.rules {
        groups.insert(rule.owner);
    }
    groups.len()
}

fn shape_has_rules(program: &ShapeProgram, shape_id: ShapeId) -> bool {
    program.rules.iter().any(|rule| rule.owner == shape_id)
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
