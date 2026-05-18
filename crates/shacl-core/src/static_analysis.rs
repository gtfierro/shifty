use crate::algebra::{
    ComponentDefId, ConstraintExpr, ConstraintId, LogicalKind, PropertyPath, RuleExpr, RuleId,
    ShapeId, ShapeProgram, TargetExpr, TargetId, TriplePatternTerm,
};
use crate::analysis::{AnalysisSummary, analyze_program};
use crate::diagnostics::InspectionGraph;
use crate::passes::canonicalize_program;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SliceRoots {
    TargetShapes,
    ExplicitShapes(Vec<ShapeId>),
    ExplicitSelectors(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ContextFootprint {
    TargetOnly,
    NodeLocal,
    SingleHopPath,
    BoundedTraversal,
    ShapeReferenceTraversal,
    RecursiveNeighborhood,
    GlobalSparql,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextFootprintReport {
    pub shape_footprints: BTreeMap<ShapeId, ContextFootprint>,
    pub rule_footprints: BTreeMap<RuleId, ContextFootprint>,
    pub shape_reasons: BTreeMap<ShapeId, Vec<String>>,
    pub rule_reasons: BTreeMap<RuleId, Vec<String>>,
    pub histogram: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateGroup<T> {
    pub fingerprint: String,
    pub ids: Vec<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FingerprintReport {
    pub constraint_fingerprints: BTreeMap<ConstraintId, String>,
    pub target_fingerprints: BTreeMap<TargetId, String>,
    pub rule_fingerprints: BTreeMap<RuleId, String>,
    pub duplicate_constraints: Vec<DuplicateGroup<ConstraintId>>,
    pub duplicate_targets: Vec<DuplicateGroup<TargetId>>,
    pub duplicate_rules: Vec<DuplicateGroup<RuleId>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SliceReason {
    Root { roots: Vec<ShapeId> },
    ReachableFromRoots { roots: Vec<ShapeId> },
    OwnedByRetainedShape { owner: ShapeId, roots: Vec<ShapeId> },
    ReferencedByRetainedConstraints { constraints: Vec<ConstraintId> },
    UnreachableFromRoots,
    OwnerShapeDropped { owner: ShapeId },
    UnreferencedAfterSlicing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootSliceSummary {
    pub root: ShapeId,
    pub retained_shapes: usize,
    pub retained_rules: usize,
    pub retained_constraints: usize,
    pub retained_targets: usize,
    pub retained_components: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramSlice {
    pub roots: Vec<ShapeId>,
    pub requested_root_selectors: Vec<String>,
    pub unresolved_root_selectors: Vec<String>,
    pub retained_shape_ids: Vec<ShapeId>,
    pub retained_rule_ids: Vec<RuleId>,
    pub retained_constraint_ids: Vec<ConstraintId>,
    pub retained_target_ids: Vec<TargetId>,
    pub retained_component_ids: Vec<ComponentDefId>,
    pub dropped_shape_ids: Vec<ShapeId>,
    pub dropped_rule_ids: Vec<RuleId>,
    pub dropped_constraint_ids: Vec<ConstraintId>,
    pub dropped_target_ids: Vec<TargetId>,
    pub dropped_component_ids: Vec<ComponentDefId>,
    pub retained_shape_reasons: BTreeMap<ShapeId, SliceReason>,
    pub retained_rule_reasons: BTreeMap<RuleId, SliceReason>,
    pub retained_constraint_reasons: BTreeMap<ConstraintId, SliceReason>,
    pub retained_target_reasons: BTreeMap<TargetId, SliceReason>,
    pub retained_component_reasons: BTreeMap<ComponentDefId, SliceReason>,
    pub dropped_shape_reasons: BTreeMap<ShapeId, SliceReason>,
    pub dropped_rule_reasons: BTreeMap<RuleId, SliceReason>,
    pub dropped_constraint_reasons: BTreeMap<ConstraintId, SliceReason>,
    pub dropped_target_reasons: BTreeMap<TargetId, SliceReason>,
    pub dropped_component_reasons: BTreeMap<ComponentDefId, SliceReason>,
    pub root_summaries: Vec<RootSliceSummary>,
    pub reduced_program: ShapeProgram,
    pub inspection: InspectionGraph,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedWorkCandidate<T> {
    pub fingerprint: String,
    pub ids: Vec<T>,
    pub owner_shapes: Vec<ShapeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedWorkReport {
    pub duplicate_constraints: Vec<SharedWorkCandidate<ConstraintId>>,
    pub duplicate_targets: Vec<SharedWorkCandidate<TargetId>>,
    pub duplicate_rules: Vec<SharedWorkCandidate<RuleId>>,
    pub duplicate_custom_component_constraints: Vec<SharedWorkCandidate<ConstraintId>>,
    pub duplicate_sparql_constraints: Vec<SharedWorkCandidate<ConstraintId>>,
    pub duplicate_sparql_rules: Vec<SharedWorkCandidate<RuleId>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StaticCostHint {
    EntryShape,
    HelperShape,
    HasSparql,
    Recursive,
    ClosedShape,
    QualifiedValueShape,
    HasRules,
    DuplicateConstraintWork,
    DuplicateTargetWork,
    DuplicateRuleWork,
    SingleHopPath,
    BoundedTraversal,
    ShapeReferenceTraversal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticCostHintReport {
    pub shape_hints: BTreeMap<ShapeId, Vec<StaticCostHint>>,
    pub rule_hints: BTreeMap<RuleId, Vec<StaticCostHint>>,
    pub histogram: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticAnalysisSummary {
    pub baseline: AnalysisSummary,
    pub slice: ProgramSlice,
    pub context: ContextFootprintReport,
    pub fingerprints: FingerprintReport,
    pub shared_work: SharedWorkReport,
    pub cost_hints: StaticCostHintReport,
}

pub fn analyze_static(program: &ShapeProgram) -> StaticAnalysisSummary {
    analyze_static_with_roots(program, SliceRoots::TargetShapes)
}

pub fn analyze_static_with_roots(
    program: &ShapeProgram,
    roots: SliceRoots,
) -> StaticAnalysisSummary {
    let baseline = analyze_program(program);
    let slice = slice_program(program, roots);
    let context = context_requirements(program);
    let fingerprints = fingerprint_program(program);
    let shared_work = shared_work_candidates(program, &fingerprints);
    let cost_hints = static_cost_hints(program, &baseline, &context, &shared_work);
    StaticAnalysisSummary {
        baseline,
        slice,
        context,
        fingerprints,
        shared_work,
        cost_hints,
    }
}

pub fn slice_program(program: &ShapeProgram, roots: SliceRoots) -> ProgramSlice {
    let resolved_roots = resolve_roots(program, roots);
    let root_ids = resolved_roots.ids;
    let adjacency = adjacency_map(program);
    let reachable_by_root = root_ids
        .iter()
        .map(|root| (*root, reachable_shapes(&[*root], &adjacency)))
        .collect::<BTreeMap<_, _>>();
    let mut shape_roots: BTreeMap<ShapeId, BTreeSet<ShapeId>> = BTreeMap::new();
    for (root, shapes) in &reachable_by_root {
        for shape in shapes {
            shape_roots.entry(*shape).or_default().insert(*root);
        }
    }
    let retained_shapes: BTreeSet<_> = shape_roots.keys().copied().collect();
    let retained_constraints: BTreeSet<_> = program
        .constraints
        .iter()
        .filter(|constraint| retained_shapes.contains(&constraint.owner))
        .map(|constraint| constraint.id)
        .collect();
    let retained_targets: BTreeSet<_> = program
        .targets
        .iter()
        .filter(|target| retained_shapes.contains(&target.owner))
        .map(|target| target.id)
        .collect();
    let retained_rules: BTreeSet<_> = program
        .rules
        .iter()
        .filter(|rule| retained_shapes.contains(&rule.owner))
        .map(|rule| rule.id)
        .collect();
    let retained_components: BTreeSet<_> = program
        .constraints
        .iter()
        .filter(|constraint| retained_constraints.contains(&constraint.id))
        .filter_map(|constraint| match &constraint.expr {
            ConstraintExpr::CustomComponent {
                component: Some(component),
                ..
            } => Some(*component),
            _ => None,
        })
        .collect();

    let mut reduced = program.clone();
    reduced.shapes = program
        .shapes
        .iter()
        .filter(|shape| retained_shapes.contains(&shape.id))
        .cloned()
        .map(|mut shape| {
            shape
                .property_shapes
                .retain(|id| retained_shapes.contains(id));
            shape
                .constraints
                .retain(|id| retained_constraints.contains(id));
            shape.targets.retain(|id| retained_targets.contains(id));
            shape
        })
        .collect();
    reduced.constraints = program
        .constraints
        .iter()
        .filter(|constraint| retained_constraints.contains(&constraint.id))
        .cloned()
        .collect();
    reduced.targets = program
        .targets
        .iter()
        .filter(|target| retained_targets.contains(&target.id))
        .cloned()
        .collect();
    reduced.rules = program
        .rules
        .iter()
        .filter(|rule| retained_rules.contains(&rule.id))
        .cloned()
        .collect();
    reduced.constraint_components = program
        .constraint_components
        .iter()
        .filter(|component| retained_components.contains(&component.id))
        .cloned()
        .collect();
    reduced.dependencies = program
        .dependencies
        .iter()
        .filter(|edge| retained_shapes.contains(&edge.from) && retained_shapes.contains(&edge.to))
        .cloned()
        .collect();
    let reduced = canonicalize_program(&reduced);

    let root_summaries = root_ids
        .iter()
        .map(|root| {
            let shapes: BTreeSet<_> = reachable_by_root
                .get(root)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect();
            let constraints = program
                .constraints
                .iter()
                .filter(|constraint| shapes.contains(&constraint.owner))
                .count();
            let targets = program
                .targets
                .iter()
                .filter(|target| shapes.contains(&target.owner))
                .count();
            let rules = program
                .rules
                .iter()
                .filter(|rule| shapes.contains(&rule.owner))
                .count();
            let components: BTreeSet<_> = program
                .constraints
                .iter()
                .filter(|constraint| shapes.contains(&constraint.owner))
                .filter_map(|constraint| match &constraint.expr {
                    ConstraintExpr::CustomComponent {
                        component: Some(component),
                        ..
                    } => Some(*component),
                    _ => None,
                })
                .collect();
            RootSliceSummary {
                root: *root,
                retained_shapes: shapes.len(),
                retained_rules: rules,
                retained_constraints: constraints,
                retained_targets: targets,
                retained_components: components.len(),
            }
        })
        .collect();

    let retained_shape_reasons = program
        .shapes
        .iter()
        .filter(|shape| retained_shapes.contains(&shape.id))
        .map(|shape| {
            let roots = sorted_roots(shape_roots.get(&shape.id));
            let reason = if root_ids.contains(&shape.id) {
                SliceReason::Root { roots }
            } else {
                SliceReason::ReachableFromRoots { roots }
            };
            (shape.id, reason)
        })
        .collect();
    let dropped_shape_reasons = program
        .shapes
        .iter()
        .filter(|shape| !retained_shapes.contains(&shape.id))
        .map(|shape| (shape.id, SliceReason::UnreachableFromRoots))
        .collect();
    let retained_constraint_reasons = program
        .constraints
        .iter()
        .filter(|constraint| retained_constraints.contains(&constraint.id))
        .map(|constraint| {
            (
                constraint.id,
                SliceReason::OwnedByRetainedShape {
                    owner: constraint.owner,
                    roots: sorted_roots(shape_roots.get(&constraint.owner)),
                },
            )
        })
        .collect();
    let dropped_constraint_reasons = program
        .constraints
        .iter()
        .filter(|constraint| !retained_constraints.contains(&constraint.id))
        .map(|constraint| {
            (
                constraint.id,
                SliceReason::OwnerShapeDropped {
                    owner: constraint.owner,
                },
            )
        })
        .collect();
    let retained_target_reasons = program
        .targets
        .iter()
        .filter(|target| retained_targets.contains(&target.id))
        .map(|target| {
            (
                target.id,
                SliceReason::OwnedByRetainedShape {
                    owner: target.owner,
                    roots: sorted_roots(shape_roots.get(&target.owner)),
                },
            )
        })
        .collect();
    let dropped_target_reasons = program
        .targets
        .iter()
        .filter(|target| !retained_targets.contains(&target.id))
        .map(|target| {
            (
                target.id,
                SliceReason::OwnerShapeDropped {
                    owner: target.owner,
                },
            )
        })
        .collect();
    let retained_rule_reasons = program
        .rules
        .iter()
        .filter(|rule| retained_rules.contains(&rule.id))
        .map(|rule| {
            (
                rule.id,
                SliceReason::OwnedByRetainedShape {
                    owner: rule.owner,
                    roots: sorted_roots(shape_roots.get(&rule.owner)),
                },
            )
        })
        .collect();
    let dropped_rule_reasons = program
        .rules
        .iter()
        .filter(|rule| !retained_rules.contains(&rule.id))
        .map(|rule| {
            (
                rule.id,
                SliceReason::OwnerShapeDropped { owner: rule.owner },
            )
        })
        .collect();
    let component_constraints = component_constraint_map(program, &retained_constraints);
    let retained_component_reasons = program
        .constraint_components
        .iter()
        .filter(|component| retained_components.contains(&component.id))
        .map(|component| {
            (
                component.id,
                SliceReason::ReferencedByRetainedConstraints {
                    constraints: component_constraints
                        .get(&component.id)
                        .cloned()
                        .unwrap_or_default(),
                },
            )
        })
        .collect();
    let dropped_component_reasons = program
        .constraint_components
        .iter()
        .filter(|component| !retained_components.contains(&component.id))
        .map(|component| (component.id, SliceReason::UnreferencedAfterSlicing))
        .collect();

    ProgramSlice {
        roots: root_ids.clone(),
        requested_root_selectors: resolved_roots.selectors,
        unresolved_root_selectors: resolved_roots.unresolved_selectors,
        retained_shape_ids: retained_shapes.iter().copied().collect(),
        retained_rule_ids: retained_rules.iter().copied().collect(),
        retained_constraint_ids: retained_constraints.iter().copied().collect(),
        retained_target_ids: retained_targets.iter().copied().collect(),
        retained_component_ids: retained_components.iter().copied().collect(),
        dropped_shape_ids: program
            .shapes
            .iter()
            .map(|shape| shape.id)
            .filter(|id| !retained_shapes.contains(id))
            .collect(),
        dropped_rule_ids: program
            .rules
            .iter()
            .map(|rule| rule.id)
            .filter(|id| !retained_rules.contains(id))
            .collect(),
        dropped_constraint_ids: program
            .constraints
            .iter()
            .map(|constraint| constraint.id)
            .filter(|id| !retained_constraints.contains(id))
            .collect(),
        dropped_target_ids: program
            .targets
            .iter()
            .map(|target| target.id)
            .filter(|id| !retained_targets.contains(id))
            .collect(),
        dropped_component_ids: program
            .constraint_components
            .iter()
            .map(|component| component.id)
            .filter(|id| !retained_components.contains(id))
            .collect(),
        retained_shape_reasons,
        retained_rule_reasons,
        retained_constraint_reasons,
        retained_target_reasons,
        retained_component_reasons,
        dropped_shape_reasons,
        dropped_rule_reasons,
        dropped_constraint_reasons,
        dropped_target_reasons,
        dropped_component_reasons,
        root_summaries,
        inspection: reduced.inspection.clone(),
        reduced_program: reduced,
    }
}

pub fn context_requirements(program: &ShapeProgram) -> ContextFootprintReport {
    let components = analyze_program(program).dependency_components;
    let recursive_shapes: HashSet<_> = components
        .into_iter()
        .filter(|component| component.recursive)
        .flat_map(|component| component.shapes)
        .collect();

    let mut shape_footprints = BTreeMap::new();
    let mut shape_reasons = BTreeMap::new();
    for shape in &program.shapes {
        let mut footprint = ContextFootprint::TargetOnly;
        let mut reasons = Vec::new();
        for target in program
            .targets
            .iter()
            .filter(|target| target.owner == shape.id)
        {
            let (target_footprint, target_reason) = target_footprint(&target.expr);
            footprint = max_footprint(footprint, target_footprint.clone());
            if let Some(reason) = target_reason {
                reasons.push(reason);
            }
        }
        if let Some(path) = &shape.path {
            let path_footprint = path_footprint(path);
            footprint = max_footprint(footprint, path_footprint.clone());
            reasons.push(format!("path {}", path_reason(path)));
        }
        if !shape.property_shapes.is_empty() {
            footprint = max_footprint(footprint, ContextFootprint::SingleHopPath);
            reasons.push("property shapes follow outgoing edges".to_string());
        }
        for constraint in program
            .constraints
            .iter()
            .filter(|constraint| constraint.owner == shape.id)
        {
            let (constraint_footprint, constraint_reason) = constraint_footprint(&constraint.expr);
            footprint = max_footprint(footprint, constraint_footprint);
            reasons.push(constraint_reason);
        }
        for rule in program.rules.iter().filter(|rule| rule.owner == shape.id) {
            let (rule_class, rule_reason) = rule_footprint(&rule.expr);
            footprint = max_footprint(footprint, rule_class);
            reasons.push(rule_reason);
        }
        if recursive_shapes.contains(&shape.id) {
            footprint = max_footprint(footprint, ContextFootprint::RecursiveNeighborhood);
            reasons.push("shape is in a recursive dependency component".to_string());
        }
        if reasons.is_empty() {
            reasons.push("targets only".to_string());
        }
        shape_footprints.insert(shape.id, footprint);
        shape_reasons.insert(shape.id, reasons);
    }

    let mut rule_footprints = BTreeMap::new();
    let mut rule_reasons = BTreeMap::new();
    for rule in &program.rules {
        let (mut footprint, reason) = rule_footprint(&rule.expr);
        let mut reasons = vec![reason];
        if recursive_shapes.contains(&rule.owner) {
            footprint = max_footprint(footprint, ContextFootprint::RecursiveNeighborhood);
            reasons.push("owner shape is recursive".to_string());
        }
        rule_footprints.insert(rule.id, footprint);
        rule_reasons.insert(rule.id, reasons);
    }

    let mut histogram = BTreeMap::new();
    for footprint in shape_footprints.values().chain(rule_footprints.values()) {
        *histogram
            .entry(footprint_name(footprint).to_string())
            .or_insert(0) += 1;
    }

    ContextFootprintReport {
        shape_footprints,
        rule_footprints,
        shape_reasons,
        rule_reasons,
        histogram,
    }
}

pub fn fingerprint_program(program: &ShapeProgram) -> FingerprintReport {
    let constraint_fingerprints: BTreeMap<_, _> = program
        .constraints
        .iter()
        .map(|constraint| (constraint.id, fingerprint_constraint(&constraint.expr)))
        .collect();
    let target_fingerprints: BTreeMap<_, _> = program
        .targets
        .iter()
        .map(|target| (target.id, fingerprint_target(&target.expr)))
        .collect();
    let rule_fingerprints: BTreeMap<_, _> = program
        .rules
        .iter()
        .map(|rule| (rule.id, fingerprint_rule(&rule.expr)))
        .collect();

    FingerprintReport {
        duplicate_constraints: duplicate_groups(&constraint_fingerprints),
        duplicate_targets: duplicate_groups(&target_fingerprints),
        duplicate_rules: duplicate_groups(&rule_fingerprints),
        constraint_fingerprints,
        target_fingerprints,
        rule_fingerprints,
    }
}

pub fn shared_work_candidates(
    program: &ShapeProgram,
    fingerprints: &FingerprintReport,
) -> SharedWorkReport {
    let duplicate_constraints = fingerprints
        .duplicate_constraints
        .iter()
        .map(|group| SharedWorkCandidate {
            fingerprint: group.fingerprint.clone(),
            ids: group.ids.clone(),
            owner_shapes: constraint_owners(program, &group.ids),
        })
        .collect();
    let duplicate_targets = fingerprints
        .duplicate_targets
        .iter()
        .map(|group| SharedWorkCandidate {
            fingerprint: group.fingerprint.clone(),
            ids: group.ids.clone(),
            owner_shapes: target_owners(program, &group.ids),
        })
        .collect();
    let duplicate_rules = fingerprints
        .duplicate_rules
        .iter()
        .map(|group| SharedWorkCandidate {
            fingerprint: group.fingerprint.clone(),
            ids: group.ids.clone(),
            owner_shapes: rule_owners(program, &group.ids),
        })
        .collect();
    let duplicate_custom_component_constraints = fingerprints
        .duplicate_constraints
        .iter()
        .filter(|group| {
            group.ids.iter().all(|id| {
                program
                    .constraints
                    .iter()
                    .find(|constraint| constraint.id == *id)
                    .map(|constraint| {
                        matches!(constraint.expr, ConstraintExpr::CustomComponent { .. })
                    })
                    .unwrap_or(false)
            })
        })
        .map(|group| SharedWorkCandidate {
            fingerprint: group.fingerprint.clone(),
            ids: group.ids.clone(),
            owner_shapes: constraint_owners(program, &group.ids),
        })
        .collect();
    let duplicate_sparql_constraints = fingerprints
        .duplicate_constraints
        .iter()
        .filter(|group| {
            group.ids.iter().all(|id| {
                program
                    .constraints
                    .iter()
                    .find(|constraint| constraint.id == *id)
                    .map(|constraint| matches!(constraint.expr, ConstraintExpr::Sparql(_)))
                    .unwrap_or(false)
            })
        })
        .map(|group| SharedWorkCandidate {
            fingerprint: group.fingerprint.clone(),
            ids: group.ids.clone(),
            owner_shapes: constraint_owners(program, &group.ids),
        })
        .collect();
    let duplicate_sparql_rules = fingerprints
        .duplicate_rules
        .iter()
        .filter(|group| {
            group.ids.iter().all(|id| {
                program
                    .rules
                    .iter()
                    .find(|rule| rule.id == *id)
                    .map(|rule| matches!(rule.expr, RuleExpr::Sparql { .. }))
                    .unwrap_or(false)
            })
        })
        .map(|group| SharedWorkCandidate {
            fingerprint: group.fingerprint.clone(),
            ids: group.ids.clone(),
            owner_shapes: rule_owners(program, &group.ids),
        })
        .collect();

    SharedWorkReport {
        duplicate_constraints,
        duplicate_targets,
        duplicate_rules,
        duplicate_custom_component_constraints,
        duplicate_sparql_constraints,
        duplicate_sparql_rules,
    }
}

pub fn static_cost_hints(
    program: &ShapeProgram,
    baseline: &AnalysisSummary,
    context: &ContextFootprintReport,
    shared_work: &SharedWorkReport,
) -> StaticCostHintReport {
    let recursive_shapes: BTreeSet<_> = baseline
        .dependency_components
        .iter()
        .filter(|component| component.recursive)
        .flat_map(|component| component.shapes.iter().copied())
        .collect();
    let duplicate_constraint_ids = shared_work
        .duplicate_constraints
        .iter()
        .flat_map(|group| group.ids.iter().copied())
        .collect::<BTreeSet<_>>();
    let duplicate_target_ids = shared_work
        .duplicate_targets
        .iter()
        .flat_map(|group| group.ids.iter().copied())
        .collect::<BTreeSet<_>>();
    let duplicate_rule_ids = shared_work
        .duplicate_rules
        .iter()
        .flat_map(|group| group.ids.iter().copied())
        .collect::<BTreeSet<_>>();

    let mut shape_hints = BTreeMap::new();
    for shape in &program.shapes {
        let mut hints = BTreeSet::new();
        if shape.targets.is_empty() {
            hints.insert(StaticCostHint::HelperShape);
        } else {
            hints.insert(StaticCostHint::EntryShape);
        }
        if recursive_shapes.contains(&shape.id) {
            hints.insert(StaticCostHint::Recursive);
        }
        if program
            .constraints
            .iter()
            .filter(|constraint| constraint.owner == shape.id)
            .any(|constraint| matches!(constraint.expr, ConstraintExpr::Closed { .. }))
        {
            hints.insert(StaticCostHint::ClosedShape);
        }
        if program
            .constraints
            .iter()
            .filter(|constraint| constraint.owner == shape.id)
            .any(|constraint| matches!(constraint.expr, ConstraintExpr::QualifiedValueShape { .. }))
        {
            hints.insert(StaticCostHint::QualifiedValueShape);
        }
        if program.rules.iter().any(|rule| rule.owner == shape.id) {
            hints.insert(StaticCostHint::HasRules);
        }
        if program
            .constraints
            .iter()
            .filter(|constraint| constraint.owner == shape.id)
            .any(|constraint| matches!(constraint.expr, ConstraintExpr::Sparql(_)))
            || program
                .rules
                .iter()
                .filter(|rule| rule.owner == shape.id)
                .any(|rule| matches!(rule.expr, RuleExpr::Sparql { .. }))
            || program
                .targets
                .iter()
                .filter(|target| target.owner == shape.id)
                .any(|target| matches!(target.expr, TargetExpr::Advanced(_)))
        {
            hints.insert(StaticCostHint::HasSparql);
        }
        if program
            .constraints
            .iter()
            .filter(|constraint| constraint.owner == shape.id)
            .any(|constraint| duplicate_constraint_ids.contains(&constraint.id))
        {
            hints.insert(StaticCostHint::DuplicateConstraintWork);
        }
        if program
            .targets
            .iter()
            .filter(|target| target.owner == shape.id)
            .any(|target| duplicate_target_ids.contains(&target.id))
        {
            hints.insert(StaticCostHint::DuplicateTargetWork);
        }
        if program
            .rules
            .iter()
            .filter(|rule| rule.owner == shape.id)
            .any(|rule| duplicate_rule_ids.contains(&rule.id))
        {
            hints.insert(StaticCostHint::DuplicateRuleWork);
        }
        match context.shape_footprints.get(&shape.id) {
            Some(ContextFootprint::SingleHopPath) => {
                hints.insert(StaticCostHint::SingleHopPath);
            }
            Some(ContextFootprint::BoundedTraversal) => {
                hints.insert(StaticCostHint::BoundedTraversal);
            }
            Some(ContextFootprint::ShapeReferenceTraversal) => {
                hints.insert(StaticCostHint::ShapeReferenceTraversal);
            }
            _ => {}
        }
        shape_hints.insert(shape.id, hints.into_iter().collect());
    }

    let mut rule_hints = BTreeMap::new();
    for rule in &program.rules {
        let mut hints = BTreeSet::new();
        if recursive_shapes.contains(&rule.owner) {
            hints.insert(StaticCostHint::Recursive);
        }
        if matches!(rule.expr, RuleExpr::Sparql { .. }) {
            hints.insert(StaticCostHint::HasSparql);
        }
        if duplicate_rule_ids.contains(&rule.id) {
            hints.insert(StaticCostHint::DuplicateRuleWork);
        }
        match context.rule_footprints.get(&rule.id) {
            Some(ContextFootprint::SingleHopPath) => {
                hints.insert(StaticCostHint::SingleHopPath);
            }
            Some(ContextFootprint::BoundedTraversal) => {
                hints.insert(StaticCostHint::BoundedTraversal);
            }
            Some(ContextFootprint::ShapeReferenceTraversal) => {
                hints.insert(StaticCostHint::ShapeReferenceTraversal);
            }
            _ => {}
        }
        rule_hints.insert(rule.id, hints.into_iter().collect());
    }

    let mut histogram = BTreeMap::new();
    for hint in shape_hints.values().chain(rule_hints.values()).flatten() {
        *histogram
            .entry(cost_hint_name(hint).to_string())
            .or_insert(0) += 1;
    }

    StaticCostHintReport {
        shape_hints,
        rule_hints,
        histogram,
    }
}

struct ResolvedRoots {
    ids: Vec<ShapeId>,
    selectors: Vec<String>,
    unresolved_selectors: Vec<String>,
}

fn resolve_roots(program: &ShapeProgram, roots: SliceRoots) -> ResolvedRoots {
    match roots {
        SliceRoots::TargetShapes => {
            let mut ids = program
                .shapes
                .iter()
                .filter(|shape| !shape.targets.is_empty())
                .map(|shape| shape.id)
                .collect::<Vec<_>>();
            ids.sort();
            ids.dedup();
            ResolvedRoots {
                ids,
                selectors: Vec::new(),
                unresolved_selectors: Vec::new(),
            }
        }
        SliceRoots::ExplicitShapes(mut ids) => {
            ids.sort();
            ids.dedup();
            ResolvedRoots {
                ids,
                selectors: Vec::new(),
                unresolved_selectors: Vec::new(),
            }
        }
        SliceRoots::ExplicitSelectors(selectors) => {
            let mut ids = Vec::new();
            let mut unresolved = Vec::new();
            for selector in &selectors {
                if let Some(id) = program
                    .normalized_shape_index
                    .get(selector)
                    .or_else(|| program.shape_index.get(selector))
                {
                    ids.push(*id);
                } else {
                    unresolved.push(selector.clone());
                }
            }
            ids.sort();
            ids.dedup();
            ResolvedRoots {
                ids,
                selectors,
                unresolved_selectors: unresolved,
            }
        }
    }
}

fn adjacency_map(program: &ShapeProgram) -> HashMap<ShapeId, Vec<ShapeId>> {
    let mut adjacency = HashMap::new();
    for shape in &program.shapes {
        adjacency.entry(shape.id).or_insert_with(Vec::new);
    }
    for dependency in &program.dependencies {
        adjacency
            .entry(dependency.from)
            .or_insert_with(Vec::new)
            .push(dependency.to);
    }
    adjacency
}

fn reachable_shapes(roots: &[ShapeId], adjacency: &HashMap<ShapeId, Vec<ShapeId>>) -> Vec<ShapeId> {
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

fn component_constraint_map(
    program: &ShapeProgram,
    retained_constraints: &BTreeSet<ConstraintId>,
) -> BTreeMap<ComponentDefId, Vec<ConstraintId>> {
    let mut map = BTreeMap::new();
    for constraint in program
        .constraints
        .iter()
        .filter(|constraint| retained_constraints.contains(&constraint.id))
    {
        if let ConstraintExpr::CustomComponent {
            component: Some(component),
            ..
        } = &constraint.expr
        {
            map.entry(*component)
                .or_insert_with(Vec::new)
                .push(constraint.id);
        }
    }
    map
}

fn sorted_roots(roots: Option<&BTreeSet<ShapeId>>) -> Vec<ShapeId> {
    roots
        .map(|roots| roots.iter().copied().collect())
        .unwrap_or_default()
}

fn max_footprint(left: ContextFootprint, right: ContextFootprint) -> ContextFootprint {
    if footprint_rank(&left) >= footprint_rank(&right) {
        left
    } else {
        right
    }
}

fn footprint_rank(footprint: &ContextFootprint) -> u8 {
    match footprint {
        ContextFootprint::TargetOnly => 0,
        ContextFootprint::NodeLocal => 1,
        ContextFootprint::SingleHopPath => 2,
        ContextFootprint::BoundedTraversal => 3,
        ContextFootprint::ShapeReferenceTraversal => 4,
        ContextFootprint::RecursiveNeighborhood => 5,
        ContextFootprint::GlobalSparql => 6,
    }
}

fn footprint_name(footprint: &ContextFootprint) -> &'static str {
    match footprint {
        ContextFootprint::TargetOnly => "target_only",
        ContextFootprint::NodeLocal => "node_local",
        ContextFootprint::SingleHopPath => "single_hop_path",
        ContextFootprint::BoundedTraversal => "bounded_traversal",
        ContextFootprint::ShapeReferenceTraversal => "shape_reference_traversal",
        ContextFootprint::RecursiveNeighborhood => "recursive_neighborhood",
        ContextFootprint::GlobalSparql => "global_sparql",
    }
}

fn constraint_footprint(constraint: &ConstraintExpr) -> (ContextFootprint, String) {
    match constraint {
        ConstraintExpr::Sparql(_) => (
            ContextFootprint::GlobalSparql,
            "SPARQL constraint can touch the whole graph".to_string(),
        ),
        ConstraintExpr::NodeRef { .. } => (
            ContextFootprint::ShapeReferenceTraversal,
            "node constraint follows a referenced shape".to_string(),
        ),
        ConstraintExpr::PropertyRef { .. } => (
            ContextFootprint::ShapeReferenceTraversal,
            "property constraint follows a referenced property shape".to_string(),
        ),
        ConstraintExpr::QualifiedValueShape { .. } => (
            ContextFootprint::ShapeReferenceTraversal,
            "qualified value shape follows a referenced shape".to_string(),
        ),
        ConstraintExpr::Logical { .. } => (
            ContextFootprint::ShapeReferenceTraversal,
            "logical constraint traverses referenced shapes".to_string(),
        ),
        ConstraintExpr::Not { .. } => (
            ContextFootprint::ShapeReferenceTraversal,
            "not constraint traverses a referenced shape".to_string(),
        ),
        ConstraintExpr::PropertyComparison { .. } => (
            ContextFootprint::SingleHopPath,
            "property comparison reads sibling property values".to_string(),
        ),
        ConstraintExpr::Class(_)
        | ConstraintExpr::Datatype(_)
        | ConstraintExpr::NodeKind(_)
        | ConstraintExpr::Cardinality { .. }
        | ConstraintExpr::NumericRange { .. }
        | ConstraintExpr::StringConstraint { .. }
        | ConstraintExpr::Closed { .. }
        | ConstraintExpr::HasValue(_)
        | ConstraintExpr::In(_)
        | ConstraintExpr::CustomComponent { .. }
        | ConstraintExpr::GenericPredicate { .. } => (
            ContextFootprint::NodeLocal,
            "constraint is local to the current node or value".to_string(),
        ),
    }
}

fn target_footprint(target: &TargetExpr) -> (ContextFootprint, Option<String>) {
    match target {
        TargetExpr::Advanced(_) => (
            ContextFootprint::GlobalSparql,
            Some("advanced target uses query-based targeting".to_string()),
        ),
        TargetExpr::SubjectsOf(_) | TargetExpr::ObjectsOf(_) => (
            ContextFootprint::SingleHopPath,
            Some("target follows one predicate hop".to_string()),
        ),
        TargetExpr::Class(_) | TargetExpr::Node(_) => (ContextFootprint::TargetOnly, None),
    }
}

fn rule_footprint(rule: &RuleExpr) -> (ContextFootprint, String) {
    match rule {
        RuleExpr::Sparql { .. } => (
            ContextFootprint::GlobalSparql,
            "SPARQL rule can touch the whole graph".to_string(),
        ),
        RuleExpr::Triple {
            subject,
            object,
            conditions,
            ..
        } => {
            if !conditions.is_empty() {
                (
                    ContextFootprint::ShapeReferenceTraversal,
                    "triple rule depends on condition shapes".to_string(),
                )
            } else if matches!(subject, Some(TriplePatternTerm::Path(_)))
                || matches!(object, Some(TriplePatternTerm::Path(_)))
            {
                let path = subject
                    .as_ref()
                    .or(object.as_ref())
                    .and_then(|term| match term {
                        TriplePatternTerm::Path(path) => Some(path),
                        _ => None,
                    });
                let footprint = path
                    .map(path_footprint)
                    .unwrap_or(ContextFootprint::SingleHopPath);
                (
                    footprint,
                    "triple rule follows property-path bindings".to_string(),
                )
            } else {
                (
                    ContextFootprint::NodeLocal,
                    "triple rule is local to the current node".to_string(),
                )
            }
        }
        RuleExpr::Generic { conditions, .. } => {
            if conditions.is_empty() {
                (
                    ContextFootprint::NodeLocal,
                    "generic rule is local to the current node".to_string(),
                )
            } else {
                (
                    ContextFootprint::ShapeReferenceTraversal,
                    "generic rule depends on condition shapes".to_string(),
                )
            }
        }
    }
}

fn path_footprint(path: &PropertyPath) -> ContextFootprint {
    match path {
        PropertyPath::Predicate(_) => ContextFootprint::SingleHopPath,
        PropertyPath::Inverse(inner) => path_footprint(inner),
        PropertyPath::Sequence(paths) => {
            if paths.len() <= 1 {
                paths
                    .first()
                    .map(path_footprint)
                    .unwrap_or(ContextFootprint::SingleHopPath)
            } else {
                ContextFootprint::BoundedTraversal
            }
        }
        PropertyPath::Alternative(_) => ContextFootprint::BoundedTraversal,
        PropertyPath::ZeroOrMore(_) | PropertyPath::OneOrMore(_) | PropertyPath::ZeroOrOne(_) => {
            ContextFootprint::BoundedTraversal
        }
        PropertyPath::Unsupported(_) => ContextFootprint::BoundedTraversal,
    }
}

fn path_reason(path: &PropertyPath) -> &'static str {
    match path {
        PropertyPath::Predicate(_) => "uses a single predicate hop",
        PropertyPath::Inverse(_) => "uses an inverse predicate hop",
        PropertyPath::Sequence(_) => "uses a sequence path traversal",
        PropertyPath::Alternative(_) => "uses an alternative path traversal",
        PropertyPath::ZeroOrMore(_) => "uses a zero-or-more path traversal",
        PropertyPath::OneOrMore(_) => "uses a one-or-more path traversal",
        PropertyPath::ZeroOrOne(_) => "uses a zero-or-one path traversal",
        PropertyPath::Unsupported(_) => "uses an unsupported path form",
    }
}

fn duplicate_groups<T>(fingerprints: &BTreeMap<T, String>) -> Vec<DuplicateGroup<T>>
where
    T: Copy + Eq + Ord + Serialize,
{
    let mut grouped: BTreeMap<String, Vec<T>> = BTreeMap::new();
    for (id, fingerprint) in fingerprints {
        grouped.entry(fingerprint.clone()).or_default().push(*id);
    }
    grouped
        .into_iter()
        .filter_map(|(fingerprint, mut ids)| {
            if ids.len() < 2 {
                return None;
            }
            ids.sort();
            Some(DuplicateGroup { fingerprint, ids })
        })
        .collect()
}

fn constraint_owners(program: &ShapeProgram, ids: &[ConstraintId]) -> Vec<ShapeId> {
    let mut owners = ids
        .iter()
        .filter_map(|id| {
            program
                .constraints
                .iter()
                .find(|constraint| constraint.id == *id)
                .map(|constraint| constraint.owner)
        })
        .collect::<Vec<_>>();
    owners.sort();
    owners.dedup();
    owners
}

fn target_owners(program: &ShapeProgram, ids: &[TargetId]) -> Vec<ShapeId> {
    let mut owners = ids
        .iter()
        .filter_map(|id| {
            program
                .targets
                .iter()
                .find(|target| target.id == *id)
                .map(|target| target.owner)
        })
        .collect::<Vec<_>>();
    owners.sort();
    owners.dedup();
    owners
}

fn rule_owners(program: &ShapeProgram, ids: &[RuleId]) -> Vec<ShapeId> {
    let mut owners = ids
        .iter()
        .filter_map(|id| {
            program
                .rules
                .iter()
                .find(|rule| rule.id == *id)
                .map(|rule| rule.owner)
        })
        .collect::<Vec<_>>();
    owners.sort();
    owners.dedup();
    owners
}

fn cost_hint_name(hint: &StaticCostHint) -> &'static str {
    match hint {
        StaticCostHint::EntryShape => "entry_shape",
        StaticCostHint::HelperShape => "helper_shape",
        StaticCostHint::HasSparql => "has_sparql",
        StaticCostHint::Recursive => "recursive",
        StaticCostHint::ClosedShape => "closed_shape",
        StaticCostHint::QualifiedValueShape => "qualified_value_shape",
        StaticCostHint::HasRules => "has_rules",
        StaticCostHint::DuplicateConstraintWork => "duplicate_constraint_work",
        StaticCostHint::DuplicateTargetWork => "duplicate_target_work",
        StaticCostHint::DuplicateRuleWork => "duplicate_rule_work",
        StaticCostHint::SingleHopPath => "single_hop_path",
        StaticCostHint::BoundedTraversal => "bounded_traversal",
        StaticCostHint::ShapeReferenceTraversal => "shape_reference_traversal",
    }
}

fn fingerprint_constraint(constraint: &ConstraintExpr) -> String {
    match constraint {
        ConstraintExpr::NodeRef { shape, source } => {
            format!("node_ref:{shape:?}:{source}")
        }
        ConstraintExpr::PropertyRef { shape, source } => {
            format!("property_ref:{shape:?}:{source}")
        }
        ConstraintExpr::QualifiedValueShape {
            shape,
            source,
            min_count,
            max_count,
            disjoint,
        } => format!("qvs:{shape:?}:{source}:{min_count:?}:{max_count:?}:{disjoint:?}"),
        ConstraintExpr::Logical { kind, shapes } => {
            format!("logical:{}:{:?}", logical_name(kind), shapes)
        }
        ConstraintExpr::Not { shape, source } => format!("not:{shape:?}:{source}"),
        ConstraintExpr::Class(term) => format!("class:{term}"),
        ConstraintExpr::Datatype(term) => format!("datatype:{term}"),
        ConstraintExpr::NodeKind(term) => format!("node_kind:{term}"),
        ConstraintExpr::Cardinality {
            predicate,
            min,
            max,
        } => {
            format!("cardinality:{predicate}:{min:?}:{max:?}")
        }
        ConstraintExpr::NumericRange { predicate, values } => {
            format!("numeric_range:{predicate}:{}", terms_key(values))
        }
        ConstraintExpr::StringConstraint { predicate, values } => {
            format!("string_constraint:{predicate}:{}", terms_key(values))
        }
        ConstraintExpr::PropertyComparison { predicate, values } => {
            format!("property_comparison:{predicate}:{}", terms_key(values))
        }
        ConstraintExpr::Closed { ignored_properties } => {
            format!("closed:{}", terms_key(ignored_properties))
        }
        ConstraintExpr::HasValue(term) => format!("has_value:{term}"),
        ConstraintExpr::In(values) => format!("in:{}", terms_key(values)),
        ConstraintExpr::Sparql(sparql) => format!(
            "sparql:{:?}:{:?}:{}:{}:{}",
            sparql.kind,
            sparql.node,
            sparql.select.as_deref().unwrap_or(""),
            sparql.ask.as_deref().unwrap_or(""),
            terms_key(&sparql.messages)
        ),
        ConstraintExpr::CustomComponent {
            predicate,
            component,
            values,
            bindings,
            message_templates,
            label_template,
        } => format!(
            "custom_component:{predicate}:{component:?}:{}:{}:{:?}",
            terms_key(values),
            bindings
                .iter()
                .map(|binding| format!(
                    "{}:{}:{}:{}",
                    binding.parameter,
                    binding.name,
                    terms_key(&binding.values),
                    binding.from_default
                ))
                .collect::<Vec<_>>()
                .join("|"),
            (
                message_templates
                    .iter()
                    .map(|template| template.raw.clone())
                    .collect::<Vec<_>>(),
                label_template.as_ref().map(|template| template.raw.clone())
            )
        ),
        ConstraintExpr::GenericPredicate { predicate, values } => {
            format!("generic:{predicate}:{}", terms_key(values))
        }
    }
}

fn fingerprint_target(target: &TargetExpr) -> String {
    match target {
        TargetExpr::Class(term) => format!("target_class:{term}"),
        TargetExpr::Node(term) => format!("target_node:{term}"),
        TargetExpr::SubjectsOf(term) => format!("target_subjects_of:{term}"),
        TargetExpr::ObjectsOf(term) => format!("target_objects_of:{term}"),
        TargetExpr::Advanced(target) => format!(
            "advanced_target:{}:{}:{:?}:{:?}:{}:{}",
            target.select.as_deref().unwrap_or(""),
            target.ask.as_deref().unwrap_or(""),
            target.target_shape_id,
            target.filter_shape_id,
            terms_key(&target.prefixes),
            target
                .declarations
                .iter()
                .map(|declaration| format!(
                    "{}={}",
                    declaration.prefix.as_deref().unwrap_or(""),
                    declaration
                        .namespace
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default()
                ))
                .collect::<Vec<_>>()
                .join("|")
        ),
    }
}

fn fingerprint_rule(rule: &RuleExpr) -> String {
    match rule {
        RuleExpr::Triple {
            subject,
            predicate,
            object,
            conditions,
            order,
            ..
        } => format!(
            "triple_rule:{}:{}:{}:{:?}:{:?}",
            triple_pattern_key(subject.as_ref()),
            predicate
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            triple_pattern_key(object.as_ref()),
            conditions,
            order
        ),
        RuleExpr::Sparql {
            query,
            conditions,
            order,
            ..
        } => format!(
            "sparql_rule:{}:{:?}:{:?}",
            query.as_deref().unwrap_or(""),
            conditions,
            order
        ),
        RuleExpr::Generic {
            node,
            conditions,
            order,
        } => format!("generic_rule:{node}:{:?}:{:?}", conditions, order),
    }
}

fn triple_pattern_key(term: Option<&TriplePatternTerm>) -> String {
    match term {
        Some(TriplePatternTerm::This) => "this".to_string(),
        Some(TriplePatternTerm::Constant(term)) => term.to_string(),
        Some(TriplePatternTerm::Path(path)) => path_key(path),
        None => String::new(),
    }
}

fn path_key(path: &PropertyPath) -> String {
    match path {
        PropertyPath::Predicate(node) => format!("pred:{node}"),
        PropertyPath::Inverse(inner) => format!("inverse:{}", path_key(inner)),
        PropertyPath::Sequence(paths) => format!(
            "seq:{}",
            paths.iter().map(path_key).collect::<Vec<_>>().join("|")
        ),
        PropertyPath::Alternative(paths) => format!(
            "alt:{}",
            paths.iter().map(path_key).collect::<Vec<_>>().join("|")
        ),
        PropertyPath::ZeroOrMore(inner) => format!("zero_or_more:{}", path_key(inner)),
        PropertyPath::OneOrMore(inner) => format!("one_or_more:{}", path_key(inner)),
        PropertyPath::ZeroOrOne(inner) => format!("zero_or_one:{}", path_key(inner)),
        PropertyPath::Unsupported(term) => format!("unsupported:{term}"),
    }
}

fn terms_key(values: &[oxrdf::Term]) -> String {
    values
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("|")
}

fn logical_name(kind: &LogicalKind) -> &'static str {
    match kind {
        LogicalKind::And => "and",
        LogicalKind::Or => "or",
        LogicalKind::Xone => "xone",
    }
}
