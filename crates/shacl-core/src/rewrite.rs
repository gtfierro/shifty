use crate::algebra::{ComponentDefId, ConstraintExpr, ConstraintId, RuleId, ShapeId, ShapeProgram};
use crate::analysis::analyze_program;
use crate::passes::canonicalize_program;
use crate::static_analysis::{
    SliceRoots, StaticCostHint, context_requirements, slice_program, static_cost_hints,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewriteOptions {
    pub roots: Option<SliceRoots>,
    pub prune_unreachable: bool,
    pub prune_unreferenced_components: bool,
    pub reorder_constraints: bool,
    pub reorder_rules: bool,
    pub annotate_recursive_regions: bool,
}

impl Default for RewriteOptions {
    fn default() -> Self {
        Self {
            roots: None,
            prune_unreachable: false,
            prune_unreferenced_components: true,
            reorder_constraints: true,
            reorder_rules: true,
            annotate_recursive_regions: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RewritePassRecord {
    pub pass: String,
    pub changed: bool,
    pub details: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecursiveRegion {
    pub id: u64,
    pub shapes: Vec<ShapeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RewriteSummary {
    pub passes: Vec<RewritePassRecord>,
    pub shapes_removed: usize,
    pub constraints_removed: usize,
    pub targets_removed: usize,
    pub rules_removed: usize,
    pub components_removed: usize,
    pub recursive_regions: Vec<RecursiveRegion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RewrittenProgram {
    pub program: ShapeProgram,
    pub summary: RewriteSummary,
}

pub fn rewrite_program(program: &ShapeProgram, options: RewriteOptions) -> RewrittenProgram {
    let original = canonicalize_program(program);
    let mut working = original.clone();
    let mut passes = Vec::new();

    if let Some(roots) = options.roots.clone() {
        let slice = slice_program(&working, roots);
        let details = vec![format!(
            "retained {} shapes, {} constraints, {} rules, {} targets, {} components",
            slice.retained_shape_ids.len(),
            slice.retained_constraint_ids.len(),
            slice.retained_rule_ids.len(),
            slice.retained_target_ids.len(),
            slice.retained_component_ids.len()
        )];
        let changed = slice.reduced_program.shapes.len() != working.shapes.len()
            || slice.reduced_program.constraints.len() != working.constraints.len()
            || slice.reduced_program.targets.len() != working.targets.len()
            || slice.reduced_program.rules.len() != working.rules.len()
            || slice.reduced_program.constraint_components.len()
                != working.constraint_components.len();
        working = canonicalize_program(&slice.reduced_program);
        passes.push(RewritePassRecord {
            pass: "slice_to_roots".to_string(),
            changed,
            details,
        });
    }

    if options.prune_unreachable {
        let before = working.clone();
        working = eliminate_dead_structure(&working);
        passes.push(RewritePassRecord {
            pass: "eliminate_dead_structure".to_string(),
            changed: program_size(&before) != program_size(&working),
            details: vec![format!(
                "removed {} dangling shapes, {} constraints, {} targets, {} rules",
                before.shapes.len().saturating_sub(working.shapes.len()),
                before
                    .constraints
                    .len()
                    .saturating_sub(working.constraints.len()),
                before.targets.len().saturating_sub(working.targets.len()),
                before.rules.len().saturating_sub(working.rules.len()),
            )],
        });
    }

    if options.prune_unreferenced_components {
        let before = working.clone();
        working = prune_unreferenced_components(&working);
        passes.push(RewritePassRecord {
            pass: "prune_unreferenced_components".to_string(),
            changed: before.constraint_components.len() != working.constraint_components.len(),
            details: vec![format!(
                "removed {} unreferenced components",
                before
                    .constraint_components
                    .len()
                    .saturating_sub(working.constraint_components.len())
            )],
        });
    }

    if options.reorder_constraints {
        let before = working.clone();
        working = reorder_constraints(&working);
        passes.push(RewritePassRecord {
            pass: "reorder_constraints".to_string(),
            changed: constraint_orders(&before) != constraint_orders(&working),
            details: vec!["ordered constraints by static cost and id".to_string()],
        });
    }

    if options.reorder_rules {
        let before = working.clone();
        working = reorder_rules(&working);
        passes.push(RewritePassRecord {
            pass: "reorder_rules".to_string(),
            changed: rule_orders(&before) != rule_orders(&working),
            details: vec!["ordered rules by static cost and id".to_string()],
        });
    }

    let recursive_regions = if options.annotate_recursive_regions {
        let baseline = analyze_program(&working);
        let recursive = baseline
            .dependency_components
            .iter()
            .enumerate()
            .filter(|(_, component)| component.recursive)
            .map(|(index, component)| RecursiveRegion {
                id: (index + 1) as u64,
                shapes: component.shapes.clone(),
            })
            .collect::<Vec<_>>();
        passes.push(RewritePassRecord {
            pass: "annotate_recursive_regions".to_string(),
            changed: !recursive.is_empty(),
            details: if recursive.is_empty() {
                vec!["no recursive regions detected".to_string()]
            } else {
                vec![format!("annotated {} recursive regions", recursive.len())]
            },
        });
        recursive
    } else {
        Vec::new()
    };

    let summary = RewriteSummary {
        shapes_removed: original.shapes.len().saturating_sub(working.shapes.len()),
        constraints_removed: original
            .constraints
            .len()
            .saturating_sub(working.constraints.len()),
        targets_removed: original.targets.len().saturating_sub(working.targets.len()),
        rules_removed: original.rules.len().saturating_sub(working.rules.len()),
        components_removed: original
            .constraint_components
            .len()
            .saturating_sub(working.constraint_components.len()),
        passes,
        recursive_regions,
    };

    RewrittenProgram {
        program: canonicalize_program(&working),
        summary,
    }
}

fn eliminate_dead_structure(program: &ShapeProgram) -> ShapeProgram {
    let kept_shape_ids: BTreeSet<_> = program.shapes.iter().map(|shape| shape.id).collect();
    let kept_constraint_ids: BTreeSet<_> = program
        .constraints
        .iter()
        .filter(|constraint| kept_shape_ids.contains(&constraint.owner))
        .map(|constraint| constraint.id)
        .collect();
    let kept_target_ids: BTreeSet<_> = program
        .targets
        .iter()
        .filter(|target| kept_shape_ids.contains(&target.owner))
        .map(|target| target.id)
        .collect();
    let kept_rule_ids: BTreeSet<_> = program
        .rules
        .iter()
        .filter(|rule| kept_shape_ids.contains(&rule.owner))
        .map(|rule| rule.id)
        .collect();

    let mut rewritten = program.clone();
    rewritten.shapes = program
        .shapes
        .iter()
        .cloned()
        .map(|mut shape| {
            shape
                .property_shapes
                .retain(|id| kept_shape_ids.contains(id));
            shape
                .constraints
                .retain(|id| kept_constraint_ids.contains(id));
            shape.targets.retain(|id| kept_target_ids.contains(id));
            shape
        })
        .collect();
    rewritten.constraints = program
        .constraints
        .iter()
        .filter(|constraint| kept_constraint_ids.contains(&constraint.id))
        .cloned()
        .collect();
    rewritten.targets = program
        .targets
        .iter()
        .filter(|target| kept_target_ids.contains(&target.id))
        .cloned()
        .collect();
    rewritten.rules = program
        .rules
        .iter()
        .filter(|rule| kept_rule_ids.contains(&rule.id))
        .cloned()
        .collect();
    rewritten.dependencies = program
        .dependencies
        .iter()
        .filter(|edge| kept_shape_ids.contains(&edge.from) && kept_shape_ids.contains(&edge.to))
        .cloned()
        .collect();
    canonicalize_program(&rewritten)
}

fn prune_unreferenced_components(program: &ShapeProgram) -> ShapeProgram {
    let kept_component_ids: BTreeSet<ComponentDefId> = program
        .constraints
        .iter()
        .filter_map(|constraint| match &constraint.expr {
            ConstraintExpr::CustomComponent {
                component: Some(component),
                ..
            } => Some(*component),
            _ => None,
        })
        .collect();
    let mut rewritten = program.clone();
    rewritten.constraint_components = program
        .constraint_components
        .iter()
        .filter(|component| kept_component_ids.contains(&component.id))
        .cloned()
        .collect();
    canonicalize_program(&rewritten)
}

fn reorder_constraints(program: &ShapeProgram) -> ShapeProgram {
    let context = context_requirements(program);
    let baseline = analyze_program(program);
    let shared_work = crate::static_analysis::shared_work_candidates(
        program,
        &crate::static_analysis::fingerprint_program(program),
    );
    let cost_hints = static_cost_hints(program, &baseline, &context, &shared_work);
    let mut rewritten = program.clone();
    rewritten.constraints.sort_by_key(|constraint| {
        (
            constraint.owner,
            constraint_rank(&constraint.expr),
            cost_rank(cost_hints.shape_hints.get(&constraint.owner)),
            constraint.id,
        )
    });
    let order_map: BTreeMap<ConstraintId, usize> = rewritten
        .constraints
        .iter()
        .enumerate()
        .map(|(idx, constraint)| (constraint.id, idx))
        .collect();
    for shape in &mut rewritten.shapes {
        shape.constraints.sort_by_key(|constraint_id| {
            order_map.get(constraint_id).copied().unwrap_or(usize::MAX)
        });
    }
    canonicalize_program(&rewritten)
}

fn reorder_rules(program: &ShapeProgram) -> ShapeProgram {
    let context = context_requirements(program);
    let baseline = analyze_program(program);
    let shared_work = crate::static_analysis::shared_work_candidates(
        program,
        &crate::static_analysis::fingerprint_program(program),
    );
    let cost_hints = static_cost_hints(program, &baseline, &context, &shared_work);
    let mut rewritten = program.clone();
    rewritten.rules.sort_by_key(|rule| {
        (
            rule.owner,
            rule_rank(&rule.expr),
            cost_rank(cost_hints.rule_hints.get(&rule.id)),
            rule.id,
        )
    });
    canonicalize_program(&rewritten)
}

fn program_size(program: &ShapeProgram) -> (usize, usize, usize, usize, usize) {
    (
        program.shapes.len(),
        program.constraints.len(),
        program.targets.len(),
        program.rules.len(),
        program.constraint_components.len(),
    )
}

fn constraint_orders(program: &ShapeProgram) -> BTreeMap<ShapeId, Vec<ConstraintId>> {
    program
        .shapes
        .iter()
        .map(|shape| (shape.id, shape.constraints.clone()))
        .collect()
}

fn rule_orders(program: &ShapeProgram) -> BTreeMap<ShapeId, Vec<RuleId>> {
    let mut per_shape = BTreeMap::<ShapeId, Vec<RuleId>>::new();
    for rule in &program.rules {
        per_shape.entry(rule.owner).or_default().push(rule.id);
    }
    per_shape
}

fn constraint_rank(expr: &ConstraintExpr) -> u8 {
    match expr {
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
        | ConstraintExpr::GenericPredicate { .. } => 0,
        ConstraintExpr::PropertyComparison { .. } => 1,
        ConstraintExpr::NodeRef { .. }
        | ConstraintExpr::PropertyRef { .. }
        | ConstraintExpr::QualifiedValueShape { .. }
        | ConstraintExpr::Logical { .. }
        | ConstraintExpr::Not { .. } => 2,
        ConstraintExpr::Sparql(_) => 3,
    }
}

fn rule_rank(expr: &crate::algebra::RuleExpr) -> u8 {
    match expr {
        crate::algebra::RuleExpr::Generic { conditions, .. } if conditions.is_empty() => 0,
        crate::algebra::RuleExpr::Triple { conditions, .. } if conditions.is_empty() => 1,
        crate::algebra::RuleExpr::Generic { .. } | crate::algebra::RuleExpr::Triple { .. } => 2,
        crate::algebra::RuleExpr::Sparql { .. } => 3,
    }
}

fn cost_rank(hints: Option<&Vec<StaticCostHint>>) -> u8 {
    match hints {
        Some(hints) if hints.contains(&StaticCostHint::HasSparql) => 3,
        Some(hints) if hints.contains(&StaticCostHint::ShapeReferenceTraversal) => 2,
        Some(hints) if hints.contains(&StaticCostHint::BoundedTraversal) => 1,
        _ => 0,
    }
}
