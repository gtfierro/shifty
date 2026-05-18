use crate::algebra::{FeatureUse, ShapeId, ShapeProgram, TargetExpr};
use crate::diagnostics::DiagnosticSeverity;
use crate::diagnostics::InspectionGraph;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyComponent {
    pub shapes: Vec<ShapeId>,
    pub recursive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisSummary {
    pub dependency_components: Vec<DependencyComponent>,
    pub feature_counts: HashMap<String, usize>,
    pub dependency_kind_counts: HashMap<String, usize>,
    pub target_kind_counts: HashMap<String, usize>,
    pub constraint_kind_counts: HashMap<String, usize>,
    pub diagnostics_by_severity: HashMap<String, usize>,
    pub root_shapes: Vec<ShapeId>,
    pub reachable_shapes: Vec<ShapeId>,
    pub unreachable_shapes: Vec<ShapeId>,
    pub reachable_rules: Vec<u64>,
    pub validation_shape_count: usize,
    pub inference_rule_count: usize,
    pub deactivated_shape_count: usize,
    pub deactivated_rule_count: usize,
    pub import_source_count: usize,
    pub shape_labels: HashMap<ShapeId, String>,
    pub inspection: InspectionGraph,
}

pub fn analyze_program(program: &ShapeProgram) -> AnalysisSummary {
    let mut adjacency: HashMap<ShapeId, Vec<ShapeId>> = HashMap::new();
    let mut reverse: HashMap<ShapeId, Vec<ShapeId>> = HashMap::new();
    for shape in &program.shapes {
        adjacency.entry(shape.id).or_default();
        reverse.entry(shape.id).or_default();
    }
    for dep in &program.dependencies {
        adjacency.entry(dep.from).or_default().push(dep.to);
        reverse.entry(dep.to).or_default().push(dep.from);
    }

    let mut visited = HashSet::new();
    let mut order = Vec::new();
    for shape in &program.shapes {
        dfs(shape.id, &adjacency, &mut visited, &mut order);
    }

    visited.clear();
    let mut components = Vec::new();
    while let Some(node) = order.pop() {
        if visited.contains(&node) {
            continue;
        }
        let mut component = Vec::new();
        reverse_dfs(node, &reverse, &mut visited, &mut component);
        component.sort();
        let recursive = component.len() > 1
            || component.iter().any(|shape| {
                adjacency
                    .get(shape)
                    .into_iter()
                    .flatten()
                    .any(|target| target == shape)
            });
        components.push(DependencyComponent {
            shapes: component,
            recursive,
        });
    }
    components.sort_by_key(|component| component.shapes.first().copied().unwrap_or(ShapeId(0)));

    let mut feature_counts = HashMap::new();
    for feature in &program.features {
        *feature_counts
            .entry(feature_name(feature).to_string())
            .or_insert(0) += 1;
    }

    let shape_labels: HashMap<ShapeId, String> = program
        .shapes
        .iter()
        .map(|shape| (shape.id, shape.normalized_key.clone()))
        .collect();
    let root_shapes: Vec<_> = program
        .shapes
        .iter()
        .filter(|shape| !shape.deactivated && !shape.targets.is_empty())
        .map(|shape| shape.id)
        .collect();
    let reachable_shapes = reachable_from_roots(&root_shapes, &adjacency);
    let reachable_shape_set: HashSet<_> = reachable_shapes.iter().copied().collect();
    let unreachable_shapes = program
        .shapes
        .iter()
        .filter(|shape| !reachable_shape_set.contains(&shape.id))
        .map(|shape| shape.id)
        .collect();
    let reachable_rules = program
        .rules
        .iter()
        .filter(|rule| reachable_shape_set.contains(&rule.owner))
        .map(|rule| rule.id.0)
        .collect();
    let mut dependency_kind_counts = HashMap::new();
    for edge in &program.dependencies {
        *dependency_kind_counts.entry(edge.kind.clone()).or_insert(0) += 1;
    }
    let mut target_kind_counts = HashMap::new();
    for target in &program.targets {
        *target_kind_counts
            .entry(target_kind_name(&target.expr).to_string())
            .or_insert(0) += 1;
    }
    let mut constraint_kind_counts = HashMap::new();
    for constraint in &program.constraints {
        *constraint_kind_counts
            .entry(constraint_kind_name(&constraint.expr).to_string())
            .or_insert(0) += 1;
    }
    let mut diagnostics_by_severity = HashMap::new();
    for diagnostic in &program.diagnostics {
        *diagnostics_by_severity
            .entry(diagnostic_severity_name(&diagnostic.severity).to_string())
            .or_insert(0) += 1;
    }

    AnalysisSummary {
        dependency_components: components,
        feature_counts,
        dependency_kind_counts,
        target_kind_counts,
        constraint_kind_counts,
        diagnostics_by_severity,
        root_shapes,
        reachable_shapes,
        unreachable_shapes,
        reachable_rules,
        validation_shape_count: program
            .shapes
            .iter()
            .filter(|shape| !shape.constraints.is_empty())
            .count(),
        inference_rule_count: program.rules.len(),
        deactivated_shape_count: program
            .shapes
            .iter()
            .filter(|shape| shape.deactivated)
            .count(),
        deactivated_rule_count: program.rules.iter().filter(|rule| rule.deactivated).count(),
        import_source_count: program
            .source_inventory
            .iter()
            .filter(|source| !source.is_root)
            .count(),
        shape_labels,
        inspection: program.inspection.clone(),
    }
}

fn dfs(
    node: ShapeId,
    adjacency: &HashMap<ShapeId, Vec<ShapeId>>,
    visited: &mut HashSet<ShapeId>,
    order: &mut Vec<ShapeId>,
) {
    if !visited.insert(node) {
        return;
    }
    if let Some(neighbors) = adjacency.get(&node) {
        for neighbor in neighbors {
            dfs(*neighbor, adjacency, visited, order);
        }
    }
    order.push(node);
}

fn reverse_dfs(
    node: ShapeId,
    reverse: &HashMap<ShapeId, Vec<ShapeId>>,
    visited: &mut HashSet<ShapeId>,
    component: &mut Vec<ShapeId>,
) {
    if !visited.insert(node) {
        return;
    }
    component.push(node);
    if let Some(neighbors) = reverse.get(&node) {
        for neighbor in neighbors {
            reverse_dfs(*neighbor, reverse, visited, component);
        }
    }
}

fn feature_name(feature: &FeatureUse) -> &'static str {
    match feature {
        FeatureUse::Core => "core",
        FeatureUse::AdvancedTargets => "advanced_targets",
        FeatureUse::Rules => "rules",
        FeatureUse::Sparql => "sparql",
        FeatureUse::Templates => "templates",
        FeatureUse::CustomComponents => "custom_components",
    }
}

fn reachable_from_roots(
    roots: &[ShapeId],
    adjacency: &HashMap<ShapeId, Vec<ShapeId>>,
) -> Vec<ShapeId> {
    let mut visited = HashSet::new();
    let mut stack = roots.to_vec();
    while let Some(shape) = stack.pop() {
        if !visited.insert(shape) {
            continue;
        }
        if let Some(next) = adjacency.get(&shape) {
            stack.extend(next.iter().copied());
        }
    }
    let mut reachable: Vec<_> = visited.into_iter().collect();
    reachable.sort();
    reachable
}

fn target_kind_name(target: &TargetExpr) -> &'static str {
    match target {
        TargetExpr::Class(_) => "class",
        TargetExpr::Node(_) => "node",
        TargetExpr::SubjectsOf(_) => "subjects_of",
        TargetExpr::ObjectsOf(_) => "objects_of",
        TargetExpr::Advanced(_) => "advanced",
    }
}

fn constraint_kind_name(constraint: &crate::algebra::ConstraintExpr) -> &'static str {
    match constraint {
        crate::algebra::ConstraintExpr::NodeRef { .. } => "node_ref",
        crate::algebra::ConstraintExpr::PropertyRef { .. } => "property_ref",
        crate::algebra::ConstraintExpr::QualifiedValueShape { .. } => "qualified_value_shape",
        crate::algebra::ConstraintExpr::Logical { .. } => "logical",
        crate::algebra::ConstraintExpr::Not { .. } => "not",
        crate::algebra::ConstraintExpr::Class(_) => "class",
        crate::algebra::ConstraintExpr::Datatype(_) => "datatype",
        crate::algebra::ConstraintExpr::NodeKind(_) => "node_kind",
        crate::algebra::ConstraintExpr::Cardinality { .. } => "cardinality",
        crate::algebra::ConstraintExpr::NumericRange { .. } => "numeric_range",
        crate::algebra::ConstraintExpr::StringConstraint { .. } => "string_constraint",
        crate::algebra::ConstraintExpr::PropertyComparison { .. } => "property_comparison",
        crate::algebra::ConstraintExpr::Closed { .. } => "closed",
        crate::algebra::ConstraintExpr::HasValue(_) => "has_value",
        crate::algebra::ConstraintExpr::In(_) => "in",
        crate::algebra::ConstraintExpr::Sparql(_) => "sparql",
        crate::algebra::ConstraintExpr::CustomComponent { .. } => "custom_component",
        crate::algebra::ConstraintExpr::GenericPredicate { .. } => "generic_predicate",
    }
}

fn diagnostic_severity_name(severity: &DiagnosticSeverity) -> &'static str {
    match severity {
        DiagnosticSeverity::Info => "info",
        DiagnosticSeverity::Warning => "warning",
        DiagnosticSeverity::Error => "error",
    }
}
