use crate::algebra::{FeatureUse, ShapeId, ShapeProgram};
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
    pub reachable_shapes: Vec<ShapeId>,
    pub validation_shape_count: usize,
    pub inference_rule_count: usize,
    pub deactivated_shape_count: usize,
    pub import_source_count: usize,
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
        *feature_counts.entry(feature_name(feature).to_string()).or_insert(0) += 1;
    }

    let reachable_shapes = program
        .shapes
        .iter()
        .filter(|shape| !shape.deactivated && (!shape.targets.is_empty() || shape.constraints.is_empty()))
        .map(|shape| shape.id)
        .collect();

    AnalysisSummary {
        dependency_components: components,
        feature_counts,
        reachable_shapes,
        validation_shape_count: program
            .shapes
            .iter()
            .filter(|shape| !shape.constraints.is_empty())
            .count(),
        inference_rule_count: program.rules.len(),
        deactivated_shape_count: program.shapes.iter().filter(|shape| shape.deactivated).count(),
        import_source_count: program
            .source_inventory
            .iter()
            .filter(|source| !source.is_root)
            .count(),
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

