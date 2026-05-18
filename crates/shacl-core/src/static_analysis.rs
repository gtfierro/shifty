use crate::algebra::{
    ComponentDefId, ConstraintExpr, ConstraintId, LogicalKind, PropertyPath, RuleExpr, RuleId,
    ShapeId, ShapeProgram, TargetExpr, TargetId, TriplePatternTerm,
};
use crate::analysis::{analyze_program, AnalysisSummary};
use crate::diagnostics::InspectionGraph;
use crate::passes::canonicalize_program;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SliceRoots {
    TargetShapes,
    ExplicitShapes(Vec<ShapeId>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContextFootprint {
    TargetOnly,
    NodeLocal,
    PathLocal,
    RecursiveNeighborhood,
    GlobalSparql,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextFootprintReport {
    pub shape_footprints: HashMap<ShapeId, ContextFootprint>,
    pub rule_footprints: HashMap<RuleId, ContextFootprint>,
    pub histogram: HashMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateGroup<T> {
    pub fingerprint: String,
    pub ids: Vec<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FingerprintReport {
    pub constraint_fingerprints: HashMap<ConstraintId, String>,
    pub target_fingerprints: HashMap<TargetId, String>,
    pub rule_fingerprints: HashMap<RuleId, String>,
    pub duplicate_constraints: Vec<DuplicateGroup<ConstraintId>>,
    pub duplicate_targets: Vec<DuplicateGroup<TargetId>>,
    pub duplicate_rules: Vec<DuplicateGroup<RuleId>>,
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
    pub root_summaries: Vec<RootSliceSummary>,
    pub reduced_program: ShapeProgram,
    pub inspection: InspectionGraph,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticAnalysisSummary {
    pub baseline: AnalysisSummary,
    pub slice: ProgramSlice,
    pub context: ContextFootprintReport,
    pub fingerprints: FingerprintReport,
}

pub fn analyze_static(program: &ShapeProgram) -> StaticAnalysisSummary {
    StaticAnalysisSummary {
        baseline: analyze_program(program),
        slice: slice_program(program, SliceRoots::TargetShapes),
        context: context_requirements(program),
        fingerprints: fingerprint_program(program),
    }
}

pub fn slice_program(program: &ShapeProgram, roots: SliceRoots) -> ProgramSlice {
    let root_ids = resolve_roots(program, roots);
    let adjacency = adjacency_map(program);
    let retained_shapes: BTreeSet<_> = reachable_shapes(&root_ids, &adjacency).into_iter().collect();
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
        .filter_map(|constraint| match &constraint.expr {
            ConstraintExpr::CustomComponent {
                component: Some(component),
                ..
            } if retained_constraints.contains(&constraint.id) => Some(*component),
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
            shape.property_shapes.retain(|id| retained_shapes.contains(id));
            shape.constraints.retain(|id| retained_constraints.contains(id));
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
            let shapes: BTreeSet<_> = reachable_shapes(&[*root], &adjacency).into_iter().collect();
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

    ProgramSlice {
        roots: root_ids.clone(),
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

    let mut shape_footprints = HashMap::new();
    for shape in &program.shapes {
        let mut footprint = ContextFootprint::TargetOnly;
        if shape
            .targets
            .iter()
            .filter_map(|target_id| program.targets.iter().find(|target| target.id == *target_id))
            .any(|target| matches!(target.expr, TargetExpr::Advanced(_)))
        {
            footprint = max_footprint(footprint, ContextFootprint::GlobalSparql);
        }
        if shape.path.is_some() || !shape.property_shapes.is_empty() {
            footprint = max_footprint(footprint, ContextFootprint::PathLocal);
        }
        for constraint in program.constraints.iter().filter(|constraint| constraint.owner == shape.id) {
            footprint = max_footprint(footprint, constraint_footprint(&constraint.expr));
        }
        if program.rules.iter().any(|rule| rule.owner == shape.id) {
            for rule in program.rules.iter().filter(|rule| rule.owner == shape.id) {
                footprint = max_footprint(footprint, rule_footprint(&rule.expr));
            }
        }
        if recursive_shapes.contains(&shape.id) {
            footprint = max_footprint(footprint, ContextFootprint::RecursiveNeighborhood);
        }
        shape_footprints.insert(shape.id, footprint);
    }

    let mut rule_footprints = HashMap::new();
    for rule in &program.rules {
        let mut footprint = rule_footprint(&rule.expr);
        if recursive_shapes.contains(&rule.owner) {
            footprint = max_footprint(footprint, ContextFootprint::RecursiveNeighborhood);
        }
        rule_footprints.insert(rule.id, footprint);
    }

    let mut histogram = HashMap::new();
    for footprint in shape_footprints.values().chain(rule_footprints.values()) {
        *histogram
            .entry(footprint_name(footprint).to_string())
            .or_insert(0) += 1;
    }

    ContextFootprintReport {
        shape_footprints,
        rule_footprints,
        histogram,
    }
}

pub fn fingerprint_program(program: &ShapeProgram) -> FingerprintReport {
    let constraint_fingerprints: HashMap<_, _> = program
        .constraints
        .iter()
        .map(|constraint| (constraint.id, fingerprint_constraint(&constraint.expr)))
        .collect();
    let target_fingerprints: HashMap<_, _> = program
        .targets
        .iter()
        .map(|target| (target.id, fingerprint_target(&target.expr)))
        .collect();
    let rule_fingerprints: HashMap<_, _> = program
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

fn resolve_roots(program: &ShapeProgram, roots: SliceRoots) -> Vec<ShapeId> {
    let mut ids = match roots {
        SliceRoots::TargetShapes => program
            .shapes
            .iter()
            .filter(|shape| !shape.targets.is_empty())
            .map(|shape| shape.id)
            .collect(),
        SliceRoots::ExplicitShapes(ids) => ids,
    };
    ids.sort();
    ids.dedup();
    ids
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
        ContextFootprint::PathLocal => 2,
        ContextFootprint::RecursiveNeighborhood => 3,
        ContextFootprint::GlobalSparql => 4,
    }
}

fn footprint_name(footprint: &ContextFootprint) -> &'static str {
    match footprint {
        ContextFootprint::TargetOnly => "target_only",
        ContextFootprint::NodeLocal => "node_local",
        ContextFootprint::PathLocal => "path_local",
        ContextFootprint::RecursiveNeighborhood => "recursive_neighborhood",
        ContextFootprint::GlobalSparql => "global_sparql",
    }
}

fn constraint_footprint(constraint: &ConstraintExpr) -> ContextFootprint {
    match constraint {
        ConstraintExpr::Sparql(_) => ContextFootprint::GlobalSparql,
        ConstraintExpr::NodeRef { .. }
        | ConstraintExpr::PropertyRef { .. }
        | ConstraintExpr::QualifiedValueShape { .. }
        | ConstraintExpr::Logical { .. }
        | ConstraintExpr::Not { .. }
        | ConstraintExpr::PropertyComparison { .. } => ContextFootprint::PathLocal,
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
        | ConstraintExpr::GenericPredicate { .. } => ContextFootprint::NodeLocal,
    }
}

fn rule_footprint(rule: &RuleExpr) -> ContextFootprint {
    match rule {
        RuleExpr::Sparql { .. } => ContextFootprint::GlobalSparql,
        RuleExpr::Triple {
            subject,
            object,
            conditions,
            ..
        } => {
            if !conditions.is_empty()
                || matches!(subject, Some(TriplePatternTerm::Path(_)))
                || matches!(object, Some(TriplePatternTerm::Path(_)))
            {
                ContextFootprint::PathLocal
            } else {
                ContextFootprint::NodeLocal
            }
        }
        RuleExpr::Generic { conditions, .. } => {
            if conditions.is_empty() {
                ContextFootprint::NodeLocal
            } else {
                ContextFootprint::PathLocal
            }
        }
    }
}

fn duplicate_groups<T>(fingerprints: &HashMap<T, String>) -> Vec<DuplicateGroup<T>>
where
    T: Copy + Eq + Ord + std::hash::Hash + Serialize,
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
        ConstraintExpr::Cardinality { predicate, min, max } => {
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
            predicate.as_ref().map(ToString::to_string).unwrap_or_default(),
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
