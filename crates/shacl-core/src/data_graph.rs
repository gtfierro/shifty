use crate::algebra::{ConstraintExpr, ConstraintId, ShapeId, ShapeProgram, TargetExpr, TargetId};
use crate::source::ResolvedShapeSet;
use oxrdf::{NamedNode, NamedOrBlankNode, Term};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectivityClass {
    Empty,
    Narrow,
    Moderate,
    Broad,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FanoutClass {
    Empty,
    Single,
    Bounded,
    Broad,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredicateDataStats {
    pub predicate: String,
    pub triple_count: usize,
    pub distinct_subjects: usize,
    pub distinct_objects: usize,
    pub literal_objects: usize,
    pub iri_objects: usize,
    pub blank_node_objects: usize,
    pub datatype_counts: BTreeMap<String, usize>,
    pub language_tagged_literals: usize,
    pub avg_fanout_per_subject: Option<String>,
    pub fanout_class: FanoutClass,
    pub selectivity_class: SelectivityClass,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassDataStats {
    pub class: String,
    pub direct_instances: usize,
    pub expanded_instances: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetEstimate {
    pub target_id: TargetId,
    pub owner_shape: ShapeId,
    pub kind: String,
    pub estimated_focus_nodes: Option<usize>,
    pub empty: Option<bool>,
    pub sampled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeDataSummary {
    pub shape: ShapeId,
    pub label: String,
    pub estimated_focus_nodes: Option<usize>,
    pub empty_target_scan: Option<bool>,
    pub sampled_estimate: bool,
    pub dead_constraint_candidates: Vec<u64>,
    pub vacuous_constraint_candidates: Vec<u64>,
    pub fanout_hints: BTreeMap<String, FanoutClass>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanningEstimationMode {
    Exact,
    Sampled,
}

impl Default for PlanningEstimationMode {
    fn default() -> Self {
        Self::Exact
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSummaryOptions {
    pub estimation_mode: PlanningEstimationMode,
    pub full_statistics: bool,
    pub subject_sample_budget: usize,
}

impl Default for DataSummaryOptions {
    fn default() -> Self {
        Self {
            estimation_mode: PlanningEstimationMode::Exact,
            full_statistics: true,
            subject_sample_budget: DEFAULT_SUBJECT_SAMPLE_BUDGET,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataSummaryMetadata {
    pub estimation_mode: PlanningEstimationMode,
    pub sampled_subjects: Option<usize>,
    pub sampled_quads: Option<usize>,
    pub exact_target_estimates: usize,
    pub sampled_target_estimates: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataGraphSummary {
    pub total_quads: usize,
    pub distinct_subjects: usize,
    pub distinct_predicates: usize,
    pub distinct_objects: usize,
    pub metadata: DataSummaryMetadata,
    pub predicate_stats: Vec<PredicateDataStats>,
    pub class_stats: Vec<ClassDataStats>,
    pub target_estimates: Vec<TargetEstimate>,
    pub shape_summaries: Vec<ShapeDataSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanningIndex {
    pub shape_positions: BTreeMap<ShapeId, usize>,
    pub constraint_positions: BTreeMap<ConstraintId, usize>,
    pub targets_by_owner: BTreeMap<ShapeId, Vec<TargetId>>,
    pub constraints_by_owner: BTreeMap<ShapeId, Vec<ConstraintId>>,
    pub direct_path_predicate_by_shape: BTreeMap<ShapeId, String>,
}

impl PlanningIndex {
    pub fn build(program: &ShapeProgram) -> Self {
        let shape_positions = program
            .shapes
            .iter()
            .enumerate()
            .map(|(index, shape)| (shape.id, index))
            .collect::<BTreeMap<_, _>>();
        let constraint_positions = program
            .constraints
            .iter()
            .enumerate()
            .map(|(index, constraint)| (constraint.id, index))
            .collect::<BTreeMap<_, _>>();
        let mut targets_by_owner = BTreeMap::new();
        let mut constraints_by_owner = BTreeMap::new();
        let mut direct_path_predicate_by_shape = BTreeMap::new();
        for shape in &program.shapes {
            if !shape.targets.is_empty() {
                targets_by_owner.insert(shape.id, shape.targets.clone());
            }
            if !shape.constraints.is_empty() {
                constraints_by_owner.insert(shape.id, shape.constraints.clone());
            }
            if let Some(path) = &shape.path
                && let Some(predicate) = direct_path_predicate(path)
            {
                direct_path_predicate_by_shape
                    .insert(shape.id, predicate.as_str().to_string());
            }
        }
        Self {
            shape_positions,
            constraint_positions,
            targets_by_owner,
            constraints_by_owner,
            direct_path_predicate_by_shape,
        }
    }
}

pub const DEFAULT_SUBJECT_SAMPLE_BUDGET: usize = 10_000;

#[derive(Debug, Default)]
struct PredicateAccumulator {
    triple_count: usize,
    subjects: BTreeSet<String>,
    objects: BTreeSet<String>,
    literal_objects: usize,
    iri_objects: usize,
    blank_node_objects: usize,
    datatype_counts: BTreeMap<String, usize>,
    language_tagged_literals: usize,
}

#[derive(Debug)]
pub(crate) struct DataIndex {
    subjects: BTreeSet<String>,
    predicates: BTreeSet<String>,
    objects: BTreeSet<String>,
    predicate_stats: BTreeMap<String, PredicateAccumulator>,
    types: HashMap<String, BTreeSet<String>>,
    superclasses: HashMap<String, BTreeSet<String>>,
    subjects_of: HashMap<String, BTreeSet<String>>,
    objects_of: HashMap<String, BTreeSet<String>>,
    sampled_subjects: Option<usize>,
    sampled_quads: Option<usize>,
}

impl DataIndex {
    fn build(data: &ResolvedShapeSet, options: &DataSummaryOptions) -> Self {
        let sampled_subjects = sampled_subject_keys(data, options);
        let mut subjects = BTreeSet::new();
        let mut predicates = BTreeSet::new();
        let mut objects = BTreeSet::new();
        let mut predicate_stats: BTreeMap<String, PredicateAccumulator> = BTreeMap::new();
        let mut types: HashMap<String, BTreeSet<String>> = HashMap::new();
        let mut superclasses: HashMap<String, BTreeSet<String>> = HashMap::new();
        let mut subjects_of: HashMap<String, BTreeSet<String>> = HashMap::new();
        let mut objects_of: HashMap<String, BTreeSet<String>> = HashMap::new();
        let mut sampled_quads = 0usize;

        for quad in &data.quads {
            let subject_term = subject_to_term(&quad.subject);
            let subject_key = subject_term.to_string();
            if sampled_subjects
                .as_ref()
                .is_some_and(|subjects| !subjects.contains(&subject_key))
            {
                continue;
            }
            sampled_quads += 1;
            let predicate_key = quad.predicate.as_str().to_string();
            let object_key = quad.object.to_string();
            subjects.insert(subject_key.clone());
            predicates.insert(predicate_key.clone());
            objects.insert(object_key.clone());

            let entry = predicate_stats.entry(predicate_key.clone()).or_default();
            entry.triple_count += 1;
            entry.subjects.insert(subject_key.clone());
            entry.objects.insert(object_key);
            match &quad.object {
                Term::Literal(literal) => {
                    entry.literal_objects += 1;
                    *entry
                        .datatype_counts
                        .entry(literal.datatype().as_str().to_string())
                        .or_insert(0) += 1;
                    if literal.language().is_some() {
                        entry.language_tagged_literals += 1;
                    }
                }
                Term::NamedNode(_) => entry.iri_objects += 1,
                Term::BlankNode(_) => entry.blank_node_objects += 1,
            }

            subjects_of
                .entry(predicate_key.clone())
                .or_default()
                .insert(subject_key.clone());
            objects_of
                .entry(predicate_key.clone())
                .or_default()
                .insert(quad.object.to_string());

            if quad.predicate.as_str() == RDF_TYPE
                && let Term::NamedNode(class) = &quad.object
            {
                types
                    .entry(subject_term.to_string())
                    .or_default()
                    .insert(class.as_str().to_string());
            }
            if quad.predicate.as_str() == RDFS_SUBCLASS_OF
                && let NamedOrBlankNode::NamedNode(child) = &quad.subject
                && let Term::NamedNode(parent) = &quad.object
            {
                superclasses
                    .entry(child.as_str().to_string())
                    .or_default()
                    .insert(parent.as_str().to_string());
            }
        }

        Self {
            subjects,
            predicates,
            objects,
            predicate_stats,
            types,
            superclasses,
            subjects_of,
            objects_of,
            sampled_subjects: sampled_subjects.as_ref().map(BTreeSet::len),
            sampled_quads: sampled_subjects.as_ref().map(|_| sampled_quads),
        }
    }
}

pub(crate) fn build_data_index(data: &ResolvedShapeSet, options: &DataSummaryOptions) -> DataIndex {
    DataIndex::build(data, options)
}

pub fn summarize_data_graph(program: &ShapeProgram, data: &ResolvedShapeSet) -> DataGraphSummary {
    summarize_data_graph_with_options(
        program,
        &PlanningIndex::build(program),
        data,
        &DataSummaryOptions::default(),
    )
}

pub fn summarize_data_graph_with_options(
    program: &ShapeProgram,
    planning_index: &PlanningIndex,
    data: &ResolvedShapeSet,
    options: &DataSummaryOptions,
) -> DataGraphSummary {
    let index = DataIndex::build(data, options);
    summarize_data_graph_from_index(program, planning_index, data, options, &index)
}

pub(crate) fn summarize_data_graph_from_index(
    program: &ShapeProgram,
    planning_index: &PlanningIndex,
    data: &ResolvedShapeSet,
    options: &DataSummaryOptions,
    index: &DataIndex,
) -> DataGraphSummary {
    let predicate_stats = if options.full_statistics {
        build_predicate_stats(&index)
    } else {
        Vec::new()
    };
    let class_stats = if options.full_statistics {
        build_class_stats(&index)
    } else {
        Vec::new()
    };
    let target_estimates = build_target_estimates(program, &index);
    let shape_summaries = build_shape_data_summaries(program, planning_index, &index, &target_estimates);
    let exact_target_estimates = target_estimates
        .iter()
        .filter(|estimate| !estimate.sampled)
        .count();
    let sampled_target_estimates = target_estimates
        .iter()
        .filter(|estimate| estimate.sampled)
        .count();

    DataGraphSummary {
        total_quads: data.quads.len(),
        distinct_subjects: index.subjects.len(),
        distinct_predicates: index.predicates.len(),
        distinct_objects: index.objects.len(),
        metadata: DataSummaryMetadata {
            estimation_mode: options.estimation_mode,
            sampled_subjects: index.sampled_subjects,
            sampled_quads: index.sampled_quads,
            exact_target_estimates,
            sampled_target_estimates,
        },
        predicate_stats,
        class_stats,
        target_estimates,
        shape_summaries,
    }
}

fn direct_class_count(index: &DataIndex, class: &str) -> usize {
    index
        .types
        .values()
        .filter(|types| types.contains(class))
        .count()
}

fn build_predicate_stats(index: &DataIndex) -> Vec<PredicateDataStats> {
    index
        .predicate_stats
        .iter()
        .map(|(predicate, stats)| {
            let avg = if stats.subjects.is_empty() {
                None
            } else {
                Some(format!(
                    "{:.2}",
                    stats.triple_count as f64 / stats.subjects.len() as f64
                ))
            };
            let fanout_class = classify_fanout(stats.triple_count, stats.subjects.len());
            PredicateDataStats {
                predicate: predicate.clone(),
                triple_count: stats.triple_count,
                distinct_subjects: stats.subjects.len(),
                distinct_objects: stats.objects.len(),
                literal_objects: stats.literal_objects,
                iri_objects: stats.iri_objects,
                blank_node_objects: stats.blank_node_objects,
                datatype_counts: stats.datatype_counts.clone(),
                language_tagged_literals: stats.language_tagged_literals,
                avg_fanout_per_subject: avg,
                fanout_class,
                selectivity_class: classify_selectivity(stats.subjects.len()),
            }
        })
        .collect()
}

fn build_class_stats(index: &DataIndex) -> Vec<ClassDataStats> {
    let mut all_classes = BTreeSet::new();
    for values in index.types.values() {
        all_classes.extend(values.iter().cloned());
    }
    all_classes.extend(index.superclasses.keys().cloned());
    all_classes.extend(
        index.superclasses
            .values()
            .flat_map(|parents| parents.iter().cloned()),
    );
    all_classes
        .into_iter()
        .map(|class| ClassDataStats {
            direct_instances: direct_class_count(index, &class),
            expanded_instances: expanded_class_count(index, &class),
            class,
        })
        .collect()
}

fn build_target_estimates(program: &ShapeProgram, index: &DataIndex) -> Vec<TargetEstimate> {
    program
        .targets
        .iter()
        .map(|target| {
            let estimate = estimate_target_focuses(&target.expr, index);
            TargetEstimate {
                target_id: target.id,
                owner_shape: target.owner,
                kind: target_kind_name(&target.expr).to_string(),
                estimated_focus_nodes: estimate.as_ref().and_then(|value| value.focus_count),
                empty: estimate.as_ref().and_then(|value| value.empty),
                sampled: estimate.as_ref().is_some_and(|value| value.sampled),
            }
        })
        .collect()
}

fn build_shape_data_summaries(
    program: &ShapeProgram,
    planning_index: &PlanningIndex,
    index: &DataIndex,
    target_estimates: &[TargetEstimate],
) -> Vec<ShapeDataSummary> {
    let target_estimate_by_id = target_estimates
        .iter()
        .map(|estimate| (estimate.target_id, estimate))
        .collect::<HashMap<_, _>>();

    program
        .shapes
        .iter()
        .map(|shape| {
            let shape_target_ids = planning_index
                .targets_by_owner
                .get(&shape.id)
                .cloned()
                .unwrap_or_default();
            let mut known_counts = Vec::new();
            let mut sampled_estimate = false;
            let mut unknown_target = false;
            let mut can_prove_empty = !shape_target_ids.is_empty();
            for target_id in &shape_target_ids {
                let Some(estimate) = target_estimate_by_id.get(target_id) else {
                    unknown_target = true;
                    can_prove_empty = false;
                    continue;
                };
                if estimate.sampled {
                    sampled_estimate = true;
                }
                match estimate.estimated_focus_nodes {
                    Some(count) => known_counts.push(count),
                    None => unknown_target = true,
                }
                if estimate.empty != Some(true) {
                    can_prove_empty = false;
                }
            }
            let estimated_focus_nodes = if shape_target_ids.is_empty() {
                Some(0)
            } else if unknown_target {
                None
            } else {
                Some(known_counts.into_iter().sum())
            };
            let empty_target_scan = if sampled_estimate {
                None
            } else if shape_target_ids.is_empty() {
                Some(false)
            } else if can_prove_empty {
                Some(true)
            } else {
                estimated_focus_nodes.map(|count| count == 0)
            };

            let dead_constraint_candidates = if empty_target_scan == Some(true) {
                planning_index
                    .constraints_by_owner
                    .get(&shape.id)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|id| id.0)
                    .collect()
            } else {
                Vec::new()
            };

            let mut vacuous_constraint_candidates = Vec::new();
            let mut fanout_hints = BTreeMap::new();
            if let Some(predicate) = planning_index.direct_path_predicate_by_shape.get(&shape.id) {
                let fanout = index
                    .predicate_stats
                    .get(predicate.as_str())
                    .map(|stats| classify_fanout(stats.triple_count, stats.subjects.len()))
                    .unwrap_or(FanoutClass::Empty);
                fanout_hints.insert(predicate.clone(), fanout);
                let predicate_absent = !index.predicate_stats.contains_key(predicate.as_str());
                let safe_vacuous_absence = predicate_absent && index.sampled_subjects.is_none();
                if safe_vacuous_absence {
                    for constraint_id in planning_index
                        .constraints_by_owner
                        .get(&shape.id)
                        .into_iter()
                        .flatten()
                    {
                        if let Some(index) = planning_index.constraint_positions.get(constraint_id)
                            && let Some(constraint) = program.constraints.get(*index)
                            && constraint_is_vacuous_on_empty_values(&constraint.expr)
                        {
                            vacuous_constraint_candidates.push(constraint.id.0);
                        }
                    }
                }
            }

            ShapeDataSummary {
                shape: shape.id,
                label: shape.normalized_key.clone(),
                estimated_focus_nodes,
                empty_target_scan,
                sampled_estimate,
                dead_constraint_candidates,
                vacuous_constraint_candidates,
                fanout_hints,
            }
        })
        .collect()
}

#[derive(Debug, Clone)]
struct TargetEstimateValue {
    focus_count: Option<usize>,
    empty: Option<bool>,
    sampled: bool,
}

fn expanded_class_count(index: &DataIndex, class: &str) -> usize {
    index
        .types
        .values()
        .filter(|types| {
            types.iter()
                .any(|candidate| class_is_or_subclass_of(candidate, class, &index.superclasses))
        })
        .count()
}

fn class_is_or_subclass_of(
    candidate: &str,
    expected: &str,
    superclasses: &HashMap<String, BTreeSet<String>>,
) -> bool {
    if candidate == expected {
        return true;
    }
    let mut active = vec![candidate.to_string()];
    let mut seen = HashSet::new();
    while let Some(next) = active.pop() {
        if !seen.insert(next.clone()) {
            continue;
        }
        if let Some(parents) = superclasses.get(&next) {
            if parents.iter().any(|parent| parent == expected) {
                return true;
            }
            active.extend(parents.iter().cloned());
        }
    }
    false
}

fn estimate_target_focuses(target: &TargetExpr, index: &DataIndex) -> Option<TargetEstimateValue> {
    match target {
        TargetExpr::Node(_term) => Some(TargetEstimateValue {
            focus_count: Some(1),
            empty: Some(false),
            sampled: false,
        }),
        TargetExpr::Class(term) => named_target_class(term).map(|class| {
            let count = index
                .types
                .iter()
                .filter(|(_, classes)| {
                    classes.iter().any(|candidate| {
                        class_is_or_subclass_of(candidate, class.as_str(), &index.superclasses)
                    })
                })
                .count();
            TargetEstimateValue {
                focus_count: Some(count),
                empty: if index.sampled_subjects.is_none() {
                    Some(count == 0)
                } else {
                    None
                },
                sampled: index.sampled_subjects.is_some(),
            }
        }),
        TargetExpr::SubjectsOf(predicate) => named_target_predicate(predicate).map(|predicate| {
            let count = index
                .subjects_of
                .get(predicate.as_str())
                .map(BTreeSet::len)
                .unwrap_or(0);
            TargetEstimateValue {
                focus_count: Some(count),
                empty: if index.sampled_subjects.is_none() {
                    Some(count == 0)
                } else {
                    None
                },
                sampled: index.sampled_subjects.is_some(),
            }
        }),
        TargetExpr::ObjectsOf(predicate) => named_target_predicate(predicate).map(|predicate| {
            let count = index
                .objects_of
                .get(predicate.as_str())
                .map(BTreeSet::len)
                .unwrap_or(0);
            TargetEstimateValue {
                focus_count: Some(count),
                empty: if index.sampled_subjects.is_none() {
                    Some(count == 0)
                } else {
                    None
                },
                sampled: index.sampled_subjects.is_some(),
            }
        }),
        TargetExpr::Advanced(_) => None,
    }
}

fn sampled_subject_keys(
    data: &ResolvedShapeSet,
    options: &DataSummaryOptions,
) -> Option<BTreeSet<String>> {
    if options.estimation_mode == PlanningEstimationMode::Exact {
        return None;
    }
    let mut subjects = data
        .quads
        .iter()
        .map(|quad| subject_to_term(&quad.subject).to_string())
        .collect::<Vec<_>>();
    subjects.sort();
    subjects.dedup();
    if subjects.len() <= options.subject_sample_budget {
        return None;
    }
    let budget = options.subject_sample_budget.max(1);
    let step = ((subjects.len() as f64) / (budget as f64)).ceil() as usize;
    Some(
        subjects
            .into_iter()
            .enumerate()
            .filter(|(index, _)| index % step == 0)
            .map(|(_, subject)| subject)
            .take(budget)
            .collect(),
    )
}

fn target_kind_name(target: &TargetExpr) -> &'static str {
    match target {
        TargetExpr::Node(_) => "node",
        TargetExpr::Class(_) => "class",
        TargetExpr::SubjectsOf(_) => "subjects_of",
        TargetExpr::ObjectsOf(_) => "objects_of",
        TargetExpr::Advanced(_) => "advanced",
    }
}

fn classify_selectivity(count: usize) -> SelectivityClass {
    match count {
        0 => SelectivityClass::Empty,
        1..=3 => SelectivityClass::Narrow,
        4..=25 => SelectivityClass::Moderate,
        _ => SelectivityClass::Broad,
    }
}

fn classify_fanout(triples: usize, subjects: usize) -> FanoutClass {
    if triples == 0 || subjects == 0 {
        return FanoutClass::Empty;
    }
    let ratio = triples as f64 / subjects as f64;
    if ratio <= 1.2 {
        FanoutClass::Single
    } else if ratio <= 4.0 {
        FanoutClass::Bounded
    } else {
        FanoutClass::Broad
    }
}

fn constraint_is_vacuous_on_empty_values(expr: &ConstraintExpr) -> bool {
    match expr {
        ConstraintExpr::Cardinality { min, max, .. } => min.unwrap_or(0) == 0 && max.is_none(),
        ConstraintExpr::HasValue(_) => true,
        ConstraintExpr::In(_)
        | ConstraintExpr::Datatype(_)
        | ConstraintExpr::NodeKind(_)
        | ConstraintExpr::Class(_)
        | ConstraintExpr::NumericRange { .. }
        | ConstraintExpr::StringConstraint { .. }
        | ConstraintExpr::PropertyComparison { .. }
        | ConstraintExpr::NodeRef { .. }
        | ConstraintExpr::PropertyRef { .. }
        | ConstraintExpr::QualifiedValueShape { .. }
        | ConstraintExpr::Logical { .. }
        | ConstraintExpr::Not { .. }
        | ConstraintExpr::Sparql(_)
        | ConstraintExpr::CustomComponent { .. } => true,
        ConstraintExpr::Closed { .. } | ConstraintExpr::GenericPredicate { .. } => false,
    }
}

fn direct_path_predicate(path: &crate::algebra::PropertyPath) -> Option<&NamedNode> {
    match path {
        crate::algebra::PropertyPath::Predicate(predicate) => Some(predicate),
        _ => None,
    }
}

fn named_target_predicate(term: &Term) -> Option<NamedNode> {
    match term {
        Term::NamedNode(node) => Some(node.clone()),
        _ => None,
    }
}

fn named_target_class(term: &Term) -> Option<NamedNode> {
    match term {
        Term::NamedNode(node) => Some(node.clone()),
        _ => None,
    }
}

fn subject_to_term(subject: &NamedOrBlankNode) -> Term {
    match subject {
        NamedOrBlankNode::NamedNode(node) => Term::NamedNode(node.clone()),
        NamedOrBlankNode::BlankNode(node) => Term::BlankNode(node.clone()),
    }
}
