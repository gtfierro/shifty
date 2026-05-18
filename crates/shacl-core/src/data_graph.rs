use crate::algebra::{ConstraintExpr, ShapeId, ShapeProgram, TargetExpr, TargetId};
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeDataSummary {
    pub shape: ShapeId,
    pub label: String,
    pub estimated_focus_nodes: Option<usize>,
    pub empty_target_scan: Option<bool>,
    pub dead_constraint_candidates: Vec<u64>,
    pub vacuous_constraint_candidates: Vec<u64>,
    pub fanout_hints: BTreeMap<String, FanoutClass>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataGraphSummary {
    pub total_quads: usize,
    pub distinct_subjects: usize,
    pub distinct_predicates: usize,
    pub distinct_objects: usize,
    pub predicate_stats: Vec<PredicateDataStats>,
    pub class_stats: Vec<ClassDataStats>,
    pub target_estimates: Vec<TargetEstimate>,
    pub shape_summaries: Vec<ShapeDataSummary>,
}

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
struct DataIndex {
    subjects: BTreeSet<String>,
    predicates: BTreeSet<String>,
    objects: BTreeSet<String>,
    predicate_stats: BTreeMap<String, PredicateAccumulator>,
    types: HashMap<String, BTreeSet<String>>,
    superclasses: HashMap<String, BTreeSet<String>>,
    subjects_of: HashMap<String, BTreeSet<String>>,
    objects_of: HashMap<String, BTreeSet<String>>,
}

impl DataIndex {
    fn build(data: &ResolvedShapeSet) -> Self {
        let mut subjects = BTreeSet::new();
        let mut predicates = BTreeSet::new();
        let mut objects = BTreeSet::new();
        let mut predicate_stats: BTreeMap<String, PredicateAccumulator> = BTreeMap::new();
        let mut types: HashMap<String, BTreeSet<String>> = HashMap::new();
        let mut superclasses: HashMap<String, BTreeSet<String>> = HashMap::new();
        let mut subjects_of: HashMap<String, BTreeSet<String>> = HashMap::new();
        let mut objects_of: HashMap<String, BTreeSet<String>> = HashMap::new();

        for quad in &data.quads {
            let subject_term = subject_to_term(&quad.subject);
            let subject_key = subject_term.to_string();
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
        }
    }
}

pub fn summarize_data_graph(program: &ShapeProgram, data: &ResolvedShapeSet) -> DataGraphSummary {
    let index = DataIndex::build(data);
    let predicate_stats = index
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
                selectivity_class: classify_selectivity(stats.subjects.len()),
                fanout_class,
            }
        })
        .collect::<Vec<_>>();

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
    let class_stats = all_classes
        .into_iter()
        .map(|class| ClassDataStats {
            direct_instances: direct_class_count(&index, &class),
            expanded_instances: expanded_class_count(&index, &class),
            class,
        })
        .collect::<Vec<_>>();

    let target_estimates = program
        .targets
        .iter()
        .map(|target| {
            let focuses = estimate_target_focuses(&target.expr, &index);
            TargetEstimate {
                target_id: target.id,
                owner_shape: target.owner,
                kind: target_kind_name(&target.expr).to_string(),
                estimated_focus_nodes: focuses.as_ref().map(BTreeSet::len),
                empty: focuses.as_ref().map(BTreeSet::is_empty),
            }
        })
        .collect::<Vec<_>>();

    let shape_summaries = program
        .shapes
        .iter()
        .map(|shape| {
            let shape_targets = program
                .targets
                .iter()
                .filter(|target| target.owner == shape.id)
                .collect::<Vec<_>>();
            let mut known_sets = Vec::new();
            let mut unknown_target = false;
            for target in &shape_targets {
                if let Some(focuses) = estimate_target_focuses(&target.expr, &index) {
                    known_sets.push(focuses);
                } else {
                    unknown_target = true;
                }
            }
            let estimated_focus_nodes = if unknown_target {
                None
            } else {
                let mut merged = BTreeSet::new();
                for set in known_sets {
                    merged.extend(set);
                }
                Some(merged.len())
            };
            let empty_target_scan = estimated_focus_nodes.map(|count| count == 0);

            let dead_constraint_candidates = if empty_target_scan == Some(true) {
                shape.constraints.iter().map(|id| id.0).collect()
            } else {
                Vec::new()
            };

            let mut vacuous_constraint_candidates = Vec::new();
            let mut fanout_hints = BTreeMap::new();
            if let Some(path) = &shape.path
                && let Some(predicate) = direct_path_predicate(path)
            {
                let fanout = index
                    .predicate_stats
                    .get(predicate.as_str())
                    .map(|stats| classify_fanout(stats.triple_count, stats.subjects.len()))
                    .unwrap_or(FanoutClass::Empty);
                fanout_hints.insert(predicate.as_str().to_string(), fanout);
                let predicate_absent = !index.predicate_stats.contains_key(predicate.as_str());
                if predicate_absent {
                    for constraint_id in &shape.constraints {
                        if let Some(constraint) = program
                            .constraints
                            .iter()
                            .find(|candidate| candidate.id == *constraint_id)
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
                dead_constraint_candidates,
                vacuous_constraint_candidates,
                fanout_hints,
            }
        })
        .collect::<Vec<_>>();

    DataGraphSummary {
        total_quads: data.quads.len(),
        distinct_subjects: index.subjects.len(),
        distinct_predicates: index.predicates.len(),
        distinct_objects: index.objects.len(),
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

fn estimate_target_focuses(target: &TargetExpr, index: &DataIndex) -> Option<BTreeSet<String>> {
    match target {
        TargetExpr::Node(term) => Some(BTreeSet::from([term.to_string()])),
        TargetExpr::Class(term) => Some(
            named_target_class(term).map_or_else(BTreeSet::new, |class| {
                index
                    .types
                    .iter()
                    .filter(|(_, classes)| {
                        classes.iter().any(|candidate| {
                            class_is_or_subclass_of(candidate, class.as_str(), &index.superclasses)
                        })
                    })
                    .map(|(subject, _)| subject.clone())
                    .collect()
            }),
        ),
        TargetExpr::SubjectsOf(predicate) => named_target_predicate(predicate).map(|predicate| {
            index
                .subjects_of
                .get(predicate.as_str())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|term| term.to_string())
                .collect()
        }),
        TargetExpr::ObjectsOf(predicate) => named_target_predicate(predicate).map(|predicate| {
            index
                .objects_of
                .get(predicate.as_str())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|term| term.to_string())
                .collect()
        }),
        TargetExpr::Advanced(_) => None,
    }
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
