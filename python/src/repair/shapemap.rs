//! Shapes-side metadata for the Python shape-map projection.

use oxrdf::dataset::CanonicalizationAlgorithm;
use oxrdf::{Dataset, Graph, GraphNameRef, NamedNode, NamedNodeRef, NamedOrBlankNode, Term};
use pyo3::prelude::*;
use shifty_algebra::{Schema, Shape, ShapeId};
use shifty_parse::{Loaded, vocab};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

pub(super) type LayoutEntries = HashMap<u32, (Option<String>, String, Vec<String>)>;

const CORE_COMPONENTS: &[(NamedNodeRef<'static>, NamedNodeRef<'static>)] = &[
    (vocab::SH_CLASS, vocab::SH_CC_CLASS),
    (vocab::SH_DATATYPE, vocab::SH_CC_DATATYPE),
    (vocab::SH_NODE_KIND, vocab::SH_CC_NODE_KIND),
    (vocab::SH_MIN_COUNT, vocab::SH_CC_MIN_COUNT),
    (vocab::SH_MAX_COUNT, vocab::SH_CC_MAX_COUNT),
    (vocab::SH_MIN_EXCLUSIVE, vocab::SH_CC_MIN_EXCLUSIVE),
    (vocab::SH_MIN_INCLUSIVE, vocab::SH_CC_MIN_INCLUSIVE),
    (vocab::SH_MAX_EXCLUSIVE, vocab::SH_CC_MAX_EXCLUSIVE),
    (vocab::SH_MAX_INCLUSIVE, vocab::SH_CC_MAX_INCLUSIVE),
    (vocab::SH_MIN_LENGTH, vocab::SH_CC_MIN_LENGTH),
    (vocab::SH_MAX_LENGTH, vocab::SH_CC_MAX_LENGTH),
    (vocab::SH_PATTERN, vocab::SH_CC_PATTERN),
    (vocab::SH_LANGUAGE_IN, vocab::SH_CC_LANGUAGE_IN),
    (vocab::SH_UNIQUE_LANG, vocab::SH_CC_UNIQUE_LANG),
    (vocab::SH_EQUALS, vocab::SH_CC_EQUALS),
    (vocab::SH_DISJOINT, vocab::SH_CC_DISJOINT),
    (vocab::SH_LESS_THAN, vocab::SH_CC_LESS_THAN),
    (
        vocab::SH_LESS_THAN_OR_EQUALS,
        vocab::SH_CC_LESS_THAN_OR_EQUALS,
    ),
    (vocab::SH_NOT, vocab::SH_CC_NOT),
    (vocab::SH_AND, vocab::SH_CC_AND),
    (vocab::SH_OR, vocab::SH_CC_OR),
    (vocab::SH_XONE, vocab::SH_CC_XONE),
    (vocab::SH_NODE, vocab::SH_CC_NODE),
    (
        vocab::SH_PROPERTY,
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#PropertyConstraintComponent"),
    ),
    (
        vocab::SH_QUALIFIED_MIN_COUNT,
        vocab::SH_CC_QUALIFIED_MIN_COUNT,
    ),
    (
        vocab::SH_QUALIFIED_MAX_COUNT,
        vocab::SH_CC_QUALIFIED_MAX_COUNT,
    ),
    (vocab::SH_CLOSED, vocab::SH_CC_CLOSED),
    (vocab::SH_HAS_VALUE, vocab::SH_CC_HAS_VALUE),
    (vocab::SH_IN, vocab::SH_CC_IN),
    (vocab::SH_SPARQL, vocab::SH_CC_SPARQL),
    (vocab::SH_EXPRESSION, vocab::SH_CC_EXPRESSION),
];

struct CustomComponent {
    iri: String,
    parameters: Vec<(NamedNode, bool)>,
    node_validator: bool,
    property_validator: bool,
}

fn custom_components(shapes: &Loaded) -> Vec<CustomComponent> {
    let mut seen = HashSet::new();
    let mut components = Vec::new();
    for triple in shapes.graph.triples_for_predicate(vocab::SH_PARAMETER) {
        let subject = triple.subject.into_owned();
        if !seen.insert(subject.clone()) {
            continue;
        }
        let NamedOrBlankNode::NamedNode(iri) = &subject else {
            continue;
        };
        if vocab::NATIVE_CONSTRAINT_COMPONENTS.contains(&iri.as_ref()) {
            continue;
        }
        let generic = shapes.object(&subject, vocab::SH_VALIDATOR).is_some();
        let node_validator = generic || shapes.object(&subject, vocab::SH_NODE_VALIDATOR).is_some();
        let property_validator = generic
            || shapes
                .object(&subject, vocab::SH_PROPERTY_VALIDATOR)
                .is_some();
        let parameters: Vec<_> = shapes
            .objects(&subject, vocab::SH_PARAMETER)
            .iter()
            .filter_map(|parameter| {
                let parameter = shifty_parse::graph::term_to_node(parameter)?;
                let Term::NamedNode(path) = shapes.object(&parameter, vocab::SH_PATH)? else {
                    return None;
                };
                let optional = matches!(shapes.object(&parameter, vocab::SH_OPTIONAL),
                Some(Term::Literal(value)) if value.value() == "true");
                Some((path, optional))
            })
            .collect();
        if !parameters.is_empty() && (node_validator || property_validator) {
            components.push(CustomComponent {
                iri: iri.as_str().to_owned(),
                parameters,
                node_validator,
                property_validator,
            });
        }
    }
    components
}

/// Shared by a session and its runs. Computing anonymous source identities is
/// deferred until a shape map actually needs them, never ordinary validation.
pub(super) struct BindingLayout {
    schema: Arc<Schema>,
    shapes: Arc<Loaded>,
    entries: OnceLock<LayoutEntries>,
    source_owners: OnceLock<HashMap<u32, u32>>,
    constraint_map: HashMap<u32, u32>,
}

pub(super) fn default_graph(graph: &Graph) -> Dataset {
    graph
        .iter()
        .map(|triple| triple.in_graph(GraphNameRef::DefaultGraph))
        .collect()
}

pub(super) type Observation = (Vec<String>, Vec<String>, Option<u64>);

impl super::EvidenceSession {
    /// Observe an authored property's path independently of which successes the
    /// canonical failure witness pruned. Cardinality and per-value qualification
    /// are separate: an invalid value still counts toward an unqualified count.
    pub(super) fn binding_observation_impl(
        &self,
        focus: &str,
        constraint_id: u32,
    ) -> PyResult<Option<Observation>> {
        let focus = super::parse_term(focus).map_err(super::py_value_error)?;
        let Some((current, property)) = shifty_engine::names::unwrap_property_shape(
            &self.raw_schema,
            &self.shapes,
            ShapeId(constraint_id),
        ) else {
            return Ok(None);
        };
        let mut seen = HashSet::new();
        let path_term = self
            .shapes
            .object(&property, vocab::SH_PATH)
            .expect("checked above");
        let path = shifty_parse::path::parse_path(&self.shapes, &path_term)
            .map_err(super::py_value_error)?;
        let raw_id = |term: &Term| {
            self.raw_schema
                .sources
                .iter()
                .find_map(|(id, source)| (source == term).then_some(*id))
        };
        let mut qualified: Vec<ShapeId> = self
            .shapes
            .objects(&property, vocab::SH_QUALIFIED_VALUE_SHAPE)
            .iter()
            .filter_map(raw_id)
            .collect();
        let mut excluded = Vec::new();
        if matches!(self.shapes.object(&property, vocab::SH_QUALIFIED_VALUE_SHAPES_DISJOINT),
            Some(Term::Literal(value)) if value.value() == "true")
        {
            for triple in self.shapes.graph.triples_for_predicate(vocab::SH_PROPERTY) {
                if triple.object != Term::from(property.clone()).as_ref() {
                    continue;
                }
                for sibling in self
                    .shapes
                    .objects(&triple.subject.into_owned(), vocab::SH_PROPERTY)
                {
                    let Some(sibling) = shifty_parse::graph::term_to_node(&sibling) else {
                        continue;
                    };
                    if sibling == property {
                        continue;
                    }
                    for other in self
                        .shapes
                        .objects(&sibling, vocab::SH_QUALIFIED_VALUE_SHAPE)
                    {
                        if let Some(id) = raw_id(&other)
                            && !qualified.contains(&id)
                        {
                            excluded.push(id);
                        }
                    }
                }
            }
        }
        let mut value_checks = Vec::new();
        let mut domains = Vec::new();
        let mut pending = vec![current];
        seen.clear();
        while let Some(id) = pending.pop() {
            if !seen.insert(id) {
                continue;
            }
            match self.raw_schema.arena.get(id) {
                Shape::Annotated { shape, .. } => pending.push(*shape),
                Shape::And(children) => pending.extend(children.iter().copied()),
                Shape::Count {
                    path: count_path,
                    qualifier,
                    min,
                    max,
                } if count_path == &path => {
                    if min.is_none()
                        && *max == Some(0)
                        && let Shape::Not(inner) = self.raw_schema.arena.get(*qualifier)
                    {
                        value_checks.push(*inner);
                    } else {
                        domains.push(*qualifier);
                    }
                }
                _ => {}
            }
        }
        // An optional qualified count may already have lowered to Top. Its
        // authored qualifier still defines extraction and its observation.
        qualified.sort_unstable();
        qualified.dedup();
        domains.sort_unstable();
        domains.dedup();
        // Counts select matches, unlike universal checks which every value
        // must satisfy. Keep the union of nontrivial count domains: distinct
        // sh:hasValue requirements may each match a different path value.
        // An unqualified cardinality must not admit unrelated candidates.
        let count_qualifiers: Vec<_> = domains
            .iter()
            .copied()
            .filter(|id| !matches!(self.raw_schema.arena.get(*id), Shape::Top))
            .collect();
        let values = shifty_engine::path::succ(self.path_graph(), &focus, &path);
        // These authored qualifiers may have been normalized out of validation.
        // Evaluate them fallibly before using their booleans for extraction or
        // disjointness; negation must not turn a query failure into a match.
        let checks: HashSet<_> = domains
            .iter()
            .chain(&qualified)
            .chain(&excluded)
            .chain(&value_checks)
            .copied()
            .filter(|shape| !matches!(self.raw_schema.arena.get(*shape), Shape::Top))
            .collect();
        let mut matches: HashMap<ShapeId, HashSet<Term>> = HashMap::new();
        for shape in checks {
            let matching = matches.entry(shape).or_default();
            for value in &values {
                if self
                    .session
                    .prepared_evidence()
                    .raw_constraint_holds(&self.session.internal_term(value), shape)
                    .map_err(crate::py_execution_error)?
                    == Some(true)
                {
                    matching.insert(value.clone());
                }
            }
        }
        let holds = |value: &Term, shape| {
            matches!(self.raw_schema.arena.get(shape), Shape::Top)
                || matches.get(&shape).is_some_and(|set| set.contains(value))
        };
        let observed = match domains.as_slice() {
            [] if qualified.is_empty() => Some(values.len() as u64),
            [] if qualified.len() == 1 => Some(
                values
                    .iter()
                    .filter(|value| {
                        holds(value, qualified[0])
                            && excluded.iter().all(|shape| !holds(value, *shape))
                    })
                    .count() as u64,
            ),
            [domain] => Some(values.iter().filter(|value| holds(value, *domain)).count() as u64),
            // Different count domains (e.g. minCount plus qualifiedMinCount)
            // cannot be represented by one honest scalar, even if they happen
            // to yield the same number in this graph.
            _ => None,
        };
        let (accepted, rejected): (Vec<_>, Vec<_>) = values.into_iter().partition(|value| {
            value_checks
                .iter()
                .chain(&qualified)
                .all(|shape| holds(value, *shape))
                && excluded.iter().all(|shape| !holds(value, *shape))
                && (count_qualifiers.is_empty()
                    || count_qualifiers.iter().any(|shape| holds(value, *shape)))
        });
        let render = |values: Vec<Term>| {
            let mut values: Vec<String> =
                values.into_iter().map(|value| value.to_string()).collect();
            values.sort();
            values.dedup();
            values
        };
        Ok(Some((render(accepted), render(rejected), observed)))
    }
}

impl BindingLayout {
    pub(super) fn new(
        schema: Arc<Schema>,
        shapes: Arc<Loaded>,
        constraint_map: HashMap<u32, u32>,
    ) -> Self {
        Self {
            schema,
            shapes,
            entries: OnceLock::new(),
            source_owners: OnceLock::new(),
            constraint_map,
        }
    }

    pub(super) fn constraint_map(&self) -> HashMap<u32, u32> {
        self.constraint_map.clone()
    }

    /// The authored property-shape owner for each raw constraint nested in
    /// that property shape. Lowering expands one property shape into several
    /// internal constraints (for example a universal datatype check plus a
    /// `minCount`); this table restores that boundary for shape-map consumers.
    pub(super) fn source_owners(&self) -> &HashMap<u32, u32> {
        self.source_owners.get_or_init(|| {
            let property_sources: Vec<ShapeId> = self
                .schema
                .sources
                .iter()
                .filter_map(|(id, source)| {
                    shifty_parse::graph::term_to_node(source)
                        .filter(|node| self.shapes.object(node, vocab::SH_PATH).is_some())
                        .map(|_| *id)
                })
                .collect();
            let source_ids: HashSet<ShapeId> = self.schema.sources.keys().copied().collect();
            let mut owners = HashMap::new();

            for owner in property_sources {
                let mut pending = vec![owner];
                let mut seen = HashSet::new();
                while let Some(id) = pending.pop() {
                    if !seen.insert(id) {
                        continue;
                    }
                    // A nested authored shape starts a distinct binding. Do not
                    // let its internals become siblings of the enclosing property.
                    if id != owner && source_ids.contains(&id) {
                        continue;
                    }
                    owners.entry(id.0).or_insert(owner.0);
                    match self.schema.arena.get(id) {
                        Shape::Annotated { shape, .. } | Shape::Not(shape) => pending.push(*shape),
                        Shape::And(children) | Shape::Or(children) => {
                            pending.extend(children.iter().copied())
                        }
                        Shape::Count { qualifier, .. } => pending.push(*qualifier),
                        _ => {}
                    }
                }
            }
            owners
        })
    }

    /// The authored property shapes a node shape's shape-map bindings come
    /// from, in binding order (`sh:order`, then canonical identity),
    /// independent of any data. Shape maps bind each member of the node
    /// shape's conjunction, grouped under its property shape; a member not
    /// inside one (such as a single-property `sh:node` or `sh:and`) binds
    /// the property shape it wraps.
    pub(super) fn property_slots(&self, node: ShapeId) -> Vec<u32> {
        let arena = &self.schema.arena;
        let owners = self.source_owners();
        let entries = self.entries();
        let mut children = shifty_engine::evidence::source_children(arena, node);
        // Without a conjunction to report, the node shape is its own member.
        if children.is_empty() {
            children.push(node);
        }
        let mut slots: Vec<(Option<f64>, String, u32)> = Vec::new();
        let mut seen_slots = HashSet::new();
        for child in children {
            let property = owners.get(&child.0).copied().or_else(|| {
                shifty_engine::names::unwrap_property_shape(&self.schema, &self.shapes, child)
                    .map(|(id, _)| id.0)
            });
            if let Some(property) = property
                && seen_slots.insert(property)
            {
                let (order, identity, _) = &entries[&property];
                let order = order
                    .as_deref()
                    .and_then(|order| order.parse::<f64>().ok())
                    .filter(|order| order.is_finite());
                slots.push((order, identity.clone(), property));
            }
        }
        slots.sort_by(|a, b| {
            (a.0.is_none(), a.0.unwrap_or(0.0), &a.1)
                .partial_cmp(&(b.0.is_none(), b.0.unwrap_or(0.0), &b.1))
                .expect("orders are finite")
        });
        slots.into_iter().map(|(_, _, id)| id).collect()
    }

    pub(super) fn entries(&self) -> &LayoutEntries {
        self.entries.get_or_init(|| {
            // Canonicalization supplies identities independent of parser-created
            // blank-node labels and graph interning order. OxRDF's algorithm is
            // deterministic within a library version; these are display-order
            // identities, not persistent public identifiers.
            let dataset = default_graph(&self.shapes.graph);
            let blanks = dataset.canonicalize_blank_nodes(CanonicalizationAlgorithm::Unstable);
            let custom = custom_components(&self.shapes);
            self.schema
                .sources
                .iter()
                .map(|(id, source)| {
                    let identity = match source {
                        Term::BlankNode(node) => blanks[&node.as_ref()].to_string(),
                        _ => source.to_string(),
                    };
                    let node = shifty_parse::graph::term_to_node(source);
                    let order = node
                        .as_ref()
                        .and_then(|node| self.shapes.object(node, vocab::SH_ORDER))
                        .and_then(|value| match value {
                            Term::Literal(value) => Some(value.value().to_owned()),
                            _ => None,
                        });
                    let mut components = Vec::new();
                    if let Some(node) = node {
                        components.extend(CORE_COMPONENTS.iter().filter_map(
                            |(parameter, component)| {
                                self.shapes
                                    .object(&node, *parameter)
                                    .map(|_| component.as_str().to_owned())
                            },
                        ));
                        let property = self.shapes.object(&node, vocab::SH_PATH).is_some();
                        components.extend(
                            custom
                                .iter()
                                .filter(|component| {
                                    (if property {
                                        component.property_validator
                                    } else {
                                        component.node_validator
                                    }) && component.parameters.iter().all(|(path, optional)| {
                                        *optional
                                            || self.shapes.object(&node, path.as_ref()).is_some()
                                    })
                                })
                                .map(|component| component.iri.clone()),
                        );
                    }
                    components.sort();
                    components.dedup();
                    (id.0, (order, identity, components))
                })
                .collect()
        })
    }
}
