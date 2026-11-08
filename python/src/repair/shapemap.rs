//! Shapes-side metadata for the Python shape-map projection.

use oxrdf::dataset::CanonicalizationAlgorithm;
use oxrdf::{Dataset, GraphNameRef, Term};
use pyo3::prelude::*;
use shifty_algebra::{Schema, Shape, ShapeId};
use shifty_parse::{Loaded, vocab};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

type LayoutEntries = HashMap<u32, (Option<String>, String)>;

/// Shared by a session and its runs. Computing anonymous source identities is
/// deferred until a shape map actually needs them, never ordinary validation.
pub(super) struct BindingLayout {
    schema: Arc<Schema>,
    shapes: Arc<Loaded>,
    entries: OnceLock<LayoutEntries>,
}

type Observation = (Vec<String>, Vec<String>, Option<u64>);

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
        let mut current = ShapeId(constraint_id);
        let mut seen = HashSet::new();
        let property = loop {
            if !seen.insert(current) {
                return Ok(None);
            }
            if let Some(source) = self.raw_schema.sources.get(&current)
                && let Some(node) = shifty_parse::graph::term_to_node(source)
                && self.shapes.object(&node, vocab::SH_PATH).is_some()
            {
                break node;
            }
            match self.raw_schema.arena.get(current) {
                Shape::Annotated { shape, .. } => current = *shape,
                _ => return Ok(None),
            }
        };
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
        if domains.is_empty() && !qualified.is_empty() {
            domains.extend(qualified.iter().copied());
        }
        domains.sort_unstable();
        domains.dedup();
        let holds = |value: &Term, shape| {
            self.session
                .prepared_evidence()
                .raw_constraint_holds(&self.session.internal_term(value), shape)
                == Some(true)
        };
        let values = shifty_engine::path::succ(self.path_graph(), &focus, &path);
        let observed = match domains.as_slice() {
            [] => Some(values.len() as u64),
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
    pub(super) fn new(schema: Arc<Schema>, shapes: Arc<Loaded>) -> Self {
        Self {
            schema,
            shapes,
            entries: OnceLock::new(),
        }
    }

    pub(super) fn entries(&self) -> &LayoutEntries {
        self.entries.get_or_init(|| {
            // Canonicalization supplies identities independent of parser-created
            // blank-node labels and graph interning order. OxRDF's algorithm is
            // deterministic within a library version; these are display-order
            // identities, not persistent public identifiers.
            let dataset: Dataset = self
                .shapes
                .graph
                .iter()
                .map(|triple| triple.in_graph(GraphNameRef::DefaultGraph))
                .collect();
            let blanks = dataset.canonicalize_blank_nodes(CanonicalizationAlgorithm::Unstable);
            self.schema
                .sources
                .iter()
                .map(|(id, source)| {
                    let identity = match source {
                        Term::BlankNode(node) => blanks[&node.as_ref()].to_string(),
                        _ => source.to_string(),
                    };
                    let order = shifty_parse::graph::term_to_node(source)
                        .and_then(|node| self.shapes.object(&node, vocab::SH_ORDER))
                        .and_then(|value| match value {
                            Term::Literal(value) => Some(value.value().to_owned()),
                            _ => None,
                        });
                    (id.0, (order, identity))
                })
                .collect()
        })
    }
}
