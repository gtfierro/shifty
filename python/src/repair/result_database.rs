//! Authored shape catalogs for the experimental SQLite projection.

use crate::{InputSpec, py_value_error};
use oxigraph::{
    sparql::{QueryResults, SparqlEvaluator},
    store::Store,
};
use oxrdf::{GraphNameRef, NamedNodeRef, NamedOrBlankNode, Term};
use pyo3::{prelude::*, pybacked::PyBackedBytes};
use shifty_parse::{Loaded, graph::term_to_node, vocab};
use std::collections::{BTreeMap, BTreeSet, HashMap};

const TABLE_NAME: NamedNodeRef<'static> =
    NamedNodeRef::new_unchecked("https://shifty.gtf.fyi/ns/sql#tableName");
const COLUMN_NAME: NamedNodeRef<'static> =
    NamedNodeRef::new_unchecked("https://shifty.gtf.fyi/ns/sql#columnName");

type FieldLayout = (u32, String, Option<String>, Option<String>, bool);
pub(super) type TableLayout = (String, Option<String>, Vec<FieldLayout>);

#[pyfunction]
#[pyo3(signature = (data, path, format, base))]
pub(super) fn _result_database_snapshot(
    data: Option<PyBackedBytes>,
    path: Option<String>,
    format: &str,
    base: Option<&str>,
) -> PyResult<String> {
    let graph = InputSpec::new(data, path, format, "graph")
        .and_then(|input| input.load(base))
        .map_err(py_value_error)?;
    // Resolved RDF terms alone would lose the base and prefixes used by
    // authored SPARQL constraints when the database is reopened.
    let mut serializer = oxttl::TurtleSerializer::new();
    if let Some(base) = graph.base {
        serializer = serializer.with_base_iri(base).map_err(value_error)?;
    }
    for (prefix, iri) in graph.prefixes {
        serializer = serializer.with_prefix(prefix, iri).map_err(value_error)?;
    }
    let mut writer = serializer.for_writer(Vec::new());
    for triple in &graph.graph {
        writer.serialize_triple(triple).map_err(value_error)?;
    }
    String::from_utf8(writer.finish().map_err(value_error)?).map_err(value_error)
}

impl super::EvidenceSession {
    pub(super) fn result_database_layout(
        &self,
        shape_names: Option<Vec<String>>,
        shape_query: Option<&str>,
    ) -> PyResult<Vec<TableLayout>> {
        let mut nodes = BTreeMap::new();
        let mut source_ids = HashMap::new();
        for (id, source) in &self.raw_schema.sources {
            let Some(node) = term_to_node(source) else {
                continue;
            };
            source_ids.insert(source, id.0);
            if let NamedOrBlankNode::NamedNode(iri) = &node
                && self.shapes.object(&node, vocab::SH_PATH).is_none()
            {
                nodes.insert(iri.as_str().to_owned(), node);
            }
        }
        let selected: BTreeSet<_> = match (shape_names, shape_query) {
            (None, None) => nodes.keys().cloned().collect(),
            (names, query) => {
                let mut selected: BTreeSet<_> = names.unwrap_or_default().into_iter().collect();
                if let Some(query) = query {
                    selected.extend(select_shapes(&self.shapes, query)?);
                }
                selected
            }
        };

        let identities = self.binding_layout.entries();
        selected
            .into_iter()
            .map(|iri| {
                let node = nodes.get(&iri).ok_or_else(|| {
                    py_value_error(format!("expected a named node shape in this graph: {iri}"))
                })?;
                let alias = sql_alias(&self.shapes, node, TABLE_NAME)?;
                let mut fields = Vec::new();
                for source in self.shapes.objects(node, vocab::SH_PROPERTY) {
                    let property = term_to_node(&source).ok_or_else(|| {
                        py_value_error(format!("invalid property shape for {iri}: {source}"))
                    })?;
                    let Some(path) = self.shapes.object(&property, vocab::SH_PATH) else {
                        return Err(py_value_error(format!(
                            "property shape {source} on {iri} has no sh:path"
                        )));
                    };
                    let id = source_ids.get(&source).copied().ok_or_else(|| {
                        py_value_error(format!("no authored property {source} on {iri}"))
                    })?;
                    let identity = identities[&id].1.clone();
                    let alias = sql_alias(&self.shapes, &property, COLUMN_NAME)?;
                    let label = field_label(&self.shapes, &property, &path);
                    let single = is_scalar(&self.shapes, &property);
                    fields.push((id, identity, alias, label, single));
                }
                fields.sort_by(|a, b| a.1.cmp(&b.1));
                Ok((iri, alias, fields))
            })
            .collect()
    }
}

fn sql_alias(
    shapes: &Loaded,
    node: &NamedOrBlankNode,
    predicate: NamedNodeRef<'static>,
) -> PyResult<Option<String>> {
    match shapes.objects(node, predicate).as_slice() {
        [] => Ok(None),
        [Term::Literal(value)] if !value.value().trim().is_empty() => {
            Ok(Some(value.value().to_owned()))
        }
        _ => Err(py_value_error(format!(
            "expected one nonempty SQL name for {node} at {predicate}"
        ))),
    }
}

fn field_label(shapes: &Loaded, property: &NamedOrBlankNode, path: &Term) -> Option<String> {
    shapes
        .objects(property, vocab::SH_NAME)
        .into_iter()
        .filter_map(|name| match name {
            Term::Literal(name) => Some(name.value().to_owned()),
            _ => None,
        })
        .min()
        .or_else(|| match (property, path) {
            (NamedOrBlankNode::NamedNode(iri), _) | (_, Term::NamedNode(iri)) => iri
                .as_str()
                .rsplit(['#', '/', ':'])
                .find(|part| !part.is_empty())
                .map(str::to_owned),
            _ => None,
        })
}

fn is_scalar(shapes: &Loaded, property: &NamedOrBlankNode) -> bool {
    let maximum = if shapes
        .object(property, vocab::SH_QUALIFIED_VALUE_SHAPE)
        .is_some()
    {
        vocab::SH_QUALIFIED_MAX_COUNT
    } else {
        vocab::SH_MAX_COUNT
    };
    matches!(shapes.object(property, maximum), Some(Term::Literal(value))
        if value.value().parse::<u64>().is_ok_and(|limit| limit <= 1))
}

fn select_shapes(shapes: &Loaded, query: &str) -> PyResult<Vec<String>> {
    let store = Store::new().map_err(value_error)?;
    store
        .extend(
            shapes
                .graph
                .iter()
                .map(|triple| triple.in_graph(GraphNameRef::DefaultGraph).into_owned()),
        )
        .map_err(value_error)?;
    let mut evaluator = SparqlEvaluator::new();
    if let Some(base) = &shapes.base {
        evaluator = evaluator.with_base_iri(base).map_err(value_error)?;
    }
    for (prefix, iri) in &shapes.prefixes {
        evaluator = evaluator.with_prefix(prefix, iri).map_err(value_error)?;
    }
    let prepared = evaluator.parse_query(query).map_err(value_error)?;
    let QueryResults::Solutions(solutions) =
        prepared.on_store(&store).execute().map_err(value_error)?
    else {
        return Err(py_value_error("shape_query must be a SPARQL SELECT".into()));
    };
    if !solutions
        .variables()
        .iter()
        .any(|var| var.as_str() == "shape")
    {
        return Err(py_value_error("shape_query must project ?shape".into()));
    }
    solutions
        .map(|row| match row.map_err(value_error)?.get("shape") {
            Some(Term::NamedNode(iri)) => Ok(iri.as_str().to_owned()),
            _ => Err(py_value_error(
                "shape_query must bind ?shape to an IRI".into(),
            )),
        })
        .collect()
}

fn value_error(error: impl std::fmt::Display) -> PyErr {
    py_value_error(error.to_string())
}
