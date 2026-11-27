//! Authored shape catalogs and SPARQL results for the experimental SQLite projection.

use super::shapemap::default_graph;
use crate::{InputSpec, py_value_error};
use oxigraph::sparql::{QueryResults, SparqlEvaluator};
use oxrdf::dataset::{CanonicalizationAlgorithm, CanonicalizationHashAlgorithm};
use oxrdf::{
    BlankNode, Dataset, Graph, NamedNodeRef, NamedOrBlankNode, NamedOrBlankNodeRef, Term, TermRef,
};
use pyo3::{prelude::*, pybacked::PyBackedBytes};
use shifty_algebra::{Path, ShapeId};
use shifty_parse::{Loaded, graph::term_to_node, vocab};
use spargebra::{Query, SparqlParser, algebra::GraphPattern};
use std::cell::OnceCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

const TABLE_NAME: NamedNodeRef<'static> =
    NamedNodeRef::new_unchecked("https://shifty.gtf.fyi/ns/sql#tableName");
const COLUMN_NAME: NamedNodeRef<'static> =
    NamedNodeRef::new_unchecked("https://shifty.gtf.fyi/ns/sql#columnName");

type FieldLayout = (u32, Option<String>, Option<String>);
pub(super) type TableLayout = (String, String, Vec<FieldLayout>);

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
    snapshot(graph)
}

fn snapshot(graph: Loaded) -> PyResult<String> {
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

/// The canonical form of a stored shapes snapshot, for recognizing a repeated
/// registration. Two snapshots of the same graph differ in blank-node labels
/// and triple order, so the triples are canonicalized with RDFC-1.0 and sorted.
/// The base IRI and prefix declarations are kept because authored SPARQL
/// constraints and table names depend on them.
#[pyfunction]
pub(super) fn _result_database_canonical_shapes(document: &str) -> PyResult<String> {
    let Loaded {
        mut graph,
        mut prefixes,
        base,
    } = Loaded::from_turtle(document.as_bytes(), None).map_err(value_error)?;
    graph.canonicalize(CanonicalizationAlgorithm::Rdfc10 {
        hash_algorithm: CanonicalizationHashAlgorithm::Sha256,
    });
    prefixes.sort();
    let mut triples: Vec<String> = graph.iter().map(|triple| format!("{triple} .")).collect();
    triples.sort();
    let mut lines = vec![format!("base {}", base.unwrap_or_default())];
    lines.extend(
        prefixes
            .iter()
            .map(|(prefix, iri)| format!("prefix {prefix}: <{iri}>")),
    );
    lines.extend(triples);
    Ok(lines.join("\n"))
}

/// One refresh's data and shapes graphs, each parsed once and shared by every
/// validation session and the SPARQL query dataset.
#[pyclass(unsendable, name = "_ResultDatabaseRefresh")]
pub(super) struct ResultDatabaseRefresh {
    data: Graph,
    shapes: Vec<Loaded>,
}

#[pymethods]
impl ResultDatabaseRefresh {
    #[new]
    #[pyo3(signature = (data, path, format, base, shapes))]
    fn new(
        data: Option<PyBackedBytes>,
        path: Option<String>,
        format: &str,
        base: Option<&str>,
        shapes: Vec<String>,
    ) -> PyResult<Self> {
        let data = InputSpec::new(data, path, format, "graph")
            .and_then(|input| input.load(base))
            .map_err(py_value_error)?
            .graph;
        // Keep the data labels and give colliding shapes blank nodes fresh
        // labels, so the sessions and the query dataset agree on every label.
        let mut used: HashSet<_> = blank_nodes(&data).collect();
        let mut next = 0;
        let shapes = shapes
            .iter()
            .map(|document| {
                let mut loaded =
                    shifty_parse::load_turtle(document.as_bytes(), None).map_err(value_error)?;
                relabel_collisions(&mut loaded.graph, &mut used, &mut next);
                Ok(loaded)
            })
            .collect::<PyResult<_>>()?;
        Ok(Self { data, shapes })
    }

    /// A validation session over the data and one registered shapes graph.
    fn session(
        &self,
        index: usize,
        infer: bool,
        graph_mode: &str,
    ) -> PyResult<super::EvidenceSession> {
        let shapes = self
            .shapes
            .get(index)
            .ok_or_else(|| py_value_error(format!("no shapes graph at index {index}")))?;
        super::EvidenceSession::from_loaded(
            Loaded {
                graph: shapes.graph.clone(),
                prefixes: shapes.prefixes.clone(),
                base: shapes.base.clone(),
            },
            Some(self.data.clone()),
            infer,
            crate::parse_mode(graph_mode).map_err(py_value_error)?,
            crate::engine_options("error").map_err(py_value_error)?,
        )
    }

    /// The dataset for query tables. Shapes are left out when no query reads them.
    fn query_dataset(&self, include_shapes: bool) -> ResultDatabaseQueryDataset {
        let mut shapes = Dataset::new();
        if include_shapes {
            for loaded in &self.shapes {
                shapes.extend(default_graph(&loaded.graph).iter());
            }
        }
        ResultDatabaseQueryDataset {
            data: default_graph(&self.data),
            shapes,
            union: OnceCell::new(),
        }
    }
}

/// Give this graph's blank nodes that are already `used` fresh labels.
fn relabel_collisions(graph: &mut Graph, used: &mut HashSet<BlankNode>, next: &mut usize) {
    let mut blanks: Vec<_> = blank_nodes(graph)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    blanks.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
    let mut replacements = HashMap::new();
    for blank in blanks {
        if used.insert(blank.clone()) {
            continue;
        }
        loop {
            *next += 1;
            let replacement = BlankNode::new_unchecked(format!("shifty_result_{next}"));
            if used.insert(replacement.clone()) {
                replacements.insert(blank, replacement);
                break;
            }
        }
    }
    if replacements.is_empty() {
        return;
    }
    // Rewrite both ends of each triple with one map for this graph.
    *graph = graph
        .iter()
        .map(|triple| {
            let mut triple = triple.into_owned();
            if let NamedOrBlankNode::BlankNode(node) = &mut triple.subject
                && let Some(replacement) = replacements.get(node)
            {
                *node = replacement.clone();
            }
            if let Term::BlankNode(node) = &mut triple.object
                && let Some(replacement) = replacements.get(node)
            {
                *node = replacement.clone();
            }
            triple
        })
        .collect();
}

fn blank_nodes(graph: &Graph) -> impl Iterator<Item = BlankNode> + '_ {
    graph.iter().flat_map(|triple| {
        let subject = match triple.subject {
            NamedOrBlankNodeRef::BlankNode(node) => Some(node.into_owned()),
            _ => None,
        };
        let object = match triple.object {
            TermRef::BlankNode(node) => Some(node.into_owned()),
            _ => None,
        };
        [subject, object].into_iter().flatten()
    })
}

#[pyfunction]
pub(super) fn _result_database_query_columns(query: &str) -> PyResult<Vec<String>> {
    let query = SparqlParser::new()
        .parse_query(query)
        .map_err(value_error)?;
    query_columns(&query)
}

fn query_columns(query: &Query) -> PyResult<Vec<String>> {
    let Query::Select { pattern, .. } = query else {
        return Err(py_value_error(
            "query table requires a SPARQL SELECT".into(),
        ));
    };
    let mut pattern = pattern;
    // The parser wraps projection in row modifiers such as DISTINCT and LIMIT.
    loop {
        match pattern {
            GraphPattern::Project { variables, .. } if !variables.is_empty() => {
                return Ok(variables
                    .iter()
                    .map(|var| var.as_str().to_owned())
                    .collect());
            }
            GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner }
            | GraphPattern::Slice { inner, .. } => pattern = inner,
            _ => {
                return Err(py_value_error(
                    "query table requires at least one projected variable".into(),
                ));
            }
        }
    }
}

type QueryRows = Vec<Vec<Option<String>>>;

#[pyclass(unsendable, name = "_ResultDatabaseQueryDataset")]
pub(super) struct ResultDatabaseQueryDataset {
    data: Dataset,
    shapes: Dataset,
    union: OnceCell<Dataset>,
}

#[pymethods]
impl ResultDatabaseQueryDataset {
    fn select(&self, query: &str, graph_mode: &str) -> PyResult<(Vec<String>, QueryRows)> {
        let query = SparqlParser::new()
            .parse_query(query)
            .map_err(value_error)?;
        let columns = query_columns(&query)?;
        let dataset = match graph_mode {
            "data" => &self.data,
            "shapes" => &self.shapes,
            // Build the combined dataset once, only when a query needs it.
            "union" => self.union.get_or_init(|| {
                let mut merged = self.data.clone();
                merged.extend(&self.shapes);
                merged
            }),
            _ => {
                return Err(py_value_error(format!(
                    "unknown query graph mode {graph_mode:?}"
                )));
            }
        };
        let prepared = SparqlEvaluator::new().for_query(query);
        let QueryResults::Solutions(solutions) = prepared
            .on_queryable_dataset(dataset)
            .execute()
            .map_err(value_error)?
        else {
            return Err(py_value_error(
                "query table requires a SPARQL SELECT".into(),
            ));
        };
        let rows = solutions
            .map(|row| {
                let row = row.map_err(value_error)?;
                Ok(columns
                    .iter()
                    .map(|column| row.get(column.as_str()).map(ToString::to_string))
                    .collect())
            })
            .collect::<PyResult<_>>()?;
        Ok((columns, rows))
    }
}

impl super::EvidenceSession {
    pub(super) fn result_database_layout(
        &self,
        name_paths: &[String],
        shape_names: Option<Vec<String>>,
        shape_query: Option<&str>,
    ) -> PyResult<Vec<TableLayout>> {
        let name_paths = self.parse_name_paths(name_paths)?;
        let mut nodes = BTreeMap::new();
        for (id, source) in &self.raw_schema.sources {
            if let Term::NamedNode(iri) = source
                && self
                    .shapes
                    .object(&NamedOrBlankNode::from(iri.clone()), vocab::SH_PATH)
                    .is_none()
            {
                nodes.insert(iri.as_str().to_owned(), (iri.clone(), *id));
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

        selected
            .into_iter()
            .map(|iri| {
                let (named, id) = nodes.get(&iri).ok_or_else(|| {
                    py_value_error(format!("expected a named node shape in this graph: {iri}"))
                })?;
                let node = NamedOrBlankNode::from(named.clone());
                let table_name = sql_alias(&self.shapes, &node, TABLE_NAME)?
                    .or_else(|| self.raw_schema.prefixes.compact_declared(&iri))
                    .unwrap_or_else(|| iri.clone());
                let mut fields = Vec::new();
                for property_id in self.binding_layout.property_slots(*id) {
                    let source = &self.raw_schema.sources[&ShapeId(property_id)];
                    let property = term_to_node(source).ok_or_else(|| {
                        py_value_error(format!("invalid property shape for {iri}: {source}"))
                    })?;
                    let path = self
                        .shapes
                        .object(&property, vocab::SH_PATH)
                        .expect("binding owners are property shapes");
                    let alias = sql_alias(&self.shapes, &property, COLUMN_NAME)?;
                    let label = field_label(&self.shapes, &name_paths, &property, &path);
                    fields.push((property_id, alias, label));
                }
                Ok((iri, table_name, fields))
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

/// A field is named like its shape-map binding: by the first authored name
/// that can label a column, a nonempty literal's text or an IRI's local name.
/// Without one, the property shape's IRI or its path supplies a local name.
fn field_label(
    shapes: &Loaded,
    name_paths: &[Path],
    property: &NamedOrBlankNode,
    path: &Term,
) -> Option<String> {
    let names = shifty_engine::names::authored_names(
        &shapes.graph,
        &Term::from(property.clone()),
        name_paths,
    );
    names
        .iter()
        .find_map(|name| match name {
            Term::Literal(name) => {
                let text = name.value().trim();
                (!text.is_empty()).then(|| text.to_owned())
            }
            Term::NamedNode(iri) => {
                shifty_engine::names::local_name(iri.as_str()).map(str::to_owned)
            }
            _ => None,
        })
        .or_else(|| match (property, path) {
            (NamedOrBlankNode::NamedNode(iri), _) | (_, Term::NamedNode(iri)) => {
                shifty_engine::names::local_name(iri.as_str()).map(str::to_owned)
            }
            _ => None,
        })
}

fn select_shapes(shapes: &Loaded, query: &str) -> PyResult<Vec<String>> {
    let dataset = default_graph(&shapes.graph);
    let mut evaluator = SparqlEvaluator::new();
    if let Some(base) = &shapes.base {
        evaluator = evaluator.with_base_iri(base).map_err(value_error)?;
    }
    for (prefix, iri) in &shapes.prefixes {
        evaluator = evaluator.with_prefix(prefix, iri).map_err(value_error)?;
    }
    let prepared = evaluator.parse_query(query).map_err(value_error)?;
    let QueryResults::Solutions(solutions) = prepared
        .on_queryable_dataset(&dataset)
        .execute()
        .map_err(value_error)?
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
