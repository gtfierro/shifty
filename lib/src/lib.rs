//! A SHACL validator library.
#![deny(clippy::all)]

// Publicly visible items
pub mod components;
pub mod shape;
pub mod types;

pub use report::ValidationReport;

// Internal modules.
pub mod canonicalization;
pub(crate) mod context;
pub(crate) mod named_nodes;
pub(crate) mod optimize;
pub(crate) mod parser;
pub(crate) mod report;
pub mod test_utils; // Often pub for integration tests
pub(crate) mod validate;

use crate::context::ValidationContext;
use crate::parser as shacl_parser;
use ontoenv::api::OntoEnv;
use oxigraph::io::RdfFormat;
use oxigraph::model::NamedNode;
use oxigraph::store::Store;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

/// Represents the source of shapes or data, which can be either a local file or a named graph from an `OntoEnv`.
#[derive(Debug)]
pub enum Source {
    /// A local file path.
    File(PathBuf),
    /// The URI of a named graph.
    Graph(String),
}

/// A simple facade for the SHACL validator.
///
/// This provides a straightforward interface for common validation tasks.
/// It handles the creation of a `ValidationContext`, parsing of shapes and data,
/// running the validation, and generating reports.
///
/// For more advanced control, such as inspecting the parsed shapes or performing
/// optimizations, use `ValidationContext` directly.
pub struct Validator {
    context: ValidationContext,
}

impl Validator {
    /// Creates a new Validator from the given shapes and data sources.
    ///
    /// This method initializes the underlying `ValidationContext`, loading data from files
    /// or an `OntoEnv` as specified.
    ///
    /// # Arguments
    ///
    /// * `shapes_source` - The source for the SHACL shapes.
    /// * `data_source` - The source for the data to be validated.
    pub fn from_sources(shapes_source: Source, data_source: Source) -> Result<Self, Box<dyn Error>> {
        let store = Store::new()?;
        let mut env: Option<OntoEnv> = None;

        // If any source is a graph URI, we need to load an OntoEnv to find it.
        if matches!(shapes_source, Source::Graph(_)) || matches!(data_source, Source::Graph(_)) {
            let onto_env = OntoEnv::load_from_directory(PathBuf::from("."), false)?;
            for (graph_id, ontology) in onto_env.ontologies() {
                if let Ok(graph) = ontology.graph() {
                    let graph_name = NamedNode::new(graph_id.to_string())?;
                    for triple in graph.iter() {
                        store.insert(triple.in_graph(graph_name.as_ref()))?;
                    }
                }
            }
            env = Some(onto_env);
        }

        // Helper to load a file into a named graph in the store.
        // The graph name is derived from the file's canonical path.
        let load_file_to_store = |path: &Path, store: &Store| -> Result<NamedNode, Box<dyn Error>> {
            let canonical_path = path.canonicalize()?;
            let graph_uri_str =
                format!("file://{}", canonical_path.to_str().ok_or("Invalid path")?);
            let graph_uri = NamedNode::new(graph_uri_str)?;
            let format = RdfFormat::from_path(&canonical_path)
                .ok_or("Could not determine RDF format from file extension")?;
            let file = File::open(path)?;
            let reader = BufReader::new(file);
            store.load_graph(reader, format, graph_uri.as_ref().into(), None)?;
            Ok(graph_uri)
        };

        let shapes_graph_uri = match shapes_source {
            Source::File(path) => load_file_to_store(&path, &store)?,
            Source::Graph(uri) => NamedNode::new(uri)?,
        };

        let data_graph_uri = match data_source {
            Source::File(path) => load_file_to_store(&path, &store)?,
            Source::Graph(uri) => NamedNode::new(uri)?,
        };

        // If we didn't load an OntoEnv, create a temporary empty one.
        let final_env = env.unwrap();

        let mut context =
            ValidationContext::new(store, final_env, shapes_graph_uri, data_graph_uri);

        shacl_parser::run_parser(&mut context)?;

        Ok(Validator { context })
    }

    /// Validates the data graph against the shapes graph.
    ///
    /// This method executes the core validation logic and returns a `ValidationReport`.
    /// The report contains the outcome of the validation (conformity) and detailed
    /// results for any failures. The returned report is tied to the lifetime of the Validator.
    pub fn validate(&self) -> ValidationReport<'_> {
        let report_builder = self.context.validate();
        // The report needs the context to be able to serialize itself later.
        ValidationReport::new(report_builder, &self.context)
    }

    /// Generates a Graphviz DOT string representation of the shapes.
    ///
    /// This can be used to visualize the structure of the SHACL shapes, including
    /// their constraints and relationships.
    pub fn to_graphviz(&self) -> Result<String, String> {
        self.context.graphviz()
    }

    /// Generates a Graphviz DOT string representation of the shapes, with nodes colored by execution frequency.
    ///
    /// This can be used to visualize which parts of the shapes graph were most active during validation.
    /// Note: `validate()` must be called before this method to populate the execution traces.
    pub fn to_graphviz_heatmap(&self, include_all_nodes: bool) -> Result<String, String> {
        self.context.graphviz_heatmap(include_all_nodes)
    }
}
