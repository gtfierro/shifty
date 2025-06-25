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
use ontoenv::ontology::OntologyLocation;
use oxigraph::store::Store;
use std::error::Error;
use std::path::PathBuf;

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
        let mut env: OntoEnv = OntoEnv::new_in_memory_online_with_search()?;

        let shapes_uris = match shapes_source {
            Source::Graph(uri) => env.add(OntologyLocation::Url(uri.clone()), true)?,
            Source::File(path) => env.add(OntologyLocation::File(path.clone()), true)?,
        };
        let datas_uris = match data_source {
            Source::Graph(uri) => env.add(OntologyLocation::Url(uri.clone()), true)?,
            Source::File(path) => env.add(OntologyLocation::File(path.clone()), true)?,
        };

        let shapes_graph_uri = shapes_uris.first().cloned().unwrap();
        let data_graph_uri = datas_uris.first().cloned().unwrap();

        let mut context =
            ValidationContext::new(store, env, shapes_graph_uri.name().into(), data_graph_uri.name().into());

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
