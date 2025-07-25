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

use crate::canonicalization::skolemize;
use crate::context::{ParsingContext, ShapesModel, ValidationContext};
use crate::optimize::Optimizer;
use crate::parser as shacl_parser;
use log::info;
use ontoenv::api::OntoEnv;
use ontoenv::config::Config;
use ontoenv::ontology::OntologyLocation;
use oxigraph::model::GraphNameRef;
use std::error::Error;
use std::path::PathBuf;
use std::rc::Rc;

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
    /// Creates a new Validator from local files.
    ///
    /// This method initializes the underlying `ValidationContext` by loading data from files.
    ///
    /// # Arguments
    ///
    /// * `shape_graph_path` - The file path for the SHACL shapes.
    /// * `data_graph_path` - The file path for the data to be validated.
    pub fn from_files(
        shape_graph_path: &str,
        data_graph_path: &str,
    ) -> Result<Self, Box<dyn Error>> {
        Self::from_sources(
            Source::File(PathBuf::from(shape_graph_path)),
            Source::File(PathBuf::from(data_graph_path)),
        )
    }

    /// Creates a new Validator from the given shapes and data sources.
    ///
    /// This method initializes the underlying `ValidationContext`, loading data from files
    /// or an `OntoEnv` as specified.
    ///
    /// # Arguments
    ///
    /// * `shapes_source` - The source for the SHACL shapes.
    /// * `data_source` - The source for the data to be validated.
    pub fn from_sources(
        shapes_source: Source,
        data_source: Source,
    ) -> Result<Self, Box<dyn Error>> {
        let config = Config::builder()
            .root(std::env::current_dir()?)
            .offline(true)
            .no_search(true)
            .temporary(true)
            .build()?;
        let mut env: OntoEnv = OntoEnv::init(config, false)?;

        let shapes_graph_id = match shapes_source {
            Source::Graph(uri) => env.add(OntologyLocation::Url(uri.clone()), true)?,
            Source::File(path) => env.add(OntologyLocation::File(path.clone()), true)?,
        };
        let shape_graph_iri = env.get_ontology(&shapes_graph_id).unwrap().name().clone();
        info!("Added shape graph: {}", shape_graph_iri);

        let data_graph_id = match data_source {
            Source::Graph(uri) => env.add(OntologyLocation::Url(uri.clone()), true)?,
            Source::File(path) => env.add(OntologyLocation::File(path.clone()), true)?,
        };
        let data_graph_iri = env.get_ontology(&data_graph_id).unwrap().name().clone();
        info!("Added data graph: {}", data_graph_iri);

        let store = env.io().store().clone();

        let shape_graph_base_iri =
            format!("{}/.well-known/skolem/", shape_graph_iri.as_str().trim_end_matches('/'));
        info!(
            "Skolemizing shape graph <{}> with base IRI <{}>",
            shape_graph_iri, shape_graph_base_iri
        );
        // TODO: The skolemization is necessary for now because of some behavior in oxigraph where
        // blank nodes in SPARQL queries will match *any* blank node in the graph, rather than
        // just the blank node with the same "identifier". This makes it difficult to compose
        // queries that find the propertyshape value nodes when the focus node is a blank node.
        // This means the query ends up looking like:
        //    SELECT ?valuenode WHERE { _:focusnode <http://example.org/path> ?valuenode . }
        // The _:focusnode will match any blank node in the graph, which is not what we want.
        // This pops up in test cases like property_and_001
        skolemize(
            &store,
            GraphNameRef::NamedNode(shape_graph_iri.as_ref()),
            &shape_graph_base_iri,
        )?;

        let data_graph_base_iri =
            format!("{}/.well-known/skolem/", data_graph_iri.as_str().trim_end_matches('/'));
        info!(
            "Skolemizing data graph <{}> with base IRI <{}>",
            data_graph_iri, data_graph_base_iri
        );
        skolemize(
            &store,
            GraphNameRef::NamedNode(data_graph_iri.as_ref()),
            &data_graph_base_iri,
        )?;

        info!(
            "Optimizing store with shape graph <{}> and data graph <{}>",
            shape_graph_iri, data_graph_iri
        );
        store.optimize().map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Error optimizing store: {}", e),
            ))
        })?;

        let mut parsing_context =
            ParsingContext::new(store, env, shape_graph_iri, data_graph_iri.clone());

        shacl_parser::run_parser(&mut parsing_context)?;

        info!("Optimizing shape graph");
        let mut o = Optimizer::new(parsing_context);
        o.optimize()?;
        info!("Finished parsing shapes and optimizing context");
        let final_ctx = o.finish();

        let model = ShapesModel {
            nodeshape_id_lookup: final_ctx.nodeshape_id_lookup,
            propshape_id_lookup: final_ctx.propshape_id_lookup,
            component_id_lookup: final_ctx.component_id_lookup,
            store: final_ctx.store,
            shape_graph_iri: final_ctx.shape_graph_iri,
            node_shapes: final_ctx.node_shapes,
            prop_shapes: final_ctx.prop_shapes,
            components: final_ctx.components,
            env: final_ctx.env,
        };

        let context = ValidationContext::new(Rc::new(model), data_graph_iri);

        Ok(Validator { context })
    }

    /// Validates the data graph against the shapes graph.
    ///
    /// This method executes the core validation logic and returns a `ValidationReport`.
    /// The report contains the outcome of the validation (conformity) and detailed
    /// results for any failures. The returned report is tied to the lifetime of the Validator.
    pub fn validate(&self) -> ValidationReport<'_> {
        let report_builder = validate::validate(&self.context);
        // The report needs the context to be able to serialize itself later.
        ValidationReport::new(report_builder.unwrap(), &self.context)
    }

    /// Generates a Graphviz DOT string representation of the shapes.
    ///
    /// This can be used to visualize the structure of the SHACL shapes, including
    /// their constraints and relationships.
    pub fn to_graphviz(&self) -> Result<String, String> {
        self.context.model.graphviz()
    }

    /// Generates a Graphviz DOT string representation of the shapes, with nodes colored by execution frequency.
    ///
    /// This can be used to visualize which parts of the shapes graph were most active during validation.
    /// Note: `validate()` must be called before this method to populate the execution traces.
    pub fn to_graphviz_heatmap(&self, include_all_nodes: bool) -> Result<String, String> {
        self.context.graphviz_heatmap(include_all_nodes)
    }
}
