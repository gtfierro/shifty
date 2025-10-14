//! A SHACL validator library.
#![deny(clippy::all)]

// Publicly visible items
pub mod model;
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
pub(crate) mod runtime;
pub mod test_utils; // Often pub for integration tests
pub(crate) mod validate;

use crate::canonicalization::skolemize;
use crate::context::{
    render_heatmap_graphviz, render_shapes_graphviz, ParsingContext, ShapesModel, ValidationContext,
};
use crate::optimize::Optimizer;
use crate::parser as shacl_parser;
use log::info;
use ontoenv::api::OntoEnv;
use ontoenv::config::Config;
use ontoenv::ontology::OntologyLocation;
use ontoenv::options::{Overwrite, RefreshStrategy};
use oxigraph::model::{GraphNameRef, NamedNode};
use oxigraph::store::Store;
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

/// Configurable builder for constructing `Validator` instances.
pub struct ValidatorBuilder {
    shapes_source: Option<Source>,
    data_source: Option<Source>,
    env_config: Option<Config>,
    skolemize_shapes: bool,
    skolemize_data: bool,
}

impl ValidatorBuilder {
    /// Creates a new builder with default configuration.
    pub fn new() -> Self {
        Self {
            shapes_source: None,
            data_source: None,
            env_config: None,
            skolemize_shapes: true,
            skolemize_data: true,
        }
    }

    /// Sets the shapes source used for validation.
    pub fn with_shapes_source(mut self, source: Source) -> Self {
        self.shapes_source = Some(source);
        self
    }

    /// Sets the data source used for validation.
    pub fn with_data_source(mut self, source: Source) -> Self {
        self.data_source = Some(source);
        self
    }

    /// Overrides the `OntoEnv` configuration.
    pub fn with_env_config(mut self, config: Config) -> Self {
        self.env_config = Some(config);
        self
    }

    /// Controls skolemization for shapes and data graphs.
    pub fn with_skolemization(mut self, shapes: bool, data: bool) -> Self {
        self.skolemize_shapes = shapes;
        self.skolemize_data = data;
        self
    }

    /// Builds a `Validator` from the configured options.
    pub fn build(self) -> Result<Validator, Box<dyn Error>> {
        let Self {
            shapes_source,
            data_source,
            env_config,
            skolemize_shapes,
            skolemize_data,
        } = self;

        let shapes_source =
            shapes_source.ok_or_else(|| "shapes source must be specified".to_string())?;
        let data_source = data_source.ok_or_else(|| "data source must be specified".to_string())?;

        let config = match env_config {
            Some(config) => config,
            None => Self::default_config()?,
        };

        let mut env: OntoEnv = OntoEnv::init(config, false)?;
        let shapes_graph_iri = Self::add_source(&mut env, &shapes_source, "shapes")?;
        let data_graph_iri = Self::add_source(&mut env, &data_source, "data")?;
        let store = env.io().store().clone();

        Self::maybe_skolemize_graph("shape", &store, &shapes_graph_iri, skolemize_shapes)?;
        Self::maybe_skolemize_graph("data", &store, &data_graph_iri, skolemize_data)?;

        info!(
            "Optimizing store with shape graph <{}> and data graph <{}>",
            shapes_graph_iri, data_graph_iri
        );
        store.optimize().map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Error optimizing store: {}", e),
            ))
        })?;

        let model =
            Self::build_shapes_model(env, store, shapes_graph_iri.clone(), data_graph_iri.clone())?;
        let context = ValidationContext::new(Rc::new(model), data_graph_iri);
        Ok(Validator { context })
    }

    fn default_config() -> Result<Config, Box<dyn Error>> {
        Config::builder()
            .root(std::env::current_dir()?)
            .offline(true)
            .no_search(true)
            .temporary(true)
            .build()
            .map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to build OntoEnv config: {}", e),
                )) as Box<dyn Error>
            })
    }

    fn add_source(
        env: &mut OntoEnv,
        source: &Source,
        label: &str,
    ) -> Result<NamedNode, Box<dyn Error>> {
        let graph_id = match source {
            Source::Graph(uri) => env.add(
                OntologyLocation::Url(uri.clone()),
                Overwrite::Allow,
                RefreshStrategy::Force,
            )?,
            Source::File(path) => env.add(
                OntologyLocation::File(path.clone()),
                Overwrite::Allow,
                RefreshStrategy::Force,
            )?,
        };

        let ontology = env
            .get_ontology(&graph_id)
            .map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to resolve {} graph: {}", label, e),
                )) as Box<dyn Error>
            })?
            .clone();
        let graph_iri = ontology.name().clone();
        let location = ontology
            .location()
            .map(|loc| loc.as_str().to_string())
            .unwrap_or_else(|| "<unknown>".into());

        eprintln!(
            "Loaded {} graph {} (location {})",
            label, graph_iri, location
        );
        info!("Added {} graph: {}", label, graph_iri);
        Ok(graph_iri)
    }

    fn skolem_base(iri: &NamedNode) -> String {
        format!("{}/.well-known/skolem/", iri.as_str().trim_end_matches('/'))
    }

    fn maybe_skolemize_graph(
        graph_label: &str,
        store: &Store,
        graph_iri: &NamedNode,
        should_skolemize: bool,
    ) -> Result<(), Box<dyn Error>> {
        if !should_skolemize {
            return Ok(());
        }

        let base = Self::skolem_base(graph_iri);
        info!(
            "Skolemizing {} graph <{}> with base IRI <{}>",
            graph_label, graph_iri, base
        );
        skolemize(store, GraphNameRef::NamedNode(graph_iri.as_ref()), &base)?;
        Ok(())
    }

    fn build_shapes_model(
        env: OntoEnv,
        store: Store,
        shape_graph_iri: NamedNode,
        data_graph_iri: NamedNode,
    ) -> Result<ShapesModel, Box<dyn Error>> {
        let mut parsing_context =
            ParsingContext::new(store, env, shape_graph_iri.clone(), data_graph_iri.clone());
        shacl_parser::run_parser(&mut parsing_context)?;

        let mut optimizer = Optimizer::new(parsing_context);
        optimizer.optimize()?;
        let final_ctx = optimizer.finish();

        Ok(ShapesModel {
            nodeshape_id_lookup: final_ctx.nodeshape_id_lookup,
            propshape_id_lookup: final_ctx.propshape_id_lookup,
            component_id_lookup: final_ctx.component_id_lookup,
            store: final_ctx.store,
            shape_graph_iri: final_ctx.shape_graph_iri,
            node_shapes: final_ctx.node_shapes,
            prop_shapes: final_ctx.prop_shapes,
            component_descriptors: final_ctx.component_descriptors,
            env: final_ctx.env,
        })
    }
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
    /// Creates a `ValidatorBuilder` for advanced configuration.
    pub fn builder() -> ValidatorBuilder {
        ValidatorBuilder::new()
    }

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
        ValidatorBuilder::new()
            .with_shapes_source(Source::File(PathBuf::from(shape_graph_path)))
            .with_data_source(Source::File(PathBuf::from(data_graph_path)))
            .build()
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
        ValidatorBuilder::new()
            .with_shapes_source(shapes_source)
            .with_data_source(data_source)
            .build()
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
        render_shapes_graphviz(self.context.model.as_ref())
    }

    /// Generates a Graphviz DOT string representation of the shapes, with nodes colored by execution frequency.
    ///
    /// This can be used to visualize which parts of the shapes graph were most active during validation.
    /// Note: `validate()` must be called before this method to populate the execution traces.
    pub fn to_graphviz_heatmap(&self, include_all_nodes: bool) -> Result<String, String> {
        render_heatmap_graphviz(&self.context, include_all_nodes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::named_nodes::SHACL;
    use oxigraph::model::vocab::rdf;
    use oxigraph::model::{NamedOrBlankNode, Term, TermRef};
    use std::error::Error;
    use std::fs;
    use std::io::Write;
    use std::panic;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(prefix: &str) -> Result<PathBuf, Box<dyn Error>> {
        let mut dir = std::env::temp_dir();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        dir.push(format!("{}_{}", prefix, timestamp));
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn validator_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn custom_sparql_message_and_severity() -> Result<(), Box<dyn Error>> {
        let _guard = validator_lock().lock().unwrap();
        let temp_dir = unique_temp_dir("shacl_message_test")?;

        let shapes_ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:MinScoreConstraintComponent
    a sh:ConstraintComponent ;
    sh:parameter [
        sh:path ex:minScore ;
        sh:optional false
    ] ;
    sh:propertyValidator [
        sh:message "Score must be at least {?minScore} (got {?value})."@en ;
        sh:select """
            PREFIX ex: <http://example.com/ns#>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            SELECT ?this ?value ?minScore
            WHERE {
                ?this $PATH ?value .
                FILTER(xsd:integer(?value) < xsd:integer(?minScore))
            }
        """ ;
        sh:severity sh:Warning ;
    ] ;
    sh:message "Default {?minScore}"@en ;
    sh:severity sh:Info .

ex:ScoreShape
    a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [
        sh:path ex:score ;
        ex:minScore 5 ;
    ] .
"#;

        let data_ttl = r#"@prefix ex: <http://example.com/ns#> .

ex:Alice a ex:Person ;
    ex:score 3 .
"#;

        let shapes_path = temp_dir.join("shapes.ttl");
        let data_path = temp_dir.join("data.ttl");

        {
            let mut file = fs::File::create(&shapes_path)?;
            file.write_all(shapes_ttl.as_bytes())?;
        }
        {
            let mut file = fs::File::create(&data_path)?;
            file.write_all(data_ttl.as_bytes())?;
        }

        let shapes_path_str = shapes_path.to_string_lossy().to_string();
        let data_path_str = data_path.to_string_lossy().to_string();

        let validator = Validator::from_files(&shapes_path_str, &data_path_str)?;
        let report = validator.validate();
        assert!(!report.conforms(), "Expected validation to fail");

        let graph = report.to_graph();
        let sh = SHACL::new();

        let mut result_nodes: Vec<NamedOrBlankNode> = Vec::new();
        for triple in graph.iter() {
            if triple.predicate == rdf::TYPE
                && triple.object == TermRef::NamedNode(sh.validation_result)
            {
                result_nodes.push(triple.subject.into_owned());
            }
        }

        assert_eq!(
            result_nodes.len(),
            1,
            "Expected exactly one validation result"
        );
        let result_subject = result_nodes[0].as_ref();

        let severity_terms: Vec<Term> = graph
            .objects_for_subject_predicate(result_subject, sh.result_severity)
            .map(|t| t.into_owned())
            .collect();
        assert_eq!(
            severity_terms,
            vec![Term::from(sh.warning)],
            "Severity should inherit sh:Warning from the validator"
        );

        let message_terms: Vec<Term> = graph
            .objects_for_subject_predicate(result_subject, sh.result_message)
            .map(|t| t.into_owned())
            .collect();
        assert!(
            message_terms.iter().any(|term| {
                if let Term::Literal(lit) = term {
                    lit.value() == "Score must be at least 5 (got 3)."
                        && matches!(lit.language(), Some(lang) if lang == "en")
                } else {
                    false
                }
            }),
            "Expected substituted result message literal"
        );

        fs::remove_dir_all(&temp_dir)?;
        Ok(())
    }

    #[test]
    fn sparql_constraint_requires_this() {
        let _guard = validator_lock().lock().unwrap();
        let temp_dir = unique_temp_dir("shacl_prebinding_this").unwrap();

        let shapes_ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .

ex:TestShape
    a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:sparql [
        sh:select """
            PREFIX ex: <http://example.com/ns#>
            SELECT ?msg WHERE { ?msg ex:note ?n . }
        """
    ] .
"#;

        let data_ttl = r#"@prefix ex: <http://example.com/ns#> .

ex:Alice a ex:Person ;
    ex:note "test" .
"#;

        let shapes_path = temp_dir.join("shapes.ttl");
        let data_path = temp_dir.join("data.ttl");

        fs::write(&shapes_path, shapes_ttl).unwrap();
        fs::write(&data_path, data_ttl).unwrap();

        assert!(
            crate::runtime::validators::validate_prebound_variable_usage(
                "SELECT ?this ?value ?minScore WHERE { ?this ex:score ?value . }",
                "unit-test",
                true,
                true,
            )
            .is_err()
        );

        let result =
            Validator::from_files(&shapes_path.to_string_lossy(), &data_path.to_string_lossy());
        let msg = match result {
            Err(err) => err.to_string(),
            Ok(_) => panic!("Expected missing $this to be rejected"),
        };
        assert!(
            msg.contains("$this"),
            "Error should mention $this, got: {}",
            msg
        );

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn custom_property_validator_requires_path() {
        let _guard = validator_lock().lock().unwrap();
        assert!(
            crate::runtime::validators::validate_prebound_variable_usage(
                "SELECT ?this ?value WHERE { ?this ex:score ?value . }",
                "unit-test",
                true,
                true,
            )
            .is_err()
        );
    }
}
