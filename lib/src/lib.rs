//! A SHACL validator library.
#![deny(clippy::all)]

// Publicly visible items
pub mod backend;
pub mod inference;
pub mod ir;
pub mod ir_cache;
pub mod model;
pub mod shape;
pub mod trace;
pub mod types;

pub use inference::{InferenceConfig, InferenceError, InferenceOutcome};
pub use report::ValidationReport;

// Internal modules.
pub mod canonicalization;
pub(crate) mod context;
pub(crate) mod named_nodes;
pub(crate) mod optimize;
pub(crate) mod parser;
pub(crate) mod planning;
pub(crate) mod report;
pub(crate) mod runtime;
pub(crate) mod sparql;
pub mod test_utils; // Often pub for integration tests
pub(crate) mod validate;

use crate::canonicalization::skolemize;
use crate::context::model::OriginalValueIndex;
use crate::context::{
    render_heatmap_graphviz, render_shapes_graphviz, ParsingContext, ShapesModel, ValidationContext,
};
use crate::optimize::Optimizer;
use crate::parser as shacl_parser;
use log::info;
use ontoenv::api::OntoEnv;
use ontoenv::api::ResolveTarget;
use ontoenv::config::Config;
use ontoenv::ontology::OntologyLocation;
use ontoenv::options::{Overwrite, RefreshStrategy};
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::{GraphName, GraphNameRef, NamedNode, Quad};
use oxigraph::store::Store;
use shacl_ir::{FeatureToggles, ShapeIR};
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

/// Represents the source of shapes or data, which can be either a local file or a named graph from an `OntoEnv`.
#[derive(Debug)]
pub enum Source {
    /// A local file path.
    File(PathBuf),
    /// The URI of a named graph.
    Graph(String),
    /// An in-memory empty graph (used when a data graph is not needed).
    Empty,
}

/// Configurable builder for constructing `Validator` instances.
pub struct ValidatorBuilder {
    shapes_source: Option<Source>,
    data_source: Option<Source>,
    env_config: Option<Config>,
    skolemize_shapes: bool,
    skolemize_data: bool,
    enable_af: bool,
    enable_rules: bool,
    skip_invalid_rules: bool,
    warnings_are_errors: bool,
    do_imports: bool,
    shape_ir: Option<ShapeIR>,
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
            enable_af: true,
            enable_rules: true,
            skip_invalid_rules: false,
            warnings_are_errors: false,
            do_imports: true,
            shape_ir: None,
        }
    }

    /// Provide a precomputed SHACL-IR artifact (skips parsing shapes).
    pub fn with_shape_ir(mut self, shape_ir: ShapeIR) -> Self {
        self.shape_ir = Some(shape_ir);
        self
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

    /// Enables or disables SHACL AF extensions.
    pub fn with_af_enabled(mut self, enabled: bool) -> Self {
        self.enable_af = enabled;
        self
    }

    /// Enables or disables SHACL rules.
    pub fn with_rules_enabled(mut self, enabled: bool) -> Self {
        self.enable_rules = enabled;
        self
    }

    /// Skip invalid SHACL constructs instead of failing fast.
    pub fn with_skip_invalid_rules(mut self, skip: bool) -> Self {
        self.skip_invalid_rules = skip;
        self
    }

    /// Treat SHACL warnings as errors (default: false).
    pub fn with_warnings_are_errors(mut self, warnings_as_errors: bool) -> Self {
        self.warnings_are_errors = warnings_as_errors;
        self
    }

    /// Whether to resolve and load owl:imports closures for shapes/data (default: true).
    pub fn with_do_imports(mut self, do_imports: bool) -> Self {
        self.do_imports = do_imports;
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
            enable_af,
            enable_rules,
            skip_invalid_rules,
            warnings_are_errors,
            do_imports,
            shape_ir,
        } = self;

        if shape_ir.is_none() && shapes_source.is_none() {
            return Err("shapes source must be specified".to_string().into());
        }
        let data_source = data_source.ok_or_else(|| "data source must be specified".to_string())?;

        let config = match env_config {
            Some(config) => config,
            None => Self::default_config()?,
        };

        let mut env: OntoEnv = OntoEnv::init(config, false)?;
        let (shapes_graph_iri, shapes_in_env) = if let Some(ir) = shape_ir.as_ref() {
            (ir.shape_graph.clone(), false)
        } else {
            let source =
                shapes_source.ok_or_else(|| "shapes source must be specified".to_string())?;
            Self::add_source(&mut env, &source, "shapes")?
        };
        let (data_graph_iri, data_in_env) = match &data_source {
            Source::Empty => (
                NamedNode::new("urn:shifty:null-data")
                    .map_err(|e| Box::new(std::io::Error::other(e)) as Box<dyn Error>)?,
                false,
            ),
            _ => {
                let (iri, in_env) = Self::add_source(&mut env, &data_source, "data")?;
                (iri, in_env)
            }
        };
        let store = if do_imports {
            let store = Store::new().map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Error creating in-memory store: {}",
                    e
                ))) as Box<dyn Error>
            })?;

            // Always load shapes when available in env; load data only when it came from env too.
            let mut import_graphs: Vec<&NamedNode> = Vec::new();
            if shapes_in_env {
                import_graphs.push(&shapes_graph_iri);
            }
            if data_in_env {
                import_graphs.push(&data_graph_iri);
            }

            if import_graphs.is_empty() {
                env.io().store().clone()
            } else {
                for iri in import_graphs {
                    let graph_id =
                        env.resolve(ResolveTarget::Graph(iri.clone()))
                            .ok_or_else(|| {
                                Box::new(std::io::Error::other(format!(
                                    "Ontology not found for graph {}",
                                    iri
                                ))) as Box<dyn Error>
                            })?;

                    let mut closure_ids = env.get_closure(&graph_id, -1).map_err(|e| {
                        Box::new(std::io::Error::other(format!(
                            "Failed to build imports closure for {}: {}",
                            iri, e
                        ))) as Box<dyn Error>
                    })?;
                    if !closure_ids.contains(&graph_id) {
                        closure_ids.push(graph_id.clone());
                    }
                    let union = env
                        .get_union_graph(&closure_ids, Some(false), Some(false))
                        .map_err(|e| {
                            Box::new(std::io::Error::other(format!(
                                "Failed to load union graph for {}: {}",
                                iri, e
                            ))) as Box<dyn Error>
                        })?;

                    for quad in union.dataset.iter() {
                        store.insert(quad).map_err(|e| {
                            Box::new(std::io::Error::other(format!(
                                "Failed to insert quad into store: {}",
                                e
                            ))) as Box<dyn Error>
                        })?;
                    }
                }
                store
            }
        } else {
            env.io().store().clone()
        };

        if let Some(ir) = shape_ir.as_ref() {
            for quad in &ir.shape_quads {
                store.insert(quad).map_err(|e| {
                    Box::new(std::io::Error::other(format!(
                        "Failed to inject shape quad into store: {}",
                        e
                    ))) as Box<dyn Error>
                })?;
            }
        }

        Self::maybe_skolemize_graph("shape", &store, &shapes_graph_iri, skolemize_shapes)?;
        Self::maybe_skolemize_graph("data", &store, &data_graph_iri, skolemize_data)?;

        info!(
            "Optimizing store with shape graph <{}> and data graph <{}>",
            shapes_graph_iri, data_graph_iri
        );
        store.optimize().map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "Error optimizing store: {}",
                e
            )))
        })?;

        let data_skolem_base = if skolemize_data {
            Some(Self::skolem_base(&data_graph_iri))
        } else {
            None
        };
        let original_values = match &data_source {
            Source::File(path) => {
                let base_ref = data_skolem_base.as_deref();
                Some(OriginalValueIndex::from_path(path, base_ref)?)
            }
            Source::Graph(_) | Source::Empty => None,
        };

        if let Some(ir) = shape_ir.as_ref() {
            info!(
                "Loaded shapes graph <{}> from SHACL-IR cache",
                ir.shape_graph
            );
        }

        let features = FeatureToggles {
            enable_af,
            enable_rules,
            skip_invalid_rules,
        };

        let mut cached_shape_ir = shape_ir;
        if let Some(ir) = cached_shape_ir.as_mut() {
            ir.data_graph = Some(data_graph_iri.clone());
        }

        let (model, shape_ir_arc) = if let Some(ir) = cached_shape_ir {
            let model = ShapesModel::from_shape_ir(ir.clone(), store, env, original_values)?;
            (model, Arc::new(ir))
        } else {
            let model = Self::build_shapes_model(
                env,
                store,
                shapes_graph_iri.clone(),
                data_graph_iri.clone(),
                features.clone(),
                original_values,
            )?;
            let shape_ir = crate::ir::build_shape_ir(&model, Some(data_graph_iri.clone()))
                .map_err(|e| {
                    Box::new(std::io::Error::other(format!(
                        "Failed to build SHACL-IR cache: {}",
                        e
                    ))) as Box<dyn Error>
                })?;
            (model, Arc::new(shape_ir))
        };
        let context = ValidationContext::new(
            Arc::new(model),
            data_graph_iri,
            warnings_are_errors,
            shape_ir_arc,
        );
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
                Box::new(std::io::Error::other(format!(
                    "Failed to build OntoEnv config: {}",
                    e
                ))) as Box<dyn Error>
            })
    }

    fn add_source(
        env: &mut OntoEnv,
        source: &Source,
        label: &str,
    ) -> Result<(NamedNode, bool), Box<dyn Error>> {
        let fallback_location = match source {
            Source::File(path) => path.display().to_string(),
            Source::Graph(uri) => uri.clone(),
            Source::Empty => "<empty>".to_string(),
        };

        // Try OntoEnv first.
        let from_env = match source {
            Source::Graph(uri) => env.add(
                OntologyLocation::Url(uri.clone()),
                Overwrite::Allow,
                RefreshStrategy::Force,
            ),
            Source::File(path) => env.add(
                OntologyLocation::File(path.clone()),
                Overwrite::Allow,
                RefreshStrategy::Force,
            ),
            Source::Empty => {
                return Err(Box::new(std::io::Error::other(format!(
                    "Empty source is not loadable for {} graph",
                    label
                ))))
            }
        };

        // Resolve through OntoEnv when possible.
        if let Ok(graph_id) = from_env {
            if let Ok(ontology) = env.get_ontology(&graph_id) {
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
                return Ok((graph_iri, true));
            }
        }

        // Fallback: manual Turtle parse into the env store (keeps the graph name stable).
        let path = match source {
            Source::File(p) => p,
            _ => {
                return Err(Box::new(std::io::Error::other(format!(
                    "Failed to resolve {} graph {} via OntoEnv",
                    label, fallback_location
                ))))
            }
        };
        let iri = Url::from_file_path(path)
            .map_err(|_| {
                std::io::Error::other(format!(
                    "Failed to build file URL for {} graph at {}",
                    label,
                    path.display()
                ))
            })?
            .to_string();
        let graph_iri = NamedNode::new(iri.clone()).map_err(|e| {
            std::io::Error::other(format!(
                "Invalid graph IRI for {} graph at {}: {}",
                label,
                path.display(),
                e
            ))
        })?;

        let content = std::fs::read_to_string(path).map_err(|read_err| {
            std::io::Error::other(format!(
                "Failed to read {} graph {}: {}",
                label,
                path.display(),
                read_err
            ))
        })?;
        let parser = RdfParser::from_format(RdfFormat::Turtle)
            .with_base_iri(&iri)
            .map_err(|parse_err| {
                std::io::Error::other(format!(
                    "Failed to prepare Turtle parser for {} graph {}: {}",
                    label,
                    path.display(),
                    parse_err
                ))
            })?;
        for quad in parser.for_reader(std::io::Cursor::new(content.as_bytes())) {
            let quad = quad.map_err(|parse_err| {
                std::io::Error::other(format!(
                    "Failed to parse {} graph {}: {}",
                    label,
                    path.display(),
                    parse_err
                ))
            })?;
            env.io().store().insert(quad.as_ref()).map_err(|ins_err| {
                std::io::Error::other(format!(
                    "Failed to insert quad into {} graph {}: {}",
                    label,
                    path.display(),
                    ins_err
                ))
            })?;
        }

        eprintln!(
            "Loaded {} graph {} via fallback (location {})",
            label, graph_iri, fallback_location
        );
        info!(
            "Added {} graph via fallback: {} ({})",
            label, graph_iri, fallback_location
        );

        Ok((graph_iri, false))
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
        features: FeatureToggles,
        original_values: Option<OriginalValueIndex>,
    ) -> Result<ShapesModel, Box<dyn Error>> {
        let mut parsing_context = ParsingContext::new(
            store,
            env,
            shape_graph_iri.clone(),
            data_graph_iri.clone(),
            features.clone(),
            original_values.clone(),
        );
        shacl_parser::run_parser(&mut parsing_context)?;

        let mut optimizer = Optimizer::new(parsing_context);
        optimizer.optimize()?;
        let final_ctx = optimizer.finish();

        Ok(ShapesModel {
            nodeshape_id_lookup: final_ctx.nodeshape_id_lookup,
            propshape_id_lookup: final_ctx.propshape_id_lookup,
            component_id_lookup: final_ctx.component_id_lookup,
            rule_id_lookup: final_ctx.rule_id_lookup,
            store: final_ctx.store,
            shape_graph_iri: final_ctx.shape_graph_iri,
            node_shapes: final_ctx.node_shapes,
            prop_shapes: final_ctx.prop_shapes,
            component_descriptors: final_ctx.component_descriptors,
            component_templates: final_ctx.component_templates,
            shape_templates: final_ctx.shape_templates,
            shape_template_cache: final_ctx.shape_template_cache,
            rules: final_ctx.rules,
            node_shape_rules: final_ctx.node_shape_rules,
            prop_shape_rules: final_ctx.prop_shape_rules,
            env: final_ctx.env,
            sparql: final_ctx.sparql.clone(),
            features: final_ctx.features.clone(),
            original_values,
        })
    }
}

impl Default for ValidatorBuilder {
    fn default() -> Self {
        Self::new()
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

    /// Executes inference with a custom configuration and returns the outcome.
    pub fn run_inference_with_config(
        &self,
        config: InferenceConfig,
    ) -> Result<InferenceOutcome, InferenceError> {
        crate::inference::run_inference(&self.context, config)
    }

    /// Runs inference using the default configuration.
    pub fn run_inference(&self) -> Result<InferenceOutcome, InferenceError> {
        self.run_inference_with_config(InferenceConfig::default())
    }

    /// Returns all quads currently stored in the validator's data graph.
    pub fn data_graph_quads(&self) -> Result<Vec<Quad>, String> {
        let graph = GraphName::NamedNode(self.context.data_graph_iri.clone());
        let mut quads = Vec::new();
        for quad_res in
            self.context
                .model
                .store
                .quads_for_pattern(None, None, None, Some(graph.as_ref()))
        {
            let quad = quad_res.map_err(|e| format!("Failed to read data graph: {}", e))?;
            quads.push(quad);
        }
        Ok(quads)
    }

    /// Runs inference followed by validation, yielding both the inference outcome and report.
    pub fn validate_with_inference(
        &self,
        config: InferenceConfig,
    ) -> Result<(InferenceOutcome, ValidationReport<'_>), InferenceError> {
        let outcome = self.run_inference_with_config(config)?;
        let report = self.validate();
        Ok((outcome, report))
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

    /// Exposes the underlying validation context for diagnostics (CLI, tooling).
    pub fn context(&self) -> &ValidationContext {
        &self.context
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::named_nodes::SHACL;
    use crate::runtime::Component;
    use crate::sparql::validate_prebound_variable_usage;
    use oxigraph::model::vocab::rdf;
    use oxigraph::model::{NamedNode, NamedOrBlankNode, Term, TermRef};
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

        let validator = Validator::builder()
            .with_shapes_source(Source::File(PathBuf::from(shapes_path_str.clone())))
            .with_data_source(Source::File(PathBuf::from(data_path_str.clone())))
            .with_warnings_are_errors(true)
            .build()?;
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
    fn template_definitions_registered_and_executed() -> Result<(), Box<dyn Error>> {
        let _guard = validator_lock().lock().unwrap();
        let temp_dir = unique_temp_dir("shacl_template_test")?;

        let shapes_path = temp_dir.join("shapes.ttl");
        let data_path = temp_dir.join("data.ttl");

        let shapes_ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:MinLabelConstraint
    a sh:ConstraintComponent ;
    sh:declare [
        sh:prefix "rdfs" ;
        sh:namespace "http://www.w3.org/2000/01/rdf-schema#"^^xsd:anyURI ;
    ] ;
    sh:parameter [
        sh:path ex:minLength ;
        sh:varName "minLen" ;
        sh:description "Minimum label length" ;
    ] ;
    sh:message "Label shorter than {?minLen}" ;
    sh:propertyValidator [
        a sh:SPARQLSelectValidator ;
        sh:select """
            SELECT $this ?value WHERE {
                $this $PATH ?value .
                FILTER(strlen(str(?value)) < ?minLen)
            }
        """ ;
    ] ;
.

ex:ItemShape
    a sh:NodeShape ;
    sh:targetClass ex:Thing ;
    sh:property ex:LabelProperty ;
.

ex:LabelProperty
    a sh:PropertyShape ;
    sh:path rdfs:label ;
    sh:constraint ex:MinLabelConstraint ;
    ex:minLength 5 ;
    ex:templatePath rdfs:label ;
.

ex:LabelShapeTemplate
    a sh:Shape ;
    sh:parameter [
        sh:path ex:templatePath ;
        sh:varName "templPath" ;
    ] ;
    sh:shape [
        sh:path ex:templatePath ;
        sh:minCount 1 ;
    ] ;
.
"#;

        let data_ttl = r#"@prefix ex: <http://example.com/ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:ThingA a ex:Thing ;
    rdfs:label "abc" .

ex:ThingB a ex:Thing ;
    rdfs:label "adequate" .
"#;

        fs::write(&shapes_path, shapes_ttl)?;
        fs::write(&data_path, data_ttl)?;

        let validator = Validator::builder()
            .with_shapes_source(Source::File(shapes_path.clone()))
            .with_data_source(Source::File(data_path.clone()))
            .with_warnings_are_errors(true)
            .build()?;

        // Template metadata registered.
        let template_iri = NamedNode::new("http://example.com/ns#MinLabelConstraint")?;
        let template = validator
            .context
            .model
            .component_templates
            .get(&template_iri)
            .expect("template definition missing");
        assert_eq!(template.parameters.len(), 1);
        assert_eq!(template.parameters[0].var_name.as_deref(), Some("minLen"));
        assert_eq!(
            template.parameters[0].description.as_deref(),
            Some("Minimum label length")
        );
        assert_eq!(template.prefix_declarations.len(), 1);

        // Components that originate from the template keep a back-reference.
        let custom_components: Vec<_> = validator
            .context
            .components
            .values()
            .filter_map(|component| match component {
                Component::CustomConstraint(custom) => Some(custom.clone()),
                _ => None,
            })
            .collect();
        assert!(custom_components
            .iter()
            .any(|c| c.definition.iri == template_iri && c.definition.template.is_some()));

        let shape_template_iri = NamedNode::new("http://example.com/ns#LabelShapeTemplate")?;
        let shape_template = validator
            .context
            .model
            .shape_templates
            .get(&shape_template_iri)
            .expect("shape template definition missing");
        assert_eq!(shape_template.parameters.len(), 1);

        // Validation should catch the short label.
        let report = validator.validate();
        assert!(!report.conforms());

        Ok(())
    }

    #[test]
    fn sparql_constraint_allows_missing_path_but_requires_this() {
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

        assert!(validate_prebound_variable_usage(
            "SELECT ?this ?value ?minScore WHERE { ?this ex:score ?value . }",
            "unit-test",
            true,
            true,
        )
        .is_ok());

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
    fn custom_property_validator_allows_missing_path() {
        let _guard = validator_lock().lock().unwrap();
        assert!(validate_prebound_variable_usage(
            "SELECT ?this ?value WHERE { ?this ex:score ?value . }",
            "unit-test",
            true,
            true,
        )
        .is_ok());
    }
}
