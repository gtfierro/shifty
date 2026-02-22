//! A SHACL validator library.
#![deny(clippy::all)]

// Publicly visible items
pub mod backend;
pub mod compiled_runtime;
pub mod inference;
pub mod ir;
pub mod ir_cache;
pub mod model;
pub mod shacl_ir;
pub mod shape;
pub(crate) mod skolem;
pub mod trace;
pub mod types;

pub use inference::{InferenceConfig, InferenceError, InferenceOutcome};
pub use report::{ValidationReport, ValidationReportOptions};
pub use types::ComponentID;

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
use crate::shacl_ir::{FeatureToggles, ShapeIR};
use log::{debug, info, warn};
use ontoenv::api::OntoEnv;
use ontoenv::api::ResolveTarget;
use ontoenv::config::Config;
use ontoenv::ontology::{GraphIdentifier, OntologyLocation};
use ontoenv::options::CacheMode;
use ontoenv::options::{Overwrite, RefreshStrategy};
use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::{GraphName, GraphNameRef, NamedNode, Quad, Term};
use oxigraph::store::Store;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use url::Url;

/// Represents the source of shapes or data, which can be either a local file or a named graph from an `OntoEnv`.
#[derive(Debug, Clone)]
pub enum Source {
    /// A local file path.
    File(PathBuf),
    /// The URI of a named graph.
    Graph(String),
    /// In-memory quads scoped to a graph identifier.
    ///
    /// Callers should provide quads that belong to this logical graph. During loading,
    /// quads from the default graph are assigned to `graph`.
    Quads { graph: String, quads: Vec<Quad> },
    /// An in-memory empty graph (used when a data graph is not needed).
    Empty,
}

#[derive(Debug, Clone)]
struct LoadedSource {
    graph_iri: NamedNode,
    in_env: bool,
    graph_id: Option<GraphIdentifier>,
}

/// Aggregated graph-call counters for a specific constraint component execution context.
#[derive(Debug, Clone)]
pub struct ComponentGraphCallStat {
    /// Constraint component ID (`sh:...` instance in SHACL-IR).
    pub component_id: ComponentID,
    /// Source shape identifier that owns the component.
    pub source_shape: String,
    /// Human-readable component label (e.g., `MinCount`, `SPARQLConstraint`).
    pub component_label: String,
    /// Number of `quads_for_pattern` calls issued while this component was active.
    pub quads_for_pattern_calls: u64,
    /// Number of `execute_prepared` calls issued while this component was active.
    pub execute_prepared_calls: u64,
    /// Number of component validator invocations observed.
    pub component_invocations: u64,
    /// Total runtime across invocations, in milliseconds.
    pub runtime_total_ms: f64,
    /// Minimum runtime across invocations, in milliseconds.
    pub runtime_min_ms: f64,
    /// Maximum runtime across invocations, in milliseconds.
    pub runtime_max_ms: f64,
    /// Mean runtime across invocations, in milliseconds.
    pub runtime_mean_ms: f64,
    /// Standard deviation of runtime across invocations, in milliseconds.
    pub runtime_stddev_ms: f64,
}

/// Aggregated runtime counters for shape-level selection phases.
#[derive(Debug, Clone)]
pub struct ShapePhaseTimingStat {
    /// Source shape identifier/term.
    pub source_shape: String,
    /// Phase name (`node_target_selection`, `property_target_selection`, `property_value_selection`).
    pub phase: String,
    /// Number of phase invocations observed.
    pub invocations: u64,
    /// Total runtime across invocations, in milliseconds.
    pub runtime_total_ms: f64,
    /// Minimum runtime across invocations, in milliseconds.
    pub runtime_min_ms: f64,
    /// Maximum runtime across invocations, in milliseconds.
    pub runtime_max_ms: f64,
    /// Mean runtime across invocations, in milliseconds.
    pub runtime_mean_ms: f64,
    /// Standard deviation of runtime across invocations, in milliseconds.
    pub runtime_stddev_ms: f64,
}

/// Aggregated runtime counters for SPARQL constraint query execution.
#[derive(Debug, Clone)]
pub struct SparqlQueryCallStat {
    /// Source shape identifier/term that owns the SPARQL component.
    pub source_shape: String,
    /// Constraint component ID (`sh:...` instance in SHACL-IR).
    pub component_id: ComponentID,
    /// Constraint node term identifying the concrete `sh:sparql` constraint.
    pub constraint_term: String,
    /// 64-bit hash of the fully rendered SPARQL query string.
    pub query_hash: u64,
    /// Number of query executions observed for this key.
    pub invocations: u64,
    /// Total number of solution rows returned across invocations.
    pub rows_returned_total: u64,
    /// Minimum rows returned by a single invocation.
    pub rows_returned_min: u64,
    /// Maximum rows returned by a single invocation.
    pub rows_returned_max: u64,
    /// Mean rows returned per invocation.
    pub rows_returned_mean: f64,
    /// Total runtime across invocations, in milliseconds.
    pub runtime_total_ms: f64,
    /// Minimum runtime across invocations, in milliseconds.
    pub runtime_min_ms: f64,
    /// Maximum runtime across invocations, in milliseconds.
    pub runtime_max_ms: f64,
    /// Mean runtime across invocations, in milliseconds.
    pub runtime_mean_ms: f64,
    /// Standard deviation of runtime across invocations, in milliseconds.
    pub runtime_stddev_ms: f64,
}

type SharedEnvHandle = Arc<RwLock<OntoEnv>>;

static SHARED_ONTOENV: OnceLock<Mutex<Option<SharedEnvHandle>>> = OnceLock::new();

fn shared_ontoenv_slot() -> &'static Mutex<Option<SharedEnvHandle>> {
    SHARED_ONTOENV.get_or_init(|| Mutex::new(None))
}

/// Replace the process-wide shared OntoEnv handle.
pub fn set_shared_ontoenv(env: OntoEnv) -> SharedEnvHandle {
    let handle = Arc::new(RwLock::new(env));
    *shared_ontoenv_slot().lock().unwrap() = Some(handle.clone());
    handle
}

/// Clear the process-wide shared OntoEnv handle.
pub fn clear_shared_ontoenv() {
    *shared_ontoenv_slot().lock().unwrap() = None;
}

fn get_shared_ontoenv() -> Option<SharedEnvHandle> {
    shared_ontoenv_slot().lock().unwrap().clone()
}

/// Configurable builder for constructing `Validator` instances.
///
/// The builder owns all validation-time flags (imports, union graphs, AF/rules, etc.) so
/// callers can create repeatable validators without threading configuration through every call.
pub struct ValidatorBuilder {
    shapes_source: Option<Source>,
    data_source: Option<Source>,
    env_config: Option<Config>,
    use_shared_env: bool,
    skolemize_shapes: bool,
    skolemize_data: bool,
    use_shapes_graph_union: bool,
    enable_af: bool,
    enable_rules: bool,
    skip_invalid_rules: bool,
    warnings_are_errors: bool,
    skip_sparql_blank_targets: bool,
    strict_custom_constraints: bool,
    do_imports: bool,
    refresh_strategy: RefreshStrategy,
    import_depth: i32,
    temporary_env: bool,
    optimize_store: bool,
    optimize_shapes: bool,
    optimize_shapes_data_dependent: bool,
    shape_ir: Option<ShapeIR>,
}

struct BuildShapesModelConfig {
    features: FeatureToggles,
    strict_custom_constraints: bool,
    original_values: Option<OriginalValueIndex>,
    optimize_shapes: bool,
    optimize_shapes_data_dependent: bool,
}

impl ValidatorBuilder {
    /// Creates a new builder with default configuration.
    pub fn new() -> Self {
        Self {
            shapes_source: None,
            data_source: None,
            env_config: None,
            use_shared_env: false,
            skolemize_shapes: true,
            skolemize_data: true,
            use_shapes_graph_union: true,
            enable_af: true,
            enable_rules: true,
            skip_invalid_rules: false,
            warnings_are_errors: false,
            skip_sparql_blank_targets: true,
            strict_custom_constraints: false,
            do_imports: true,
            refresh_strategy: RefreshStrategy::UseCache,
            import_depth: -1,
            temporary_env: true,
            optimize_store: true,
            optimize_shapes: true,
            optimize_shapes_data_dependent: true,
            shape_ir: None,
        }
    }

    /// Provide a precomputed SHACL-IR artifact (skips parsing shapes).
    pub fn with_shape_ir(mut self, shape_ir: ShapeIR) -> Self {
        self.shape_ir = Some(shape_ir);
        self
    }

    /// Controls whether the validator should treat the data graph as the union of the data and shapes graphs.
    /// Defaults to `true`.
    pub fn with_shapes_data_union(mut self, enabled: bool) -> Self {
        self.use_shapes_graph_union = enabled;
        self
    }

    /// Sets the shapes source used for validation.
    ///
    /// If omitted, `build()` reuses the configured data source as the shapes source.
    /// This fallback is only available when the data source is not `Source::Empty`.
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

    /// Use the process-wide shared OntoEnv for this validator.
    pub fn with_shared_ontoenv(mut self, enabled: bool) -> Self {
        self.use_shared_env = enabled;
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

    /// Skip SPARQL-based constraints when the focus node is a blank node (default: true).
    pub fn with_skip_sparql_blank_targets(mut self, skip: bool) -> Self {
        self.skip_sparql_blank_targets = skip;
        self
    }

    /// Require custom constraint components to declare validators (default: false).
    pub fn with_strict_custom_constraints(mut self, strict: bool) -> Self {
        self.strict_custom_constraints = strict;
        self
    }

    /// Whether to resolve and load owl:imports closures for shapes/data (default: true).
    pub fn with_do_imports(mut self, do_imports: bool) -> Self {
        self.do_imports = do_imports;
        self
    }

    /// Force-refresh ontologies instead of using cached entries (default: use cache).
    pub fn with_force_refresh(mut self, force_refresh: bool) -> Self {
        self.refresh_strategy = if force_refresh {
            RefreshStrategy::Force
        } else {
            RefreshStrategy::UseCache
        };
        self
    }

    /// Set the maximum owl:imports recursion depth (-1 = unlimited, 0 = only the root graph).
    pub fn with_import_depth(mut self, import_depth: i32) -> Self {
        self.import_depth = import_depth;
        self
    }

    /// Store OntoEnv data in a temporary directory (default: true). Set to false to
    /// reuse a local OntoEnv cache if present.
    pub fn with_temporary_env(mut self, temporary_env: bool) -> Self {
        self.temporary_env = temporary_env;
        self
    }

    /// Optimize the Oxigraph store before validation (default: true).
    pub fn with_store_optimization(mut self, enabled: bool) -> Self {
        self.optimize_store = enabled;
        self
    }

    /// Optimize parsed shapes (defaults to true). Disable to preserve raw parser output.
    pub fn with_shape_optimization(mut self, enabled: bool) -> Self {
        self.optimize_shapes = enabled;
        self
    }

    /// Enable or disable data-dependent shape optimization passes (default: true).
    ///
    /// These passes inspect the current data graph and may prune targets.
    /// Use `false` for shapes-only compilation flows that do not yet have runtime data.
    pub fn with_data_dependent_shape_optimization(mut self, enabled: bool) -> Self {
        self.optimize_shapes_data_dependent = enabled;
        self
    }

    /// Builds a `Validator` from the configured options.
    pub fn build(self) -> Result<Validator, Box<dyn Error>> {
        let Self {
            mut shapes_source,
            data_source,
            env_config,
            use_shared_env,
            skolemize_shapes,
            skolemize_data,
            use_shapes_graph_union,
            enable_af,
            enable_rules,
            skip_invalid_rules,
            warnings_are_errors,
            skip_sparql_blank_targets,
            strict_custom_constraints,
            do_imports,
            refresh_strategy,
            import_depth,
            temporary_env,
            optimize_store,
            optimize_shapes,
            optimize_shapes_data_dependent,
            shape_ir,
        } = self;

        let data_source = data_source.ok_or_else(|| "data source must be specified".to_string())?;
        if shape_ir.is_none() && shapes_source.is_none() {
            if matches!(data_source, Source::Empty) {
                return Err("shapes source must be specified when data source is empty"
                    .to_string()
                    .into());
            }
            info!("No shapes source provided; using data source as shapes source");
            shapes_source = Some(data_source.clone());
        }

        let config = match env_config {
            Some(config) => config,
            None => Self::default_config(temporary_env)?,
        };

        let env_handle = if use_shared_env {
            Self::get_or_init_shared_env(config, temporary_env)?
        } else {
            Arc::new(RwLock::new(Self::open_env(config, temporary_env)?))
        };

        let (
            shapes_graph_iri,
            shapes_graph_id,
            data_graph_iri,
            mut shape_graphs_for_union,
            _merged_closure_ids,
            store,
        ) = {
            let mut env = env_handle.write().unwrap();
            let include_imports_on_add = do_imports && import_depth != 0;
            let shapes_source_info = if let Some(ir) = shape_ir.as_ref() {
                LoadedSource {
                    graph_iri: ir.shape_graph.clone(),
                    in_env: false,
                    graph_id: None,
                }
            } else {
                let source = shapes_source
                    .as_ref()
                    .expect("shapes source is initialized when SHACL-IR is absent");
                Self::add_source(
                    &mut env,
                    source,
                    "shapes",
                    include_imports_on_add,
                    refresh_strategy,
                    import_depth,
                )?
            };
            let shapes_graph_iri = shapes_source_info.graph_iri.clone();
            let shapes_in_env = shapes_source_info.in_env;
            let shapes_graph_id = shapes_source_info.graph_id.clone();

            let data_source_info = match &data_source {
                Source::Empty => LoadedSource {
                    graph_iri: NamedNode::new("urn:shifty:null-data")
                        .map_err(|e| Box::new(std::io::Error::other(e)) as Box<dyn Error>)?,
                    in_env: false,
                    graph_id: None,
                },
                _ => {
                    Self::add_source(
                        &mut env,
                        &data_source,
                        "data",
                        include_imports_on_add,
                        refresh_strategy,
                        import_depth,
                    )?
                }
            };
            let data_graph_iri = data_source_info.graph_iri.clone();
            let data_in_env = data_source_info.in_env;
            let data_graph_id = data_source_info.graph_id.clone();

            let shapes_quads_import_graphs = if do_imports && !shapes_in_env {
                if let Some(Source::Quads { quads, .. }) = shapes_source.as_ref() {
                    Self::load_import_graphs_for_quads(
                        &mut env,
                        quads,
                        import_depth,
                        "shapes",
                        refresh_strategy,
                    )?
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };
            let data_closure_ids = if do_imports && data_in_env {
                Some(Self::resolve_closure_ids_for_graph(
                    &mut env,
                    data_graph_id.as_ref(),
                    &data_graph_iri,
                    import_depth,
                    "data",
                )?)
            } else {
                None
            };
            if do_imports && !data_in_env {
                if let Source::Quads { quads, .. } = &data_source {
                    let _ = Self::load_import_graphs_for_quads(
                        &mut env,
                        quads,
                        import_depth,
                        "data",
                        refresh_strategy,
                    )?;
                }
            }
            let data_contains_shapes = data_closure_ids
                .as_ref()
                .is_some_and(|ids| Self::closure_contains_graph(ids, &shapes_graph_iri));
            let shapes_closure_ids = if do_imports && shapes_in_env {
                if data_contains_shapes {
                    info!(
                        "Data imports closure already contains shapes graph <{}>; skipping separate shapes closure resolution",
                        shapes_graph_iri
                    );
                    None
                } else {
                    Some(Self::resolve_closure_ids_for_graph(
                        &mut env,
                        shapes_graph_id.as_ref(),
                        &shapes_graph_iri,
                        import_depth,
                        "shapes",
                    )?)
                }
            } else {
                None
            };
            let merged_closure_ids = if do_imports {
                let merged = Self::merge_closure_ids_by_ontology_uri(
                    shapes_closure_ids.as_ref(),
                    data_closure_ids.as_ref(),
                );
                if merged.is_empty() {
                    None
                } else {
                    if data_contains_shapes {
                        info!(
                            "Resolving imports once from data closure ({} ontologies)",
                            merged.len()
                        );
                    } else if data_closure_ids.is_some() && shapes_closure_ids.is_some() {
                        info!(
                            "Merged shapes/data import closures with explicit shapes-source precedence ({} ontologies)",
                            merged.len()
                        );
                    }
                    Some(merged)
                }
            } else {
                None
            };

            let mut shape_graphs_for_union: Vec<NamedNode> = vec![shapes_graph_iri.clone()];
            shape_graphs_for_union.extend(shapes_quads_import_graphs);
            let shape_union_closure_ids = shapes_closure_ids.as_ref().or({
                if data_contains_shapes {
                    data_closure_ids.as_ref()
                } else {
                    None
                }
            });
            if let Some(ids) = shape_union_closure_ids {
                for id in ids {
                    let name = id.name().into_owned();
                    if name == data_graph_iri {
                        continue;
                    }
                    if !shape_graphs_for_union.iter().any(|g| g == &name) {
                        shape_graphs_for_union.push(name);
                    }
                }
            }

            let store = if do_imports {
                if let Some(ids) = &merged_closure_ids {
                    let can_reuse_env_store = Self::can_reuse_ontoenv_store_for_imports(
                        temporary_env,
                        use_shared_env,
                        shapes_closure_ids.as_ref(),
                        data_closure_ids.as_ref(),
                    );
                    if can_reuse_env_store {
                        info!(
                            "Using OntoEnv store directly for merged imports closure ({} ontologies); skipping union store materialization",
                            ids.len()
                        );
                        env.io().store().clone()
                    } else {
                        let store = Store::new().map_err(|e| {
                            Box::new(std::io::Error::other(format!(
                                "Error creating in-memory store: {}",
                                e
                            ))) as Box<dyn Error>
                        })?;
                        let union_base = if ids
                            .iter()
                            .any(|id| id.name().as_str() == shapes_graph_iri.as_str())
                        {
                            shapes_graph_iri.as_ref()
                        } else {
                            data_graph_iri.as_ref()
                        };
                        let union = env
                            .get_union_graph(ids, union_base, Some(false), Some(false))
                            .map_err(|e| {
                                Box::new(std::io::Error::other(format!(
                                    "Failed to load merged union graph for shape/data closures: {}",
                                    e
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
                        store
                    }
                } else {
                    env.io().store().clone()
                }
            } else {
                let scoped_store = Store::new().map_err(|e| {
                    Box::new(std::io::Error::other(format!(
                        "Error creating in-memory store: {}",
                        e
                    ))) as Box<dyn Error>
                })?;
                Self::copy_named_graph(env.io().store(), &scoped_store, &shapes_graph_iri)?;
                if !matches!(data_source, Source::Empty) && data_graph_iri != shapes_graph_iri {
                    Self::copy_named_graph(env.io().store(), &scoped_store, &data_graph_iri)?;
                }
                scoped_store
            };

            (
                shapes_graph_iri,
                shapes_graph_id,
                data_graph_iri,
                shape_graphs_for_union,
                merged_closure_ids,
                store,
            )
        };

        if let Some(ir) = shape_ir.as_ref() {
            shape_graphs_for_union =
                Self::graph_names_from_quads(&ir.shape_quads, &shapes_graph_iri);
            for quad in &ir.shape_quads {
                store.insert(quad).map_err(|e| {
                    Box::new(std::io::Error::other(format!(
                        "Failed to inject shape quad into store: {}",
                        e
                    ))) as Box<dyn Error>
                })?;
            }
        }

        if shape_graphs_for_union.is_empty() {
            shape_graphs_for_union.push(shapes_graph_iri.clone());
        }
        Self::dedup_graphs(&mut shape_graphs_for_union);

        Self::maybe_skolemize_graph("shape", &store, &shapes_graph_iri, skolemize_shapes)?;
        Self::maybe_skolemize_graph("data", &store, &data_graph_iri, skolemize_data)?;

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
            Source::Graph(_) | Source::Quads { .. } | Source::Empty => None,
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
            let model = ShapesModel::from_shape_ir(
                ir.clone(),
                store,
                Arc::clone(&env_handle),
                original_values,
            )?;
            (model, Arc::new(ir))
        } else {
            let model_config = BuildShapesModelConfig {
                features: features.clone(),
                strict_custom_constraints,
                original_values,
                optimize_shapes,
                optimize_shapes_data_dependent,
            };
                let model = Self::build_shapes_model(
                    Arc::clone(&env_handle),
                    store,
                    shapes_graph_iri.clone(),
                    shapes_graph_id.clone(),
                    data_graph_iri.clone(),
                    model_config,
                )?;
            let shape_ir = crate::ir::build_shape_ir(
                &model,
                Some(data_graph_iri.clone()),
                &shape_graphs_for_union,
            )
            .map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Failed to build SHACL-IR cache: {}",
                    e
                ))) as Box<dyn Error>
            })?;
            (model, Arc::new(shape_ir))
        };

        if use_shapes_graph_union && !matches!(data_source, Source::Empty) {
            Self::union_shapes_into_data_graph(
                &model.store,
                &shape_graphs_for_union,
                &data_graph_iri,
            )?;
        }

        if optimize_store {
            info!(
                "Optimizing store with shape graph <{}> and data graph <{}>",
                shapes_graph_iri, data_graph_iri
            );
            model.store.optimize().map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Error optimizing store: {}",
                    e
                )))
            })?;
            info!(
                "Finished store optimization with shape graph <{}> and data graph <{}>",
                shapes_graph_iri, data_graph_iri
            );
        } else {
            info!(
                "Skipping store optimization for shape graph <{}> and data graph <{}>",
                shapes_graph_iri, data_graph_iri
            );
        }
        let context = ValidationContext::new(
            Arc::new(model),
            data_graph_iri,
            warnings_are_errors,
            skip_sparql_blank_targets,
            shape_ir_arc,
        );
        Ok(Validator { context })
    }

    fn default_config(temporary: bool) -> Result<Config, Box<dyn Error>> {
        let root = std::env::current_dir()?;
        Config::builder()
            .root(root.clone())
            .locations(vec![root])
            .offline(false)
            .temporary(temporary)
            .use_cached_ontologies(CacheMode::Enabled)
            .build()
            .map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Failed to build OntoEnv config: {}",
                    e
                ))) as Box<dyn Error>
            })
    }

    fn open_env(config: Config, temporary_env: bool) -> Result<OntoEnv, Box<dyn Error>> {
        match OntoEnv::open_or_init(config, false) {
            Ok(env) => Ok(env),
            Err(e) if !temporary_env => {
                warn!(
                    "Failed to open existing OntoEnv ({}); falling back to temporary store",
                    e
                );
                let fallback_config = Self::default_config(true)?;
                Ok(OntoEnv::init(fallback_config, false)?)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn get_or_init_shared_env(
        config: Config,
        temporary_env: bool,
    ) -> Result<SharedEnvHandle, Box<dyn Error>> {
        if let Some(handle) = get_shared_ontoenv() {
            return Ok(handle);
        }
        let env = Self::open_env(config, temporary_env)?;
        let handle = Arc::new(RwLock::new(env));
        *shared_ontoenv_slot().lock().unwrap() = Some(handle.clone());
        Ok(handle)
    }

    fn add_source(
        env: &mut OntoEnv,
        source: &Source,
        label: &str,
        include_imports: bool,
        refresh_strategy: RefreshStrategy,
        import_depth: i32,
    ) -> Result<LoadedSource, Box<dyn Error>> {
        let file_path = match source {
            Source::File(path) => {
                let abs = if path.is_absolute() {
                    path.clone()
                } else {
                    std::env::current_dir()?.join(path)
                };
                Some(abs)
            }
            _ => None,
        };
        let fallback_location = match source {
            Source::File(path) => path.display().to_string(),
            Source::Graph(uri) => uri.clone(),
            Source::Quads { graph, quads } => {
                format!("{} ({} in-memory quads)", graph, quads.len())
            }
            Source::Empty => "<empty>".to_string(),
        };

        if let Source::Quads { graph, quads } = source {
            let normalized_graph = graph
                .strip_prefix('<')
                .and_then(|s| s.strip_suffix('>'))
                .unwrap_or(graph.as_str());
            let graph_iri = NamedNode::new(normalized_graph).map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Invalid graph IRI for {} graph {}: {}",
                    label, graph, e
                ))) as Box<dyn Error>
            })?;

            for quad in quads {
                let quad_to_insert = match &quad.graph_name {
                    GraphName::NamedNode(_) => quad.clone(),
                    _ => Quad::new(
                        quad.subject.clone(),
                        quad.predicate.clone(),
                        quad.object.clone(),
                        GraphName::NamedNode(graph_iri.clone()),
                    ),
                };
                env.io()
                    .store()
                    .insert(quad_to_insert.as_ref())
                    .map_err(|ins_err| {
                        Box::new(std::io::Error::other(format!(
                            "Failed to insert in-memory quad into {} graph {}: {}",
                            label, graph_iri, ins_err
                        ))) as Box<dyn Error>
                    })?;
            }

            debug!(
                "Loaded {} graph {} from in-memory quads ({})",
                label,
                graph_iri,
                quads.len()
            );
            info!("Added {} graph from in-memory quads: {}", label, graph_iri);
            return Ok(LoadedSource {
                graph_iri,
                in_env: false,
                graph_id: None,
            });
        }

        // Try OntoEnv first.
        let from_env = match source {
            Source::Graph(uri) => {
                if include_imports {
                    env.add(
                        OntologyLocation::Url(uri.clone()),
                        Overwrite::Allow,
                        refresh_strategy,
                    )
                } else {
                    env.add_no_imports(
                        OntologyLocation::Url(uri.clone()),
                        Overwrite::Allow,
                        refresh_strategy,
                    )
                }
            }
            Source::File(_) => {
                let location = OntologyLocation::File(
                    file_path
                        .as_ref()
                        .ok_or_else(|| std::io::Error::other("Missing file path"))?
                        .clone(),
                );
                if include_imports {
                    env.add(location, Overwrite::Allow, refresh_strategy)
                } else {
                    env.add_no_imports(location, Overwrite::Allow, refresh_strategy)
                }
            }
            Source::Empty => {
                return Err(Box::new(std::io::Error::other(format!(
                    "Empty source is not loadable for {} graph",
                    label
                ))))
            }
            Source::Quads { .. } => unreachable!("in-memory quads handled above"),
        };

        // Resolve through OntoEnv when possible.
        if let Ok(graph_id) = from_env {
            if let Ok(ontology) = env.get_ontology(&graph_id) {
                let graph_iri = ontology.name().clone();
                let location = ontology
                    .location()
                    .map(|loc| loc.as_str().to_string())
                    .unwrap_or_else(|| "<unknown>".into());
                debug!(
                    "Loaded {} graph {} (location {})",
                    label, graph_iri, location
                );
                info!("Added {} graph: {}", label, graph_iri);
                return Ok(LoadedSource {
                    graph_iri,
                    in_env: true,
                    graph_id: Some(graph_id),
                });
            }
        }

        // Fallback: manual Turtle parse into the env store (keeps the graph name stable).
        let path = match &file_path {
            Some(p) => p,
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
        let mut parsed_quads = Vec::new();
        for quad in parser.for_reader(std::io::Cursor::new(content.as_bytes())) {
            let quad = quad.map_err(|parse_err| {
                std::io::Error::other(format!(
                    "Failed to parse {} graph {}: {}",
                    label,
                    path.display(),
                    parse_err
                ))
            })?;
            let quad = Quad::new(
                quad.subject,
                quad.predicate,
                quad.object,
                GraphName::NamedNode(graph_iri.clone()),
            );
            env.io().store().insert(quad.as_ref()).map_err(|ins_err| {
                std::io::Error::other(format!(
                    "Failed to insert quad into {} graph {}: {}",
                    label,
                    path.display(),
                    ins_err
                ))
            })?;
            parsed_quads.push(quad);
        }

        if include_imports {
            // Keep fallback/manual parsing behavior aligned with OntoEnv-backed modes.
            // This follows owl:imports links directly from the loaded quads.
            let _ = Self::load_import_graphs_for_quads(
                env,
                &parsed_quads,
                import_depth,
                label,
                refresh_strategy,
            )?;
        }

        debug!(
            "Loaded {} graph {} via fallback (location {})",
            label, graph_iri, fallback_location
        );
        info!(
            "Added {} graph via fallback: {} ({})",
            label, graph_iri, fallback_location
        );

        Ok(LoadedSource {
            graph_iri,
            in_env: false,
            graph_id: None,
        })
    }

    fn extract_import_iris_from_quads(quads: &[Quad]) -> Vec<String> {
        let mut imports = Vec::new();
        let mut seen = HashSet::new();
        let owl_imports = "http://www.w3.org/2002/07/owl#imports";
        for quad in quads {
            if quad.predicate.as_str() != owl_imports {
                continue;
            }
            if let Term::NamedNode(node) = &quad.object {
                let iri = node.as_str().to_string();
                if seen.insert(iri.clone()) {
                    imports.push(iri);
                }
            }
        }
        imports
    }

    fn load_import_graphs_for_quads(
        env: &mut OntoEnv,
        quads: &[Quad],
        import_depth: i32,
        label: &str,
        refresh_strategy: RefreshStrategy,
    ) -> Result<Vec<NamedNode>, Box<dyn Error>> {
        if import_depth == 0 {
            return Ok(Vec::new());
        }

        let imports = Self::extract_import_iris_from_quads(quads);
        if imports.is_empty() {
            return Ok(Vec::new());
        }

        let mut imported_graphs = Vec::new();
        let mut seen = HashSet::new();
        let strict = env.is_strict();
        let closure_depth = if import_depth < 0 {
            -1
        } else {
            import_depth - 1
        };

        for import_iri in imports {
            let location = match OntologyLocation::from_str(&import_iri) {
                Ok(location) => location,
                Err(err) => {
                    if strict {
                        return Err(err.into());
                    }
                    warn!(
                        "Failed to parse {} import location {} from in-memory graph: {}",
                        label, import_iri, err
                    );
                    continue;
                }
            };
            let graph_id = match env.add(location, Overwrite::Allow, refresh_strategy) {
                Ok(id) => id,
                Err(err) => {
                    if strict {
                        return Err(err.into());
                    }
                    warn!(
                        "Failed to load {} import {} from in-memory graph: {}",
                        label, import_iri, err
                    );
                    continue;
                }
            };
            let imported_graph_iri = graph_id.name().into_owned();

            let graph_id = match env.resolve(ResolveTarget::Graph(imported_graph_iri.clone())) {
                Some(id) => id,
                None => {
                    let message = format!(
                        "Failed to resolve imported {} graph {} after loading",
                        label, imported_graph_iri
                    );
                    if strict {
                        return Err(std::io::Error::other(message).into());
                    }
                    warn!("{}", message);
                    continue;
                }
            };

            let closure = match env.get_closure(&graph_id, closure_depth) {
                Ok(ids) => ids,
                Err(err) => {
                    if strict {
                        return Err(err.into());
                    }
                    warn!(
                        "Failed to compute imports closure for {} import {}: {}",
                        label, imported_graph_iri, err
                    );
                    continue;
                }
            };

            for id in closure {
                let name = id.name().into_owned();
                if seen.insert(name.clone()) {
                    imported_graphs.push(name);
                }
            }
        }

        Ok(imported_graphs)
    }

    fn skolem_base(iri: &NamedNode) -> String {
        skolem::skolem_base(iri)
    }

    fn copy_named_graph(
        source: &Store,
        target: &Store,
        graph_iri: &NamedNode,
    ) -> Result<(), Box<dyn Error>> {
        for quad_res in source.quads_for_pattern(
            None,
            None,
            None,
            Some(GraphNameRef::NamedNode(graph_iri.as_ref())),
        ) {
            let quad = quad_res.map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Failed to read quad from graph {}: {}",
                    graph_iri, e
                ))) as Box<dyn Error>
            })?;
            target.insert(quad.as_ref()).map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Failed to insert quad from graph {}: {}",
                    graph_iri, e
                ))) as Box<dyn Error>
            })?;
        }
        Ok(())
    }

    fn dedup_graphs(graphs: &mut Vec<NamedNode>) {
        let mut seen = HashSet::new();
        graphs.retain(|g| seen.insert(g.clone()));
    }

    fn resolve_closure_ids_for_graph(
        env: &mut OntoEnv,
        graph_id: Option<&GraphIdentifier>,
        graph_iri: &NamedNode,
        import_depth: i32,
        label: &str,
    ) -> Result<Vec<GraphIdentifier>, Box<dyn Error>> {
        let graph_id = match graph_id {
            Some(id) => id.clone(),
            None => env
                .resolve(ResolveTarget::Graph(graph_iri.clone()))
                .ok_or_else(|| {
                    Box::new(std::io::Error::other(format!(
                        "Ontology not found for {} graph {}",
                        label, graph_iri
                    ))) as Box<dyn Error>
                })?,
        };
        let mut closure_ids = env.get_closure(&graph_id, import_depth).map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "Failed to build imports closure for {} graph {}: {}",
                label, graph_iri, e
            ))) as Box<dyn Error>
        })?;
        if !closure_ids.contains(&graph_id) {
            closure_ids.push(graph_id);
        }
        Ok(closure_ids)
    }

    fn closure_contains_graph(closure_ids: &[GraphIdentifier], graph_iri: &NamedNode) -> bool {
        closure_ids
            .iter()
            .any(|id| id.name().as_str() == graph_iri.as_str())
    }

    fn has_conflicting_graph_sources_for_same_uri(
        primary_ids: Option<&Vec<GraphIdentifier>>,
        secondary_ids: Option<&Vec<GraphIdentifier>>,
    ) -> bool {
        let mut primary_by_uri: HashMap<String, &GraphIdentifier> = HashMap::new();
        if let Some(ids) = primary_ids {
            for id in ids {
                primary_by_uri.insert(id.name().as_str().to_string(), id);
            }
        }
        if let Some(ids) = secondary_ids {
            for id in ids {
                if let Some(primary) = primary_by_uri.get(id.name().as_str()) {
                    if *primary != id {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn can_reuse_ontoenv_store_for_imports(
        temporary_env: bool,
        use_shared_env: bool,
        shapes_closure_ids: Option<&Vec<GraphIdentifier>>,
        data_closure_ids: Option<&Vec<GraphIdentifier>>,
    ) -> bool {
        if !temporary_env || use_shared_env {
            return false;
        }
        // If the same ontology URI resolves to different sources, keep materialization so
        // merged-closure source precedence stays deterministic.
        !Self::has_conflicting_graph_sources_for_same_uri(shapes_closure_ids, data_closure_ids)
    }

    /// Merge closure IDs and deduplicate by ontology URI.
    ///
    /// IDs from `preferred_closure_ids` win when both closures contain the same URI.
    fn merge_closure_ids_by_ontology_uri(
        preferred_closure_ids: Option<&Vec<GraphIdentifier>>,
        secondary_closure_ids: Option<&Vec<GraphIdentifier>>,
    ) -> Vec<GraphIdentifier> {
        let mut merged = Vec::new();
        let mut seen_uris: HashSet<String> = HashSet::new();

        for ids in [preferred_closure_ids, secondary_closure_ids]
            .into_iter()
            .flatten()
        {
            for id in ids {
                let uri = id.name().as_str().to_string();
                if seen_uris.insert(uri) {
                    merged.push(id.clone());
                }
            }
        }

        merged
    }

    fn graph_names_from_quads(quads: &[Quad], fallback: &NamedNode) -> Vec<NamedNode> {
        let mut graphs: Vec<NamedNode> = quads
            .iter()
            .filter_map(|q| match &q.graph_name {
                GraphName::NamedNode(nn) => Some(nn.clone()),
                _ => None,
            })
            .collect();

        if graphs.is_empty() {
            graphs.push(fallback.clone());
        }

        Self::dedup_graphs(&mut graphs);
        graphs
    }

    fn union_shapes_into_data_graph(
        store: &Store,
        shape_graphs: &[NamedNode],
        data_graph_iri: &NamedNode,
    ) -> Result<(), Box<dyn Error>> {
        for graph in shape_graphs {
            if graph == data_graph_iri {
                continue;
            }
            let graph_name = GraphName::NamedNode(graph.clone());
            for quad_res in store.quads_for_pattern(None, None, None, Some(graph_name.as_ref())) {
                let quad = quad_res.map_err(|e| {
                    Box::new(std::io::Error::other(format!(
                        "Failed to read quad from graph {}: {}",
                        graph, e
                    ))) as Box<dyn Error>
                })?;
                let new_quad = Quad::new(
                    quad.subject.clone(),
                    quad.predicate.clone(),
                    quad.object.clone(),
                    GraphName::NamedNode(data_graph_iri.clone()),
                );
                store.insert(&new_quad).map_err(|e| {
                    Box::new(std::io::Error::other(format!(
                        "Failed to insert quad into union data graph: {}",
                        e
                    ))) as Box<dyn Error>
                })?;
            }
        }
        Ok(())
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
        env: SharedEnvHandle,
        store: Store,
        shape_graph_iri: NamedNode,
        shape_graph_id: Option<GraphIdentifier>,
        data_graph_iri: NamedNode,
        config: BuildShapesModelConfig,
    ) -> Result<ShapesModel, Box<dyn Error>> {
        let BuildShapesModelConfig {
            features,
            strict_custom_constraints,
            original_values,
            optimize_shapes,
            optimize_shapes_data_dependent,
        } = config;
        let mut parsing_context = ParsingContext::new(
            store,
            env,
            shape_graph_iri.clone(),
            data_graph_iri.clone(),
            features.clone(),
            strict_custom_constraints,
            original_values.clone(),
        );
        shacl_parser::run_parser(&mut parsing_context)?;

        let final_ctx = if optimize_shapes {
            let mut optimizer = Optimizer::new(parsing_context);
            optimizer.optimize_shape_only()?;
            if optimize_shapes_data_dependent {
                optimizer.optimize_data_dependent()?;
            }
            optimizer.finish()
        } else {
            parsing_context
        };

        Ok(ShapesModel {
            nodeshape_id_lookup: final_ctx.nodeshape_id_lookup,
            propshape_id_lookup: final_ctx.propshape_id_lookup,
            component_id_lookup: final_ctx.component_id_lookup,
            rule_id_lookup: final_ctx.rule_id_lookup,
            store: final_ctx.store,
            shape_graph_iri: final_ctx.shape_graph_iri,
            shape_graph_id,
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
/// A configured SHACL validator instance.
///
/// Holds parsed model + runtime context for repeated validation/inference runs against
/// the same shapes + data sources.
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
        info!(
            "Starting validation with shape graph <{}> and data graph <{}>",
            self.context.model.shape_graph_iri, self.context.data_graph_iri
        );
        self.context.reset_validation_run_state();
        let report_builder = validate::validate(&self.context);
        // The report needs the context to be able to serialize itself later.
        let report = ValidationReport::new(report_builder.unwrap(), &self.context);
        info!(
            "Finished validation with shape graph <{}> and data graph <{}>; conforms={}",
            self.context.model.shape_graph_iri,
            self.context.data_graph_iri,
            report.conforms()
        );
        report
    }

    /// Returns per-component counters for graph-query operations collected during validation.
    pub fn component_graph_call_stats(&self) -> Vec<ComponentGraphCallStat> {
        self.context
            .component_graph_call_stats()
            .into_iter()
            .map(|row| {
                let descriptor = self
                    .context
                    .shape_ir()
                    .components
                    .get(&row.component_id)
                    .or_else(|| {
                        self.context
                            .model
                            .get_component_descriptor(&row.component_id)
                    });
                let component_label = descriptor
                    .map(|d| crate::runtime::build_component_from_descriptor(d).label())
                    .unwrap_or_else(|| "unknown".to_string());
                let total_nanos = row.runtime_nanos_total as f64;
                let (mean_nanos, variance_nanos) = if row.component_invocations == 0 {
                    (0.0, 0.0)
                } else {
                    let invocations_f = row.component_invocations as f64;
                    let mean_nanos = total_nanos / invocations_f;
                    let mean_sq = row.runtime_nanos_sum_squares / invocations_f;
                    let variance_nanos = (mean_sq - (mean_nanos * mean_nanos)).max(0.0);
                    (mean_nanos, variance_nanos)
                };
                ComponentGraphCallStat {
                    component_id: row.component_id,
                    source_shape: row
                        .source_shape
                        .get_term(&self.context)
                        .map(|t| t.to_string())
                        .unwrap_or_else(|| row.source_shape.to_string()),
                    component_label,
                    quads_for_pattern_calls: row.quads_for_pattern_calls,
                    execute_prepared_calls: row.execute_prepared_calls,
                    component_invocations: row.component_invocations,
                    runtime_total_ms: total_nanos / 1_000_000.0,
                    runtime_min_ms: row.runtime_nanos_min as f64 / 1_000_000.0,
                    runtime_max_ms: row.runtime_nanos_max as f64 / 1_000_000.0,
                    runtime_mean_ms: mean_nanos / 1_000_000.0,
                    runtime_stddev_ms: variance_nanos.sqrt() / 1_000_000.0,
                }
            })
            .collect()
    }

    /// Returns per-shape timing statistics for target/value selection phases.
    pub fn shape_phase_timing_stats(&self) -> Vec<ShapePhaseTimingStat> {
        self.context
            .shape_timing_stats()
            .into_iter()
            .map(|row| {
                let total_nanos = row.runtime_nanos_total as f64;
                let (mean_nanos, variance_nanos) = if row.invocations == 0 {
                    (0.0, 0.0)
                } else {
                    let invocations_f = row.invocations as f64;
                    let mean_nanos = total_nanos / invocations_f;
                    let mean_sq = row.runtime_nanos_sum_squares / invocations_f;
                    let variance_nanos = (mean_sq - (mean_nanos * mean_nanos)).max(0.0);
                    (mean_nanos, variance_nanos)
                };
                let phase = match row.phase {
                    crate::context::ShapeTimingPhase::NodeTarget => "node_target_selection",
                    crate::context::ShapeTimingPhase::NodeValue => "node_value_selection",
                    crate::context::ShapeTimingPhase::PropertyTarget => "property_target_selection",
                    crate::context::ShapeTimingPhase::PropertyValue => "property_value_selection",
                };
                ShapePhaseTimingStat {
                    source_shape: row
                        .source_shape
                        .get_term(&self.context)
                        .map(|t| t.to_string())
                        .unwrap_or_else(|| row.source_shape.to_string()),
                    phase: phase.to_string(),
                    invocations: row.invocations,
                    runtime_total_ms: total_nanos / 1_000_000.0,
                    runtime_min_ms: row.runtime_nanos_min as f64 / 1_000_000.0,
                    runtime_max_ms: row.runtime_nanos_max as f64 / 1_000_000.0,
                    runtime_mean_ms: mean_nanos / 1_000_000.0,
                    runtime_stddev_ms: variance_nanos.sqrt() / 1_000_000.0,
                }
            })
            .collect()
    }

    /// Returns per-SPARQL-query timing and row-count statistics collected during validation.
    pub fn sparql_query_call_stats(&self) -> Vec<SparqlQueryCallStat> {
        self.context
            .sparql_query_call_stats()
            .into_iter()
            .map(|row| {
                let total_nanos = row.runtime_nanos_total as f64;
                let invocations_f = row.invocations as f64;
                let (mean_nanos, variance_nanos) = if row.invocations == 0 {
                    (0.0, 0.0)
                } else {
                    let mean_nanos = total_nanos / invocations_f;
                    let mean_sq = row.runtime_nanos_sum_squares / invocations_f;
                    let variance_nanos = (mean_sq - (mean_nanos * mean_nanos)).max(0.0);
                    (mean_nanos, variance_nanos)
                };
                let rows_returned_mean = if row.invocations == 0 {
                    0.0
                } else {
                    row.rows_returned_total as f64 / invocations_f
                };

                SparqlQueryCallStat {
                    source_shape: row
                        .source_shape
                        .get_term(&self.context)
                        .map(|t| t.to_string())
                        .unwrap_or_else(|| row.source_shape.to_string()),
                    component_id: row.component_id,
                    constraint_term: row.constraint_term.to_string(),
                    query_hash: row.query_hash,
                    invocations: row.invocations,
                    rows_returned_total: row.rows_returned_total,
                    rows_returned_min: row.rows_returned_min,
                    rows_returned_max: row.rows_returned_max,
                    rows_returned_mean,
                    runtime_total_ms: total_nanos / 1_000_000.0,
                    runtime_min_ms: row.runtime_nanos_min as f64 / 1_000_000.0,
                    runtime_max_ms: row.runtime_nanos_max as f64 / 1_000_000.0,
                    runtime_mean_ms: mean_nanos / 1_000_000.0,
                    runtime_stddev_ms: variance_nanos.sqrt() / 1_000_000.0,
                }
            })
            .collect()
    }

    /// Returns the parsed SHACL-IR for the current validator.
    pub fn shape_ir(&self) -> &ShapeIR {
        self.context.shape_ir()
    }

    /// Returns a SHACL-IR copy with owl:imports resolved into the shapes graph.
    pub fn shape_ir_with_imports(&self, import_depth: i32) -> Result<ShapeIR, String> {
        let mut shape_ir = self.context.shape_ir().clone();
        let shapes_graph = shape_ir.shape_graph.clone();
        let graph_id = if let Some(id) = self.context.model.shape_graph_id.as_ref() {
            id.clone()
        } else {
            let env = self.context.model.env.read().unwrap();
            env.resolve(ResolveTarget::Graph(shapes_graph.clone()))
                .ok_or_else(|| format!("Ontology not found for shapes graph {}", shapes_graph))?
        };
        let mut closure_ids = {
            let env = self.context.model.env.read().unwrap();
            env.get_closure(&graph_id, import_depth).map_err(|e| {
                format!(
                    "Failed to build imports closure for shapes graph {}: {}",
                    shapes_graph, e
                )
            })?
        };
        if !closure_ids.contains(&graph_id) {
            closure_ids.push(graph_id);
        }
        let mut graph_names = Vec::new();
        for id in &closure_ids {
            let env = self.context.model.env.read().unwrap();
            if let Ok(ont) = env.get_ontology(id) {
                let name = ont.name().clone();
                if !graph_names.iter().any(|g| g == &name) {
                    graph_names.push(name);
                }
            }
        }
        if graph_names.is_empty() {
            graph_names.push(shapes_graph.clone());
        }
        let mut merged: HashSet<Quad> = shape_ir.shape_quads.iter().cloned().collect();
        for graph in graph_names {
            let graph_name = GraphName::NamedNode(graph.clone());
            for quad_res in self.context.model.store.quads_for_pattern(
                None,
                None,
                None,
                Some(graph_name.as_ref()),
            ) {
                let quad = quad_res.map_err(|e| e.to_string())?;
                merged.insert(quad);
            }
        }
        shape_ir.shape_quads = merged.into_iter().collect();
        Ok(shape_ir)
    }

    /// Executes inference with a custom configuration and returns the outcome.
    pub fn run_inference_with_config(
        &self,
        config: InferenceConfig,
    ) -> Result<InferenceOutcome, InferenceError> {
        info!(
            "Starting inference with shape graph <{}> and data graph <{}>",
            self.context.model.shape_graph_iri, self.context.data_graph_iri
        );
        let outcome = crate::inference::run_inference(&self.context, config);
        match &outcome {
            Ok(outcome) => info!(
                "Finished inference with shape graph <{}> and data graph <{}>; iterations={} triples_added={} converged={}",
                self.context.model.shape_graph_iri,
                self.context.data_graph_iri,
                outcome.iterations_executed,
                outcome.triples_added,
                outcome.converged
            ),
            Err(err) => warn!(
                "Finished inference with shape graph <{}> and data graph <{}>; failed with error: {}",
                self.context.model.shape_graph_iri, self.context.data_graph_iri, err
            ),
        }
        outcome
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

    /// Copies all quads from the shapes graph(s) into the data graph.
    pub fn copy_shape_graph_to_data_graph(&self) -> Result<(), String> {
        let data_graph_iri = self.context.data_graph_iri.clone();
        let store = &self.context.model.store;
        let mut graphs = HashSet::new();

        for quad in &self.context.shape_ir().shape_quads {
            if let GraphName::NamedNode(nn) = &quad.graph_name {
                graphs.insert(nn.clone());
            }
        }

        if graphs.is_empty() {
            graphs.insert(self.context.model.shape_graph_iri.clone());
        }

        for graph in graphs {
            if graph == data_graph_iri {
                continue;
            }
            let graph_name = GraphName::NamedNode(graph.clone());
            for quad_res in store.quads_for_pattern(None, None, None, Some(graph_name.as_ref())) {
                let quad = quad_res.map_err(|e| format!("Failed to read shape quad: {}", e))?;
                let new_quad = Quad::new(
                    quad.subject.clone(),
                    quad.predicate.clone(),
                    quad.object.clone(),
                    GraphName::NamedNode(data_graph_iri.clone()),
                );
                store
                    .insert(&new_quad)
                    .map_err(|e| format!("Failed to insert shape quad into data graph: {}", e))?;
            }
        }

        Ok(())
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
    use oxigraph::model::{
        GraphName, GraphNameRef, NamedNode, NamedOrBlankNode, Quad, Term, TermRef,
    };
    use std::error::Error;
    use std::fs;
    use std::io::Write;
    use std::panic;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(prefix: &str) -> Result<PathBuf, Box<dyn Error>> {
        let mut dir = std::env::temp_dir();
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        dir.push(format!("{}_{}", prefix, timestamp));
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn temp_env_config(root: &Path) -> Result<Config, Box<dyn Error>> {
        Config::builder()
            .root(root.to_path_buf())
            .locations(vec![root.to_path_buf()])
            .offline(false)
            .temporary(true)
            .build()
            .map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "Failed to build test OntoEnv config: {}",
                    e
                ))) as Box<dyn Error>
            })
    }

    fn store_has_graph(store: &Store, graph_iri: &NamedNode) -> bool {
        store
            .quads_for_pattern(
                None,
                None,
                None,
                Some(GraphNameRef::NamedNode(graph_iri.as_ref())),
            )
            .next()
            .is_some()
    }

    fn validator_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn validates_from_in_memory_quads_source() -> Result<(), Box<dyn Error>> {
        let _guard = validator_lock().lock().unwrap();

        let shape_graph = NamedNode::new("urn:shifty:test:shape")?;
        let data_graph = NamedNode::new("urn:shifty:test:data")?;
        let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")?;
        let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape")?;
        let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode")?;
        let sh_class = NamedNode::new("http://www.w3.org/ns/shacl#class")?;
        let person_shape = NamedNode::new("http://example.com/PersonShape")?;
        let alice = NamedNode::new("http://example.com/Alice")?;
        let person = NamedNode::new("http://example.com/Person")?;

        let shape_quads = vec![
            Quad::new(
                person_shape.clone(),
                rdf_type.clone(),
                sh_node_shape.clone(),
                GraphName::NamedNode(shape_graph.clone()),
            ),
            Quad::new(
                person_shape.clone(),
                sh_target_node.clone(),
                alice.clone(),
                GraphName::NamedNode(shape_graph.clone()),
            ),
            Quad::new(
                person_shape.clone(),
                sh_class.clone(),
                person.clone(),
                GraphName::NamedNode(shape_graph.clone()),
            ),
        ];
        let data_quads = vec![Quad::new(
            alice.clone(),
            rdf_type,
            person,
            GraphName::NamedNode(data_graph.clone()),
        )];

        let validator = Validator::builder()
            .with_shapes_source(Source::Quads {
                graph: shape_graph.to_string(),
                quads: shape_quads,
            })
            .with_data_source(Source::Quads {
                graph: data_graph.to_string(),
                quads: data_quads,
            })
            .with_do_imports(false)
            .build()?;

        let report = validator.validate();
        assert!(report.conforms(), "Expected validation to pass");
        Ok(())
    }

    #[test]
    fn defaults_shapes_source_to_data_source_when_omitted() -> Result<(), Box<dyn Error>> {
        let _guard = validator_lock().lock().unwrap();

        let data_graph = NamedNode::new("urn:shifty:test:data-as-shapes")?;
        let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")?;
        let sh_node_shape = NamedNode::new("http://www.w3.org/ns/shacl#NodeShape")?;
        let sh_target_node = NamedNode::new("http://www.w3.org/ns/shacl#targetNode")?;
        let sh_class = NamedNode::new("http://www.w3.org/ns/shacl#class")?;
        let person_shape = NamedNode::new("http://example.com/PersonShape")?;
        let alice = NamedNode::new("http://example.com/Alice")?;
        let person = NamedNode::new("http://example.com/Person")?;

        let merged_quads = vec![
            Quad::new(
                person_shape.clone(),
                rdf_type.clone(),
                sh_node_shape,
                GraphName::NamedNode(data_graph.clone()),
            ),
            Quad::new(
                person_shape.clone(),
                sh_target_node,
                alice.clone(),
                GraphName::NamedNode(data_graph.clone()),
            ),
            Quad::new(
                person_shape,
                sh_class,
                person.clone(),
                GraphName::NamedNode(data_graph.clone()),
            ),
            Quad::new(
                alice,
                rdf_type,
                person,
                GraphName::NamedNode(data_graph.clone()),
            ),
        ];

        let validator = Validator::builder()
            .with_data_source(Source::Quads {
                graph: data_graph.to_string(),
                quads: merged_quads,
            })
            .with_do_imports(false)
            .build()?;

        assert_eq!(
            validator.context.model.shape_graph_iri, data_graph,
            "shape graph should default to the data graph when omitted"
        );
        let report = validator.validate();
        assert!(report.conforms(), "Expected validation to pass");
        Ok(())
    }

    #[test]
    fn rejects_missing_shapes_when_data_is_empty() {
        let _guard = validator_lock().lock().unwrap();
        let err = Validator::builder()
            .with_data_source(Source::Empty)
            .build()
            .err()
            .expect("expected builder error when both shapes and data are absent");
        assert!(
            err.to_string()
                .contains("shapes source must be specified when data source is empty"),
            "unexpected error message: {}",
            err
        );
    }

    #[test]
    fn in_memory_quads_can_load_import_dependencies() -> Result<(), Box<dyn Error>> {
        let _guard = validator_lock().lock().unwrap();
        let temp_dir = unique_temp_dir("shacl_in_memory_imports")?;

        let imported_path = temp_dir.join("imported.ttl");
        fs::write(
            &imported_path,
            "<http://example.com/s> <http://example.com/p> <http://example.com/o> .\n",
        )?;
        let imported_url = format!("file://{}", imported_path.display());

        let shapes_graph = NamedNode::new("urn:shifty:test:shape-import-root")?;
        let data_graph = NamedNode::new("urn:shifty:test:data-import-root")?;
        let root_ontology = NamedNode::new("urn:shifty:test:root-ontology")?;
        let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")?;
        let owl_ontology = NamedNode::new("http://www.w3.org/2002/07/owl#Ontology")?;
        let owl_imports = NamedNode::new("http://www.w3.org/2002/07/owl#imports")?;
        let imported_node = NamedNode::new(imported_url.clone())?;

        let shape_quads = vec![
            Quad::new(
                root_ontology.clone(),
                rdf_type,
                owl_ontology,
                GraphName::NamedNode(shapes_graph.clone()),
            ),
            Quad::new(
                root_ontology,
                owl_imports,
                imported_node.clone(),
                GraphName::NamedNode(shapes_graph.clone()),
            ),
        ];

        let validator = Validator::builder()
            .with_shapes_source(Source::Quads {
                graph: shapes_graph.to_string(),
                quads: shape_quads,
            })
            .with_data_source(Source::Quads {
                graph: data_graph.to_string(),
                quads: Vec::new(),
            })
            .with_do_imports(true)
            .build()?;

        let imported_graph = NamedNode::new(imported_url)?;
        let has_imported_triples = validator
            .context
            .model
            .store
            .quads_for_pattern(
                None,
                None,
                None,
                Some(GraphNameRef::NamedNode(imported_graph.as_ref())),
            )
            .next()
            .is_some();
        assert!(
            has_imported_triples,
            "Expected imported graph triples to be loaded for in-memory source"
        );

        fs::remove_dir_all(&temp_dir)?;
        Ok(())
    }

    #[test]
    fn file_and_quads_sources_skip_imports_when_disabled() -> Result<(), Box<dyn Error>> {
        let _guard = validator_lock().lock().unwrap();
        let temp_dir = unique_temp_dir("shacl_skip_imports_parity")?;

        let imported_path = temp_dir.join("imported.ttl");
        fs::write(
            &imported_path,
            "<http://example.com/s> <http://example.com/p> <http://example.com/o> .\n",
        )?;
        let imported_url = format!("file://{}", imported_path.display());
        let imported_graph = NamedNode::new(imported_url.clone())?;

        let shapes_path = temp_dir.join("shapes.ttl");
        let shapes_ttl = format!(
            "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n<urn:shifty:test:root> a owl:Ontology ; owl:imports <{}> .\n",
            imported_url
        );
        fs::write(&shapes_path, shapes_ttl)?;

        let file_validator = Validator::builder()
            .with_shapes_source(Source::File(shapes_path))
            .with_data_source(Source::Empty)
            .with_env_config(temp_env_config(&temp_dir)?)
            .with_do_imports(false)
            .build()?;
        assert!(
            !store_has_graph(&file_validator.context.model.store, &imported_graph),
            "file source should not load imports when do_imports=false"
        );

        let shapes_graph = NamedNode::new("urn:shifty:test:shape-import-root")?;
        let root_ontology = NamedNode::new("urn:shifty:test:root-ontology")?;
        let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")?;
        let owl_ontology = NamedNode::new("http://www.w3.org/2002/07/owl#Ontology")?;
        let owl_imports = NamedNode::new("http://www.w3.org/2002/07/owl#imports")?;
        let imported_node = NamedNode::new(imported_url)?;
        let shape_quads = vec![
            Quad::new(
                root_ontology.clone(),
                rdf_type,
                owl_ontology,
                GraphName::NamedNode(shapes_graph.clone()),
            ),
            Quad::new(
                root_ontology,
                owl_imports,
                imported_node,
                GraphName::NamedNode(shapes_graph.clone()),
            ),
        ];

        let quads_validator = Validator::builder()
            .with_shapes_source(Source::Quads {
                graph: shapes_graph.to_string(),
                quads: shape_quads,
            })
            .with_data_source(Source::Empty)
            .with_env_config(temp_env_config(&temp_dir)?)
            .with_do_imports(false)
            .build()?;
        assert!(
            !store_has_graph(&quads_validator.context.model.store, &imported_graph),
            "quads source should not load imports when do_imports=false"
        );

        fs::remove_dir_all(&temp_dir)?;
        Ok(())
    }

    #[test]
    fn file_and_quads_sources_load_imports_when_enabled() -> Result<(), Box<dyn Error>> {
        let _guard = validator_lock().lock().unwrap();
        let temp_dir = unique_temp_dir("shacl_load_imports_parity")?;

        let imported_path = temp_dir.join("imported.ttl");
        fs::write(
            &imported_path,
            "<http://example.com/s> <http://example.com/p> <http://example.com/o> .\n",
        )?;
        let imported_url = format!("file://{}", imported_path.display());
        let imported_graph = NamedNode::new(imported_url.clone())?;

        let shapes_path = temp_dir.join("shapes.ttl");
        let shapes_ttl = format!(
            "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n<urn:shifty:test:root> a owl:Ontology ; owl:imports <{}> .\n",
            imported_url
        );
        fs::write(&shapes_path, shapes_ttl)?;

        let file_validator = Validator::builder()
            .with_shapes_source(Source::File(shapes_path))
            .with_data_source(Source::Empty)
            .with_env_config(temp_env_config(&temp_dir)?)
            .with_do_imports(true)
            .build()?;
        assert!(
            store_has_graph(&file_validator.context.model.store, &imported_graph),
            "file source should load imports when do_imports=true"
        );

        let shapes_graph = NamedNode::new("urn:shifty:test:shape-import-root-enabled")?;
        let root_ontology = NamedNode::new("urn:shifty:test:root-ontology-enabled")?;
        let rdf_type = NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")?;
        let owl_ontology = NamedNode::new("http://www.w3.org/2002/07/owl#Ontology")?;
        let owl_imports = NamedNode::new("http://www.w3.org/2002/07/owl#imports")?;
        let imported_node = NamedNode::new(imported_url)?;
        let shape_quads = vec![
            Quad::new(
                root_ontology.clone(),
                rdf_type,
                owl_ontology,
                GraphName::NamedNode(shapes_graph.clone()),
            ),
            Quad::new(
                root_ontology,
                owl_imports,
                imported_node,
                GraphName::NamedNode(shapes_graph.clone()),
            ),
        ];

        let quads_validator = Validator::builder()
            .with_shapes_source(Source::Quads {
                graph: shapes_graph.to_string(),
                quads: shape_quads,
            })
            .with_data_source(Source::Empty)
            .with_env_config(temp_env_config(&temp_dir)?)
            .with_do_imports(true)
            .build()?;
        assert!(
            store_has_graph(&quads_validator.context.model.store, &imported_graph),
            "quads source should load imports when do_imports=true"
        );

        fs::remove_dir_all(&temp_dir)?;
        Ok(())
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
