use crate::diagnostics::SourceRef;
use ontoenv::api::{OntoEnv, ResolveTarget};
use ontoenv::config::Config;
use ontoenv::ontology::{GraphIdentifier, OntologyLocation};
use ontoenv::options::{CacheMode, Overwrite, RefreshStrategy};
use oxigraph::model::GraphNameRef;
use oxrdf::{GraphName, NamedNode, Quad};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::error::Error;
use std::path::PathBuf;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShapeSource {
    File(PathBuf),
    Url(String),
    Quads {
        graph_iri: NamedNode,
        quads: Vec<Quad>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefreshMode {
    UseCache,
    Force,
}

impl RefreshMode {
    fn ontoenv(self) -> RefreshStrategy {
        match self {
            Self::UseCache => RefreshStrategy::UseCache,
            Self::Force => RefreshStrategy::Force,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceLoadOptions {
    pub include_imports: bool,
    pub import_depth: i32,
    pub temporary_env: bool,
    pub refresh_mode: RefreshMode,
}

impl Default for SourceLoadOptions {
    fn default() -> Self {
        Self {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedGraph {
    pub graph_iri: NamedNode,
    pub source: SourceRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedShapeSet {
    pub root_graphs: Vec<LoadedGraph>,
    pub imported_graphs: Vec<LoadedGraph>,
    pub quads: Vec<Quad>,
}

impl ResolvedShapeSet {
    pub fn all_graphs(&self) -> Vec<&LoadedGraph> {
        self.root_graphs
            .iter()
            .chain(self.imported_graphs.iter())
            .collect()
    }
}

pub fn load_with_ontoenv(
    sources: &[ShapeSource],
    options: &SourceLoadOptions,
) -> Result<ResolvedShapeSet, Box<dyn Error>> {
    let root = std::env::current_dir()?;
    let config = Config::builder()
        .root(root.clone())
        .locations(vec![root])
        .offline(false)
        .temporary(options.temporary_env)
        .use_cached_ontologies(CacheMode::Enabled)
        .build()?;

    let mut env = match OntoEnv::open_or_init(config.clone(), false) {
        Ok(env) => env,
        Err(_) if !options.temporary_env => OntoEnv::init(config, false)?,
        Err(err) => return Err(err.into()),
    };

    let mut root_graphs = Vec::new();
    let mut imported_graphs = Vec::new();
    let mut graph_names = Vec::new();
    let mut seen_graphs = HashSet::new();
    let mut inline_quads = Vec::new();

    for source in sources {
        match source {
            ShapeSource::Quads { graph_iri, quads } => {
                if seen_graphs.insert(graph_iri.clone()) {
                    graph_names.push(graph_iri.clone());
                    root_graphs.push(LoadedGraph {
                        graph_iri: graph_iri.clone(),
                        source: SourceRef {
                            graph_iri: graph_iri.as_str().to_string(),
                            locator: Some(graph_iri.as_str().to_string()),
                            is_root: true,
                        },
                    });
                }
                for quad in quads {
                    inline_quads.push(match &quad.graph_name {
                        GraphName::NamedNode(_) => quad.clone(),
                        _ => Quad::new(
                            quad.subject.clone(),
                            quad.predicate.clone(),
                            quad.object.clone(),
                            GraphName::NamedNode(graph_iri.clone()),
                        ),
                    });
                }
            }
            ShapeSource::File(path) => {
                let location = OntologyLocation::File(if path.is_absolute() {
                    path.clone()
                } else {
                    std::env::current_dir()?.join(path)
                });
                let graph_id = add_location(&mut env, &location, options)?;
                register_graph(
                    &env,
                    &graph_id,
                    true,
                    path.display().to_string(),
                    &mut root_graphs,
                    &mut graph_names,
                    &mut seen_graphs,
                )?;
                collect_imports(
                    &mut env,
                    &graph_id,
                    options.import_depth,
                    &mut imported_graphs,
                    &mut graph_names,
                    &mut seen_graphs,
                )?;
            }
            ShapeSource::Url(url) => {
                let location = OntologyLocation::Url(url.clone());
                let graph_id = add_location(&mut env, &location, options)?;
                register_graph(
                    &env,
                    &graph_id,
                    true,
                    url.clone(),
                    &mut root_graphs,
                    &mut graph_names,
                    &mut seen_graphs,
                )?;
                collect_imports(
                    &mut env,
                    &graph_id,
                    options.import_depth,
                    &mut imported_graphs,
                    &mut graph_names,
                    &mut seen_graphs,
                )?;
            }
        }
    }

    let mut quads = inline_quads;
    for graph in graph_names {
        for quad in env.io().store().quads_for_pattern(
            None,
            None,
            None,
            Some(GraphNameRef::NamedNode(graph.as_ref())),
        ) {
            quads.push(quad?);
        }
    }

    Ok(ResolvedShapeSet {
        root_graphs,
        imported_graphs,
        quads,
    })
}

fn add_location(
    env: &mut OntoEnv,
    location: &OntologyLocation,
    options: &SourceLoadOptions,
) -> Result<GraphIdentifier, Box<dyn Error>> {
    let graph_id = if options.include_imports {
        env.add(
            location.clone(),
            Overwrite::Allow,
            options.refresh_mode.ontoenv(),
        )?
    } else {
        env.add_no_imports(
            location.clone(),
            Overwrite::Allow,
            options.refresh_mode.ontoenv(),
        )?
    };
    Ok(graph_id)
}

fn register_graph(
    env: &OntoEnv,
    graph_id: &GraphIdentifier,
    is_root: bool,
    locator: String,
    sink: &mut Vec<LoadedGraph>,
    graph_names: &mut Vec<NamedNode>,
    seen_graphs: &mut HashSet<NamedNode>,
) -> Result<(), Box<dyn Error>> {
    let ontology = env.get_ontology(graph_id)?;
    let graph_iri = ontology.name().clone();
    if seen_graphs.insert(graph_iri.clone()) {
        graph_names.push(graph_iri.clone());
        sink.push(LoadedGraph {
            graph_iri: graph_iri.clone(),
            source: SourceRef {
                graph_iri: graph_iri.as_str().to_string(),
                locator: Some(locator),
                is_root,
            },
        });
    }
    Ok(())
}

fn collect_imports(
    env: &mut OntoEnv,
    graph_id: &GraphIdentifier,
    import_depth: i32,
    imported_graphs: &mut Vec<LoadedGraph>,
    graph_names: &mut Vec<NamedNode>,
    seen_graphs: &mut HashSet<NamedNode>,
) -> Result<(), Box<dyn Error>> {
    if import_depth == 0 {
        return Ok(());
    }
    let mut closure = env.get_closure(graph_id, import_depth)?;
    if !closure.contains(graph_id) {
        closure.push(graph_id.clone());
    }
    for imported in closure {
        if imported == *graph_id {
            continue;
        }
        if let Some(resolved) = env.resolve(ResolveTarget::Graph(imported.name().into_owned())) {
            let ontology = env.get_ontology(&resolved)?;
            let graph_iri = ontology.name().clone();
            if seen_graphs.insert(graph_iri.clone()) {
                graph_names.push(graph_iri.clone());
                imported_graphs.push(LoadedGraph {
                    graph_iri: graph_iri.clone(),
                    source: SourceRef {
                        graph_iri: graph_iri.as_str().to_string(),
                        locator: ontology.location().map(|loc| loc.as_str().to_string()),
                        is_root: false,
                    },
                });
            }
        }
    }
    Ok(())
}

pub fn source_from_str(value: &str) -> ShapeSource {
    match OntologyLocation::from_str(value) {
        Ok(OntologyLocation::Url(_)) => ShapeSource::Url(value.to_string()),
        _ => ShapeSource::File(PathBuf::from(value)),
    }
}
