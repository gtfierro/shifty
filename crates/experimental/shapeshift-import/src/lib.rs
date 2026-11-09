//! Shapeshift import resolution.
//!
//! **Experimental**: this crate may change in any release, without a
//! deprecation period. It is part of the implementation of
//! [`shifty-experimental`](https://docs.rs/shifty-experimental), the entry
//! point for Shapeshift (`shifty_experimental::shapeshift`): depend on that
//! instead.
//!
//! A file declares what it depends on with `import`; resolving is a separate
//! pass, so a parsed file can list its imports without network access. This
//! crate walks the import graph from a root file and produces the
//! [`Program`] the builder consumes:
//!
//! - `.shm` files are parsed and become modules, deduplicated by their `base`
//!   (or canonical path). Cycles are allowed: every module is parsed before
//!   any name is bound.
//! - RDF graphs are loaded and lowered with shifty's SHACL front end; their
//!   shapes become referenceable and their targets keep firing. Their
//!   classes and properties feed the vocabulary checks.
//!
//! The [`Resolver`] trait is the seam for caching and pinning. [`FsResolver`]
//! reads local paths and `file:` IRIs, optionally fetches `http(s)` IRIs, and
//! takes an IRI-to-file catalog for offline builds; [`NoResolver`] resolves
//! nothing, for listing imports.

use oxrdf::Graph;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shapeshift_macro::ast::{ImportTarget, Module};
use shapeshift_macro::build::{Binding, ModuleUnit, Program, RdfUnit};
use shapeshift_macro::{Diagnostic, Vocabulary};
use shifty_algebra::{ImportKind, SourceLocation};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};

/// A fetched import.
pub enum Fetched {
    Shapeshift {
        text: String,
    },
    Rdf {
        bytes: Vec<u8>,
        content_type: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct ResolveError(pub String);

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where an import comes from, normalized: a file path or an IRI.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Location {
    File(PathBuf),
    Iri(String),
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Location::File(p) => write!(f, "{}", p.display()),
            Location::Iri(i) => write!(f, "<{i}>"),
        }
    }
}

impl Location {
    fn label(&self) -> String {
        match self {
            Location::File(p) => p.display().to_string(),
            Location::Iri(i) => i.clone(),
        }
    }

    fn is_shapeshift(&self) -> bool {
        match self {
            Location::File(p) => p.extension().is_some_and(|e| e == "shm"),
            Location::Iri(i) => i.ends_with(".shm"),
        }
    }

    fn dir(&self) -> Option<PathBuf> {
        match self {
            Location::File(p) => p.parent().map(Path::to_path_buf),
            Location::Iri(_) => None,
        }
    }
}

pub trait Resolver {
    /// Turn an import target, written in a file at `from`, into a location.
    fn locate(
        &self,
        target: &ImportTarget,
        from: Option<&Location>,
    ) -> Result<Location, ResolveError>;
    /// Fetch a location's content.
    fn fetch(&self, loc: &Location) -> Result<Fetched, ResolveError>;
}

/// Lists imports, resolves nothing.
pub struct NoResolver;

impl Resolver for NoResolver {
    fn locate(
        &self,
        target: &ImportTarget,
        from: Option<&Location>,
    ) -> Result<Location, ResolveError> {
        FsResolver::default().locate(target, from)
    }
    fn fetch(&self, loc: &Location) -> Result<Fetched, ResolveError> {
        Err(ResolveError(format!("not resolving {loc}")))
    }
}

/// Local files, `file:` IRIs, an IRI catalog, and (optionally) HTTP.
#[derive(Debug, Clone, Default)]
pub struct FsResolver {
    /// IRI -> local file, consulted before any network access.
    pub catalog: HashMap<String, PathBuf>,
    /// Allow fetching `http(s)` IRIs that are not in the catalog.
    pub allow_http: bool,
}

impl Resolver for FsResolver {
    fn locate(
        &self,
        target: &ImportTarget,
        from: Option<&Location>,
    ) -> Result<Location, ResolveError> {
        match target {
            ImportTarget::Path(p) => {
                let base = from
                    .and_then(Location::dir)
                    .unwrap_or_else(|| PathBuf::from("."));
                let joined = base.join(p);
                let canon = std::fs::canonicalize(&joined).unwrap_or(joined);
                Ok(Location::File(canon))
            }
            ImportTarget::Iri(i) => {
                if let Some(path) = self.catalog.get(i) {
                    return Ok(Location::File(path.clone()));
                }
                if let Some(rest) = i.strip_prefix("file://") {
                    return Ok(Location::File(PathBuf::from(rest)));
                }
                Ok(Location::Iri(i.clone()))
            }
        }
    }

    fn fetch(&self, loc: &Location) -> Result<Fetched, ResolveError> {
        let shm = loc.is_shapeshift();
        match loc {
            Location::File(p) => {
                let bytes = std::fs::read(p)
                    .map_err(|e| ResolveError(format!("cannot read {}: {e}", p.display())))?;
                if shm {
                    Ok(Fetched::Shapeshift {
                        text: String::from_utf8_lossy(&bytes).into_owned(),
                    })
                } else {
                    Ok(Fetched::Rdf {
                        bytes,
                        content_type: None,
                    })
                }
            }
            Location::Iri(i) => {
                if !self.allow_http {
                    return Err(ResolveError(format!(
                        "<{i}> is not available offline; map it to a local file with `--map {i}=<path>` or allow fetching with `--fetch`"
                    )));
                }
                let response = ureq::get(i)
                    .header("Accept", "text/turtle, application/rdf+xml;q=0.8, application/n-triples;q=0.7, */*;q=0.1")
                    .call()
                    .map_err(|e| ResolveError(format!("fetching <{i}>: {e}")))?;
                let content_type = response
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .map(ToOwned::to_owned);
                let mut bytes = Vec::new();
                std::io::Read::read_to_end(&mut response.into_body().into_reader(), &mut bytes)
                    .map_err(|e| ResolveError(format!("reading <{i}>: {e}")))?;
                if shm {
                    Ok(Fetched::Shapeshift {
                        text: String::from_utf8_lossy(&bytes).into_owned(),
                    })
                } else {
                    Ok(Fetched::Rdf {
                        bytes,
                        content_type,
                    })
                }
            }
        }
    }
}

/// One entry of the resolved import closure, for `shifty shape imports`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportInfo {
    pub location: Location,
    pub kind: ImportKind,
    /// Canonical identity: a module's `base`, or the location.
    pub canonical: String,
    /// SHA-256 of the fetched content, when it was fetched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// The file that first imported it.
    pub imported_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
}

/// A resolved program, plus what validation needs alongside it.
pub struct Resolved {
    pub program: Program,
    /// RDF graphs of every RDF import: the shapes view of the session, so
    /// `rdfs:subClassOf` from an imported ontology is visible to validation.
    pub graphs: Vec<Graph>,
    pub closure: Vec<ImportInfo>,
    pub diagnostics: Vec<Diagnostic>,
}

impl Resolved {
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(Diagnostic::is_error)
    }

    /// The union of every imported RDF graph.
    pub fn shapes_graph(&self) -> Graph {
        let mut g = Graph::new();
        for graph in &self.graphs {
            for t in graph.iter() {
                g.insert(t);
            }
        }
        g
    }
}

fn sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// Resolve the import closure of a root module.
///
/// `root_location` is where the root file lives (for relative imports);
/// `None` resolves relative paths against the current directory. When
/// `fetch` is false, imports are located and listed but not loaded.
pub fn resolve(
    root: Module,
    root_label: &str,
    root_location: Option<Location>,
    resolver: &dyn Resolver,
    fetch: bool,
) -> Resolved {
    let mut program = Program::default();
    let mut graphs = Vec::new();
    let mut closure = Vec::new();
    let mut diagnostics = Vec::new();
    // canonical identity -> binding
    let mut seen: HashMap<String, Binding> = HashMap::new();
    let mut locations: HashMap<Location, Binding> = HashMap::new();

    program.modules.push(ModuleUnit {
        label: root_label.to_string(),
        module: root,
        bindings: BTreeMap::new(),
        open: Vec::new(),
    });
    if let Some(loc) = &root_location {
        locations.insert(loc.clone(), Binding::Module(0));
    }
    if let Some(base) = &program.modules[0].module.base {
        seen.insert(base.clone(), Binding::Module(0));
    }
    let mut module_locs: Vec<Option<Location>> = vec![root_location];
    let mut queue: VecDeque<usize> = VecDeque::from([0]);

    while let Some(m) = queue.pop_front() {
        let imports = program.modules[m].module.imports.clone();
        let from = module_locs[m].clone();
        let label = program.modules[m].label.clone();
        let mut aliases: HashMap<String, u32> = HashMap::new();
        for imp in imports {
            let at = Some(SourceLocation {
                file: label.clone(),
                line: imp.span.line,
                column: imp.span.column,
            });
            if let Some(alias) = &imp.alias
                && let Some(prev) = aliases.insert(alias.clone(), imp.span.line)
            {
                diagnostics.push(Diagnostic::error(
                    format!("two imports bind the alias `{alias}` (the first is on line {prev})"),
                    at.clone(),
                ));
                continue;
            }
            let loc = match resolver.locate(&imp.source, from.as_ref()) {
                Ok(l) => l,
                Err(e) => {
                    diagnostics.push(Diagnostic::error(e.0, at.clone()));
                    continue;
                }
            };
            let kind = if loc.is_shapeshift() {
                ImportKind::Shapeshift
            } else {
                ImportKind::Rdf
            };
            let binding = if let Some(b) = locations.get(&loc) {
                Some(*b)
            } else if !fetch {
                closure.push(ImportInfo {
                    location: loc.clone(),
                    kind,
                    canonical: loc.label(),
                    sha256: None,
                    imported_by: label.clone(),
                    alias: imp.alias.clone(),
                });
                None
            } else {
                match resolver.fetch(&loc) {
                    Err(e) => {
                        diagnostics.push(Diagnostic::error(
                            format!("cannot import {loc}: {}", e.0),
                            at.clone(),
                        ));
                        None
                    }
                    Ok(Fetched::Shapeshift { text }) => {
                        let hash = sha256(text.as_bytes());
                        let parsed = shapeshift_parse::parse(&text, &loc.label());
                        diagnostics.extend(parsed.diagnostics);
                        let canonical = parsed.module.base.clone().unwrap_or_else(|| loc.label());
                        let b = if let Some(b) = seen.get(&canonical) {
                            *b
                        } else {
                            let idx = program.modules.len();
                            program.modules.push(ModuleUnit {
                                label: loc.label(),
                                module: parsed.module,
                                bindings: BTreeMap::new(),
                                open: Vec::new(),
                            });
                            module_locs.push(Some(loc.clone()));
                            queue.push_back(idx);
                            let b = Binding::Module(idx);
                            seen.insert(canonical.clone(), b);
                            b
                        };
                        closure.push(ImportInfo {
                            location: loc.clone(),
                            kind,
                            canonical,
                            sha256: Some(hash),
                            imported_by: label.clone(),
                            alias: imp.alias.clone(),
                        });
                        locations.insert(loc.clone(), b);
                        Some(b)
                    }
                    Ok(Fetched::Rdf {
                        bytes,
                        content_type,
                    }) => {
                        let hash = sha256(&bytes);
                        let src = loc.label();
                        match shifty_parse::load_rdf_auto(
                            &bytes,
                            content_type.as_deref(),
                            Some(&src),
                            None,
                        ) {
                            Err(e) => {
                                diagnostics.push(Diagnostic::error(
                                    format!("cannot read {loc} as RDF: {e}"),
                                    at.clone(),
                                ));
                                None
                            }
                            Ok(loaded) => {
                                let lowered = shifty_parse::parse_loaded(&loaded);
                                let errors: Vec<_> = lowered
                                    .diagnostics
                                    .iter()
                                    .filter(|d| d.level == shifty_parse::DiagLevel::Error)
                                    .collect();
                                for d in &errors {
                                    diagnostics.push(Diagnostic::error(
                                        format!("in imported graph {loc}: {d}"),
                                        at.clone(),
                                    ));
                                }
                                let others = lowered.diagnostics.len() - errors.len();
                                if others > 0 {
                                    diagnostics.push(Diagnostic::warning(
                                        format!("imported graph {loc} produced {others} lowering note{} (unsupported or ignored SHACL features)", if others == 1 { "" } else { "s" }),
                                        at.clone(),
                                    ));
                                }
                                let vocabulary = Vocabulary::from_graph(&loaded.graph);
                                let iri = match &loc {
                                    Location::Iri(i) => Some(i.clone()),
                                    Location::File(_) => match &imp.source {
                                        ImportTarget::Iri(i) => Some(i.clone()),
                                        ImportTarget::Path(_) => None,
                                    },
                                };
                                let idx = program.rdf.len();
                                program.rdf.push(RdfUnit {
                                    label: src,
                                    iri: iri.clone(),
                                    schema: lowered.schema,
                                    vocabulary,
                                });
                                graphs.push(loaded.graph);
                                let b = Binding::Rdf(idx);
                                closure.push(ImportInfo {
                                    location: loc.clone(),
                                    kind,
                                    canonical: iri.unwrap_or_else(|| loc.label()),
                                    sha256: Some(hash),
                                    imported_by: label.clone(),
                                    alias: imp.alias.clone(),
                                });
                                locations.insert(loc.clone(), b);
                                Some(b)
                            }
                        }
                    }
                }
            };
            if let Some(b) = binding {
                let unit = &mut program.modules[m];
                match &imp.alias {
                    Some(alias) => {
                        unit.bindings.insert(alias.clone(), b);
                    }
                    None => {
                        if matches!(b, Binding::Module(_)) {
                            unit.open.push(b);
                        }
                    }
                }
            }
        }
    }

    Resolved {
        program,
        graphs,
        closure,
        diagnostics,
    }
}

/// Parse a file from disk and resolve its imports.
pub fn resolve_file(
    path: &Path,
    resolver: &dyn Resolver,
    fetch: bool,
) -> Result<(Resolved, Vec<Diagnostic>), ResolveError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| ResolveError(format!("cannot read {}: {e}", path.display())))?;
    let label = path.display().to_string();
    let parsed = shapeshift_parse::parse(&text, &label);
    let loc = std::fs::canonicalize(path).ok().map(Location::File);
    let resolved = resolve(parsed.module, &label, loc, resolver, fetch);
    Ok((resolved, parsed.diagnostics))
}
