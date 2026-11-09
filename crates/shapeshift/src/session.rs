//! A Shapeshift session: definitions, data, and import settings that persist
//! across commands. The REPL is a thin loop over this; the CLI uses it for
//! one-shot commands.
//!
//! Changes are transactional: [`Session::apply`] builds the candidate program
//! and commits only if it has no errors, so a session always holds a valid
//! set of definitions. A session can be saved as `.shm` text (definitions
//! only) or as an *image*: a JSON document with the definitions as AST, the
//! data graphs, import settings, and input history, from which
//! [`Session::load_image`] restores the session exactly.

use crate::report::{Report, ReportContext, report};
use oxrdf::Graph;
use serde::{Deserialize, Serialize};
use shapeshift_import::{FsResolver, Location, Resolved, resolve};
use shapeshift_macro::ast::{Module, ShapeDecl};
use shapeshift_macro::diagnostic::Diagnostic;
use shapeshift_macro::{BuildOutput, build, display};
use shapeshift_raise::{RaiseOptions, raise};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const IMAGE_FORMAT: &str = "shapeshift-image";
pub const IMAGE_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImportSettings {
    /// IRI -> local file, consulted before any network access.
    #[serde(default)]
    pub catalog: BTreeMap<String, PathBuf>,
    /// Allow fetching `http(s)` imports.
    #[serde(default)]
    pub allow_http: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSource {
    pub label: String,
    /// The graph as N-Triples, so an image is self-contained.
    pub ntriples: String,
}

/// A saved session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Image {
    pub format: String,
    pub version: u32,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<PathBuf>,
    pub module: Module,
    #[serde(default)]
    pub data: Vec<DataSource>,
    #[serde(default)]
    pub imports: ImportSettings,
    #[serde(default)]
    pub history: Vec<String>,
}

/// The result of compiling a session's program.
pub struct Compiled {
    pub resolved: Resolved,
    pub built: BuildOutput,
    /// Resolution and build diagnostics together.
    pub diagnostics: Vec<Diagnostic>,
}

impl Compiled {
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(Diagnostic::is_error)
    }
}

/// What [`Session::apply`] did.
#[derive(Debug, Default)]
pub struct Applied {
    pub accepted: bool,
    pub defined: Vec<String>,
    pub redefined: Vec<String>,
    pub diagnostics: Vec<Diagnostic>,
}

pub struct Session {
    pub module: Module,
    pub label: String,
    /// Directory relative imports resolve against.
    pub dir: Option<PathBuf>,
    pub data: Vec<DataSource>,
    pub imports: ImportSettings,
    pub history: Vec<String>,
}

#[derive(Debug)]
pub struct SessionError(pub String);

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SessionError {}

type Result<T> = std::result::Result<T, SessionError>;

fn err(e: impl std::fmt::Display) -> SessionError {
    SessionError(e.to_string())
}

/// Merge `extra` into `base`: headers are replaced by name, shapes with the
/// same name are redefined in place, new shapes are appended.
pub fn merge_modules(base: &mut Module, extra: Module) -> (Vec<String>, Vec<String>) {
    if extra.base.is_some() {
        base.base = extra.base;
    }
    for p in extra.prefixes {
        base.prefixes.retain(|q| q.name != p.name);
        base.prefixes.push(p);
    }
    for i in extra.imports {
        if !base
            .imports
            .iter()
            .any(|j| j.source == i.source && j.alias == i.alias)
        {
            base.imports.push(i);
        }
    }
    base.header_order.clear();
    let mut defined = Vec::new();
    let mut redefined = Vec::new();
    for s in extra.shapes {
        match base.shapes.iter_mut().find(|t| t.name == s.name) {
            Some(slot) => {
                redefined.push(s.name.clone());
                *slot = s;
            }
            None => {
                defined.push(s.name.clone());
                base.shapes.push(s);
            }
        }
    }
    (defined, redefined)
}

impl Session {
    pub fn new(label: impl Into<String>) -> Self {
        Session {
            module: Module::default(),
            label: label.into(),
            dir: std::env::current_dir().ok(),
            data: Vec::new(),
            imports: ImportSettings::default(),
            history: Vec::new(),
        }
    }

    fn resolver(&self) -> FsResolver {
        FsResolver {
            catalog: self.imports.catalog.clone().into_iter().collect(),
            allow_http: self.imports.allow_http,
        }
    }

    fn root_location(&self) -> Option<Location> {
        // A virtual file in `dir`, so `import "./x.shm"` resolves next to it.
        self.dir
            .as_ref()
            .map(|d| Location::File(d.join(format!("{}.shm", sanitize(&self.label)))))
    }

    /// Resolve imports and build `module` as this session would.
    pub fn compile_module(&self, module: &Module) -> Compiled {
        let resolved = resolve(
            module.clone(),
            &self.label,
            self.root_location(),
            &self.resolver(),
            true,
        );
        let built = build(&resolved.program);
        let mut diagnostics = resolved.diagnostics.clone();
        diagnostics.extend(built.diagnostics.iter().cloned());
        Compiled {
            resolved,
            built,
            diagnostics,
        }
    }

    pub fn compile(&self) -> Compiled {
        self.compile_module(&self.module)
    }

    /// Parse `text` and merge it into the session if the result builds.
    pub fn apply(&mut self, text: &str) -> Applied {
        let parsed = shapeshift_parse::parse(text, &self.label);
        if parsed.has_errors() {
            return Applied {
                accepted: false,
                diagnostics: parsed.diagnostics,
                ..Default::default()
            };
        }
        let mut candidate = self.module.clone();
        let (defined, redefined) = merge_modules(&mut candidate, parsed.module);
        let compiled = self.compile_module(&candidate);
        let mut diagnostics = parsed.diagnostics;
        diagnostics.extend(compiled.diagnostics);
        let accepted = !diagnostics.iter().any(Diagnostic::is_error);
        if accepted {
            self.module = candidate;
            self.history.push(text.to_string());
        }
        Applied {
            accepted,
            defined,
            redefined,
            diagnostics,
        }
    }

    /// Load a `.shm` file into the session (merging its definitions).
    pub fn load_shm(&mut self, path: &Path) -> Result<Applied> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| err(format!("cannot read {}: {e}", path.display())))?;
        if self.module.shapes.is_empty() && self.module.imports.is_empty() {
            // First file: take its directory and name, so relative imports
            // and diagnostics refer to it.
            if let Some(parent) = std::fs::canonicalize(path)
                .ok()
                .and_then(|p| p.parent().map(Path::to_path_buf))
            {
                self.dir = Some(parent);
            }
            self.label = path.display().to_string();
        }
        Ok(self.apply(&text))
    }

    /// Remove a shape by name; refused if something else still uses it.
    pub fn remove(&mut self, name: &str) -> Applied {
        let mut candidate = self.module.clone();
        let before = candidate.shapes.len();
        candidate.shapes.retain(|s| s.name != name);
        if candidate.shapes.len() == before {
            return Applied {
                accepted: false,
                diagnostics: vec![Diagnostic::error(format!("no shape named `{name}`"), None)],
                ..Default::default()
            };
        }
        let compiled = self.compile_module(&candidate);
        let accepted = !compiled.has_errors();
        if accepted {
            self.module = candidate;
        }
        Applied {
            accepted,
            diagnostics: compiled.diagnostics,
            ..Default::default()
        }
    }

    /// Add a data graph (any RDF syntax shifty reads).
    pub fn add_data(&mut self, path: &Path) -> Result<usize> {
        let bytes =
            std::fs::read(path).map_err(|e| err(format!("cannot read {}: {e}", path.display())))?;
        let label = path.display().to_string();
        let loaded = shifty_parse::load_rdf_auto(&bytes, None, Some(&label), None).map_err(err)?;
        let n = loaded.graph.len();
        let ntriples: String = loaded.graph.iter().map(|t| format!("{t} .\n")).collect();
        self.data.retain(|d| d.label != label);
        self.data.push(DataSource { label, ntriples });
        Ok(n)
    }

    pub fn clear_data(&mut self) {
        self.data.clear();
    }

    pub fn data_graph(&self) -> Result<Graph> {
        let mut g = Graph::new();
        for d in &self.data {
            let loaded = shifty_parse::load_ntriples(d.ntriples.as_bytes()).map_err(err)?;
            for t in loaded.graph.iter() {
                g.insert(t);
            }
        }
        Ok(g)
    }

    /// Validate the session's data against its definitions.
    pub fn validate(&self) -> Result<Report> {
        let compiled = self.compile();
        if compiled.has_errors() {
            return Err(err(shapeshift_macro::diagnostic::render_all(
                &compiled.diagnostics,
            )));
        }
        validate_compiled(&compiled, &self.data_graph()?)
    }

    /// Raise the session's definitions to SHACL Turtle.
    pub fn export(&self, core: bool) -> Result<(String, Vec<String>)> {
        let compiled = self.compile();
        if compiled.has_errors() {
            return Err(err(shapeshift_macro::diagnostic::render_all(
                &compiled.diagnostics,
            )));
        }
        let raised = raise(&compiled.built, RaiseOptions { core }).map_err(err)?;
        Ok((raised.to_turtle(), raised.warnings))
    }

    /// The definitions as canonical `.shm` text.
    pub fn source(&self) -> String {
        display::format_module(&self.module)
    }

    pub fn shape(&self, name: &str) -> Option<&ShapeDecl> {
        self.module.shapes.iter().find(|s| s.name == name)
    }

    /// Source, algebra, and SHACL of one shape.
    pub fn explain(&self, name: &str) -> Result<String> {
        let decl = self
            .shape(name)
            .ok_or_else(|| err(format!("no shape named `{name}`")))?;
        let mut one = self.module.clone();
        one.shapes = vec![decl.clone()];
        one.header_order.clear();
        let source = display::format_module(&Module {
            shapes: vec![decl.clone()],
            ..Default::default()
        });
        let compiled = self.compile();
        let mut out = format!("-- source\n{source}");
        if let Some(slot) = compiled.built.shapes.get(name) {
            let s = &compiled.built.schema;
            let body = match s.arena.get(*slot) {
                shifty_algebra::Shape::Annotated { shape, .. } => *shape,
                _ => *slot,
            };
            out.push_str("\n-- algebra\n");
            out.push_str(&shifty_algebra::render::describe_shape_pretty(
                &s.arena,
                body,
                &s.prefixes,
                88,
            ));
            out.push('\n');
            for st in &s.statements {
                if st.shape == *slot {
                    out.push_str(&format!(
                        "for {}\n",
                        shifty_algebra::render::selector_to_string_in_px(
                            &st.selector,
                            &s.arena,
                            &s.prefixes
                        )
                    ));
                }
            }
        }
        Ok(out)
    }

    pub fn save_source(&self, path: &Path) -> Result<()> {
        std::fs::write(path, self.source())
            .map_err(|e| err(format!("cannot write {}: {e}", path.display())))
    }

    pub fn image(&self) -> Image {
        Image {
            format: IMAGE_FORMAT.into(),
            version: IMAGE_VERSION,
            label: self.label.clone(),
            dir: self.dir.clone(),
            module: self.module.clone(),
            data: self.data.clone(),
            imports: self.imports.clone(),
            history: self.history.clone(),
        }
    }

    pub fn save_image(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(&self.image()).map_err(err)?;
        std::fs::write(path, json).map_err(|e| err(format!("cannot write {}: {e}", path.display())))
    }

    pub fn from_image(image: Image) -> Result<Session> {
        if image.format != IMAGE_FORMAT {
            return Err(err(format!(
                "not a Shapeshift image (format `{}`)",
                image.format
            )));
        }
        if image.version > IMAGE_VERSION {
            return Err(err(format!(
                "image version {} is newer than this shifty understands ({IMAGE_VERSION})",
                image.version
            )));
        }
        Ok(Session {
            module: image.module,
            label: image.label,
            dir: image.dir,
            data: image.data,
            imports: image.imports,
            history: image.history,
        })
    }

    pub fn load_image(path: &Path) -> Result<Session> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| err(format!("cannot read {}: {e}", path.display())))?;
        let image: Image = serde_json::from_str(&text)
            .map_err(|e| err(format!("{} is not a valid image: {e}", path.display())))?;
        Session::from_image(image)
    }
}

/// Validate data against an already compiled program.
pub fn validate_compiled(compiled: &Compiled, data: &Graph) -> Result<Report> {
    let shapes = compiled.resolved.shapes_graph();
    let outcome =
        shifty_engine::validate_graphs(data, &shapes, &compiled.built.schema).map_err(err)?;
    let ctx = ReportContext {
        built: &compiled.built,
        data,
        shapes: &shapes,
    };
    Ok(report(&ctx, &outcome))
}

fn sanitize(label: &str) -> String {
    Path::new(label)
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| {
            s.chars()
                .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
                .collect()
        })
        .filter(|s: &String| !s.is_empty())
        .unwrap_or_else(|| "repl".into())
}
