use oxrdf::{Graph, Term, Triple};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use shifty_engine::{
    CompiledShapes, EngineOptions, EvaluationSession, FindingOptions, SessionData, SessionOptions,
    UnsupportedPolicy, ValidationGraphMode, ValidationOptions, ValidationReport, report_to_graph,
};
use std::path::PathBuf;
use std::sync::OnceLock;

mod repair;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn finding_options(options: &ValidationOptions) -> FindingOptions {
    FindingOptions {
        entry_shape_names: options.entry_shape_names.clone(),
        minimum_severity: options.minimum_severity.clone(),
        sort_results: options.sort_results,
    }
}

fn session_diagnostics(compiled: &CompiledShapes, session: &EvaluationSession) -> Vec<String> {
    compiled
        .diagnostics()
        .iter()
        .map(ToString::to_string)
        .chain(
            session
                .diagnostics()
                .iter()
                .map(|diagnostic| diagnostic.message.clone()),
        )
        .collect()
}

fn compiled_session(
    data: InputSpec,
    shapes: Option<InputSpec>,
    base: Option<&str>,
    mode: ValidationGraphMode,
    run_infer: bool,
    engine: EngineOptions,
) -> Result<(CompiledShapes, EvaluationSession), String> {
    let data_loaded = data.load(base)?;
    let shapes_loaded = shapes.map(|input| input.load(base)).transpose()?;
    compiled_session_loaded(data_loaded, shapes_loaded, mode, run_infer, engine)
}

fn compiled_session_loaded(
    data_loaded: shifty_parse::Loaded,
    shapes_loaded: Option<shifty_parse::Loaded>,
    mode: ValidationGraphMode,
    run_infer: bool,
    engine: EngineOptions,
) -> Result<(CompiledShapes, EvaluationSession), String> {
    let (source, session_data) = if let Some(source) = shapes_loaded {
        check_explicit_shapes_not_empty(Some(&source))?;
        (source, SessionData::Separate(data_loaded.graph))
    } else {
        (data_loaded, SessionData::Embedded)
    };
    let compiled = CompiledShapes::compile(source).map_err(|error| error.to_string())?;
    let session = compiled
        .session(
            session_data,
            SessionOptions {
                graph_mode: mode,
                inference: run_infer,
                engine,
            },
        )
        .map_err(|error| error.to_string())?;
    Ok((compiled, session))
}

// ── Algebra-path types ────────────────────────────────────────────────────────

/// Debugging detail for a failed `sh:sparql`/custom SPARQL-based constraint
/// component: what query ran, what it was bound to, and what it returned, so
/// a SPARQL failure is never a dead end.
#[pyclass(get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct SparqlDiagnostic {
    /// The query actually executed, after every static SHACL substitution
    /// (`$PATH`/`$currentShape`/`$shapesGraph`/custom-component parameters).
    /// `$this` is left as a free variable here; its value is the first entry
    /// of `bindings`.
    pub query: String,
    /// `(name, value)` SHACL prebindings applied before execution, in
    /// application order.
    pub bindings: Vec<(String, String)>,
    /// The solution rows the query actually produced: one entry per row, each
    /// a `(name, value)` list in projection order. Empty for `ASK` queries.
    pub results: Vec<Vec<(String, String)>>,
    /// Why native lowering did not apply, when known. `None` when the query
    /// ran natively, or when it's a custom constraint component (always
    /// opaque today).
    pub fallback_reason: Option<String>,
}

#[pymethods]
impl SparqlDiagnostic {
    fn __repr__(&self) -> String {
        format!(
            "SparqlDiagnostic(query={:?}, results={})",
            self.query,
            self.results.len()
        )
    }
}

/// Render a [`SparqlDiagnostic`] as indented, human-readable text, appended
/// after a violation's other fields in both the algebra and W3C text reports.
fn format_sparql_diagnostic(d: &SparqlDiagnostic, indent: &str) -> String {
    render_sparql_diagnostic_parts(
        indent,
        &d.query,
        &d.bindings,
        &d.results,
        &d.fallback_reason,
    )
}

/// Like [`format_sparql_diagnostic`] but over the engine's own
/// `shifty_engine::SparqlDiagnostic` (used by the W3C text report, which reads
/// `ValidationResult` directly rather than through the pyclass wrapper).
fn format_engine_sparql_diagnostic(d: &shifty_engine::SparqlDiagnostic, indent: &str) -> String {
    let bindings: Vec<(String, String)> = d
        .bindings
        .iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect();
    let results: Vec<Vec<(String, String)>> = d
        .results
        .iter()
        .map(|row| {
            row.iter()
                .map(|(k, v)| (k.clone(), v.to_string()))
                .collect()
        })
        .collect();
    render_sparql_diagnostic_parts(indent, &d.query, &bindings, &results, &d.fallback_reason)
}

/// Shared renderer for both [`format_sparql_diagnostic`] and
/// [`format_engine_sparql_diagnostic`], which differ only in where their
/// `query`/`bindings`/`results`/`fallback_reason` come from.
fn render_sparql_diagnostic_parts(
    indent: &str,
    query: &str,
    bindings: &[(String, String)],
    results: &[Vec<(String, String)>],
    fallback_reason: &Option<String>,
) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(out, "{indent}SPARQL:");
    let _ = writeln!(out, "{indent}  Query:");
    for line in query.lines() {
        let _ = writeln!(out, "{indent}    {line}");
    }
    if !bindings.is_empty() {
        let _ = writeln!(out, "{indent}  Bound:");
        for (k, v) in bindings {
            let _ = writeln!(out, "{indent}    ${k} = {v}");
        }
    }
    if !results.is_empty() {
        let _ = writeln!(out, "{indent}  Results:");
        for (i, row) in results.iter().enumerate() {
            if row.is_empty() {
                let _ = writeln!(out, "{indent}    [{}] (no projected variables)", i + 1);
                continue;
            }
            let cols = row
                .iter()
                .map(|(k, v)| format!("?{k} = {v}"))
                .collect::<Vec<_>>()
                .join(", ");
            let _ = writeln!(out, "{indent}    [{}] {cols}", i + 1);
        }
    }
    if let Some(reason) = fallback_reason {
        let _ = writeln!(out, "{indent}  Did not use the native executor: {reason}");
    }
    out
}

fn sparql_diagnostic_to_py(
    py: Python<'_>,
    d: &shifty_engine::SparqlDiagnostic,
) -> PyResult<Py<SparqlDiagnostic>> {
    Py::new(
        py,
        SparqlDiagnostic {
            query: d.query.clone(),
            bindings: d
                .bindings
                .iter()
                .map(|(k, v)| (k.clone(), v.to_string()))
                .collect(),
            results: d
                .results
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|(k, v)| (k.clone(), v.to_string()))
                        .collect()
                })
                .collect(),
            fallback_reason: d.fallback_reason.clone(),
        },
    )
}

#[pyclass(eq, eq_int, hash, frozen, name = "ConstraintKind", skip_from_py_object)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ConstraintKind {
    Unknown,
    Top,
    Constant,
    ClassMembership,
    ValueType,
    NodeKind,
    Closed,
    Equals,
    Disjoint,
    LessThan,
    LessThanOrEquals,
    UniqueLang,
    Negation,
    Conjunction,
    Disjunction,
    Cardinality,
    Sparql,
    Expression,
}

pub(crate) fn constraint_kind_to_py(kind: shifty_algebra::ConstraintKind) -> ConstraintKind {
    match kind {
        shifty_algebra::ConstraintKind::Unknown => ConstraintKind::Unknown,
        shifty_algebra::ConstraintKind::Top => ConstraintKind::Top,
        shifty_algebra::ConstraintKind::Constant => ConstraintKind::Constant,
        shifty_algebra::ConstraintKind::ClassMembership => ConstraintKind::ClassMembership,
        shifty_algebra::ConstraintKind::ValueType => ConstraintKind::ValueType,
        shifty_algebra::ConstraintKind::NodeKind => ConstraintKind::NodeKind,
        shifty_algebra::ConstraintKind::Closed => ConstraintKind::Closed,
        shifty_algebra::ConstraintKind::Equals => ConstraintKind::Equals,
        shifty_algebra::ConstraintKind::Disjoint => ConstraintKind::Disjoint,
        shifty_algebra::ConstraintKind::LessThan => ConstraintKind::LessThan,
        shifty_algebra::ConstraintKind::LessThanOrEquals => ConstraintKind::LessThanOrEquals,
        shifty_algebra::ConstraintKind::UniqueLang => ConstraintKind::UniqueLang,
        shifty_algebra::ConstraintKind::Negation => ConstraintKind::Negation,
        shifty_algebra::ConstraintKind::Conjunction => ConstraintKind::Conjunction,
        shifty_algebra::ConstraintKind::Disjunction => ConstraintKind::Disjunction,
        shifty_algebra::ConstraintKind::Cardinality => ConstraintKind::Cardinality,
        shifty_algebra::ConstraintKind::Sparql => ConstraintKind::Sparql,
        shifty_algebra::ConstraintKind::Expression => ConstraintKind::Expression,
    }
}

#[pyclass(name = "Constraint", skip_from_py_object)]
#[derive(Clone)]
pub struct Constraint {
    /// Algebra arena id for this constraint.
    #[pyo3(get)]
    pub id: u32,
    /// Stable semantic operator kind.
    #[pyo3(get)]
    pub kind: ConstraintKind,
    /// One-level algebra rendering. Child constraints appear as `@id`.
    #[pyo3(get)]
    pub render: String,
    /// Fully-expanded human description, on one line.
    #[pyo3(get)]
    pub definition: String,
    /// The same description laid out over several lines and indented by nesting
    /// depth. Identical to `definition` whenever that already fits on a line, so
    /// a caller can render this unconditionally; only a genuinely nested
    /// constraint comes back broken.
    #[pyo3(get)]
    pub definition_pretty: String,
    shape: shifty_algebra::Shape,
}

#[pymethods]
impl Constraint {
    /// JSON serialization of the algebra node, computed when requested.
    #[getter]
    fn json(&self) -> String {
        serde_json::to_string(&self.shape)
            .unwrap_or_else(|error| format!("{{\"error\":\"{error}\"}}"))
    }

    fn __repr__(&self) -> String {
        format!(
            "Constraint(id={}, kind={:?}, render={:?})",
            self.id, self.kind, self.render
        )
    }

    fn __str__(&self) -> String {
        self.definition.clone()
    }
}

pub(crate) fn constraint_to_py(
    py: Python<'_>,
    arena: &shifty_algebra::ShapeArena,
    px: &shifty_algebra::Prefixes,
    id: shifty_algebra::ShapeId,
) -> PyResult<Py<Constraint>> {
    Py::new(
        py,
        Constraint {
            id: id.0,
            kind: constraint_kind_to_py(shifty_algebra::ConstraintKind::of(arena, id)),
            render: shifty_algebra::render::shape_to_string_in(arena, id, px),
            definition: shifty_algebra::render::describe_shape_in(arena, id, px),
            definition_pretty: shifty_algebra::render::describe_shape_pretty(
                arena,
                id,
                px,
                shifty_algebra::render::PRETTY_WIDTH,
            ),
            shape: arena.get(id).clone(),
        },
    )
}

#[pyclass(get_all)]
pub struct Reason {
    /// The node at which the constraint failed.
    pub value: String,
    /// Path from the focus node to the value, in π notation (e.g. `ex:name`).
    pub path: Option<String>,
    /// Engine-generated description of the failing constraint — always present.
    pub message: String,
    /// The source shape's `sh:message`, if the author supplied one (with
    /// `{$this}`/`{?var}` resolved). `None` otherwise. Prefer this over
    /// `message` when it is set.
    pub author_message: Option<String>,
    /// SHACL severity (`"Violation"`, `"Warning"`, `"Info"`, or a custom IRI).
    pub severity: String,
    /// The complete originating algebraic constraint/operator.
    pub constraint: Py<Constraint>,
    /// Stable semantic operator kind of `constraint`.
    pub constraint_kind: ConstraintKind,
    /// Algebra arena id of `constraint`.
    pub constraint_id: u32,
    /// Statement id shared with repair witnesses.
    pub statement_id: usize,
    /// For a cardinality constraint, how many values along the path satisfied
    /// the qualifier. The bound it had to meet is in `constraint` itself, so
    /// this is the one number a report needs that the algebra does not carry —
    /// state the shortfall from these two rather than parsing `message`.
    /// `None` for every other constraint kind.
    pub observed_count: Option<u64>,
    /// Present only for a failed `sh:sparql`/custom SPARQL-based constraint
    /// component. `None` for every other failed constraint.
    pub sparql_diagnostic: Option<Py<SparqlDiagnostic>>,
}

#[pymethods]
impl Reason {
    fn __repr__(&self) -> String {
        format!("Reason(value={:?}, message={:?})", self.value, self.message)
    }
}

#[pyclass]
pub struct Violation {
    #[pyo3(get)]
    pub focus_node: String,
    /// Statement id shared with repair witnesses.
    #[pyo3(get)]
    pub statement_id: usize,
    /// Algebra arena id for the statement's top-level shape.
    #[pyo3(get)]
    pub constraint_id: u32,
    /// Named shape IRI if the violated statement was a named SHACL shape.
    #[pyo3(get)]
    pub shape_name: Option<String>,
    /// Most severe reason in this grouped finding.
    #[pyo3(get)]
    pub severity: String,
    reasons: Vec<Py<Reason>>,
}

#[pymethods]
impl Violation {
    #[getter]
    fn reasons(&self, py: Python<'_>) -> Vec<Py<Reason>> {
        self.reasons.iter().map(|r| r.clone_ref(py)).collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "Violation(focus_node={:?}, shape={:?}, reasons={})",
            self.focus_node,
            self.shape_name,
            self.reasons.len()
        )
    }
}

#[pyclass]
pub struct AlgebraResult {
    #[pyo3(get)]
    pub conforms: bool,
    #[pyo3(get)]
    pub diagnostics: Vec<String>,
    violations: Vec<Py<Violation>>,
    results_text_cache: OnceLock<String>,
    /// Triples added by SHACL-AF inference before validation ran. Only an
    /// in-place caller keeps them for write-back into its data graph.
    pub inferred: Vec<Triple>,
    pub inferred_ntriples_cache: OnceLock<String>,
}

#[pymethods]
impl AlgebraResult {
    /// Serialize the retained inference delta for the Python write-back path.
    #[getter]
    fn _inferred_ntriples(&self, py: Python<'_>) -> String {
        py.detach(|| {
            self.inferred_ntriples_cache
                .get_or_init(|| triples_to_ntriples(&self.inferred))
                .clone()
        })
    }

    #[getter]
    fn violations(&self, py: Python<'_>) -> Vec<Py<Violation>> {
        self.violations.iter().map(|v| v.clone_ref(py)).collect()
    }

    fn __bool__(&self) -> bool {
        self.conforms
    }

    fn __repr__(&self) -> String {
        if self.conforms {
            "AlgebraResult(conforms=True)".to_string()
        } else {
            format!(
                "AlgebraResult(conforms=False, violations={})",
                self.violations.len()
            )
        }
    }

    #[getter]
    fn results_text(&self, py: Python<'_>) -> String {
        self.results_text_cache
            .get_or_init(|| {
                if self.conforms {
                    return "Validation Report\nConforms: True".to_string();
                }
                let mut out = String::from("Validation Report\nConforms: False\n");
                for v in &self.violations {
                    let v = v.borrow(py);
                    let shape = v.shape_name.as_deref().unwrap_or("<anonymous>");
                    out.push_str(&format!(
                        "\n{} result in {} ({}):\n",
                        v.severity, shape, v.focus_node
                    ));
                    for r in &v.reasons {
                        let r = r.borrow(py);
                        if let Some(path) = &r.path {
                            out.push_str(&format!("  Path: {path}\n"));
                        }
                        out.push_str(&format!("  Severity: {}\n", r.severity));
                        out.push_str(&format!("  Value: {}\n", r.value));
                        out.push_str(&format!("  Message: {}\n", r.message));
                        if let Some(d) = &r.sparql_diagnostic {
                            let d = d.borrow(py);
                            out.push_str(&format_sparql_diagnostic(&d, "  "));
                        }
                    }
                }
                out
            })
            .clone()
    }
}

// ── W3C-report-path types ────────────────────────────────────────────────────

#[pyclass]
pub struct W3cResult {
    /// Whether the data graph conforms to all shapes.
    #[pyo3(get)]
    pub conforms: bool,
    #[pyo3(get)]
    pub diagnostics: Vec<String>,
    /// The `sh:ValidationReport` serialized as Turtle.
    #[pyo3(get)]
    pub report_turtle: String,
    /// Human-readable summary (pyshacl-esque text).
    #[pyo3(get)]
    pub results_text: String,
    /// Triples added by SHACL-AF inference before validation ran. Only an
    /// in-place caller keeps them for write-back into its data graph.
    pub inferred: Vec<Triple>,
    pub inferred_ntriples_cache: OnceLock<String>,
}

#[pymethods]
impl W3cResult {
    /// Serialize the retained inference delta for the Python write-back path.
    #[getter]
    fn _inferred_ntriples(&self, py: Python<'_>) -> String {
        py.detach(|| {
            self.inferred_ntriples_cache
                .get_or_init(|| triples_to_ntriples(&self.inferred))
                .clone()
        })
    }

    fn __bool__(&self) -> bool {
        self.conforms
    }

    fn __repr__(&self) -> String {
        if self.conforms {
            "W3cResult(conforms=True)".to_string()
        } else {
            "W3cResult(conforms=False)".to_string()
        }
    }
}

// ── Property witness types ───────────────────────────────────────────────────

/// The observed binding of one `sh:property` shape at one *conforming* focus
/// node — the inverse of a violation: not what failed, but what a passing
/// property shape's `sh:path` actually resolved to.
#[pyclass(get_all)]
pub struct PropertyWitness {
    /// The focus node (e.g. an equipment IRI) that conformed.
    pub focus: String,
    /// The node shape (application profile) `focus` conformed to.
    pub shape: String,
    /// A stable id for the `sh:property` shape: the lexical value reached by
    /// evaluating `key_path` from the property shape's own node (over the
    /// shapes graph) when it resolves to a value, otherwise the property
    /// shape's own IRI/blank-node id.
    pub key: String,
    /// The `sh:path` value nodes, deduped and rendered in full (`<iri>`,
    /// `_:label`, `"lit"`, `"lit"@lang`, `"lit"^^<datatype>`) so IRI and
    /// literal bindings stay distinguishable. Narrowed to the
    /// `sh:qualifiedValueShape` matches when the property shape declares one.
    pub values: Vec<String>,
}

#[pymethods]
impl PropertyWitness {
    fn __repr__(&self) -> String {
        format!(
            "PropertyWitness(focus={:?}, key={:?}, values={})",
            self.focus,
            self.key,
            self.values.len()
        )
    }
}

fn property_witness_to_py(w: shifty_engine::PropertyWitness) -> PropertyWitness {
    PropertyWitness {
        focus: w.focus.to_string(),
        shape: w.shape.to_string(),
        key: term_text(&w.key),
        values: w.values.iter().map(ToString::to_string).collect(),
    }
}

// ── Inference types ───────────────────────────────────────────────────────────

#[pyclass]
pub struct InferResult {
    inferred_count: usize,
    diagnostics: Vec<String>,
    graph: Graph,
    /// The triples inference added, i.e. `graph` minus the original data —
    /// kept separately so callers can write just the delta back into a
    /// caller-owned graph instead of re-materializing everything.
    inferred: Vec<Triple>,
    graph_ntriples_cache: OnceLock<String>,
    inferred_ntriples_cache: OnceLock<String>,
}

#[pymethods]
impl InferResult {
    #[getter]
    fn inferred_count(&self) -> usize {
        self.inferred_count
    }

    #[getter]
    fn diagnostics(&self) -> Vec<String> {
        self.diagnostics.clone()
    }

    #[getter]
    fn graph_ntriples(&self, py: Python<'_>) -> String {
        py.detach(|| {
            self.graph_ntriples_cache
                .get_or_init(|| graph_to_ntriples(&self.graph))
                .clone()
        })
    }

    /// Just the newly inferred triples (not the original data), as
    /// N-Triples. Empty when nothing was inferred.
    #[getter]
    fn inferred_ntriples(&self, py: Python<'_>) -> String {
        py.detach(|| {
            self.inferred_ntriples_cache
                .get_or_init(|| triples_to_ntriples(&self.inferred))
                .clone()
        })
    }

    fn __repr__(&self) -> String {
        format!("InferResult(inferred={})", self.inferred_count)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

pub(crate) fn parse_mode(mode: &str) -> Result<ValidationGraphMode, String> {
    match mode {
        "data" => Ok(ValidationGraphMode::Data),
        "union" => Ok(ValidationGraphMode::Union),
        "union-all" => Ok(ValidationGraphMode::UnionAll),
        other => Err(format!(
            "unknown graph_mode {other:?}; expected 'data', 'union', or 'union-all'"
        )),
    }
}

pub(crate) fn parse_minimum_severity(value: &str) -> Result<shifty_algebra::Severity, String> {
    match value.to_ascii_lowercase().as_str() {
        "info" => Ok(shifty_algebra::Severity::Info),
        "warning" => Ok(shifty_algebra::Severity::Warning),
        "violation" => Ok(shifty_algebra::Severity::Violation),
        _ => Err(format!(
            "unknown minimum_severity {value:?}; expected 'info', 'warning', or 'violation'"
        )),
    }
}

/// Parse the `on_unsupported` kwarg into an [`UnsupportedPolicy`]: `"ignore"`
/// (best-effort, the default) or `"error"`/`"strict"` (fail loudly).
fn parse_unsupported_policy(value: &str) -> Result<UnsupportedPolicy, String> {
    match value.to_ascii_lowercase().as_str() {
        "ignore" | "lenient" => Ok(UnsupportedPolicy::Ignore),
        "error" | "strict" => Ok(UnsupportedPolicy::Error),
        _ => Err(format!(
            "unknown on_unsupported {value:?}; expected 'ignore' or 'error'"
        )),
    }
}

/// Build [`EngineOptions`] from the `on_unsupported` kwarg.
fn engine_options(on_unsupported: &str) -> Result<EngineOptions, String> {
    Ok(EngineOptions {
        unsupported: parse_unsupported_policy(on_unsupported)?,
    })
}

#[derive(Debug, Clone, Copy)]
enum InputFormat {
    Auto,
    Turtle,
    NTriples,
}

impl InputFormat {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "auto" | "" => Ok(Self::Auto),
            "turtle" => Ok(Self::Turtle),
            "nt" | "ntriples" => Ok(Self::NTriples),
            other => Err(format!(
                "unknown RDF format {other:?}; expected 'auto', 'turtle', or 'nt'"
            )),
        }
    }
}

enum InputSource {
    Bytes(PyBackedBytes),
    Path(PathBuf),
}

pub(crate) struct InputSpec {
    source: InputSource,
    format: InputFormat,
}

impl InputSpec {
    pub(crate) fn new(
        data: Option<PyBackedBytes>,
        path: Option<String>,
        format: &str,
        label: &str,
    ) -> Result<Self, String> {
        let source = match (data, path) {
            (Some(data), None) => InputSource::Bytes(data),
            (None, Some(path)) => InputSource::Path(path.into()),
            (Some(_), Some(_)) => {
                return Err(format!("{label} must provide bytes or a path, not both"));
            }
            (None, None) => return Err(format!("{label} is required")),
        };
        Ok(Self {
            source,
            format: InputFormat::parse(format)?,
        })
    }

    pub(crate) fn load(&self, base: Option<&str>) -> Result<shifty_parse::Loaded, String> {
        match &self.source {
            InputSource::Bytes(data) => match self.format {
                InputFormat::Auto => shifty_parse::load_rdf_auto(data, None, None, base),
                InputFormat::Turtle => shifty_parse::load_turtle(data, base),
                InputFormat::NTriples => shifty_parse::load_ntriples(data),
            }
            .map_err(|e| format!("parse error: {e}")),
            InputSource::Path(path) => match self.format {
                InputFormat::Auto => {
                    let bytes = std::fs::read(path)
                        .map_err(|e| format!("failed to open {}: {e}", path.display()))?;
                    shifty_parse::load_rdf_auto(
                        &bytes,
                        None,
                        path.to_str(),
                        base.or_else(|| path.to_str()),
                    )
                    .map_err(|e| format!("parse error: {e}"))
                }
                InputFormat::Turtle => {
                    shifty_parse::Loaded::from_path(path, shifty_parse::RdfFormat::Turtle, base)
                        .map_err(|e| format!("parse error: {e}"))
                }
                InputFormat::NTriples => {
                    shifty_parse::Loaded::from_path(path, shifty_parse::RdfFormat::NTriples, base)
                        .map_err(|e| format!("parse error: {e}"))
                }
            },
        }
    }
}

pub(crate) fn py_value_error(message: String) -> PyErr {
    PyValueError::new_err(message)
}

/// Validation uses the inferred graph when requested, but only an in-place
/// caller needs to retain the delta after evaluation.
#[derive(Clone, Copy)]
struct ValidationInference {
    run: bool,
    keep_delta: bool,
}

/// Serialize any borrowed-triple iterable (a `&Graph`, a `&[Triple]`, ...) as
/// N-Triples. Shared by `graph_to_ntriples` (a whole graph) and callers that
/// only need to serialize a delta (e.g. `InferResult::inferred_ntriples`).
pub(crate) fn triples_to_ntriples<'a, T: Into<oxrdf::TripleRef<'a>>>(
    triples: impl IntoIterator<Item = T>,
) -> String {
    let mut writer = oxttl::NTriplesSerializer::new().for_writer(Vec::new());
    for triple in triples {
        writer.serialize_triple(triple).unwrap();
    }
    // NTriplesSerializer::finish() returns the writer (Vec<u8>) directly, not Result
    let bytes = writer.finish();
    String::from_utf8(bytes).unwrap()
}

pub(crate) fn graph_to_ntriples(graph: &Graph) -> String {
    triples_to_ntriples(graph)
}

fn graph_to_turtle(graph: &Graph) -> String {
    let ser = oxttl::TurtleSerializer::new()
        .with_prefix("sh", "http://www.w3.org/ns/shacl#")
        .unwrap()
        .with_prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        .unwrap()
        .with_prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
        .unwrap()
        .with_prefix("xsd", "http://www.w3.org/2001/XMLSchema#")
        .unwrap();
    let bytes = graph
        .iter()
        .try_fold(ser.for_writer(Vec::new()), |mut s, triple| {
            s.serialize_triple(triple).map(|()| s)
        })
        .unwrap()
        .finish()
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

fn term_text(term: &Term) -> String {
    match term {
        Term::Literal(lit) => lit.value().to_string(),
        other => other.to_string(),
    }
}

fn format_report_text(report: &ValidationReport) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    writeln!(out, "Validation Report").unwrap();
    writeln!(
        out,
        "Conforms: {}",
        if report.conforms { "True" } else { "False" }
    )
    .unwrap();
    if report.results.is_empty() {
        return out;
    }
    writeln!(out, "Results ({}):", report.results.len()).unwrap();
    for r in &report.results {
        let component_local = r
            .component
            .as_str()
            .rsplit_once('#')
            .or_else(|| r.component.as_str().rsplit_once('/'))
            .map(|(_, local)| local)
            .unwrap_or(r.component.as_str());
        writeln!(out, "Constraint Violation in {component_local}").unwrap();
        let severity_local = r
            .severity
            .as_str()
            .rsplit_once('#')
            .or_else(|| r.severity.as_str().rsplit_once('/'))
            .map(|(_, local)| local)
            .unwrap_or(r.severity.as_str());
        writeln!(out, "  Severity: sh:{severity_local}").unwrap();
        writeln!(out, "  Source Shape: {}", r.source_shape).unwrap();
        writeln!(out, "  Focus Node: {}", r.focus).unwrap();
        if let Some(path) = &r.path {
            writeln!(out, "  Result Path: {path}").unwrap();
        }
        if let Some(value) = &r.value {
            writeln!(out, "  Value: {value}").unwrap();
        }
        for msg in &r.messages {
            writeln!(out, "  Message: {}", term_text(msg)).unwrap();
        }
        if let Some(d) = &r.sparql_diagnostic {
            out.push_str(&format_engine_sparql_diagnostic(d, "  "));
        }
        writeln!(out).unwrap();
    }
    out
}

fn build_w3c_result(
    report: &ValidationReport,
    report_graph: &Graph,
    inferred: Option<Vec<Triple>>,
    diagnostics: Vec<String>,
) -> W3cResult {
    W3cResult {
        conforms: report.conforms,
        diagnostics,
        report_turtle: graph_to_turtle(report_graph),
        results_text: format_report_text(report),
        inferred: inferred.unwrap_or_default(),
        inferred_ntriples_cache: OnceLock::new(),
    }
}

pub(crate) fn shape_name_for(
    v: &shifty_engine::Violation,
    schema: &shifty_algebra::Schema,
) -> Option<String> {
    let shape_id = schema.statements.get(v.statement)?.shape;
    schema.name_of(shape_id).map(str::to_string)
}

/// Build a Python [`Violation`] from an engine violation (shared by the
/// validation and repair-gate paths).
pub(crate) fn violation_to_py(
    py: Python<'_>,
    v: &shifty_engine::Violation,
    schema: &shifty_algebra::Schema,
) -> PyResult<Py<Violation>> {
    violation_to_py_with_arena(py, v, schema, &schema.arena)
}

/// Constraint *text* is compacted against the schema's vocabulary. Node identity
/// — `focus_node`, `value` — stays absolute: callers match those against IRIs
/// they hold (`failure_for(focus)`), and a compacted form is not resolvable
/// without the prefix table alongside it.
pub(crate) fn violation_to_py_with_arena(
    py: Python<'_>,
    v: &shifty_engine::Violation,
    schema: &shifty_algebra::Schema,
    arena: &shifty_algebra::ShapeArena,
) -> PyResult<Py<Violation>> {
    let reasons = v
        .reasons
        .iter()
        .map(|r| {
            let constraint = constraint_to_py(py, arena, &schema.prefixes, r.constraint_id)?;
            let sparql_diagnostic = r
                .sparql_diagnostic
                .as_ref()
                .map(|d| sparql_diagnostic_to_py(py, d))
                .transpose()?;
            Py::new(
                py,
                Reason {
                    value: r.value.to_string(),
                    path: r.path.clone(),
                    message: r.message.clone(),
                    author_message: r.author_message.clone(),
                    severity: r.severity.label().to_string(),
                    constraint,
                    constraint_kind: constraint_kind_to_py(r.constraint_kind),
                    constraint_id: r.constraint_id.0,
                    statement_id: r.statement_id,
                    observed_count: r.observed_count,
                    sparql_diagnostic,
                },
            )
        })
        .collect::<PyResult<Vec<_>>>()?;
    Py::new(
        py,
        Violation {
            focus_node: v.focus.to_string(),
            statement_id: v.statement,
            constraint_id: schema
                .statements
                .get(v.statement)
                .map(|statement| statement.shape.0)
                .unwrap_or(u32::MAX),
            shape_name: shape_name_for(v, schema),
            severity: v.severity.label().to_string(),
            reasons,
        },
    )
}

struct RawSparqlDiagnostic {
    query: String,
    bindings: Vec<(String, String)>,
    results: Vec<Vec<(String, String)>>,
    fallback_reason: Option<String>,
}

impl RawSparqlDiagnostic {
    fn from_engine(d: &shifty_engine::SparqlDiagnostic) -> Self {
        Self {
            query: d.query.clone(),
            bindings: d
                .bindings
                .iter()
                .map(|(k, v)| (k.clone(), v.to_string()))
                .collect(),
            results: d
                .results
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|(k, v)| (k.clone(), v.to_string()))
                        .collect()
                })
                .collect(),
            fallback_reason: d.fallback_reason.clone(),
        }
    }

    fn into_python(self, py: Python<'_>) -> PyResult<Py<SparqlDiagnostic>> {
        Py::new(
            py,
            SparqlDiagnostic {
                query: self.query,
                bindings: self.bindings,
                results: self.results,
                fallback_reason: self.fallback_reason,
            },
        )
    }
}

struct RawConstraint {
    id: u32,
    kind: ConstraintKind,
    render: String,
    definition: String,
    definition_pretty: String,
    shape: shifty_algebra::Shape,
}

impl RawConstraint {
    fn from_arena(
        arena: &shifty_algebra::ShapeArena,
        px: &shifty_algebra::Prefixes,
        id: shifty_algebra::ShapeId,
    ) -> Self {
        Self {
            id: id.0,
            kind: constraint_kind_to_py(shifty_algebra::ConstraintKind::of(arena, id)),
            render: shifty_algebra::render::shape_to_string_in(arena, id, px),
            definition: shifty_algebra::render::describe_shape_in(arena, id, px),
            definition_pretty: shifty_algebra::render::describe_shape_pretty(
                arena,
                id,
                px,
                shifty_algebra::render::PRETTY_WIDTH,
            ),
            shape: arena.get(id).clone(),
        }
    }

    fn into_python(self, py: Python<'_>) -> PyResult<Py<Constraint>> {
        Py::new(
            py,
            Constraint {
                id: self.id,
                kind: self.kind,
                render: self.render,
                definition: self.definition,
                definition_pretty: self.definition_pretty,
                shape: self.shape,
            },
        )
    }
}

struct RawReason {
    value: String,
    path: Option<String>,
    message: String,
    author_message: Option<String>,
    severity: String,
    constraint: RawConstraint,
    constraint_kind: ConstraintKind,
    constraint_id: u32,
    statement_id: usize,
    observed_count: Option<u64>,
    sparql_diagnostic: Option<RawSparqlDiagnostic>,
}

struct RawViolation {
    focus_node: String,
    statement_id: usize,
    constraint_id: u32,
    shape_name: Option<String>,
    severity: String,
    reasons: Vec<RawReason>,
}

struct RawAlgebraResult {
    conforms: bool,
    diagnostics: Vec<String>,
    violations: Vec<RawViolation>,
    inferred: Vec<Triple>,
}

impl RawAlgebraResult {
    fn into_python(self, py: Python<'_>) -> PyResult<AlgebraResult> {
        let violations = self
            .violations
            .into_iter()
            .map(|violation| {
                let reasons = violation
                    .reasons
                    .into_iter()
                    .map(|reason| {
                        let sparql_diagnostic = reason
                            .sparql_diagnostic
                            .map(|d| d.into_python(py))
                            .transpose()?;
                        Py::new(
                            py,
                            Reason {
                                value: reason.value,
                                path: reason.path,
                                message: reason.message,
                                author_message: reason.author_message,
                                severity: reason.severity,
                                constraint: reason.constraint.into_python(py)?,
                                constraint_kind: reason.constraint_kind,
                                constraint_id: reason.constraint_id,
                                statement_id: reason.statement_id,
                                observed_count: reason.observed_count,
                                sparql_diagnostic,
                            },
                        )
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                Py::new(
                    py,
                    Violation {
                        focus_node: violation.focus_node,
                        statement_id: violation.statement_id,
                        constraint_id: violation.constraint_id,
                        shape_name: violation.shape_name,
                        severity: violation.severity,
                        reasons,
                    },
                )
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(AlgebraResult {
            conforms: self.conforms,
            diagnostics: self.diagnostics,
            violations,
            results_text_cache: OnceLock::new(),
            inferred: self.inferred,
            inferred_ntriples_cache: OnceLock::new(),
        })
    }
}

fn raw_algebra_result(
    outcome: shifty_engine::ValidationOutcome,
    schema: &shifty_algebra::Schema,
    arena: &shifty_algebra::ShapeArena,
    inferred: Option<Vec<Triple>>,
    diagnostics: Vec<String>,
) -> RawAlgebraResult {
    let violations = outcome
        .violations
        .iter()
        .map(|violation| RawViolation {
            focus_node: violation.focus.to_string(),
            statement_id: violation.statement,
            constraint_id: schema
                .statements
                .get(violation.statement)
                .map(|statement| statement.shape.0)
                .unwrap_or(u32::MAX),
            shape_name: shape_name_for(violation, schema),
            severity: violation.severity.label().to_string(),
            reasons: violation
                .reasons
                .iter()
                .map(|reason| RawReason {
                    value: reason.value.to_string(),
                    path: reason.path.clone(),
                    message: reason.message.clone(),
                    author_message: reason.author_message.clone(),
                    severity: reason.severity.label().to_string(),
                    constraint: RawConstraint::from_arena(
                        arena,
                        &schema.prefixes,
                        reason.constraint_id,
                    ),
                    constraint_kind: constraint_kind_to_py(reason.constraint_kind),
                    observed_count: reason.observed_count,
                    constraint_id: reason.constraint_id.0,
                    statement_id: reason.statement_id,
                    sparql_diagnostic: reason
                        .sparql_diagnostic
                        .as_ref()
                        .map(RawSparqlDiagnostic::from_engine),
                })
                .collect(),
        })
        .collect();
    RawAlgebraResult {
        conforms: outcome.conforms,
        diagnostics,
        violations,
        inferred: inferred.unwrap_or_default(),
    }
}

fn check_explicit_shapes_not_empty(shapes: Option<&shifty_parse::Loaded>) -> Result<(), String> {
    if shapes.is_some_and(|loaded| loaded.graph.is_empty()) {
        return Err(
            "explicit shapes graph is empty; omit the shapes argument or pass None to use \
             shapes embedded in the data graph"
                .to_string(),
        );
    }
    Ok(())
}

// ── Exported functions ────────────────────────────────────────────────────────

/// Return the pyshifty package version.
#[pyfunction]
pub fn version() -> &'static str {
    VERSION
}

/// Run algebra-path validation. Returns an `AlgebraResult` with structured
/// `Violation`/`Reason` objects representing the algebraic failure AST.
/// `entry_shape_names`, when set, limits validation to those named shapes as
/// top-level entry points while preserving normal dependency evaluation.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    data=None,
    data_path=None,
    data_format="auto",
    shapes=None,
    shapes_path=None,
    shapes_format="auto",
    graph_mode="union",
    entry_shape_names=None,
    run_infer=true,
    minimum_severity="info",
    sort_results=true,
    on_unsupported="ignore",
    base=None,
    keep_inferred=false
))]
pub fn _validate_algebra(
    py: Python<'_>,
    data: Option<PyBackedBytes>,
    data_path: Option<String>,
    data_format: &str,
    shapes: Option<PyBackedBytes>,
    shapes_path: Option<String>,
    shapes_format: &str,
    graph_mode: &str,
    entry_shape_names: Option<Vec<String>>,
    run_infer: bool,
    minimum_severity: &str,
    sort_results: bool,
    on_unsupported: &str,
    base: Option<String>,
    keep_inferred: bool,
) -> PyResult<AlgebraResult> {
    let data = InputSpec::new(data, data_path, data_format, "data").map_err(py_value_error)?;
    let shapes = match (shapes, shapes_path) {
        (None, None) => None,
        (data, path) => {
            Some(InputSpec::new(data, path, shapes_format, "shapes").map_err(py_value_error)?)
        }
    };
    let mode = parse_mode(graph_mode).map_err(py_value_error)?;
    let options = ValidationOptions {
        minimum_severity: parse_minimum_severity(minimum_severity).map_err(py_value_error)?,
        sort_results,
        entry_shape_names: entry_shape_names.unwrap_or_default(),
        engine: engine_options(on_unsupported).map_err(py_value_error)?,
    };
    let inference = ValidationInference {
        run: run_infer,
        keep_delta: keep_inferred,
    };
    let raw = py
        .detach(move || {
            let (compiled, session) = compiled_session(
                data,
                shapes,
                base.as_deref(),
                mode,
                inference.run,
                options.engine,
            )?;
            let outcome = session.validate(&finding_options(&options));
            Ok(raw_algebra_result(
                outcome,
                compiled.normalized_schema(),
                &compiled.normalized_schema().arena,
                inference.keep_delta.then(|| session.inferred().to_vec()),
                session_diagnostics(&compiled, &session),
            ))
        })
        .map_err(py_value_error)?;
    raw.into_python(py)
}

/// Run W3C-report-path validation. Returns a `W3cResult` whose `report_turtle`
/// is a full `sh:ValidationReport` Turtle document (same as pyshacl's second
/// return value) and `results_text` is a human-readable summary.
/// `entry_shape_names`, when set, limits validation to those named shapes as
/// top-level entry points while preserving normal dependency evaluation.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    data=None,
    data_path=None,
    data_format="auto",
    shapes=None,
    shapes_path=None,
    shapes_format="auto",
    graph_mode="union",
    entry_shape_names=None,
    run_infer=true,
    minimum_severity="info",
    sort_results=true,
    on_unsupported="ignore",
    base=None,
    keep_inferred=false
))]
pub fn _validate_w3c(
    py: Python<'_>,
    data: Option<PyBackedBytes>,
    data_path: Option<String>,
    data_format: &str,
    shapes: Option<PyBackedBytes>,
    shapes_path: Option<String>,
    shapes_format: &str,
    graph_mode: &str,
    entry_shape_names: Option<Vec<String>>,
    run_infer: bool,
    minimum_severity: &str,
    sort_results: bool,
    on_unsupported: &str,
    base: Option<String>,
    keep_inferred: bool,
) -> PyResult<W3cResult> {
    let data = InputSpec::new(data, data_path, data_format, "data").map_err(py_value_error)?;
    let shapes = match (shapes, shapes_path) {
        (None, None) => None,
        (data, path) => {
            Some(InputSpec::new(data, path, shapes_format, "shapes").map_err(py_value_error)?)
        }
    };
    let mode = parse_mode(graph_mode).map_err(py_value_error)?;
    let options = ValidationOptions {
        minimum_severity: parse_minimum_severity(minimum_severity).map_err(py_value_error)?,
        sort_results,
        entry_shape_names: entry_shape_names.unwrap_or_default(),
        engine: engine_options(on_unsupported).map_err(py_value_error)?,
    };
    let inference = ValidationInference {
        run: run_infer,
        keep_delta: keep_inferred,
    };
    py.detach(move || {
        let (compiled, session) = compiled_session(
            data,
            shapes,
            base.as_deref(),
            mode,
            inference.run,
            options.engine,
        )?;
        let report = session.report(&finding_options(&options));
        let report_graph = report_to_graph(&report);
        Ok(build_w3c_result(
            &report,
            &report_graph,
            inference.keep_delta.then(|| session.inferred().to_vec()),
            session_diagnostics(&compiled, &session),
        ))
    })
    .map_err(py_value_error)
}

/// Run SHACL-AF forward-chaining rules to a fixed point. Returns an
/// `InferResult` with the full graph (as N-Triples) and inferred triple count.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    data=None,
    data_path=None,
    data_format="auto",
    shapes=None,
    shapes_path=None,
    shapes_format="auto",
    on_unsupported="ignore",
    base=None
))]
pub fn _infer(
    py: Python<'_>,
    data: Option<PyBackedBytes>,
    data_path: Option<String>,
    data_format: &str,
    shapes: Option<PyBackedBytes>,
    shapes_path: Option<String>,
    shapes_format: &str,
    on_unsupported: &str,
    base: Option<String>,
) -> PyResult<InferResult> {
    let data = InputSpec::new(data, data_path, data_format, "data").map_err(py_value_error)?;
    let shapes = match (shapes, shapes_path) {
        (None, None) => None,
        (data, path) => {
            Some(InputSpec::new(data, path, shapes_format, "shapes").map_err(py_value_error)?)
        }
    };
    let engine = engine_options(on_unsupported).map_err(py_value_error)?;
    py.detach(move || {
        let data_loaded = data.load(base.as_deref())?;
        let shapes_loaded = shapes
            .map(|input| input.load(base.as_deref()))
            .transpose()?;
        if shapes_loaded
            .as_ref()
            .is_some_and(|source| source.graph.is_empty())
        {
            return Ok(InferResult {
                inferred_count: 0,
                diagnostics: Vec::new(),
                graph: data_loaded.graph,
                inferred: Vec::new(),
                graph_ntriples_cache: OnceLock::new(),
                inferred_ntriples_cache: OnceLock::new(),
            });
        }
        let (_, session) = compiled_session_loaded(
            data_loaded,
            shapes_loaded,
            ValidationGraphMode::Data,
            true,
            engine,
        )?;
        let inferred = session.inferred().to_vec();
        Ok(InferResult {
            inferred_count: inferred.len(),
            diagnostics: session
                .diagnostics()
                .iter()
                .map(|diagnostic| diagnostic.message.clone())
                .collect(),
            graph: session.data().clone(),
            inferred,
            graph_ntriples_cache: OnceLock::new(),
            inferred_ntriples_cache: OnceLock::new(),
        })
    })
    .map_err(py_value_error)
}

#[pyclass]
pub struct PreparedValidator {
    compiled: CompiledShapes,
    base: Option<String>,
}

#[pymethods]
impl PreparedValidator {
    #[new]
    #[pyo3(signature = (
        shapes=None,
        shapes_path=None,
        shapes_format="auto",
        base=None
    ))]
    fn new(
        py: Python<'_>,
        shapes: Option<PyBackedBytes>,
        shapes_path: Option<String>,
        shapes_format: &str,
        base: Option<String>,
    ) -> PyResult<Self> {
        let input =
            InputSpec::new(shapes, shapes_path, shapes_format, "shapes").map_err(py_value_error)?;
        py.detach({
            let base = base.clone();
            move || {
                let loaded = input.load(base.as_deref())?;
                check_explicit_shapes_not_empty(Some(&loaded))?;
                let compiled =
                    CompiledShapes::compile(loaded).map_err(|error| error.to_string())?;
                Ok(Self { compiled, base })
            }
        })
        .map_err(py_value_error)
    }

    #[getter]
    fn diagnostics(&self) -> Vec<String> {
        self.compiled
            .diagnostics()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[pyo3(signature = (
        data=None,
        data_path=None,
        data_format="auto",
        graph_mode="union",
        entry_shape_names=None,
        run_infer=true,
        minimum_severity="info",
        sort_results=true,
        on_unsupported="ignore",
        keep_inferred=false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn validate_algebra(
        &self,
        py: Python<'_>,
        data: Option<PyBackedBytes>,
        data_path: Option<String>,
        data_format: &str,
        graph_mode: &str,
        entry_shape_names: Option<Vec<String>>,
        run_infer: bool,
        minimum_severity: &str,
        sort_results: bool,
        on_unsupported: &str,
        keep_inferred: bool,
    ) -> PyResult<AlgebraResult> {
        let data = InputSpec::new(data, data_path, data_format, "data").map_err(py_value_error)?;
        let mode = parse_mode(graph_mode).map_err(py_value_error)?;
        let options = ValidationOptions {
            minimum_severity: parse_minimum_severity(minimum_severity).map_err(py_value_error)?,
            sort_results,
            entry_shape_names: entry_shape_names.unwrap_or_default(),
            engine: engine_options(on_unsupported).map_err(py_value_error)?,
        };
        let inference = ValidationInference {
            run: run_infer,
            keep_delta: keep_inferred,
        };
        let raw = py
            .detach(|| {
                let data_loaded = data.load(self.base.as_deref())?;
                let session = self
                    .compiled
                    .session(
                        SessionData::Separate(data_loaded.graph),
                        SessionOptions {
                            graph_mode: mode,
                            inference: inference.run,
                            engine: options.engine,
                        },
                    )
                    .map_err(|error| error.to_string())?;
                let outcome = session.validate(&finding_options(&options));
                Ok(raw_algebra_result(
                    outcome,
                    self.compiled.normalized_schema(),
                    &self.compiled.normalized_schema().arena,
                    inference.keep_delta.then(|| session.inferred().to_vec()),
                    session_diagnostics(&self.compiled, &session),
                ))
            })
            .map_err(py_value_error)?;
        raw.into_python(py)
    }

    #[pyo3(signature = (
        data=None,
        data_path=None,
        data_format="auto",
        graph_mode="union",
        entry_shape_names=None,
        run_infer=true,
        minimum_severity="info",
        sort_results=true,
        on_unsupported="ignore",
        keep_inferred=false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn validate_w3c(
        &self,
        py: Python<'_>,
        data: Option<PyBackedBytes>,
        data_path: Option<String>,
        data_format: &str,
        graph_mode: &str,
        entry_shape_names: Option<Vec<String>>,
        run_infer: bool,
        minimum_severity: &str,
        sort_results: bool,
        on_unsupported: &str,
        keep_inferred: bool,
    ) -> PyResult<W3cResult> {
        let data = InputSpec::new(data, data_path, data_format, "data").map_err(py_value_error)?;
        let mode = parse_mode(graph_mode).map_err(py_value_error)?;
        let options = ValidationOptions {
            minimum_severity: parse_minimum_severity(minimum_severity).map_err(py_value_error)?,
            sort_results,
            entry_shape_names: entry_shape_names.unwrap_or_default(),
            engine: engine_options(on_unsupported).map_err(py_value_error)?,
        };
        let inference = ValidationInference {
            run: run_infer,
            keep_delta: keep_inferred,
        };
        py.detach(|| {
            let data_loaded = data.load(self.base.as_deref())?;
            let session = self
                .compiled
                .session(
                    SessionData::Separate(data_loaded.graph),
                    SessionOptions {
                        graph_mode: mode,
                        inference: inference.run,
                        engine: options.engine,
                    },
                )
                .map_err(|error| error.to_string())?;
            let report = session.report(&finding_options(&options));
            let report_graph = report_to_graph(&report);
            Ok(build_w3c_result(
                &report,
                &report_graph,
                inference.keep_delta.then(|| session.inferred().to_vec()),
                session_diagnostics(&self.compiled, &session),
            ))
        })
        .map_err(py_value_error)
    }

    /// Return the observed `sh:property` bindings for every focus node that
    /// conforms to a target/profile node shape — the inverse of
    /// `validate`/`validate_algebra`: successful bindings rather than
    /// violations. `key_path`, when given, is a SPARQL 1.1 property path
    /// expression (e.g. `"zea:roleName"`, `"zea:role/zea:roleName"`,
    /// `"^zea:describes/zea:roleName"`) evaluated from each `sh:property`
    /// shape's own node, over the shapes graph, to produce a stable key;
    /// property shapes where it resolves to no value fall back to their own
    /// IRI/blank-node id as the key. Prefixes resolve against the shapes
    /// document's declared `@prefix`es.
    #[pyo3(signature = (
        data=None,
        data_path=None,
        data_format="auto",
        key_path=None,
        graph_mode="union",
        run_infer=true,
        on_unsupported="ignore"
    ))]
    #[allow(clippy::too_many_arguments)]
    fn witnesses(
        &self,
        py: Python<'_>,
        data: Option<PyBackedBytes>,
        data_path: Option<String>,
        data_format: &str,
        key_path: Option<String>,
        graph_mode: &str,
        run_infer: bool,
        on_unsupported: &str,
    ) -> PyResult<Vec<PropertyWitness>> {
        let data = InputSpec::new(data, data_path, data_format, "data").map_err(py_value_error)?;
        let mode = parse_mode(graph_mode).map_err(py_value_error)?;
        let engine = engine_options(on_unsupported).map_err(py_value_error)?;
        let key_path = key_path
            .map(|expr| shifty_parse::parse_property_path(&expr, self.compiled.source()))
            .transpose()
            .map_err(|e| format!("invalid key_path: {e}"))
            .map_err(py_value_error)?;
        let witnesses = py
            .detach(|| {
                let data_loaded = data.load(self.base.as_deref())?;
                let session = self
                    .compiled
                    .session(
                        SessionData::Separate(data_loaded.graph),
                        SessionOptions {
                            graph_mode: mode,
                            inference: run_infer,
                            engine,
                        },
                    )
                    .map_err(|error| error.to_string())?;
                Ok(session.property_witnesses(key_path.as_ref(), &FindingOptions::default()))
            })
            .map_err(py_value_error)?;
        Ok(witnesses.into_iter().map(property_witness_to_py).collect())
    }

    fn __repr__(&self) -> String {
        format!(
            "PreparedValidator(statements={}, rules={})",
            self.compiled.normalized_schema().statements.len(),
            self.compiled.normalized_schema().rules.len()
        )
    }
}

// ── Module ────────────────────────────────────────────────────────────────────

#[pymodule]
fn _shifty(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add_class::<SparqlDiagnostic>()?;
    m.add_class::<ConstraintKind>()?;
    m.add_class::<Constraint>()?;
    m.add_class::<Reason>()?;
    m.add_class::<Violation>()?;
    m.add_class::<AlgebraResult>()?;
    m.add_class::<W3cResult>()?;
    m.add_class::<InferResult>()?;
    m.add_class::<PropertyWitness>()?;
    m.add_class::<PreparedValidator>()?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(_validate_algebra, m)?)?;
    m.add_function(wrap_pyfunction!(_validate_w3c, m)?)?;
    m.add_function(wrap_pyfunction!(_infer, m)?)?;
    repair::register(m)?;
    Ok(())
}
