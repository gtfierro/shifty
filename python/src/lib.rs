use oxrdf::{Graph, Term};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use shifty_engine::{
    EngineOptions, UnsupportedPolicy, ValidationGraphMode, ValidationOptions, ValidationReport,
    property_witnesses_graphs_with_mode_and_options, report_to_graph,
    validate_plan_graphs_with_mode_and_options, validate_plan_with_options,
    validate_report_graphs_with_mode_and_options, validate_report_with_options,
};
use std::path::PathBuf;
use std::sync::OnceLock;

mod repair;

type ValidationInputs = (
    shifty_parse::Loaded,
    Option<shifty_parse::Loaded>,
    shifty_algebra::Schema,
    shifty_opt::PhysicalPlan,
    Vec<shifty_parse::Diagnostic>,
);

const VERSION: &str = env!("CARGO_PKG_VERSION");

// ── Algebra-path types ────────────────────────────────────────────────────────

/// Debugging detail for a failed `sh:sparql`/custom SPARQL-based constraint
/// component: what query ran, what it was bound to, and what it returned, so
/// a SPARQL failure is never a dead end.
#[pyclass(get_all)]
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
    violations: Vec<Py<Violation>>,
    results_text_cache: OnceLock<String>,
}

#[pymethods]
impl AlgebraResult {
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

#[pyclass(get_all)]
pub struct W3cResult {
    /// Whether the data graph conforms to all shapes.
    pub conforms: bool,
    /// The `sh:ValidationReport` serialized as Turtle.
    pub report_turtle: String,
    /// Human-readable summary (pyshacl-esque text).
    pub results_text: String,
}

#[pymethods]
impl W3cResult {
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
    graph_ntriples_cache: OnceLock<String>,
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
        py.allow_threads(|| {
            self.graph_ntriples_cache
                .get_or_init(|| graph_to_ntriples(&self.graph))
                .clone()
        })
    }

    fn __repr__(&self) -> String {
        format!("InferResult(inferred={})", self.inferred_count)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn parse_mode(mode: &str) -> Result<ValidationGraphMode, String> {
    match mode {
        "data" => Ok(ValidationGraphMode::Data),
        "union" => Ok(ValidationGraphMode::Union),
        "union-all" => Ok(ValidationGraphMode::UnionAll),
        other => Err(format!(
            "unknown graph_mode {other:?}; expected 'data', 'union', or 'union-all'"
        )),
    }
}

fn parse_minimum_severity(value: &str) -> Result<shifty_algebra::Severity, String> {
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

/// Parse, normalize, and plan a shapes graph; return (Loaded, Schema, PhysicalPlan, diagnostics).
fn prepare_loaded_shapes(
    loaded: shifty_parse::Loaded,
) -> (
    shifty_parse::Loaded,
    shifty_algebra::Schema,
    shifty_opt::PhysicalPlan,
    Vec<shifty_parse::Diagnostic>,
) {
    let parse_out = shifty_parse::parse_loaded(&loaded);
    let schema = shifty_opt::normalize(&parse_out.schema);
    let plan = shifty_opt::plan(&schema);
    (loaded, schema, plan, parse_out.diagnostics)
}

/// Optionally run SHACL-AF inference; returns the graph to validate against.
fn maybe_infer(
    data: &Graph,
    shapes: &Graph,
    schema: &shifty_algebra::Schema,
    run_infer: bool,
) -> Result<Option<Graph>, String> {
    if run_infer && !schema.rules.is_empty() {
        let out = shifty_engine::infer_graphs(data, shapes, schema)
            .map_err(|e| format!("non-stratifiable schema: {e}"))?;
        Ok(Some(out.graph))
    } else {
        Ok(None)
    }
}

fn maybe_infer_embedded(
    data: &Graph,
    schema: &shifty_algebra::Schema,
    run_infer: bool,
) -> Result<Option<Graph>, String> {
    if run_infer && !schema.rules.is_empty() {
        let out = shifty_engine::infer(data, schema)
            .map_err(|e| format!("non-stratifiable schema: {e}"))?;
        Ok(Some(out.graph))
    } else {
        Ok(None)
    }
}

pub(crate) fn graph_to_ntriples(graph: &Graph) -> String {
    let mut writer = oxttl::NTriplesSerializer::new().for_writer(Vec::new());
    for triple in graph {
        writer.serialize_triple(triple).unwrap();
    }
    // NTriplesSerializer::finish() returns the writer (Vec<u8>) directly, not Result
    let bytes = writer.finish();
    String::from_utf8(bytes).unwrap()
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

fn build_w3c_result(report: &ValidationReport, report_graph: &Graph) -> W3cResult {
    W3cResult {
        conforms: report.conforms,
        report_turtle: graph_to_turtle(report_graph),
        results_text: format_report_text(report),
    }
}

pub(crate) fn shape_name_for(
    v: &shifty_engine::Violation,
    schema: &shifty_algebra::Schema,
) -> Option<String> {
    let shape_id = schema.statements.get(v.statement)?.shape;
    schema.names.get(&shape_id).cloned()
}

/// Build a Python [`Violation`] from an engine violation (shared by the
/// validation and repair-gate paths).
pub(crate) fn violation_to_py(
    py: Python<'_>,
    v: &shifty_engine::Violation,
    schema: &shifty_algebra::Schema,
) -> PyResult<Py<Violation>> {
    let reasons = v
        .reasons
        .iter()
        .map(|r| {
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
                    sparql_diagnostic,
                },
            )
        })
        .collect::<PyResult<Vec<_>>>()?;
    Py::new(
        py,
        Violation {
            focus_node: v.focus.to_string(),
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

struct RawReason {
    value: String,
    path: Option<String>,
    message: String,
    author_message: Option<String>,
    severity: String,
    sparql_diagnostic: Option<RawSparqlDiagnostic>,
}

struct RawViolation {
    focus_node: String,
    shape_name: Option<String>,
    severity: String,
    reasons: Vec<RawReason>,
}

struct RawAlgebraResult {
    conforms: bool,
    violations: Vec<RawViolation>,
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
                                sparql_diagnostic,
                            },
                        )
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                Py::new(
                    py,
                    Violation {
                        focus_node: violation.focus_node,
                        shape_name: violation.shape_name,
                        severity: violation.severity,
                        reasons,
                    },
                )
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(AlgebraResult {
            conforms: self.conforms,
            violations,
            results_text_cache: OnceLock::new(),
        })
    }
}

fn raw_algebra_result(
    outcome: shifty_engine::ValidationOutcome,
    schema: &shifty_algebra::Schema,
) -> RawAlgebraResult {
    let violations = outcome
        .violations
        .iter()
        .map(|violation| RawViolation {
            focus_node: violation.focus.to_string(),
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
        violations,
    }
}

fn validate_algebra_loaded(
    data_loaded: &shifty_parse::Loaded,
    shapes_loaded: &shifty_parse::Loaded,
    schema: &shifty_algebra::Schema,
    plan: &shifty_opt::PhysicalPlan,
    mode: ValidationGraphMode,
    run_infer: bool,
    options: &ValidationOptions,
) -> Result<RawAlgebraResult, String> {
    let inferred = maybe_infer(&data_loaded.graph, &shapes_loaded.graph, schema, run_infer)?;
    let eval_data = inferred.as_ref().unwrap_or(&data_loaded.graph);
    let outcome = validate_plan_graphs_with_mode_and_options(
        eval_data,
        &shapes_loaded.graph,
        plan,
        mode,
        options,
    )
    .map_err(|e| format!("non-stratifiable schema: {e}"))?;
    Ok(raw_algebra_result(outcome, schema))
}

fn validate_algebra_embedded(
    loaded: &shifty_parse::Loaded,
    schema: &shifty_algebra::Schema,
    plan: &shifty_opt::PhysicalPlan,
    run_infer: bool,
    options: &ValidationOptions,
) -> Result<RawAlgebraResult, String> {
    let inferred = maybe_infer_embedded(&loaded.graph, schema, run_infer)?;
    let eval_data = inferred.as_ref().unwrap_or(&loaded.graph);
    let outcome = validate_plan_with_options(eval_data, plan, options)
        .map_err(|e| format!("non-stratifiable schema: {e}"))?;
    Ok(raw_algebra_result(outcome, schema))
}

fn validate_w3c_loaded(
    data_loaded: &shifty_parse::Loaded,
    shapes_loaded: &shifty_parse::Loaded,
    schema: &shifty_algebra::Schema,
    mode: ValidationGraphMode,
    run_infer: bool,
    options: &ValidationOptions,
) -> Result<W3cResult, String> {
    let inferred = maybe_infer(&data_loaded.graph, &shapes_loaded.graph, schema, run_infer)?;
    let eval_data = inferred.as_ref().unwrap_or(&data_loaded.graph);
    let report =
        validate_report_graphs_with_mode_and_options(shapes_loaded, eval_data, mode, options);
    let report_graph = report_to_graph(&report);
    Ok(build_w3c_result(&report, &report_graph))
}

fn validate_w3c_embedded(
    loaded: &shifty_parse::Loaded,
    schema: &shifty_algebra::Schema,
    run_infer: bool,
    options: &ValidationOptions,
) -> Result<W3cResult, String> {
    let inferred = maybe_infer_embedded(&loaded.graph, schema, run_infer)?;
    let eval_data = inferred.as_ref().unwrap_or(&loaded.graph);
    let report = validate_report_with_options(loaded, eval_data, options);
    let report_graph = report_to_graph(&report);
    Ok(build_w3c_result(&report, &report_graph))
}

fn load_validation_inputs(
    data: InputSpec,
    shapes: Option<InputSpec>,
    base: Option<&str>,
) -> Result<ValidationInputs, String> {
    let data_loaded = data.load(base)?;
    let shapes_loaded = shapes.map(|spec| spec.load(base)).transpose()?;
    let shapes_ref = shapes_loaded.as_ref().unwrap_or(&data_loaded);
    let parse_out = shifty_parse::parse_loaded(shapes_ref);
    let schema = shifty_opt::normalize(&parse_out.schema);
    let plan = shifty_opt::plan(&schema);
    Ok((
        data_loaded,
        shapes_loaded,
        schema,
        plan,
        parse_out.diagnostics,
    ))
}

/// Apply `UnsupportedPolicy` to parse-time diagnostics. Returns `Err` with a
/// combined message if the policy is `Error` and any `Unsupported` diagnostics
/// are present; otherwise returns `Ok(())`.
fn check_unsupported_diagnostics(
    diagnostics: &[shifty_parse::Diagnostic],
    policy: UnsupportedPolicy,
) -> Result<(), String> {
    if policy != UnsupportedPolicy::Error {
        return Ok(());
    }
    let unsupported: Vec<_> = diagnostics
        .iter()
        .filter(|d| d.level == shifty_parse::DiagLevel::Unsupported)
        .collect();
    if unsupported.is_empty() {
        return Ok(());
    }
    let msgs = unsupported
        .iter()
        .map(|d| d.to_string())
        .collect::<Vec<_>>()
        .join("; ");
    Err(format!(
        "unsupported features with on_unsupported='error': {msgs}"
    ))
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
    base=None
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
    let raw = py
        .allow_threads(move || {
            let (data_loaded, shapes_loaded, schema, plan, diagnostics) =
                load_validation_inputs(data, shapes, base.as_deref())?;
            check_unsupported_diagnostics(&diagnostics, options.engine.unsupported)?;
            match shapes_loaded.as_ref() {
                Some(shapes_loaded) => validate_algebra_loaded(
                    &data_loaded,
                    shapes_loaded,
                    &schema,
                    &plan,
                    mode,
                    run_infer,
                    &options,
                ),
                None => {
                    validate_algebra_embedded(&data_loaded, &schema, &plan, run_infer, &options)
                }
            }
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
    base=None
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
    py.allow_threads(move || {
        let (data_loaded, shapes_loaded, schema, _, _) =
            load_validation_inputs(data, shapes, base.as_deref())?;
        match shapes_loaded.as_ref() {
            Some(shapes_loaded) => validate_w3c_loaded(
                &data_loaded,
                shapes_loaded,
                &schema,
                mode,
                run_infer,
                &options,
            ),
            None => validate_w3c_embedded(&data_loaded, &schema, run_infer, &options),
        }
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
    py.allow_threads(move || {
        let (data_loaded, shapes_loaded, schema, _, _) =
            load_validation_inputs(data, shapes, base.as_deref())?;
        let outcome = match shapes_loaded.as_ref() {
            Some(shapes_loaded) => shifty_engine::infer_with_context_and_options(
                &data_loaded.graph,
                &shifty_engine::graph_union(&data_loaded.graph, &shapes_loaded.graph),
                &schema,
                &engine,
            ),
            None => shifty_engine::infer_with_options(&data_loaded.graph, &schema, &engine),
        }
        .map_err(|e| format!("non-stratifiable schema: {e}"))?;
        Ok(InferResult {
            inferred_count: outcome.inferred.len(),
            diagnostics: outcome.diagnostics,
            graph: outcome.graph,
            graph_ntriples_cache: OnceLock::new(),
        })
    })
    .map_err(py_value_error)
}

#[pyclass]
pub struct PreparedValidator {
    shapes: shifty_parse::Loaded,
    schema: shifty_algebra::Schema,
    plan: shifty_opt::PhysicalPlan,
    diagnostics: Vec<shifty_parse::Diagnostic>,
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
        py.allow_threads({
            let base = base.clone();
            move || {
                let loaded = input.load(base.as_deref())?;
                let (shapes, schema, plan, diagnostics) = prepare_loaded_shapes(loaded);
                Ok(Self {
                    shapes,
                    schema,
                    plan,
                    diagnostics,
                    base,
                })
            }
        })
        .map_err(py_value_error)
    }

    #[getter]
    fn diagnostics(&self) -> Vec<String> {
        self.diagnostics.iter().map(ToString::to_string).collect()
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
        on_unsupported="ignore"
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
    ) -> PyResult<AlgebraResult> {
        let data = InputSpec::new(data, data_path, data_format, "data").map_err(py_value_error)?;
        let mode = parse_mode(graph_mode).map_err(py_value_error)?;
        let options = ValidationOptions {
            minimum_severity: parse_minimum_severity(minimum_severity).map_err(py_value_error)?,
            sort_results,
            entry_shape_names: entry_shape_names.unwrap_or_default(),
            engine: engine_options(on_unsupported).map_err(py_value_error)?,
        };
        let diagnostics = self.diagnostics.clone();
        let raw = py
            .allow_threads(|| {
                check_unsupported_diagnostics(&diagnostics, options.engine.unsupported)?;
                let data_loaded = data.load(self.base.as_deref())?;
                validate_algebra_loaded(
                    &data_loaded,
                    &self.shapes,
                    &self.schema,
                    &self.plan,
                    mode,
                    run_infer,
                    &options,
                )
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
        on_unsupported="ignore"
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
    ) -> PyResult<W3cResult> {
        let data = InputSpec::new(data, data_path, data_format, "data").map_err(py_value_error)?;
        let mode = parse_mode(graph_mode).map_err(py_value_error)?;
        let options = ValidationOptions {
            minimum_severity: parse_minimum_severity(minimum_severity).map_err(py_value_error)?,
            sort_results,
            entry_shape_names: entry_shape_names.unwrap_or_default(),
            engine: engine_options(on_unsupported).map_err(py_value_error)?,
        };
        py.allow_threads(|| {
            let data_loaded = data.load(self.base.as_deref())?;
            validate_w3c_loaded(
                &data_loaded,
                &self.shapes,
                &self.schema,
                mode,
                run_infer,
                &options,
            )
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
        let options = ValidationOptions {
            engine: engine_options(on_unsupported).map_err(py_value_error)?,
            ..ValidationOptions::default()
        };
        let key_path = key_path
            .map(|expr| shifty_parse::parse_property_path(&expr, &self.shapes))
            .transpose()
            .map_err(|e| format!("invalid key_path: {e}"))
            .map_err(py_value_error)?;
        let witnesses = py
            .allow_threads(|| {
                let data_loaded = data.load(self.base.as_deref())?;
                let inferred = maybe_infer(
                    &data_loaded.graph,
                    &self.shapes.graph,
                    &self.schema,
                    run_infer,
                )?;
                let eval_data = inferred.as_ref().unwrap_or(&data_loaded.graph);
                Ok::<_, String>(property_witnesses_graphs_with_mode_and_options(
                    &self.shapes,
                    eval_data,
                    mode,
                    key_path.as_ref(),
                    &options,
                ))
            })
            .map_err(py_value_error)?;
        Ok(witnesses.into_iter().map(property_witness_to_py).collect())
    }

    fn __repr__(&self) -> String {
        format!(
            "PreparedValidator(statements={}, rules={})",
            self.schema.statements.len(),
            self.schema.rules.len()
        )
    }
}

// ── Module ────────────────────────────────────────────────────────────────────

#[pymodule]
fn _shifty(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add_class::<SparqlDiagnostic>()?;
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
