use oxrdf::{Graph, Term};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use shifty_engine::{
    ValidationGraphMode, ValidationOptions, ValidationReport, report_to_graph,
    validate_plan_graphs_with_mode_and_options, validate_plan_with_options,
    validate_report_graphs_with_mode_and_options, validate_report_with_options,
};
use std::path::PathBuf;
use std::sync::OnceLock;

// ── Algebra-path types ────────────────────────────────────────────────────────

#[pyclass(get_all)]
#[derive(Clone)]
pub struct Reason {
    /// The node at which the constraint failed.
    pub value: String,
    /// Path from the focus node to the value, in π notation (e.g. `ex:name`).
    pub path: Option<String>,
    /// Human-readable description of the failing constraint.
    pub message: String,
    /// SHACL severity (`"Violation"`, `"Warning"`, `"Info"`, or a custom IRI).
    pub severity: String,
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

#[derive(Debug, Clone, Copy)]
enum InputFormat {
    Turtle,
    NTriples,
}

impl InputFormat {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "turtle" => Ok(Self::Turtle),
            "nt" | "ntriples" => Ok(Self::NTriples),
            other => Err(format!(
                "unknown RDF format {other:?}; expected 'turtle' or 'nt'"
            )),
        }
    }

    fn parse_format(self) -> shifty_parse::RdfFormat {
        match self {
            Self::Turtle => shifty_parse::RdfFormat::Turtle,
            Self::NTriples => shifty_parse::RdfFormat::NTriples,
        }
    }
}

enum InputSource {
    Bytes(PyBackedBytes),
    Path(PathBuf),
}

struct InputSpec {
    source: InputSource,
    format: InputFormat,
}

impl InputSpec {
    fn new(
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

    fn load(&self, base: Option<&str>) -> Result<shifty_parse::Loaded, String> {
        let loaded = match &self.source {
            InputSource::Bytes(data) => match self.format {
                InputFormat::Turtle => shifty_parse::load_turtle(data, base),
                InputFormat::NTriples => shifty_parse::load_ntriples(data),
            },
            InputSource::Path(path) => {
                shifty_parse::Loaded::from_path(path, self.format.parse_format(), base)
            }
        };
        loaded.map_err(|e| format!("parse error: {e}"))
    }
}

fn py_value_error(message: String) -> PyErr {
    PyValueError::new_err(message)
}

/// Parse, normalize, and plan a shapes graph; return (Loaded, Schema, PhysicalPlan).
fn prepare_loaded_shapes(
    loaded: shifty_parse::Loaded,
) -> (
    shifty_parse::Loaded,
    shifty_algebra::Schema,
    shifty_opt::PhysicalPlan,
    Vec<String>,
) {
    let parse_out = shifty_parse::parse_loaded(&loaded);
    let diagnostics = parse_out
        .diagnostics
        .iter()
        .map(ToString::to_string)
        .collect();
    let schema = shifty_opt::normalize(&parse_out.schema);
    let plan = shifty_opt::plan(&schema);
    (loaded, schema, plan, diagnostics)
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

fn graph_to_ntriples(graph: &Graph) -> String {
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

fn shape_name_for(v: &shifty_engine::Violation, schema: &shifty_algebra::Schema) -> Option<String> {
    let shape_id = schema.statements.get(v.statement)?.shape;
    schema.names.get(&shape_id).cloned()
}

struct RawReason {
    value: String,
    path: Option<String>,
    message: String,
    severity: String,
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
                        Py::new(
                            py,
                            Reason {
                                value: reason.value,
                                path: reason.path,
                                message: reason.message,
                                severity: reason.severity,
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
                    severity: reason.severity.label().to_string(),
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
) -> Result<
    (
        shifty_parse::Loaded,
        Option<shifty_parse::Loaded>,
        shifty_algebra::Schema,
        shifty_opt::PhysicalPlan,
    ),
    String,
> {
    let data_loaded = data.load(base)?;
    let shapes_loaded = shapes.map(|spec| spec.load(base)).transpose()?;
    let shapes_ref = shapes_loaded.as_ref().unwrap_or(&data_loaded);
    let parse_out = shifty_parse::parse_loaded(shapes_ref);
    let schema = shifty_opt::normalize(&parse_out.schema);
    let plan = shifty_opt::plan(&schema);
    Ok((data_loaded, shapes_loaded, schema, plan))
}

// ── Exported functions ────────────────────────────────────────────────────────

/// Run algebra-path validation. Returns an `AlgebraResult` with structured
/// `Violation`/`Reason` objects representing the algebraic failure AST.
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    data=None,
    data_path=None,
    data_format="turtle",
    shapes=None,
    shapes_path=None,
    shapes_format="turtle",
    graph_mode="union",
    run_infer=true,
    minimum_severity="info",
    sort_results=true,
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
    run_infer: bool,
    minimum_severity: &str,
    sort_results: bool,
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
    };
    let raw = py
        .allow_threads(move || {
            let (data_loaded, shapes_loaded, schema, plan) =
                load_validation_inputs(data, shapes, base.as_deref())?;
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
#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    data=None,
    data_path=None,
    data_format="turtle",
    shapes=None,
    shapes_path=None,
    shapes_format="turtle",
    graph_mode="union",
    run_infer=true,
    minimum_severity="info",
    sort_results=true,
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
    run_infer: bool,
    minimum_severity: &str,
    sort_results: bool,
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
    };
    py.allow_threads(move || {
        let (data_loaded, shapes_loaded, schema, _) =
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
    data_format="turtle",
    shapes=None,
    shapes_path=None,
    shapes_format="turtle",
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
    base: Option<String>,
) -> PyResult<InferResult> {
    let data = InputSpec::new(data, data_path, data_format, "data").map_err(py_value_error)?;
    let shapes = match (shapes, shapes_path) {
        (None, None) => None,
        (data, path) => {
            Some(InputSpec::new(data, path, shapes_format, "shapes").map_err(py_value_error)?)
        }
    };
    py.allow_threads(move || {
        let (data_loaded, shapes_loaded, schema, _) =
            load_validation_inputs(data, shapes, base.as_deref())?;
        let outcome = match shapes_loaded.as_ref() {
            Some(shapes_loaded) => {
                shifty_engine::infer_graphs(&data_loaded.graph, &shapes_loaded.graph, &schema)
            }
            None => shifty_engine::infer(&data_loaded.graph, &schema),
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
    diagnostics: Vec<String>,
    base: Option<String>,
}

#[pymethods]
impl PreparedValidator {
    #[new]
    #[pyo3(signature = (
        shapes=None,
        shapes_path=None,
        shapes_format="turtle",
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
        self.diagnostics.clone()
    }

    #[pyo3(signature = (
        data=None,
        data_path=None,
        data_format="turtle",
        graph_mode="union",
        run_infer=true,
        minimum_severity="info",
        sort_results=true
    ))]
    fn validate_algebra(
        &self,
        py: Python<'_>,
        data: Option<PyBackedBytes>,
        data_path: Option<String>,
        data_format: &str,
        graph_mode: &str,
        run_infer: bool,
        minimum_severity: &str,
        sort_results: bool,
    ) -> PyResult<AlgebraResult> {
        let data = InputSpec::new(data, data_path, data_format, "data").map_err(py_value_error)?;
        let mode = parse_mode(graph_mode).map_err(py_value_error)?;
        let options = ValidationOptions {
            minimum_severity: parse_minimum_severity(minimum_severity).map_err(py_value_error)?,
            sort_results,
        };
        let raw = py
            .allow_threads(|| {
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
        data_format="turtle",
        graph_mode="union",
        run_infer=true,
        minimum_severity="info",
        sort_results=true
    ))]
    fn validate_w3c(
        &self,
        py: Python<'_>,
        data: Option<PyBackedBytes>,
        data_path: Option<String>,
        data_format: &str,
        graph_mode: &str,
        run_infer: bool,
        minimum_severity: &str,
        sort_results: bool,
    ) -> PyResult<W3cResult> {
        let data = InputSpec::new(data, data_path, data_format, "data").map_err(py_value_error)?;
        let mode = parse_mode(graph_mode).map_err(py_value_error)?;
        let options = ValidationOptions {
            minimum_severity: parse_minimum_severity(minimum_severity).map_err(py_value_error)?,
            sort_results,
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
    m.add_class::<Reason>()?;
    m.add_class::<Violation>()?;
    m.add_class::<AlgebraResult>()?;
    m.add_class::<W3cResult>()?;
    m.add_class::<InferResult>()?;
    m.add_class::<PreparedValidator>()?;
    m.add_function(wrap_pyfunction!(_validate_algebra, m)?)?;
    m.add_function(wrap_pyfunction!(_validate_w3c, m)?)?;
    m.add_function(wrap_pyfunction!(_infer, m)?)?;
    Ok(())
}
