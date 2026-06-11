use oxrdf::{Graph, Term};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use shifty_engine::{
    ValidationGraphMode, ValidationReport, report_to_graph, validate_plan_graphs_with_mode,
    validate_report_graphs_with_mode,
};

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

#[pyclass(get_all)]
pub struct InferResult {
    /// Number of newly derived triples.
    pub inferred_count: usize,
    /// Warnings about unsupported rule features encountered during inference.
    pub diagnostics: Vec<String>,
    /// The full graph (original data + all inferred triples) as N-Triples.
    pub graph_ntriples: String,
}

#[pymethods]
impl InferResult {
    fn __repr__(&self) -> String {
        format!("InferResult(inferred={})", self.inferred_count)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn parse_mode(mode: &str) -> PyResult<ValidationGraphMode> {
    match mode {
        "data" => Ok(ValidationGraphMode::Data),
        "union" => Ok(ValidationGraphMode::Union),
        "union-all" => Ok(ValidationGraphMode::UnionAll),
        other => Err(PyValueError::new_err(format!(
            "unknown graph_mode {other:?}; expected 'data', 'union', or 'union-all'"
        ))),
    }
}

fn load_turtle(data: &[u8], base: Option<&str>) -> PyResult<shifty_parse::Loaded> {
    shifty_parse::load_turtle(data, base)
        .map_err(|e| PyValueError::new_err(format!("parse error: {e}")))
}

/// Parse, normalize, and plan a shapes graph; return (Loaded, Schema, PhysicalPlan).
fn prepare_shapes(
    shapes_data: &[u8],
    base: Option<&str>,
) -> PyResult<(
    shifty_parse::Loaded,
    shifty_algebra::Schema,
    shifty_opt::PhysicalPlan,
)> {
    let loaded = load_turtle(shapes_data, base)?;
    let parse_out = shifty_parse::parse_loaded(&loaded);
    let schema = shifty_opt::normalize(&parse_out.schema);
    let plan = shifty_opt::plan(&schema);
    Ok((loaded, schema, plan))
}

/// Optionally run SHACL-AF inference; returns the graph to validate against.
fn maybe_infer(
    data: &Graph,
    shapes: &Graph,
    schema: &shifty_algebra::Schema,
    run_infer: bool,
) -> PyResult<Option<Graph>> {
    if run_infer && !schema.rules.is_empty() {
        let out = shifty_engine::infer_graphs(data, shapes, schema)
            .map_err(|e| PyValueError::new_err(format!("non-stratifiable schema: {e}")))?;
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

// ── Exported functions ────────────────────────────────────────────────────────

/// Run algebra-path validation. Returns an `AlgebraResult` with structured
/// `Violation`/`Reason` objects representing the algebraic failure AST.
#[pyfunction]
#[pyo3(signature = (data, shapes=None, graph_mode="union", run_infer=true, base=None))]
pub fn _validate_algebra(
    py: Python<'_>,
    data: &[u8],
    shapes: Option<&[u8]>,
    graph_mode: &str,
    run_infer: bool,
    base: Option<&str>,
) -> PyResult<AlgebraResult> {
    let shapes_data = shapes.unwrap_or(data);
    let (shapes_loaded, schema, plan) = prepare_shapes(shapes_data, base)?;
    let data_loaded = load_turtle(data, base)?;
    let mode = parse_mode(graph_mode)?;

    let eval_data = match maybe_infer(&data_loaded.graph, &shapes_loaded.graph, &schema, run_infer)?
    {
        Some(g) => g,
        None => data_loaded.graph.clone(),
    };

    let outcome = validate_plan_graphs_with_mode(&eval_data, &shapes_loaded.graph, &plan, mode)
        .map_err(|e| PyValueError::new_err(format!("non-stratifiable schema: {e}")))?;

    let violations = outcome
        .violations
        .iter()
        .map(|v| {
            let reasons: Vec<Py<Reason>> = v
                .reasons
                .iter()
                .map(|r| {
                    Py::new(
                        py,
                        Reason {
                            value: r.value.to_string(),
                            path: r.path.clone(),
                            message: r.message.clone(),
                        },
                    )
                    .unwrap()
                })
                .collect();
            Py::new(
                py,
                Violation {
                    focus_node: v.focus.to_string(),
                    shape_name: shape_name_for(v, &schema),
                    reasons,
                },
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    Ok(AlgebraResult {
        conforms: outcome.conforms,
        violations,
    })
}

/// Run W3C-report-path validation. Returns a `W3cResult` whose `report_turtle`
/// is a full `sh:ValidationReport` Turtle document (same as pyshacl's second
/// return value) and `results_text` is a human-readable summary.
#[pyfunction]
#[pyo3(signature = (data, shapes=None, graph_mode="union", run_infer=true, base=None))]
pub fn _validate_w3c(
    data: &[u8],
    shapes: Option<&[u8]>,
    graph_mode: &str,
    run_infer: bool,
    base: Option<&str>,
) -> PyResult<W3cResult> {
    let shapes_data = shapes.unwrap_or(data);
    let shapes_loaded = load_turtle(shapes_data, base)?;
    let data_loaded = load_turtle(data, base)?;
    let mode = parse_mode(graph_mode)?;

    // Inference needs the parsed schema; parse it here even though the report
    // path re-parses internally, since we need it to run inference.
    let eval_data = if run_infer {
        let parse_out = shifty_parse::parse_loaded(&shapes_loaded);
        let schema = shifty_opt::normalize(&parse_out.schema);
        match maybe_infer(&data_loaded.graph, &shapes_loaded.graph, &schema, run_infer)? {
            Some(g) => g,
            None => data_loaded.graph.clone(),
        }
    } else {
        data_loaded.graph.clone()
    };

    let report = validate_report_graphs_with_mode(&shapes_loaded, &eval_data, mode);
    let report_graph = report_to_graph(&report);
    Ok(build_w3c_result(&report, &report_graph))
}

/// Run SHACL-AF forward-chaining rules to a fixed point. Returns an
/// `InferResult` with the full graph (as N-Triples) and inferred triple count.
#[pyfunction]
#[pyo3(signature = (data, shapes=None, base=None))]
pub fn _infer(data: &[u8], shapes: Option<&[u8]>, base: Option<&str>) -> PyResult<InferResult> {
    let shapes_data = shapes.unwrap_or(data);
    let shapes_loaded = load_turtle(shapes_data, base)?;
    let data_loaded = load_turtle(data, base)?;

    let parse_out = shifty_parse::parse_loaded(&shapes_loaded);
    let schema = shifty_opt::normalize(&parse_out.schema);

    let outcome = shifty_engine::infer_graphs(&data_loaded.graph, &shapes_loaded.graph, &schema)
        .map_err(|e| PyValueError::new_err(format!("non-stratifiable schema: {e}")))?;

    Ok(InferResult {
        inferred_count: outcome.inferred.len(),
        diagnostics: outcome.diagnostics,
        graph_ntriples: graph_to_ntriples(&outcome.graph),
    })
}

// ── Module ────────────────────────────────────────────────────────────────────

#[pymodule]
fn _shifty(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Reason>()?;
    m.add_class::<Violation>()?;
    m.add_class::<AlgebraResult>()?;
    m.add_class::<W3cResult>()?;
    m.add_class::<InferResult>()?;
    m.add_function(wrap_pyfunction!(_validate_algebra, m)?)?;
    m.add_function(wrap_pyfunction!(_validate_w3c, m)?)?;
    m.add_function(wrap_pyfunction!(_infer, m)?)?;
    Ok(())
}
