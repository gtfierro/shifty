//! WebAssembly bindings for the shifty SHACL engine.
//!
//! Exposes the full inference + validation pipeline so it can run entirely in
//! the browser: parse Turtle, optionally apply SHACL-AF rules (inference), and
//! validate — returning either the structured "algebra" findings or a W3C
//! `sh:ValidationReport` (Turtle + human-readable text).
//!
//! The three entry points ([`validate`], [`validate_w3c`], [`infer`]) take
//! RDF strings and a small options object. Shapes and data may be supplied
//! as one combined graph (pass `null`/`""` for `data_ttl`) or as two separate
//! graphs.

use oxrdf::{Graph, Term, Triple};
use serde::Serialize;
use shifty_algebra::{Schema, Severity};
use shifty_engine::{
    InferenceOutcome, ValidationGraphMode, ValidationOptions, ValidationReport, Violation, infer,
    infer_graphs, report_to_graph, validate_graphs_with_mode_and_options,
    validate_report_graphs_with_mode_and_options, validate_report_with_options,
    validate_with_options,
};
use shifty_parse::Loaded;
use wasm_bindgen::prelude::*;

/// Install a panic hook that forwards Rust panics to `console.error`, making
/// wasm panics debuggable in the browser. Safe to call more than once.
#[wasm_bindgen(start)]
pub fn start() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

// ── Options ───────────────────────────────────────────────────────────────────

/// Caller-supplied options, deserialized from a plain JS object. Every field is
/// optional; omitted fields fall back to the documented defaults.
#[derive(serde::Deserialize, Default)]
#[serde(default, rename_all = "camelCase")]
struct Options {
    /// Run SHACL-AF rule inference before validating. Defaults to `true`.
    #[serde(default = "default_true")]
    infer: bool,
    /// How shapes and data graphs combine: `"data"`, `"union"`, or
    /// `"union-all"`. Defaults to `"data"`. Only relevant when data and shapes
    /// are supplied separately.
    graph_mode: Option<String>,
    /// Lowest severity that makes `conforms` false: `"info"`, `"warning"`, or
    /// `"violation"`. Defaults to `"info"` (any finding fails).
    minimum_severity: Option<String>,
    /// Sort results deterministically. Defaults to `true`.
    #[serde(default = "default_true")]
    sort_results: bool,
}

fn default_true() -> bool {
    true
}

impl Options {
    fn from_js(value: JsValue) -> Result<Self, JsError> {
        if value.is_undefined() || value.is_null() {
            return Ok(Self::default());
        }
        serde_wasm_bindgen::from_value(value).map_err(|e| JsError::new(&e.to_string()))
    }

    fn graph_mode(&self) -> Result<ValidationGraphMode, JsError> {
        match self.graph_mode.as_deref() {
            None | Some("data") => Ok(ValidationGraphMode::Data),
            Some("union") => Ok(ValidationGraphMode::Union),
            Some("union-all") => Ok(ValidationGraphMode::UnionAll),
            Some(other) => Err(JsError::new(&format!(
                "unknown graphMode {other:?}; expected 'data', 'union', or 'union-all'"
            ))),
        }
    }

    fn validation_options(&self) -> Result<ValidationOptions, JsError> {
        let minimum_severity = match self.minimum_severity.as_deref() {
            None => Severity::default(),
            Some(value) => match value.to_ascii_lowercase().as_str() {
                "info" => Severity::Info,
                "warning" => Severity::Warning,
                "violation" => Severity::Violation,
                other => {
                    return Err(JsError::new(&format!(
                        "unknown minimumSeverity {other:?}; expected 'info', 'warning', or 'violation'"
                    )));
                }
            },
        };
        Ok(ValidationOptions {
            minimum_severity,
            sort_results: self.sort_results,
            ..Default::default()
        })
    }
}

// ── Result shapes (serialized to JS objects) ──────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AlgebraResult {
    conforms: bool,
    violations: Vec<JsViolation>,
    results_text: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsViolation {
    focus_node: String,
    shape_name: Option<String>,
    severity: String,
    reasons: Vec<JsReason>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsReason {
    value: String,
    path: Option<String>,
    message: String,
    severity: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct W3cResult {
    conforms: bool,
    report_turtle: String,
    results_text: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InferResult {
    /// Number of newly inferred triples (the delta).
    inferred_count: usize,
    /// Total triples in the union graph (original + inferred).
    total_count: usize,
    /// The union graph (original data + inferred) as N-Triples.
    graph_ntriples: String,
    /// Just the newly inferred triples, as N-Triples — handy for showing the
    /// delta or downloading only what the rules added.
    inferred_ntriples: String,
    /// Unsupported rule features encountered during inference, if any.
    diagnostics: Vec<String>,
}

// ── Public entry points ───────────────────────────────────────────────────────

/// Validate `data_ttl` against the shapes in `shapes_ttl`, returning structured
/// findings: `{ conforms, violations: [...], resultsText }`.
///
/// Pass `null`/`""` for `data_ttl` to treat `shapes_ttl` as a single combined
/// shapes+data graph.
#[wasm_bindgen(js_name = validate)]
pub fn validate(
    shapes_ttl: &str,
    data_ttl: Option<String>,
    options: JsValue,
) -> Result<JsValue, JsError> {
    let opts = Options::from_js(options)?;
    let vopts = opts.validation_options()?;
    let mode = opts.graph_mode()?;

    let shapes = load(shapes_ttl, "shapes")?;
    let parsed = shifty_parse::parse_loaded(&shapes);
    let schema = shifty_opt::normalize(&parsed.schema);

    let outcome = match data_text(&data_ttl) {
        Some(data_ttl) => {
            let data = load(data_ttl, "data")?;
            let inferred = maybe_infer_graphs(&data.graph, &shapes.graph, &schema, opts.infer)?;
            let eval_data = inferred.as_ref().unwrap_or(&data.graph);
            validate_graphs_with_mode_and_options(eval_data, &shapes.graph, &schema, mode, &vopts)
        }
        None => {
            let inferred = maybe_infer(&shapes.graph, &schema, opts.infer)?;
            let eval_data = inferred.as_ref().unwrap_or(&shapes.graph);
            validate_with_options(eval_data, &schema, &vopts)
        }
    }
    .map_err(|e| JsError::new(&e.to_string()))?;

    let result = AlgebraResult {
        conforms: outcome.conforms,
        violations: outcome
            .violations
            .iter()
            .map(|v| violation_to_js(v, &schema))
            .collect(),
        results_text: format_algebra_text(&outcome.conforms, &outcome.violations, &schema),
    };
    to_js(&result)
}

/// Validate and return a W3C `sh:ValidationReport`:
/// `{ conforms, reportTurtle, resultsText }`.
#[wasm_bindgen(js_name = validateW3c)]
pub fn validate_w3c(
    shapes_ttl: &str,
    data_ttl: Option<String>,
    options: JsValue,
) -> Result<JsValue, JsError> {
    let opts = Options::from_js(options)?;
    let vopts = opts.validation_options()?;
    let mode = opts.graph_mode()?;

    let shapes = load(shapes_ttl, "shapes")?;
    let parsed = shifty_parse::parse_loaded(&shapes);
    let schema = shifty_opt::normalize(&parsed.schema);

    let report = match data_text(&data_ttl) {
        Some(data_ttl) => {
            let data = load(data_ttl, "data")?;
            let inferred = maybe_infer_graphs(&data.graph, &shapes.graph, &schema, opts.infer)?;
            let eval_data = inferred.as_ref().unwrap_or(&data.graph);
            validate_report_graphs_with_mode_and_options(&shapes, eval_data, mode, &vopts)
        }
        None => {
            let inferred = maybe_infer(&shapes.graph, &schema, opts.infer)?;
            let eval_data = inferred.as_ref().unwrap_or(&shapes.graph);
            validate_report_with_options(&shapes, eval_data, &vopts)
        }
    };

    let report_graph = report_to_graph(&report);
    let result = W3cResult {
        conforms: report.conforms,
        report_turtle: graph_to_turtle(&report_graph),
        results_text: format_report_text(&report),
    };
    to_js(&result)
}

/// Run SHACL-AF rule inference and return the resulting graph as N-Triples,
/// plus the number of newly inferred triples: `{ inferredCount, graphNtriples }`.
#[wasm_bindgen(js_name = infer)]
pub fn infer_js(shapes_ttl: &str, data_ttl: Option<String>) -> Result<JsValue, JsError> {
    let shapes = load(shapes_ttl, "shapes")?;
    let parsed = shifty_parse::parse_loaded(&shapes);
    let schema = shifty_opt::normalize(&parsed.schema);

    let outcome: InferenceOutcome = match data_text(&data_ttl) {
        Some(data_ttl) => {
            let data = load(data_ttl, "data")?;
            infer_graphs(&data.graph, &shapes.graph, &schema)
        }
        None => infer(&shapes.graph, &schema),
    }
    .map_err(|e| JsError::new(&e.to_string()))?;

    let result = InferResult {
        inferred_count: outcome.inferred.len(),
        total_count: outcome.graph.len(),
        graph_ntriples: graph_to_ntriples(&outcome.graph),
        inferred_ntriples: triples_to_ntriples(&outcome.inferred),
        diagnostics: outcome.diagnostics,
    };
    to_js(&result)
}

/// Parse RDF in a browser-fetched format and re-serialize it as Turtle.
/// The UI uses this to normalize fetched ontology dependencies before merging
/// them into the Turtle-only validation/inference inputs.
#[wasm_bindgen(js_name = rdfToTurtle)]
pub fn rdf_to_turtle(
    text: &str,
    content_type: Option<String>,
    url: Option<String>,
) -> Result<String, JsError> {
    let loaded = shifty_parse::load_rdf_auto(
        text.as_bytes(),
        content_type.as_deref(),
        url.as_deref(),
        url.as_deref(),
    )
    .map_err(|e| JsError::new(&format!("failed to parse RDF: {e}")))?;
    Ok(graph_to_turtle(&loaded.graph))
}

/// Re-serialize an N-Triples string as prettified Turtle (with common prefixes).
/// Lets the UI offer a Turtle download of a graph it only holds as N-Triples
/// (e.g. the inference union) without re-running the engine.
#[wasm_bindgen(js_name = ntriplesToTurtle)]
pub fn ntriples_to_turtle(ntriples: &str) -> Result<String, JsError> {
    let loaded = shifty_parse::load_ntriples(ntriples.as_bytes())
        .map_err(|e| JsError::new(&format!("failed to parse N-Triples: {e}")))?;
    Ok(graph_to_turtle(&loaded.graph))
}

// ── Pipeline helpers ──────────────────────────────────────────────────────────

/// Treat empty data input as "absent" so `""` behaves like `null` (combined
/// shapes+data graph).
fn data_text(data_ttl: &Option<String>) -> Option<&str> {
    data_ttl.as_deref().filter(|s| !s.trim().is_empty())
}

fn load(ttl: &str, label: &str) -> Result<Loaded, JsError> {
    shifty_parse::load_rdf_auto(ttl.as_bytes(), None, None, None)
        .map_err(|e| JsError::new(&format!("failed to parse {label} graph: {e}")))
}

fn maybe_infer(graph: &Graph, schema: &Schema, run_infer: bool) -> Result<Option<Graph>, JsError> {
    if run_infer && !schema.rules.is_empty() {
        let out = infer(graph, schema).map_err(|e| JsError::new(&e.to_string()))?;
        Ok(Some(out.graph))
    } else {
        Ok(None)
    }
}

fn maybe_infer_graphs(
    data: &Graph,
    shapes: &Graph,
    schema: &Schema,
    run_infer: bool,
) -> Result<Option<Graph>, JsError> {
    if run_infer && !schema.rules.is_empty() {
        let out = infer_graphs(data, shapes, schema).map_err(|e| JsError::new(&e.to_string()))?;
        Ok(Some(out.graph))
    } else {
        Ok(None)
    }
}

// ── Serialization helpers ─────────────────────────────────────────────────────

fn to_js<T: Serialize>(value: &T) -> Result<JsValue, JsError> {
    serde_wasm_bindgen::to_value(value).map_err(|e| JsError::new(&e.to_string()))
}

fn shape_name_for(v: &Violation, schema: &Schema) -> Option<String> {
    let shape_id = schema.statements.get(v.statement)?.shape;
    schema.names.get(&shape_id).cloned()
}

fn violation_to_js(v: &Violation, schema: &Schema) -> JsViolation {
    JsViolation {
        focus_node: v.focus.to_string(),
        shape_name: shape_name_for(v, schema),
        severity: v.severity.label().to_string(),
        reasons: v
            .reasons
            .iter()
            .map(|r| JsReason {
                value: r.value.to_string(),
                path: r.path.clone(),
                message: r.message.clone(),
                severity: r.severity.label().to_string(),
            })
            .collect(),
    }
}

fn graph_to_ntriples(graph: &Graph) -> String {
    let mut writer = oxttl::NTriplesSerializer::new().for_writer(Vec::new());
    for triple in graph {
        writer.serialize_triple(triple).unwrap();
    }
    String::from_utf8(writer.finish()).unwrap()
}

fn triples_to_ntriples(triples: &[Triple]) -> String {
    let mut writer = oxttl::NTriplesSerializer::new().for_writer(Vec::new());
    for triple in triples {
        writer.serialize_triple(triple).unwrap();
    }
    String::from_utf8(writer.finish()).unwrap()
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

fn format_algebra_text(conforms: &bool, violations: &[Violation], schema: &Schema) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    writeln!(out, "Validation Report").unwrap();
    writeln!(
        out,
        "Conforms: {}",
        if *conforms { "True" } else { "False" }
    )
    .unwrap();
    writeln!(out, "Results ({}):", violations.len()).unwrap();
    for v in violations {
        let shape = shape_name_for(v, schema).unwrap_or_else(|| "<unnamed>".to_string());
        writeln!(
            out,
            "Constraint Violation ({}) in shape {} at focus {}",
            v.severity.label(),
            shape,
            v.focus
        )
        .unwrap();
        for r in &v.reasons {
            if let Some(path) = &r.path {
                writeln!(out, "  Path: {path}").unwrap();
            }
            writeln!(out, "  Severity: {}", r.severity.label()).unwrap();
            writeln!(out, "  Value: {}", r.value).unwrap();
            writeln!(out, "  Message: {}", r.message).unwrap();
        }
    }
    out
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
        writeln!(
            out,
            "Constraint Violation in {}",
            local_name(r.component.as_str())
        )
        .unwrap();
        writeln!(out, "  Severity: sh:{}", local_name(r.severity.as_str())).unwrap();
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

fn local_name(iri: &str) -> &str {
    iri.rsplit_once('#')
        .or_else(|| iri.rsplit_once('/'))
        .map(|(_, local)| local)
        .unwrap_or(iri)
}
