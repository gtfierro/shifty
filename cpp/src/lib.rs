//! Stable C ABI backing the public C++ SDK.
//!
//! The C layer intentionally exposes opaque handles and length-delimited UTF-8
//! strings. Rust-owned RDF and query types never cross the ABI boundary.
#![allow(clippy::missing_safety_doc)]

use oxrdf::{Dataset as OxDataset, Graph, GraphName, Quad, Term};
use oxttl::{NTriplesSerializer, TurtleSerializer};
use shifty_algebra::{Schema, Severity};
use shifty_engine::{
    EngineOptions, ValidationGraphMode, ValidationOptions as EngineValidationOptions,
    ValidationOutcome, ValidationReport, Violation, infer_graphs,
    property_witnesses_graphs_with_mode, report_to_graph,
    validate_plan_graphs_with_mode_and_options, validate_report_graphs_with_mode_and_options,
};
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use spareval::{QueryEvaluator, QueryResults};
use spargebra::SparqlParser;
use std::cell::RefCell;
use std::ffi::{CString, c_char};
use std::fmt::Write as _;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::ptr;
use std::slice;

thread_local! {
    static LAST_ERROR: RefCell<CString> =
        RefCell::new(CString::new("").expect("an empty CString is valid"));
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShiftyStatus {
    Ok = 0,
    InvalidArgument = 1,
    IoError = 2,
    ParseError = 3,
    QueryError = 4,
    ValidationError = 5,
    InternalError = 255,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShiftyRdfFormat {
    Turtle = 0,
    NTriples = 1,
    Auto = 2,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShiftyGraphMode {
    Data = 0,
    Union = 1,
    UnionAll = 2,
}

/// Mirrors `ShiftySeverity` from the C header. See `severity()` for parsing.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShiftySeverity {
    Info = 0,
    Warning = 1,
    Violation = 2,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShiftyQueryResultKind {
    Boolean = 0,
    Solutions = 1,
    Graph = 2,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ShiftyStringView {
    pub data: *const c_char,
    pub len: usize,
}

pub struct ShiftyDataset {
    graph: Graph,
    query_dataset: OxDataset,
}

pub struct ShiftyPreparedValidator {
    shapes: shifty_parse::Loaded,
    schema: Schema,
    plan: shifty_opt::PhysicalPlan,
    diagnostics_json: String,
}

pub struct ShiftyQueryResult {
    kind: ShiftyQueryResultKind,
    boolean_value: bool,
    data: String,
    media_type: String,
}

pub struct ShiftyValidationResult {
    conforms: bool,
    report_turtle: String,
    results_text: String,
}

/// One `sh:property` binding at one conforming focus node, pre-stringified so
/// the C ABI never has to hand a live RDF term across the boundary.
struct PropertyWitnessItem {
    focus: String,
    shape: String,
    key: String,
    values: Vec<String>,
}

pub struct ShiftyPropertyWitnessList {
    items: Vec<PropertyWitnessItem>,
}

/// One failed atomic constraint within an [`AlgebraViolationItem`], pre-
/// stringified for the C ABI. An absent `path`/`author_message` is
/// represented as an empty string.
struct AlgebraReasonItem {
    value: String,
    path: String,
    message: String,
    author_message: String,
    severity: String,
}

/// One focus node that failed a shape, from the algebra validation path (the
/// engine's own conformance oracle, distinct from the W3C `sh:ValidationReport`
/// path). An absent `shape_name` (anonymous shape) is an empty string.
struct AlgebraViolationItem {
    focus: String,
    shape_name: String,
    severity: String,
    reasons: Vec<AlgebraReasonItem>,
}

pub struct ShiftyAlgebraResult {
    conforms: bool,
    violations: Vec<AlgebraViolationItem>,
    results_text: String,
}

#[derive(Debug)]
struct ApiError {
    status: ShiftyStatus,
    message: String,
}

impl ApiError {
    fn new(status: ShiftyStatus, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl ShiftyDataset {
    fn new() -> Self {
        Self {
            graph: Graph::new(),
            query_dataset: OxDataset::new(),
        }
    }

    fn extend(&mut self, loaded: shifty_parse::Loaded) {
        for triple in &loaded.graph {
            let triple = triple.into_owned();
            self.query_dataset.insert(&Quad::new(
                triple.subject.clone(),
                triple.predicate.clone(),
                triple.object.clone(),
                GraphName::DefaultGraph,
            ));
            self.graph.insert(&triple);
        }
    }

    fn query(&self, query: &str) -> Result<ShiftyQueryResult, ApiError> {
        let parsed = SparqlParser::new()
            .parse_query(query)
            .map_err(|error| ApiError::new(ShiftyStatus::QueryError, error.to_string()))?;
        let evaluator = QueryEvaluator::new();
        let results = evaluator
            .prepare(&parsed)
            .execute(&self.query_dataset)
            .map_err(|error| ApiError::new(ShiftyStatus::QueryError, error.to_string()))?;
        serialize_query_results(results)
    }
}

fn serialize_query_results(results: QueryResults<'_>) -> Result<ShiftyQueryResult, ApiError> {
    match results {
        QueryResults::Boolean(value) => {
            let bytes = QueryResultsSerializer::from_format(QueryResultsFormat::Json)
                .serialize_boolean_to_writer(Vec::new(), value)
                .map_err(internal_error)?;
            Ok(ShiftyQueryResult {
                kind: ShiftyQueryResultKind::Boolean,
                boolean_value: value,
                data: utf8(bytes)?,
                media_type: "application/sparql-results+json".to_string(),
            })
        }
        QueryResults::Solutions(mut solutions) => {
            let variables = solutions.variables().to_vec();
            let mut serializer = QueryResultsSerializer::from_format(QueryResultsFormat::Json)
                .serialize_solutions_to_writer(Vec::new(), variables)
                .map_err(internal_error)?;
            for solution in &mut solutions {
                let solution = solution
                    .map_err(|error| ApiError::new(ShiftyStatus::QueryError, error.to_string()))?;
                serializer
                    .serialize(
                        solution
                            .iter()
                            .map(|(variable, term)| (variable.as_ref(), term.as_ref())),
                    )
                    .map_err(internal_error)?;
            }
            let bytes = serializer.finish().map_err(internal_error)?;
            Ok(ShiftyQueryResult {
                kind: ShiftyQueryResultKind::Solutions,
                boolean_value: false,
                data: utf8(bytes)?,
                media_type: "application/sparql-results+json".to_string(),
            })
        }
        QueryResults::Graph(triples) => {
            let mut serializer = NTriplesSerializer::new().for_writer(Vec::new());
            for triple in triples {
                let triple = triple
                    .map_err(|error| ApiError::new(ShiftyStatus::QueryError, error.to_string()))?;
                serializer
                    .serialize_triple(&triple)
                    .map_err(internal_error)?;
            }
            Ok(ShiftyQueryResult {
                kind: ShiftyQueryResultKind::Graph,
                boolean_value: false,
                data: utf8(serializer.finish())?,
                media_type: "application/n-triples".to_string(),
            })
        }
    }
}

fn parse_bytes(
    data: &[u8],
    format: u32,
    base: Option<&str>,
) -> Result<shifty_parse::Loaded, ApiError> {
    match rdf_format(format)? {
        ShiftyRdfFormat::Auto => shifty_parse::load_rdf_auto(data, None, None, base),
        ShiftyRdfFormat::Turtle => shifty_parse::load_turtle(data, base),
        ShiftyRdfFormat::NTriples => shifty_parse::load_ntriples(data),
    }
    .map_err(|error| ApiError::new(ShiftyStatus::ParseError, error.to_string()))
}

fn parse_file(
    path: &str,
    format: u32,
    base: Option<&str>,
) -> Result<shifty_parse::Loaded, ApiError> {
    match rdf_format(format)? {
        ShiftyRdfFormat::Auto => std::fs::read(path)
            .map_err(|error| ApiError::new(ShiftyStatus::IoError, error.to_string()))
            .and_then(|bytes| {
                shifty_parse::load_rdf_auto(&bytes, None, Some(path), base.or(Some(path)))
                    .map_err(|error| ApiError::new(ShiftyStatus::ParseError, error.to_string()))
            }),
        ShiftyRdfFormat::Turtle => {
            shifty_parse::Loaded::from_path(Path::new(path), shifty_parse::RdfFormat::Turtle, base)
                .map_err(|error| {
                    let message = error.to_string();
                    let status = if message.starts_with("failed to open") {
                        ShiftyStatus::IoError
                    } else {
                        ShiftyStatus::ParseError
                    };
                    ApiError::new(status, message)
                })
        }
        ShiftyRdfFormat::NTriples => shifty_parse::Loaded::from_path(
            Path::new(path),
            shifty_parse::RdfFormat::NTriples,
            base,
        )
        .map_err(|error| {
            let message = error.to_string();
            let status = if message.starts_with("failed to open") {
                ShiftyStatus::IoError
            } else {
                ShiftyStatus::ParseError
            };
            ApiError::new(status, message)
        }),
    }
}

fn prepare(loaded: shifty_parse::Loaded) -> ShiftyPreparedValidator {
    let parsed = shifty_parse::parse_loaded(&loaded);
    let diagnostics: Vec<String> = parsed.diagnostics.iter().map(ToString::to_string).collect();
    let schema = shifty_opt::normalize(&parsed.schema);
    let plan = shifty_opt::plan(&schema);
    ShiftyPreparedValidator {
        shapes: loaded,
        schema,
        plan,
        diagnostics_json: serde_json::to_string(&diagnostics)
            .expect("serializing strings to JSON cannot fail"),
    }
}

fn validate_dataset(
    validator: &ShiftyPreparedValidator,
    dataset: &ShiftyDataset,
    mode: u32,
    run_inference: bool,
    minimum_severity: Severity,
) -> Result<ShiftyValidationResult, ApiError> {
    let mode = match graph_mode(mode)? {
        ShiftyGraphMode::Data => ValidationGraphMode::Data,
        ShiftyGraphMode::Union => ValidationGraphMode::Union,
        ShiftyGraphMode::UnionAll => ValidationGraphMode::UnionAll,
    };
    let inferred = if run_inference && !validator.schema.rules.is_empty() {
        Some(
            infer_graphs(&dataset.graph, &validator.shapes.graph, &validator.schema)
                .map_err(|error| ApiError::new(ShiftyStatus::ValidationError, error.to_string()))?
                .graph,
        )
    } else {
        None
    };
    let data = inferred.as_ref().unwrap_or(&dataset.graph);
    let options = engine_validation_options(minimum_severity);
    let report =
        validate_report_graphs_with_mode_and_options(&validator.shapes, data, mode, &options);
    let report_graph = report_to_graph(&report);
    Ok(ShiftyValidationResult {
        conforms: report.conforms,
        report_turtle: graph_to_turtle(&report_graph)?,
        results_text: format_report_text(&report),
    })
}

/// Full-fidelity term rendering (`<iri>`, `_:label`, `"lit"^^<dt>`/`"lit"@lang`)
/// — unambiguous and used for every witness field except `key`.
fn term_string(term: &Term) -> String {
    term.to_string()
}

/// A witness `key` renders as its bare lexical value when it is a literal
/// (the common case: a `zea:roleName "outsideAirTemp"`-style annotation), so
/// callers can use it directly as a join key without stripping quotes. Falls
/// back to the full term rendering for the IRI/blank-node case (no key path
/// matched, so the property shape's own source node is the key).
fn key_string(term: &Term) -> String {
    match term {
        Term::Literal(literal) => literal.value().to_string(),
        other => other.to_string(),
    }
}

fn witness_dataset(
    validator: &ShiftyPreparedValidator,
    dataset: &ShiftyDataset,
    key_path: Option<&str>,
    mode: u32,
    run_inference: bool,
) -> Result<ShiftyPropertyWitnessList, ApiError> {
    let mode = match graph_mode(mode)? {
        ShiftyGraphMode::Data => ValidationGraphMode::Data,
        ShiftyGraphMode::Union => ValidationGraphMode::Union,
        ShiftyGraphMode::UnionAll => ValidationGraphMode::UnionAll,
    };
    let inferred = if run_inference && !validator.schema.rules.is_empty() {
        Some(
            infer_graphs(&dataset.graph, &validator.shapes.graph, &validator.schema)
                .map_err(|error| ApiError::new(ShiftyStatus::ValidationError, error.to_string()))?
                .graph,
        )
    } else {
        None
    };
    let data = inferred.as_ref().unwrap_or(&dataset.graph);
    let key_path = key_path
        .filter(|expr| !expr.is_empty())
        .map(|expr| shifty_parse::parse_property_path(expr, &validator.shapes))
        .transpose()
        .map_err(|error| {
            ApiError::new(
                ShiftyStatus::InvalidArgument,
                format!("invalid key_path: {error}"),
            )
        })?;
    let witnesses =
        property_witnesses_graphs_with_mode(&validator.shapes, data, mode, key_path.as_ref());
    let items = witnesses
        .into_iter()
        .map(|w| PropertyWitnessItem {
            focus: term_string(&w.focus),
            shape: term_string(&w.shape),
            key: key_string(&w.key),
            values: w.values.iter().map(term_string).collect(),
        })
        .collect();
    Ok(ShiftyPropertyWitnessList { items })
}

/// Looks up the named-shape IRI a violated statement belongs to, if the
/// shape was declared with an IRI/blank-node id (rather than inlined).
fn shape_name_for(violation: &Violation, schema: &Schema) -> Option<String> {
    let shape_id = schema.statements.get(violation.statement)?.shape;
    schema.names.get(&shape_id).cloned()
}

fn build_algebra_result(outcome: ValidationOutcome, schema: &Schema) -> ShiftyAlgebraResult {
    let violations: Vec<AlgebraViolationItem> = outcome
        .violations
        .iter()
        .map(|violation| AlgebraViolationItem {
            focus: term_string(&violation.focus),
            shape_name: shape_name_for(violation, schema).unwrap_or_default(),
            severity: violation.severity.label().to_string(),
            reasons: violation
                .reasons
                .iter()
                .map(|reason| AlgebraReasonItem {
                    value: term_string(&reason.value),
                    path: reason.path.clone().unwrap_or_default(),
                    message: reason.message.clone(),
                    author_message: reason.author_message.clone().unwrap_or_default(),
                    severity: reason.severity.label().to_string(),
                })
                .collect(),
        })
        .collect();
    let results_text = format_algebra_report_text(outcome.conforms, &violations);
    ShiftyAlgebraResult {
        conforms: outcome.conforms,
        violations,
        results_text,
    }
}

fn format_algebra_report_text(conforms: bool, violations: &[AlgebraViolationItem]) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "Validation Report");
    let _ = writeln!(
        output,
        "Conforms: {}",
        if conforms { "True" } else { "False" }
    );
    for violation in violations {
        let shape = if violation.shape_name.is_empty() {
            "<anonymous>"
        } else {
            &violation.shape_name
        };
        let _ = writeln!(
            output,
            "\n{} result in {} ({}):",
            violation.severity, shape, violation.focus
        );
        for reason in &violation.reasons {
            if !reason.path.is_empty() {
                let _ = writeln!(output, "  Path: {}", reason.path);
            }
            let _ = writeln!(output, "  Severity: {}", reason.severity);
            let _ = writeln!(output, "  Value: {}", reason.value);
            let _ = writeln!(output, "  Message: {}", reason.message);
        }
    }
    output
}

fn validate_algebra_dataset(
    validator: &ShiftyPreparedValidator,
    dataset: &ShiftyDataset,
    mode: u32,
    run_inference: bool,
    minimum_severity: Severity,
) -> Result<ShiftyAlgebraResult, ApiError> {
    let mode = match graph_mode(mode)? {
        ShiftyGraphMode::Data => ValidationGraphMode::Data,
        ShiftyGraphMode::Union => ValidationGraphMode::Union,
        ShiftyGraphMode::UnionAll => ValidationGraphMode::UnionAll,
    };
    let inferred = if run_inference && !validator.schema.rules.is_empty() {
        Some(
            infer_graphs(&dataset.graph, &validator.shapes.graph, &validator.schema)
                .map_err(|error| ApiError::new(ShiftyStatus::ValidationError, error.to_string()))?
                .graph,
        )
    } else {
        None
    };
    let data = inferred.as_ref().unwrap_or(&dataset.graph);
    let options = engine_validation_options(minimum_severity);
    let outcome = validate_plan_graphs_with_mode_and_options(
        data,
        &validator.shapes.graph,
        &validator.plan,
        mode,
        &options,
    )
    .map_err(|error| ApiError::new(ShiftyStatus::ValidationError, error.to_string()))?;
    Ok(build_algebra_result(outcome, &validator.schema))
}

fn graph_to_ntriples(graph: &Graph) -> Result<String, ApiError> {
    let mut serializer = NTriplesSerializer::new().for_writer(Vec::new());
    for triple in graph {
        serializer
            .serialize_triple(triple)
            .map_err(internal_error)?;
    }
    utf8(serializer.finish())
}

fn graph_to_turtle(graph: &Graph) -> Result<String, ApiError> {
    let serializer = TurtleSerializer::new()
        .with_prefix("sh", "http://www.w3.org/ns/shacl#")
        .map_err(internal_error)?
        .with_prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        .map_err(internal_error)?
        .with_prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
        .map_err(internal_error)?
        .with_prefix("xsd", "http://www.w3.org/2001/XMLSchema#")
        .map_err(internal_error)?;
    let mut writer = serializer.for_writer(Vec::new());
    for triple in graph {
        writer.serialize_triple(triple).map_err(internal_error)?;
    }
    utf8(writer.finish().map_err(internal_error)?)
}

fn format_report_text(report: &ValidationReport) -> String {
    let mut output = String::new();
    let _ = writeln!(output, "Validation Report");
    let _ = writeln!(
        output,
        "Conforms: {}",
        if report.conforms { "True" } else { "False" }
    );
    if report.results.is_empty() {
        return output;
    }
    let _ = writeln!(output, "Results ({}):", report.results.len());
    for result in &report.results {
        let component = local_name(result.component.as_str());
        let severity = local_name(result.severity.as_str());
        let _ = writeln!(output, "Constraint Violation in {component}");
        let _ = writeln!(output, "  Severity: sh:{severity}");
        let _ = writeln!(output, "  Source Shape: {}", result.source_shape);
        let _ = writeln!(output, "  Focus Node: {}", result.focus);
        if let Some(path) = &result.path {
            let _ = writeln!(output, "  Result Path: {path}");
        }
        if let Some(value) = &result.value {
            let _ = writeln!(output, "  Value: {value}");
        }
        for message in &result.messages {
            let text = match message {
                oxrdf::Term::Literal(literal) => literal.value().to_string(),
                other => other.to_string(),
            };
            let _ = writeln!(output, "  Message: {text}");
        }
        let _ = writeln!(output);
    }
    output
}

fn local_name(iri: &str) -> &str {
    iri.rsplit_once('#')
        .or_else(|| iri.rsplit_once('/'))
        .map_or(iri, |(_, local)| local)
}

fn utf8(bytes: Vec<u8>) -> Result<String, ApiError> {
    String::from_utf8(bytes).map_err(internal_error)
}

fn internal_error(error: impl ToString) -> ApiError {
    ApiError::new(ShiftyStatus::InternalError, error.to_string())
}

fn rdf_format(value: u32) -> Result<ShiftyRdfFormat, ApiError> {
    match value {
        0 => Ok(ShiftyRdfFormat::Turtle),
        1 => Ok(ShiftyRdfFormat::NTriples),
        2 => Ok(ShiftyRdfFormat::Auto),
        _ => Err(ApiError::new(
            ShiftyStatus::InvalidArgument,
            format!("unknown RDF format value {value}"),
        )),
    }
}

fn graph_mode(value: u32) -> Result<ShiftyGraphMode, ApiError> {
    match value {
        0 => Ok(ShiftyGraphMode::Data),
        1 => Ok(ShiftyGraphMode::Union),
        2 => Ok(ShiftyGraphMode::UnionAll),
        _ => Err(ApiError::new(
            ShiftyStatus::InvalidArgument,
            format!("unknown graph mode value {value}"),
        )),
    }
}

/// Parse a `ShiftySeverity` discriminant into the engine's `Severity`. Custom
/// severities are not expressible through the C ABI; callers that need them
/// should use the Python / Rust APIs directly.
fn severity(value: u32) -> Result<Severity, ApiError> {
    match value {
        0 => Ok(Severity::Info),
        1 => Ok(Severity::Warning),
        2 => Ok(Severity::Violation),
        _ => Err(ApiError::new(
            ShiftyStatus::InvalidArgument,
            format!("unknown severity value {value}"),
        )),
    }
}

/// Build the engine `ValidationOptions` from the C-level severity discriminant,
/// preserving the historical defaults for the fields the ABI does not expose
/// (`sort_results = true`, `engine.unsupported = Ignore`).
fn engine_validation_options(minimum_severity: Severity) -> EngineValidationOptions {
    EngineValidationOptions {
        minimum_severity,
        sort_results: true,
        engine: EngineOptions::default(),
    }
}

fn set_last_error(message: &str) {
    let sanitized = message.replace('\0', "\\0");
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() =
            CString::new(sanitized).expect("NUL bytes were replaced before CString creation");
    });
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "Rust panic crossed the SDK boundary".to_string()
    }
}

fn ffi_call(operation: impl FnOnce() -> Result<(), ApiError>) -> u32 {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(())) => {
            set_last_error("");
            ShiftyStatus::Ok as u32
        }
        Ok(Err(error)) => {
            set_last_error(&error.message);
            error.status as u32
        }
        Err(payload) => {
            set_last_error(&panic_message(payload));
            ShiftyStatus::InternalError as u32
        }
    }
}

unsafe fn bytes_from_raw<'a>(data: *const u8, len: usize) -> Result<&'a [u8], ApiError> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() {
        return Err(ApiError::new(
            ShiftyStatus::InvalidArgument,
            "data is null but len is non-zero",
        ));
    }
    Ok(unsafe { slice::from_raw_parts(data, len) })
}

unsafe fn str_from_raw<'a>(
    data: *const c_char,
    len: usize,
    label: &str,
) -> Result<&'a str, ApiError> {
    let bytes = unsafe { bytes_from_raw(data.cast(), len) }?;
    std::str::from_utf8(bytes).map_err(|error| {
        ApiError::new(
            ShiftyStatus::InvalidArgument,
            format!("{label} is not valid UTF-8: {error}"),
        )
    })
}

unsafe fn optional_str_from_raw<'a>(
    data: *const c_char,
    len: usize,
    label: &str,
) -> Result<Option<&'a str>, ApiError> {
    if data.is_null() && len == 0 {
        return Ok(None);
    }
    unsafe { str_from_raw(data, len, label) }.map(Some)
}

fn string_view(value: &str) -> ShiftyStringView {
    ShiftyStringView {
        data: value.as_ptr().cast(),
        len: value.len(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn shifty_abi_version() -> u32 {
    2
}

#[unsafe(no_mangle)]
pub extern "C" fn shifty_last_error_message() -> *const c_char {
    LAST_ERROR.with(|slot| slot.borrow().as_ptr())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_dataset_create(out: *mut *mut ShiftyDataset) -> u32 {
    ffi_call(|| {
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out dataset pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        unsafe { out.write(Box::into_raw(Box::new(ShiftyDataset::new()))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_dataset_destroy(dataset: *mut ShiftyDataset) {
    if !dataset.is_null() {
        unsafe { drop(Box::from_raw(dataset)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_dataset_load_memory(
    dataset: *mut ShiftyDataset,
    data: *const u8,
    len: usize,
    format: u32,
    base: *const c_char,
    base_len: usize,
) -> u32 {
    ffi_call(|| {
        let dataset = unsafe { dataset.as_mut() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "dataset is null"))?;
        let data = unsafe { bytes_from_raw(data, len) }?;
        let base = unsafe { optional_str_from_raw(base, base_len, "base IRI") }?;
        dataset.extend(parse_bytes(data, format, base)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_dataset_load_file(
    dataset: *mut ShiftyDataset,
    path: *const c_char,
    path_len: usize,
    format: u32,
    base: *const c_char,
    base_len: usize,
) -> u32 {
    ffi_call(|| {
        let dataset = unsafe { dataset.as_mut() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "dataset is null"))?;
        let path = unsafe { str_from_raw(path, path_len, "path") }?;
        let base = unsafe { optional_str_from_raw(base, base_len, "base IRI") }?;
        dataset.extend(parse_file(path, format, base)?);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_dataset_len(dataset: *const ShiftyDataset) -> usize {
    unsafe { dataset.as_ref() }.map_or(0, |dataset| dataset.graph.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_dataset_ntriples(
    dataset: *const ShiftyDataset,
    out: *mut *mut ShiftyQueryResult,
) -> u32 {
    ffi_call(|| {
        let dataset = unsafe { dataset.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "dataset is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out result pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let result = ShiftyQueryResult {
            kind: ShiftyQueryResultKind::Graph,
            boolean_value: false,
            data: graph_to_ntriples(&dataset.graph)?,
            media_type: "application/n-triples".to_string(),
        };
        unsafe { out.write(Box::into_raw(Box::new(result))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_dataset_query(
    dataset: *const ShiftyDataset,
    query: *const c_char,
    query_len: usize,
    out: *mut *mut ShiftyQueryResult,
) -> u32 {
    ffi_call(|| {
        let dataset = unsafe { dataset.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "dataset is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out result pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let query = unsafe { str_from_raw(query, query_len, "SPARQL query") }?;
        unsafe { out.write(Box::into_raw(Box::new(dataset.query(query)?))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_query_result_destroy(result: *mut ShiftyQueryResult) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_query_result_kind(result: *const ShiftyQueryResult) -> u32 {
    unsafe { result.as_ref() }.map_or(ShiftyQueryResultKind::Graph as u32, |result| {
        result.kind as u32
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_query_result_boolean(result: *const ShiftyQueryResult) -> u8 {
    u8::from(unsafe { result.as_ref() }.is_some_and(|result| result.boolean_value))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_query_result_data(
    result: *const ShiftyQueryResult,
) -> ShiftyStringView {
    unsafe { result.as_ref() }.map_or(
        ShiftyStringView {
            data: ptr::null(),
            len: 0,
        },
        |result| string_view(&result.data),
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_query_result_media_type(
    result: *const ShiftyQueryResult,
) -> ShiftyStringView {
    unsafe { result.as_ref() }.map_or(
        ShiftyStringView {
            data: ptr::null(),
            len: 0,
        },
        |result| string_view(&result.media_type),
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_prepared_validator_create_memory(
    data: *const u8,
    len: usize,
    format: u32,
    base: *const c_char,
    base_len: usize,
    out: *mut *mut ShiftyPreparedValidator,
) -> u32 {
    ffi_call(|| {
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out validator pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let data = unsafe { bytes_from_raw(data, len) }?;
        let base = unsafe { optional_str_from_raw(base, base_len, "base IRI") }?;
        let validator = prepare(parse_bytes(data, format, base)?);
        unsafe { out.write(Box::into_raw(Box::new(validator))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_prepared_validator_create_file(
    path: *const c_char,
    path_len: usize,
    format: u32,
    base: *const c_char,
    base_len: usize,
    out: *mut *mut ShiftyPreparedValidator,
) -> u32 {
    ffi_call(|| {
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out validator pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let path = unsafe { str_from_raw(path, path_len, "path") }?;
        let base = unsafe { optional_str_from_raw(base, base_len, "base IRI") }?;
        let validator = prepare(parse_file(path, format, base)?);
        unsafe { out.write(Box::into_raw(Box::new(validator))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_prepared_validator_destroy(
    validator: *mut ShiftyPreparedValidator,
) {
    if !validator.is_null() {
        unsafe { drop(Box::from_raw(validator)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_prepared_validator_diagnostics_json(
    validator: *const ShiftyPreparedValidator,
) -> ShiftyStringView {
    unsafe { validator.as_ref() }.map_or(
        ShiftyStringView {
            data: ptr::null(),
            len: 0,
        },
        |validator| string_view(&validator.diagnostics_json),
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_prepared_validator_validate(
    validator: *const ShiftyPreparedValidator,
    dataset: *const ShiftyDataset,
    graph_mode: u32,
    run_inference: u8,
    minimum_severity: u32,
    out: *mut *mut ShiftyValidationResult,
) -> u32 {
    ffi_call(|| {
        let validator = unsafe { validator.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "validator is null"))?;
        let dataset = unsafe { dataset.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "dataset is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out result pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let minimum_severity = severity(minimum_severity)?;
        let result = validate_dataset(
            validator,
            dataset,
            graph_mode,
            run_inference != 0,
            minimum_severity,
        )?;
        unsafe { out.write(Box::into_raw(Box::new(result))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_validation_result_destroy(result: *mut ShiftyValidationResult) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_validation_result_conforms(
    result: *const ShiftyValidationResult,
) -> u8 {
    u8::from(unsafe { result.as_ref() }.is_some_and(|result| result.conforms))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_validation_result_report_turtle(
    result: *const ShiftyValidationResult,
) -> ShiftyStringView {
    unsafe { result.as_ref() }.map_or(
        ShiftyStringView {
            data: ptr::null(),
            len: 0,
        },
        |result| string_view(&result.report_turtle),
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_validation_result_results_text(
    result: *const ShiftyValidationResult,
) -> ShiftyStringView {
    unsafe { result.as_ref() }.map_or(
        ShiftyStringView {
            data: ptr::null(),
            len: 0,
        },
        |result| string_view(&result.results_text),
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_prepared_validator_witnesses(
    validator: *const ShiftyPreparedValidator,
    dataset: *const ShiftyDataset,
    key_path: *const c_char,
    key_path_len: usize,
    graph_mode: u32,
    run_inference: u8,
    out: *mut *mut ShiftyPropertyWitnessList,
) -> u32 {
    ffi_call(|| {
        let validator = unsafe { validator.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "validator is null"))?;
        let dataset = unsafe { dataset.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "dataset is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out result pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let key_path = unsafe { optional_str_from_raw(key_path, key_path_len, "key path") }?;
        let result = witness_dataset(validator, dataset, key_path, graph_mode, run_inference != 0)?;
        unsafe { out.write(Box::into_raw(Box::new(result))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_property_witness_list_destroy(
    list: *mut ShiftyPropertyWitnessList,
) {
    if !list.is_null() {
        unsafe { drop(Box::from_raw(list)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_property_witness_list_len(
    list: *const ShiftyPropertyWitnessList,
) -> usize {
    unsafe { list.as_ref() }.map_or(0, |list| list.items.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_property_witness_focus(
    list: *const ShiftyPropertyWitnessList,
    index: usize,
) -> ShiftyStringView {
    unsafe { list.as_ref() }
        .and_then(|list| list.items.get(index))
        .map_or(
            ShiftyStringView {
                data: ptr::null(),
                len: 0,
            },
            |item| string_view(&item.focus),
        )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_property_witness_shape(
    list: *const ShiftyPropertyWitnessList,
    index: usize,
) -> ShiftyStringView {
    unsafe { list.as_ref() }
        .and_then(|list| list.items.get(index))
        .map_or(
            ShiftyStringView {
                data: ptr::null(),
                len: 0,
            },
            |item| string_view(&item.shape),
        )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_property_witness_key(
    list: *const ShiftyPropertyWitnessList,
    index: usize,
) -> ShiftyStringView {
    unsafe { list.as_ref() }
        .and_then(|list| list.items.get(index))
        .map_or(
            ShiftyStringView {
                data: ptr::null(),
                len: 0,
            },
            |item| string_view(&item.key),
        )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_property_witness_value_count(
    list: *const ShiftyPropertyWitnessList,
    index: usize,
) -> usize {
    unsafe { list.as_ref() }
        .and_then(|list| list.items.get(index))
        .map_or(0, |item| item.values.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_property_witness_value(
    list: *const ShiftyPropertyWitnessList,
    index: usize,
    value_index: usize,
) -> ShiftyStringView {
    unsafe { list.as_ref() }
        .and_then(|list| list.items.get(index))
        .and_then(|item| item.values.get(value_index))
        .map_or(
            ShiftyStringView {
                data: ptr::null(),
                len: 0,
            },
            |value| string_view(value),
        )
}

fn empty_view() -> ShiftyStringView {
    ShiftyStringView {
        data: ptr::null(),
        len: 0,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_prepared_validator_validate_algebra(
    validator: *const ShiftyPreparedValidator,
    dataset: *const ShiftyDataset,
    graph_mode: u32,
    run_inference: u8,
    minimum_severity: u32,
    out: *mut *mut ShiftyAlgebraResult,
) -> u32 {
    ffi_call(|| {
        let validator = unsafe { validator.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "validator is null"))?;
        let dataset = unsafe { dataset.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "dataset is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out result pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let minimum_severity = severity(minimum_severity)?;
        let result = validate_algebra_dataset(
            validator,
            dataset,
            graph_mode,
            run_inference != 0,
            minimum_severity,
        )?;
        unsafe { out.write(Box::into_raw(Box::new(result))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_algebra_result_destroy(result: *mut ShiftyAlgebraResult) {
    if !result.is_null() {
        unsafe { drop(Box::from_raw(result)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_algebra_result_conforms(result: *const ShiftyAlgebraResult) -> u8 {
    u8::from(unsafe { result.as_ref() }.is_some_and(|result| result.conforms))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_algebra_result_results_text(
    result: *const ShiftyAlgebraResult,
) -> ShiftyStringView {
    unsafe { result.as_ref() }.map_or(empty_view(), |result| string_view(&result.results_text))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_algebra_result_violation_count(
    result: *const ShiftyAlgebraResult,
) -> usize {
    unsafe { result.as_ref() }.map_or(0, |result| result.violations.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_algebra_violation_focus(
    result: *const ShiftyAlgebraResult,
    index: usize,
) -> ShiftyStringView {
    unsafe { result.as_ref() }
        .and_then(|result| result.violations.get(index))
        .map_or(empty_view(), |violation| string_view(&violation.focus))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_algebra_violation_shape_name(
    result: *const ShiftyAlgebraResult,
    index: usize,
) -> ShiftyStringView {
    unsafe { result.as_ref() }
        .and_then(|result| result.violations.get(index))
        .map_or(empty_view(), |violation| string_view(&violation.shape_name))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_algebra_violation_severity(
    result: *const ShiftyAlgebraResult,
    index: usize,
) -> ShiftyStringView {
    unsafe { result.as_ref() }
        .and_then(|result| result.violations.get(index))
        .map_or(empty_view(), |violation| string_view(&violation.severity))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_algebra_violation_reason_count(
    result: *const ShiftyAlgebraResult,
    index: usize,
) -> usize {
    unsafe { result.as_ref() }
        .and_then(|result| result.violations.get(index))
        .map_or(0, |violation| violation.reasons.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_algebra_reason_value(
    result: *const ShiftyAlgebraResult,
    index: usize,
    reason_index: usize,
) -> ShiftyStringView {
    unsafe { result.as_ref() }
        .and_then(|result| result.violations.get(index))
        .and_then(|violation| violation.reasons.get(reason_index))
        .map_or(empty_view(), |reason| string_view(&reason.value))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_algebra_reason_path(
    result: *const ShiftyAlgebraResult,
    index: usize,
    reason_index: usize,
) -> ShiftyStringView {
    unsafe { result.as_ref() }
        .and_then(|result| result.violations.get(index))
        .and_then(|violation| violation.reasons.get(reason_index))
        .map_or(empty_view(), |reason| string_view(&reason.path))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_algebra_reason_message(
    result: *const ShiftyAlgebraResult,
    index: usize,
    reason_index: usize,
) -> ShiftyStringView {
    unsafe { result.as_ref() }
        .and_then(|result| result.violations.get(index))
        .and_then(|violation| violation.reasons.get(reason_index))
        .map_or(empty_view(), |reason| string_view(&reason.message))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_algebra_reason_author_message(
    result: *const ShiftyAlgebraResult,
    index: usize,
    reason_index: usize,
) -> ShiftyStringView {
    unsafe { result.as_ref() }
        .and_then(|result| result.violations.get(index))
        .and_then(|violation| violation.reasons.get(reason_index))
        .map_or(empty_view(), |reason| string_view(&reason.author_message))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_algebra_reason_severity(
    result: *const ShiftyAlgebraResult,
    index: usize,
    reason_index: usize,
) -> ShiftyStringView {
    unsafe { result.as_ref() }
        .and_then(|result| result.violations.get(index))
        .and_then(|violation| violation.reasons.get(reason_index))
        .map_or(empty_view(), |reason| string_view(&reason.severity))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATA: &str = r#"
        @prefix ex: <http://example.com/> .
        ex:alice ex:name "Alice" .
    "#;

    #[test]
    fn dataset_executes_select_and_ask() {
        let mut dataset = ShiftyDataset::new();
        dataset.extend(parse_bytes(DATA.as_bytes(), 0, None).unwrap());

        let select = dataset
            .query(
                "SELECT ?name WHERE { <http://example.com/alice> <http://example.com/name> ?name }",
            )
            .unwrap();
        assert_eq!(select.kind, ShiftyQueryResultKind::Solutions);
        assert!(select.data.contains("Alice"));

        let ask = dataset
            .query("ASK { <http://example.com/alice> <http://example.com/name> \"Alice\" }")
            .unwrap();
        assert_eq!(ask.kind, ShiftyQueryResultKind::Boolean);
        assert!(ask.boolean_value);
    }
}
