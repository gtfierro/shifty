//! Stable C ABI backing the public C++ SDK.
//!
//! The C layer intentionally exposes opaque handles and length-delimited UTF-8
//! strings. Rust-owned RDF and query types never cross the ABI boundary.
#![allow(clippy::missing_safety_doc)]

use oxrdf::{Dataset as OxDataset, Graph, GraphName, Quad};
use oxttl::{NTriplesSerializer, TurtleSerializer};
use shifty_engine::{
    ValidationGraphMode, ValidationReport, infer_graphs, report_to_graph,
    validate_report_graphs_with_mode,
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
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShiftyGraphMode {
    Data = 0,
    Union = 1,
    UnionAll = 2,
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
    schema: shifty_algebra::Schema,
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
    let format = match rdf_format(format)? {
        ShiftyRdfFormat::Turtle => shifty_parse::RdfFormat::Turtle,
        ShiftyRdfFormat::NTriples => shifty_parse::RdfFormat::NTriples,
    };
    shifty_parse::Loaded::from_path(Path::new(path), format, base).map_err(|error| {
        let message = error.to_string();
        let status = if message.starts_with("failed to open") {
            ShiftyStatus::IoError
        } else {
            ShiftyStatus::ParseError
        };
        ApiError::new(status, message)
    })
}

fn prepare(loaded: shifty_parse::Loaded) -> ShiftyPreparedValidator {
    let parsed = shifty_parse::parse_loaded(&loaded);
    let diagnostics: Vec<String> = parsed.diagnostics.iter().map(ToString::to_string).collect();
    let schema = shifty_opt::normalize(&parsed.schema);
    ShiftyPreparedValidator {
        shapes: loaded,
        schema,
        diagnostics_json: serde_json::to_string(&diagnostics)
            .expect("serializing strings to JSON cannot fail"),
    }
}

fn validate_dataset(
    validator: &ShiftyPreparedValidator,
    dataset: &ShiftyDataset,
    mode: u32,
    run_inference: bool,
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
    let report = validate_report_graphs_with_mode(&validator.shapes, data, mode);
    let report_graph = report_to_graph(&report);
    Ok(ShiftyValidationResult {
        conforms: report.conforms,
        report_turtle: graph_to_turtle(&report_graph)?,
        results_text: format_report_text(&report),
    })
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
    1
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
        let result = validate_dataset(validator, dataset, graph_mode, run_inference != 0)?;
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
