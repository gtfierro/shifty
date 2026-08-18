//! Stable C ABI backing the public C++ SDK.
//!
//! The C layer intentionally exposes opaque handles and length-delimited UTF-8
//! strings. Rust-owned RDF and query types never cross the ABI boundary.
#![allow(clippy::missing_safety_doc)]

mod shapemap;

use oxrdf::{Dataset as OxDataset, Graph, GraphName, Quad, Term};
use oxttl::{NTriplesSerializer, TurtleSerializer};
use shifty_algebra::{Schema, Severity, ShapeId};
use shifty_engine::{
    ConformanceRun, EngineOptions, EvaluationStatus, EvidenceRun, PreparedEvidenceValidator,
    SelectedPair, ValidationGraphMode, ValidationOptions as EngineValidationOptions,
    ValidationOutcome, ValidationReport, Violation, compact_value, expand_value, graph_union,
    infer_graphs, property_witnesses_graphs_with_mode, report_to_graph,
    validate_plan_graphs_with_mode_and_options, validate_report_graphs_with_mode_and_options,
};
use shifty_parse::{Loaded as LoadedShapes, parse_property_path};
use sparesults::{QueryResultsFormat, QueryResultsSerializer};
use spareval::{QueryEvaluator, QueryResults};
use spargebra::SparqlParser;
use std::cell::RefCell;
use std::collections::HashMap;
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
    /// The schema as authored, before normalization. Evidence is reported
    /// against authored statements, so the evidence path needs the raw form;
    /// the W3C-report and algebra paths only ever use `schema`.
    raw_schema: Schema,
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

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShiftyEvaluationStatus {
    Pass = 0,
    Fail = 1,
}

/// Conformance-only totals over *normalized* `(statement, focus)` pairs — the
/// pairs evidence is materialized against, before authored fan-out. Returned by
/// value; there is nothing to own.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ShiftyConformanceRun {
    pub conforms: u8,
    pub selected_pairs: usize,
    pub passed: usize,
    pub failed: usize,
}

impl From<ConformanceRun> for ShiftyConformanceRun {
    fn from(run: ConformanceRun) -> Self {
        Self {
            conforms: u8::from(run.conforms),
            selected_pairs: run.selected_pairs,
            passed: run.passed,
            failed: run.failed,
        }
    }
}

/// The three RDF term kinds the shape-map ABI reports (IRI / literal /
/// blank node).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShiftyTermKind {
    Iri = 0,
    Literal = 1,
    BNode = 2,
}

/// One RDF term, returned by value with the string components pointing into
/// the handle that owns them. `datatype` and `language` are set only for
/// literals and are empty otherwise.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ShiftyTerm {
    pub kind: ShiftyTermKind,
    pub value: ShiftyStringView,
    pub datatype: ShiftyStringView,
    pub language: ShiftyStringView,
}

/// A (label, value) string pair, used for `value_paths`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ShiftyStringPair {
    pub first: ShiftyStringView,
    pub second: ShiftyStringView,
}

/// A prepared evidence snapshot: normalization, stratification, indexing, and
/// SPARQL preparation done once and reused across every call.
pub struct ShiftyEvidenceSession {
    prepared: PreparedEvidenceValidator,
    /// Retained so authored selectors can be rendered without depending on the
    /// `ShiftyPreparedValidator` outliving this session.
    raw_schema: Schema,
    constraints_json: String,
    /// Retained so `name_path`/`value_paths` resolve against the same shapes
    /// graph the snapshot was prepared over (the shape-map v2 features).
    shapes: LoadedShapes,
    /// Retained for the same reason: `resolve_path` evaluates over the data
    /// graph alone in `Data` mode and over the union otherwise.
    graph_mode: ValidationGraphMode,
}

/// One selected focus under one authored statement, pre-stringified for the ABI.
struct EvidenceFocusItem {
    focus: String,
    status: ShiftyEvaluationStatus,
    evidence_json: String,
    explanation: String,
}

/// One authored statement and every focus its selector chose — including none.
/// Absent normalized identities are reported as `usize::MAX` / `u32::MAX`,
/// matching the `SHIFTY_EVIDENCE_NO_*` sentinels in the C header.
struct EvidenceStatementItem {
    source_statement_id: usize,
    normalized_statement_id: usize,
    source_constraint_id: u32,
    normalized_constraint_id: u32,
    constraint_kind: String,
    target: String,
    foci: Vec<EvidenceFocusItem>,
}

pub struct ShiftyEvidenceRun {
    conforms: bool,
    json: String,
    statements: Vec<EvidenceStatementItem>,
}

/// The failing pairs of one conformance scan, with the handles `explain` needs.
/// The `SelectedPair`s are retained so explaining one costs no term re-parsing.
pub struct ShiftyFailureList {
    conformance: ShiftyConformanceRun,
    pairs: Vec<SelectedPair>,
    foci: Vec<String>,
}

/// An owned UTF-8 buffer produced by a call rather than cached in a handle
/// (the compact encoding and its inverse), so its lifetime is the caller's.
pub struct ShiftyString {
    value: String,
}

/// The four qualifier kinds a shape-map key can carry.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShiftyQualifierKind {
    Cls = 0,
    Const = 1,
    Datatype = 2,
    ShapeRef = 3,
}

/// One `name_path` result: a source constraint id and the names its
/// originating shapes-graph node resolves to.
struct ShapeMapNameItem {
    constraint_id: u32,
    names: Vec<String>,
}

/// The binding_names result of one call, retained by the handle.
pub struct ShiftyBindingNameList {
    items: Vec<ShapeMapNameItem>,
}

/// One `resolve_path` result: an input node and every node it reaches.
pub struct ShiftyPathResolutionList {
    entries: Vec<(String, Vec<String>)>,
}

/// A term pre-stringified for the ABI, with the components the C++ side
/// reassembles into its own `Term`.
struct ShapeMapTermItem {
    kind: ShiftyTermKind,
    value: String,
    datatype: String,
    language: String,
}

impl ShapeMapTermItem {
    fn from_info(info: &shapemap::TermInfo) -> Self {
        Self {
            kind: match info.kind {
                shapemap::TermKind::Iri => ShiftyTermKind::Iri,
                shapemap::TermKind::Literal => ShiftyTermKind::Literal,
                shapemap::TermKind::BNode => ShiftyTermKind::BNode,
            },
            value: info.value.clone(),
            datatype: info.datatype.clone().unwrap_or_default(),
            language: info.language.clone().unwrap_or_default(),
        }
    }

    fn c(&self) -> ShiftyTerm {
        ShiftyTerm {
            kind: self.kind,
            value: string_view(&self.value),
            datatype: string_view(&self.datatype),
            language: string_view(&self.language),
        }
    }
}

/// A qualifier pre-stringified for the ABI: an IRI for `Cls`/`Datatype`/
/// `ShapeRef`, a term for `Const`.
struct ShapeMapQualifierItem {
    kind: ShiftyQualifierKind,
    iri: String,
    term: ShapeMapTermItem,
}

impl ShapeMapQualifierItem {
    fn from_info(info: &shapemap::QualifierInfo) -> Self {
        let empty = ShapeMapTermItem {
            kind: ShiftyTermKind::Literal,
            value: String::new(),
            datatype: String::new(),
            language: String::new(),
        };
        match info {
            shapemap::QualifierInfo::Cls(iri) => Self {
                kind: ShiftyQualifierKind::Cls,
                iri: iri.clone(),
                term: empty,
            },
            shapemap::QualifierInfo::Const(term) => Self {
                kind: ShiftyQualifierKind::Const,
                iri: String::new(),
                term: ShapeMapTermItem::from_info(term),
            },
            shapemap::QualifierInfo::Datatype(iri) => Self {
                kind: ShiftyQualifierKind::Datatype,
                iri: iri.clone(),
                term: empty,
            },
            shapemap::QualifierInfo::ShapeRef(iri) => Self {
                kind: ShiftyQualifierKind::ShapeRef,
                iri: iri.clone(),
                term: empty,
            },
        }
    }
}

/// One `value_paths` label and every `(bound value, reached)` pair.
struct ShapeMapAnnotationItem {
    label: String,
    entries: Vec<(ShapeMapTermItem, Vec<ShapeMapTermItem>)>,
}

/// One key of a mapping, pre-stringified for the ABI.
struct ShapeMapBindingItem {
    key: String,
    key_path_json: String,
    qualifier: Option<ShapeMapQualifierItem>,
    ordinal: usize,
    kind: String,
    status: ShiftyEvaluationStatus,
    source_constraint_id: u32,
    /// `u32::MAX` when absent (matches `SHIFTY_EVIDENCE_NO_CONSTRAINT`).
    normalized_constraint_id: u32,
    severity: String,
    names: Vec<String>,
    /// `usize::MAX` when absent (matches `SHIFTY_EVIDENCE_NO_INDEX`).
    min: usize,
    max: usize,
    observed: usize,
    missing: usize,
    values: Vec<ShapeMapTermItem>,
    rejected_values: Vec<ShapeMapTermItem>,
    evidence_json: String,
    explain: String,
    annotations: Vec<ShapeMapAnnotationItem>,
}

/// One mapping, pre-stringified for the ABI.
struct ShapeMapMappingItem {
    focus: String,
    shape_name: String,
    target: String,
    conforms: bool,
    bindings: Vec<ShapeMapBindingItem>,
    evidence_json: String,
    explanation: String,
}

/// All mappings of one shape identity.
struct ShapeMapShapeItem {
    name: String,
    mappings: Vec<ShapeMapMappingItem>,
}

/// The shape map itself: conforms, per-shape mappings, and the plain-JSON
/// summary. Every `ShiftyStringView`/`ShiftyTerm` handed out points into
/// this handle and stays valid until destroy.
pub struct ShiftyShapeMap {
    conforms: bool,
    json: String,
    shapes: Vec<ShapeMapShapeItem>,
}

impl ShiftyShapeMap {
    fn from_data(data: shapemap::ShapeMapData) -> Self {
        Self {
            conforms: data.conforms,
            json: data.json,
            shapes: data
                .shapes
                .into_iter()
                .map(|shape| ShapeMapShapeItem {
                    name: shape.name,
                    mappings: shape
                        .mappings
                        .into_iter()
                        .map(|mapping| ShapeMapMappingItem {
                            focus: mapping.focus,
                            shape_name: mapping.shape_name,
                            target: mapping.target,
                            conforms: mapping.conforms,
                            bindings: mapping
                                .bindings
                                .into_iter()
                                .map(|binding| {
                                    let status = if binding.ok() {
                                        ShiftyEvaluationStatus::Pass
                                    } else {
                                        ShiftyEvaluationStatus::Fail
                                    };
                                    ShapeMapBindingItem {
                                        key: binding.key,
                                        key_path_json: binding.key_path_json,
                                        qualifier: binding
                                            .qualifier
                                            .as_ref()
                                            .map(ShapeMapQualifierItem::from_info),
                                        ordinal: binding.ordinal as usize,
                                        kind: binding.kind,
                                        status,
                                    source_constraint_id: binding.source_constraint_id,
                                    normalized_constraint_id: binding
                                        .normalized_constraint_id
                                        .unwrap_or(u32::MAX),
                                    severity: binding.severity,
                                    names: binding.names,
                                    min: binding.min.map(|value| value as usize).unwrap_or(usize::MAX),
                                    max: binding.max.map(|value| value as usize).unwrap_or(usize::MAX),
                                    observed: binding
                                        .observed
                                        .map(|value| value as usize)
                                        .unwrap_or(usize::MAX),
                                    missing: binding.missing as usize,
                                    values: binding
                                        .values
                                        .iter()
                                        .map(ShapeMapTermItem::from_info)
                                        .collect(),
                                    rejected_values: binding
                                        .rejected_values
                                        .iter()
                                        .map(ShapeMapTermItem::from_info)
                                        .collect(),
                                    evidence_json: binding.evidence_json.unwrap_or_default(),
                                    explain: binding.explain,
                                    annotations: binding
                                        .annotations
                                        .into_iter()
                                        .map(|group| ShapeMapAnnotationItem {
                                            label: group.label,
                                            entries: group
                                                .entries
                                                .into_iter()
                                                .map(|(term, reached)| {
                                                    (
                                                        ShapeMapTermItem::from_info(&term),
                                                        reached
                                                            .iter()
                                                            .map(ShapeMapTermItem::from_info)
                                                            .collect(),
                                                    )
                                                })
                                                .collect(),
                                        })
                                        .collect(),
                                }})
                                .collect(),
                            evidence_json: mapping.evidence_json,
                            explanation: mapping.explanation,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
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
                // Only use the path as a base IRI when it is already an absolute
                // URL (http(s):// or file://). A bare filesystem path is not a
                // valid IRI, so using it as a base would fail parsing — this
                // matches the CLI, which only derives a base from http(s) URLs.
                let path_as_base = (path.starts_with("http://")
                    || path.starts_with("https://")
                    || path.starts_with("file://"))
                .then_some(path);
                shifty_parse::load_rdf_auto(&bytes, None, Some(path), base.or(path_as_base))
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
        raw_schema: parsed.schema,
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
    entry_shape_names: &[String],
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
    let options = engine_validation_options(minimum_severity, entry_shape_names);
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

/// A term rendered for a `name_path` value: literals as their bare lexical
/// form (so `sh:name "zone temperature point"` joins cleanly), IRIs and
/// blank nodes in full.
fn term_text(term: &Term) -> String {
    match term {
        Term::Literal(literal) => literal.value().to_string(),
        other => other.to_string(),
    }
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
    entry_shape_names: &[String],
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
    let options = engine_validation_options(minimum_severity, entry_shape_names);
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

/// Prepare an evidence snapshot over `validator`'s shapes and `dataset`.
///
/// Inference (when requested and the schema has rules) runs first, exactly as
/// on the report and algebra paths, so all three see the same evaluated graph.
/// The snapshot is built from the *raw* schema: `PreparedEvidenceValidator`
/// normalizes internally and keeps the authored form to fan evidence back out
/// to authored statements, which pre-normalizing here would erase.
fn prepare_evidence(
    validator: &ShiftyPreparedValidator,
    dataset: &ShiftyDataset,
    mode: u32,
    run_inference: bool,
) -> Result<ShiftyEvidenceSession, ApiError> {
    let mode = match graph_mode(mode)? {
        ShiftyGraphMode::Data => ValidationGraphMode::Data,
        ShiftyGraphMode::Union => ValidationGraphMode::Union,
        ShiftyGraphMode::UnionAll => ValidationGraphMode::UnionAll,
    };
    let inferred = if run_inference && !validator.raw_schema.rules.is_empty() {
        Some(
            infer_graphs(
                &dataset.graph,
                &validator.shapes.graph,
                &validator.raw_schema,
            )
            .map_err(|error| ApiError::new(ShiftyStatus::ValidationError, error.to_string()))?
            .graph,
        )
    } else {
        None
    };
    let data = inferred.as_ref().unwrap_or(&dataset.graph);
    let prepared = PreparedEvidenceValidator::with_graphs(
        data,
        &validator.shapes.graph,
        &validator.raw_schema,
        mode,
    )
    .map_err(|error| {
        ApiError::new(
            ShiftyStatus::ValidationError,
            format!("non-stratifiable schema: {error}"),
        )
    })?;
    let constraints_json =
        serde_json::to_string(&prepared.constraints()).map_err(internal_error)?;
    Ok(ShiftyEvidenceSession {
        prepared,
        raw_schema: validator.raw_schema.clone(),
        constraints_json,
        // `Loaded` is not `Clone`; rebuild it from the validator's parts so the
        // session owns its own copy of the shapes graph and prefixes.
        shapes: LoadedShapes {
            graph: validator.shapes.graph.clone(),
            prefixes: validator.shapes.prefixes.clone(),
            base: validator.shapes.base.clone(),
        },
        graph_mode: mode,
    })
}

impl ShiftyEvidenceSession {
    /// For every *raw* (source) constraint with shapes-graph provenance
    /// (`Schema::sources`), the values `name_path` reaches from that
    /// constraint's originating node, evaluated over the shapes graph.
    /// `name_path = None` means `sh:name`. Constraints with no source-node
    /// provenance, or where `name_path` resolves to nothing, are omitted.
    fn binding_names(&self, name_path: Option<&str>) -> Result<HashMap<u32, Vec<String>>, ApiError> {
        let expr = name_path.unwrap_or("sh:name");
        let path = parse_property_path(expr, &self.shapes)
            .map_err(|error| ApiError::new(ShiftyStatus::InvalidArgument, format!("invalid name_path: {error}")))?;
        let mut out = HashMap::new();
        for (id, source) in &self.raw_schema.sources {
            let mut matches: Vec<String> = shifty_engine::path::succ(&self.shapes.graph, source, &path)
                .into_iter()
                .map(|term| term_text(&term))
                .collect();
            if matches.is_empty() {
                continue;
            }
            matches.sort();
            out.insert(id.0, matches);
        }
        Ok(out)
    }

    /// The raw schema's shape name for `constraint_id` — the IRI of the named
    /// (non-blank) RDF node it was lowered from, when it has one.
    fn shape_name_of(&self, constraint_id: u32) -> Option<String> {
        self.raw_schema.names.get(&ShapeId(constraint_id)).cloned()
    }

    /// Batch-evaluate `path` (a SPARQL 1.1 property path, same grammar as
    /// `name_path`) from each of `nodes` (N-Triples spellings) over the
    /// session's evaluation graph — the data graph, unioned with the shapes
    /// graph to match this session's own `graph_mode` (`union`/`union_all`;
    /// `data` mode reads the data graph alone). Returns each input node's
    /// N-Triples spelling mapped to the N-Triples spellings it reaches.
    fn resolve_path(&self, nodes: &[String], path: &str) -> Result<HashMap<String, Vec<String>>, ApiError> {
        let parsed = parse_property_path(path, &self.shapes)
            .map_err(|error| ApiError::new(ShiftyStatus::InvalidArgument, format!("invalid path: {error}")))?;
        let union_graph;
        let graph: &Graph = match self.graph_mode {
            ValidationGraphMode::Data => self.prepared.data(),
            ValidationGraphMode::Union | ValidationGraphMode::UnionAll => {
                union_graph = graph_union(self.prepared.data(), &self.shapes.graph);
                &union_graph
            }
        };
        let mut out = HashMap::with_capacity(nodes.len());
        for node in nodes {
            let term = parse_term(node)?;
            let mut matches: Vec<String> = shifty_engine::path::succ(graph, &term, &parsed)
                .into_iter()
                .map(|term| term.to_string())
                .collect();
            matches.sort();
            out.insert(node.clone(), matches);
        }
        Ok(out)
    }

    /// Build the shape map for `run` (Python's `ShapeMap.from_run`).
    fn shape_map(
        &self,
        run: &ShiftyEvidenceRun,
        name_path: Option<&str>,
        value_paths: &[(String, String)],
    ) -> Result<ShiftyShapeMap, ApiError> {
        let parsed: serde_json::Value = serde_json::from_str(&run.json).map_err(internal_error)?;
        let statements: Vec<shapemap::StatementMeta> = run
            .statements
            .iter()
            .map(|statement| shapemap::StatementMeta {
                source_statement_id: statement.source_statement_id,
                source_constraint_id: statement.source_constraint_id,
                normalized_constraint_id: if statement.normalized_constraint_id == u32::MAX {
                    None
                } else {
                    Some(statement.normalized_constraint_id)
                },
                target: statement.target.clone(),
                foci: statement
                    .foci
                    .iter()
                    .map(|focus| shapemap::FocusMeta {
                        focus: focus.focus.clone(),
                        evidence_json: focus.evidence_json.clone(),
                        explanation: focus.explanation.clone(),
                    })
                    .collect(),
            })
            .collect();
        let inputs = shapemap::ShapeMapBuildInputs {
            shape_name_of: &|id| self.shape_name_of(id),
            binding_names: &|name_path| {
                self.binding_names(name_path)
                    .map_err(|error| error.message)
            },
            explain_constraint: &|focus, ref_id| {
                let term = parse_term(focus).map_err(|error| error.message)?;
                let evidence = self
                    .prepared
                    .explain_constraint(&term, ShapeId(ref_id))
                    .map(|evidence| match evidence {
                        shifty_engine::Evidence::Satisfaction(trace) => {
                            serde_json::to_value(trace).ok()
                        }
                        shifty_engine::Evidence::Failure(witness) => {
                            serde_json::to_value(witness).ok()
                        }
                    })
                    .unwrap_or(None);
                Ok(evidence)
            },
            resolve_path: &|nodes, path| {
                self.resolve_path(nodes, path)
                    .map_err(|error| error.message)
            },
        };
        let data = shapemap::build(&parsed, name_path, value_paths, &statements, &inputs)
            .map_err(|error| ApiError::new(ShiftyStatus::InternalError, error))?;
        Ok(ShiftyShapeMap::from_data(data))
    }
}

/// The serde spelling of a constraint kind, so the string a caller reads here
/// is the one that appears in the run's JSON.
fn constraint_kind_string(kind: shifty_algebra::ConstraintKind) -> String {
    match serde_json::to_value(kind) {
        Ok(serde_json::Value::String(value)) => value,
        _ => "Unknown".to_string(),
    }
}

/// Flatten a typed run into the pre-stringified form the ABI hands out, keeping
/// the run's own JSON alongside it for callers that want the whole tree.
fn build_evidence_run(
    run: EvidenceRun,
    raw_schema: &Schema,
) -> Result<ShiftyEvidenceRun, ApiError> {
    let json = run.to_json().map_err(internal_error)?;
    let conforms = run.conforms;
    let statements = run
        .statements
        .into_iter()
        .map(|statement| {
            let target = raw_schema
                .statements
                .get(statement.source_statement_id)
                .map(|raw| {
                    shifty_algebra::render::selector_to_string_in(&raw.selector, &raw_schema.arena)
                })
                .unwrap_or_default();
            let foci = statement
                .selected_foci
                .into_iter()
                .map(|focus| {
                    Ok(EvidenceFocusItem {
                        focus: term_string(&focus.focus),
                        status: match focus.status() {
                            EvaluationStatus::Pass => ShiftyEvaluationStatus::Pass,
                            EvaluationStatus::Fail => ShiftyEvaluationStatus::Fail,
                        },
                        explanation: focus.explain(),
                        evidence_json: serde_json::to_string(&focus.evidence)
                            .map_err(internal_error)?,
                    })
                })
                .collect::<Result<Vec<_>, ApiError>>()?;
            Ok(EvidenceStatementItem {
                source_statement_id: statement.source_statement_id,
                normalized_statement_id: statement.normalized_statement_id.unwrap_or(usize::MAX),
                source_constraint_id: statement.source_constraint_id.0,
                normalized_constraint_id: statement
                    .normalized_constraint_id
                    .map_or(u32::MAX, |id| id.0),
                constraint_kind: constraint_kind_string(statement.constraint_kind),
                target,
                foci,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(ShiftyEvidenceRun {
        conforms,
        json,
        statements,
    })
}

/// `explain` returns statements only — the catalog is fixed per snapshot and is
/// taken once from the session. Wrapping them in an otherwise-empty `EvidenceRun`
/// keeps one JSON shape across both entry points, so the same compact encoder,
/// decoder, and accessors work on either.
fn explanation_run(
    statements: Vec<shifty_engine::StatementEvaluation>,
    raw_schema: &Schema,
) -> Result<ShiftyEvidenceRun, ApiError> {
    let conforms = statements.iter().all(|statement| {
        statement
            .selected_foci
            .iter()
            .all(|focus| focus.status() == EvaluationStatus::Pass)
    });
    build_evidence_run(
        EvidenceRun {
            conforms,
            constraints: shifty_engine::ConstraintCatalog {
                source: Vec::new(),
                normalized: Vec::new(),
            },
            statements,
        },
        raw_schema,
    )
}

/// Parse one RDF term from its N-Triples rendering by reading it back in object
/// position, which is the only position that admits IRIs, blank nodes, *and*
/// literals — so any focus a run reported round-trips through this.
fn parse_term(text: &str) -> Result<Term, ApiError> {
    let triple = format!("<urn:shifty:s> <urn:shifty:p> {text} .");
    let mut parser = oxttl::NTriplesParser::new().for_slice(triple.as_bytes());
    match parser.next() {
        Some(Ok(triple)) => Ok(triple.object),
        Some(Err(error)) => Err(ApiError::new(
            ShiftyStatus::InvalidArgument,
            format!("focus is not a valid N-Triples term: {error}"),
        )),
        None => Err(ApiError::new(
            ShiftyStatus::InvalidArgument,
            "focus is not a valid N-Triples term",
        )),
    }
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
fn engine_validation_options(
    minimum_severity: Severity,
    entry_shape_names: &[String],
) -> EngineValidationOptions {
    EngineValidationOptions {
        minimum_severity,
        sort_results: true,
        entry_shape_names: entry_shape_names.to_vec(),
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

unsafe fn shape_names_from_raw(
    data: *const ShiftyStringView,
    len: usize,
) -> Result<Vec<String>, ApiError> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if data.is_null() {
        return Err(ApiError::new(
            ShiftyStatus::InvalidArgument,
            "shape names pointer is null",
        ));
    }
    unsafe { slice::from_raw_parts(data, len) }
        .iter()
        .enumerate()
        .map(|(index, view)| {
            if view.len != 0 && view.data.is_null() {
                return Err(ApiError::new(
                    ShiftyStatus::InvalidArgument,
                    format!("shape name {index} pointer is null"),
                ));
            }
            let bytes = unsafe { bytes_from_raw(view.data.cast(), view.len) }?;
            std::str::from_utf8(bytes)
                .map(str::to_string)
                .map_err(|error| {
                    ApiError::new(
                        ShiftyStatus::InvalidArgument,
                        format!("shape name {index} is not valid UTF-8: {error}"),
                    )
                })
        })
        .collect()
}

fn string_view(value: &str) -> ShiftyStringView {
    ShiftyStringView {
        data: value.as_ptr().cast(),
        len: value.len(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn shifty_abi_version() -> u32 {
    5
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
            &[],
        )?;
        unsafe { out.write(Box::into_raw(Box::new(result))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_prepared_validator_validate_with_shapes(
    validator: *const ShiftyPreparedValidator,
    dataset: *const ShiftyDataset,
    graph_mode: u32,
    run_inference: u8,
    minimum_severity: u32,
    shape_names: *const ShiftyStringView,
    shape_names_len: usize,
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
        let shape_names = unsafe { shape_names_from_raw(shape_names, shape_names_len) }?;
        let result = validate_dataset(
            validator,
            dataset,
            graph_mode,
            run_inference != 0,
            minimum_severity,
            &shape_names,
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
            &[],
        )?;
        unsafe { out.write(Box::into_raw(Box::new(result))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_prepared_validator_validate_algebra_with_shapes(
    validator: *const ShiftyPreparedValidator,
    dataset: *const ShiftyDataset,
    graph_mode: u32,
    run_inference: u8,
    minimum_severity: u32,
    shape_names: *const ShiftyStringView,
    shape_names_len: usize,
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
        let shape_names = unsafe { shape_names_from_raw(shape_names, shape_names_len) }?;
        let result = validate_algebra_dataset(
            validator,
            dataset,
            graph_mode,
            run_inference != 0,
            minimum_severity,
            &shape_names,
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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_session_create(
    validator: *const ShiftyPreparedValidator,
    dataset: *const ShiftyDataset,
    graph_mode: u32,
    run_inference: u8,
    out: *mut *mut ShiftyEvidenceSession,
) -> u32 {
    ffi_call(|| {
        let validator = unsafe { validator.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "validator is null"))?;
        let dataset = unsafe { dataset.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "dataset is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out session pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let session = prepare_evidence(validator, dataset, graph_mode, run_inference != 0)?;
        unsafe { out.write(Box::into_raw(Box::new(session))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_session_destroy(session: *mut ShiftyEvidenceSession) {
    if !session.is_null() {
        unsafe { drop(Box::from_raw(session)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_session_constraints_json(
    session: *const ShiftyEvidenceSession,
) -> ShiftyStringView {
    unsafe { session.as_ref() }.map_or(empty_view(), |session| {
        string_view(&session.constraints_json)
    })
}

/// For every source constraint with shapes-graph provenance, the values
/// `name_path` reaches from its originating shapes-graph node. `name_path`
/// may be NULL/empty for `sh:name`. The result is an opaque list of
/// (constraint id, names) entries.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_session_binding_names(
    session: *const ShiftyEvidenceSession,
    name_path: *const c_char,
    name_path_len: usize,
    out: *mut *mut ShiftyBindingNameList,
) -> u32 {
    ffi_call(|| {
        let session = unsafe { session.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "session is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out list pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let name_path = unsafe { optional_str_from_raw(name_path, name_path_len, "name path") }?;
        let table = session.binding_names(name_path)?;
        let mut items: Vec<ShapeMapNameItem> = table
            .into_iter()
            .map(|(constraint_id, mut names)| {
                names.sort();
                ShapeMapNameItem { constraint_id, names }
            })
            .collect();
        items.sort_by_key(|item| item.constraint_id);
        unsafe { out.write(Box::into_raw(Box::new(ShiftyBindingNameList { items }))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_binding_name_list_destroy(list: *mut ShiftyBindingNameList) {
    if !list.is_null() {
        unsafe { drop(Box::from_raw(list)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_binding_name_list_len(list: *const ShiftyBindingNameList) -> usize {
    unsafe { list.as_ref() }.map_or(0, |list| list.items.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_binding_name_constraint(
    list: *const ShiftyBindingNameList,
    index: usize,
) -> u32 {
    unsafe { list.as_ref() }
        .and_then(|list| list.items.get(index))
        .map_or(0, |item| item.constraint_id)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_binding_name_value_count(
    list: *const ShiftyBindingNameList,
    index: usize,
) -> usize {
    unsafe { list.as_ref() }
        .and_then(|list| list.items.get(index))
        .map_or(0, |item| item.names.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_binding_name_value(
    list: *const ShiftyBindingNameList,
    index: usize,
    value_index: usize,
) -> ShiftyStringView {
    unsafe { list.as_ref() }
        .and_then(|list| list.items.get(index))
        .and_then(|item| item.names.get(value_index))
        .map_or(empty_view(), |name| string_view(name))
}

/// The raw schema's shape name for `constraint_id` — the IRI of the named
/// (non-blank) RDF node it was lowered from. `*out` is NULL when the
/// constraint has no name (anonymous shape or no such id).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_session_shape_name_of(
    session: *const ShiftyEvidenceSession,
    constraint_id: u32,
    out: *mut *mut ShiftyString,
) -> u32 {
    ffi_call(|| {
        let session = unsafe { session.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "session is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out string pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        if let Some(name) = session.shape_name_of(constraint_id) {
            unsafe { out.write(Box::into_raw(Box::new(ShiftyString { value: name }))) };
        }
        Ok(())
    })
}

/// Batch-evaluate `path` (a SPARQL 1.1 property path) from each of `nodes`
/// (N-Triples spellings) over the session's evaluation graph. The result is
/// an opaque list of (node, reached) entries in input order.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_session_resolve_path(
    session: *const ShiftyEvidenceSession,
    nodes: *const ShiftyStringView,
    nodes_len: usize,
    path: *const c_char,
    path_len: usize,
    out: *mut *mut ShiftyPathResolutionList,
) -> u32 {
    ffi_call(|| {
        let session = unsafe { session.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "session is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out list pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let nodes = unsafe { shape_names_from_raw(nodes, nodes_len) }?;
        let path = unsafe { str_from_raw(path, path_len, "path") }?;
        let table = session.resolve_path(&nodes, path)?;
        // Preserve input order, as Python's resolve_path does (dict
        // insertion order over the input list).
        let entries = nodes
            .into_iter()
            .map(|node| {
                let mut reached = table.get(&node).cloned().unwrap_or_default();
                reached.sort();
                (node, reached)
            })
            .collect();
        unsafe { out.write(Box::into_raw(Box::new(ShiftyPathResolutionList { entries }))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_path_resolution_list_destroy(
    list: *mut ShiftyPathResolutionList,
) {
    if !list.is_null() {
        unsafe { drop(Box::from_raw(list)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_path_resolution_list_len(
    list: *const ShiftyPathResolutionList,
) -> usize {
    unsafe { list.as_ref() }.map_or(0, |list| list.entries.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_path_resolution_node(
    list: *const ShiftyPathResolutionList,
    index: usize,
) -> ShiftyStringView {
    unsafe { list.as_ref() }
        .and_then(|list| list.entries.get(index))
        .map_or(empty_view(), |entry| string_view(&entry.0))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_path_resolution_value_count(
    list: *const ShiftyPathResolutionList,
    index: usize,
) -> usize {
    unsafe { list.as_ref() }
        .and_then(|list| list.entries.get(index))
        .map_or(0, |entry| entry.1.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_path_resolution_value(
    list: *const ShiftyPathResolutionList,
    index: usize,
    value_index: usize,
) -> ShiftyStringView {
    unsafe { list.as_ref() }
        .and_then(|list| list.entries.get(index))
        .and_then(|entry| entry.1.get(value_index))
        .map_or(empty_view(), |value| string_view(value))
}

/// Build the shape map for a run of this session: typed key -> value
/// bindings for every selected (shape, focus) pair. `name_path` may be
/// NULL/empty for `sh:name`; pass a NULL `name_path` pointer with len 0 to
/// skip name resolution entirely.
///
/// `value_paths` is an array of (label, path) pairs evaluated from each
/// bound value over the data graph; NULL with length 0 to skip.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_session_shape_map(
    session: *const ShiftyEvidenceSession,
    run: *const ShiftyEvidenceRun,
    name_path: *const c_char,
    name_path_len: usize,
    value_paths: *const ShiftyStringPair,
    value_paths_len: usize,
    out: *mut *mut ShiftyShapeMap,
) -> u32 {
    ffi_call(|| {
        let session = unsafe { session.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "session is null"))?;
        let run = unsafe { run.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "run is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out shape map pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        // A NULL `name_path` pointer with length 0 means *skip* name
        // resolution; a non-NULL path is used verbatim, and an empty path
        // falls back to `sh:name` (the Python default). A NULL pointer with a
        // non-zero length is rejected below.
        let name_path: Option<&str> = if name_path.is_null() {
            if name_path_len != 0 {
                return Err(ApiError::new(
                    ShiftyStatus::InvalidArgument,
                    "name path pointer is null but length is non-zero",
                ));
            }
            None
        } else {
            let path = unsafe { str_from_raw(name_path, name_path_len, "name path") }?;
            Some(if path.is_empty() { "sh:name" } else { path })
        };
        let mut value_paths_vec = Vec::with_capacity(value_paths_len);
        if value_paths_len > 0 {
            if value_paths.is_null() {
                return Err(ApiError::new(
                    ShiftyStatus::InvalidArgument,
                    "value paths pointer is null",
                ));
            }
            let pairs = unsafe { slice::from_raw_parts(value_paths, value_paths_len) };
            for pair in pairs.iter() {
                let label = unsafe { str_from_raw(pair.first.data.cast(), pair.first.len, "value path label") }?;
                let path = unsafe { str_from_raw(pair.second.data.cast(), pair.second.len, "value path") }?;
                value_paths_vec.push((label.to_string(), path.to_string()));
            }
        }
        let map = session.shape_map(run, name_path, &value_paths_vec)?;
        unsafe { out.write(Box::into_raw(Box::new(map))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_destroy(map: *mut ShiftyShapeMap) {
    if !map.is_null() {
        unsafe { drop(Box::from_raw(map)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_conforms(map: *const ShiftyShapeMap) -> u8 {
    u8::from(unsafe { map.as_ref() }.is_some_and(|map| map.conforms))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_to_json(map: *const ShiftyShapeMap) -> ShiftyStringView {
    unsafe { map.as_ref() }.map_or(empty_view(), |map| string_view(&map.json))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_shape_count(map: *const ShiftyShapeMap) -> usize {
    unsafe { map.as_ref() }.map_or(0, |map| map.shapes.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_shape_name(
    map: *const ShiftyShapeMap,
    shape_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .map_or(empty_view(), |shape| string_view(&shape.name))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_mapping_count(
    map: *const ShiftyShapeMap,
    shape_index: usize,
) -> usize {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .map_or(0, |shape| shape.mappings.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_mapping_focus(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .map_or(empty_view(), |mapping| string_view(&mapping.focus))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_mapping_shape_name(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .map_or(empty_view(), |mapping| string_view(&mapping.shape_name))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_mapping_target(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .map_or(empty_view(), |mapping| string_view(&mapping.target))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_mapping_conforms(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
) -> u8 {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .map_or(0, |mapping| u8::from(mapping.conforms))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_mapping_evidence_json(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .map_or(empty_view(), |mapping| string_view(&mapping.evidence_json))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_mapping_explanation(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .map_or(empty_view(), |mapping| string_view(&mapping.explanation))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_mapping_binding_count(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
) -> usize {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .map_or(0, |mapping| mapping.bindings.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_key(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(empty_view(), |binding| string_view(&binding.key))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_key_path_json(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(empty_view(), |binding| string_view(&binding.key_path_json))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_key_kind(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(empty_view(), |binding| string_view(&binding.kind))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_key_ordinal(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> usize {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(0, |binding| binding.ordinal)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_status(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> ShiftyEvaluationStatus {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(ShiftyEvaluationStatus::Fail, |binding| binding.status)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_source_constraint(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> u32 {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(0, |binding| binding.source_constraint_id)
}

/// `SHIFTY_EVIDENCE_NO_CONSTRAINT` when the binding has no normalized
/// counterpart.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_normalized_constraint(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> u32 {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(u32::MAX, |binding| binding.normalized_constraint_id)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_severity(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(empty_view(), |binding| string_view(&binding.severity))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_name_count(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> usize {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(0, |binding| binding.names.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_name(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
    name_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .and_then(|binding| binding.names.get(name_index))
        .map_or(empty_view(), |name| string_view(name))
}

/// `SHIFTY_EVIDENCE_NO_INDEX` when the source constraint declares no lower
/// bound.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_min(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> usize {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(usize::MAX, |binding| binding.min)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_max(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> usize {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(usize::MAX, |binding| binding.max)
}

/// `SHIFTY_EVIDENCE_NO_INDEX` when the count evidence was not materialized.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_observed(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> usize {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(usize::MAX, |binding| binding.observed)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_missing(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> usize {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(0, |binding| binding.missing)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_value_count(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> usize {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(0, |binding| binding.values.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_value(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
    value_index: usize,
) -> ShiftyTerm {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .and_then(|binding| binding.values.get(value_index))
        .map_or(empty_term(), ShapeMapTermItem::c)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_rejected_value_count(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> usize {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(0, |binding| binding.rejected_values.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_rejected_value(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
    value_index: usize,
) -> ShiftyTerm {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .and_then(|binding| binding.rejected_values.get(value_index))
        .map_or(empty_term(), ShapeMapTermItem::c)
}

/// Empty when the evidence was not materialized.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_evidence_json(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(empty_view(), |binding| string_view(&binding.evidence_json))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_explain(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(empty_view(), |binding| string_view(&binding.explain))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_has_qualifier(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> u8 {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(false, |binding| binding.qualifier.is_some())
        .into()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_qualifier_kind(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> ShiftyQualifierKind {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .and_then(|binding| binding.qualifier.as_ref())
        .map_or(ShiftyQualifierKind::Cls, |qualifier| qualifier.kind)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_qualifier_iri(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .and_then(|binding| binding.qualifier.as_ref())
        .map_or(empty_view(), |qualifier| string_view(&qualifier.iri))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_qualifier_term(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> ShiftyTerm {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .and_then(|binding| binding.qualifier.as_ref())
        .map_or(empty_term(), |qualifier| qualifier.term.c())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_annotation_label_count(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
) -> usize {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .map_or(0, |binding| binding.annotations.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_annotation_label(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
    label_index: usize,
) -> ShiftyStringView {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .and_then(|binding| binding.annotations.get(label_index))
        .map_or(empty_view(), |group| string_view(&group.label))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_annotation_term_count(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
    label_index: usize,
) -> usize {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .and_then(|binding| binding.annotations.get(label_index))
        .map_or(0, |group| group.entries.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_annotation_term(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
    label_index: usize,
    term_index: usize,
) -> ShiftyTerm {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .and_then(|binding| binding.annotations.get(label_index))
        .and_then(|group| group.entries.get(term_index))
        .map_or(empty_term(), |entry| entry.0.c())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_annotation_reached_count(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
    label_index: usize,
    term_index: usize,
) -> usize {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .and_then(|binding| binding.annotations.get(label_index))
        .and_then(|group| group.entries.get(term_index))
        .map_or(0, |entry| entry.1.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_shape_map_binding_annotation_reached(
    map: *const ShiftyShapeMap,
    shape_index: usize,
    mapping_index: usize,
    binding_index: usize,
    label_index: usize,
    term_index: usize,
    reached_index: usize,
) -> ShiftyTerm {
    unsafe { map.as_ref() }
        .and_then(|map| map.shapes.get(shape_index))
        .and_then(|shape| shape.mappings.get(mapping_index))
        .and_then(|mapping| mapping.bindings.get(binding_index))
        .and_then(|binding| binding.annotations.get(label_index))
        .and_then(|group| group.entries.get(term_index))
        .and_then(|entry| entry.1.get(reached_index))
        .map_or(empty_term(), ShapeMapTermItem::c)
}

fn empty_term() -> ShiftyTerm {
    ShiftyTerm {
        kind: ShiftyTermKind::Literal,
        value: empty_view(),
        datatype: empty_view(),
        language: empty_view(),
    }
}


#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_session_validate(
    session: *const ShiftyEvidenceSession,
    minimum_severity: u32,
    shape_names: *const ShiftyStringView,
    shape_names_len: usize,
    out: *mut *mut ShiftyEvidenceRun,
) -> u32 {
    ffi_call(|| {
        let session = unsafe { session.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "session is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out run pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let minimum_severity = severity(minimum_severity)?;
        let shape_names = unsafe { shape_names_from_raw(shape_names, shape_names_len) }?;
        let options = engine_validation_options(minimum_severity, &shape_names);
        let run = build_evidence_run(session.prepared.validate(&options), &session.raw_schema)?;
        unsafe { out.write(Box::into_raw(Box::new(run))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_session_validate_conformance(
    session: *const ShiftyEvidenceSession,
    shape_names: *const ShiftyStringView,
    shape_names_len: usize,
    out: *mut ShiftyConformanceRun,
) -> u32 {
    ffi_call(|| {
        let session = unsafe { session.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "session is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out conformance pointer is null",
            ));
        }
        let shape_names = unsafe { shape_names_from_raw(shape_names, shape_names_len) }?;
        let options = engine_validation_options(Severity::Info, &shape_names);
        unsafe { out.write(session.prepared.validate_conformance(&options).into()) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_session_find_failures(
    session: *const ShiftyEvidenceSession,
    shape_names: *const ShiftyStringView,
    shape_names_len: usize,
    out: *mut *mut ShiftyFailureList,
) -> u32 {
    ffi_call(|| {
        let session = unsafe { session.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "session is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out failure list pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let shape_names = unsafe { shape_names_from_raw(shape_names, shape_names_len) }?;
        let options = engine_validation_options(Severity::Info, &shape_names);
        let (conformance, pairs) = session.prepared.find_failures(&options);
        let foci = pairs.iter().map(|pair| term_string(&pair.focus)).collect();
        let list = ShiftyFailureList {
            conformance: conformance.into(),
            pairs,
            foci,
        };
        unsafe { out.write(Box::into_raw(Box::new(list))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_failure_list_destroy(list: *mut ShiftyFailureList) {
    if !list.is_null() {
        unsafe { drop(Box::from_raw(list)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_failure_list_len(list: *const ShiftyFailureList) -> usize {
    unsafe { list.as_ref() }.map_or(0, |list| list.pairs.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_failure_list_conformance(
    list: *const ShiftyFailureList,
) -> ShiftyConformanceRun {
    unsafe { list.as_ref() }.map_or(
        ShiftyConformanceRun {
            conforms: 0,
            selected_pairs: 0,
            passed: 0,
            failed: 0,
        },
        |list| list.conformance,
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_failure_statement(
    list: *const ShiftyFailureList,
    index: usize,
) -> usize {
    unsafe { list.as_ref() }
        .and_then(|list| list.pairs.get(index))
        .map_or(usize::MAX, |pair| pair.statement)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_failure_focus(
    list: *const ShiftyFailureList,
    index: usize,
) -> ShiftyStringView {
    unsafe { list.as_ref() }
        .and_then(|list| list.foci.get(index))
        .map_or(empty_view(), |focus| string_view(focus))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_session_explain_failure(
    session: *const ShiftyEvidenceSession,
    failures: *const ShiftyFailureList,
    index: usize,
    out: *mut *mut ShiftyEvidenceRun,
) -> u32 {
    ffi_call(|| {
        let session = unsafe { session.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "session is null"))?;
        let failures = unsafe { failures.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "failure list is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out run pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let pair = failures.pairs.get(index).ok_or_else(|| {
            ApiError::new(
                ShiftyStatus::InvalidArgument,
                format!("failure index {index} is out of bounds"),
            )
        })?;
        let run = explanation_run(session.prepared.explain(pair), &session.raw_schema)?;
        unsafe { out.write(Box::into_raw(Box::new(run))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_session_explain(
    session: *const ShiftyEvidenceSession,
    statement: usize,
    focus: *const c_char,
    focus_len: usize,
    out: *mut *mut ShiftyEvidenceRun,
) -> u32 {
    ffi_call(|| {
        let session = unsafe { session.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "session is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out run pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let focus = unsafe { str_from_raw(focus, focus_len, "focus") }?;
        let pair = SelectedPair {
            statement,
            focus: parse_term(focus)?,
        };
        let run = explanation_run(session.prepared.explain(&pair), &session.raw_schema)?;
        unsafe { out.write(Box::into_raw(Box::new(run))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_run_destroy(run: *mut ShiftyEvidenceRun) {
    if !run.is_null() {
        unsafe { drop(Box::from_raw(run)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_run_conforms(run: *const ShiftyEvidenceRun) -> u8 {
    u8::from(unsafe { run.as_ref() }.is_some_and(|run| run.conforms))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_run_json(
    run: *const ShiftyEvidenceRun,
) -> ShiftyStringView {
    unsafe { run.as_ref() }.map_or(empty_view(), |run| string_view(&run.json))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_run_compact_json(
    run: *const ShiftyEvidenceRun,
    include_catalog: u8,
    out: *mut *mut ShiftyString,
) -> u32 {
    ffi_call(|| {
        let run = unsafe { run.as_ref() }
            .ok_or_else(|| ApiError::new(ShiftyStatus::InvalidArgument, "run is null"))?;
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out string pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let value: serde_json::Value = serde_json::from_str(&run.json).map_err(internal_error)?;
        let compact = compact_value(value, include_catalog != 0);
        let value = serde_json::to_string(&compact).map_err(internal_error)?;
        unsafe { out.write(Box::into_raw(Box::new(ShiftyString { value }))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_expand_json(
    compact: *const c_char,
    compact_len: usize,
    catalog: *const c_char,
    catalog_len: usize,
    out: *mut *mut ShiftyString,
) -> u32 {
    ffi_call(|| {
        if out.is_null() {
            return Err(ApiError::new(
                ShiftyStatus::InvalidArgument,
                "out string pointer is null",
            ));
        }
        unsafe { out.write(ptr::null_mut()) };
        let compact = unsafe { str_from_raw(compact, compact_len, "compact evidence") }?;
        let catalog = unsafe { optional_str_from_raw(catalog, catalog_len, "catalog") }?;
        let value: serde_json::Value = serde_json::from_str(compact).map_err(|error| {
            ApiError::new(
                ShiftyStatus::InvalidArgument,
                format!("cannot read compact evidence: {error}"),
            )
        })?;
        let catalog = match catalog {
            Some(text) => serde_json::from_str(text).map_err(|error| {
                ApiError::new(
                    ShiftyStatus::InvalidArgument,
                    format!("cannot read catalog: {error}"),
                )
            })?,
            None => value.get("constraints").cloned().ok_or_else(|| {
                ApiError::new(
                    ShiftyStatus::InvalidArgument,
                    "compact evidence omits its constraint catalog; pass one",
                )
            })?,
        };
        let expanded = expand_value(&value, catalog)
            .map_err(|error| ApiError::new(ShiftyStatus::InvalidArgument, error.to_string()))?;
        let value = serde_json::to_string(&expanded).map_err(internal_error)?;
        unsafe { out.write(Box::into_raw(Box::new(ShiftyString { value }))) };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_string_destroy(value: *mut ShiftyString) {
    if !value.is_null() {
        unsafe { drop(Box::from_raw(value)) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_string_data(value: *const ShiftyString) -> ShiftyStringView {
    unsafe { value.as_ref() }.map_or(empty_view(), |value| string_view(&value.value))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_run_statement_count(
    run: *const ShiftyEvidenceRun,
) -> usize {
    unsafe { run.as_ref() }.map_or(0, |run| run.statements.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_statement_source_id(
    run: *const ShiftyEvidenceRun,
    index: usize,
) -> usize {
    unsafe { run.as_ref() }
        .and_then(|run| run.statements.get(index))
        .map_or(usize::MAX, |statement| statement.source_statement_id)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_statement_normalized_id(
    run: *const ShiftyEvidenceRun,
    index: usize,
) -> usize {
    unsafe { run.as_ref() }
        .and_then(|run| run.statements.get(index))
        .map_or(usize::MAX, |statement| statement.normalized_statement_id)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_statement_source_constraint(
    run: *const ShiftyEvidenceRun,
    index: usize,
) -> u32 {
    unsafe { run.as_ref() }
        .and_then(|run| run.statements.get(index))
        .map_or(u32::MAX, |statement| statement.source_constraint_id)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_statement_normalized_constraint(
    run: *const ShiftyEvidenceRun,
    index: usize,
) -> u32 {
    unsafe { run.as_ref() }
        .and_then(|run| run.statements.get(index))
        .map_or(u32::MAX, |statement| statement.normalized_constraint_id)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_statement_constraint_kind(
    run: *const ShiftyEvidenceRun,
    index: usize,
) -> ShiftyStringView {
    unsafe { run.as_ref() }
        .and_then(|run| run.statements.get(index))
        .map_or(empty_view(), |statement| {
            string_view(&statement.constraint_kind)
        })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_statement_target(
    run: *const ShiftyEvidenceRun,
    index: usize,
) -> ShiftyStringView {
    unsafe { run.as_ref() }
        .and_then(|run| run.statements.get(index))
        .map_or(empty_view(), |statement| string_view(&statement.target))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_statement_focus_count(
    run: *const ShiftyEvidenceRun,
    index: usize,
) -> usize {
    unsafe { run.as_ref() }
        .and_then(|run| run.statements.get(index))
        .map_or(0, |statement| statement.foci.len())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_focus_node(
    run: *const ShiftyEvidenceRun,
    index: usize,
    focus_index: usize,
) -> ShiftyStringView {
    unsafe { run.as_ref() }
        .and_then(|run| run.statements.get(index))
        .and_then(|statement| statement.foci.get(focus_index))
        .map_or(empty_view(), |focus| string_view(&focus.focus))
}

/// Returns `SHIFTY_EVALUATION_FAIL` for an out-of-range index; check
/// `shifty_evidence_statement_focus_count` first.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_focus_status(
    run: *const ShiftyEvidenceRun,
    index: usize,
    focus_index: usize,
) -> u32 {
    unsafe { run.as_ref() }
        .and_then(|run| run.statements.get(index))
        .and_then(|statement| statement.foci.get(focus_index))
        .map_or(ShiftyEvaluationStatus::Fail as u32, |focus| {
            focus.status as u32
        })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_focus_evidence_json(
    run: *const ShiftyEvidenceRun,
    index: usize,
    focus_index: usize,
) -> ShiftyStringView {
    unsafe { run.as_ref() }
        .and_then(|run| run.statements.get(index))
        .and_then(|statement| statement.foci.get(focus_index))
        .map_or(empty_view(), |focus| string_view(&focus.evidence_json))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn shifty_evidence_focus_explanation(
    run: *const ShiftyEvidenceRun,
    index: usize,
    focus_index: usize,
) -> ShiftyStringView {
    unsafe { run.as_ref() }
        .and_then(|run| run.statements.get(index))
        .and_then(|statement| statement.foci.get(focus_index))
        .map_or(empty_view(), |focus| string_view(&focus.explanation))
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

    const EVIDENCE_SHAPES: &str = r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://example.com/> .
        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [ sh:path ex:name ; sh:minCount 1 ] .
    "#;

    const EVIDENCE_DATA: &str = r#"
        @prefix ex: <http://example.com/> .
        ex:alice a ex:Person ; ex:name "Alice" .
        ex:bob a ex:Person .
    "#;

    fn evidence_session() -> ShiftyEvidenceSession {
        let validator = prepare(parse_bytes(EVIDENCE_SHAPES.as_bytes(), 0, None).unwrap());
        let mut dataset = ShiftyDataset::new();
        dataset.extend(parse_bytes(EVIDENCE_DATA.as_bytes(), 0, None).unwrap());
        prepare_evidence(&validator, &dataset, ShiftyGraphMode::Union as u32, true).unwrap()
    }

    /// Both polarities are present: a report would carry only bob.
    #[test]
    fn evidence_run_covers_passes_and_failures() {
        let session = evidence_session();
        let options = engine_validation_options(Severity::Info, &[]);
        let run =
            build_evidence_run(session.prepared.validate(&options), &session.raw_schema).unwrap();

        assert!(!run.conforms);
        let foci: Vec<_> = run
            .statements
            .iter()
            .flat_map(|statement| &statement.foci)
            .map(|focus| (focus.focus.as_str(), focus.status))
            .collect();
        assert!(foci.contains(&("<http://example.com/alice>", ShiftyEvaluationStatus::Pass)));
        assert!(foci.contains(&("<http://example.com/bob>", ShiftyEvaluationStatus::Fail)));
    }

    /// Compaction round-trips as JSON *values*: the encoding is lossless, but
    /// object key order is not part of what it preserves, so comparing the
    /// serialized text would test serde_json's map ordering instead.
    #[test]
    fn compaction_round_trips_with_and_without_the_catalog() {
        let session = evidence_session();
        let options = engine_validation_options(Severity::Info, &[]);
        let run =
            build_evidence_run(session.prepared.validate(&options), &session.raw_schema).unwrap();
        let original: serde_json::Value = serde_json::from_str(&run.json).unwrap();
        let catalog: serde_json::Value = serde_json::from_str(&session.constraints_json).unwrap();

        let full = compact_value(original.clone(), true);
        assert_eq!(
            expand_value(&full, full.get("constraints").cloned().unwrap()).unwrap(),
            original
        );

        let elided = compact_value(original.clone(), false);
        assert!(elided.get("constraints").is_none());
        assert_eq!(expand_value(&elided, catalog).unwrap(), original);
    }

    /// Explaining a pair reaches the same evidence the whole run carries, which
    /// is what makes scan-then-explain a substitute for materializing everything.
    #[test]
    fn explaining_a_failure_matches_the_full_run() {
        let session = evidence_session();
        let options = engine_validation_options(Severity::Info, &[]);
        let run =
            build_evidence_run(session.prepared.validate(&options), &session.raw_schema).unwrap();
        let (conformance, pairs) = session.prepared.find_failures(&options);

        assert_eq!(conformance.failed, 1);
        assert_eq!(pairs.len(), 1);
        assert_eq!(term_string(&pairs[0].focus), "<http://example.com/bob>");

        let explained =
            explanation_run(session.prepared.explain(&pairs[0]), &session.raw_schema).unwrap();
        assert!(!explained.conforms);
        let explained_evidence = &explained.statements[0].foci[0].evidence_json;
        let from_run = run
            .statements
            .iter()
            .flat_map(|statement| &statement.foci)
            .find(|focus| focus.focus == "<http://example.com/bob>")
            .unwrap();
        assert_eq!(&from_run.evidence_json, explained_evidence);
    }

    /// Every position a focus term can take must survive being named as a
    /// string and read back — literals included, which is why the parser reads
    /// the term in object position.
    #[test]
    fn focus_terms_round_trip_through_their_rendering() {
        for text in [
            "<http://example.com/bob>",
            "_:b0",
            "\"Alice\"",
            "\"Alice\"@en",
            "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        ] {
            assert_eq!(term_string(&parse_term(text).unwrap()), text);
        }
        assert!(parse_term("not a term").is_err());
    }
}
