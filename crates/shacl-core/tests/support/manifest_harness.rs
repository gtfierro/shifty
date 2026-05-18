use oxigraph::io::{RdfFormat, RdfParser};
use oxrdf::{NamedOrBlankNode, NamedNode, Term, Triple};
use shifty_shacl_core::source::{RefreshMode, ShapeSource, SourceLoadOptions, load_with_ontoenv};
use shifty_shacl_core::{
    BackendViewOptions, ValidationBackend, ValidationResult, build_validation_report,
    derive_validation_logical_plan, lower_to_program,
};
use shifty_shacl_core_inmemory::InMemoryValidationBackend;
use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};

const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const MF_ENTRIES: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#entries";
const MF_INCLUDE: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#include";
const MF_RESULT: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#result";
const MF_STATUS: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#status";
const SH_CONFORMS: &str = "http://www.w3.org/ns/shacl#conforms";
const SH_RESULT: &str = "http://www.w3.org/ns/shacl#result";
const SHT_APPROVED: &str = "http://www.w3.org/ns/shacl-test#approved";
const SHT_DATA_GRAPH: &str = "http://www.w3.org/ns/shacl-test#dataGraph";
const SHT_FAILURE: &str = "http://www.w3.org/ns/shacl-test#Failure";
const SHT_SHAPES_GRAPH: &str = "http://www.w3.org/ns/shacl-test#shapesGraph";
const SHT_VALIDATE: &str = "http://www.w3.org/ns/shacl-test#Validate";

#[derive(Debug, Clone)]
pub struct ManifestCase {
    pub manifest_path: PathBuf,
    pub case_subject: String,
    pub label: Option<String>,
    pub status: String,
    pub data_path: PathBuf,
    pub shapes_path: PathBuf,
    pub expected: ManifestExpectation,
}

#[derive(Debug, Clone)]
pub enum ManifestExpectation {
    Report {
        conforms: bool,
        result_count: usize,
    },
    Failure,
}

#[derive(Debug, Clone)]
pub enum CaseSupport {
    Supported,
    Skipped(&'static str),
}

#[derive(Debug, Clone)]
pub struct BackendManifestMetadata {
    pub supported_regions: Vec<&'static str>,
    pub skipped_regions: Vec<SkippedManifestRegion>,
    pub known_divergence_cases: Vec<KnownDivergenceCase>,
}

#[derive(Debug, Clone)]
pub struct SkippedManifestRegion {
    pub prefix: &'static str,
    pub reason: &'static str,
}

#[derive(Debug, Clone)]
pub struct KnownDivergenceCase {
    pub manifest_path: &'static str,
    pub reason: &'static str,
}

#[derive(Debug, Clone)]
pub struct SuiteOutcome {
    pub total_cases: usize,
    pub executed_cases: usize,
    pub skipped_cases: Vec<(String, &'static str)>,
}

#[derive(Debug, Clone)]
pub struct ManifestFailure {
    pub case_key: String,
    pub message: String,
}

pub trait ManifestValidationBackend {
    fn backend_name(&self) -> &'static str;
    fn manifest_metadata(&self) -> BackendManifestMetadata;
    fn support_for_case(&self, case: &ManifestCase) -> CaseSupport;
    fn execute_case(&self, case: &ManifestCase) -> Result<ValidationResult, String>;
}

pub fn suite_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../lib/tests/test-suite")
        .join(name)
}

fn suite_root_path() -> PathBuf {
    suite_path("")
        .canonicalize()
        .unwrap_or_else(|_| suite_path(""))
}

pub fn collect_manifest_cases(root_relative_path: &str) -> Vec<ManifestCase> {
    let root = suite_path(root_relative_path);
    let mut visited = HashSet::new();
    let mut cases = Vec::new();
    collect_manifest_cases_from_path(&root, &mut visited, &mut cases);
    cases.sort_by(|left, right| case_key(left).cmp(&case_key(right)));
    cases
}

pub fn run_manifest_suite<B: ManifestValidationBackend>(
    backend: &B,
    root_relative_path: &str,
) -> SuiteOutcome {
    let cases = collect_manifest_cases(root_relative_path);
    let mut executed_cases = 0usize;
    let mut skipped_cases = Vec::new();
    let mut failures = Vec::new();
    for case in cases {
        match backend.support_for_case(&case) {
            CaseSupport::Skipped(reason) => {
                skipped_cases.push((case_key(&case), reason));
            }
            CaseSupport::Supported => {
                executed_cases += 1;
                if let Err(message) = assert_case(backend, &case) {
                    failures.push(ManifestFailure {
                        case_key: case_key(&case),
                        message,
                    });
                }
            }
        }
    }
    if !failures.is_empty() {
        let mut message = format!(
            "{} manifest suite failures under {}:",
            backend.backend_name(),
            root_relative_path
        );
        for failure in failures {
            message.push_str(&format!("\n- {}: {}", failure.case_key, failure.message));
        }
        panic!("{message}");
    }
    SuiteOutcome {
        total_cases: executed_cases + skipped_cases.len(),
        executed_cases,
        skipped_cases,
    }
}

pub fn classify_case_support(
    metadata: &BackendManifestMetadata,
    case: &ManifestCase,
) -> CaseSupport {
    if case.status != SHT_APPROVED {
        return CaseSupport::Skipped("non-approved manifest status");
    }
    let relative_manifest = relative_suite_manifest_path(&case.manifest_path);
    if matches!(case.expected, ManifestExpectation::Failure) {
        return CaseSupport::Skipped("manifest failure expectations not modeled yet");
    }
    for region in &metadata.skipped_regions {
        if relative_manifest.starts_with(region.prefix) {
            return CaseSupport::Skipped(region.reason);
        }
    }
    for divergence in &metadata.known_divergence_cases {
        if relative_manifest == divergence.manifest_path {
            return CaseSupport::Skipped(divergence.reason);
        }
    }
    CaseSupport::Supported
}

fn assert_case<B: ManifestValidationBackend>(
    backend: &B,
    case: &ManifestCase,
) -> Result<(), String> {
    match &case.expected {
        ManifestExpectation::Failure => match backend.execute_case(case) {
            Ok(result) => Err(format!(
                "expected backend failure but got conforms={} violations={}",
                result.conforms,
                result.violations.len()
            )),
            Err(_) => Ok(()),
        },
        ManifestExpectation::Report {
            conforms,
            result_count,
        } => {
            let result = backend.execute_case(case)?;
            let resolved_shapes = load_resolved(&case.shapes_path)?;
            let syntax = shifty_shacl_core::parse_resolved(&resolved_shapes);
            let program = lower_to_program(&syntax);
            let report = build_validation_report(&result, &program);
            let actual_report_count = report
                .quads
                .iter()
                .filter(|quad| quad.predicate.as_str() == SH_RESULT)
                .count();
            if result.conforms != *conforms {
                return Err(format!(
                    "expected conforms={} but got {}",
                    conforms, result.conforms
                ));
            }
            if result.violations.len() != *result_count {
                return Err(format!(
                    "expected {} violations but got {}",
                    result_count,
                    result.violations.len()
                ));
            }
            if actual_report_count != *result_count {
                return Err(format!(
                    "expected {} report results but got {}",
                    result_count, actual_report_count
                ));
            }
            Ok(())
        }
    }
}

fn collect_manifest_cases_from_path(
    path: &Path,
    visited: &mut HashSet<PathBuf>,
    cases: &mut Vec<ManifestCase>,
) {
    let canonical = path.to_path_buf();
    if !visited.insert(canonical.clone()) {
        return;
    }
    let triples = load_triples(path);
    for include in triples
        .iter()
        .filter(|triple| triple.predicate.as_str() == MF_INCLUDE)
        .filter_map(|triple| term_named_node(&triple.object))
    {
        let include_path = resolve_graph_reference(path, include);
        collect_manifest_cases_from_path(&include_path, visited, cases);
    }
    let entry_subjects = triples
        .iter()
        .filter(|triple| triple.predicate.as_str() == MF_ENTRIES)
        .flat_map(|triple| rdf_list_members(&triples, &triple.object))
        .collect::<Vec<_>>();
    for entry in entry_subjects {
        if let Some(case) = parse_manifest_case(path, &triples, &entry) {
            cases.push(case);
        }
    }
}

fn parse_manifest_case(path: &Path, triples: &[Triple], entry: &Term) -> Option<ManifestCase> {
    let subject = term_to_subject(entry)?;
    if !triples.iter().any(|triple| {
        triple.subject == subject
            && triple.predicate.as_str() == RDF_TYPE
            && triple.object.to_string() == format!("<{}>", SHT_VALIDATE)
    }) {
        return None;
    }
    let status = triples
        .iter()
        .find(|triple| triple.subject == subject && triple.predicate.as_str() == MF_STATUS)
        .and_then(|triple| term_named_node(&triple.object))
        .map(|node| node.as_str().to_string())
        .unwrap_or_default();
    let action_node = triples
        .iter()
        .find(|triple| triple.subject == subject && triple.predicate.as_str() == "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action")
        .map(|triple| triple.object.clone())?;
    let data_path = triples
        .iter()
        .find(|triple| {
            subject_matches_term(&triple.subject, &action_node)
                && triple.predicate.as_str() == SHT_DATA_GRAPH
        })
        .and_then(|triple| term_named_node(&triple.object))
        .map(|node| resolve_graph_reference(path, node))?;
    let shapes_path = triples
        .iter()
        .find(|triple| {
            subject_matches_term(&triple.subject, &action_node)
                && triple.predicate.as_str() == SHT_SHAPES_GRAPH
        })
        .and_then(|triple| term_named_node(&triple.object))
        .map(|node| resolve_graph_reference(path, node))?;
    let expected = triples
        .iter()
        .find(|triple| triple.subject == subject && triple.predicate.as_str() == MF_RESULT)
        .and_then(|triple| parse_manifest_expectation(triples, &triple.object))?;
    let label = triples
        .iter()
        .find(|triple| {
            triple.subject == subject
                && triple.predicate.as_str() == "http://www.w3.org/2000/01/rdf-schema#label"
        })
        .and_then(|triple| match &triple.object {
            Term::Literal(literal) => Some(literal.value().to_string()),
            _ => None,
        });
    Some(ManifestCase {
        manifest_path: path.to_path_buf(),
        case_subject: subject.to_string(),
        label,
        status,
        data_path,
        shapes_path,
        expected,
    })
}

fn parse_manifest_expectation(triples: &[Triple], object: &Term) -> Option<ManifestExpectation> {
    if let Some(node) = term_named_node(object) {
        if node.as_str() == SHT_FAILURE {
            return Some(ManifestExpectation::Failure);
        }
    }
    let conforms = triples
        .iter()
        .find(|triple| subject_matches_term(&triple.subject, object) && triple.predicate.as_str() == SH_CONFORMS)
        .and_then(|triple| match &triple.object {
            Term::Literal(literal) => Some(literal.value() == "true"),
            _ => None,
        })?;
    let result_count = triples
        .iter()
        .filter(|triple| subject_matches_term(&triple.subject, object) && triple.predicate.as_str() == SH_RESULT)
        .count();
    Some(ManifestExpectation::Report {
        conforms,
        result_count,
    })
}

fn load_resolved(path: &Path) -> Result<shifty_shacl_core::source::ResolvedShapeSet, String> {
    load_with_ontoenv(
        &[ShapeSource::File(path.to_path_buf())],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .map_err(|error| error.to_string())
}

fn load_triples(path: &Path) -> Vec<Triple> {
    let file = File::open(path).expect("manifest file should open");
    let base_iri = format!(
        "file://{}",
        path.canonicalize()
            .expect("manifest path should canonicalize")
            .to_string_lossy()
    );
    let parser = RdfParser::from_format(RdfFormat::Turtle)
        .with_base_iri(&base_iri)
        .expect("manifest base IRI should be valid")
        .without_named_graphs();
    parser
        .for_reader(file)
        .map(|result| result.map(Into::into))
        .collect::<Result<Vec<Triple>, _>>()
        .expect("manifest should parse")
}

fn rdf_list_members(triples: &[Triple], head: &Term) -> Vec<Term> {
    let mut members = Vec::new();
    let mut current = head.clone();
    while current.to_string() != format!("<{}>", RDF_NIL) {
        let first = triples
            .iter()
            .find(|triple| subject_matches_term(&triple.subject, &current) && triple.predicate.as_str() == RDF_FIRST)
            .map(|triple| triple.object.clone());
        let rest = triples
            .iter()
            .find(|triple| subject_matches_term(&triple.subject, &current) && triple.predicate.as_str() == RDF_REST)
            .map(|triple| triple.object.clone());
        match (first, rest) {
            (Some(first), Some(rest)) => {
                members.push(first);
                current = rest;
            }
            _ => break,
        }
    }
    members
}

fn resolve_graph_reference(current_path: &Path, node: &NamedNode) -> PathBuf {
    let iri = node.as_str();
    if let Some(path) = iri.strip_prefix("file://") {
        return PathBuf::from(path);
    }
    let current_dir = current_path
        .parent()
        .expect("manifest paths should have a parent directory");
    let candidate_name = iri
        .rsplit(['#', '/', ':'])
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or_default();
    let sibling = current_dir.join(candidate_name);
    if sibling.exists() {
        sibling
    } else {
        current_path.to_path_buf()
    }
}

fn term_to_subject(term: &Term) -> Option<NamedOrBlankNode> {
    match term {
        Term::NamedNode(node) => Some(NamedOrBlankNode::NamedNode(node.clone())),
        Term::BlankNode(node) => Some(NamedOrBlankNode::BlankNode(node.clone())),
        _ => None,
    }
}

fn term_named_node(term: &Term) -> Option<&NamedNode> {
    match term {
        Term::NamedNode(node) => Some(node),
        _ => None,
    }
}

fn subject_matches_term(subject: &NamedOrBlankNode, term: &Term) -> bool {
    match (subject, term) {
        (NamedOrBlankNode::NamedNode(left), Term::NamedNode(right)) => left == right,
        (NamedOrBlankNode::BlankNode(left), Term::BlankNode(right)) => left == right,
        _ => false,
    }
}

fn case_key(case: &ManifestCase) -> String {
    let suite_root = suite_root_path();
    let manifest = case
        .manifest_path
        .strip_prefix(&suite_root)
        .unwrap_or(&case.manifest_path)
        .display()
        .to_string();
    let label = case.label.clone().unwrap_or_else(|| case.case_subject.clone());
    format!("{manifest} :: {label}")
}

fn relative_suite_manifest_path(path: &Path) -> String {
    let suite_root = suite_root_path();
    path.canonicalize()
        .ok()
        .and_then(|canonical| {
            canonical
                .strip_prefix(&suite_root)
                .ok()
                .map(Path::to_path_buf)
        })
        .unwrap_or_else(|| path.to_path_buf())
        .display()
        .to_string()
}

pub struct InMemoryManifestBackend;

impl ManifestValidationBackend for InMemoryManifestBackend {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn manifest_metadata(&self) -> BackendManifestMetadata {
        BackendManifestMetadata {
            supported_regions: vec!["core/", "sparql/", "advanced/"],
            skipped_regions: vec![
                SkippedManifestRegion {
                    prefix: "sparql/pre-binding/",
                    reason: "pre-binding manifest semantics not modeled yet",
                },
                SkippedManifestRegion {
                    prefix: "advanced/expression/",
                    reason: "advanced function/expression execution not implemented",
                },
                SkippedManifestRegion {
                    prefix: "advanced/function/",
                    reason: "advanced function/expression execution not implemented",
                },
            ],
            known_divergence_cases: KNOWN_IN_MEMORY_DIVERGENCES
                .iter()
                .map(|(manifest_path, reason)| KnownDivergenceCase {
                    manifest_path,
                    reason,
                })
                .collect(),
        }
    }

    fn support_for_case(&self, case: &ManifestCase) -> CaseSupport {
        classify_case_support(&self.manifest_metadata(), case)
    }

    fn execute_case(&self, case: &ManifestCase) -> Result<ValidationResult, String> {
        let resolved_shapes = load_resolved(&case.shapes_path)?;
        let resolved_data = if case.data_path == case.shapes_path {
            resolved_shapes.clone()
        } else {
            load_resolved(&case.data_path)?
        };
        let syntax = shifty_shacl_core::parse_resolved(&resolved_shapes);
        let program = lower_to_program(&syntax);
        let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
        let backend = InMemoryValidationBackend;
        backend.execute(&plan, &resolved_shapes.merged_with(&resolved_data))
    }
}

const KNOWN_IN_MEMORY_DIVERGENCES: &[(&str, &str)] = &[
    ("core/complex/personexample.ttl", "known in-memory backend divergence"),
    ("core/complex/shacl-shacl.ttl", "known in-memory backend divergence"),
    ("core/property/uniqueLang-002.ttl", "known in-memory backend divergence"),
    ("core/validation-reports/shared.ttl", "known in-memory backend divergence"),
    ("sparql/component/optional-001.ttl", "known in-memory backend divergence"),
    ("sparql/component/propertyValidator-select-001.ttl", "known in-memory backend divergence"),
    ("sparql/node/prefixes-001.ttl", "known in-memory backend divergence"),
    ("sparql/pre-binding/shapesGraph-001.ttl", "known in-memory backend divergence"),
];
