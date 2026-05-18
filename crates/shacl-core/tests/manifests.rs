use oxigraph::io::{RdfFormat, RdfParser};
use oxrdf::{NamedOrBlankNode, Term};
use shifty_shacl_core::source::{RefreshMode, ShapeSource, SourceLoadOptions, load_with_ontoenv};
use shifty_shacl_core::{
    BackendViewOptions, InMemoryValidationBackend, ValidationBackend, build_validation_report,
    derive_validation_logical_plan, lower_to_program,
};
use std::fs::File;
use std::path::{Path, PathBuf};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const MF_RESULT: &str = "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#result";
const SH_CONFORMS: &str = "http://www.w3.org/ns/shacl#conforms";
const SH_RESULT: &str = "http://www.w3.org/ns/shacl#result";
const SHT_VALIDATE: &str = "http://www.w3.org/ns/shacl-test#Validate";

#[derive(Debug, Clone, Copy)]
struct ManifestExpectation {
    conforms: bool,
    result_count: usize,
}

fn suite_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../lib/tests/test-suite")
        .join(name)
}

fn load_manifest_expectation(path: &Path) -> ManifestExpectation {
    let file = File::open(path).expect("manifest file should open");
    let parser = RdfParser::from_format(RdfFormat::Turtle).without_named_graphs();
    let triples = parser
        .for_reader(file)
        .collect::<Result<Vec<_>, _>>()
        .expect("manifest should parse");
    let validate_subject = triples
        .iter()
        .find(|triple| {
            triple.predicate.as_str() == RDF_TYPE
                && triple.object.to_string() == format!("<{}>", SHT_VALIDATE)
        })
        .map(|triple| triple.subject.clone())
        .expect("manifest should contain a sht:Validate case");
    let report_node = triples
        .iter()
        .find(|triple| triple.subject == validate_subject && triple.predicate.as_str() == MF_RESULT)
        .map(|triple| triple.object.clone())
        .expect("manifest should contain mf:result");
    let conforms = triples
        .iter()
        .find(|triple| {
            subject_matches_term(&triple.subject, &report_node)
                && triple.predicate.as_str() == SH_CONFORMS
        })
        .and_then(|triple| match &triple.object {
            Term::Literal(literal) => Some(literal.value() == "true"),
            _ => None,
        })
        .expect("manifest result should include sh:conforms");
    let result_count = triples
        .iter()
        .filter(|triple| {
            subject_matches_term(&triple.subject, &report_node)
                && triple.predicate.as_str() == SH_RESULT
        })
        .count();
    ManifestExpectation {
        conforms,
        result_count,
    }
}

fn subject_matches_term(subject: &NamedOrBlankNode, term: &Term) -> bool {
    match (subject, term) {
        (NamedOrBlankNode::NamedNode(left), Term::NamedNode(right)) => left == right,
        (NamedOrBlankNode::BlankNode(left), Term::BlankNode(right)) => left == right,
        _ => false,
    }
}

fn run_self_contained_manifest_case(relative_path: &str) {
    let path = suite_path(relative_path);
    let expected = load_manifest_expectation(&path);
    let resolved = load_with_ontoenv(
        &[ShapeSource::File(path)],
        &SourceLoadOptions {
            include_imports: true,
            import_depth: -1,
            temporary_env: true,
            refresh_mode: RefreshMode::UseCache,
        },
    )
    .expect("fixture should load");
    let syntax = shifty_shacl_core::parse_resolved(&resolved);
    let program = lower_to_program(&syntax);
    let plan = derive_validation_logical_plan(&program, BackendViewOptions::default());
    let backend = InMemoryValidationBackend;
    let result = backend
        .execute(&plan, &resolved)
        .expect("validation executes");
    let report = build_validation_report(&result, &plan.view.program);

    assert_eq!(result.conforms, expected.conforms);
    assert_eq!(result.violations.len(), expected.result_count);
    assert_eq!(
        report
            .quads
            .iter()
            .filter(|quad| quad.predicate.as_str() == SH_RESULT)
            .count(),
        expected.result_count
    );
}

#[test]
fn manifest_core_hasvalue_case_matches_expected_conformance() {
    run_self_contained_manifest_case("core/node/hasValue-001.ttl");
}

#[test]
fn manifest_sparql_node_case_matches_expected_conformance() {
    run_self_contained_manifest_case("sparql/node/sparql-001.ttl");
}

#[test]
#[ignore = "custom component attachment resolution for validator-001 is not wired through this manifest case yet"]
fn manifest_sparql_component_case_matches_expected_conformance() {
    run_self_contained_manifest_case("sparql/component/validator-001.ttl");
}
