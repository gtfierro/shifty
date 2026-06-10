use oxrdf::{NamedNode, Triple};
use shacl_engine::{infer, validate};
use std::path::{Path, PathBuf};

fn fixture(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata")
        .join(path)
}

fn load(path: &str) -> (shacl_parse::Loaded, shacl_parse::ParseOutput) {
    let path = fixture(path).canonicalize().expect("fixture");
    let bytes = std::fs::read(&path).expect("fixture");
    let base = format!("file://{}", path.display());
    let loaded = shacl_parse::load_turtle(&bytes, Some(&base)).expect("valid Turtle");
    let parsed = shacl_parse::parse_turtle(&bytes, Some(&base)).expect("valid shapes");
    assert!(
        parsed.diagnostics.is_empty(),
        "diags: {:?}",
        parsed.diagnostics
    );
    (loaded, parsed)
}

#[test]
fn w3c_node_sparql_constraint() {
    let (loaded, parsed) =
        load("data-shapes/data-shapes-test-suite/tests/sparql/node/sparql-001.ttl");
    let outcome = validate(&loaded.graph, &parsed.schema).expect("stratifiable");
    assert!(!outcome.conforms);
    assert_eq!(outcome.violations.len(), 2);
    assert_eq!(
        outcome
            .violations
            .iter()
            .map(|violation| violation.reasons.len())
            .sum::<usize>(),
        3
    );
}

#[test]
fn w3c_property_sparql_constraint_prebinds_path() {
    let (loaded, parsed) = load(
        "data-shapes/data-shapes-test-suite/tests/sparql/property/sparql-001.ttl",
    );
    let outcome = validate(&loaded.graph, &parsed.schema).expect("stratifiable");
    assert!(!outcome.conforms);
    assert_eq!(outcome.violations.len(), 1);
    assert_eq!(outcome.violations[0].reasons.len(), 1);
    assert_eq!(outcome.violations[0].reasons[0].value.to_string(), "\"Spain\"@en");
}

#[test]
fn w3c_sparql_target() {
    let (loaded, parsed) = load("test-suite/advanced/target/sparqlTarget-001.test.ttl");
    let outcome = validate(&loaded.graph, &parsed.schema).expect("stratifiable");
    assert!(!outcome.conforms);
    assert_eq!(outcome.violations.len(), 1);
    assert_eq!(
        outcome.violations[0].focus.to_string(),
        "<http://datashapes.org/sh/tests/sparql/target/sparqlTarget-001.test#InvalidInstance1>"
    );
}

#[test]
fn w3c_sparql_construct_rule() {
    let (loaded, parsed) = load("test-suite/advanced/rules/sparql/classify-square.test.ttl");
    let outcome = infer(&loaded.graph, &parsed.schema).expect("stratifiable");
    assert!(
        outcome.diagnostics.is_empty(),
        "diags: {:?}",
        outcome.diagnostics
    );
    assert!(outcome.graph.contains(&Triple::new(
        NamedNode::new(
            "http://datashapes.org/shasf/tests/rules/sparql/classify-square.test#SquareRectangle",
        )
        .unwrap(),
        NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap(),
        NamedNode::new(
            "http://datashapes.org/shasf/tests/rules/sparql/classify-square.test#Square",
        )
        .unwrap(),
    )));
}

#[test]
fn construct_blank_nodes_are_rejected_to_preserve_termination() {
    let ttl = br#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ;
            sh:targetNode ex:x ;
            sh:rule [
                a sh:SPARQLRule ;
                sh:construct "CONSTRUCT { $this ex:p [] } WHERE {}"
            ] .
    "#;
    let loaded = shacl_parse::load_turtle(ttl, None).expect("valid Turtle");
    let parsed = shacl_parse::parse_turtle(ttl, None).expect("valid shapes");
    let outcome = infer(&loaded.graph, &parsed.schema).expect("stratifiable");
    assert!(outcome.inferred.is_empty());
    assert!(outcome.diagnostics.iter().any(|message| message.contains("blank nodes")));
}
