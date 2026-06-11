use oxrdf::{NamedNode, Triple};
use shacl_engine::{infer, validate, validate_report};
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

/// SHACL `$PATH` prebinding for complex (non-predicate) property shapes.
/// Each sub-test uses `validate_report` so that `collect_sparql` on the
/// report path exercises the same `compile_constraint` code that the
/// algebra path uses.
mod complex_path_prebinding {
    use super::*;

    fn shapes_data(shapes_ttl: &str) -> shacl_parse::Loaded {
        shacl_parse::load_turtle(shapes_ttl.as_bytes(), None).expect("valid Turtle")
    }

    fn prefix() -> &'static str {
        r#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix ex:  <http://example.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        "#
    }

    /// `$PATH` is an `sh:alternativePath` — rewrites the BGP triple to a
    /// SPARQL `|` property-path pattern.
    #[test]
    fn alternative_path() {
        let ttl = format!(
            r#"{prefix}
            ex:S a sh:PropertyShape ;
                sh:path [ sh:alternativePath ( ex:p ex:q ) ] ;
                sh:targetClass ex:C ;
                sh:sparql [
                    sh:select """
                        SELECT $this ?value
                        WHERE {{ $this $PATH ?value .
                                 FILTER (isLiteral(?value) && str(?value) = "bad") }}
                    """ ] .
            ex:Good  a ex:C ; ex:p "ok" .
            ex:Bad1  a ex:C ; ex:p "bad" .
            ex:Bad2  a ex:C ; ex:q "bad" .
            "#,
            prefix = prefix()
        );
        let loaded = shapes_data(&ttl);
        let report = validate_report(&loaded, &loaded.graph);
        assert!(!report.conforms);
        let focuses: Vec<_> = report.results.iter().map(|r| r.focus.to_string()).collect();
        assert!(focuses.contains(&"<http://example.org/Bad1>".to_string()), "{focuses:?}");
        assert!(focuses.contains(&"<http://example.org/Bad2>".to_string()), "{focuses:?}");
        assert!(!focuses.contains(&"<http://example.org/Good>".to_string()), "{focuses:?}");
    }

    /// `$PATH` is an `sh:inversePath` — rewrites to `^ex:p`.
    #[test]
    fn inverse_path() {
        let ttl = format!(
            r#"{prefix}
            ex:S a sh:PropertyShape ;
                sh:path [ sh:inversePath ex:child ] ;
                sh:targetClass ex:Person ;
                sh:sparql [
                    sh:select """
                        SELECT $this ?value
                        WHERE {{ $this $PATH ?value .
                                 FILTER (!isIRI(?value)) }}
                    """ ] .
            ex:Alice a ex:Person .
            ex:Bob   a ex:Person ; ex:child ex:Alice .
            "#,
            prefix = prefix()
        );
        let loaded = shapes_data(&ttl);
        let report = validate_report(&loaded, &loaded.graph);
        // ex:Alice has ex:Bob as inverse-child (ex:Bob ex:child ex:Alice)
        // ex:Bob has no inverse-child
        // The constraint fires when the value node is not an IRI, but
        // ex:Bob is an IRI, so nothing should violate.
        assert!(report.conforms, "results: {:?}", report.results);
    }

    /// `$PATH` is an `sh:sequencePath` — rewrites to `ex:p/ex:q`.
    #[test]
    fn sequence_path() {
        let ttl = format!(
            r#"{prefix}
            ex:S a sh:PropertyShape ;
                sh:path ( ex:p ex:q ) ;
                sh:targetClass ex:C ;
                sh:sparql [
                    sh:select """
                        SELECT $this ?value
                        WHERE {{ $this $PATH ?value .
                                 FILTER (str(?value) = "bad") }}
                    """ ] .
            ex:Good a ex:C ; ex:p [ ex:q "ok" ] .
            ex:Bad  a ex:C ; ex:p [ ex:q "bad" ] .
            "#,
            prefix = prefix()
        );
        let loaded = shapes_data(&ttl);
        let report = validate_report(&loaded, &loaded.graph);
        assert!(!report.conforms);
        let focuses: Vec<_> = report.results.iter().map(|r| r.focus.to_string()).collect();
        assert!(focuses.contains(&"<http://example.org/Bad>".to_string()), "{focuses:?}");
        assert!(!focuses.contains(&"<http://example.org/Good>".to_string()), "{focuses:?}");
    }

    /// `$PATH` is an `sh:zeroOrMorePath` — rewrites to `ex:p*`.
    #[test]
    fn zero_or_more_path() {
        let ttl = format!(
            r#"{prefix}
            ex:S a sh:PropertyShape ;
                sh:path [ sh:zeroOrMorePath ex:next ] ;
                sh:targetClass ex:Node ;
                sh:sparql [
                    sh:select """
                        SELECT $this ?value
                        WHERE {{ $this $PATH ?value .
                                 FILTER (?value = ex:Sink) }}
                    """ ] .
            ex:A a ex:Node ; ex:next ex:B .
            ex:B a ex:Node ; ex:next ex:Sink .
            ex:Sink a ex:Node .
            "#,
            prefix = prefix()
        );
        let loaded = shapes_data(&ttl);
        let report = validate_report(&loaded, &loaded.graph);
        // ex:A and ex:B can reach ex:Sink via ex:next*; ex:Sink reaches itself.
        // All three violate the constraint (they all reach ex:Sink).
        assert!(!report.conforms);
        let focuses: Vec<_> = report.results.iter().map(|r| r.focus.to_string()).collect();
        assert!(focuses.contains(&"<http://example.org/A>".to_string()), "{focuses:?}");
        assert!(focuses.contains(&"<http://example.org/B>".to_string()), "{focuses:?}");
    }
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
