use oxrdf::{Graph, NamedNode, Term};
use shifty_engine::{
    CompiledShapes, ConformanceOptions, EngineOptions, EvaluationError, EvidenceOptions,
    FindingOptions, SessionData, SessionOptions, SparqlOperation, UnsupportedPolicy,
    ValidationReport, ValidationResult, report_to_graph, validate_report,
};

const PREFIXES: &str = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .";
const FUNCTION: &str = r#"ex:exists a sh:SPARQLFunction ; sh:ask "ASK { ?s <http://ex/p> ?o }" ."#;

fn session(shape: &str, policy: UnsupportedPolicy) -> shifty_engine::EvaluationSession {
    let loaded =
        shifty_parse::load_turtle(format!("{PREFIXES} {FUNCTION} {shape}").as_bytes(), None)
            .unwrap();
    CompiledShapes::compile(loaded)
        .unwrap()
        .session(
            SessionData::Embedded,
            SessionOptions {
                engine: EngineOptions {
                    unsupported: policy,
                },
                ..Default::default()
            },
        )
        .unwrap()
}

#[test]
fn execution_errors_propagate_through_every_session_projection_and_do_not_leak() {
    let session = session(
        r#"
        ex:Bad a sh:NodeShape ; sh:targetNode ex:x ;
            sh:not [ sh:sparql [ sh:select "SELECT $this WHERE { FILTER (<http://ex/exists>()) }" ] ] .
        ex:Good a sh:NodeShape ; sh:targetNode ex:y ; sh:nodeKind sh:IRI .
    "#,
        UnsupportedPolicy::Error,
    );
    for _ in 0..2 {
        assert!(matches!(
            session.validate(&FindingOptions::default()),
            Err(EvaluationError::Sparql { .. })
        ));
        assert!(session.report(&FindingOptions::default()).is_err());
        assert!(session.evidence(&EvidenceOptions::default()).is_err());
        assert!(session.conformance(&ConformanceOptions::default()).is_err());
        assert!(
            session
                .find_failures(&ConformanceOptions::default())
                .is_err()
        );
        assert!(
            session
                .property_witnesses(None, &FindingOptions::default())
                .is_err()
        );
        let good = FindingOptions {
            entry_shape_names: vec!["http://ex/Good".into()],
            ..Default::default()
        };
        assert!(session.validate(&good).unwrap().conforms);
        assert!(session.report(&good).unwrap().conforms);
    }
}

#[test]
fn targets_and_both_custom_validator_kinds_preserve_failure_categories() {
    for (shape, operation) in [
        (
            r#"ex:S a sh:NodeShape ; sh:target [ a sh:SPARQLTarget ; sh:select "SELECT ?this WHERE { BIND(<http://ex/exists>() AS ?this) }" ] ."#,
            SparqlOperation::Target,
        ),
        (
            r#"ex:C a sh:ConstraintComponent ; sh:parameter [ sh:path ex:arg ] ; sh:validator [ sh:ask "ASK { FILTER (<http://ex/exists>()) }" ] . ex:S a sh:NodeShape ; sh:targetNode ex:x ; ex:arg true ."#,
            SparqlOperation::AskValidator,
        ),
        (
            r#"ex:C a sh:ConstraintComponent ; sh:parameter [ sh:path ex:arg ] ; sh:nodeValidator [ sh:select "SELECT $this WHERE { FILTER (<http://ex/exists>()) }" ] . ex:S a sh:NodeShape ; sh:targetNode ex:x ; ex:arg true ."#,
            SparqlOperation::SelectValidator,
        ),
    ] {
        let session = session(shape, UnsupportedPolicy::Error);
        assert!(
            matches!(session.report(&FindingOptions::default()), Err(EvaluationError::Sparql { operation: actual, .. }) if actual == operation)
        );
        assert!(session.validate(&FindingOptions::default()).is_err());
    }
}

#[test]
fn ignore_does_not_suppress_execution_errors_or_wrong_query_forms() {
    let shape = r#"ex:C a sh:ConstraintComponent ; sh:parameter [ sh:path ex:arg ] ; sh:validator [ sh:ask "SELECT $this WHERE {}" ] . ex:S a sh:NodeShape ; sh:targetNode ex:x ; ex:arg true ."#;
    let session = session(shape, UnsupportedPolicy::Ignore);
    assert!(session.report(&FindingOptions::default()).is_err());
    assert!(session.validate(&FindingOptions::default()).is_err());
}

#[test]
fn strict_default_and_report_construction_are_public_api_contracts() {
    assert_eq!(
        EngineOptions::default().unsupported,
        UnsupportedPolicy::Error
    );
    let focus: Term = NamedNode::new("http://ex/x").unwrap().into();
    let result = ValidationResult::new(
        focus.clone(),
        NamedNode::new("http://ex/component").unwrap(),
        focus,
    );
    let report = ValidationReport::new(false, vec![result]).with_path_triples(Graph::new());
    assert!(!report.conforms());
    assert_eq!(report.results().len(), 1);
    assert!(report.results()[0].source_constraint().is_none());
    assert!(!report_to_graph(&report).is_empty());
    let invalid = shifty_parse::load_turtle(format!("{PREFIXES} ex:C a sh:ConstraintComponent ; sh:parameter [ sh:path ex:arg ] ; sh:validator [ sh:ask \"invalid query\" ] .").as_bytes(), None).unwrap();
    assert!(validate_report(&invalid, &Graph::new()).is_err());
}

#[test]
fn standalone_witnesses_and_repair_gate_cannot_turn_query_errors_into_findings() {
    let turtle = format!(
        r#"{PREFIXES}
        ex:C a sh:ConstraintComponent ; sh:parameter [ sh:path ex:arg ] ;
            sh:validator [ sh:ask "SELECT $this WHERE {{}}" ] .
        ex:S a sh:NodeShape ; sh:targetNode ex:x ; ex:arg true ."#
    );
    let parsed = shifty_parse::parse_turtle(turtle.as_bytes(), None).unwrap();
    let loaded = shifty_parse::load_turtle(turtle.as_bytes(), None).unwrap();
    let id = shifty_engine::shape_id_for_iri(&parsed.schema, "http://ex/S").unwrap();
    let node: Term = NamedNode::new("http://ex/x").unwrap().into();
    assert!(shifty_engine::validate(&loaded.graph, &parsed.schema).is_err());
    assert!(
        shifty_engine::witness_violations(&loaded.graph, &loaded.graph, &parsed.schema).is_err()
    );
    assert!(
        shifty_engine::witness_shape(&loaded.graph, &loaded.graph, &parsed.schema, id).is_err()
    );
    assert!(
        shifty_engine::satisfy_shape(&loaded.graph, &loaded.graph, &parsed.schema, id).is_err()
    );
    assert!(shifty_engine::witness_node(&loaded.graph, &parsed.schema, &node, id).is_err());
    assert!(
        shifty_engine::gate(
            &loaded.graph,
            &loaded.graph,
            &parsed.schema,
            &shifty_repair::GraphDelta::default()
        )
        .is_err()
    );
}
