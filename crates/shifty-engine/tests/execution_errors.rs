use oxrdf::{Graph, NamedNode, Term};
use shifty_engine::{
    CompiledShapes, ConformanceOptions, EngineOptions, EvaluationError, EvidenceOptions,
    FindingOptions, SessionData, SessionError, SessionOptions, SparqlOperation, UnsupportedPolicy,
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
fn retained_raw_qualifiers_propagate_failures_after_normalization() {
    let shape = r#"
        ex:Bad a sh:NodeShape ; sh:not [ sh:sparql [
            sh:select "SELECT $this WHERE { FILTER (<http://ex/exists>()) }"
        ] ] .
        ex:S a sh:NodeShape ; sh:targetNode ex:x ;
            sh:property [ sh:path ex:p ; sh:qualifiedValueShape ex:Bad ] .
        "#;
    let loaded =
        shifty_parse::load_turtle(format!("{PREFIXES} {FUNCTION} {shape}").as_bytes(), None)
            .unwrap();
    let compiled = CompiledShapes::compile(loaded).unwrap();
    let bad = shifty_engine::shape_id_for_iri(compiled.authored_schema(), "http://ex/Bad").unwrap();
    let session = compiled
        .session(SessionData::Embedded, SessionOptions::default())
        .unwrap();
    assert!(
        session
            .validate(&FindingOptions::default())
            .unwrap()
            .conforms
    );
    let prepared = session.prepared_evidence();
    let focus = NamedNode::new("http://ex/x").unwrap().into();
    for _ in 0..2 {
        assert!(matches!(
            prepared.raw_constraint_holds(&focus, bad),
            Err(EvaluationError::Sparql { .. })
        ));
    }
    assert_eq!(
        prepared
            .raw_constraint_holds(&focus, shifty_algebra::ShapeId(u32::MAX))
            .unwrap(),
        None
    );
}

#[test]
fn inference_rejects_failed_targets_conditions_and_node_filters() {
    for (target, condition, subject, operation) in [
        (
            r#"sh:target [ a sh:SPARQLTarget ; sh:select "SELECT ?this WHERE { BIND(<http://ex/exists>() AS ?this) }" ]"#,
            "",
            "sh:this",
            SparqlOperation::Target,
        ),
        (
            "sh:targetNode ex:x",
            "sh:condition ex:Guard ;",
            "sh:this",
            SparqlOperation::Constraint,
        ),
        (
            "sh:targetNode ex:x",
            "",
            "[ sh:filterShape ex:Guard ; sh:nodes sh:this ]",
            SparqlOperation::Constraint,
        ),
    ] {
        let loaded = shifty_parse::load_turtle(
            format!(
                r#"{PREFIXES} {FUNCTION}
                ex:x ex:p "present" .
                ex:Guard a sh:NodeShape ; sh:not [ sh:sparql [
                    sh:select "SELECT $this WHERE {{ FILTER (<http://ex/exists>()) }}"
                ] ] .
                ex:S a sh:NodeShape ; {target} ; sh:rule [ a sh:TripleRule ;
                    {condition} sh:subject {subject} ; sh:predicate ex:derived ; sh:object true ] ."#
            )
            .as_bytes(),
            None,
        )
        .unwrap();
        let compiled = CompiledShapes::compile(loaded).unwrap();
        let error = compiled
            .session(
                SessionData::Embedded,
                SessionOptions {
                    inference: true,
                    ..Default::default()
                },
            )
            .err()
            .expect("failed rule guards must reject the inferred snapshot");
        assert!(
            matches!(error, SessionError::Evaluation(EvaluationError::Sparql { operation: actual, .. }) if actual == operation)
        );
        let legacy = compiled
            .session(
                SessionData::Embedded,
                SessionOptions {
                    inference: true,
                    engine: EngineOptions {
                        unsupported: UnsupportedPolicy::Ignore,
                    },
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(legacy.inferred().is_empty());
    }
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
