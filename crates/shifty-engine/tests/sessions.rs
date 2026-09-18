use oxrdf::{NamedNode, Triple};
use shifty_engine::{
    CompiledShapes, ConformanceOptions, EngineOptions, EvaluationError, FindingOptions,
    SessionData, SessionError, SessionOptions, UnsupportedPolicy, ValidationGraphMode,
};
use shifty_repair::GraphDelta;

fn loaded(ttl: &str) -> shifty_parse::Loaded {
    shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap()
}

const SHAPES: &str = r#"
    @prefix sh: <http://www.w3.org/ns/shacl#> .
    @prefix ex: <http://ex/> .
    ex:S a sh:NodeShape ; sh:targetClass ex:T ;
        sh:property [ sh:path ex:p ; sh:minCount 1 ] .
"#;

#[test]
fn one_compilation_serves_multiple_snapshots_and_rejects_foreign_pairs() {
    let compiled = CompiledShapes::compile(loaded(SHAPES)).unwrap();
    let data = loaded("@prefix ex: <http://ex/> . ex:a a ex:T .").graph;
    let first = compiled
        .session(
            SessionData::Separate(data.clone()),
            SessionOptions::default(),
        )
        .unwrap();
    let second = compiled
        .session(SessionData::Separate(data), SessionOptions::default())
        .unwrap();
    let (run, pairs) = first.find_failures(&ConformanceOptions::default()).unwrap();
    let (_, other_pairs) = second
        .find_failures(&ConformanceOptions::default())
        .unwrap();
    assert_eq!(run.failed, 1);
    assert_eq!(pairs.len(), 1);
    assert_ne!(pairs[0], other_pairs[0]);
    assert!(!first.explain(&pairs[0]).unwrap().is_empty());
    assert!(matches!(
        second.explain(&pairs[0]),
        Err(EvaluationError::ForeignPair)
    ));
    assert!(
        !second
            .validate(&FindingOptions::default())
            .unwrap()
            .conforms
    );
}

#[test]
fn split_graph_modes_select_the_expected_focus_nodes() {
    let shapes = loaded(&format!(
        "{SHAPES}\n@prefix ex: <http://ex/> . ex:shapeItem a ex:T ."
    ));
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let data = loaded("@prefix ex: <http://ex/> . ex:dataItem a ex:T .").graph;
    let union = compiled
        .session(
            SessionData::Separate(data.clone()),
            SessionOptions::default(),
        )
        .unwrap();
    let union_all = compiled
        .session(
            SessionData::Separate(data),
            SessionOptions {
                graph_mode: ValidationGraphMode::UnionAll,
                ..SessionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(
        union
            .conformance(&ConformanceOptions::default())
            .unwrap()
            .failed,
        1
    );
    assert_eq!(
        union_all
            .conformance(&ConformanceOptions::default())
            .unwrap()
            .failed,
        2
    );
}

#[test]
fn deleting_last_inference_support_recomputes_the_snapshot() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetClass ex:T ;
            sh:rule [ a sh:TripleRule ; sh:subject sh:this ;
                sh:predicate ex:p ; sh:object ex:value ] .
        "#,
    );
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let asserted = Triple::new(
        NamedNode::new_unchecked("http://ex/a"),
        NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
        NamedNode::new_unchecked("http://ex/T"),
    );
    let mut data = oxrdf::Graph::new();
    data.insert(&asserted);
    let session = compiled
        .session(
            SessionData::Separate(data),
            SessionOptions {
                inference: true,
                ..SessionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(session.inferred().len(), 1);
    let next = session
        .with_delta(&GraphDelta {
            delete: vec![asserted],
            ..GraphDelta::default()
        })
        .unwrap();
    assert!(next.inferred().is_empty());
    assert_eq!(session.inferred().len(), 1);
}

#[test]
fn authored_negative_recursion_fails_at_compilation() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetNode ex:a ; sh:not ex:S .
        "#,
    );
    assert!(CompiledShapes::compile(shapes).is_err());
}

#[test]
fn compiled_functions_are_available_to_validation_queries() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:isBad a sh:SPARQLFunction ;
            sh:parameter [ sh:path ex:arg ] ;
            sh:ask "ASK { FILTER (STR($arg) = \"bad\") }" .
        ex:S a sh:NodeShape ; sh:targetNode ex:x ;
            sh:sparql [ sh:select """SELECT $this WHERE {
                $this <http://ex/value> ?value .
                FILTER (<http://ex/isBad>(?value))
            }""" ] .
        "#,
    );
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let data = loaded("@prefix ex: <http://ex/> . ex:x ex:value \"bad\" .").graph;
    let session = compiled
        .session(SessionData::Separate(data), SessionOptions::default())
        .unwrap();
    assert!(
        !session
            .validate(&FindingOptions::default())
            .unwrap()
            .conforms
    );
    assert!(!session.report(&FindingOptions::default()).unwrap().conforms);
}

#[test]
fn union_all_keeps_data_out_of_the_named_shapes_graph() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetNode ex:a ;
            sh:sparql [ sh:select """SELECT $this WHERE {
                GRAPH $shapesGraph { ex:marker ex:p ex:value }
            }""" ] .
        "#,
    );
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let data = loaded("@prefix ex: <http://ex/> . ex:marker ex:p ex:value .").graph;
    let session = compiled
        .session(
            SessionData::Separate(data),
            SessionOptions {
                graph_mode: ValidationGraphMode::UnionAll,
                ..SessionOptions::default()
            },
        )
        .unwrap();
    assert!(session.report(&FindingOptions::default()).unwrap().conforms);
}

#[test]
fn strict_rule_diagnostics_fail_session_construction() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetNode ex:x ;
            sh:rule [ a sh:SPARQLRule ;
                sh:construct "CONSTRUCT { $this ex:p [] } WHERE {}" ] .
        "#,
    );
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let result = compiled.session(
        SessionData::Embedded,
        SessionOptions {
            inference: true,
            engine: EngineOptions {
                unsupported: UnsupportedPolicy::Error,
            },
            ..SessionOptions::default()
        },
    );
    assert!(matches!(result, Err(SessionError::StrictInference(_))));
}

#[test]
fn embedded_inference_does_not_change_the_named_shapes_graph() {
    let compiled = CompiledShapes::compile(loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetNode ex:a ;
            sh:rule [ a sh:TripleRule ; sh:subject ex:marker ;
                sh:predicate ex:p ; sh:object ex:value ] ;
            sh:sparql [ sh:select """SELECT $this WHERE {
                GRAPH $shapesGraph { ex:marker ex:p ex:value }
            }""" ] .
        "#,
    ))
    .unwrap();
    let session = compiled
        .session(
            SessionData::Embedded,
            SessionOptions {
                inference: true,
                ..SessionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(session.inferred().len(), 1);
    assert!(session.report(&FindingOptions::default()).unwrap().conforms);
}

#[test]
fn data_edits_preserve_graph_roles_and_the_original_snapshot() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetNode ex:a ;
            sh:property [ sh:path ex:p ; sh:minCount 1 ] .
        ex:a ex:p ex:value .
        "#,
    );
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let fact = Triple::new(
        NamedNode::new_unchecked("http://ex/a"),
        NamedNode::new_unchecked("http://ex/p"),
        NamedNode::new_unchecked("http://ex/value"),
    );
    let mut data = oxrdf::Graph::new();
    data.insert(&fact);
    let delta = GraphDelta {
        delete: vec![fact],
        ..GraphDelta::default()
    };

    let union = compiled
        .session(
            SessionData::Separate(data.clone()),
            SessionOptions::default(),
        )
        .unwrap();
    let union_after = union.with_delta(&delta).unwrap();
    assert!(
        union_after
            .validate(&FindingOptions::default())
            .unwrap()
            .conforms
    );

    let data_only = compiled
        .session(
            SessionData::Separate(data),
            SessionOptions {
                graph_mode: ValidationGraphMode::Data,
                ..SessionOptions::default()
            },
        )
        .unwrap();
    let data_after = data_only.with_delta(&delta).unwrap();
    assert!(
        !data_after
            .validate(&FindingOptions::default())
            .unwrap()
            .conforms
    );
    assert!(
        data_only
            .validate(&FindingOptions::default())
            .unwrap()
            .conforms
    );

    let embedded = compiled
        .session(SessionData::Embedded, SessionOptions::default())
        .unwrap();
    assert!(
        !embedded
            .with_delta(&delta)
            .unwrap()
            .validate(&FindingOptions::default())
            .unwrap()
            .conforms
    );
}

#[test]
fn repair_gate_checks_unfiltered_whole_session() {
    let compiled = CompiledShapes::compile(loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetClass ex:T ;
            sh:property [ sh:path ex:p ; sh:minCount 1 ] .
        "#,
    ))
    .unwrap();
    let data = loaded("@prefix ex: <http://ex/> . ex:a a ex:T .").graph;
    let session = compiled
        .session(SessionData::Separate(data), SessionOptions::default())
        .unwrap();
    let delta = GraphDelta {
        add: vec![
            Triple::new(
                NamedNode::new_unchecked("http://ex/a"),
                NamedNode::new_unchecked("http://ex/p"),
                NamedNode::new_unchecked("http://ex/value"),
            ),
            Triple::new(
                NamedNode::new_unchecked("http://ex/b"),
                NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                NamedNode::new_unchecked("http://ex/T"),
            ),
        ],
        ..GraphDelta::default()
    };
    let verdict = session.gate(&delta).unwrap();
    assert_eq!(verdict.fixed.len(), 1);
    assert_eq!(verdict.introduced.len(), 1);
    assert!(!verdict.is_sound());
}

#[test]
fn rule_queries_bind_the_authored_shapes_graph() {
    let source = r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetNode ex:a ;
            sh:rule [ a sh:SPARQLRule ;
                sh:construct """CONSTRUCT { $this ex:derived ex:value } WHERE {
                    GRAPH $shapesGraph { ex:marker ex:p ex:value }
                }""" ] .
    "#;
    let data = loaded("@prefix ex: <http://ex/> . ex:a ex:q ex:value .").graph;
    let with_marker = CompiledShapes::compile(loaded(&format!(
        "{source}\n@prefix ex: <http://ex/> . ex:marker ex:p ex:value ."
    )))
    .unwrap();
    let without_marker = CompiledShapes::compile(loaded(source)).unwrap();
    let options = SessionOptions {
        inference: true,
        ..SessionOptions::default()
    };
    assert_eq!(
        with_marker
            .session(SessionData::Separate(data.clone()), options)
            .unwrap()
            .inferred()
            .len(),
        1
    );
    assert!(
        without_marker
            .session(SessionData::Separate(data), options)
            .unwrap()
            .inferred()
            .is_empty()
    );
}
