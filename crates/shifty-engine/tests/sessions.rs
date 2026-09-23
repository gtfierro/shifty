use oxrdf::{NamedNode, Term, Triple};
use shifty_engine::profile;
use shifty_engine::{
    CompiledShapes, ConformanceOptions, EngineOptions, EvaluationError, EvidenceOptions,
    FindingOptions, SessionData, SessionError, SessionOptions, UnsupportedPolicy,
    ValidationGraphMode,
};
use shifty_repair::GraphDelta;
use std::sync::Arc;

fn loaded(ttl: &str) -> shifty_parse::Loaded {
    shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap()
}

#[test]
fn split_blank_node_identity_is_public_across_session_results() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetSubjectsOf ex:p ; sh:property _:same .
        _:same sh:path ex:q ; sh:minCount 1 .
        "#,
    );
    let data = loaded(
        r#"
        @prefix ex: <http://ex/> .
        _:same ex:p ex:o .
        "#,
    );
    let session = CompiledShapes::compile(shapes)
        .unwrap()
        .session(SessionData::Separate(data.graph), SessionOptions::default())
        .unwrap();
    let focus = Term::BlankNode(oxrdf::BlankNode::new("same").unwrap());
    assert_eq!(
        session.validate(&FindingOptions::default()).violations[0].focus,
        focus
    );
    assert_eq!(
        session.report(&FindingOptions::default()).results[0].focus,
        focus
    );
    let evidence = session.evidence(&EvidenceOptions::default());
    assert_eq!(evidence.statements[0].selected_foci[0].focus, focus);
    let (_, pairs) = session.find_failures(&ConformanceOptions::default());
    assert_eq!(pairs[0].focus(), &focus);
    assert!(!session.explain(&pairs[0]).unwrap().is_empty());
}

#[test]
fn sparql_rule_reuses_data_blank_node_after_shapes_label_collision() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetNode ex:a ; sh:rule _:same .
        _:same a sh:SPARQLRule ;
            sh:construct "CONSTRUCT { $this ex:copy ?o } WHERE { $this ex:has ?o }" .
        "#,
    );
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let data = loaded(
        r#"
        @prefix ex: <http://ex/> .
        ex:a ex:has _:same .
        _:same ex:name "existing" .
        "#,
    );
    let session = compiled
        .session(
            SessionData::Separate(data.graph),
            SessionOptions {
                inference: true,
                ..SessionOptions::default()
            },
        )
        .unwrap();

    assert!(session.diagnostics().is_empty());
    assert_eq!(session.inferred().len(), 1);
    let input_node = session
        .data()
        .iter()
        .find(|triple| triple.predicate.as_str() == "http://ex/has")
        .unwrap()
        .object
        .into_owned();
    assert!(matches!(input_node, Term::BlankNode(_)));
    assert_eq!(session.inferred()[0].object, input_node);
    assert_eq!(
        session.inferred_for_write_back()[0].object,
        Term::BlankNode(oxrdf::BlankNode::new("same").unwrap())
    );
}

#[test]
fn sparql_rule_reuses_shapes_blank_node_after_data_label_collision() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetNode ex:a ;
            sh:rule [ a sh:SPARQLRule ;
                sh:construct """
                    CONSTRUCT { $this ex:uses ?option }
                    WHERE { GRAPH $shapesGraph { ex:Config ex:option ?option } }
                """ ] .
        ex:Config ex:option _:same .
        _:same ex:kind ex:K .
        "#,
    );
    let shapes_node = shapes
        .graph
        .iter()
        .find(|triple| triple.predicate.as_str() == "http://ex/option")
        .unwrap()
        .object
        .into_owned();
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let data = loaded(
        r#"
        @prefix ex: <http://ex/> .
        ex:a ex:marker _:same .
        _:same ex:name "data" .
        "#,
    );
    let session = compiled
        .session(
            SessionData::Separate(data.graph),
            SessionOptions {
                inference: true,
                ..SessionOptions::default()
            },
        )
        .unwrap();

    assert!(session.diagnostics().is_empty());
    assert_eq!(session.inferred().len(), 1);
    let data_node = session
        .data()
        .iter()
        .find(|triple| triple.predicate.as_str() == "http://ex/marker")
        .unwrap()
        .object
        .into_owned();
    assert!(matches!(shapes_node, Term::BlankNode(_)));
    assert!(matches!(data_node, Term::BlankNode(_)));
    assert_eq!(
        data_node,
        Term::BlankNode(oxrdf::BlankNode::new("same").unwrap())
    );
    assert_eq!(shapes_node, data_node);
    assert_ne!(session.inferred()[0].object, data_node);
    assert_eq!(
        session.inferred_for_write_back()[0].object,
        session.inferred()[0].object
    );
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
    let (run, pairs) = first.find_failures(&ConformanceOptions::default());
    let (_, other_pairs) = second.find_failures(&ConformanceOptions::default());
    assert_eq!(run.failed, 1);
    assert_eq!(pairs.len(), 1);
    assert_ne!(pairs[0], other_pairs[0]);
    assert!(!first.explain(&pairs[0]).unwrap().is_empty());
    assert!(matches!(
        second.explain(&pairs[0]),
        Err(EvaluationError::ForeignPair)
    ));
    assert!(!second.validate(&FindingOptions::default()).conforms);
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
    assert_eq!(union.conformance(&ConformanceOptions::default()).failed, 1);
    assert_eq!(
        union_all.conformance(&ConformanceOptions::default()).failed,
        2
    );
}

#[test]
fn union_all_focus_uses_a_view_until_graph_compatibility_is_requested() {
    let shapes = loaded(&format!(
        "{SHAPES}\n@prefix ex: <http://ex/> . ex:shapeItem a ex:T ."
    ));
    let expected_union_rows = shapes.graph.len() + 1;
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let data = loaded("@prefix ex: <http://ex/> . ex:dataItem a ex:T .").graph;
    profile::enable();
    let session = compiled
        .session(
            SessionData::Separate(data),
            SessionOptions {
                graph_mode: ValidationGraphMode::UnionAll,
                ..SessionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(
        session
            .validate(&FindingOptions::default())
            .violations
            .len(),
        2
    );
    assert_eq!(session.report(&FindingOptions::default()).results.len(), 2);
    let storage = profile::take().unwrap().storage().clone();
    assert_eq!(storage.graph_union_builds, 0);
    assert_eq!(storage.graph_projection_builds, 0);

    profile::enable();
    let prepared = session.prepared_evidence();
    assert_eq!(prepared.data().len(), expected_union_rows);
    assert_eq!(prepared.data().len(), expected_union_rows);
    let storage = profile::take().unwrap().storage().clone();
    assert_eq!(storage.graph_projection_builds, 1);
    assert_eq!(storage.graph_projection_rows, expected_union_rows as u64);
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
fn validation_reuses_inference_dataset_when_the_default_view_matches() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetNode ex:a ;
            sh:rule [ a sh:TripleRule ; sh:subject sh:this ;
                sh:predicate ex:p ; sh:object ex:b ] ;
            sh:property [ sh:path ex:p ; sh:minCount 1 ] .
        "#,
    );
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let data = loaded("@prefix ex: <http://ex/> . ex:a ex:seed ex:b .").graph;
    for (mode, expected_builds) in [
        (ValidationGraphMode::Union, 1),
        (ValidationGraphMode::UnionAll, 1),
        (ValidationGraphMode::Data, 1),
    ] {
        profile::enable();
        let session = compiled
            .session(
                SessionData::Separate(data.clone()),
                SessionOptions {
                    graph_mode: mode,
                    inference: true,
                    ..SessionOptions::default()
                },
            )
            .unwrap();
        assert!(session.validate(&FindingOptions::default()).conforms);
        let storage = profile::take().unwrap().storage().clone();
        assert_eq!(storage.store_builds, 0);
        assert_eq!(storage.dataset_builds, expected_builds, "mode {mode:?}");
    }
}

#[test]
fn data_mode_after_inference_excludes_source_only_triples() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetNode ex:a ;
            sh:rule [ a sh:TripleRule ; sh:subject sh:this ;
                sh:predicate ex:inferred ; sh:object ex:value ] ;
            sh:sparql [ sh:select """SELECT $this WHERE {
                $this ex:inferred ex:value .
                ex:sourceOnly ex:p ex:value .
            }""" ] .
        ex:sourceOnly ex:p ex:value .
        "#,
    );
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let data = loaded("@prefix ex: <http://ex/> . ex:a ex:seed ex:value .").graph;
    for (mode, conforms) in [
        (ValidationGraphMode::Data, true),
        (ValidationGraphMode::Union, false),
    ] {
        let session = compiled
            .session(
                SessionData::Separate(data.clone()),
                SessionOptions {
                    graph_mode: mode,
                    inference: true,
                    ..SessionOptions::default()
                },
            )
            .unwrap();
        assert_eq!(session.inferred().len(), 1);
        assert_eq!(
            session.validate(&FindingOptions::default()).conforms,
            conforms
        );
    }
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
    assert!(!session.validate(&FindingOptions::default()).conforms);
    assert!(!session.report(&FindingOptions::default()).conforms);
}

#[test]
fn compiled_inference_uses_only_source_functions() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetNode ex:x ;
            sh:rule [ a sh:SPARQLRule ;
                sh:construct """CONSTRUCT { $this <http://ex/p> <http://ex/y> }
                    WHERE { FILTER (<http://ex/allowed>()) }""" ] .
        "#,
    );
    let data = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:x ex:q ex:y .
        ex:allowed a sh:SPARQLFunction ;
            sh:ask "ASK { FILTER (true) }" .
        "#,
    )
    .graph;
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let session = compiled
        .session(
            SessionData::Separate(data),
            SessionOptions {
                inference: true,
                ..SessionOptions::default()
            },
        )
        .unwrap();
    assert!(session.inferred().is_empty());
}

#[test]
fn compiled_node_expression_uses_canonical_function_query() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:lookup a sh:SPARQLFunction ;
            sh:parameter [ sh:path ex:arg ] ;
            sh:select "SELECT ?result WHERE { ?arg ex:p ?result }" .
        ex:S a sh:NodeShape ; sh:targetNode ex:a ;
            sh:rule [ a sh:TripleRule ; sh:subject sh:this ;
                sh:predicate ex:out ; sh:object [ ex:lookup ( sh:this ) ] ] .
        "#,
    );
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let data = loaded("@prefix ex: <http://ex/> . ex:a ex:p ex:b .").graph;
    let session = compiled
        .session(
            SessionData::Separate(data),
            SessionOptions {
                inference: true,
                ..SessionOptions::default()
            },
        )
        .unwrap();
    assert_eq!(
        session.inferred(),
        &[Triple::new(
            NamedNode::new_unchecked("http://ex/a"),
            NamedNode::new_unchecked("http://ex/out"),
            NamedNode::new_unchecked("http://ex/b")
        )]
    );
}

#[test]
fn compiled_inference_reads_source_without_materializing_union() {
    let shapes = loaded(
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetNode ex:a ;
            sh:rule [ a sh:SPARQLRule ; sh:construct """
                CONSTRUCT { $this ex:out ex:value }
                WHERE { ex:sourceOnly ex:p ex:value }
            """ ] .
        ex:sourceOnly ex:p ex:value .
        "#,
    );
    let compiled = CompiledShapes::compile(shapes).unwrap();
    let data = loaded("@prefix ex: <http://ex/> . ex:a ex:seed ex:value .").graph;
    profile::enable();
    let session = compiled
        .session(
            SessionData::Separate(data),
            SessionOptions {
                inference: true,
                ..SessionOptions::default()
            },
        )
        .unwrap();
    assert!(session.validate(&FindingOptions::default()).conforms);
    let storage = profile::take().unwrap().storage().clone();
    assert_eq!(session.inferred().len(), 1);
    assert_eq!(storage.store_builds, 0);
    assert_eq!(storage.graph_union_builds, 0);
    assert_eq!(storage.graph_projection_builds, 0);
    assert_eq!(storage.dataset_builds, 1);

    profile::enable();
    let evaluated = session.data_shared();
    assert_eq!(evaluated.len(), 2);
    assert!(evaluated.contains(&Triple::new(
        NamedNode::new_unchecked("http://ex/a"),
        NamedNode::new_unchecked("http://ex/out"),
        NamedNode::new_unchecked("http://ex/value"),
    )));
    assert!(!evaluated.contains(&Triple::new(
        NamedNode::new_unchecked("http://ex/sourceOnly"),
        NamedNode::new_unchecked("http://ex/p"),
        NamedNode::new_unchecked("http://ex/value"),
    )));
    assert!(Arc::ptr_eq(&evaluated, &session.data_shared()));
    assert_eq!(
        profile::take().unwrap().storage().graph_projection_builds,
        1
    );
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
    assert!(session.report(&FindingOptions::default()).conforms);
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
    assert!(session.report(&FindingOptions::default()).conforms);
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
    assert!(union_after.validate(&FindingOptions::default()).conforms);

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
    assert!(!data_after.validate(&FindingOptions::default()).conforms);
    assert!(data_only.validate(&FindingOptions::default()).conforms);

    let embedded = compiled
        .session(SessionData::Embedded, SessionOptions::default())
        .unwrap();
    assert!(
        !embedded
            .with_delta(&delta)
            .unwrap()
            .validate(&FindingOptions::default())
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
