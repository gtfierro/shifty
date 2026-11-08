//! Report graphs must remain usable without the original shapes graph.

use oxrdf::{Graph, NamedNode, NamedNodeRef, Term};
use shifty_engine::{report_to_graph, validate_report};
use shifty_parse::{graph::Loaded, load_turtle, path::parse_path, vocab};

#[test]
fn complex_result_paths_can_be_read_from_the_report_alone() {
    let shapes = load_turtle(
        br#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://example.org/> .
        ex:S a sh:PropertyShape ; sh:targetNode ex:focus ;
            sh:path [ sh:inversePath ( ex:p ex:q ) ] ; sh:minCount 1 .
        "#,
        None,
    )
    .unwrap();
    let report = validate_report(&shapes, &Graph::new()).unwrap();
    assert!(!report.conforms);
    assert_eq!(report.results.len(), 1);
    let path = report.results[0].path.as_ref().unwrap();
    let expected = parse_path(&shapes, path).unwrap();
    let serialized = Loaded {
        graph: report_to_graph(&report),
        prefixes: Vec::new(),
        base: None,
    };
    assert_eq!(parse_path(&serialized, path).unwrap(), expected);
    assert!(
        !serialized
            .graph
            .iter()
            .any(|triple| triple.predicate == vocab::SH_TARGET_NODE)
    );
}

#[test]
fn sparql_results_identify_the_authored_constraint_in_rdf() {
    let shapes = load_turtle(
        br#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://example.org/> .
        ex:S a sh:NodeShape ; sh:targetNode ex:focus ; sh:sparql ex:C .
        ex:C sh:select "SELECT $this WHERE {}" .
        "#,
        None,
    )
    .unwrap();
    let report = validate_report(&shapes, &Graph::new()).unwrap();
    assert_eq!(report.results.len(), 1);
    let constraint = Term::NamedNode(NamedNode::new("http://example.org/C").unwrap());
    assert_eq!(
        report.results[0].source_constraint.as_ref(),
        Some(&constraint)
    );
    let graph = report_to_graph(&report);
    assert!(graph.iter().any(|triple| {
        triple.predicate
            == NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#sourceConstraint")
            && triple.object == constraint.as_ref()
    }));
}

#[test]
fn unique_lang_uses_the_same_exact_boolean_term_as_the_upstream_suite() {
    let data = load_turtle(
        br#"<http://example.org/focus> <http://example.org/p> "A"@en, "B"@en ."#,
        None,
    )
    .unwrap();
    for (value, conforms) in [("true", false), (r#""1"^^xsd:boolean"#, true)] {
        let turtle = format!(
            r#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            @prefix ex: <http://example.org/> .
            ex:S a sh:PropertyShape ; sh:targetNode ex:focus ;
                sh:path ex:p ; sh:uniqueLang {value} .
            "#
        );
        let shapes = load_turtle(turtle.as_bytes(), None).unwrap();
        assert_eq!(
            validate_report(&shapes, &data.graph).unwrap().conforms,
            conforms
        );
    }
}
