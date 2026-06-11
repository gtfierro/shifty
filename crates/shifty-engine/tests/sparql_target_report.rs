//! The RDF-driven W3C report validator discovers focus nodes from SPARQL-based
//! targets (`sh:target [ sh:select … ]`) and validates them, matching the
//! algebra conformance path.

use oxrdf::{NamedNode, Term};
use shifty_parse::vocab;

const SHAPES_AND_DATA: &str = r#"
@prefix sh:  <http://www.w3.org/ns/shacl#> .
@prefix ex:  <http://example.org/> .

ex:WidgetShape a sh:NodeShape ;
    sh:target [
        a sh:SPARQLTarget ;
        sh:select "SELECT $this WHERE { $this a ex:Widget }" ;
    ] ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .

ex:w1    a ex:Widget .
ex:w2    a ex:Widget ; ex:name "ok" .
ex:other a ex:Thing .
"#;

fn iri(s: &str) -> Term {
    Term::NamedNode(NamedNode::new_unchecked(s.to_string()))
}

#[test]
fn report_validates_sparql_target_focus_nodes() {
    let loaded = shifty_parse::load_turtle(SHAPES_AND_DATA.as_bytes(), None).expect("parse");
    let report = shifty_engine::validate_report(&loaded, &loaded.graph);

    // ex:w1 (a Widget without ex:name) is the only violation; ex:w2 conforms
    // and ex:other is not a Widget so it is never selected as a focus node.
    assert!(!report.conforms, "graph should not conform");
    assert_eq!(report.results.len(), 1, "exactly one result: {:?}", report.results);

    let result = &report.results[0];
    assert_eq!(result.focus, iri("http://example.org/w1"));
    assert_eq!(
        result.component.as_str(),
        vocab::SH_CC_MIN_COUNT.as_str(),
        "MinCount component"
    );

    // Sanity: no result mentions the conforming or untargeted nodes.
    for focus in [iri("http://example.org/w2"), iri("http://example.org/other")] {
        assert!(
            report.results.iter().all(|r| r.focus != focus),
            "{focus:?} should not appear in results"
        );
    }
}
