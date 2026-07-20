//! `sh:resultMessage` resolution in the W3C report path (`report.rs`).
//!
//! Mirrors the algebra/explain path's "nearest-enclosing shape" `sh:message`
//! resolution (see `lib.rs::explain`): a violation on a shape without its own
//! `sh:message` should still surface an enclosing shape's message rather than
//! going silent.

use oxrdf::Term;
use shifty_engine::validate_report;

fn report_messages(shapes_ttl: &[u8], data_ttl: &[u8]) -> Vec<Vec<String>> {
    let shapes = shifty_parse::load_turtle(shapes_ttl, None).expect("valid shapes");
    let data = shifty_parse::load_turtle(data_ttl, None)
        .expect("valid data")
        .graph;
    let report = validate_report(&shapes, &data);
    report
        .results
        .iter()
        .map(|r| {
            r.messages
                .iter()
                .map(|m| match m {
                    Term::Literal(lit) => lit.value().to_string(),
                    other => other.to_string(),
                })
                .collect()
        })
        .collect()
}

#[test]
fn nested_property_shape_inherits_enclosing_sh_message() {
    let shapes = br#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:PersonShape a sh:NodeShape ;
            sh:targetNode ex:alice ;
            sh:message "Alice must have a valid quantity kind reference" ;
            sh:property [
                sh:path ex:hasQuantityKind ;
                sh:class ex:QuantityKind ;
            ] .
    "#;
    let data = br#"
        @prefix ex: <http://ex/> .
        ex:alice ex:hasQuantityKind ex:Temperature .
        ex:Temperature a ex:NotAQuantityKind .
    "#;
    let messages = report_messages(shapes, data);
    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0],
        vec!["Alice must have a valid quantity kind reference"]
    );
}

#[test]
fn nested_property_shape_own_sh_message_takes_precedence() {
    let shapes = br#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:PersonShape a sh:NodeShape ;
            sh:targetNode ex:alice ;
            sh:message "outer message" ;
            sh:property [
                sh:path ex:hasQuantityKind ;
                sh:class ex:QuantityKind ;
                sh:message "must reference a known quantity kind" ;
            ] .
    "#;
    let data = br#"
        @prefix ex: <http://ex/> .
        ex:alice ex:hasQuantityKind ex:Temperature .
        ex:Temperature a ex:NotAQuantityKind .
    "#;
    let messages = report_messages(shapes, data);
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0], vec!["must reference a known quantity kind"]);
}

#[test]
fn without_any_sh_message_a_default_message_is_generated() {
    let shapes = br#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:PersonShape a sh:NodeShape ;
            sh:targetNode ex:alice ;
            sh:property [
                sh:path ex:hasQuantityKind ;
                sh:class ex:QuantityKind ;
            ] .
    "#;
    let data = br#"
        @prefix ex: <http://ex/> .
        ex:alice ex:hasQuantityKind ex:Temperature .
        ex:Temperature a ex:NotAQuantityKind .
    "#;
    let messages = report_messages(shapes, data);
    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0],
        vec!["Value <http://ex/Temperature> is not an instance of class <http://ex/QuantityKind>"]
    );
}

#[test]
fn default_min_count_message_omits_absent_value_and_names_the_path() {
    let shapes = br#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:PersonShape a sh:NodeShape ;
            sh:targetNode ex:alice ;
            sh:property [
                sh:path ex:name ;
                sh:minCount 1 ;
            ] .
    "#;
    let data = br#"
        @prefix ex: <http://ex/> .
        ex:alice a ex:Person .
    "#;
    let messages = report_messages(shapes, data);
    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0],
        vec!["Fewer than 1 values on path <http://ex/name>"]
    );
}

#[test]
fn default_datatype_message_names_value_and_datatype() {
    let shapes = br#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix ex: <http://ex/> .
        ex:PersonShape a sh:NodeShape ;
            sh:targetNode ex:alice ;
            sh:property [
                sh:path ex:age ;
                sh:datatype xsd:integer ;
            ] .
    "#;
    let data = br#"
        @prefix ex: <http://ex/> .
        ex:alice ex:age "old" .
    "#;
    let messages = report_messages(shapes, data);
    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0],
        vec!["Value old does not have datatype <http://www.w3.org/2001/XMLSchema#integer>"]
    );
}

#[test]
fn default_pattern_message_includes_the_regex() {
    let shapes = br#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:PersonShape a sh:NodeShape ;
            sh:targetNode ex:alice ;
            sh:property [
                sh:path ex:code ;
                sh:pattern "^[A-Z]+$" ;
            ] .
    "#;
    let data = br#"
        @prefix ex: <http://ex/> .
        ex:alice ex:code "abc" .
    "#;
    let messages = report_messages(shapes, data);
    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0],
        vec!["Value abc does not match pattern \"^[A-Z]+$\""]
    );
}
