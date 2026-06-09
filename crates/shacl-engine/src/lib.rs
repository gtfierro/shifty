//! Validation + SHACL-AF inference execution (Layers 3, 6, 7).
//!
//! Layer 3 lives here: the naive denotational evaluator that is the conformance
//! oracle — relational path evaluation ([`path`]), value-type checks
//! ([`value`]), and shape/schema satisfaction ([`validate`]). The rule/fixpoint
//! inference engine (Layer 6) and compiled executors (Layer 7) come later; every
//! execution mode must agree with this oracle.

pub mod path;
pub mod validate;
pub mod value;

pub use validate::{focus_nodes, validate, Reason, ValidationOutcome, Violation};

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::Graph;
    use shacl_parse::parse_turtle;

    fn run(shapes_and_data: &str) -> ValidationOutcome {
        let out = parse_turtle(shapes_and_data.as_bytes(), None).unwrap();
        // data graph = the same graph (shapes + data coexist), as in the suite.
        let loaded = shacl_parse::load_turtle(shapes_and_data.as_bytes(), None).unwrap();
        validate(&loaded.graph, &out.schema)
    }

    const PREFIXES: &str = r#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex:  <http://ex/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    "#;

    #[test]
    fn reports_specific_failing_constraints() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ;
                sh:targetNode ex:x ;
                sh:closed true ;
                sh:ignoredProperties ( rdf:type ) ;
                sh:property [ sh:path ex:age ; sh:datatype xsd:integer ; sh:maxCount 1 ] .
            ex:x ex:age \"foo\" , 5 ; ex:extra 1 .
            "
        );
        let outcome = run(&ttl);
        assert!(!outcome.conforms);
        assert_eq!(outcome.violations.len(), 1);
        let msgs: Vec<&str> = outcome.violations[0]
            .reasons
            .iter()
            .map(|r| r.message.as_str())
            .collect();
        // each distinct constraint is reported, not just "the node failed"
        assert!(
            msgs.iter().any(|m| m.contains("datatype(xsd:integer)")),
            "missing datatype reason: {msgs:?}"
        );
        assert!(
            msgs.iter().any(|m| m.contains("at most 1")),
            "missing maxCount reason: {msgs:?}"
        );
        assert!(
            msgs.iter().any(|m| m.contains("closed") && m.contains("extra")),
            "missing closed reason: {msgs:?}"
        );
    }

    #[test]
    fn cardinality_and_datatype() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ;
                sh:targetNode ex:alice, ex:bob ;
                sh:property [ sh:path ex:age ; sh:maxCount 1 ; sh:datatype xsd:integer ] .
            ex:alice ex:age 30 .
            ex:bob   ex:age 30 ; ex:age 40 .
            "
        );
        let outcome = run(&ttl);
        assert!(!outcome.conforms);
        // only ex:bob violates maxCount 1
        let bad: Vec<_> = outcome.violations.iter().map(|r| r.focus.to_string()).collect();
        assert_eq!(bad, vec!["<http://ex/bob>".to_string()]);
    }

    #[test]
    fn datatype_violation() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ;
                sh:targetNode ex:x ;
                sh:property [ sh:path ex:p ; sh:datatype xsd:integer ] .
            ex:x ex:p \"hello\" .
            "
        );
        assert!(!run(&ttl).conforms);
    }

    #[test]
    fn nodekind_and_class_target() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ;
                sh:targetClass ex:Person ;
                sh:property [ sh:path ex:knows ; sh:nodeKind sh:IRI ] .
            ex:alice a ex:Person ; ex:knows ex:bob .
            ex:carol a ex:Person ; ex:knows \"notaniri\" .
            "
        );
        let outcome = run(&ttl);
        assert!(!outcome.conforms);
        let bad: Vec<_> = outcome.violations.iter().map(|r| r.focus.to_string()).collect();
        assert_eq!(bad, vec!["<http://ex/carol>".to_string()]);
    }

    #[test]
    fn recursion_over_cyclic_data_terminates() {
        // S requires every ex:knows neighbour to also satisfy S; data is a cycle.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ;
                sh:targetNode ex:a ;
                sh:property [ sh:path ex:knows ; sh:node ex:S ; sh:nodeKind sh:IRI ] .
            ex:a ex:knows ex:b .
            ex:b ex:knows ex:a .
            "
        );
        // Must terminate; with all-IRI neighbours it conforms under the
        // provisional cycle-breaking semantics.
        assert!(run(&ttl).conforms);
    }

    #[test]
    fn empty_graph_conforms() {
        let outcome = validate(&Graph::new(), &shacl_algebra::Schema::new());
        assert!(outcome.conforms);
    }
}
