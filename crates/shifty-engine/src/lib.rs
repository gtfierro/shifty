//! Validation + SHACL-AF inference execution (Layers 3, 6, 7).
//!
//! Layer 3 lives here: the naive denotational evaluator that is the conformance
//! oracle — relational path evaluation ([`path`]), value-type checks
//! ([`value`]), and shape/schema satisfaction ([`validate`]). The rule/fixpoint
//! inference engine (Layer 6) and compiled executors (Layer 7) come later; every
//! execution mode must agree with this oracle.

pub mod frozen;
pub mod infer;
mod native_exec;
pub mod path;
mod path_plan;
pub mod profile;
pub mod report;
mod sparql;
pub mod synthesize;
pub mod validate;
pub mod value;
pub mod witness;

pub use infer::{InferenceOutcome, infer, infer_graphs, infer_with_context};
pub use report::{
    ValidationReport, ValidationResult, report_to_graph, validate_report, validate_report_graphs,
    validate_report_graphs_with_mode,
};
pub use validate::{
    NonStratifiable, Reason, ValidationGraphMode, ValidationOutcome, Violation, focus_nodes,
    validate, validate_graphs, validate_graphs_with_mode, validate_plan, validate_plan_graphs,
    validate_plan_graphs_with_mode, validate_plan_with_context, validate_with_context,
};
pub use synthesize::{synthesize, synthesize_focus};
pub use witness::{
    BlockReason, FocusWitness, PathSupport, RelKind, SatTrace, Witness, witness_violations,
};

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::Graph;
    use shifty_parse::parse_turtle;

    fn run(shapes_and_data: &str) -> ValidationOutcome {
        let out = parse_turtle(shapes_and_data.as_bytes(), None).unwrap();
        // data graph = the same graph (shapes + data coexist), as in the suite.
        let loaded = shifty_parse::load_turtle(shapes_and_data.as_bytes(), None).unwrap();
        validate(&loaded.graph, &out.schema).expect("stratifiable schema")
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
            msgs.iter()
                .any(|m| m.contains("closed") && m.contains("extra")),
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
        let bad: Vec<_> = outcome
            .violations
            .iter()
            .map(|r| r.focus.to_string())
            .collect();
        assert_eq!(bad, vec!["<http://ex/bob>".to_string()]);
    }

    #[test]
    fn qualified_value_shape_disjoint_uses_all_sibling_property_shapes() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ;
                sh:targetNode ex:x ;
                sh:property ex:A, ex:B .
            ex:A a sh:PropertyShape ;
                sh:path ex:p ;
                sh:qualifiedValueShape [ sh:class ex:TypeA ] ;
                sh:qualifiedValueShapesDisjoint true ;
                sh:qualifiedMinCount 1 .
            ex:B a sh:PropertyShape ;
                sh:path ex:q ;
                sh:qualifiedValueShape [ sh:class ex:TypeB ] ;
                sh:qualifiedValueShapesDisjoint true ;
                sh:qualifiedMaxCount 10 .
            ex:x ex:p ex:value .
            ex:value a ex:TypeA, ex:TypeB .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        assert!(
            parsed.diagnostics.is_empty(),
            "diags: {:?}",
            parsed.diagnostics
        );
        let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap();

        let algebra = validate(&loaded.graph, &parsed.schema).unwrap();
        assert!(!algebra.conforms);

        let report = validate_report(&loaded, &loaded.graph);
        assert!(!report.conforms);
        assert_eq!(report.results.len(), 1);
        assert_eq!(
            report.results[0].component.as_str(),
            "http://www.w3.org/ns/shacl#QualifiedMinCountConstraintComponent"
        );
    }

    #[test]
    fn disjoint_on_node_shape_uses_the_focus_node_as_the_value() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ;
                sh:targetNode ex:valid, ex:invalid ;
                sh:disjoint ex:p .
            ex:valid ex:p ex:other .
            ex:invalid ex:p ex:invalid .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        assert!(
            parsed.diagnostics.is_empty(),
            "diags: {:?}",
            parsed.diagnostics
        );
        let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap();

        let algebra = validate(&loaded.graph, &parsed.schema).unwrap();
        assert!(!algebra.conforms);
        assert_eq!(algebra.violations.len(), 1);
        assert_eq!(
            algebra.violations[0].focus.to_string(),
            "<http://ex/invalid>"
        );

        let normalized = shifty_opt::normalize(&parsed.schema);
        let plan = shifty_opt::plan(&normalized);
        let planned = validate_plan(&loaded.graph, &plan).unwrap();
        assert_eq!(planned.conforms, algebra.conforms);
        assert_eq!(planned.violations.len(), algebra.violations.len());

        let report = validate_report(&loaded, &loaded.graph);
        assert!(!report.conforms);
        assert_eq!(report.results.len(), 1);
        assert_eq!(
            report.results[0].component.as_str(),
            "http://www.w3.org/ns/shacl#DisjointConstraintComponent"
        );
        assert_eq!(
            report.results[0].value.as_ref().map(ToString::to_string),
            Some("<http://ex/invalid>".to_string())
        );
    }

    #[test]
    fn equals_on_node_shape_uses_the_focus_node_as_the_value() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ;
                sh:targetNode ex:valid, ex:extra, ex:missing ;
                sh:equals ex:p .
            ex:valid ex:p ex:valid .
            ex:extra ex:p ex:extra, ex:other .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        assert!(
            parsed.diagnostics.is_empty(),
            "diags: {:?}",
            parsed.diagnostics
        );
        let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap();

        let algebra = validate(&loaded.graph, &parsed.schema).unwrap();
        assert!(!algebra.conforms);
        let mut foci: Vec<_> = algebra
            .violations
            .iter()
            .map(|violation| violation.focus.to_string())
            .collect();
        foci.sort();
        assert_eq!(
            foci,
            [
                "<http://ex/extra>".to_string(),
                "<http://ex/missing>".to_string()
            ]
        );

        let normalized = shifty_opt::normalize(&parsed.schema);
        let plan = shifty_opt::plan(&normalized);
        let planned = validate_plan(&loaded.graph, &plan).unwrap();
        assert_eq!(planned.conforms, algebra.conforms);
        assert_eq!(planned.violations.len(), algebra.violations.len());

        let report = validate_report(&loaded, &loaded.graph);
        assert!(!report.conforms);
        assert_eq!(report.results.len(), 2);
        assert!(report.results.iter().all(|result| result.component.as_str()
            == "http://www.w3.org/ns/shacl#EqualsConstraintComponent"));
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
        let bad: Vec<_> = outcome
            .violations
            .iter()
            .map(|r| r.focus.to_string())
            .collect();
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
        let outcome = validate(&Graph::new(), &shifty_algebra::Schema::new()).unwrap();
        assert!(outcome.conforms);
    }

    #[test]
    fn non_stratifiable_schema_is_diagnosed() {
        // S := ¬∃p.S — recursion through negation; no defined 2-valued semantics.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ;
                sh:targetNode ex:x ;
                sh:not [ sh:path ex:p ; sh:qualifiedValueShape ex:S ; sh:qualifiedMinCount 1 ] .
            ex:x ex:p ex:y .
            "
        );
        let out = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap();
        assert!(validate(&loaded.graph, &out.schema).is_err());
    }

    fn triple(s: &str, p: &str, o: &str) -> oxrdf::Triple {
        use oxrdf::NamedNode;
        oxrdf::Triple::new(
            NamedNode::new(s).unwrap(),
            NamedNode::new(p).unwrap(),
            NamedNode::new(o).unwrap(),
        )
    }

    #[test]
    fn triple_rule_infers_from_path() {
        // copy each ex:knows value to ex:knows2
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetClass ex:Person ;
                sh:rule [ a sh:TripleRule ;
                    sh:subject sh:this ; sh:predicate ex:knows2 ;
                    sh:object [ sh:path ex:knows ] ] .
            ex:a a ex:Person ; ex:knows ex:b .
            "
        );
        let out = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap();
        let outcome = infer(&loaded.graph, &out.schema).unwrap();
        assert_eq!(outcome.inferred.len(), 1);
        assert!(
            outcome
                .graph
                .contains(&triple("http://ex/a", "http://ex/knows2", "http://ex/b"))
        );
    }

    #[test]
    fn inference_reaches_a_fixpoint() {
        // ex:reaches := ex:knows ∪ (ex:knows / ex:reaches) — transitive closure
        // a→b→c, so a reaches c is derivable only after b reaches c.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetClass ex:Person ;
                sh:rule [ a sh:TripleRule ;
                    sh:subject sh:this ; sh:predicate ex:reaches ;
                    sh:object [ sh:path [ sh:alternativePath ( ex:knows ( ex:knows ex:reaches ) ) ] ] ] .
            ex:a a ex:Person ; ex:knows ex:b .
            ex:b a ex:Person ; ex:knows ex:c .
            ex:c a ex:Person .
            "
        );
        let out = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap();
        let outcome = infer(&loaded.graph, &out.schema).unwrap();
        assert!(
            outcome
                .graph
                .contains(&triple("http://ex/a", "http://ex/reaches", "http://ex/b"))
        );
        assert!(
            outcome
                .graph
                .contains(&triple("http://ex/b", "http://ex/reaches", "http://ex/c"))
        );
        // the fixpoint result: a reaches c (only via b reaches c)
        assert!(
            outcome
                .graph
                .contains(&triple("http://ex/a", "http://ex/reaches", "http://ex/c"))
        );
    }

    #[test]
    fn later_order_output_reactivates_an_earlier_rule() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ; sh:targetNode ex:x ;
                sh:rule [
                    a sh:TripleRule ; sh:order 0 ;
                    sh:subject sh:this ; sh:predicate ex:done ;
                    sh:object [ sh:path ex:ready ]
                ] ;
                sh:rule [
                    a sh:TripleRule ; sh:order 1 ;
                    sh:subject sh:this ; sh:predicate ex:ready ;
                    sh:object ex:y
                ] .
            "
        );
        let out = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap();
        let outcome = infer(&loaded.graph, &out.schema).unwrap();

        assert!(
            outcome
                .graph
                .contains(&triple("http://ex/x", "http://ex/ready", "http://ex/y"))
        );
        assert!(
            outcome
                .graph
                .contains(&triple("http://ex/x", "http://ex/done", "http://ex/y"))
        );
    }

    #[test]
    fn inferred_triples_can_create_new_rule_targets() {
        let ttl = format!(
            "{PREFIXES}
            ex:Seed a sh:NodeShape ; sh:targetNode ex:x ;
                sh:rule [
                    a sh:TripleRule ;
                    sh:subject sh:this ; sh:predicate ex:eligible ;
                    sh:object ex:y
                ] .
            ex:Eligible a sh:NodeShape ; sh:targetSubjectsOf ex:eligible ;
                sh:rule [
                    a sh:TripleRule ;
                    sh:subject sh:this ; sh:predicate ex:classified ;
                    sh:object ex:yes
                ] .
            "
        );
        let out = parse_turtle(ttl.as_bytes(), None).unwrap();
        let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap();
        let outcome = infer(&loaded.graph, &out.schema).unwrap();

        assert!(outcome.graph.contains(&triple(
            "http://ex/x",
            "http://ex/classified",
            "http://ex/yes",
        )));
    }

    #[test]
    fn split_inference_uses_shapes_graph_as_rule_context() {
        let shapes_ttl = format!(
            "{PREFIXES}
            ex:InverseShape a sh:NodeShape ;
                sh:targetClass ex:Thing ;
                sh:rule [
                    a sh:SPARQLRule ;
                    sh:construct \"\"\"
                        CONSTRUCT {{ ?o ?inverse $this }}
                        WHERE {{
                            $this ?predicate ?o .
                            ?predicate ex:inverseOf ?inverse .
                        }}
                    \"\"\"
                ] .
            ex:p ex:inverseOf ex:q .
            "
        );
        let data_ttl = format!(
            "{PREFIXES}
            ex:a a ex:Thing ; ex:p ex:b .
            "
        );
        let shapes = shifty_parse::load_turtle(shapes_ttl.as_bytes(), None).unwrap();
        let parsed = shifty_parse::parse_loaded(&shapes);
        let data = shifty_parse::load_turtle(data_ttl.as_bytes(), None).unwrap();

        let outcome = infer_graphs(&data.graph, &shapes.graph, &parsed.schema).unwrap();

        assert!(
            outcome
                .graph
                .contains(&triple("http://ex/b", "http://ex/q", "http://ex/a"))
        );
        assert_eq!(outcome.inferred.len(), 1);
    }
}
