//! Validation + SHACL-AF inference execution (Layers 3, 6, 7).
//!
//! Layer 3 lives here: the naive denotational evaluator that is the conformance
//! oracle — relational path evaluation ([`path`]), value-type checks
//! ([`value`]), and shape/schema satisfaction ([`validate`]). The rule/fixpoint
//! inference engine (Layer 6) and compiled executors (Layer 7) come later; every
//! execution mode must agree with this oracle.

pub mod enumerate;
pub mod frozen;
pub mod gate;
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

pub use enumerate::{
    EnumOptions, FixpointResult, RepairSolution, candidates, enumerate_repair, repair_to_fixpoint,
};
pub use gate::{RepairOutcome, apply, gate};
pub use infer::{InferenceOutcome, infer, infer_graphs, infer_with_context};
pub use report::{
    ValidationReport, ValidationResult, evaluate_function_expression, report_to_graph,
    validate_report, validate_report_graphs, validate_report_graphs_with_mode,
    validate_report_graphs_with_mode_and_options, validate_report_with_options,
};
pub use synthesize::{synthesize, synthesize_focus};
pub use validate::{
    NonStratifiable, Reason, ValidationGraphMode, ValidationOptions, ValidationOutcome, Violation,
    focus_nodes, graph_union, validate, validate_graphs, validate_graphs_with_mode,
    validate_graphs_with_mode_and_options, validate_plan, validate_plan_graphs,
    validate_plan_graphs_with_mode, validate_plan_graphs_with_mode_and_options,
    validate_plan_with_context, validate_plan_with_context_and_options, validate_plan_with_options,
    validate_with_context, validate_with_context_and_options, validate_with_options,
};
pub use witness::{
    BlockReason, FocusSat, FocusWitness, PathSupport, RelKind, SatTrace, Witness, satisfy_shape,
    shape_id_for_iri, witness_node, witness_shape, witness_violations,
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
    fn inference_rules_fire_for_implicit_class_targets() {
        let ttl = br#"
            @prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix owl:   <http://www.w3.org/2002/07/owl#> .
            @prefix sh:    <http://www.w3.org/ns/shacl#> .
            @prefix ex:    <http://ex/> .

            ex:Parent a owl:Class, sh:NodeShape ;
                sh:rule [
                    a sh:TripleRule ;
                    sh:subject sh:this ;
                    sh:predicate ex:hasTag ;
                    sh:object ex:Tag
                ] .

            ex:Child rdfs:subClassOf ex:Parent .
            ex:item a ex:Child .
        "#;
        let loaded = shifty_parse::load_turtle(ttl, None).unwrap();
        let parsed = shifty_parse::parse_loaded(&loaded);
        let normalized = shifty_opt::normalize(&parsed.schema);

        let outcome = infer(&loaded.graph, &normalized).expect("stratifiable schema");

        assert!(outcome.graph.contains(&oxrdf::Triple::new(
            oxrdf::NamedNode::new_unchecked("http://ex/item"),
            oxrdf::NamedNode::new_unchecked("http://ex/hasTag"),
            oxrdf::NamedNode::new_unchecked("http://ex/Tag"),
        )));
    }

    /// Parse + normalize + infer, asserting the parser emitted no diagnostics
    /// (so the node expressions under test were actually lowered, not skipped).
    fn infer_ttl(ttl: &[u8]) -> InferenceOutcome {
        let loaded = shifty_parse::load_turtle(ttl, None).unwrap();
        let parsed = shifty_parse::parse_loaded(&loaded);
        assert!(
            parsed.diagnostics.is_empty(),
            "parse diagnostics: {:?}",
            parsed.diagnostics
        );
        let normalized = shifty_opt::normalize(&parsed.schema);
        infer(&loaded.graph, &normalized).expect("stratifiable schema")
    }

    fn triple_term(s: &str, p: &str, o: impl Into<oxrdf::Term>) -> oxrdf::Triple {
        oxrdf::Triple::new(
            oxrdf::NamedNode::new_unchecked(s),
            oxrdf::NamedNode::new_unchecked(p),
            o,
        )
    }

    #[test]
    fn rule_object_union_node_expression() {
        // sh:union of two paths: both reachable values are inferred.
        let outcome = infer_ttl(
            br#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            ex:PersonShape a sh:NodeShape ;
                sh:targetClass ex:Person ;
                sh:rule [
                    a sh:TripleRule ;
                    sh:subject sh:this ;
                    sh:predicate ex:contact ;
                    sh:object [ sh:union ( [ sh:path ex:email ] [ sh:path ex:phone ] ) ]
                ] .
            ex:alice a ex:Person ; ex:email "a@x.org" ; ex:phone "555-1234" .
        "#,
        );
        let email = oxrdf::Literal::new_simple_literal("a@x.org");
        let phone = oxrdf::Literal::new_simple_literal("555-1234");
        assert!(outcome.graph.contains(&triple_term(
            "http://ex/alice",
            "http://ex/contact",
            email
        )));
        assert!(outcome.graph.contains(&triple_term(
            "http://ex/alice",
            "http://ex/contact",
            phone
        )));
    }

    #[test]
    fn rule_object_intersection_node_expression() {
        // sh:intersection of two paths: only the value reachable by both is inferred.
        let outcome = infer_ttl(
            br#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            ex:S a sh:NodeShape ;
                sh:targetClass ex:T ;
                sh:rule [
                    a sh:TripleRule ;
                    sh:subject sh:this ;
                    sh:predicate ex:both ;
                    sh:object [ sh:intersection ( [ sh:path ex:a ] [ sh:path ex:b ] ) ]
                ] .
            ex:x a ex:T ; ex:a ex:shared, ex:onlyA ; ex:b ex:shared, ex:onlyB .
        "#,
        );
        let both = "http://ex/both";
        assert!(outcome.graph.contains(&triple_term(
            "http://ex/x",
            both,
            oxrdf::NamedNode::new_unchecked("http://ex/shared")
        )));
        assert!(!outcome.graph.contains(&triple_term(
            "http://ex/x",
            both,
            oxrdf::NamedNode::new_unchecked("http://ex/onlyA")
        )));
        assert!(!outcome.graph.contains(&triple_term(
            "http://ex/x",
            both,
            oxrdf::NamedNode::new_unchecked("http://ex/onlyB")
        )));
    }

    #[test]
    fn rule_object_filter_node_expression() {
        // sh:filterShape + sh:nodes: keep only the values that conform to the shape.
        let outcome = infer_ttl(
            br#"
            @prefix sh:  <http://www.w3.org/ns/shacl#> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            @prefix ex:  <http://ex/> .
            ex:IntShape a sh:NodeShape ; sh:datatype xsd:integer .
            ex:S a sh:NodeShape ;
                sh:targetClass ex:T ;
                sh:rule [
                    a sh:TripleRule ;
                    sh:subject sh:this ;
                    sh:predicate ex:intValue ;
                    sh:object [ sh:filterShape ex:IntShape ; sh:nodes [ sh:path ex:value ] ]
                ] .
            ex:x a ex:T ; ex:value 42, "hello" .
        "#,
        );
        let int_value = "http://ex/intValue";
        assert!(outcome.graph.contains(&triple_term(
            "http://ex/x",
            int_value,
            oxrdf::Literal::new_typed_literal(
                "42",
                oxrdf::NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer")
            )
        )));
        assert!(!outcome.graph.contains(&triple_term(
            "http://ex/x",
            int_value,
            oxrdf::Literal::new_simple_literal("hello")
        )));
    }

    #[test]
    fn planned_validation_preserves_severity_and_applies_threshold() {
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ;
                sh:targetNode ex:x ;
                sh:property ex:InfoShape, ex:WarningShape .
            ex:InfoShape a sh:PropertyShape ;
                sh:path ex:required ;
                sh:minCount 1 ;
                sh:severity sh:Info .
            ex:WarningShape a sh:PropertyShape ;
                sh:path ex:required ;
                sh:minCount 1 ;
                sh:severity sh:Warning .
            "
        );
        let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap();
        let parsed = shifty_parse::parse_loaded(&loaded);
        let normalized = shifty_opt::normalize(&parsed.schema);
        let plan = shifty_opt::plan(&normalized);

        let info = validate_plan_with_options(
            &loaded.graph,
            &plan,
            &ValidationOptions {
                minimum_severity: shifty_algebra::Severity::Info,
                sort_results: true,
            },
        )
        .unwrap();
        assert!(!info.conforms);
        assert_eq!(info.violations.len(), 1);
        assert_eq!(
            info.violations[0].severity,
            shifty_algebra::Severity::Warning
        );
        let mut severities: Vec<_> = info.violations[0]
            .reasons
            .iter()
            .map(|reason| reason.severity.clone())
            .collect();
        severities.sort_by_key(shifty_algebra::Severity::rank);
        assert_eq!(
            severities,
            vec![
                shifty_algebra::Severity::Info,
                shifty_algebra::Severity::Warning
            ]
        );

        let warning = validate_plan_with_options(
            &loaded.graph,
            &plan,
            &ValidationOptions {
                minimum_severity: shifty_algebra::Severity::Warning,
                sort_results: true,
            },
        )
        .unwrap();
        assert!(!warning.conforms);

        let violation = validate_plan_with_options(
            &loaded.graph,
            &plan,
            &ValidationOptions {
                minimum_severity: shifty_algebra::Severity::Violation,
                sort_results: true,
            },
        )
        .unwrap();
        assert!(violation.conforms);
        assert_eq!(violation.violations.len(), 1);

        let report = validate_report_with_options(
            &loaded,
            &loaded.graph,
            &ValidationOptions {
                minimum_severity: shifty_algebra::Severity::Violation,
                sort_results: true,
            },
        );
        assert!(report.conforms);
        assert_eq!(report.results.len(), 2);
    }

    #[test]
    fn validation_findings_sort_by_severity_then_focus_node() {
        let ttl = format!(
            "{PREFIXES}
            ex:InfoShape a sh:NodeShape ;
                sh:targetNode ex:a ;
                sh:nodeKind sh:Literal ;
                sh:severity sh:Info .
            ex:WarningShape a sh:NodeShape ;
                sh:targetNode ex:z ;
                sh:nodeKind sh:Literal ;
                sh:severity sh:Warning .
            ex:ViolationShape a sh:NodeShape ;
                sh:targetNode ex:m ;
                sh:nodeKind sh:Literal .
            "
        );
        let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap();
        let parsed = shifty_parse::parse_loaded(&loaded);
        let plan = shifty_opt::plan(&shifty_opt::normalize(&parsed.schema));
        let outcome = validate_plan(&loaded.graph, &plan).unwrap();

        let ordered: Vec<_> = outcome
            .violations
            .iter()
            .map(|finding| (finding.severity.clone(), finding.focus.to_string()))
            .collect();
        assert_eq!(
            ordered,
            vec![
                (
                    shifty_algebra::Severity::Violation,
                    "<http://ex/m>".to_string()
                ),
                (
                    shifty_algebra::Severity::Warning,
                    "<http://ex/z>".to_string()
                ),
                (shifty_algebra::Severity::Info, "<http://ex/a>".to_string()),
            ]
        );
    }

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
    fn expression_constraint_reports_non_true_values() {
        // The W3C booleans-001 shape: sh:expression sh:this over boolean foci.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ;
                sh:targetNode true, false ;
                sh:expression sh:this .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        assert!(
            parsed.diagnostics.is_empty(),
            "diags: {:?}",
            parsed.diagnostics
        );
        let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap();

        // algebra path: only the `false` focus violates.
        let algebra = validate(&loaded.graph, &parsed.schema).unwrap();
        assert!(!algebra.conforms);
        assert_eq!(algebra.violations.len(), 1);
        assert_eq!(
            algebra.violations[0].focus,
            oxrdf::Term::Literal(oxrdf::Literal::new_typed_literal(
                "false",
                oxrdf::vocab::xsd::BOOLEAN
            ))
        );

        // planned path agrees with the algebra oracle.
        let normalized = shifty_opt::normalize(&parsed.schema);
        let plan = shifty_opt::plan(&normalized);
        let planned = validate_plan(&loaded.graph, &plan).unwrap();
        assert_eq!(planned.conforms, algebra.conforms);
        assert_eq!(planned.violations.len(), algebra.violations.len());

        // W3C report path: one ExpressionConstraintComponent result, sh:value false.
        let report = validate_report(&loaded, &loaded.graph);
        assert!(!report.conforms);
        assert_eq!(report.results.len(), 1);
        let result = &report.results[0];
        assert_eq!(
            result.component.as_str(),
            "http://www.w3.org/ns/shacl#ExpressionConstraintComponent"
        );
        assert_eq!(result.path, None);
        assert_eq!(
            result.value.as_ref().map(ToString::to_string),
            Some("\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>".to_string())
        );
    }

    #[test]
    fn expression_constraint_with_path_and_filter() {
        // The expression traverses a path from the focus and filters the values
        // by a shape; only nodes passing the filter must (here, fail to) be true,
        // exercising Path + Filter node expressions on the report path.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ;
                sh:targetNode ex:x ;
                sh:expression [
                    sh:filterShape [ sh:datatype xsd:boolean ] ;
                    sh:nodes [ sh:path ex:flag ] ;
                ] .
            ex:x ex:flag true, false, \"not-a-bool\" .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        assert!(
            parsed.diagnostics.is_empty(),
            "diags: {:?}",
            parsed.diagnostics
        );
        let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap();

        // The boolean values that survive the filter are {true, false}; `false`
        // is the lone non-true value, so exactly one result, sh:value false.
        let report = validate_report(&loaded, &loaded.graph);
        assert!(!report.conforms);
        assert_eq!(report.results.len(), 1);
        assert_eq!(
            report.results[0].component.as_str(),
            "http://www.w3.org/ns/shacl#ExpressionConstraintComponent"
        );
        assert_eq!(
            report.results[0].value.as_ref().map(ToString::to_string),
            Some("\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>".to_string())
        );

        // algebra path agrees on (non-)conformance.
        let algebra = validate(&loaded.graph, &parsed.schema).unwrap();
        assert!(!algebra.conforms);
        assert_eq!(algebra.violations.len(), 1);
    }

    #[test]
    fn expression_constraint_with_function_is_diagnosed() {
        // A function application inside sh:expression cannot be evaluated by the
        // validation paths yet, so it is diagnosed rather than silently dropped.
        let ttl = format!(
            "{PREFIXES}
            ex:S a sh:NodeShape ;
                sh:targetNode ex:x ;
                sh:expression [ ex:fn ( sh:this ) ] .
            "
        );
        let parsed = parse_turtle(ttl.as_bytes(), None).unwrap();
        assert!(
            parsed
                .diagnostics
                .iter()
                .any(|d| d.message.contains("sh:expression")),
            "expected an unsupported-expression diagnostic, got: {:?}",
            parsed.diagnostics
        );
    }

    #[test]
    fn sparql_function_expression_evaluates() {
        // The W3C simpleSPARQLFunction shapes: a no-arg ASK function and a
        // two-argument SELECT function, called via dash:expression strings.
        let ttl = br#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            ex:booleanFunction a sh:SPARQLFunction ;
                sh:returnType xsd:boolean ;
                sh:ask "ASK { FILTER (true) }" .
            ex:withArguments a sh:SPARQLFunction ;
                sh:parameter [ sh:name "arg1" ; sh:path ex:arg1 ] ,
                             [ sh:name "arg2" ; sh:path ex:arg2 ] ;
                sh:returnType xsd:string ;
                sh:select "SELECT ?result WHERE { BIND (CONCAT($arg1, \"-\", $arg2) AS ?result) }" .
        "#;
        let loaded = shifty_parse::load_turtle(ttl, None).unwrap();

        let b = evaluate_function_expression(&loaded, "ex:booleanFunction()").unwrap();
        assert_eq!(b, Some(oxrdf::Term::Literal(oxrdf::Literal::from(true))));

        let s = evaluate_function_expression(&loaded, "ex:withArguments(\"A\", \"B\")").unwrap();
        assert_eq!(
            s,
            Some(oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(
                "A-B"
            )))
        );
    }

    #[test]
    fn sparql_function_called_from_sparql_constraint() {
        // A SHACL function is callable inside a sh:sparql constraint query: the
        // constraint flags values for which ex:isOk returns false.
        let ttl = br#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            ex:isOk a sh:SPARQLFunction ;
                sh:parameter [ sh:path ex:arg ] ;
                sh:returnType xsd:boolean ;
                sh:ask "ASK { FILTER (STR($arg) = \"ok\") }" .
            ex:S a sh:NodeShape ;
                sh:targetNode ex:x, ex:y ;
                sh:sparql [ sh:select """SELECT $this ?value WHERE {
                    $this <http://ex/val> ?value .
                    FILTER (! <http://ex/isOk>(?value))
                }""" ] .
            ex:x <http://ex/val> "ok" .
            ex:y <http://ex/val> "bad" .
        "#;
        let loaded = shifty_parse::load_turtle(ttl, None).unwrap();
        let report = validate_report(&loaded, &loaded.graph);
        assert!(!report.conforms);
        assert_eq!(report.results.len(), 1, "results: {:?}", report.results);
        assert_eq!(report.results[0].focus.to_string(), "<http://ex/y>");
        assert_eq!(
            report.results[0].value.as_ref().map(ToString::to_string),
            Some("\"bad\"".to_string())
        );
    }

    #[test]
    fn custom_component_ask_validator() {
        // A two-parameter ASK component (the W3C validator-001 shape): a value
        // conforms iff it is the concatenation of the two parameters.
        let ttl = br#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            ex:TestConstraintComponent a sh:ConstraintComponent ;
                sh:parameter [ sh:path ex:test1 ] , [ sh:path ex:test2 ] ;
                sh:validator [ a sh:SPARQLAskValidator ;
                    sh:ask "ASK { FILTER (?value = CONCAT($test1, $test2)) }" ] .
            ex:TestShape a sh:NodeShape ;
                ex:test1 "Hello " ;
                ex:test2 "World" ;
                sh:targetNode "Hallo Welt", "Hello World" .
        "#;
        let loaded = shifty_parse::load_turtle(ttl, None).unwrap();
        let report = validate_report(&loaded, &loaded.graph);
        assert!(!report.conforms);
        assert_eq!(report.results.len(), 1);
        let r = &report.results[0];
        assert_eq!(r.component.as_str(), "http://ex/TestConstraintComponent");
        assert_eq!(r.focus.to_string(), "\"Hallo Welt\"");
        assert_eq!(
            r.value.as_ref().map(ToString::to_string),
            Some("\"Hallo Welt\"".to_string())
        );
        assert_eq!(r.source_shape.to_string(), "<http://ex/TestShape>");
        assert_eq!(r.path, None);

        // The lowered algebra cannot evaluate components, so it diagnoses the
        // activation rather than silently under-validating.
        let parsed = parse_turtle(ttl, None).unwrap();
        assert!(
            parsed
                .diagnostics
                .iter()
                .any(|d| d.message.contains("custom constraint component")),
            "expected a custom-component diagnostic, got: {:?}",
            parsed.diagnostics
        );
    }

    #[test]
    fn custom_component_select_node_validator() {
        // A SELECT node validator with a required parameter: a focus violates
        // when it lacks an ex:property edge equal to the bound $requiredParam.
        let ttl = br#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            ex:C a sh:ConstraintComponent ;
                sh:parameter [ sh:path ex:requiredParam ] ;
                sh:nodeValidator [ a sh:SPARQLSelectValidator ;
                    sh:select """SELECT $this WHERE {
                        $this ?p ?o .
                        FILTER NOT EXISTS { $this <http://ex/property> $requiredParam }
                    }""" ] .
            ex:S a sh:NodeShape ;
                ex:requiredParam "Value" ;
                sh:targetNode ex:Good, ex:Bad .
            ex:Good <http://ex/property> "Value" .
            ex:Bad <http://ex/property> "Other" .
        "#;
        let loaded = shifty_parse::load_turtle(ttl, None).unwrap();
        let report = validate_report(&loaded, &loaded.graph);
        assert!(!report.conforms);
        assert_eq!(report.results.len(), 1);
        let r = &report.results[0];
        assert_eq!(r.component.as_str(), "http://ex/C");
        assert_eq!(r.focus.to_string(), "<http://ex/Bad>");
        assert_eq!(
            r.value.as_ref().map(ToString::to_string),
            Some("<http://ex/Bad>".to_string())
        );
    }

    #[test]
    fn custom_component_not_activated_when_mandatory_param_absent() {
        // The component is only activated for shapes that supply every mandatory
        // parameter; a shape missing it produces no results (and no diagnostic).
        let ttl = br#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            ex:C a sh:ConstraintComponent ;
                sh:parameter [ sh:path ex:requiredParam ] ;
                sh:validator [ a sh:SPARQLAskValidator ; sh:ask "ASK { FILTER (false) }" ] .
            ex:S a sh:NodeShape ;
                sh:targetNode ex:x .
            ex:x ex:other "z" .
        "#;
        let loaded = shifty_parse::load_turtle(ttl, None).unwrap();
        let report = validate_report(&loaded, &loaded.graph);
        assert!(report.conforms, "results: {:?}", report.results);

        let parsed = parse_turtle(ttl, None).unwrap();
        assert!(
            !parsed
                .diagnostics
                .iter()
                .any(|d| d.message.contains("custom constraint component")),
            "an inactive component must not be diagnosed: {:?}",
            parsed.diagnostics
        );
    }

    #[test]
    fn custom_component_property_validator_complex_path() {
        // A property shape with a *sequence* path activates a SELECT property
        // validator: `$PATH` is pre-bound to ex:a/ex:b, so the validator reaches
        // the value nodes two hops away and flags the forbidden one.
        let ttl = br#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            ex:ForbidComponent a sh:ConstraintComponent ;
                sh:parameter [ sh:path ex:forbidden ] ;
                sh:propertyValidator [ a sh:SPARQLSelectValidator ;
                    sh:select """SELECT $this ?value WHERE {
                        $this $PATH ?value .
                        FILTER (STR(?value) = STR($forbidden))
                    }""" ] .
            ex:S a sh:NodeShape ;
                sh:targetNode ex:x ;
                sh:property [ sh:path ( ex:a ex:b ) ; ex:forbidden "bad" ] .
            ex:x ex:a ex:m .
            ex:m ex:b "bad", "ok" .
        "#;
        let loaded = shifty_parse::load_turtle(ttl, None).unwrap();
        let report = validate_report(&loaded, &loaded.graph);
        assert!(!report.conforms);
        assert_eq!(report.results.len(), 1, "results: {:?}", report.results);
        let r = &report.results[0];
        assert_eq!(r.component.as_str(), "http://ex/ForbidComponent");
        assert_eq!(r.focus.to_string(), "<http://ex/x>");
        assert_eq!(
            r.value.as_ref().map(ToString::to_string),
            Some("\"bad\"".to_string())
        );
    }

    #[test]
    fn custom_component_property_validator_inverse_path() {
        // An inverse path `^ex:parent`: the value nodes are the subjects that
        // point at the focus via ex:parent. The ASK validator runs per value node
        // with `$PATH` pre-bound, flagging values whose label is not "ok".
        let ttl = br#"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://ex/> .
            ex:OkComponent a sh:ConstraintComponent ;
                sh:parameter [ sh:path ex:want ] ;
                sh:validator [ a sh:SPARQLAskValidator ;
                    sh:ask "ASK { $value <http://ex/label> $want }" ] .
            ex:S a sh:NodeShape ;
                sh:targetNode ex:p ;
                sh:property [ sh:path [ sh:inversePath ex:parent ] ; ex:want "ok" ] .
            ex:c1 ex:parent ex:p ; ex:label "ok" .
            ex:c2 ex:parent ex:p ; ex:label "no" .
        "#;
        let loaded = shifty_parse::load_turtle(ttl, None).unwrap();
        let report = validate_report(&loaded, &loaded.graph);
        // Value nodes of ex:p along ^ex:parent are {ex:c1, ex:c2}; only ex:c2
        // lacks `ex:label "ok"`, so the ASK is false there → one violation.
        assert!(!report.conforms);
        assert_eq!(report.results.len(), 1, "results: {:?}", report.results);
        assert_eq!(
            report.results[0].component.as_str(),
            "http://ex/OkComponent"
        );
        assert_eq!(
            report.results[0].value.as_ref().map(ToString::to_string),
            Some("<http://ex/c2>".to_string())
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
