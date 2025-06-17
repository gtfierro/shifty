use oxigraph::io::{RdfFormat, RdfSerializer};
use shacl::canonicalization::{are_isomorphic, graph_diff};
use shacl::context::ValidationContext;
use shacl::test_utils::{load_manifest, Manifest, TestCase};
use std::error::Error;
use std::path::Path;
use std::path::PathBuf;

fn graph_to_turtle(graph: &oxigraph::model::Graph) -> Result<String, Box<dyn Error>> {
    let mut buffer = Vec::new();
    let mut serializer = RdfSerializer::from_format(RdfFormat::Turtle).for_writer(&mut buffer);
    for triple in graph.iter() {
        serializer.serialize_triple(triple)?;
    }
    let turtle_string = String::from_utf8(buffer)?;
    // put a '.' at the end to make it a valid Turtle document
    let turtle_string = if turtle_string.ends_with('.') {
        turtle_string
    } else {
        format!("{}\n.", turtle_string)
    };
    Ok(turtle_string)
}

fn parse_tests(tests: &[&str]) -> Result<Vec<TestCase>, Box<dyn Error>> {
    let mut all_tests = Vec::new();
    for test_path in tests {
        let test_path = PathBuf::from(test_path);
        let man = load_manifest(&test_path).map_err(|e| {
            format!(
                "Failed to load manifest from {}: {}",
                test_path.display(),
                e
            )
        })?;
        all_tests.extend(man.test_cases);
    }
    Ok(all_tests)
}

fn run_test_file(file: &str) -> Result<(), Box<dyn Error>> {
    let tests = parse_tests(&[file])?;
    for test in tests {
        let test_name = test.name.as_str();
        println!("Running test: {} from file: {}", test_name, file);
        let data_graph_path = test
            .data_graph_path
            .to_str()
            .ok_or("Invalid data graph path")?;
        let shapes_graph_path = test
            .shapes_graph_path
            .to_str()
            .ok_or("Invalid shapes graph path")?;
        let context =
            ValidationContext::from_files(shapes_graph_path, data_graph_path).map_err(|e| {
                format!(
                    "Failed to create ValidationContext for test '{}': {}",
                    test_name, e
                )
            })?;
        let report = context.validate();
        let conforms = report.results().is_empty();
        let expects_conform = test.conforms;
        let report_graph = report.to_graph(&context);
        let report_turtle = graph_to_turtle(&report_graph).map_err(|e| {
            format!(
                "Failed to convert report graph to Turtle for test '{}': {}",
                test_name, e
            )
        })?;
        // write to report.ttl
        let report_path = PathBuf::from("report.ttl");
        std::fs::write(&report_path, report_turtle.as_bytes()).map_err(|e| {
            format!(
                "Failed to write report to {} for test '{}': {}",
                report_path.display(),
                test_name,
                e
            )
        })?;
        let expected_turtle = graph_to_turtle(&test.expected_report).map_err(|e| {
            format!(
                "Failed to convert expected report graph to Turtle for test '{}': {}",
                test_name, e
            )
        })?;
        // write to expected.ttl
        let expected_path = PathBuf::from("expected.ttl");
        std::fs::write(&expected_path, expected_turtle.as_bytes()).map_err(|e| {
            format!(
                "Failed to write expected report to {} for test '{}': {}",
                expected_path.display(),
                test_name,
                e
            )
        })?;
        assert_eq!(
            conforms, expects_conform,
            "Conformance mismatch for test: {}. Expected {}\n(expected report:\n {})\n(our report:\n {})",
            test_name, expects_conform, expected_turtle, report_turtle
        );
        //let diff = graph_diff(&report_graph, &test.expected_report);
        //diff.dump();
        assert!(are_isomorphic(
            &report_graph,
            &test.expected_report,
        ), "Validation report does not match expected report for test: {}.\nExpected:\n{}\nGot:\n{}",
            test_name, expected_turtle, report_turtle
        );
    }
    Ok(())
}

macro_rules! generate_test_cases {
    ($($name:ident: $file:expr),* $(,)?) => {
        $(
            #[test]
            fn $name() -> Result<(), Box<dyn std::error::Error>> {
                run_test_file($file)
            }
        )*
    }
}

generate_test_cases! {
    test_targetNode_001: "tests/test-suite/core/targets/targetNode-001.ttl",
    test_targetClass_001: "tests/test-suite/core/targets/targetClass-001.ttl",
    test_targetObjectsOf_001: "tests/test-suite/core/targets/targetObjectsOf-001.ttl",
    test_multipleTargets_001: "tests/test-suite/core/targets/multipleTargets-001.ttl",
    test_targetSubjectsOf_001: "tests/test-suite/core/targets/targetSubjectsOf-001.ttl",
    test_targetSubjectsOf_002: "tests/test-suite/core/targets/targetSubjectsOf-002.ttl",
    test_targetClassImplicit_001: "tests/test-suite/core/targets/targetClassImplicit-001.ttl",

    test_and_001: "tests/test-suite/core/property/and-001.ttl",
    test_class_001: "tests/test-suite/core/property/class-001.ttl",
    test_datatype_001: "tests/test-suite/core/property/datatype-001.ttl",
    test_datatype_002: "tests/test-suite/core/property/datatype-002.ttl",
    test_datatype_003: "tests/test-suite/core/property/datatype-003.ttl",
    test_datatypeIllFormed: "tests/test-suite/core/property/datatype-ill-formed.ttl",
    test_datatypeIllFormedData: "tests/test-suite/core/property/datatype-ill-formed-data.ttl",
    test_datatypeIllFormedShapes: "tests/test-suite/core/property/datatype-ill-formed-shapes.ttl",
    test_disjoint_001: "tests/test-suite/core/property/disjoint-001.ttl",
    test_equals_001: "tests/test-suite/core/property/equals-001.ttl",
    test_hasValue_001: "tests/test-suite/core/property/hasValue-001.ttl",
    test_in_001: "tests/test-suite/core/property/in-001.ttl",
    test_languageIn_001: "tests/test-suite/core/property/languageIn-001.ttl",
    test_lessThan_001: "tests/test-suite/core/property/lessThan-001.ttl",
    test_lessThan_002: "tests/test-suite/core/property/lessThan-002.ttl",
    test_lessThanOrEquals_001: "tests/test-suite/core/property/lessThanOrEquals-001.ttl",
    test_manifest: "tests/test-suite/core/property/manifest.ttl",
    test_maxCount_001: "tests/test-suite/core/property/maxCount-001.ttl",
    test_maxCount_002: "tests/test-suite/core/property/maxCount-002.ttl",
    test_maxExclusive_001: "tests/test-suite/core/property/maxExclusive-001.ttl",
    test_maxInclusive_001: "tests/test-suite/core/property/maxInclusive-001.ttl",
    test_maxLength_001: "tests/test-suite/core/property/maxLength-001.ttl",
    test_minCount_001: "tests/test-suite/core/property/minCount-001.ttl",
    test_minCount_002: "tests/test-suite/core/property/minCount-002.ttl",
    test_minExclusive_001: "tests/test-suite/core/property/minExclusive-001.ttl",
    test_minExclusive_002: "tests/test-suite/core/property/minExclusive-002.ttl",
    test_minLength_001: "tests/test-suite/core/property/minLength-001.ttl",
    test_node_001: "tests/test-suite/core/property/node-001.ttl",
    test_node_002: "tests/test-suite/core/property/node-002.ttl",
    test_nodeKind_001: "tests/test-suite/core/property/nodeKind-001.ttl",
    test_not_001: "tests/test-suite/core/property/not-001.ttl",
    test_or_001: "tests/test-suite/core/property/or-001.ttl",
    test_orDatatypes_001: "tests/test-suite/core/property/or-datatypes-001.ttl",
    test_pattern_001: "tests/test-suite/core/property/pattern-001.ttl",
    test_pattern_002: "tests/test-suite/core/property/pattern-002.ttl",
    test_property_001: "tests/test-suite/core/property/property-001.ttl",
    test_qualifiedMinCountDisjoint_001: "tests/test-suite/core/property/qualifiedMinCountDisjoint-001.ttl",
    test_qualifiedValueShape_001: "tests/test-suite/core/property/qualifiedValueShape-001.ttl",
    test_qualifiedValueShapesDisjoint_001: "tests/test-suite/core/property/qualifiedValueShapesDisjoint-001.ttl",
    test_uniqueLang_001: "tests/test-suite/core/property/uniqueLang-001.ttl",
    test_uniqueLang002: "tests/test-suite/core/property/uniqueLang-002.ttl",
}
