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

    test_property_and_001: "tests/test-suite/core/property/and-001.ttl",
    test_property_class_001: "tests/test-suite/core/property/class-001.ttl",
    test_property_datatype_001: "tests/test-suite/core/property/datatype-001.ttl",
    test_property_datatype_002: "tests/test-suite/core/property/datatype-002.ttl",
    test_property_datatype_003: "tests/test-suite/core/property/datatype-003.ttl",
    test_property_datatypeIllFormed: "tests/test-suite/core/property/datatype-ill-formed.ttl",
    test_property_datatypeIllFormedData: "tests/test-suite/core/property/datatype-ill-formed-data.ttl",
    test_property_datatypeIllFormedShapes: "tests/test-suite/core/property/datatype-ill-formed-shapes.ttl",
    test_property_disjoint_001: "tests/test-suite/core/property/disjoint-001.ttl",
    test_property_equals_001: "tests/test-suite/core/property/equals-001.ttl",
    test_property_hasValue_001: "tests/test-suite/core/property/hasValue-001.ttl",
    test_property_in_001: "tests/test-suite/core/property/in-001.ttl",
    test_property_languageIn_001: "tests/test-suite/core/property/languageIn-001.ttl",
    test_property_lessThan_001: "tests/test-suite/core/property/lessThan-001.ttl",
    test_property_lessThan_002: "tests/test-suite/core/property/lessThan-002.ttl",
    test_property_lessThanOrEquals_001: "tests/test-suite/core/property/lessThanOrEquals-001.ttl",
    test_property_manifest: "tests/test-suite/core/property/manifest.ttl",
    test_property_maxCount_001: "tests/test-suite/core/property/maxCount-001.ttl",
    test_property_maxCount_002: "tests/test-suite/core/property/maxCount-002.ttl",
    test_property_maxExclusive_001: "tests/test-suite/core/property/maxExclusive-001.ttl",
    test_property_maxInclusive_001: "tests/test-suite/core/property/maxInclusive-001.ttl",
    test_property_maxLength_001: "tests/test-suite/core/property/maxLength-001.ttl",
    test_property_minCount_001: "tests/test-suite/core/property/minCount-001.ttl",
    test_property_minCount_002: "tests/test-suite/core/property/minCount-002.ttl",
    test_property_minExclusive_001: "tests/test-suite/core/property/minExclusive-001.ttl",
    test_property_minExclusive_002: "tests/test-suite/core/property/minExclusive-002.ttl",
    test_property_minLength_001: "tests/test-suite/core/property/minLength-001.ttl",
    test_property_node_001: "tests/test-suite/core/property/node-001.ttl",
    test_property_node_002: "tests/test-suite/core/property/node-002.ttl",
    test_property_nodeKind_001: "tests/test-suite/core/property/nodeKind-001.ttl",
    test_property_not_001: "tests/test-suite/core/property/not-001.ttl",
    test_property_or_001: "tests/test-suite/core/property/or-001.ttl",
    test_property_orDatatypes_001: "tests/test-suite/core/property/or-datatypes-001.ttl",
    test_property_pattern_001: "tests/test-suite/core/property/pattern-001.ttl",
    test_property_pattern_002: "tests/test-suite/core/property/pattern-002.ttl",
    test_property_property_001: "tests/test-suite/core/property/property-001.ttl",
    test_property_qualifiedMinCountDisjoint_001: "tests/test-suite/core/property/qualifiedMinCountDisjoint-001.ttl",
    test_property_qualifiedValueShape_001: "tests/test-suite/core/property/qualifiedValueShape-001.ttl",
    test_property_qualifiedValueShapesDisjoint_001: "tests/test-suite/core/property/qualifiedValueShapesDisjoint-001.ttl",
    test_property_uniqueLang_001: "tests/test-suite/core/property/uniqueLang-001.ttl",
    test_property_uniqueLang002: "tests/test-suite/core/property/uniqueLang-002.ttl",


    test_node_and_001: "tests/test-suite/core/node/and-001.ttl",
    test_node_and_002: "tests/test-suite/core/node/and-002.ttl",
    test_node_class_001: "tests/test-suite/core/node/class-001.ttl",
    test_node_class_002: "tests/test-suite/core/node/class-002.ttl",
    test_node_class_003: "tests/test-suite/core/node/class-003.ttl",
    test_node_closed_001: "tests/test-suite/core/node/closed-001.ttl",
    test_node_closed_002: "tests/test-suite/core/node/closed-002.ttl",
    test_node_datatype_001: "tests/test-suite/core/node/datatype-001.ttl",
    test_node_datatype_002: "tests/test-suite/core/node/datatype-002.ttl",
    test_node_disjoint_001: "tests/test-suite/core/node/disjoint-001.ttl",
    test_node_equals_001: "tests/test-suite/core/node/equals-001.ttl",
    test_node_hasValue_001: "tests/test-suite/core/node/hasValue-001.ttl",
    test_node_in_001: "tests/test-suite/core/node/in-001.ttl",
    test_node_languageIn_001: "tests/test-suite/core/node/languageIn-001.ttl",
    test_node_manifest: "tests/test-suite/core/node/manifest.ttl",
    test_node_maxExclusive_001: "tests/test-suite/core/node/maxExclusive-001.ttl",
    test_node_maxInclusive_001: "tests/test-suite/core/node/maxInclusive-001.ttl",
    test_node_maxLength_001: "tests/test-suite/core/node/maxLength-001.ttl",
    test_node_minExclusive_001: "tests/test-suite/core/node/minExclusive-001.ttl",
    test_node_minInclusive_001: "tests/test-suite/core/node/minInclusive-001.ttl",
    test_node_minInclusive_002: "tests/test-suite/core/node/minInclusive-002.ttl",
    test_node_minInclusive_003: "tests/test-suite/core/node/minInclusive-003.ttl",
    test_node_minLength_001: "tests/test-suite/core/node/minLength-001.ttl",
    test_node_node_001: "tests/test-suite/core/node/node-001.ttl",
    test_node_nodeKind_001: "tests/test-suite/core/node/nodeKind-001.ttl",
    test_node_not_001: "tests/test-suite/core/node/not-001.ttl",
    test_node_not_002: "tests/test-suite/core/node/not-002.ttl",
    test_node_or_001: "tests/test-suite/core/node/or-001.ttl",
    test_node_pattern_001: "tests/test-suite/core/node/pattern-001.ttl",
    test_node_pattern_002: "tests/test-suite/core/node/pattern-002.ttl",
    test_node_qualified_001_data: "tests/test-suite/core/node/qualified-001-data.ttl",
    test_node_qualified_001_shapes: "tests/test-suite/core/node/qualified-001-shapes.ttl",
    test_node_qualified_001: "tests/test-suite/core/node/qualified-001.ttl",
    test_node_xone_001: "tests/test-suite/core/node/xone-001.ttl",
    test_node_xoneDuplicate: "tests/test-suite/core/node/xone-duplicate.ttl",
    test_node_xoneDuplicateData: "tests/test-suite/core/node/xone-duplicate-data.ttl",
    test_node_xoneDuplicateShapes: "tests/test-suite/core/node/xone-duplicate-shapes.ttl",


}
