use shacl::context::ValidationContext;
use std::path::Path;
use shacl::test_utils::{Manifest, load_manifest, TestCase};
use std::error::Error;
use std::path::PathBuf;


fn parse_tests(tests: &[&str]) -> Result<Vec<TestCase>, Box<dyn Error>> {
    let mut all_tests = Vec::new();
    for test_path in tests {
        let test_path = PathBuf::from(test_path);
        let man = load_manifest(&test_path)
            .map_err(|e| format!("Failed to load manifest from {}: {}", test_path.display(), e))?;
        all_tests.extend(man.test_cases);
    }
    Ok(all_tests)
}

#[test]
fn run_sht_tests() -> Result<(), Box<dyn Error>> {

    let tests = [
        // targets
        "tests/test-suite/core/targets/targetNode-001.ttl",
        "tests/test-suite/core/targets/targetClass-001.ttl",
        "tests/test-suite/core/targets/targetObjectsOf-001.ttl",
        "tests/test-suite/core/targets/multipleTargets-001.ttl",
        "tests/test-suite/core/targets/targetSubjectsOf-001.ttl",
        "tests/test-suite/core/targets/targetSubjectsOf-002.ttl",
        "tests/test-suite/core/targets/targetClassImplicit-001.ttl",
    ];

    let all_tests = parse_tests(&tests)?;
    println!("Found {} SHT tests", all_tests.len());

    for test in all_tests {
        let test_node = test.node.to_string();
        let test_name = test.label.as_deref().unwrap_or(&test_node);
        println!("Running test: {}", test_name);

        let data_graph_path = test
            .data_graph_path
            .to_str()
            .ok_or("Invalid data graph path")?;
        let shapes_graph_path = test
            .shapes_graph_path
            .as_deref()
            .unwrap_or(&test.data_graph_path)
            .to_str()
            .ok_or("Invalid shapes graph path")?;

        let context = match ValidationContext::from_files(shapes_graph_path, data_graph_path) {
            Ok(c) => c,
            Err(e) => {
                // TODO: Some tests are expected to fail at parsing, we should check for that.
                // For now, we panic.
                panic!(
                    "Failed to create ValidationContext for test '{}': {}",
                    test_name, e
                );
            }
        };

        let report = context.validate();
        let conforms = report.results().is_empty();

        // A test expects to conform if it's not a failure test and has an empty result graph.
        let expects_conform = !test.expected_failure && test.result_graph.is_empty();

        assert_eq!(
            conforms, expects_conform,
            "Conformance mismatch for test: {}",
            test_name
        );

        // TODO: Add graph isomorphism check for tests with non-empty result graphs.
    }

    Ok(())
}
