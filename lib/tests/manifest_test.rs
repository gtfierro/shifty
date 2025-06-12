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
        let context = ValidationContext::from_files(shapes_graph_path, data_graph_path)
            .map_err(|e| format!("Failed to create ValidationContext for test '{}': {}", test_name, e))?;
        let report = context.validate();
        let conforms = report.results().is_empty();
        let expects_conform = test.status == "conform" && test.expected_report.is_empty();
        report.dump();
        assert_eq!(
            conforms, expects_conform,
            "Conformance mismatch for test: {}. Expected {}",
            test_name, expects_conform
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
}
