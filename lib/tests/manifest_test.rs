use ontoenv::config::Config;
use oxigraph::io::{RdfFormat, RdfSerializer};
use oxigraph::model::{Graph, NamedNode};
use shifty::canonicalization::{are_isomorphic, deskolemize_graph};
use shifty::test_utils::{list_includes, load_manifest, TestCase};
use shifty::Validator;
use std::collections::{HashSet, VecDeque};
use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use url::Url;

fn graph_to_turtle(graph: &oxigraph::model::Graph) -> Result<String, Box<dyn Error + Send + Sync>> {
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

fn collect_tests_from_manifest(
    manifest_path: &Path,
) -> Result<Vec<(PathBuf, TestCase)>, Box<dyn Error + Send + Sync>> {
    let mut all_tests = Vec::new();
    let mut to_visit = VecDeque::new();
    let mut visited = HashSet::new();

    let manifest_path = manifest_path.to_path_buf();
    to_visit.push_back(manifest_path);

    while let Some(current_manifest) = to_visit.pop_front() {
        let canonical = current_manifest
            .canonicalize()
            .unwrap_or_else(|_| current_manifest.clone());
        if !visited.insert(canonical.clone()) {
            continue;
        }

        let manifest = load_manifest(&canonical).map_err(|e| {
            io::Error::other(format!(
                "Failed to load manifest from {}: {}",
                canonical.display(),
                e
            ))
        })?;
        for case in manifest.test_cases {
            all_tests.push((canonical.clone(), case));
        }

        let includes = list_includes(&canonical).map_err(|e| {
            io::Error::other(format!(
                "Failed to list mf:include targets for {}: {}",
                canonical.display(),
                e
            ))
        })?;
        for include in includes {
            let include_canonical = include.canonicalize().unwrap_or_else(|_| include.clone());
            if visited.contains(&include_canonical) {
                continue;
            }
            to_visit.push_back(include_canonical);
        }
    }

    all_tests.sort_by(|(path_a, case_a), (path_b, case_b)| {
        path_a
            .cmp(path_b)
            .then_with(|| case_a.name.cmp(&case_b.name))
    });

    Ok(all_tests)
}

fn skip_reason(test: &TestCase) -> Option<&'static str> {
    let advanced_expr = "/advanced/expression/";
    if test
        .data_graph_path
        .to_string_lossy()
        .contains(advanced_expr)
        || test
            .shapes_graph_path
            .to_string_lossy()
            .contains(advanced_expr)
    {
        return Some("SHACL-AF expressions not supported yet");
    }
    // if data_path.contains("data-shapes-test-suite") || shapes_path.contains("data-shapes-test-suite")
    // {
    //     return Some("SHACL 1.0 data-shapes suite pending parser support");
    // }
    // let is_advanced = data_path.contains("/advanced/") || shapes_path.contains("/advanced/");

    // if is_advanced {
    //     return Some("SHACL-AF advanced manifest support pending");
    // }

    None
}

fn run_test_file(file: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let tests = collect_tests_from_manifest(Path::new(file))?;
    for (manifest_path, test) in tests {
        let test_name = test.name.as_str();
        if let Some(reason) = skip_reason(&test) {
            println!(
                "[skip] {} — {} (manifest: {})",
                test_name,
                reason,
                manifest_path.display()
            );
            continue;
        }
        println!(
            "Running test: {} from manifest: {}",
            test_name,
            manifest_path.display()
        );
        let data_graph_path = test
            .data_graph_path
            .to_str()
            .ok_or("Invalid data graph path")?;
        let shapes_graph_path = test
            .shapes_graph_path
            .to_str()
            .ok_or("Invalid shapes graph path")?;
        let root = std::env::current_dir()?;
        let env_config = Config::builder()
            .root(root)
            .locations(Vec::new())
            .offline(true)
            .temporary(true)
            .build()?;

        let validator = Validator::builder()
            .with_shapes_source(shifty::Source::File(PathBuf::from(shapes_graph_path)))
            .with_data_source(shifty::Source::File(PathBuf::from(data_graph_path)))
            .with_warnings_are_errors(true)
            .with_env_config(env_config)
            .build()
            .map_err(|e| {
                io::Error::other(format!(
                    "Failed to create Validator for test '{}': {}",
                    test_name, e
                ))
            })?;
        let report = validator.validate();
        let conforms = report.conforms();
        let expects_conform = test.conforms;
        let mut report_graph = report.to_graph();

        // Deskolemize the report graph before comparison
        let data_graph_url =
            Url::from_file_path(test.data_graph_path.canonicalize()?).map_err(|()| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Failed to create file URL for data graph",
                )
            })?;
        let data_base_iri = format!("{}/.sk/", data_graph_url.as_str().trim_end_matches('/'));
        report_graph = deskolemize_graph(&report_graph, &data_base_iri);

        let shapes_graph_url = Url::from_file_path(test.shapes_graph_path.canonicalize()?)
            .map_err(|()| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Failed to create file URL for shapes graph",
                )
            })?;
        let shapes_base_iri = format!("{}/.sk/", shapes_graph_url.as_str().trim_end_matches('/'));
        report_graph = deskolemize_graph(&report_graph, &shapes_base_iri);

        let report_turtle = graph_to_turtle(&report_graph).map_err(|e| {
            io::Error::other(format!(
                "Failed to convert report graph to Turtle for test '{}': {}",
                test_name, e
            ))
        })?;
        // write to report.ttl
        let report_path = PathBuf::from("report.ttl");
        std::fs::write(&report_path, report_turtle.as_bytes()).map_err(|e| {
            io::Error::other(format!(
                "Failed to write report to {} for test '{}': {}",
                report_path.display(),
                test_name,
                e
            ))
        })?;
        let expected_turtle = graph_to_turtle(&test.expected_report).map_err(|e| {
            io::Error::other(format!(
                "Failed to convert expected report graph to Turtle for test '{}': {}",
                test_name, e
            ))
        })?;
        // write to expected.ttl
        let expected_path = PathBuf::from("expected.ttl");
        std::fs::write(&expected_path, expected_turtle.as_bytes()).map_err(|e| {
            io::Error::other(format!(
                "Failed to write expected report to {} for test '{}': {}",
                expected_path.display(),
                test_name,
                e
            ))
        })?;
        assert_eq!(
            conforms, expects_conform,
            "Conformance mismatch for test: {}. Expected {}\n(expected report:\n {})\n(our report:\n {})",
            test_name, expects_conform, expected_turtle, report_turtle
        );
        //let diff = graph_diff(&report_graph, &test.expected_report);
        //diff.dump();
        let result_message_pred =
            NamedNode::new("http://www.w3.org/ns/shacl#resultMessage").unwrap();
        let expected_has_result_messages = test
            .expected_report
            .iter()
            .any(|triple| triple.predicate == result_message_pred);

        let report_graph_for_compare: Graph = if expected_has_result_messages {
            report_graph.clone()
        } else {
            let mut filtered = Graph::new();
            for triple in report_graph.iter() {
                if triple.predicate != result_message_pred {
                    filtered.insert(triple);
                }
            }
            filtered
        };

        let expected_graph_for_compare: Graph = test.expected_report.clone();

        assert!(are_isomorphic(
            &report_graph_for_compare,
            &expected_graph_for_compare,
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
            #[ntest::timeout(10000)]
            fn $name() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                run_test_file($file)
            }
        )*
    }
}

include!(concat!(env!("OUT_DIR"), "/generated_manifest_tests.rs"));
