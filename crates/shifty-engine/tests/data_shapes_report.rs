//! Selected-field report regressions against locally adapted Core fixtures.
//!
//! Full W3C report comparison is performed by python/conformance against the
//! unmodified upstream manifests; see testdata/README.md.
//!
//! For each test we compare our `sh:ValidationReport` to the expected one,
//! matching result-sets on (focusNode, resultPath, value, component,
//! sourceShape).
//!
//! Blank-node handling:
//! - focusNode / value: expected and actual come from the same Turtle file so
//!   blank-node IDs are identical on both sides — exact comparison.
//! - resultPath: complex paths are blank-node subgraphs; two DIFFERENT blank
//!   nodes in the file represent the same path (one in the shape definition,
//!   one in the expected result). Canonicalized by parsing and re-serializing.
//! - sourceShape: expected results sometimes use an empty blank node `[ ]` as a
//!   placeholder meaning "some anonymous shape"; wildcarded.
//!
//! Severity and message fields are not checked.
//! Unsupported feature regions are skipped; every executed report case must match.

mod common;

use common::{Key, expected_report, result_key};
use std::path::{Path, PathBuf};

const UNSUPPORTED: &[&str] = &[
    "http://www.w3.org/ns/shacl#sparql",
    "http://www.w3.org/ns/shacl#rule",
    "http://www.w3.org/ns/shacl#target",
    "http://www.w3.org/ns/shacl#js",
    "http://www.w3.org/ns/shacl#parameter",
];

fn suite_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata/data-shapes/data-shapes-test-suite/tests/core")
        .canonicalize()
        .expect("data-shapes core suite present")
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            collect(&p, out);
        } else if p.extension().is_some_and(|x| x == "ttl")
            && p.file_name().is_some_and(|n| n != "manifest.ttl")
        {
            out.push(p);
        }
    }
}

#[test]
fn data_shapes_core_reports() {
    let mut files = Vec::new();
    collect(&suite_dir(), &mut files);
    files.sort();
    assert!(!files.is_empty());

    let (mut pass, mut fail, mut skip) = (0u32, 0u32, 0u32);
    let mut failures = Vec::new();

    for file in &files {
        let bytes = std::fs::read(file).unwrap();
        let Ok(loaded) = shifty_parse::load_turtle(&bytes, None) else {
            skip += 1;
            continue;
        };
        if loaded
            .graph
            .iter()
            .any(|t| UNSUPPORTED.contains(&t.predicate.as_str()))
        {
            skip += 1;
            continue;
        }
        let Some((exp_conforms, mut exp)) = expected_report(&loaded) else {
            skip += 1;
            continue;
        };

        let report = shifty_engine::validate_report(&loaded, &loaded.graph).unwrap();
        let mut got: Vec<Key> = report
            .results
            .iter()
            .map(|r| result_key(&loaded, r))
            .collect();
        exp.sort();
        got.sort();

        if report.conforms == exp_conforms && got == exp {
            pass += 1;
        } else {
            fail += 1;
            let name = file.strip_prefix(suite_dir()).unwrap_or(file);
            failures.push(format!("{}", name.display()));
        }
    }

    eprintln!(
        "\ndata-shapes core reports: {pass} pass, {fail} fail, {skip} skip (of {})",
        files.len()
    );
    for f in failures.iter().take(30) {
        eprintln!("  FAIL {f}");
    }
    assert_eq!(fail, 0, "report coverage has {fail} failing cases");
    assert!(pass >= 98, "report coverage regressed: only {pass} passed");
}
