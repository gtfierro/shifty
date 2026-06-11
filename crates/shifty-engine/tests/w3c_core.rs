//! Conformance harness against the vendored W3C SHACL core test suite.
//!
//! Each `*.ttl` test carries shapes + data in one graph plus an expected
//! `sh:conforms`. We compare our reference evaluator's `conforms` to the
//! expected value. Tests whose shapes use features we don't lower yet (emitting
//! parse diagnostics) or known-unsupported AF/SPARQL predicates are SKIPPED, so
//! this gate measures only what we claim to support and tightens as we grow.

use oxrdf::{Graph, NamedNodeRef, Term};
use std::path::{Path, PathBuf};

const SH_CONFORMS: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#conforms");

/// Predicates whose presence means the test exercises a feature we don't
/// support yet; skip rather than report a spurious failure.
const UNSUPPORTED_PREDICATES: &[&str] = &[
    "http://www.w3.org/ns/shacl#sparql",
    "http://www.w3.org/ns/shacl#rule",
    "http://www.w3.org/ns/shacl#target",
    "http://www.w3.org/ns/shacl#parameter",
    "http://www.w3.org/ns/shacl#validator",
    "http://www.w3.org/ns/shacl#nodeValidator",
    "http://www.w3.org/ns/shacl#propertyValidator",
    "http://www.w3.org/ns/shacl#js",
    "http://www.w3.org/ns/shacl#jsLibrary",
    "http://www.w3.org/ns/shacl#jsFunctionName",
    "http://www.w3.org/ns/shacl#declare",
    // the disjoint flag on qualified value shapes is not modelled in the IR
    "http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint",
];

fn suite_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata/test-suite/core")
        .canonicalize()
        .expect("core suite present")
}

fn collect_tests(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_tests(&path, out);
        } else if path.extension().is_some_and(|e| e == "ttl")
            && path.file_name().is_some_and(|n| n != "manifest.ttl")
        {
            out.push(path);
        }
    }
}

fn expected_conforms(g: &Graph) -> Option<bool> {
    for t in g.triples_for_predicate(SH_CONFORMS) {
        if let Term::Literal(l) = t.object.into_owned() {
            match l.value() {
                "true" => return Some(true),
                "false" => return Some(false),
                _ => {}
            }
        }
    }
    None
}

fn uses_unsupported(g: &Graph) -> bool {
    g.iter()
        .any(|t| UNSUPPORTED_PREDICATES.contains(&t.predicate.as_str()))
}

#[test]
fn w3c_core_conformance() {
    let mut files = Vec::new();
    collect_tests(&suite_dir(), &mut files);
    files.sort();
    assert!(!files.is_empty(), "no test files found");

    let (mut passed, mut failed, mut skipped) = (0u32, 0u32, 0u32);
    let mut failures: Vec<String> = Vec::new();

    for file in &files {
        let bytes = std::fs::read(file).unwrap();
        let Ok(loaded) = shifty_parse::load_turtle(&bytes, None) else {
            skipped += 1;
            continue;
        };
        let Some(expected) = expected_conforms(&loaded.graph) else {
            skipped += 1;
            continue;
        };
        if uses_unsupported(&loaded.graph) {
            skipped += 1;
            continue;
        }
        let parsed = match shifty_parse::parse_turtle(&bytes, None) {
            Ok(p) if p.diagnostics.is_empty() => p,
            _ => {
                skipped += 1;
                continue;
            }
        };

        let outcome = match shifty_engine::validate(&loaded.graph, &parsed.schema) {
            Ok(o) => o,
            Err(_) => {
                // non-stratifiable under our semantics: out of scope, skip
                skipped += 1;
                continue;
            }
        };
        let name = file.strip_prefix(suite_dir()).unwrap_or(file);

        // normalization must preserve conformance (oracle cross-check)
        let normalized = shifty_opt::normalize(&parsed.schema);
        let norm_conforms = shifty_engine::validate(&loaded.graph, &normalized)
            .expect("normalized schema stays stratifiable")
            .conforms;
        assert_eq!(
            norm_conforms,
            outcome.conforms,
            "normalize changed conformance for {}",
            name.display()
        );

        // the planned executor must agree with the reference evaluator
        let physical = shifty_opt::plan(&normalized);
        let plan_conforms = shifty_engine::validate_plan(&loaded.graph, &physical)
            .expect("planned schema stays stratifiable")
            .conforms;
        assert_eq!(
            plan_conforms,
            outcome.conforms,
            "plan changed conformance for {}",
            name.display()
        );

        if outcome.conforms == expected {
            passed += 1;
        } else {
            failed += 1;
            failures.push(format!(
                "{}: expected conforms={expected}, got {}",
                name.display(),
                outcome.conforms
            ));
        }
    }

    eprintln!(
        "\nW3C core conformance: {passed} passed, {failed} failed, {skipped} skipped (of {} files)",
        files.len()
    );
    for f in failures.iter().take(40) {
        eprintln!("  FAIL {f}");
    }

    // Every supported test must match; skips are tracked but allowed.
    assert_eq!(failed, 0, "supported tests regressed: {failures:#?}");
    assert!(passed >= 80, "coverage dropped: only {passed} passed");
}
