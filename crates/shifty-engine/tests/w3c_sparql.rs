//! Conformance harness against the vendored W3C SHACL-SPARQL test suite,
//! exercising BOTH validation paths:
//!   * the algebra evaluator (`shifty_engine::validate`), and
//!   * the RDF-driven W3C report validator (`shifty_engine::validate_report`).
//!
//! Each `*.ttl` carries shapes + data in one graph plus an expected
//! `sh:conforms`; we require both paths to match it. Tests for features we don't
//! implement yet (custom components, JS, rules) are skipped via their
//! predicates. Pre-binding (`$this`, `$PATH`, `$shapesGraph`, `$currentShape`)
//! is implemented by algebra substitution, so all pre-binding cases run.

use oxrdf::{Graph, NamedNodeRef, Term};
use std::path::{Path, PathBuf};

const SH_CONFORMS: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#conforms");

/// Predicates whose presence means the test exercises a feature we don't
/// support yet. SHACL-SPARQL constraints/targets/prefixes ARE supported, so
/// they are deliberately absent here.
const UNSUPPORTED_PREDICATES: &[&str] = &[
    "http://www.w3.org/ns/shacl#rule",
    "http://www.w3.org/ns/shacl#parameter",
    "http://www.w3.org/ns/shacl#validator",
    "http://www.w3.org/ns/shacl#nodeValidator",
    "http://www.w3.org/ns/shacl#propertyValidator",
    "http://www.w3.org/ns/shacl#js",
    "http://www.w3.org/ns/shacl#jsLibrary",
    "http://www.w3.org/ns/shacl#jsFunctionName",
    "http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint",
];

fn suite_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata/test-suite/sparql")
        .canonicalize()
        .expect("sparql suite present")
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
fn w3c_sparql_conformance() {
    let mut files = Vec::new();
    collect_tests(&suite_dir(), &mut files);
    files.sort();
    assert!(!files.is_empty(), "no test files found");

    let (mut algebra_pass, mut report_pass, mut skipped) = (0u32, 0u32, 0u32);
    let mut failures: Vec<String> = Vec::new();

    for file in &files {
        let name = file
            .strip_prefix(suite_dir())
            .unwrap_or(file)
            .display()
            .to_string();
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

        // Algebra conformance path.
        match shifty_engine::validate(&loaded.graph, &parsed.schema) {
            Ok(o) if o.conforms == expected => algebra_pass += 1,
            Ok(o) => failures.push(format!(
                "{name}: algebra conforms={} expected={expected}",
                o.conforms
            )),
            Err(_) => failures.push(format!("{name}: algebra path returned non-stratifiable")),
        }

        // RDF-driven W3C report path.
        let report = shifty_engine::validate_report(&loaded, &loaded.graph);
        if report.conforms == expected {
            report_pass += 1;
        } else {
            failures.push(format!(
                "{name}: report conforms={} expected={expected}",
                report.conforms
            ));
        }
    }

    eprintln!(
        "\nW3C sparql conformance: algebra {algebra_pass} pass, report {report_pass} pass, \
         {skipped} skipped (of {} files)",
        files.len()
    );
    for f in &failures {
        eprintln!("  FAIL {f}");
    }

    assert!(
        failures.is_empty(),
        "supported sparql tests regressed: {failures:#?}"
    );
    assert!(
        algebra_pass >= 12 && report_pass >= 12,
        "coverage dropped: algebra={algebra_pass}, report={report_pass}"
    );
}
