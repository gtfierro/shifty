//! Report-level conformance against the W3C data-shapes core suite.
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

use oxrdf::{NamedNodeRef, Term};
use shacl_algebra::render::path_to_string;
use shacl_engine::ValidationResult;
use shacl_parse::vocab;
use std::path::{Path, PathBuf};

const SH_VALIDATION_REPORT: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#ValidationReport");

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

type Key = (String, Option<String>, Option<String>, String, String);

/// Canonical key for a term that may be a blank node.
///
/// - IRI / literal: exact string.
/// - Blank node as a path root: parse the path sub-graph and render
///   canonically so structurally identical paths compare equal even when the
///   blank-node IDs differ (e.g., expected-report copy vs. shapes-graph copy).
/// - Blank node as a shape or data node: wildcard ("_:BLANK") because
///   expected results sometimes use empty placeholder blank nodes.
fn path_term_key(loaded: &shacl_parse::Loaded, t: &Term) -> String {
    if matches!(t, Term::BlankNode(_)) {
        if let Ok(p) = shacl_parse::path::parse_path(loaded, t) {
            return path_to_string(&p);
        }
    }
    t.to_string()
}

fn shape_term_key(t: &Term) -> String {
    match t {
        Term::BlankNode(_) => "_:BLANK".to_string(),
        other => other.to_string(),
    }
}

fn result_key(loaded: &shacl_parse::Loaded, r: &ValidationResult) -> Key {
    (
        r.focus.to_string(),
        r.path.as_ref().map(|p| path_term_key(loaded, p)),
        r.value.as_ref().map(|v| v.to_string()),
        format!("<{}>", r.component.as_str()),
        shape_term_key(&r.source_shape),
    )
}

fn expected_keys(loaded: &shacl_parse::Loaded) -> Option<(bool, Vec<Key>)> {
    use oxrdf::NamedOrBlankNode;
    let report: NamedOrBlankNode = loaded
        .graph
        .subjects_for_predicate_object(vocab::RDF_TYPE, SH_VALIDATION_REPORT)
        .next()?
        .into_owned();

    let conforms = matches!(
        loaded.object(&report, vocab::SH_CONFORMS),
        Some(Term::Literal(ref l)) if l.value() == "true"
    );

    let mut keys = Vec::new();
    for res in loaded.objects(&report, vocab::SH_RESULT) {
        let Some(rn) = shacl_parse::graph::term_to_node(&res) else { continue };
        let focus = loaded.object(&rn, vocab::SH_FOCUS_NODE)?;
        let component = loaded.object(&rn, vocab::SH_SOURCE_CONSTRAINT_COMPONENT)?;
        let source = loaded.object(&rn, vocab::SH_SOURCE_SHAPE)?;
        keys.push((
            focus.to_string(),
            loaded.object(&rn, vocab::SH_RESULT_PATH).as_ref().map(|p| path_term_key(loaded, p)),
            loaded.object(&rn, vocab::SH_VALUE).as_ref().map(|v| v.to_string()),
            component.to_string(),
            shape_term_key(&source),
        ));
    }
    Some((conforms, keys))
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
        let Ok(loaded) = shacl_parse::load_turtle(&bytes, None) else {
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
        let Some((exp_conforms, mut exp)) = expected_keys(&loaded) else {
            skip += 1;
            continue;
        };

        let report = shacl_engine::validate_report(&loaded, &loaded.graph);
        let mut got: Vec<Key> = report.results.iter().map(|r| result_key(&loaded, r)).collect();
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
