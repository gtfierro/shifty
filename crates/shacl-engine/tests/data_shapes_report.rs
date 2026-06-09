//! Report-level conformance against the W3C data-shapes core suite.
//!
//! For each test we compare our `sh:ValidationReport` to the expected one,
//! matching result-sets on (focusNode, resultPath, value, component,
//! sourceShape). Blank nodes are wildcarded (we don't do full graph
//! isomorphism yet) and severity/message are ignored. Tests using features we
//! don't produce yet are expected to FAIL here — this harness measures
//! report-level coverage as it grows.

use oxrdf::{NamedNodeRef, Term};
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

/// A comparison key: blank nodes wildcarded, IRIs/literals exact.
type Key = (String, Option<String>, Option<String>, String, String);

fn term_key(t: &Term) -> String {
    match t {
        Term::BlankNode(_) => "_:BLANK".to_string(),
        other => other.to_string(),
    }
}

fn result_key(r: &ValidationResult) -> Key {
    (
        term_key(&r.focus),
        r.path.as_ref().map(term_key),
        r.value.as_ref().map(term_key),
        format!("<{}>", r.component.as_str()),
        term_key(&r.source_shape),
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
            term_key(&focus),
            loaded.object(&rn, vocab::SH_RESULT_PATH).as_ref().map(term_key),
            loaded.object(&rn, vocab::SH_VALUE).as_ref().map(term_key),
            term_key(&component),
            term_key(&source),
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
        let mut got: Vec<Key> = report.results.iter().map(result_key).collect();
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
    // regression floor; raise as more components land (closed, pairs, qualified…)
    assert!(pass >= 73, "report coverage regressed: only {pass} passed");
}
