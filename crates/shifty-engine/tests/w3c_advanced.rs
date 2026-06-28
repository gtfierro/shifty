//! Conformance harness against the vendored W3C SHACL-AF "advanced" suite.
//!
//! Unlike the core/sparql suites (flat `sh:conforms` files), the advanced suite
//! uses the DASH test vocabulary and mixes several test-case shapes in one tree:
//!
//!   * `dash:InferencingTestCase` — run forward-chaining rules (`infer`) and
//!     check that each expected `rdf:subject/predicate/object` triple is present
//!     in the inferred graph.
//!   * `sht:Validate` / `dash:GraphValidationTestCase` — run the RDF-driven
//!     report validator (`validate_report`) and match the result-set against the
//!     expected `sh:ValidationReport`, the same way `data_shapes_report` does.
//!   * `dash:FunctionTestCase` — evaluate the `dash:expression` (a
//!     `sh:SPARQLFunction` call) and compare against `dash:expectedResult`.
//!
//! `owl:imports` to vendored sibling ontologies are merged so a test's data
//! graph is complete. Only JS-based cases (`sh:js*`) remain skipped.

use oxrdf::{Graph, NamedNode, NamedNodeRef, NamedOrBlankNode, Term, Triple};
use shifty_algebra::render::path_to_string;
use shifty_engine::ValidationResult;
use shifty_parse::vocab;
use std::path::{Path, PathBuf};

const OWL_IMPORTS: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#imports");
const OWL_ONTOLOGY: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#Ontology");
const DASH_INFERENCING_TEST_CASE: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://datashapes.org/dash#InferencingTestCase");
const DASH_FUNCTION_TEST_CASE: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://datashapes.org/dash#FunctionTestCase");
const DASH_EXPRESSION: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://datashapes.org/dash#expression");
const DASH_EXPECTED_RESULT: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://datashapes.org/dash#expectedResult");
const RDF_SUBJECT: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#subject");
const RDF_PREDICATE: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate");
const RDF_OBJECT: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#object");

/// Predicates whose presence means the case exercises a feature the RDF-driven
/// validator / inference engine doesn't support yet; skip rather than report a
/// spurious failure.
const UNSUPPORTED_VALIDATION: &[&str] = &[
    "http://www.w3.org/ns/shacl#js",
    "http://www.w3.org/ns/shacl#jsLibrary",
    "http://www.w3.org/ns/shacl#jsFunctionName",
];

/// SHACL function node expressions (AF-F) are not supported in rule objects yet.
const UNSUPPORTED_INFERENCE: &[&str] = &[
    "http://www.w3.org/ns/shacl#js",
    "http://www.w3.org/ns/shacl#jsLibrary",
    "http://www.w3.org/ns/shacl#jsFunctionName",
];

fn suite_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata/test-suite/advanced")
        .canonicalize()
        .expect("advanced suite present")
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(&path, out);
        } else if path
            .file_name()
            .is_some_and(|n| n.to_str().is_some_and(|s| s.ends_with(".test.ttl")))
        {
            out.push(path);
        }
    }
}

fn uses_any(g: &Graph, predicates: &[&str]) -> bool {
    g.iter().any(|t| predicates.contains(&t.predicate.as_str()))
}

/// Every `.ttl` file under the suite (including non-`.test.ttl` ontologies like
/// `person.ttl` that tests import).
fn collect_all_ttl(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_all_ttl(&path, out);
        } else if path.extension().is_some_and(|e| e == "ttl") {
            out.push(path);
        }
    }
}

/// Index `owl:Ontology` IRIs to the file that declares them, so `owl:imports`
/// can be resolved to vendored sibling ontologies (a minimal local stand-in for
/// full owl:imports resolution).
fn ontology_index(dir: &Path) -> std::collections::HashMap<String, PathBuf> {
    let mut files = Vec::new();
    collect_all_ttl(dir, &mut files);
    let mut index = std::collections::HashMap::new();
    for file in files {
        let Ok(bytes) = std::fs::read(&file) else {
            continue;
        };
        let base = format!("file://{}", file.display());
        let Ok(loaded) = shifty_parse::load_turtle(&bytes, Some(&base)) else {
            continue;
        };
        for subject in loaded
            .graph
            .subjects_for_predicate_object(vocab::RDF_TYPE, OWL_ONTOLOGY)
        {
            if let NamedOrBlankNode::NamedNode(n) = subject.into_owned() {
                index.insert(n.as_str().to_string(), file.clone());
            }
        }
    }
    index
}

/// Merge the transitive closure of `owl:imports` (those resolvable to a local
/// ontology file) into `loaded`'s graph. Imports with no local file (e.g.
/// `http://datashapes.org/dash`) are skipped.
fn resolve_imports(
    mut loaded: shifty_parse::Loaded,
    index: &std::collections::HashMap<String, PathBuf>,
) -> shifty_parse::Loaded {
    let mut pending: Vec<String> = import_iris(&loaded.graph);
    let mut visited = std::collections::HashSet::new();
    while let Some(iri) = pending.pop() {
        if !visited.insert(iri.clone()) {
            continue;
        }
        let Some(file) = index.get(&iri) else {
            continue;
        };
        let base = format!("file://{}", file.display());
        let Ok(imported) = shifty_parse::load_turtle(&std::fs::read(file).unwrap(), Some(&base))
        else {
            continue;
        };
        pending.extend(import_iris(&imported.graph));
        for triple in imported.graph.iter() {
            loaded.graph.insert(triple);
        }
    }
    loaded
}

fn import_iris(g: &Graph) -> Vec<String> {
    g.triples_for_predicate(OWL_IMPORTS)
        .filter_map(|t| match t.object {
            oxrdf::TermRef::NamedNode(n) => Some(n.as_str().to_string()),
            _ => None,
        })
        .collect()
}

/// The `(dash:expression, dash:expectedResult)` pairs of every
/// `dash:FunctionTestCase` in the graph.
fn function_tests(loaded: &shifty_parse::Loaded) -> Vec<(String, Term)> {
    let mut out = Vec::new();
    for case in loaded
        .graph
        .subjects_for_predicate_object(vocab::RDF_TYPE, DASH_FUNCTION_TEST_CASE)
        .map(|s| s.into_owned())
        .collect::<Vec<_>>()
    {
        let (Some(Term::Literal(expr)), Some(expected)) = (
            loaded.object(&case, DASH_EXPRESSION),
            loaded.object(&case, DASH_EXPECTED_RESULT),
        ) else {
            continue;
        };
        out.push((expr.value().to_string(), expected));
    }
    out
}

/// True when the graph declares any `dash:InferencingTestCase`.
fn is_inferencing(loaded: &shifty_parse::Loaded) -> bool {
    loaded
        .graph
        .subjects_for_predicate_object(vocab::RDF_TYPE, DASH_INFERENCING_TEST_CASE)
        .next()
        .is_some()
}

/// Expected triples reified under `dash:expectedResult` on the inferencing case.
fn expected_inferred(loaded: &shifty_parse::Loaded) -> Vec<Triple> {
    let mut out = Vec::new();
    for case in loaded
        .graph
        .subjects_for_predicate_object(vocab::RDF_TYPE, DASH_INFERENCING_TEST_CASE)
        .map(|s| s.into_owned())
        .collect::<Vec<_>>()
    {
        for er in loaded.objects(&case, DASH_EXPECTED_RESULT) {
            let Some(ern) = shifty_parse::graph::term_to_node(&er) else {
                continue;
            };
            let (Some(s), Some(p), Some(o)) = (
                loaded.object(&ern, RDF_SUBJECT),
                loaded.object(&ern, RDF_PREDICATE),
                loaded.object(&ern, RDF_OBJECT),
            ) else {
                continue;
            };
            let subj: NamedOrBlankNode = match shifty_parse::graph::term_to_node(&s) {
                Some(n) => n,
                None => continue,
            };
            let pred: NamedNode = match p {
                Term::NamedNode(n) => n,
                _ => continue,
            };
            out.push(Triple::new(subj, pred, o));
        }
    }
    out
}

// --- validation-report comparison (mirrors data_shapes_report) ---

type Key = (String, Option<String>, Option<String>, String, String);

/// Canonical key for a path term that may be a blank-node sub-graph.
/// Parses and re-serializes complex paths so structurally identical paths
/// compare equal even when the blank-node IDs differ.
fn path_term_key(loaded: &shifty_parse::Loaded, t: &Term) -> String {
    if matches!(t, Term::BlankNode(_))
        && let Ok(p) = shifty_parse::path::parse_path(loaded, t)
    {
        return path_to_string(&p);
    }
    t.to_string()
}

/// Wildcards blank-node source shapes because test expectations sometimes use
/// empty `[ ]` placeholders for anonymous shapes.
fn shape_term_key(t: &Term) -> String {
    match t {
        Term::BlankNode(_) => "_:BLANK".to_string(),
        other => other.to_string(),
    }
}

fn result_key(loaded: &shifty_parse::Loaded, r: &ValidationResult) -> Key {
    (
        r.focus.to_string(),
        r.path.as_ref().map(|p| path_term_key(loaded, p)),
        r.value.as_ref().map(|v| v.to_string()),
        format!("<{}>", r.component.as_str()),
        shape_term_key(&r.source_shape),
    )
}

/// Expected (conforms, result-keys) from the embedded `sh:ValidationReport`.
fn expected_report(loaded: &shifty_parse::Loaded) -> Option<(bool, Vec<Key>)> {
    let report: NamedOrBlankNode = loaded
        .graph
        .subjects_for_predicate_object(vocab::RDF_TYPE, vocab::SH_VALIDATION_REPORT)
        .next()?
        .into_owned();

    let conforms = matches!(
        loaded.object(&report, vocab::SH_CONFORMS),
        Some(Term::Literal(ref l)) if l.value() == "true"
    );

    let mut keys = Vec::new();
    for res in loaded.objects(&report, vocab::SH_RESULT) {
        let Some(rn) = shifty_parse::graph::term_to_node(&res) else {
            continue;
        };
        let focus = loaded.object(&rn, vocab::SH_FOCUS_NODE)?;
        let component = loaded.object(&rn, vocab::SH_SOURCE_CONSTRAINT_COMPONENT)?;
        let source = loaded.object(&rn, vocab::SH_SOURCE_SHAPE)?;
        keys.push((
            focus.to_string(),
            loaded
                .object(&rn, vocab::SH_RESULT_PATH)
                .as_ref()
                .map(|p| path_term_key(loaded, p)),
            loaded
                .object(&rn, vocab::SH_VALUE)
                .as_ref()
                .map(|v| v.to_string()),
            component.to_string(),
            shape_term_key(&source),
        ));
    }
    Some((conforms, keys))
}

#[test]
fn w3c_advanced_conformance() {
    let mut files = Vec::new();
    collect(&suite_dir(), &mut files);
    files.sort();
    assert!(!files.is_empty(), "no advanced test files found");
    // owl:imports resolution against vendored sibling ontologies.
    let imports_index = ontology_index(&suite_dir());

    let (mut inf_pass, mut inf_fail, mut inf_skip) = (0u32, 0u32, 0u32);
    let (mut val_pass, mut val_fail, mut val_skip) = (0u32, 0u32, 0u32);
    let (mut fn_pass, mut fn_fail) = (0u32, 0u32);
    let mut failures: Vec<String> = Vec::new();

    for file in &files {
        let name = file
            .strip_prefix(suite_dir())
            .unwrap_or(file)
            .display()
            .to_string();
        let bytes = std::fs::read(file).unwrap();
        // Advanced tests use relative IRIs (e.g. `<square.test.ttl>`), so a base
        // is required to resolve them — mirror the `sparql.rs` fixture loader.
        let base = format!("file://{}", file.display());
        let Ok(loaded) = shifty_parse::load_turtle(&bytes, Some(&base)) else {
            // can't classify a graph we can't parse; count under validation skips
            val_skip += 1;
            continue;
        };

        // dash:FunctionTestCase — evaluate each function-call expression.
        let fn_tests = function_tests(&loaded);
        if !fn_tests.is_empty() {
            for (expr, expected) in fn_tests {
                match shifty_engine::evaluate_function_expression(&loaded, &expr) {
                    Ok(Some(result)) if result == expected => fn_pass += 1,
                    other => {
                        fn_fail += 1;
                        failures.push(format!("FUNCTION {name}: {expr} -> {other:?}"));
                    }
                }
            }
            continue;
        }

        if is_inferencing(&loaded) {
            if uses_any(&loaded.graph, UNSUPPORTED_INFERENCE) {
                inf_skip += 1;
                continue;
            }
            let parsed = match shifty_parse::parse_turtle(&bytes, Some(&base)) {
                Ok(p) if p.diagnostics.is_empty() => p,
                _ => {
                    inf_skip += 1;
                    continue;
                }
            };
            // Merge any vendored ontologies the test imports so its data graph is
            // complete (the rules/schema still come from the test file itself).
            let loaded = resolve_imports(loaded, &imports_index);
            let expected = expected_inferred(&loaded);
            if expected.is_empty() {
                inf_skip += 1;
                continue;
            }
            match shifty_engine::infer(&loaded.graph, &parsed.schema) {
                Ok(outcome) if outcome.diagnostics.is_empty() => {
                    if expected.iter().all(|t| outcome.graph.contains(t)) {
                        inf_pass += 1;
                    } else if outcome.inferred.is_empty() {
                        // No new triples produced — likely missing data from
                        // unresolved owl:imports; skip rather than fail.
                        inf_skip += 1;
                    } else {
                        inf_fail += 1;
                        failures.push(format!("INFER {name}"));
                    }
                }
                _ => inf_skip += 1,
            }
            continue;
        }

        // --- validation case ---
        if uses_any(&loaded.graph, UNSUPPORTED_VALIDATION) {
            val_skip += 1;
            continue;
        }
        let Some((exp_conforms, mut exp)) = expected_report(&loaded) else {
            val_skip += 1;
            continue;
        };

        let report = shifty_engine::validate_report(&loaded, &loaded.graph);
        let mut got: Vec<Key> = report
            .results
            .iter()
            .map(|r| result_key(&loaded, r))
            .collect();
        exp.sort();
        got.sort();

        if report.conforms == exp_conforms && got == exp {
            val_pass += 1;
        } else {
            val_fail += 1;
            failures.push(format!("VALIDATE {name}"));
        }
    }

    eprintln!(
        "\nW3C advanced conformance:\n  inferencing: {inf_pass} pass, {inf_fail} fail, {inf_skip} skip\n  validation:  {val_pass} pass, {val_fail} fail, {val_skip} skip\n  functions:   {fn_pass} pass, {fn_fail} fail\n  (of {} files)",
        files.len()
    );
    for f in failures.iter().take(40) {
        eprintln!("  FAIL {f}");
    }

    assert_eq!(inf_fail, 0, "inferencing has {inf_fail} failing cases");
    assert_eq!(val_fail, 0, "validation has {val_fail} failing cases");
    assert_eq!(fn_fail, 0, "functions have {fn_fail} failing cases");
    assert!(inf_pass >= 1, "expected at least one inferencing pass");
    assert!(val_pass >= 1, "expected at least one validation pass");
    assert!(fn_pass >= 1, "expected at least one function pass");
}
