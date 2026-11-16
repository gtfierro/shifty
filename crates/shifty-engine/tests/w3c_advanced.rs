//! Conformance harness against the vendored W3C SHACL-AF "advanced" suite.
//!
//! Unlike the core/sparql suites (flat `sh:conforms` files), the advanced suite
//! uses the DASH test vocabulary and mixes several test-case shapes in one tree:
//!
//!   * `dash:InferencingTestCase` — run forward-chaining rules (`infer`) and
//!     compare the complete derived delta, allowing fresh blank-node labels.
//!   * `sht:Validate` / `dash:GraphValidationTestCase` — run the RDF-driven
//!     report validator (`validate_report`) and match the result-set against the
//!     expected `sh:ValidationReport`, the same way `data_shapes_report` does.
//!   * `dash:FunctionTestCase` — evaluate the `dash:expression` (a
//!     `sh:SPARQLFunction` call) and compare against `dash:expectedResult`.
//!
//! `owl:imports` to vendored sibling ontologies are merged so a test's data
//! graph is complete. Fixture inventory and category counts are fixed; parsing,
//! execution and comparison failures never become skips. JS stays excluded.

mod common;

use common::{path_term_key, result_key};
use oxrdf::{Graph, NamedNode, NamedNodeRef, NamedOrBlankNode, Term, Triple};
use shifty_engine::{CompiledShapes, SessionData, SessionOptions};
use shifty_parse::vocab;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

const OWL_IMPORTS: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#imports");
const SH_SOURCE_CONSTRAINT: NamedNodeRef =
    NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#sourceConstraint");
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

/// JavaScript is outside the supported AF profile. Every other case must run.
const JS_PREDICATES: &[&str] = &[
    "http://www.w3.org/ns/shacl#js",
    "http://www.w3.org/ns/shacl#jsLibrary",
    "http://www.w3.org/ns/shacl#jsFunctionName",
];
/// Adding a JS fixture requires an explicit profile exclusion, never an
/// automatic skip based on an unexpected predicate or execution diagnostic.
const EXCLUDED_JS_CASES: &[&str] = &[];

fn suite_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata/test-suite/advanced")
        .canonicalize()
        .expect("advanced suite present")
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
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
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
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
        let bytes = std::fs::read(&file).unwrap();
        let base = format!("file://{}", file.display());
        let loaded = shifty_parse::load_turtle(&bytes, Some(&base))
            .unwrap_or_else(|error| panic!("{}: {error}", file.display()));
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
        if loaded
            .graph
            .triples_for_subject(NamedNode::new(&iri).unwrap().as_ref())
            .next()
            .is_some()
        {
            // Some fixtures inline the imported prefix-declaration resource.
            continue;
        }
        let Some(file) = index.get(&iri) else {
            // Metadata-only vocabulary imports used by the local fixtures.
            assert!(
                ["http://datashapes.org/dash", "http://datashapes.org/schema"]
                    .contains(&iri.as_str()),
                "unresolved AF fixture import: {iri}"
            );
            continue;
        };
        let base = format!("file://{}", file.display());
        let imported =
            shifty_parse::load_turtle(&std::fs::read(file).unwrap(), Some(&base)).unwrap();
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
            panic!("malformed AF expected result or function case: {case}");
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
            let ern = shifty_parse::graph::term_to_node(&er).expect("expected triple resource");
            let (Some(s), Some(p), Some(o)) = (
                loaded.object(&ern, RDF_SUBJECT),
                loaded.object(&ern, RDF_PREDICATE),
                loaded.object(&ern, RDF_OBJECT),
            ) else {
                panic!("malformed expected inference triple: {ern}");
            };
            let subj: NamedOrBlankNode = match shifty_parse::graph::term_to_node(&s) {
                Some(n) => n,
                None => panic!("invalid expected triple subject"),
            };
            let pred: NamedNode = match p {
                Term::NamedNode(n) => n,
                _ => panic!("invalid expected triple predicate"),
            };
            out.push(Triple::new(subj, pred, o));
        }
    }
    out
}

/// The expected inference graph describes the entire delta, not a subset of
/// the final graph. Already-asserted triples do not count as inferred output.
fn compare_inferred(
    asserted: &Graph,
    expected: &[Triple],
    actual: &[Triple],
) -> Result<(), String> {
    let expected: HashSet<_> = expected
        .iter()
        .filter(|triple| !asserted.contains(*triple))
        .cloned()
        .collect();
    let actual: HashSet<_> = actual.iter().cloned().collect();
    if expected == actual {
        return Ok(());
    }
    // Fresh template nodes may have different labels. Anchor existing input
    // nodes, then compare complete deltas by graph isomorphism. Reified expected
    // triples describe output placeholders rather than asserted node identity.
    let mut input_nodes = HashSet::new();
    for triple in asserted.iter() {
        if [RDF_SUBJECT, RDF_PREDICATE, RDF_OBJECT].contains(&triple.predicate) {
            continue;
        }
        if let oxrdf::NamedOrBlankNodeRef::BlankNode(node) = triple.subject {
            input_nodes.insert(node.into_owned());
        }
        if let oxrdf::TermRef::BlankNode(node) = triple.object {
            input_nodes.insert(node.into_owned());
        }
    }
    let canonical = |triples: &HashSet<Triple>| {
        let mut graph = Graph::new();
        for triple in triples {
            graph.insert(triple);
        }
        for node in &input_nodes {
            graph.insert(&Triple::new(
                node.clone(),
                NamedNode::new("urn:shifty:af-input-identity").unwrap(),
                oxrdf::Literal::new_simple_literal(node.as_str()),
            ));
        }
        graph.canonicalize(oxrdf::dataset::CanonicalizationAlgorithm::Unstable);
        graph
    };
    if canonical(&expected) == canonical(&actual) {
        return Ok(());
    }
    Err(format!(
        "inference mismatch: missing {:?}; unexpected {:?}",
        expected.difference(&actual).collect::<Vec<_>>(),
        actual.difference(&expected).collect::<Vec<_>>()
    ))
}

fn compare_report(
    loaded: &shifty_parse::Loaded,
    actual: &shifty_engine::ValidationReport,
) -> Result<(), String> {
    common::expected_report(loaded).ok_or("validation case has no well-formed expected report")?;
    // Some adapted fixtures carry both DASH and sht:Validate expectations.
    // Check every declared report, including severity, authored messages and
    // sourceConstraint whenever the fixture specifies them.
    let reports: Vec<_> = loaded
        .graph
        .subjects_for_predicate_object(vocab::RDF_TYPE, vocab::SH_VALIDATION_REPORT)
        .map(|node| node.into_owned())
        .collect();
    if reports.is_empty() {
        return Err("validation case has no expected report".into());
    }
    for report in reports {
        let expected_conforms = loaded
            .object(&report, vocab::SH_CONFORMS)
            .ok_or("expected report lacks sh:conforms")?;
        if expected_conforms != Term::Literal(oxrdf::Literal::from(actual.conforms)) {
            return Err("report conformance differs".into());
        }
        let mut remaining: Vec<_> = actual.results.iter().collect();
        for expected in loaded.objects(&report, vocab::SH_RESULT) {
            let expected = shifty_parse::graph::term_to_node(&expected)
                .ok_or("expected result is not a resource")?;
            let focus = loaded
                .object(&expected, vocab::SH_FOCUS_NODE)
                .ok_or("missing expected focus")?;
            let component = loaded
                .object(&expected, vocab::SH_SOURCE_CONSTRAINT_COMPONENT)
                .ok_or("missing expected component")?;
            let shape = loaded
                .object(&expected, vocab::SH_SOURCE_SHAPE)
                .ok_or("missing expected shape")?;
            let severity = loaded
                .object(&expected, vocab::SH_RESULT_SEVERITY)
                .ok_or("missing expected severity")?;
            let source = loaded.object(&expected, SH_SOURCE_CONSTRAINT);
            let messages = loaded.objects(&expected, vocab::SH_RESULT_MESSAGE);
            let path = loaded.object(&expected, vocab::SH_RESULT_PATH);
            let value = loaded.object(&expected, vocab::SH_VALUE);
            let Some(position) = remaining.iter().position(|result| {
                let key = result_key(loaded, result);
                key.0 == focus.to_string()
                    && key.3 == component.to_string()
                    && (matches!(shape, Term::BlankNode(_)) || result.source_shape == shape)
                    && key.1 == path.as_ref().map(|path| path_term_key(loaded, path))
                    && result.value == value
                    && Term::NamedNode(result.severity.clone()) == severity
                    && source
                        .as_ref()
                        .is_none_or(|source| result.source_constraint.as_ref() == Some(source))
                    && messages
                        .iter()
                        .all(|message| result.messages.contains(message))
            }) else {
                return Err(format!(
                    "missing or incorrect report result {expected}; actual: {:?}",
                    actual.results
                ));
            };
            remaining.remove(position);
        }
        if !remaining.is_empty() {
            return Err(format!("unexpected report results: {remaining:?}"));
        }
    }
    Ok(())
}

fn run_fixture(
    file: &Path,
    imports: &std::collections::HashMap<String, PathBuf>,
) -> Result<(usize, usize, usize), String> {
    let base = shifty_parse::graph::file_base_iri(file).map_err(|error| error.to_string())?;
    let loaded = shifty_parse::load_turtle(
        &std::fs::read(file).map_err(|error| error.to_string())?,
        Some(&base),
    )
    .map_err(|error| error.to_string())?;
    if uses_any(&loaded.graph, JS_PREDICATES) {
        return Err("SHACL-JS case requires an explicit inventory exclusion".into());
    }
    let loaded = resolve_imports(loaded, imports);
    let parsed = shifty_parse::parse_loaded(&loaded);
    if !parsed.diagnostics.is_empty() {
        return Err(format!(
            "unexpected lowering diagnostics: {:?}",
            parsed.diagnostics
        ));
    }
    let functions = function_tests(&loaded);
    if !functions.is_empty() {
        for (expression, expected) in &functions {
            let result = shifty_engine::evaluate_function_expression(&loaded, expression)?;
            if result.as_ref() != Some(expected) {
                return Err(format!(
                    "function {expression}: {result:?}, expected {expected}"
                ));
            }
        }
        return Ok((0, 0, functions.len()));
    }
    if is_inferencing(&loaded) {
        let expected = expected_inferred(&loaded);
        let asserted = loaded.graph.clone();
        let session = CompiledShapes::compile(loaded)
            .map_err(|error| error.to_string())?
            .session(
                SessionData::Embedded,
                SessionOptions {
                    inference: true,
                    ..Default::default()
                },
            )
            .map_err(|error| error.to_string())?;
        if !session.diagnostics().is_empty() {
            return Err(format!(
                "inference diagnostics: {:?}",
                session.diagnostics()
            ));
        }
        compare_inferred(&asserted, &expected, session.inferred())?;
        return Ok((1, 0, 0));
    }
    let report = shifty_engine::validate_report(&loaded, &loaded.graph)
        .map_err(|error| error.to_string())?;
    compare_report(&loaded, &report)?;
    Ok((0, 1, 0))
}

#[test]
fn w3c_advanced_conformance() {
    let mut files = Vec::new();
    collect(&suite_dir(), &mut files);
    files.sort();
    let names: Vec<_> = files
        .iter()
        .map(|file| {
            file.strip_prefix(suite_dir())
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    let inventory: Vec<_> = include_str!("../../../testdata/test-suite/advanced/cases.txt")
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    assert_eq!(
        names, inventory,
        "AF fixture inventory changed; review coverage before updating cases.txt"
    );
    let imports = ontology_index(&suite_dir());
    let mut counts = (0, 0, 0);
    let mut failures = Vec::new();
    let mut excluded = 0;
    for file in files {
        let name = file.strip_prefix(suite_dir()).unwrap().to_str().unwrap();
        if EXCLUDED_JS_CASES.contains(&name) {
            let loaded =
                shifty_parse::load_turtle(&std::fs::read(&file).unwrap(), Some("urn:af-excluded:"))
                    .unwrap();
            assert!(
                uses_any(&loaded.graph, JS_PREDICATES),
                "only SHACL-JS cases may be excluded: {name}"
            );
            excluded += 1;
            continue;
        }
        match run_fixture(&file, &imports) {
            Ok((inference, validation, functions)) => {
                counts.0 += inference;
                counts.1 += validation;
                counts.2 += functions;
            }
            Err(error) => failures.push(format!("{}: {error}", file.display())),
        }
    }
    assert!(
        failures.is_empty(),
        "AF regressions:\n{}",
        failures.join("\n")
    );
    assert_eq!(counts, (7, 97, 2), "AF category coverage changed");
    assert_eq!(
        excluded,
        EXCLUDED_JS_CASES.len(),
        "missing excluded JS fixture"
    );
    eprintln!(
        "AF/DASH: {} inference, {} validation, {} functions; zero skips",
        counts.0, counts.1, counts.2
    );
}

#[test]
fn inference_oracle_rejects_missing_and_extra_output() {
    let triple = Triple::new(
        NamedNode::new("urn:s").unwrap(),
        NamedNode::new("urn:p").unwrap(),
        NamedNode::new("urn:o").unwrap(),
    );
    assert!(compare_inferred(&Graph::new(), std::slice::from_ref(&triple), &[]).is_err());
    assert!(compare_inferred(&Graph::new(), &[], std::slice::from_ref(&triple)).is_err());
    let mut asserted = Graph::new();
    asserted.insert(&triple);
    assert!(compare_inferred(&asserted, &[triple], &[]).is_ok());
}

#[test]
fn project_af_regressions() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/regressions/af");
    let mut files = Vec::new();
    collect(&root, &mut files);
    files.sort();
    let names: Vec<_> = files
        .iter()
        .map(|file| file.file_name().unwrap().to_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "ask-and-optional.test.ttl",
            "expressions.test.ttl",
            "fresh-blank-node.test.ttl",
            "ordering.test.ttl",
            "path-inputs.test.ttl"
        ]
    );
    for file in files {
        run_fixture(&file, &std::collections::HashMap::new())
            .unwrap_or_else(|error| panic!("{}: {error}", file.display()));
    }
}

#[test]
fn report_oracle_checks_provenance_messages_severity_and_extra_results() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata/regressions/af/expressions.test.ttl");
    let loaded = shifty_parse::load_turtle(&std::fs::read(path).unwrap(), None).unwrap();
    let report = shifty_engine::validate_report(&loaded, &loaded.graph).unwrap();
    for defect in 0..4 {
        let mut broken = report.clone();
        match defect {
            0 => broken.results[0].source_constraint = None,
            1 => broken.results[0].messages.clear(),
            2 => {
                broken.results[0].severity =
                    NamedNode::new("http://www.w3.org/ns/shacl#Info").unwrap()
            }
            _ => broken.results.push(broken.results[0].clone()),
        }
        assert!(
            compare_report(&loaded, &broken).is_err(),
            "report defect {defect} escaped the oracle"
        );
    }
}

#[test]
fn inference_oracle_preserves_input_blank_identity() {
    let a = oxrdf::BlankNode::new("a").unwrap();
    let b = oxrdf::BlankNode::new("b").unwrap();
    let predicate = NamedNode::new("urn:p").unwrap();
    let mut asserted = Graph::new();
    for node in [&a, &b] {
        asserted.insert(&Triple::new(
            node.clone(),
            predicate.clone(),
            oxrdf::Literal::from(0),
        ));
    }
    let expected = Triple::new(a, predicate.clone(), oxrdf::Literal::from(1));
    let wrong = Triple::new(b, predicate, oxrdf::Literal::from(1));
    assert!(compare_inferred(&asserted, &[expected], &[wrong]).is_err());
}
