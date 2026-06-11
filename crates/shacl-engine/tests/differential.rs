//! Differential-testing utilities and the 223P/NIST baseline fixture.
//!
//! ## Role of this file
//!
//! **Stage 1 (this file):** Establishes:
//! 1. `assert_same_outcome` — canonical comparison of two `ValidationOutcome`
//!    values (normalised, order-independent). Stages 2–3 use this to prove the
//!    frozen-dataset and native-executor paths agree with the Spareval oracle.
//! 2. The 223P/NIST baseline test — asserts the current validation result for
//!    the NIST building model against the 223P shapes does not regress as later
//!    stages land.
//!
//! **Stage 2** will add: `assert_same_sparql_solutions` comparing Spareval over
//! `Store` vs. Spareval over `FrozenIndexedDataset`.
//!
//! **Stage 3** will add: native-executor vs. Spareval differential assertions
//! across the `w3c_sparql` suite and the 223P/NIST workload.

use shacl_engine::profile::{self, ExecutorKind};
use shacl_engine::{ValidationGraphMode, infer_graphs, validate, validate_plan_graphs_with_mode};
use std::path::Path;

// ── helpers ─────────────────────────────────────────────────────────────────

/// Compare two `ValidationOutcome` values in a canonical, order-independent
/// way. Panics with a diff if they disagree.
pub fn assert_same_outcome(
    label: &str,
    left: &shacl_engine::ValidationOutcome,
    right: &shacl_engine::ValidationOutcome,
) {
    assert_eq!(
        left.conforms, right.conforms,
        "{label}: conforms mismatch (left={}, right={})",
        left.conforms, right.conforms,
    );

    let mut left_foci: Vec<String> = left
        .violations
        .iter()
        .map(|v| v.focus.to_string())
        .collect();
    let mut right_foci: Vec<String> = right
        .violations
        .iter()
        .map(|v| v.focus.to_string())
        .collect();
    left_foci.sort();
    right_foci.sort();

    assert_eq!(
        left_foci, right_foci,
        "{label}: violation focus sets differ\n  left:  {left_foci:?}\n  right: {right_foci:?}",
    );

    // For each focus node, compare the multiset of (path, message) reason pairs.
    for focus in &left_foci {
        let left_reasons = reason_set(left, focus);
        let right_reasons = reason_set(right, focus);
        assert_eq!(
            left_reasons, right_reasons,
            "{label}: reasons differ for focus {focus}\n  left:  {left_reasons:?}\n  right: {right_reasons:?}",
        );
    }
}

fn reason_set(outcome: &shacl_engine::ValidationOutcome, focus: &str) -> Vec<String> {
    let mut reasons: Vec<String> = outcome
        .violations
        .iter()
        .filter(|v| v.focus.to_string() == focus)
        .flat_map(|v| {
            v.reasons.iter().map(|r| match &r.path {
                Some(p) => format!("({p}) {}", r.message),
                None => r.message.clone(),
            })
        })
        .collect();
    reasons.sort();
    reasons.dedup();
    reasons
}

// ── 223P/NIST baseline ───────────────────────────────────────────────────────

/// Load a Turtle file relative to the workspace root (two levels up from the
/// shacl-engine crate manifest directory).
fn load_ws(rel: &str) -> shacl_parse::Loaded {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../")
        .join(rel);
    let bytes =
        std::fs::read(&path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    shacl_parse::load_turtle(&bytes, None)
        .unwrap_or_else(|e| panic!("failed to parse {}: {e}", path.display()))
}

/// Baseline: the NIST building-1 model must conform against the 223P shapes.
/// This must remain true as stages 2–3 replace the storage and executor.
#[test]
fn nist_bdg1_conforms_against_223p() {
    let shapes = load_ws("benchmark/s223/223p.ttl");
    let data = load_ws("nist-bdg1-1.ttl");
    let parsed = shacl_parse::parse_loaded(&shapes);
    let normalized = shacl_opt::normalize(&parsed.schema);
    let physical = shacl_opt::plan(&normalized);

    // Run inference first (SHACL-AF rules populate the graph before validation).
    let inference = infer_graphs(&data.graph, &shapes.graph, &normalized)
        .expect("223P schema must be stratifiable");

    let outcome = validate_plan_graphs_with_mode(
        &inference.graph,
        &shapes.graph,
        &physical,
        ValidationGraphMode::Union,
    )
    .expect("validated schema must be stratifiable");

    assert!(
        outcome.conforms,
        "NIST bdg1-1 must conform against 223P shapes; {} violation(s): {:?}",
        outcome.violations.len(),
        outcome
            .violations
            .iter()
            .map(|v| v.focus.to_string())
            .collect::<Vec<_>>(),
    );
}

/// Smoke test: the reference evaluator and the planned executor agree on
/// the 223P/NIST outcome. Both use the same Spareval backend in stage 1.
#[test]
fn reference_and_plan_agree_on_nist_bdg1() {
    let shapes = load_ws("benchmark/s223/223p.ttl");
    let data = load_ws("nist-bdg1-1.ttl");
    let parsed = shacl_parse::parse_loaded(&shapes);
    let normalized = shacl_opt::normalize(&parsed.schema);
    let physical = shacl_opt::plan(&normalized);

    let inference = infer_graphs(&data.graph, &shapes.graph, &normalized).expect("stratifiable");

    let ref_outcome = shacl_engine::validate_graphs_with_mode(
        &inference.graph,
        &shapes.graph,
        &parsed.schema,
        ValidationGraphMode::Union,
    )
    .expect("stratifiable");

    let plan_outcome = validate_plan_graphs_with_mode(
        &inference.graph,
        &shapes.graph,
        &physical,
        ValidationGraphMode::Union,
    )
    .expect("stratifiable");

    assert_same_outcome("223P/NIST bdg1-1", &ref_outcome, &plan_outcome);
}

// ── Stage 3: native executor ─────────────────────────────────────────────────

/// A pure-BGP `sh:sparql` constraint must (a) actually take the native execution
/// path and (b) produce the same result the Spareval fallback would. The
/// debug-build differential gate inside `constraint_violations` enforces (b) on
/// every native run; this test additionally proves (a) via the profiler, so the
/// native path can't silently rot into all-fallback.
#[test]
fn native_executor_fires_on_bgp_constraint() {
    let ttl = r#"
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix ex:  <http://ex/> .
        ex:S a sh:NodeShape ;
            sh:targetNode ex:a, ex:d ;
            sh:sparql [
                a sh:SPARQLConstraint ;
                sh:select "SELECT $this ?value WHERE { $this <http://ex/p> ?value . ?value <http://ex/flag> <http://ex/bad> }"
            ] .
        ex:a ex:p ex:b, ex:c .
        ex:c ex:flag ex:bad .
        ex:d ex:p ex:e .
    "#;
    let loaded = shacl_parse::load_turtle(ttl.as_bytes(), None).expect("valid Turtle");
    let parsed = shacl_parse::parse_turtle(ttl.as_bytes(), None).expect("valid shapes");
    assert!(
        parsed.diagnostics.is_empty(),
        "diags: {:?}",
        parsed.diagnostics
    );

    profile::enable();
    let outcome = validate(&loaded.graph, &parsed.schema).expect("stratifiable");
    let profile = profile::take().expect("profiling was enabled");

    // ex:a reaches a flagged value (ex:c) ⇒ violation with ?value = ex:c.
    // ex:d reaches only ex:e (unflagged) ⇒ conforms.
    assert!(!outcome.conforms);
    assert_eq!(outcome.violations.len(), 1);
    assert_eq!(outcome.violations[0].focus.to_string(), "<http://ex/a>");
    assert_eq!(
        outcome.violations[0].reasons[0].value.to_string(),
        "<http://ex/c>",
    );

    // The constraint lowered to native and ran there (not on the fallback).
    assert!(
        profile
            .records()
            .iter()
            .any(|r| r.executor == ExecutorKind::Native),
        "expected a native execution record; got: {:?}",
        profile.records(),
    );
}
