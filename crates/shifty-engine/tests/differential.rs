//! Differential-testing utilities and the 223P/NIST baseline fixture.
//!
//! ## Role of this file
//!
//! **Stage 1 (this file):** Establishes:
//! 1. `assert_same_verdict` (conformance + violating focus set) and the stricter
//!    `assert_same_outcome` (also reason text), order-independent. The verdict
//!    form is for comparisons across `normalize` (which rewrites shapes); the
//!    strict form is for stages 2–3 proving the frozen-dataset and
//!    native-executor paths agree with the Spareval oracle over identical shapes.
//! 2. The 223P/NIST baseline test — asserts the current validation result for
//!    the NIST building model against the 223P shapes does not regress as later
//!    stages land.
//!
//! **Stage 2** will add: `assert_same_sparql_solutions` comparing Spareval over
//! `Store` vs. Spareval over `FrozenIndexedDataset`.
//!
//! **Stage 3** will add: native-executor vs. Spareval differential assertions
//! across the `w3c_sparql` suite and the 223P/NIST workload.

use shifty_engine::profile::{self, ExecutorKind};
use shifty_engine::{ValidationGraphMode, infer_graphs, validate, validate_plan_graphs_with_mode};
use std::path::Path;

// ── helpers ─────────────────────────────────────────────────────────────────

/// Assert two outcomes reach the same *verdict*: identical `conforms` flag and
/// identical sets of violating focus nodes. This is the semantic invariant that
/// `normalize`/`plan` must preserve.
///
/// It deliberately does NOT compare reason text. The algebra path's reasons are
/// intentionally coarse (the RDF-driven report validator is the W3C-faithful
/// reasons path — see `docs/BACKLOG.md`), and normalization legitimately changes
/// their granularity: e.g. NNF-rewriting a `∀path.sh:class` qualifier
/// (`∃≤0 π . ¬(∃≥1 subClassOf.{=C})`) drops the `Not(inner)` form that
/// `explain_count` drills into, so the same violation is reported at the outer
/// count level instead of the inner `sh:class` level. The conformance verdict is
/// unchanged either way.
pub fn assert_same_verdict(
    label: &str,
    left: &shifty_engine::ValidationOutcome,
    right: &shifty_engine::ValidationOutcome,
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
    left_foci.dedup();
    right_foci.sort();
    right_foci.dedup();

    assert_eq!(
        left_foci, right_foci,
        "{label}: violation focus sets differ\n  left:  {left_foci:?}\n  right: {right_foci:?}",
    );
}

/// Like [`assert_same_verdict`] but additionally requires identical per-focus
/// `(path, message)` reason sets. Use this to compare two evaluators over the
/// *same* shape representation (e.g. frozen-dataset vs Store, native vs
/// Spareval — stages 2–3), where reason text must match exactly. Not suitable
/// for comparing across `normalize`, which rewrites shapes (see
/// [`assert_same_verdict`]).
#[allow(dead_code)] // reserved for the stage 2/3 executor-backend differentials
pub fn assert_same_outcome(
    label: &str,
    left: &shifty_engine::ValidationOutcome,
    right: &shifty_engine::ValidationOutcome,
) {
    assert_same_verdict(label, left, right);

    let mut foci: Vec<String> = left
        .violations
        .iter()
        .map(|v| v.focus.to_string())
        .collect();
    foci.sort();
    foci.dedup();
    for focus in &foci {
        let left_reasons = reason_set(left, focus);
        let right_reasons = reason_set(right, focus);
        assert_eq!(
            left_reasons, right_reasons,
            "{label}: reasons differ for focus {focus}\n  left:  {left_reasons:?}\n  right: {right_reasons:?}",
        );
    }
}

fn reason_set(outcome: &shifty_engine::ValidationOutcome, focus: &str) -> Vec<String> {
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
/// shifty-engine crate manifest directory).
fn load_ws(rel: &str) -> shifty_parse::Loaded {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../")
        .join(rel);
    let bytes =
        std::fs::read(&path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    shifty_parse::load_turtle(&bytes, None)
        .unwrap_or_else(|e| panic!("failed to parse {}: {e}", path.display()))
}

/// Baseline: the NIST building-1 model validated against the 223P closure. The
/// model fully conforms, and this pins the empty violation set so any regression
/// in targeting or evaluation is caught. Three `qudt:vocab/unit` nodes
/// (`DEG_F`, `FT3-PER-MIN`, `PSI`) previously appeared here, but those were
/// false positives: their high-precision `qudt:conversionMultiplier` decimals
/// were rejected by a fixed-point lexical check even though `xsd:decimal` is
/// arbitrary-precision. See `value::is_decimal_lexical`.
#[test]
fn nist_bdg1_known_violations_against_223p_closure() {
    let shapes = load_ws("benchmark/s223/223p-closure.ttl");
    let data = load_ws("benchmark/s223/models/nist-bdg1-1.ttl");
    let parsed = shifty_parse::parse_loaded(&shapes);
    let normalized = shifty_opt::normalize(&parsed.schema);
    let physical = shifty_opt::plan(&normalized);

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

    let mut foci: Vec<String> = outcome
        .violations
        .iter()
        .map(|v| v.focus.to_string())
        .collect();
    foci.sort();
    foci.dedup();
    assert_eq!(
        foci,
        Vec::<String>::new(),
        "NIST/223P-closure violation set changed",
    );
}

/// Smoke test: the reference evaluator and the planned executor agree on
/// the 223P/NIST outcome. Both use the same Spareval backend in stage 1.
#[test]
fn reference_and_plan_agree_on_nist_bdg1() {
    let shapes = load_ws("benchmark/s223/223p.ttl");
    let data = load_ws("benchmark/s223/models/nist-bdg1-1.ttl");
    let parsed = shifty_parse::parse_loaded(&shapes);
    let normalized = shifty_opt::normalize(&parsed.schema);
    let physical = shifty_opt::plan(&normalized);

    let inference = infer_graphs(&data.graph, &shapes.graph, &normalized).expect("stratifiable");

    let ref_outcome = shifty_engine::validate_graphs_with_mode(
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

    // Compare verdicts only: the reference uses the logical schema and the plan
    // the NNF-normalized one, so the algebra path's (coarse) reason text can
    // legitimately differ even though conformance and the violating focus set
    // must match. See `assert_same_verdict`.
    assert_same_verdict("223P/NIST bdg1-1", &ref_outcome, &plan_outcome);
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
    let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).expect("valid Turtle");
    let parsed = shifty_parse::parse_turtle(ttl.as_bytes(), None).expect("valid shapes");
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
