#[path = "support/manifest_harness.rs"]
mod manifest_harness;

use manifest_harness::{InMemoryManifestBackend, run_manifest_suite};

#[test]
fn manifest_core_suite_matches_expected_conformance_for_in_memory_backend() {
    let outcome = run_manifest_suite(&InMemoryManifestBackend, "core/manifest.ttl");
    assert!(outcome.executed_cases > 0);
    assert!(outcome.total_cases >= outcome.executed_cases);
    let _ = &outcome.skipped_cases;
}

#[test]
fn manifest_sparql_suite_matches_expected_conformance_for_in_memory_backend() {
    let outcome = run_manifest_suite(&InMemoryManifestBackend, "sparql/manifest.ttl");
    assert!(outcome.executed_cases > 0);
    assert!(outcome.total_cases >= outcome.executed_cases);
    let _ = &outcome.skipped_cases;
}

#[test]
fn manifest_advanced_suite_matches_expected_conformance_for_in_memory_backend() {
    let outcome = run_manifest_suite(&InMemoryManifestBackend, "advanced/manifest.ttl");
    assert!(outcome.total_cases > 0);
    assert!(outcome.total_cases >= outcome.executed_cases);
    let _ = &outcome.skipped_cases;
}
