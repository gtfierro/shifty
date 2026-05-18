#[path = "support/manifest_harness.rs"]
mod manifest_harness;

use manifest_harness::{InMemoryManifestBackend, ManifestValidationBackend, run_manifest_suite};

#[test]
fn in_memory_backend_declares_manifest_conformance_metadata() {
    let backend = InMemoryManifestBackend;
    let metadata = backend.manifest_metadata();
    assert!(metadata.supported_regions.contains(&"core/"));
    assert!(metadata.supported_regions.contains(&"sparql/"));
    assert!(metadata.supported_regions.contains(&"advanced/"));
    assert!(
        metadata
            .skipped_regions
            .iter()
            .any(|region| region.prefix == "sparql/pre-binding/")
    );
    assert!(
        metadata
            .known_divergence_cases
            .iter()
            .any(|case| case.manifest_path == "core/complex/shacl-shacl.ttl")
    );
}

#[test]
fn manifest_core_suite_matches_expected_conformance_for_in_memory_backend() {
    let backend = InMemoryManifestBackend;
    let outcome = run_manifest_suite(&backend, "core/manifest.ttl");
    assert!(outcome.executed_cases > 0);
    assert!(outcome.total_cases >= outcome.executed_cases);
    assert!(
        outcome
            .skipped_cases
            .iter()
            .all(|(_, reason)| !reason.is_empty())
    );
}

#[test]
fn manifest_sparql_suite_matches_expected_conformance_for_in_memory_backend() {
    let backend = InMemoryManifestBackend;
    let outcome = run_manifest_suite(&backend, "sparql/manifest.ttl");
    assert!(outcome.executed_cases > 0);
    assert!(outcome.total_cases >= outcome.executed_cases);
    assert!(
        outcome
            .skipped_cases
            .iter()
            .all(|(_, reason)| !reason.is_empty())
    );
}

#[test]
fn manifest_advanced_suite_matches_expected_conformance_for_in_memory_backend() {
    let backend = InMemoryManifestBackend;
    let outcome = run_manifest_suite(&backend, "advanced/manifest.ttl");
    assert!(outcome.total_cases > 0);
    assert!(outcome.total_cases >= outcome.executed_cases);
    assert!(
        outcome
            .skipped_cases
            .iter()
            .all(|(_, reason)| !reason.is_empty())
    );
}
