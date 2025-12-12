use shifty::Validator;

fn fixture_path(name: &str) -> String {
    format!("{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), name)
}

#[test]
fn custom_component_uses_default_parameter_value() {
    let shapes = fixture_path("af_default_shapes.ttl");
    let data = fixture_path("af_default_data.ttl");

    let validator = Validator::from_files(&shapes, &data)
        .expect("validator should build with SHACL-AF defaults enabled");
    let report = validator.validate();

    assert!(
        !report.conforms(),
        "expected defaulted parameter to trigger a validation failure"
    );

    let report_ttl = report
        .to_turtle()
        .expect("failed to serialize validation report");

    assert!(
        report_ttl.contains("Value shorter than 5"),
        "default parameter value should materialize in the failure message: {}",
        report_ttl
    );
    assert!(
        report_ttl.contains("http://example.org/ShortName"),
        "short name individual should fail the constraint: {}",
        report_ttl
    );
}
