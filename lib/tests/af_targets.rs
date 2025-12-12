use shifty::Validator;

fn fixture_path(name: &str) -> String {
    format!("{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), name)
}

#[test]
fn sparql_target_selects_expected_focus_nodes() {
    let shapes = fixture_path("af_target_shapes.ttl");
    let data = fixture_path("af_target_data.ttl");

    let validator = Validator::from_files(&shapes, &data)
        .expect("validator should build with SHACL-AF defaults enabled");
    let report = validator.validate();

    assert!(
        !report.conforms(),
        "advanced target should surface failing focus nodes"
    );

    let ttl = report
        .to_turtle()
        .expect("failed to serialise validation report");

    assert!(
        ttl.contains("http://example.org/Beta"),
        "advanced target SELECT should include ex:Beta as a focus node: {}",
        ttl
    );
}

#[test]
fn target_shape_filter_and_ask_limit_focus_nodes() {
    let shapes = fixture_path("af_target_shapes.ttl");
    let data = fixture_path("af_target_data.ttl");

    let validator = Validator::from_files(&shapes, &data)
        .expect("validator should build with SHACL-AF defaults enabled");
    let report = validator.validate();
    let ttl = report
        .to_turtle()
        .expect("failed to serialise validation report");

    assert!(
        ttl.contains("http://example.org/Alpha"),
        "targetShape/filterShape combination should surface ex:Alpha: {}",
        ttl
    );
    assert!(
        !ttl.contains("http://example.org/Gamma"),
        "ASK-based target validator should filter out ex:Gamma: {}",
        ttl
    );
}
