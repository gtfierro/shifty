use shapeshift_import::{FsResolver, resolve_file};
use shapeshift_macro::build;
use shapeshift_macro::diagnostic::render_all;
use std::path::PathBuf;

fn example(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../examples/shapeshift")
        .join(name)
}

#[test]
fn hvac_builds_and_validates() {
    let (resolved, parse_diags) =
        resolve_file(&example("hvac.shm"), &FsResolver::default(), true).unwrap();
    assert!(parse_diags.is_empty(), "{}", render_all(&parse_diags));
    assert!(
        !resolved.has_errors(),
        "{}",
        render_all(&resolved.diagnostics)
    );
    assert_eq!(resolved.program.modules.len(), 2, "sensors.shm is imported");

    let built = build(&resolved.program);
    assert!(!built.has_errors(), "{}", render_all(&built.diagnostics));
    assert!(built.portability.recursive, "Space refers to itself");

    let data = std::fs::read(example("hvac-data.ttl")).unwrap();
    let data = shifty_parse::load_turtle(&data, None).unwrap().graph;
    let outcome = shifty_engine::validate(&data, &built.schema).unwrap();
    let failing: Vec<String> = outcome
        .violations
        .iter()
        .map(|v| v.focus.to_string())
        .collect();
    assert!(!outcome.conforms);
    assert!(
        failing.contains(&"<https://example.org/building#vav-7>".to_string()),
        "{failing:?}"
    );
    assert!(
        !failing.contains(&"<https://example.org/building#vav-1>".to_string()),
        "{failing:?}"
    );
    assert!(
        !failing.contains(&"<https://example.org/building#rvav-1>".to_string()),
        "{failing:?}"
    );
}
