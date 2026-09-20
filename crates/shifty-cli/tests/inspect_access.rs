use std::process::Command;

#[test]
fn inspect_access_exposes_graph_scope_and_function_demand() {
    let path =
        std::env::temp_dir().join(format!("shifty-inspect-access-{}.ttl", std::process::id()));
    std::fs::write(
        &path,
        r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://ex/> .
        ex:reads a sh:SPARQLFunction ;
            sh:ask "ASK { ?s <http://ex/fromFunction> ?o }" .
        ex:S a sh:NodeShape ; sh:targetNode ex:a ;
            sh:sparql [ sh:select """SELECT $this WHERE {
                GRAPH $shapesGraph { ?s <http://ex/fromShapes> ?o }
                FILTER(<http://ex/reads>())
            }""" ] .
        "#,
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_shifty"))
        .args(["inspect", "--stage", "access", "--format", "json"])
        .arg(&path)
        .output()
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let catalog: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let statement = &catalog["consumers"][0];
    assert_eq!(
        statement["default"]["predicates"][0]["value"],
        "http://ex/fromFunction"
    );
    assert_eq!(
        statement["shapes"]["predicates"][0]["value"],
        "http://ex/fromShapes"
    );
    assert_eq!(statement["calls"][0]["value"], "http://ex/reads");
}
