use std::process::Command;

#[test]
fn validation_executes_over_data_and_shapes_union() {
    let dir =
        std::env::temp_dir().join(format!("shacl-cli-union-class-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let shapes = dir.join("shapes.ttl");
    let data = dir.join("data.ttl");

    std::fs::write(
        &shapes,
        r#"
            @prefix ex: <http://ex/> .
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix sh: <http://www.w3.org/ns/shacl#> .

            ex:Parent a rdfs:Class, sh:NodeShape ;
                sh:property [ sh:path ex:forbidden ; sh:maxCount 0 ] .
            ex:Child rdfs:subClassOf ex:Parent .
            ex:shapeItem a ex:Child ; ex:forbidden ex:value .
        "#,
    )
    .unwrap();
    std::fs::write(
        &data,
        r#"
            @prefix ex: <http://ex/> .
            ex:item a ex:Child ; ex:forbidden ex:value .
        "#,
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_shacl"))
        .args([
            "validate",
            "--shapes",
            shapes.to_str().unwrap(),
            "--data",
            data.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("conforms: false"), "stdout: {stdout}");
    assert!(stdout.contains("<http://ex/item>"), "stdout: {stdout}");
    assert!(!stdout.contains("<http://ex/shapeItem>"), "stdout: {stdout}");

    let data_only = Command::new(env!("CARGO_BIN_EXE_shacl"))
        .args([
            "validate",
            "--shapes",
            shapes.to_str().unwrap(),
            "--data",
            data.to_str().unwrap(),
            "--graph-mode",
            "data",
        ])
        .output()
        .unwrap();
    let data_stdout = String::from_utf8(data_only.stdout).unwrap();
    assert!(data_stdout.contains("conforms: true"), "stdout: {data_stdout}");

    let union_all = Command::new(env!("CARGO_BIN_EXE_shacl"))
        .args([
            "validate",
            "--shapes",
            shapes.to_str().unwrap(),
            "--data",
            data.to_str().unwrap(),
            "--graph-mode",
            "union-all",
        ])
        .output()
        .unwrap();
    let union_all_stdout = String::from_utf8(union_all.stdout).unwrap();
    assert!(union_all_stdout.contains("<http://ex/item>"), "stdout: {union_all_stdout}");
    assert!(
        union_all_stdout.contains("<http://ex/shapeItem>"),
        "stdout: {union_all_stdout}"
    );

    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn sparql_constraints_see_the_shapes_graph() {
    let dir =
        std::env::temp_dir().join(format!("shacl-cli-union-sparql-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let shapes = dir.join("shapes.ttl");
    let data = dir.join("data.ttl");

    std::fs::write(
        &shapes,
        r#"
            @prefix ex: <http://ex/> .
            @prefix sh: <http://www.w3.org/ns/shacl#> .

            ex:enabled ex:value true .
            ex:S a sh:NodeShape ;
                sh:targetNode ex:item ;
                sh:sparql [
                    sh:select "SELECT $this WHERE { ex:enabled ex:value true }"
                ] .
        "#,
    )
    .unwrap();
    std::fs::write(&data, "@prefix ex: <http://ex/> . ex:item ex:value 1 .").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_shacl"))
        .args([
            "validate",
            "--shapes",
            shapes.to_str().unwrap(),
            "--data",
            data.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("conforms: false"), "stdout: {stdout}");
    assert!(
        stdout.contains("SPARQL constraint produced a violation"),
        "stdout: {stdout}"
    );

    std::fs::remove_dir_all(dir).unwrap();
}
