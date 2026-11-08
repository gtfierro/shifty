use std::process::Command;

#[test]
fn strict_policy_reaches_inference_and_file_bases_are_independent() {
    let directory = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("cli inputs #é-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let shapes = directory.join("shapes.ttl");
    let data = directory.join("data.ttl");
    std::fs::write(&data, "<> <http://example.org/marker> \"present\" .").unwrap();
    std::fs::write(&shapes, r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://example.org/> .
        ex:exists a sh:SPARQLFunction ;
            sh:ask "ASK { ?s <http://example.org/marker> ?o }" .
        ex:S a sh:NodeShape ; sh:targetNode <data.ttl> ;
            sh:rule [ a sh:SPARQLRule ;
                sh:construct "CONSTRUCT { $this <http://example.org/flag> true } WHERE { FILTER (! <http://example.org/exists>()) }" ] ;
            sh:property [ sh:path ex:flag ; sh:maxCount 0 ] .
    "#).unwrap();
    let run = |strict| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_shifty"));
        command
            .arg("validate")
            .arg("--shapes")
            .arg(&shapes)
            .arg("--data")
            .arg(&data)
            .arg("--report");
        if strict {
            command.args(["--on-unsupported", "error"]);
        }
        command.output().unwrap()
    };
    let strict = run(true);
    assert_eq!(strict.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&strict.stderr).contains("strict inference failed"),
        "{}",
        String::from_utf8_lossy(&strict.stderr)
    );
    assert!(strict.stdout.is_empty());
    assert!(
        run(false).status.success(),
        "default handling remains permissive"
    );
}
