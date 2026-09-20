//! `--profile` input telemetry: what each source parsed as, and how many
//! triples it contributed.
//!
//! Without it a run says nothing about its own inputs: `conforms: true` reads
//! the same whether a data document parsed into hundreds of triples or into
//! none at all, and whether a file was read as the format its extension
//! suggests or as something else entirely.

use std::process::Command;

struct Fixture {
    dir: std::path::PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("shifty-cli-profile-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    fn write(&self, name: &str, contents: &str) -> String {
        let path = self.dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path.to_str().unwrap().to_string()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn run(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shifty"))
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

const SHAPES: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://ex/> .
ex:S a sh:NodeShape ;
    sh:targetSubjectsOf ex:p ;
    sh:property [ sh:path ex:p ; sh:maxCount 4 ] .
"#;

/// Literate Turtle: every prose line is a `#` comment and every statement is
/// indented, so the document parses as Turtle despite the `.md` extension.
/// Extension sniffing must not be what the reported format comes from.
const LITERATE: &str = r#"# Some notes

## A heading that is also a Turtle comment

    @prefix ex: <http://ex/> .

    ex:a ex:p ex:b .
    ex:b ex:p ex:c .
"#;

#[test]
fn profile_names_the_format_and_count_of_every_input() {
    let fx = Fixture::new("inputs");
    let shapes = fx.write("shapes.ttl", SHAPES);
    let data = fx.write("data.ttl.md", LITERATE);

    let stdout = run(&[
        "validate",
        "--shapes",
        &shapes,
        "--data",
        &data,
        "--profile",
    ]);

    assert!(stdout.starts_with("conforms: true"), "stdout: {stdout}");
    assert!(
        stdout.contains(&format!("profile: data: 2 triples from {data} [turtle]")),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains(&format!(
            "profile: shapes: 5 triples from {shapes} [turtle]"
        )),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("profile: inference: 0 triples added before validation"),
        "stdout: {stdout}"
    );
    for stage in [
        "shapes load",
        "compile",
        "data load",
        "session and inference",
        "first validation",
        "export",
    ] {
        assert!(
            stdout.contains(&format!("profile: stage: {stage}: ")),
            "missing {stage} timing in stdout: {stdout}"
        );
    }
    // The engine's own telemetry still follows the inputs: `validate` lost its
    // `print_summary` call once already.
    assert!(stdout.contains("profile: shape cache:"), "stdout: {stdout}");
}

#[test]
fn profile_is_silent_without_the_flag() {
    let fx = Fixture::new("silent");
    let shapes = fx.write("shapes.ttl", SHAPES);
    let data = fx.write("data.ttl.md", LITERATE);

    let stdout = run(&["validate", "--shapes", &shapes, "--data", &data]);

    assert_eq!(stdout.trim(), "conforms: true");
}

#[test]
fn profile_separates_merged_sources_from_their_overlap() {
    let fx = Fixture::new("merged");
    let shapes = fx.write("shapes.ttl", SHAPES);
    let first = fx.write(
        "first.ttl",
        "@prefix ex: <http://ex/> .\nex:a ex:p ex:b .\n",
    );
    let second = fx.write(
        "second.ttl",
        "@prefix ex: <http://ex/> .\nex:a ex:p ex:b .\nex:b ex:p ex:c .\n",
    );

    let stdout = run(&[
        "validate",
        "--shapes",
        &shapes,
        "--data",
        &first,
        "--data",
        &second,
        "--profile",
    ]);

    assert!(
        stdout.contains("profile: data: 2 triples from 2 sources (1 triple dropped as duplicate)"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains(&format!("  {first}: 1 triple [turtle]")),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains(&format!("  {second}: 2 triples [turtle]")),
        "stdout: {stdout}"
    );
}

#[test]
fn profile_says_when_the_shapes_graph_is_also_the_data_graph() {
    let fx = Fixture::new("selfdata");
    let shapes = fx.write("shapes.ttl", SHAPES);

    let stdout = run(&["validate", "--shapes", &shapes, "--profile"]);

    assert!(
        stdout.contains("profile: data: none given; the shapes graph is also the data graph"),
        "stdout: {stdout}"
    );
}

#[test]
fn profile_records_that_inference_was_skipped() {
    let fx = Fixture::new("noinfer");
    let shapes = fx.write("shapes.ttl", SHAPES);
    let data = fx.write("data.ttl.md", LITERATE);

    let stdout = run(&[
        "validate",
        "--shapes",
        &shapes,
        "--data",
        &data,
        "--no-infer",
        "--profile",
    ]);

    assert!(
        stdout.contains("profile: inference: skipped (--no-infer)"),
        "stdout: {stdout}"
    );
}

#[test]
fn infer_profiles_its_inputs_too() {
    let fx = Fixture::new("infer");
    let shapes = fx.write("shapes.ttl", SHAPES);
    let data = fx.write("data.ttl.md", LITERATE);

    let stdout = run(&["infer", "--shapes", &shapes, "--data", &data, "--profile"]);

    assert!(
        stdout.contains(&format!("profile: data: 2 triples from {data} [turtle]")),
        "stdout: {stdout}"
    );
    for stage in [
        "shapes load",
        "compile",
        "data load",
        "session and inference",
        "export",
    ] {
        assert!(
            stdout.contains(&format!("profile: stage: {stage}:")),
            "stdout: {stdout}"
        );
    }
}

#[test]
fn validation_does_not_project_inferred_data_without_a_dump() {
    let fx = Fixture::new("lazy-data");
    let shapes = fx.write(
        "shapes.ttl",
        r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
           @prefix ex: <http://ex/> .
           ex:S a sh:NodeShape ; sh:targetNode ex:a ;
             sh:rule [ a sh:TripleRule ; sh:subject sh:this ;
                       sh:predicate ex:out ; sh:object ex:value ] ."#,
    );
    let data = fx.write(
        "data.ttl",
        "<http://ex/a> <http://ex/seed> <http://ex/value> .",
    );

    for report in [false, true] {
        let mut args = vec![
            "validate",
            "--shapes",
            &shapes,
            "--data",
            &data,
            "--profile",
        ];
        if report {
            args.push("--report");
        }
        let stdout = run(&args);
        assert!(
            stdout.contains("0 compatibility projection(s) / 0 row(s)"),
            "stdout: {stdout}"
        );
    }
}

#[test]
fn report_mode_keeps_the_profile_after_the_document() {
    let fx = Fixture::new("report");
    let shapes = fx.write("shapes.ttl", SHAPES);
    let data = fx.write("data.ttl.md", LITERATE);

    let stdout = run(&[
        "validate",
        "--shapes",
        &shapes,
        "--data",
        &data,
        "--report",
        "--profile",
    ]);

    let (document, profile) = stdout.split_once("profile: ").unwrap();
    assert!(document.contains("sh:ValidationReport"), "stdout: {stdout}");
    assert!(profile.starts_with("shapes: "), "stdout: {stdout}");
}
