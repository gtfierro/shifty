//! `--dump-data` / `--dump-shapes`: the graphs validation actually read.
//!
//! Neither is the file on disk. The data graph carries whatever SHACL-AF
//! inference added, and the shapes graph is every `--shapes` source merged, so
//! re-reading the inputs is not a way to see what was evaluated.

use std::process::Command;

struct Fixture {
    dir: std::path::PathBuf,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("shifty-cli-dump-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    fn write(&self, name: &str, contents: &str) -> String {
        let path = self.dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path.to_str().unwrap().to_string()
    }

    fn path(&self, name: &str) -> String {
        self.dir.join(name).to_str().unwrap().to_string()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn shifty(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_shifty"))
        .args(args)
        .output()
        .unwrap()
}

fn run(args: &[&str]) -> String {
    let output = shifty(args);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

/// A rule that derives `ex:b rdfs:subClassOf ex:a` from `ex:b ex:aliasOf ex:a`,
/// so the evaluated data graph is strictly larger than the input document.
const RULES: &str = r#"
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix ex:   <http://ex/> .

ex:AliasShape
    a sh:NodeShape ;
    sh:targetSubjectsOf ex:aliasOf ;
    sh:rule [
        a sh:TripleRule ;
        sh:subject sh:this ;
        sh:predicate rdfs:subClassOf ;
        sh:object [ sh:path ex:aliasOf ] ;
    ] .
"#;

const DATA: &str = "@prefix ex: <http://ex/> .\nex:b ex:aliasOf ex:a .\n";

#[test]
fn dumped_data_graph_carries_what_inference_added() {
    let fx = Fixture::new("inferred");
    let shapes = fx.write("shapes.ttl", RULES);
    let data = fx.write("data.ttl", DATA);
    let dump = fx.path("used-data.ttl");

    run(&[
        "validate",
        "--shapes",
        &shapes,
        "--data",
        &data,
        "--dump-data",
        &dump,
    ]);

    let dumped = std::fs::read_to_string(&dump).unwrap();
    assert!(dumped.contains("ex:aliasOf ex:a"), "dumped: {dumped}");
    // The point of the flag: this triple exists in no input file.
    assert!(dumped.contains("rdfs:subClassOf ex:a"), "dumped: {dumped}");
    assert!(
        !std::fs::read_to_string(&data)
            .unwrap()
            .contains("subClassOf")
    );
}

#[test]
fn no_infer_dumps_the_data_graph_as_read() {
    let fx = Fixture::new("noinfer");
    let shapes = fx.write("shapes.ttl", RULES);
    let data = fx.write("data.ttl", DATA);
    let dump = fx.path("used-data.ttl");

    run(&[
        "validate",
        "--shapes",
        &shapes,
        "--data",
        &data,
        "--no-infer",
        "--dump-data",
        &dump,
    ]);

    let dumped = std::fs::read_to_string(&dump).unwrap();
    assert!(dumped.contains("ex:aliasOf ex:a"), "dumped: {dumped}");
    assert!(!dumped.contains("subClassOf"), "dumped: {dumped}");
}

#[test]
fn dumped_shapes_graph_is_every_source_merged() {
    let fx = Fixture::new("merged");
    let first = fx.write("first.ttl", RULES);
    let second = fx.write(
        "second.ttl",
        r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://ex/> .
ex:Second a sh:NodeShape ; sh:targetNode ex:b .
"#,
    );
    let data = fx.write("data.ttl", DATA);
    let dump = fx.path("used-shapes.ttl");

    run(&[
        "validate",
        "--shapes",
        &first,
        "--shapes",
        &second,
        "--data",
        &data,
        "--dump-shapes",
        &dump,
    ]);

    let dumped = std::fs::read_to_string(&dump).unwrap();
    assert!(dumped.contains("ex:AliasShape"), "dumped: {dumped}");
    assert!(dumped.contains("ex:Second"), "dumped: {dumped}");
}

#[test]
fn a_dash_writes_the_graph_to_stdout_before_the_result() {
    let fx = Fixture::new("stdout");
    let shapes = fx.write("shapes.ttl", RULES);
    let data = fx.write("data.ttl", DATA);

    let stdout = run(&[
        "validate",
        "--shapes",
        &shapes,
        "--data",
        &data,
        "--dump-data",
        "-",
    ]);

    let (document, rest) = stdout.split_once("conforms:").unwrap();
    assert!(document.contains("@prefix ex:"), "stdout: {stdout}");
    assert!(
        document.contains("rdfs:subClassOf ex:a"),
        "stdout: {stdout}"
    );
    assert!(rest.trim_start().starts_with("true"), "stdout: {stdout}");
}

#[test]
fn both_graphs_can_be_dumped_in_one_run() {
    let fx = Fixture::new("both");
    let shapes = fx.write("shapes.ttl", RULES);
    let data = fx.write("data.ttl", DATA);
    let data_dump = fx.path("used-data.ttl");
    let shapes_dump = fx.path("used-shapes.ttl");

    run(&[
        "validate",
        "--shapes",
        &shapes,
        "--data",
        &data,
        "--dump-data",
        &data_dump,
        "--dump-shapes",
        &shapes_dump,
    ]);

    assert!(
        std::fs::read_to_string(&data_dump)
            .unwrap()
            .contains("ex:b")
    );
    assert!(
        std::fs::read_to_string(&shapes_dump)
            .unwrap()
            .contains("ex:AliasShape")
    );
}

#[test]
fn an_unwritable_destination_names_the_path() {
    let fx = Fixture::new("unwritable");
    let shapes = fx.write("shapes.ttl", RULES);
    let data = fx.write("data.ttl", DATA);
    let dump = fx.path("no-such-directory/used-data.ttl");

    let output = shifty(&[
        "validate",
        "--shapes",
        &shapes,
        "--data",
        &data,
        "--dump-data",
        &dump,
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&format!("failed to write {dump}")),
        "{stderr}"
    );
}

/// Without `--data` the shapes graph is the data graph, and `--dump-data` shows
/// it post-inference rather than refusing.
#[test]
fn dump_data_works_when_the_shapes_graph_is_the_data_graph() {
    let fx = Fixture::new("selfdata");
    let shapes = fx.write(
        "shapes.ttl",
        &format!("{RULES}\n@prefix ex2: <http://ex/> .\nex2:b ex2:aliasOf ex2:a .\n"),
    );
    let dump = fx.path("used-data.ttl");

    run(&["validate", "--shapes", &shapes, "--dump-data", &dump]);

    let dumped = std::fs::read_to_string(&dump).unwrap();
    assert!(dumped.contains("ex:AliasShape"), "dumped: {dumped}");
    assert!(dumped.contains("rdfs:subClassOf ex:a"), "dumped: {dumped}");
}
