#![cfg(feature = "compiled-tests")]

use std::error::Error;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("lib crate should have a parent workspace")
        .to_path_buf()
}

fn shared_target_dir() -> PathBuf {
    workspace_root()
        .join("target")
        .join("compiler-parity-tests")
}

fn unique_temp_dir() -> Result<PathBuf, Box<dyn Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let dir = std::env::temp_dir().join(format!("shifty_compiler_parity_{}", nanos));
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn compiled_test_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .expect("compiled parity lock should not be poisoned")
}

fn write_file(path: &Path, contents: &str) -> Result<(), Box<dyn Error>> {
    let mut file = fs::File::create(path)?;
    file.write_all(contents.as_bytes())?;
    Ok(())
}

fn assert_report_contains(path: &Path, needle: &str) -> Result<(), Box<dyn Error>> {
    let contents = fs::read_to_string(path)?;
    assert!(
        contents.contains(needle),
        "report {} does not contain expected text: {}",
        path.display(),
        needle
    );
    Ok(())
}

fn run_command(cmd: &mut Command) -> Result<String, Box<dyn Error>> {
    let output = cmd.output()?;
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "command {:?} failed (status {:?})\nstdout:\n{}\nstderr:\n{}",
            cmd, output.status, stdout, stderr
        )
        .into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn run_cli_to_file(args: &[&str], output_path: &Path) -> Result<(), Box<dyn Error>> {
    let mut child = Command::new("cargo")
        .args(["run", "-p", "cli", "--features", "shacl-compiler", "--"])
        .args(args)
        .env("CARGO_TARGET_DIR", shared_target_dir())
        .stdout(Stdio::from(fs::File::create(output_path)?))
        .spawn()?;
    let status = child.wait()?;
    if !status.success() {
        return Err(format!("cargo run cli {:?} failed with {}", args, status).into());
    }
    Ok(())
}

fn run_binary_to_file(
    binary_path: &Path,
    args: &[&str],
    output_path: &Path,
) -> Result<(), Box<dyn Error>> {
    let mut child = Command::new(binary_path)
        .args(args)
        .stdout(Stdio::from(fs::File::create(output_path)?))
        .spawn()?;
    let status = child.wait()?;
    if !status.success() {
        return Err(format!("binary {:?} {:?} failed with {}", binary_path, args, status).into());
    }
    Ok(())
}

fn assert_reports_isomorphic(left: &Path, right: &Path) -> Result<(), Box<dyn Error>> {
    let iso_output = run_command(
        Command::new("cargo")
            .args(["run", "-p", "shifty-shacl", "--bin", "isomorphic", "--"])
            .env("CARGO_TARGET_DIR", shared_target_dir())
            .arg(left.to_str().unwrap())
            .arg(right.to_str().unwrap()),
    )?;

    assert!(
        iso_output.contains("isomorphic: true"),
        "reports are not isomorphic:\n{}",
        iso_output
    );
    Ok(())
}

fn compile_with_track(
    shapes: &Path,
    out_dir: &Path,
    bin_name: &str,
    compiler: &str,
    backend: &str,
) -> Result<(), Box<dyn Error>> {
    let shifty_path = workspace_root().join("lib");
    let compile_stdout = out_dir
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("compile-{compiler}.stdout"));
    let mut args = vec![
        "compile",
        "--shapes-file",
        shapes.to_str().unwrap(),
        "--compiler",
        compiler,
        "--backend",
        backend,
        "--out-dir",
        out_dir.to_str().unwrap(),
        "--bin-name",
        bin_name,
        "--shifty-path",
        shifty_path.to_str().unwrap(),
    ];
    if compiler == "legacy" {
        args.push("--allow-legacy-compiler");
    }
    run_cli_to_file(&args, &compile_stdout)
}

#[test]
fn legacy_and_srcgen_compiled_reports_are_isomorphic() -> Result<(), Box<dyn Error>> {
    let _guard = compiled_test_lock();
    let tmp = unique_temp_dir()?;
    let shapes = tmp.join("shapes.ttl");
    let data = tmp.join("data.ttl");
    let legacy_out = tmp.join("legacy-compiled");
    let srcgen_out = tmp.join("srcgen-compiled");
    let legacy_report = tmp.join("legacy-report.ttl");
    let srcgen_report = tmp.join("srcgen-report.ttl");
    let runtime_report = tmp.join("runtime-report.ttl");

    let shapes_ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:PersonShape
  a sh:NodeShape ;
  sh:targetClass ex:Person ;
  sh:class ex:Person ;
  sh:property [
    sh:path ex:age ;
    sh:datatype xsd:integer ;
    sh:minCount 1 ;
    sh:maxCount 1 ;
  ] ;
  sh:property [
    sh:path ex:nickname ;
    sh:nodeKind sh:Literal ;
    sh:minLength 3 ;
    sh:maxLength 8 ;
    sh:pattern "^[A-Z]+$" ;
  ] .
"#;

    let data_ttl = r#"@prefix ex: <http://example.com/ns#> .

ex:Alice a ex:Person ;
  ex:age "not-an-integer" ;
  ex:nickname "ab" .
"#;

    write_file(&shapes, shapes_ttl)?;
    write_file(&data, data_ttl)?;

    compile_with_track(&shapes, &legacy_out, "legacy-bin-core", "legacy", "aot")?;
    compile_with_track(
        &shapes,
        &srcgen_out,
        "srcgen-bin-core",
        "srcgen",
        "specialized",
    )?;

    run_binary_to_file(
        &shared_target_dir().join("debug").join("legacy-bin-core"),
        &["--run-inference=false", data.to_str().unwrap()],
        &legacy_report,
    )?;

    run_binary_to_file(
        &shared_target_dir().join("debug").join("srcgen-bin-core"),
        &["--run-inference=false", data.to_str().unwrap()],
        &srcgen_report,
    )?;

    run_cli_to_file(
        &[
            "validate",
            "--shapes-file",
            shapes.to_str().unwrap(),
            "--data-file",
            data.to_str().unwrap(),
            "--format",
            "turtle",
        ],
        &runtime_report,
    )?;

    assert_reports_isomorphic(&legacy_report, &srcgen_report)?;
    assert_reports_isomorphic(&legacy_report, &runtime_report)?;
    assert_reports_isomorphic(&srcgen_report, &runtime_report)?;

    Ok(())
}

#[test]
fn legacy_srcgen_and_runtime_reports_are_isomorphic_for_phase2_expanded_constraints(
) -> Result<(), Box<dyn Error>> {
    let _guard = compiled_test_lock();
    let tmp = unique_temp_dir()?;
    let shapes = tmp.join("shapes.ttl");
    let data = tmp.join("data.ttl");
    let legacy_out = tmp.join("legacy-compiled");
    let srcgen_out = tmp.join("srcgen-compiled");
    let legacy_report = tmp.join("legacy-report.ttl");
    let srcgen_report = tmp.join("srcgen-report.ttl");
    let runtime_report = tmp.join("runtime-report.ttl");

    let shapes_ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .

ex:DeviceShape
  a sh:NodeShape ;
  sh:targetClass ex:Device ;
  sh:property [
    sh:path ex:reading ;
    sh:minInclusive 10 ;
    sh:maxExclusive 20 ;
    sh:hasValue 15 ;
    sh:in (15 16) ;
  ] ;
  sh:property [
    sh:path ex:label ;
    sh:languageIn ("en" "fr") ;
    sh:uniqueLang true ;
  ] ;
  sh:property [
    sh:path ex:code ;
    sh:equals ex:mirrorCode ;
    sh:disjoint ex:bannedCode ;
    sh:lessThan ex:maxCode ;
    sh:lessThanOrEquals ex:maxCodeInclusive ;
  ] .
"#;

    let data_ttl = r#"@prefix ex: <http://example.com/ns#> .

ex:d1 a ex:Device ;
  ex:reading 9, 21 ;
  ex:label "hello"@en, "bonjour"@EN ;
  ex:code 5, 8 ;
  ex:mirrorCode 8 ;
  ex:bannedCode 8 ;
  ex:maxCode 7 ;
  ex:maxCodeInclusive 8 .
"#;

    write_file(&shapes, shapes_ttl)?;
    write_file(&data, data_ttl)?;

    compile_with_track(&shapes, &legacy_out, "legacy-bin-expanded", "legacy", "aot")?;
    compile_with_track(
        &shapes,
        &srcgen_out,
        "srcgen-bin-expanded",
        "srcgen",
        "specialized",
    )?;

    run_binary_to_file(
        &shared_target_dir()
            .join("debug")
            .join("legacy-bin-expanded"),
        &["--run-inference=false", data.to_str().unwrap()],
        &legacy_report,
    )?;

    run_binary_to_file(
        &shared_target_dir()
            .join("debug")
            .join("srcgen-bin-expanded"),
        &["--run-inference=false", data.to_str().unwrap()],
        &srcgen_report,
    )?;

    run_cli_to_file(
        &[
            "validate",
            "--shapes-file",
            shapes.to_str().unwrap(),
            "--data-file",
            data.to_str().unwrap(),
            "--format",
            "turtle",
        ],
        &runtime_report,
    )?;

    assert_reports_isomorphic(&legacy_report, &srcgen_report)?;
    assert_reports_isomorphic(&legacy_report, &runtime_report)?;
    assert_reports_isomorphic(&srcgen_report, &runtime_report)?;

    Ok(())
}

#[test]
fn legacy_srcgen_and_runtime_reports_are_isomorphic_for_logical_node_constraints(
) -> Result<(), Box<dyn Error>> {
    let _guard = compiled_test_lock();
    let tmp = unique_temp_dir()?;
    let shapes = tmp.join("shapes.ttl");
    let data = tmp.join("data.ttl");
    let legacy_out = tmp.join("legacy-compiled");
    let srcgen_out = tmp.join("srcgen-compiled");
    let legacy_report = tmp.join("legacy-report.ttl");
    let srcgen_report = tmp.join("srcgen-report.ttl");
    let runtime_report = tmp.join("runtime-report.ttl");

    let shapes_ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .

ex:ChildShape
  a sh:NodeShape ;
  sh:targetClass ex:Child ;
  sh:property [ sh:path ex:age ; sh:minCount 1 ] .

ex:AltChildShape
  a sh:NodeShape ;
  sh:targetClass ex:AltChild ;
  sh:property [ sh:path ex:age ; sh:minCount 1 ] .

ex:ParentShape
  a sh:NodeShape ;
  sh:targetClass ex:Parent ;
  sh:property [
    sh:path ex:child ;
    sh:node ex:ChildShape ;
    sh:not ex:AltChildShape ;
    sh:and (ex:ChildShape) ;
    sh:or (ex:ChildShape ex:AltChildShape) ;
    sh:xone (ex:ChildShape ex:AltChildShape) ;
  ] .
"#;

    let data_ttl = r#"@prefix ex: <http://example.com/ns#> .

ex:p1 a ex:Parent ;
  ex:child ex:c1, ex:c2 .

ex:c1 a ex:Child ;
  ex:age 10 .

ex:c2 a ex:Child, ex:AltChild ;
  ex:age 5 .
"#;

    write_file(&shapes, shapes_ttl)?;
    write_file(&data, data_ttl)?;

    compile_with_track(
        &shapes,
        &legacy_out,
        "legacy-bin-logical-node",
        "legacy",
        "aot",
    )?;
    compile_with_track(
        &shapes,
        &srcgen_out,
        "srcgen-bin-logical-node",
        "srcgen",
        "specialized",
    )?;

    run_binary_to_file(
        &shared_target_dir()
            .join("debug")
            .join("legacy-bin-logical-node"),
        &["--run-inference=false", data.to_str().unwrap()],
        &legacy_report,
    )?;

    run_binary_to_file(
        &shared_target_dir()
            .join("debug")
            .join("srcgen-bin-logical-node"),
        &["--run-inference=false", data.to_str().unwrap()],
        &srcgen_report,
    )?;

    run_cli_to_file(
        &[
            "validate",
            "--shapes-file",
            shapes.to_str().unwrap(),
            "--data-file",
            data.to_str().unwrap(),
            "--format",
            "turtle",
        ],
        &runtime_report,
    )?;

    assert_reports_isomorphic(&legacy_report, &srcgen_report)?;
    assert_reports_isomorphic(&legacy_report, &runtime_report)?;
    assert_reports_isomorphic(&srcgen_report, &runtime_report)?;

    Ok(())
}

#[test]
fn legacy_srcgen_and_runtime_reports_are_isomorphic_for_closed_and_qualified_constraints(
) -> Result<(), Box<dyn Error>> {
    let _guard = compiled_test_lock();
    let tmp = unique_temp_dir()?;
    let shapes = tmp.join("shapes.ttl");
    let data = tmp.join("data.ttl");
    let legacy_out = tmp.join("legacy-compiled");
    let srcgen_out = tmp.join("srcgen-compiled");
    let legacy_report = tmp.join("legacy-report.ttl");
    let srcgen_report = tmp.join("srcgen-report.ttl");
    let runtime_report = tmp.join("runtime-report.ttl");

    let shapes_ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

ex:FingerShape
  a sh:NodeShape ;
  sh:class ex:Finger .

ex:ThumbShape
  a sh:NodeShape ;
  sh:class ex:Thumb .

ex:HandShape
  a sh:NodeShape ;
  sh:targetClass ex:Hand ;
  sh:closed true ;
  sh:ignoredProperties (rdf:type) ;
  sh:property [
    sh:path ex:digit ;
    sh:qualifiedMinCount 1 ;
    sh:qualifiedValueShape ex:ThumbShape ;
    sh:qualifiedValueShapesDisjoint true ;
  ] ;
  sh:property [
    sh:path ex:digit ;
    sh:qualifiedMaxCount 4 ;
    sh:qualifiedValueShape ex:FingerShape ;
    sh:qualifiedValueShapesDisjoint true ;
  ] .
"#;

    let data_ttl = r#"@prefix ex: <http://example.com/ns#> .

ex:badHand a ex:Hand ;
  ex:digit ex:fingerThumb ;
  ex:illegal ex:oops .

ex:fingerThumb a ex:Finger, ex:Thumb .
"#;

    write_file(&shapes, shapes_ttl)?;
    write_file(&data, data_ttl)?;

    compile_with_track(
        &shapes,
        &legacy_out,
        "legacy-bin-closed-qualified",
        "legacy",
        "aot",
    )?;
    compile_with_track(
        &shapes,
        &srcgen_out,
        "srcgen-bin-closed-qualified",
        "srcgen",
        "specialized",
    )?;

    run_binary_to_file(
        &shared_target_dir()
            .join("debug")
            .join("legacy-bin-closed-qualified"),
        &["--run-inference=false", data.to_str().unwrap()],
        &legacy_report,
    )?;

    run_binary_to_file(
        &shared_target_dir()
            .join("debug")
            .join("srcgen-bin-closed-qualified"),
        &["--run-inference=false", data.to_str().unwrap()],
        &srcgen_report,
    )?;

    run_cli_to_file(
        &[
            "validate",
            "--shapes-file",
            shapes.to_str().unwrap(),
            "--data-file",
            data.to_str().unwrap(),
            "--format",
            "turtle",
        ],
        &runtime_report,
    )?;

    assert_reports_isomorphic(&legacy_report, &srcgen_report)?;
    assert_reports_isomorphic(&legacy_report, &runtime_report)?;
    assert_reports_isomorphic(&srcgen_report, &runtime_report)?;

    Ok(())
}

#[test]
fn legacy_srcgen_and_runtime_reports_are_isomorphic_for_node_datatype_and_membership_constraints(
) -> Result<(), Box<dyn Error>> {
    let _guard = compiled_test_lock();
    let tmp = unique_temp_dir()?;
    let shapes = tmp.join("shapes.ttl");
    let data = tmp.join("data.ttl");
    let legacy_out = tmp.join("legacy-compiled");
    let srcgen_out = tmp.join("srcgen-compiled");
    let legacy_report = tmp.join("legacy-report.ttl");
    let srcgen_report = tmp.join("srcgen-report.ttl");
    let runtime_report = tmp.join("runtime-report.ttl");

    let shapes_ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:ItemShape
  a sh:NodeShape ;
  sh:targetClass ex:Item ;
  sh:datatype xsd:string ;
  sh:hasValue ex:requiredItem ;
  sh:in (ex:requiredItem ex:alternateItem) .
"#;

    let data_ttl = r#"@prefix ex: <http://example.com/ns#> .

ex:item1 a ex:Item .
"#;

    write_file(&shapes, shapes_ttl)?;
    write_file(&data, data_ttl)?;

    compile_with_track(
        &shapes,
        &legacy_out,
        "legacy-bin-node-membership",
        "legacy",
        "aot",
    )?;
    compile_with_track(
        &shapes,
        &srcgen_out,
        "srcgen-bin-node-membership",
        "srcgen",
        "specialized",
    )?;

    run_binary_to_file(
        &shared_target_dir()
            .join("debug")
            .join("legacy-bin-node-membership"),
        &["--run-inference=false", data.to_str().unwrap()],
        &legacy_report,
    )?;

    run_binary_to_file(
        &shared_target_dir()
            .join("debug")
            .join("srcgen-bin-node-membership"),
        &["--run-inference=false", data.to_str().unwrap()],
        &srcgen_report,
    )?;

    run_cli_to_file(
        &[
            "validate",
            "--shapes-file",
            shapes.to_str().unwrap(),
            "--data-file",
            data.to_str().unwrap(),
            "--format",
            "turtle",
        ],
        &runtime_report,
    )?;

    assert_reports_isomorphic(&legacy_report, &srcgen_report)?;
    assert_reports_isomorphic(&legacy_report, &runtime_report)?;
    assert_reports_isomorphic(&srcgen_report, &runtime_report)?;

    Ok(())
}

#[test]
fn legacy_srcgen_and_runtime_reports_are_isomorphic_with_inference() -> Result<(), Box<dyn Error>> {
    let _guard = compiled_test_lock();
    let tmp = unique_temp_dir()?;
    let shapes = tmp.join("shapes.ttl");
    let data = tmp.join("data.ttl");
    let legacy_out = tmp.join("legacy-compiled");
    let srcgen_out = tmp.join("srcgen-compiled");
    let legacy_report = tmp.join("legacy-report.ttl");
    let srcgen_report = tmp.join("srcgen-report.ttl");
    let runtime_report = tmp.join("runtime-report.ttl");

    let shapes_ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .

ex:PersonShape
  a sh:NodeShape ;
  sh:targetClass ex:Person ;
  sh:property [
    sh:path ex:flag ;
    sh:minCount 1 ;
  ] ;
  sh:rule [
    a sh:TripleRule ;
    sh:subject sh:this ;
    sh:predicate ex:flag ;
    sh:object "true" ;
  ] .
"#;

    let data_ttl = r#"@prefix ex: <http://example.com/ns#> .

ex:Alice a ex:Person .
"#;

    write_file(&shapes, shapes_ttl)?;
    write_file(&data, data_ttl)?;

    compile_with_track(&shapes, &legacy_out, "legacy-bin-infer", "legacy", "aot")?;
    compile_with_track(
        &shapes,
        &srcgen_out,
        "srcgen-bin-infer",
        "srcgen",
        "specialized",
    )?;

    run_binary_to_file(
        &shared_target_dir().join("debug").join("legacy-bin-infer"),
        &["--run-inference=true", data.to_str().unwrap()],
        &legacy_report,
    )?;

    run_binary_to_file(
        &shared_target_dir().join("debug").join("srcgen-bin-infer"),
        &["--run-inference=true", data.to_str().unwrap()],
        &srcgen_report,
    )?;

    run_cli_to_file(
        &[
            "validate",
            "--shapes-file",
            shapes.to_str().unwrap(),
            "--data-file",
            data.to_str().unwrap(),
            "--run-inference",
            "--format",
            "turtle",
        ],
        &runtime_report,
    )?;

    assert_reports_isomorphic(&legacy_report, &srcgen_report)?;
    assert_reports_isomorphic(&legacy_report, &runtime_report)?;
    assert_reports_isomorphic(&srcgen_report, &runtime_report)?;

    Ok(())
}

#[test]
fn legacy_srcgen_and_runtime_reports_are_isomorphic_with_shape_data_union(
) -> Result<(), Box<dyn Error>> {
    let _guard = compiled_test_lock();
    let tmp = unique_temp_dir()?;
    let shapes = tmp.join("shapes.ttl");
    let data = tmp.join("data.ttl");
    let legacy_out = tmp.join("legacy-compiled");
    let srcgen_out = tmp.join("srcgen-compiled");
    let legacy_report = tmp.join("legacy-report.ttl");
    let srcgen_report = tmp.join("srcgen-report.ttl");
    let runtime_report = tmp.join("runtime-report.ttl");

    // ex:refValue only has rdf:type in the shapes graph. Correct validation requires
    // querying the union of shapes+data graphs for sh:class checks.
    let shapes_ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .

ex:refValue a ex:ApprovedThing .

ex:PersonShape
  a sh:NodeShape ;
  sh:targetClass ex:Person ;
  sh:property [
    sh:path ex:linked ;
    sh:minCount 1 ;
    sh:class ex:ApprovedThing ;
  ] .
"#;

    let data_ttl = r#"@prefix ex: <http://example.com/ns#> .

ex:Alice a ex:Person ;
  ex:linked ex:refValue .
"#;

    write_file(&shapes, shapes_ttl)?;
    write_file(&data, data_ttl)?;

    compile_with_track(&shapes, &legacy_out, "legacy-bin-union", "legacy", "aot")?;
    compile_with_track(
        &shapes,
        &srcgen_out,
        "srcgen-bin-union",
        "srcgen",
        "specialized",
    )?;

    run_binary_to_file(
        &shared_target_dir().join("debug").join("legacy-bin-union"),
        &["--run-inference=false", data.to_str().unwrap()],
        &legacy_report,
    )?;

    run_binary_to_file(
        &shared_target_dir().join("debug").join("srcgen-bin-union"),
        &["--run-inference=false", data.to_str().unwrap()],
        &srcgen_report,
    )?;

    run_cli_to_file(
        &[
            "validate",
            "--shapes-file",
            shapes.to_str().unwrap(),
            "--data-file",
            data.to_str().unwrap(),
            "--format",
            "turtle",
        ],
        &runtime_report,
    )?;

    assert_reports_isomorphic(&legacy_report, &srcgen_report)?;
    assert_reports_isomorphic(&legacy_report, &runtime_report)?;
    assert_reports_isomorphic(&srcgen_report, &runtime_report)?;

    Ok(())
}

#[test]
fn legacy_srcgen_and_runtime_detect_domain_sparql_property_violation() -> Result<(), Box<dyn Error>>
{
    let _guard = compiled_test_lock();
    let tmp = unique_temp_dir()?;
    let shapes = tmp.join("shapes.ttl");
    let data = tmp.join("data.ttl");
    let legacy_out = tmp.join("legacy-compiled");
    let srcgen_out = tmp.join("srcgen-compiled");
    let legacy_report = tmp.join("legacy-report.ttl");
    let srcgen_report = tmp.join("srcgen-report.ttl");
    let runtime_report = tmp.join("runtime-report.ttl");

    let shapes_ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/hvac#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:Prefixes
  sh:declare [
    sh:prefix "ex" ;
    sh:namespace "http://example.com/hvac#"^^xsd:anyURI ;
  ] .

ex:HvacZoneShape
  a sh:NodeShape ;
  sh:targetClass ex:HvacZone ;
  sh:property [
    sh:path ex:airFlowRate ;
    sh:sparql [
      sh:prefixes ex:Prefixes ;
      sh:select """
        SELECT $this ?value ?path
        WHERE {
          $this $PATH ?value .
          FILTER(?value < 0)
          BIND($PATH AS ?path)
        }
      """ ;
    ] ;
  ] .
"#;

    let data_ttl = r#"@prefix ex: <http://example.com/hvac#> .

ex:zoneOk a ex:HvacZone ;
  ex:airFlowRate 5 .

ex:zoneBad a ex:HvacZone ;
  ex:airFlowRate -1 .
"#;

    write_file(&shapes, shapes_ttl)?;
    write_file(&data, data_ttl)?;

    compile_with_track(
        &shapes,
        &legacy_out,
        "legacy-bin-domain-sparql",
        "legacy",
        "aot",
    )?;
    compile_with_track(
        &shapes,
        &srcgen_out,
        "srcgen-bin-domain-sparql",
        "srcgen",
        "specialized",
    )?;

    run_binary_to_file(
        &shared_target_dir()
            .join("debug")
            .join("legacy-bin-domain-sparql"),
        &["--run-inference=false", data.to_str().unwrap()],
        &legacy_report,
    )?;

    run_binary_to_file(
        &shared_target_dir()
            .join("debug")
            .join("srcgen-bin-domain-sparql"),
        &["--run-inference=false", data.to_str().unwrap()],
        &srcgen_report,
    )?;

    run_cli_to_file(
        &[
            "validate",
            "--shapes-file",
            shapes.to_str().unwrap(),
            "--data-file",
            data.to_str().unwrap(),
            "--format",
            "turtle",
        ],
        &runtime_report,
    )?;

    for report in [&legacy_report, &srcgen_report, &runtime_report] {
        assert_report_contains(report, "sh:conforms false")?;
        assert_report_contains(report, "<http://example.com/hvac#zoneBad>")?;
        assert_report_contains(report, "<http://example.com/hvac#airFlowRate>")?;
        assert_report_contains(
            report,
            "sh:sourceConstraintComponent sh:SPARQLConstraintComponent",
        )?;
        assert_report_contains(report, "sh:value -1")?;
    }

    Ok(())
}

#[test]
#[ignore = "perf gate: run manually with --ignored"]
fn srcgen_compiled_perf_smoke_gate_against_runtime_validate() -> Result<(), Box<dyn Error>> {
    let _guard = compiled_test_lock();
    let tmp = unique_temp_dir()?;
    let shapes = tmp.join("shapes.ttl");
    let data = tmp.join("data.ttl");
    let srcgen_out = tmp.join("srcgen-compiled");

    let shapes_ttl = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:PersonShape
  a sh:NodeShape ;
  sh:targetClass ex:Person ;
  sh:property [
    sh:path ex:age ;
    sh:datatype xsd:integer ;
    sh:minCount 1 ;
    sh:maxCount 1 ;
  ] ;
  sh:property [
    sh:path ex:nickname ;
    sh:nodeKind sh:Literal ;
    sh:minLength 3 ;
    sh:maxLength 8 ;
    sh:pattern "^[A-Z]+$" ;
  ] .
"#;
    write_file(&shapes, shapes_ttl)?;

    let mut data_ttl = String::from("@prefix ex: <http://example.com/ns#> .\n\n");
    for i in 0..400 {
        data_ttl.push_str(&format!(
            "ex:P{0} a ex:Person ; ex:age \"{0}\" ; ex:nickname \"ABC\" .\n",
            i
        ));
    }
    write_file(&data, &data_ttl)?;

    compile_with_track(
        &shapes,
        &srcgen_out,
        "srcgen-bin-perf",
        "srcgen",
        "specialized",
    )?;

    let srcgen_bin = shared_target_dir().join("debug").join("srcgen-bin-perf");
    let mut srcgen_runs: Vec<Duration> = Vec::new();
    let mut runtime_runs: Vec<Duration> = Vec::new();

    // Warmup
    run_binary_to_file(
        &srcgen_bin,
        &["--run-inference=false", data.to_str().unwrap()],
        &tmp.join("warmup-srcgen.ttl"),
    )?;
    run_cli_to_file(
        &[
            "validate",
            "--shapes-file",
            shapes.to_str().unwrap(),
            "--data-file",
            data.to_str().unwrap(),
            "--format",
            "turtle",
        ],
        &tmp.join("warmup-runtime.ttl"),
    )?;

    for idx in 0..5 {
        let start = Instant::now();
        run_binary_to_file(
            &srcgen_bin,
            &["--run-inference=false", data.to_str().unwrap()],
            &tmp.join(format!("srcgen-{idx}.ttl")),
        )?;
        srcgen_runs.push(start.elapsed());

        let start = Instant::now();
        run_cli_to_file(
            &[
                "validate",
                "--shapes-file",
                shapes.to_str().unwrap(),
                "--data-file",
                data.to_str().unwrap(),
                "--format",
                "turtle",
            ],
            &tmp.join(format!("runtime-{idx}.ttl")),
        )?;
        runtime_runs.push(start.elapsed());
    }

    srcgen_runs.sort();
    runtime_runs.sort();
    let srcgen_median = srcgen_runs[srcgen_runs.len() / 2];
    let runtime_median = runtime_runs[runtime_runs.len() / 2];

    assert!(
        srcgen_median.as_secs_f64() <= runtime_median.as_secs_f64() * 1.25,
        "srcgen perf gate failed: srcgen_median={:?}, runtime_median={:?}",
        srcgen_median,
        runtime_median
    );

    Ok(())
}
