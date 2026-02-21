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
    run_cli_to_file(
        &[
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
        ],
        &compile_stdout,
    )
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
