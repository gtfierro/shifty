use std::error::Error;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir() -> Result<PathBuf, Box<dyn Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let dir = std::env::temp_dir().join(format!("shifty_iso_test_{}", nanos));
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
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
        .args(["run", "-p", "cli", "--"])
        .args(args)
        .stdout(Stdio::from(fs::File::create(output_path)?))
        .spawn()?;
    let status = child.wait()?;
    if !status.success() {
        return Err(format!("cargo run cli {:?} failed with {}", args, status).into());
    }
    Ok(())
}

#[test]
fn validate_and_cached_ir_reports_are_isomorphic_with_inference() -> Result<(), Box<dyn Error>> {
    let tmp = unique_temp_dir()?;
    let shapes = tmp.join("shapes.ttl");
    let data = tmp.join("data.ttl");
    let report_a = tmp.join("report-direct.ttl");
    let report_b = tmp.join("report-ir.ttl");
    let ir_path = tmp.join("shapes.ir");

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

    // Direct validate with inference.
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
        &report_a,
    )?;

    // Generate IR then validate with inference using the cache.
    run_cli_to_file(
        &[
            "generate-ir",
            "--shapes-file",
            shapes.to_str().unwrap(),
            "--output-file",
            ir_path.to_str().unwrap(),
        ],
        &tmp.join("generate_ir.stdout"),
    )?;

    run_cli_to_file(
        &[
            "validate",
            "--shacl-ir",
            ir_path.to_str().unwrap(),
            "--data-file",
            data.to_str().unwrap(),
            "--run-inference",
            "--format",
            "turtle",
        ],
        &report_b,
    )?;

    // Compare reports via the isomorphic binary.
    let iso_output = run_command(
        Command::new("cargo")
            .args(["run", "-p", "shifty", "--bin", "isomorphic", "--"])
            .arg(report_a.to_str().unwrap())
            .arg(report_b.to_str().unwrap()),
    )?;

    assert!(
        iso_output.contains("isomorphic: true"),
        "reports are not isomorphic:\n{}",
        iso_output
    );

    Ok(())
}
