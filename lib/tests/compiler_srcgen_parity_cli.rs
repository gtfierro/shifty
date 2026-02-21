#![cfg(feature = "compiled-tests")]

use std::error::Error;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

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

#[test]
fn legacy_and_srcgen_compiled_reports_are_isomorphic() -> Result<(), Box<dyn Error>> {
    let tmp = unique_temp_dir()?;
    let shapes = tmp.join("shapes.ttl");
    let data = tmp.join("data.ttl");
    let legacy_out = tmp.join("legacy-compiled");
    let srcgen_out = tmp.join("srcgen-compiled");
    let legacy_report = tmp.join("legacy-report.ttl");
    let srcgen_report = tmp.join("srcgen-report.ttl");

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
  ] .
"#;

    let data_ttl = r#"@prefix ex: <http://example.com/ns#> .

ex:Alice a ex:Person ;
  ex:age "not-an-integer" .
"#;

    write_file(&shapes, shapes_ttl)?;
    write_file(&data, data_ttl)?;

    let shifty_path = workspace_root().join("lib");

    run_cli_to_file(
        &[
            "compile",
            "--shapes-file",
            shapes.to_str().unwrap(),
            "--compiler",
            "legacy",
            "--backend",
            "aot",
            "--out-dir",
            legacy_out.to_str().unwrap(),
            "--bin-name",
            "legacy-bin",
            "--shifty-path",
            shifty_path.to_str().unwrap(),
        ],
        &tmp.join("compile-legacy.stdout"),
    )?;

    run_cli_to_file(
        &[
            "compile",
            "--shapes-file",
            shapes.to_str().unwrap(),
            "--compiler",
            "srcgen",
            "--backend",
            "specialized",
            "--out-dir",
            srcgen_out.to_str().unwrap(),
            "--bin-name",
            "srcgen-bin",
            "--shifty-path",
            shifty_path.to_str().unwrap(),
        ],
        &tmp.join("compile-srcgen.stdout"),
    )?;

    run_binary_to_file(
        &shared_target_dir().join("debug").join("legacy-bin"),
        &["--run-inference=false", data.to_str().unwrap()],
        &legacy_report,
    )?;

    run_binary_to_file(
        &shared_target_dir().join("debug").join("srcgen-bin"),
        &["--run-inference=false", data.to_str().unwrap()],
        &srcgen_report,
    )?;

    let iso_output = run_command(
        Command::new("cargo")
            .args(["run", "-p", "shifty-shacl", "--bin", "isomorphic", "--"])
            .env("CARGO_TARGET_DIR", shared_target_dir())
            .arg(legacy_report.to_str().unwrap())
            .arg(srcgen_report.to_str().unwrap()),
    )?;

    assert!(
        iso_output.contains("isomorphic: true"),
        "compiled reports are not isomorphic:\n{}",
        iso_output
    );

    Ok(())
}
