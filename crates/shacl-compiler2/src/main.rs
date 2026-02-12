use clap::Parser;
use shacl_compiler2::{
    generate_rust_from_plan, generate_rust_modules_from_plan, GeneratedRust, PlanIR,
};
use shifty::{Source, Validator};
use std::fs;
use std::path::Path;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    /// Path to the SHACL shapes graph (Turtle)
    shapes: PathBuf,
    /// Output file for generated Rust (stdout if omitted)
    #[arg(long)]
    out: Option<PathBuf>,
    /// Optional output path for PlanIR JSON
    #[arg(long)]
    plan_out: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let validator = Validator::builder()
        .with_shapes_source(Source::File(args.shapes.clone()))
        .with_data_source(Source::Empty)
        .with_shape_optimization(false)
        .build()?;
    validator
        .copy_shape_graph_to_data_graph()
        .map_err(|e| format!("compile error: {}", e))?;
    validator
        .run_inference()
        .map_err(|e| format!("inference error: {}", e))?;
    let inferred_quads = validator
        .data_graph_quads()
        .map_err(|e| format!("compile error: {}", e))?;
    let shape_ir = validator.shape_ir();
    let plan = PlanIR::from_shape_ir_with_quads(shape_ir, &inferred_quads)
        .map_err(|e| format!("compile error: {}", e))?;

    if let Some(plan_out) = args.plan_out {
        let json = plan
            .to_json_pretty()
            .map_err(|e| format!("plan serialization error: {}", e))?;
        fs::write(plan_out, json)?;
    }

    if let Some(out) = args.out {
        let modules =
            generate_rust_modules_from_plan(&plan).map_err(|e| format!("compile error: {}", e))?;
        write_generated_modules(&out, modules)?;
    } else {
        let generated =
            generate_rust_from_plan(&plan).map_err(|e| format!("compile error: {}", e))?;
        print!("{}", generated);
    }
    Ok(())
}

fn write_generated_modules(
    out: &Path,
    modules: GeneratedRust,
) -> Result<(), Box<dyn std::error::Error>> {
    if out.extension().and_then(|ext| ext.to_str()) == Some("rs") {
        let base_name = out
            .file_stem()
            .ok_or("output path missing file stem")?
            .to_string_lossy();
        let module_dir = out.with_file_name(base_name.as_ref());
        write_modules_dir(&module_dir, &modules)?;
        let shim = format!(
            "#[path = \"{}/mod.rs\"]\nmod generated;\npub use generated::*;\n",
            module_dir
                .file_name()
                .ok_or("module dir missing file name")?
                .to_string_lossy()
        );
        fs::write(out, shim)?;
    } else {
        write_modules_dir(out, &modules)?;
    }
    Ok(())
}

fn write_modules_dir(
    dir: &Path,
    modules: &GeneratedRust,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(dir)?;
    fs::write(dir.join("mod.rs"), &modules.root)?;
    for (name, content) in &modules.files {
        fs::write(dir.join(name), content)?;
    }
    Ok(())
}
