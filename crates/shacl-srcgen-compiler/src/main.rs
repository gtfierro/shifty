use clap::{Parser, ValueEnum};
use shacl_srcgen_compiler::{
    generate_modules_from_ir_with_backend, lower_shape_ir, write_generated_modules, SrcGenBackend,
};
use shifty::{ir_cache, Source, ValidatorBuilder};
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    /// Path to the SHACL shapes graph (Turtle or .ir cache)
    shapes: PathBuf,
    /// Disable owl:imports resolution for compile-time shape loading
    #[arg(long)]
    no_imports: bool,
    /// Force-refresh ontology fetches instead of reusing cached copies
    #[arg(long)]
    force_refresh: bool,
    /// Maximum owl:imports recursion depth for shapes (-1 = unlimited, 0 = only the root graph)
    #[arg(long, default_value_t = -1)]
    import_depth: i32,
    /// Output file or directory for generated Rust modules (stdout if omitted)
    #[arg(long)]
    out: Option<PathBuf>,
    /// Optional output path for SrcGenIR JSON
    #[arg(long)]
    ir_out: Option<PathBuf>,
    /// Srcgen backend mode
    #[arg(long, value_enum, default_value_t = BackendArg::Specialized)]
    backend: BackendArg,
}

#[derive(Clone, Debug, ValueEnum)]
enum BackendArg {
    Specialized,
    Tables,
}

impl BackendArg {
    fn to_backend(&self) -> SrcGenBackend {
        match self {
            BackendArg::Specialized => SrcGenBackend::Specialized,
            BackendArg::Tables => SrcGenBackend::Tables,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let shape_ir = if args
        .shapes
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("ir"))
        .unwrap_or(false)
    {
        ir_cache::read_shape_ir(&args.shapes).map_err(|err| {
            format!(
                "failed to read SHACL-IR cache {}: {err}",
                args.shapes.display()
            )
        })?
    } else {
        let validator = ValidatorBuilder::new()
            .with_shapes_source(Source::File(args.shapes.clone()))
            .with_data_source(Source::Empty)
            .with_do_imports(!args.no_imports)
            .with_force_refresh(args.force_refresh)
            .with_import_depth(args.import_depth)
            .with_shape_optimization(true)
            .with_data_dependent_shape_optimization(false)
            .build()?;
        validator
            .shape_ir_with_imports(args.import_depth)
            .map_err(|err| format!("compile error: {err}"))?
    };

    let ir = lower_shape_ir(&shape_ir).map_err(|err| format!("lowering error: {err}"))?;

    if let Some(path) = args.ir_out {
        fs::write(&path, ir.to_json_pretty()?)?;
    }

    let modules = generate_modules_from_ir_with_backend(&ir, args.backend.to_backend())
        .map_err(|err| format!("codegen error: {err}"))?;

    if let Some(out) = args.out {
        write_generated_modules(&out, &modules).map_err(|err| format!("emit error: {err}"))?;
    } else {
        print!("{}", modules.to_single_file());
    }

    Ok(())
}
