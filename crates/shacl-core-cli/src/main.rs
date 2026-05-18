use clap::{Parser, Subcommand, ValueEnum};
use shifty_shacl_core::source::{ShapeSource, SourceLoadOptions, source_from_str};
use shifty_shacl_core::{
    load_and_parse_with_ontoenv, lower_to_program, parse_resolved, render_shape_program_dot,
};
use std::io::{self, Write};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about = "Inspect parsed SHACL core structures")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Dump parsed shapes as JSON
    Dump(DumpArgs),
    /// Emit Graphviz DOT for the shape/component graph
    Graphviz(GraphvizArgs),
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum DumpStage {
    Syntax,
    Program,
}

#[derive(Parser, Debug)]
struct SharedArgs {
    /// Shapes files or URLs to load
    #[arg(value_name = "SHAPES", required = true)]
    shapes: Vec<String>,

    /// Do not resolve owl:imports
    #[arg(long)]
    no_imports: bool,

    /// Import depth (-1 = unlimited)
    #[arg(long, default_value_t = -1)]
    import_depth: i32,

    /// Force refresh remote/local OntoEnv entries
    #[arg(long)]
    force_refresh: bool,

    /// Reuse a persistent OntoEnv workspace instead of a temporary one
    #[arg(long)]
    persistent: bool,
}

#[derive(Parser, Debug)]
struct DumpArgs {
    #[command(flatten)]
    shared: SharedArgs,

    /// Whether to dump syntax or lowered program
    #[arg(long, value_enum, default_value_t = DumpStage::Program)]
    stage: DumpStage,

    /// Write output to a file instead of stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct GraphvizArgs {
    #[command(flatten)]
    shared: SharedArgs,

    /// Write output to a file instead of stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Dump(args) => run_dump(args)?,
        Command::Graphviz(args) => run_graphviz(args)?,
    }
    Ok(())
}

fn run_dump(args: DumpArgs) -> Result<(), Box<dyn std::error::Error>> {
    let resolved = load_shapes(&args.shared)?;
    let syntax = parse_resolved(&resolved);
    match args.stage {
        DumpStage::Syntax => write_json(&syntax, args.output.as_deref())?,
        DumpStage::Program => {
            let program = lower_to_program(&syntax);
            write_json(&program, args.output.as_deref())?;
        }
    }
    Ok(())
}

fn run_graphviz(args: GraphvizArgs) -> Result<(), Box<dyn std::error::Error>> {
    let syntax = load_and_parse_with_ontoenv(&shape_sources(&args.shared), &load_options(&args.shared))?;
    let program = lower_to_program(&syntax);
    let dot = render_shape_program_dot(&program);
    write_text(&dot, args.output.as_deref())?;
    Ok(())
}

fn load_shapes(
    args: &SharedArgs,
) -> Result<shifty_shacl_core::source::ResolvedShapeSet, Box<dyn std::error::Error>> {
    shifty_shacl_core::source::load_with_ontoenv(&shape_sources(args), &load_options(args))
}

fn shape_sources(args: &SharedArgs) -> Vec<ShapeSource> {
    args.shapes.iter().map(|shape| source_from_str(shape)).collect()
}

fn load_options(args: &SharedArgs) -> SourceLoadOptions {
    SourceLoadOptions {
        include_imports: !args.no_imports,
        import_depth: args.import_depth,
        temporary_env: !args.persistent,
        refresh_mode: if args.force_refresh {
            shifty_shacl_core::source::RefreshMode::Force
        } else {
            shifty_shacl_core::source::RefreshMode::UseCache
        },
    }
}

fn write_json<T: serde::Serialize>(
    value: &T,
    output: Option<&std::path::Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = output {
        let file = std::fs::File::create(path)?;
        serde_json::to_writer_pretty(file, value)?;
    } else {
        let stdout = io::stdout();
        let mut lock = stdout.lock();
        serde_json::to_writer_pretty(&mut lock, value)?;
        writeln!(&mut lock)?;
    }
    Ok(())
}

fn write_text(text: &str, output: Option<&std::path::Path>) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = output {
        std::fs::write(path, text)?;
    } else {
        let stdout = io::stdout();
        let mut lock = stdout.lock();
        lock.write_all(text.as_bytes())?;
    }
    Ok(())
}
