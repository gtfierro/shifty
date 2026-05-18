use clap::{Parser, Subcommand, ValueEnum};
use shifty_shacl_core::source::{ShapeSource, SourceLoadOptions, source_from_str};
use shifty_shacl_core::{
    AnalysisSummary, analyze_program, load_and_parse_with_ontoenv, lower_to_program,
    parse_resolved, render_shape_program_dot,
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
    /// Summarize the lowered shape program and diagnostics
    Analyze(AnalyzeArgs),
    /// Emit Graphviz DOT for the shape/component graph
    Graphviz(GraphvizArgs),
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum DumpStage {
    Syntax,
    Program,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum AnalyzeFormat {
    Text,
    Json,
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

#[derive(Parser, Debug)]
struct AnalyzeArgs {
    #[command(flatten)]
    shared: SharedArgs,

    /// Output format
    #[arg(long, value_enum, default_value_t = AnalyzeFormat::Text)]
    format: AnalyzeFormat,

    /// Write output to a file instead of stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Dump(args) => run_dump(args)?,
        Command::Analyze(args) => run_analyze(args)?,
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
    let syntax =
        load_and_parse_with_ontoenv(&shape_sources(&args.shared), &load_options(&args.shared))?;
    let program = lower_to_program(&syntax);
    let dot = render_shape_program_dot(&program);
    write_text(&dot, args.output.as_deref())?;
    Ok(())
}

fn run_analyze(args: AnalyzeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let resolved = load_shapes(&args.shared)?;
    let syntax = parse_resolved(&resolved);
    let program = lower_to_program(&syntax);
    let analysis = analyze_program(&program);
    match args.format {
        AnalyzeFormat::Json => {
            write_json(
                &serde_json::json!({
                    "analysis": analysis,
                    "program_diagnostics": program.diagnostics,
                    "syntax_diagnostics": syntax.diagnostics,
                }),
                args.output.as_deref(),
            )?;
        }
        AnalyzeFormat::Text => {
            let text = render_analysis_text(&analysis, &program, &syntax);
            write_text(&text, args.output.as_deref())?;
        }
    }
    Ok(())
}

fn load_shapes(
    args: &SharedArgs,
) -> Result<shifty_shacl_core::source::ResolvedShapeSet, Box<dyn std::error::Error>> {
    shifty_shacl_core::source::load_with_ontoenv(&shape_sources(args), &load_options(args))
}

fn shape_sources(args: &SharedArgs) -> Vec<ShapeSource> {
    args.shapes
        .iter()
        .map(|shape| source_from_str(shape))
        .collect()
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

fn write_text(
    text: &str,
    output: Option<&std::path::Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = output {
        std::fs::write(path, text)?;
    } else {
        let stdout = io::stdout();
        let mut lock = stdout.lock();
        lock.write_all(text.as_bytes())?;
    }
    Ok(())
}

fn render_analysis_text(
    analysis: &AnalysisSummary,
    program: &shifty_shacl_core::algebra::ShapeProgram,
    syntax: &shifty_shacl_core::syntax::ShapeSyntaxDocument,
) -> String {
    let mut out = String::new();
    out.push_str("Shapes\n");
    out.push_str(&format!("  total: {}\n", program.shapes.len()));
    out.push_str(&format!(
        "  validation: {}\n",
        analysis.validation_shape_count
    ));
    out.push_str(&format!(
        "  deactivated: {}\n",
        analysis.deactivated_shape_count
    ));
    out.push_str(&format!(
        "  reachable: {}\n",
        analysis.reachable_shapes.len()
    ));
    out.push_str(&format!("  rules: {}\n", analysis.inference_rule_count));
    out.push_str(&format!("  targets: {}\n", program.targets.len()));
    out.push_str(&format!("  constraints: {}\n", program.constraints.len()));
    out.push_str(&format!(
        "  sources: {} root / {} imported\n",
        program
            .source_inventory
            .iter()
            .filter(|source| source.is_root)
            .count(),
        analysis.import_source_count
    ));

    out.push_str("\nFeatures\n");
    let mut features: Vec<_> = analysis.feature_counts.iter().collect();
    features.sort_by_key(|(name, _)| *name);
    for (name, count) in features {
        out.push_str(&format!("  {}: {}\n", name, count));
    }

    out.push_str("\nDependency Components\n");
    for (index, component) in analysis.dependency_components.iter().enumerate() {
        out.push_str(&format!(
            "  {}. size={} recursive={}\n",
            index + 1,
            component.shapes.len(),
            component.recursive
        ));
        let labels = component
            .shapes
            .iter()
            .filter_map(|shape_id| {
                program
                    .shapes
                    .iter()
                    .find(|shape| shape.id == *shape_id)
                    .map(|shape| shape.source.to_string())
            })
            .collect::<Vec<_>>();
        if !labels.is_empty() {
            out.push_str(&format!("     {}\n", labels.join(", ")));
        }
    }

    out.push_str("\nDiagnostics\n");
    out.push_str(&format!("  syntax: {}\n", syntax.diagnostics.len()));
    out.push_str(&format!("  program: {}\n", program.diagnostics.len()));
    for diagnostic in syntax.diagnostics.iter().chain(program.diagnostics.iter()) {
        out.push_str(&format!(
            "  - {:?}: {}\n",
            diagnostic.severity, diagnostic.message
        ));
    }

    out
}
