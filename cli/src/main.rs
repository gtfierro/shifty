use clap::{ArgAction, Parser, ValueEnum};
use graphviz_rust::cmd::{CommandArg, Format};
use graphviz_rust::exec_dot;
use log::{info, LevelFilter};
use oxigraph::io::{RdfFormat, RdfSerializer};
#[cfg(feature = "shacl-compiler")]
use oxigraph::model::{Graph, Triple};
use oxigraph::model::{Quad, Term, TripleRef};
use serde_json::json;
#[cfg(feature = "shacl-compiler")]
use shacl_compiler::{generate_rust_modules_from_plan, PlanIR};
use shifty::ir_cache;
#[cfg(feature = "shacl-compiler")]
use shifty::shacl_ir::ShapeIR;
use shifty::trace::TraceEvent;
use shifty::{InferenceConfig, Source, ValidationReportOptions, Validator, ValidatorBuilder};
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
#[cfg(feature = "shacl-compiler")]
use std::process::Command;

fn try_read_shape_ir_from_path(path: &Path) -> Option<Result<shifty::shacl_ir::ShapeIR, String>> {
    let is_ir = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("ir"))
        .unwrap_or(false);
    if !is_ir {
        return None;
    }

    Some(
        ir_cache::read_shape_ir(path)
            .map_err(|e| format!("Failed to read SHACL-IR cache {}: {}", path.display(), e)),
    )
}
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Set the base log level (use -v / -q to adjust relative to this level)
    #[arg(
        long,
        value_enum,
        default_value_t = LogLevel::Info,
        global = true,
        help = "error | warn | info | debug | trace"
    )]
    log_level: LogLevel,

    /// Increase logging verbosity (can be used multiple times)
    #[arg(short, long, action = ArgAction::Count, global = true)]
    verbose: u8,

    /// Decrease logging verbosity (can be used multiple times)
    #[arg(short, long, action = ArgAction::Count, global = true)]
    quiet: u8,

    #[command(subcommand)]
    command: Commands,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    fn to_filter(self) -> LevelFilter {
        match self {
            LogLevel::Error => LevelFilter::Error,
            LogLevel::Warn => LevelFilter::Warn,
            LogLevel::Info => LevelFilter::Info,
            LogLevel::Debug => LevelFilter::Debug,
            LogLevel::Trace => LevelFilter::Trace,
        }
    }
}

fn init_logging(base: LogLevel, verbose: u8, quiet: u8) {
    let levels = [
        LevelFilter::Error,
        LevelFilter::Warn,
        LevelFilter::Info,
        LevelFilter::Debug,
        LevelFilter::Trace,
    ];

    let base_idx = levels
        .iter()
        .position(|lvl| *lvl == base.to_filter())
        .unwrap_or(2) as i8; // default to Info
    let adjusted =
        (base_idx + verbose as i8 - quiet as i8).clamp(0, (levels.len() - 1) as i8) as usize;

    env_logger::Builder::from_default_env()
        .format_target(false)
        .filter_level(levels[adjusted])
        .init();
}

#[derive(Parser, Debug)]
struct ShapesSourceCli {
    /// Path to the shapes file
    #[arg(short, long, value_name = "FILE")]
    shapes_file: Option<PathBuf>,

    /// URI of the shapes graph
    #[arg(long, value_name = "URI")]
    shapes_graph: Option<String>,
}

#[derive(Parser, Debug)]
#[clap(group(
    clap::ArgGroup::new("data_source")
        .required(true)
        .args(&["data_file", "data_graph"]),
))]
struct DataSourceCli {
    /// Path to the data file
    #[arg(short, long, value_name = "FILE")]
    data_file: Option<PathBuf>,

    /// URI of the data graph
    #[arg(long, value_name = "URI")]
    data_graph: Option<String>,
}

#[derive(Parser)]
struct GenerateIrArgs {
    #[clap(flatten)]
    shapes: ShapesSourceCli,

    /// Skip invalid SHACL constructs when generating the IR
    #[arg(long)]
    skip_invalid_rules: bool,

    /// Treat SHACL warnings as errors when generating the IR
    #[arg(long)]
    warnings_are_errors: bool,

    /// Require custom constraint components to declare validators
    #[arg(long)]
    strict_custom_constraints: bool,

    /// Disable resolving owl:imports for the shapes graph while generating the IR
    #[arg(long)]
    no_imports: bool,

    /// Maximum owl:imports recursion depth for shapes (-1 = unlimited, 0 = only the root graph)
    #[arg(long, default_value_t = -1)]
    import_depth: i32,

    /// Use a temporary OntoEnv workspace (set to false to reuse a local store if present)
    #[arg(long, default_value_t = false, value_parser = clap::value_parser!(bool))]
    temporary: bool,

    /// Disable store optimization when building the SHACL-IR
    #[arg(long)]
    no_store_optimize: bool,

    /// Output path for the serialized SHACL-IR file
    #[arg(short, long, value_name = "FILE")]
    output_file: PathBuf,
}

#[cfg(feature = "shacl-compiler")]
#[derive(Parser)]
struct CompileArgs {
    #[clap(flatten)]
    shapes: ShapesSourceCli,

    /// Skip invalid SHACL constructs when generating the plan
    #[arg(long)]
    skip_invalid_rules: bool,

    /// Treat SHACL warnings as errors when generating the plan
    #[arg(long)]
    warnings_are_errors: bool,

    /// Require custom constraint components to declare validators
    #[arg(long)]
    strict_custom_constraints: bool,

    /// Disable resolving owl:imports for the shapes graph while generating the plan
    #[arg(long)]
    no_imports: bool,

    /// Maximum owl:imports recursion depth for shapes (-1 = unlimited, 0 = only the root graph)
    #[arg(long, default_value_t = -1)]
    import_depth: i32,

    /// Use a temporary OntoEnv workspace (set to false to reuse a local store if present)
    #[arg(long, default_value_t = false, value_parser = clap::value_parser!(bool))]
    temporary: bool,

    /// Disable store optimization when building the plan
    #[arg(long)]
    no_store_optimize: bool,

    /// Output directory for the generated crate and binary
    #[arg(long, value_name = "DIR", default_value = "target/compiled-shacl")]
    out_dir: PathBuf,

    /// Name for the generated crate/binary
    #[arg(long, value_name = "NAME", default_value = "shacl-compiled")]
    bin_name: String,

    /// Optional output path for PlanIR JSON
    #[arg(long, value_name = "FILE")]
    plan_out: Option<PathBuf>,

    /// Build in release mode
    #[arg(long)]
    release: bool,

    /// Use a local path for the shifty dependency in the generated Cargo.toml (non-portable)
    #[arg(long, value_name = "DIR")]
    shifty_path: Option<PathBuf>,

    /// Git URL for the shifty dependency in the generated Cargo.toml (default: this repo)
    #[arg(long, value_name = "URL")]
    shifty_git: Option<String>,

    /// Git revision/tag/branch for the shifty dependency (optional)
    #[arg(long, value_name = "REF")]
    shifty_git_ref: Option<String>,
}

#[derive(Parser, Debug, Clone, Default)]
struct ShaclIrArgs {
    /// Path to a serialized SHACL-IR artifact (optional)
    #[arg(long, value_name = "FILE")]
    shacl_ir: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct CommonArgs {
    #[clap(flatten)]
    shapes: ShapesSourceCli,
    #[clap(flatten)]
    data: DataSourceCli,

    /// Skip invalid SHACL constructs (log and continue)
    #[arg(long)]
    skip_invalid_rules: bool,

    /// Treat SHACL warnings as errors (default: false)
    #[arg(long)]
    warnings_are_errors: bool,

    /// Disable resolving owl:imports for shapes/data graphs (default: do imports)
    #[arg(long)]
    no_imports: bool,

    /// Disable using the union of the shapes and data graphs for validation/inference
    #[arg(long)]
    no_union_graphs: bool,

    /// Allow SPARQL constraints to run when the focus node is a blank node
    #[arg(long)]
    allow_sparql_blank_targets: bool,

    /// Require custom constraint components to declare validators (default: allow missing)
    #[arg(long)]
    strict_custom_constraints: bool,

    /// Maximum owl:imports recursion depth (-1 = unlimited, 0 = only the root graph)
    #[arg(long, default_value_t = -1)]
    import_depth: i32,

    /// Use a temporary OntoEnv workspace (set to false to reuse a local store if present)
    #[arg(long, default_value_t = false, value_parser = clap::value_parser!(bool))]
    temporary: bool,

    /// Disable optimizing the store before validation/inference
    #[arg(long)]
    no_store_optimize: bool,
}

#[derive(Parser)]
struct VisualizeArgs {
    #[clap(flatten)]
    common: CommonArgs,

    /// Emit the Graphviz DOT description (default behavior when --pdf is not provided)
    #[arg(long, conflicts_with = "pdf")]
    graphviz: bool,

    /// Path to the output PDF file (mutually exclusive with --graphviz)
    #[arg(long, value_name = "FILE", conflicts_with = "graphviz")]
    pdf: Option<PathBuf>,
}

#[derive(ValueEnum, Clone, Debug, Default)]
enum ValidateOutputFormat {
    #[default]
    Turtle,
    Dump,
    RdfXml,
    NTriples,
}

#[derive(Parser)]
#[clap(group(
    clap::ArgGroup::new("shapes_input")
        .required(true)
        .args(&["shapes_file", "shapes_graph", "shacl_ir"]),
))]
struct ValidateArgs {
    #[clap(flatten)]
    common: CommonArgs,
    #[clap(flatten)]
    trace: TraceOutputArgs,
    #[clap(flatten)]
    shacl_ir: ShaclIrArgs,

    /// The output format for the validation report
    #[arg(long, value_enum, default_value_t = ValidateOutputFormat::Turtle)]
    format: ValidateOutputFormat,

    /// Follow blank nodes referenced in the report and include their CBD
    #[arg(long)]
    follow_bnodes: bool,

    /// Run SHACL rule inference before validation (default: true, set false via --run-inference=false)
    #[arg(long, default_value_t = true, value_parser = clap::value_parser!(bool))]
    run_inference: bool,

    /// Minimum iterations for inference
    #[arg(long)]
    inference_min_iterations: Option<usize>,

    /// Maximum iterations for inference
    #[arg(long)]
    inference_max_iterations: Option<usize>,

    /// Disable convergence-based early exit
    #[arg(long)]
    inference_no_converge: bool,

    /// Fail if inference produces blank nodes
    #[arg(long)]
    inference_error_on_blank_nodes: bool,

    /// Enable verbose inference logging
    #[arg(long)]
    inference_debug: bool,

    /// Print the Graphviz DOT for the shape graph after validation
    #[arg(long)]
    graphviz: bool,

    /// Write a PDF heatmap for the executed shapes
    #[arg(long, value_name = "FILE")]
    pdf_heatmap: Option<PathBuf>,

    /// Include shapes/components that did not execute in the heatmap (requires --pdf-heatmap)
    #[arg(long, requires = "pdf_heatmap")]
    pdf_heatmap_all: bool,

    /// Print per-component graph call counts captured during validation
    #[arg(long)]
    component_graph_calls: bool,
}

#[derive(Parser)]
#[clap(group(
    clap::ArgGroup::new("shapes_input")
        .required(true)
        .args(&["shapes_file", "shapes_graph", "shacl_ir"]),
))]
struct InferenceArgs {
    #[clap(flatten)]
    common: CommonArgs,
    #[clap(flatten)]
    trace: TraceOutputArgs,
    #[clap(flatten)]
    shacl_ir: ShaclIrArgs,

    /// Minimum iterations for inference
    #[arg(long)]
    min_iterations: Option<usize>,

    /// Maximum iterations for inference
    #[arg(long)]
    max_iterations: Option<usize>,

    /// Disable convergence-based early exit
    #[arg(long)]
    no_converge: bool,

    /// Fail if inference produces blank nodes
    #[arg(long)]
    error_on_blank_nodes: bool,

    /// Enable verbose inference logging
    #[arg(long)]
    debug: bool,

    /// Path to write the inferred triples as Turtle
    #[arg(long, value_name = "FILE")]
    output_file: Option<PathBuf>,

    /// Output the union of the original data graph with inferred triples
    #[arg(long)]
    union: bool,

    /// Print the Graphviz DOT for the shape graph after inference
    #[arg(long)]
    graphviz: bool,

    /// Write a PDF heatmap for the executed shapes
    #[arg(long, value_name = "FILE")]
    pdf_heatmap: Option<PathBuf>,

    /// Include shapes/components that did not execute in the heatmap (requires --pdf-heatmap)
    #[arg(long, requires = "pdf_heatmap")]
    pdf_heatmap_all: bool,
}

#[derive(Parser)]
struct HeatArgs {
    #[clap(flatten)]
    common: CommonArgs,
}

#[derive(Parser)]
struct VisualizeHeatmapArgs {
    #[clap(flatten)]
    common: CommonArgs,

    /// Include all shapes and components, even those not executed
    #[arg(long)]
    all: bool,

    /// Emit the Graphviz DOT description (default behavior when --pdf is not provided)
    #[arg(long, conflicts_with = "pdf")]
    graphviz: bool,

    /// Path to the output PDF file (mutually exclusive with --graphviz)
    #[arg(long, value_name = "FILE", conflicts_with = "graphviz")]
    pdf: Option<PathBuf>,
}

#[derive(Parser)]
struct TraceCmdArgs {
    #[clap(flatten)]
    common: CommonArgs,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Output the Graphviz DOT string of the shape graph (or write it as a PDF via --pdf)
    Visualize(VisualizeArgs),
    /// Validate the data against the shapes and output a frequency table of component invocations
    Heat(HeatArgs),
    /// Validate the data and output a graphviz heatmap of the shape graph (or write it as a PDF via --pdf)
    #[command(name = "visualize-heatmap")]
    VisualizeHeatmap(VisualizeHeatmapArgs),
    /// Generate a serialized SHACL-IR artifact for reuse
    GenerateIr(GenerateIrArgs),
    /// Generate + compile a specialized SHACL executable for the given shapes
    #[cfg(feature = "shacl-compiler")]
    Compile(CompileArgs),
    /// Validate the data against the shapes
    Validate(ValidateArgs),
    /// Run SHACL rule inference without performing validation
    Infer(InferenceArgs),
    /// Print the execution traces for debugging
    Trace(TraceCmdArgs),
}

fn get_validator(
    common: &CommonArgs,
    shacl_ir_path: Option<&PathBuf>,
) -> Result<Validator, Box<dyn std::error::Error>> {
    let data_source = if let Some(path) = &common.data.data_file {
        Source::File(path.clone())
    } else {
        Source::Graph(common.data.data_graph.clone().unwrap())
    };

    let mut builder = ValidatorBuilder::new()
        .with_data_source(data_source)
        .with_skip_invalid_rules(common.skip_invalid_rules)
        .with_warnings_are_errors(common.warnings_are_errors)
        .with_skip_sparql_blank_targets(!common.allow_sparql_blank_targets)
        .with_strict_custom_constraints(common.strict_custom_constraints)
        .with_do_imports(!common.no_imports)
        .with_temporary_env(common.temporary)
        .with_import_depth(common.import_depth)
        .with_shapes_data_union(!common.no_union_graphs)
        .with_store_optimization(!common.no_store_optimize);

    if let Some(path) = shacl_ir_path {
        let shape_ir = ir_cache::read_shape_ir(path)
            .map_err(|e| format!("Failed to read SHACL-IR cache: {}", e))?;
        builder = builder.with_shape_ir(shape_ir);
    } else if let Some(path) = &common.shapes.shapes_file {
        if let Some(shape_ir) = try_read_shape_ir_from_path(path) {
            builder = builder.with_shape_ir(shape_ir?);
        } else {
            builder = builder.with_shapes_source(Source::File(path.clone()));
        }
    } else if let Some(graph) = &common.shapes.shapes_graph {
        builder = builder.with_shapes_source(Source::Graph(graph.clone()));
    } else {
        return Err(
            "shapes input must be provided via --shapes-file, --shapes-graph, or --shacl-ir".into(),
        );
    }

    builder
        .build()
        .map_err(|e| format!("Error creating validator: {}", e).into())
}

fn get_validator_shapes_only(
    args: &GenerateIrArgs,
) -> Result<Validator, Box<dyn std::error::Error>> {
    let mut builder = ValidatorBuilder::new();
    if let Some(path) = &args.shapes.shapes_file {
        if let Some(shape_ir) = try_read_shape_ir_from_path(path) {
            builder = builder.with_shape_ir(shape_ir?);
        } else {
            builder = builder.with_shapes_source(Source::File(path.clone()));
        }
    } else if let Some(graph) = &args.shapes.shapes_graph {
        builder = builder.with_shapes_source(Source::Graph(graph.clone()));
    } else {
        return Err("shapes input must be provided via --shapes-file or --shapes-graph".into());
    }

    builder
        .with_data_source(Source::Empty)
        .with_skip_invalid_rules(args.skip_invalid_rules)
        .with_warnings_are_errors(args.warnings_are_errors)
        .with_strict_custom_constraints(args.strict_custom_constraints)
        .with_do_imports(!args.no_imports)
        .with_temporary_env(args.temporary)
        .with_import_depth(args.import_depth)
        .with_store_optimization(!args.no_store_optimize)
        .with_shapes_data_union(true)
        // Generate SHACL-IR with shape-only optimization, but disable data-dependent
        // target pruning because generate-ir uses Source::Empty.
        .with_shape_optimization(true)
        .with_data_dependent_shape_optimization(false)
        .build()
        .map_err(|e| format!("Error creating validator: {}", e).into())
}

#[cfg(feature = "shacl-compiler")]
fn get_validator_shapes_only_for_compile(
    args: &CompileArgs,
) -> Result<Validator, Box<dyn std::error::Error>> {
    if args.no_imports {
        info!(
            "Ignoring --no-imports for compile; compiled binaries always embed the full shapes imports closure"
        );
    }

    let mut builder = ValidatorBuilder::new();
    if let Some(path) = &args.shapes.shapes_file {
        if let Some(shape_ir) = try_read_shape_ir_from_path(path) {
            builder = builder.with_shape_ir(shape_ir?);
        } else {
            builder = builder.with_shapes_source(Source::File(path.clone()));
        }
    } else if let Some(graph) = &args.shapes.shapes_graph {
        builder = builder.with_shapes_source(Source::Graph(graph.clone()));
    } else {
        return Err("shapes input must be provided via --shapes-file or --shapes-graph".into());
    }

    builder
        .with_data_source(Source::Empty)
        .with_skip_invalid_rules(args.skip_invalid_rules)
        .with_warnings_are_errors(args.warnings_are_errors)
        .with_strict_custom_constraints(args.strict_custom_constraints)
        .with_do_imports(true)
        .with_temporary_env(args.temporary)
        .with_import_depth(args.import_depth)
        .with_store_optimization(!args.no_store_optimize)
        .with_shapes_data_union(true)
        .with_shape_optimization(true)
        .with_data_dependent_shape_optimization(false)
        .build()
        .map_err(|e| format!("Error creating validator: {}", e).into())
}

fn build_inference_config(
    min_iterations: Option<usize>,
    max_iterations: Option<usize>,
    no_converge: bool,
    error_on_blank_nodes: bool,
    trace: bool,
) -> InferenceConfig {
    let mut config = InferenceConfig::default();
    if let Some(min) = min_iterations {
        config.min_iterations = min;
    }
    if let Some(max) = max_iterations {
        config.max_iterations = max;
    }
    if no_converge {
        config.run_until_converged = false;
    }
    config.error_on_blank_nodes = error_on_blank_nodes;
    config.trace = trace;
    config
}

fn serialize_quads_to_turtle(quads: &[Quad]) -> Result<Vec<u8>, String> {
    let mut serializer = RdfSerializer::from_format(RdfFormat::Turtle).for_writer(Vec::new());
    for quad in quads {
        let triple_ref = TripleRef::new(
            quad.subject.as_ref(),
            quad.predicate.as_ref(),
            quad.object.as_ref(),
        );
        serializer
            .serialize_triple(triple_ref)
            .map_err(|e| format!("Failed to serialize triple: {}", e))?;
    }
    serializer
        .finish()
        .map_err(|e| format!("Failed to finish Turtle serialization: {}", e))
}

fn term_to_string(term: &Term) -> String {
    term.to_string()
}

fn trace_event_to_json(event: &TraceEvent) -> serde_json::Value {
    match event {
        TraceEvent::EnterNodeShape(id) => json!({
            "type": "EnterNodeShape",
            "node_shape_id": id.0,
        }),
        TraceEvent::EnterPropertyShape(id) => json!({
            "type": "EnterPropertyShape",
            "property_shape_id": id.0,
        }),
        TraceEvent::ComponentPassed {
            component,
            focus,
            value,
        } => json!({
            "type": "ComponentPassed",
            "component_id": component.0,
            "focus": term_to_string(focus),
            "value": value.as_ref().map(term_to_string),
        }),
        TraceEvent::ComponentFailed {
            component,
            focus,
            value,
            message,
        } => json!({
            "type": "ComponentFailed",
            "component_id": component.0,
            "focus": term_to_string(focus),
            "value": value.as_ref().map(term_to_string),
            "message": message,
        }),
        TraceEvent::SparqlQuery { label } => json!({
            "type": "SparqlQuery",
            "label": label,
        }),
        TraceEvent::RuleApplied { rule, inserted } => json!({
            "type": "RuleApplied",
            "rule_id": rule.0,
            "inserted": inserted,
        }),
    }
}

fn emit_trace_outputs(events: &[TraceEvent], args: &TraceOutputArgs) -> Result<(), String> {
    if events.is_empty()
        || (!args.trace_events && args.trace_file.is_none() && args.trace_jsonl.is_none())
    {
        return Ok(());
    }

    if args.trace_events {
        info!("--- trace events ({} entries) ---", events.len());
        for ev in events {
            info!("{:?}", ev);
        }
    }

    if let Some(path) = args.trace_file.as_ref() {
        let mut lines = String::new();
        for ev in events {
            lines.push_str(&format!("{:?}\n", ev));
        }
        fs::write(path, lines)
            .map_err(|e| format!("Failed to write trace file {}: {}", path.display(), e))?;
        info!("Wrote trace events to {}", path.display());
    }

    if let Some(path) = args.trace_jsonl.as_ref() {
        let mut buf = String::new();
        for ev in events {
            let value = trace_event_to_json(ev);
            let line = serde_json::to_string(&value)
                .map_err(|e| format!("Failed to serialise trace event to JSON: {}", e))?;
            buf.push_str(&line);
            buf.push('\n');
        }
        fs::write(path, buf)
            .map_err(|e| format!("Failed to write trace jsonl {}: {}", path.display(), e))?;
        info!("Wrote trace events (JSONL) to {}", path.display());
    }

    Ok(())
}

fn run_validation_then_get_heatmap_dot(
    validator: &Validator,
    include_all_nodes: bool,
) -> Result<String, String> {
    let _report = validator.validate();
    validator.to_graphviz_heatmap(include_all_nodes)
}

fn write_pdf_from_dot(
    dot_string: String,
    output_file: &Path,
    description: &str,
) -> Result<(), String> {
    let output_file_path_str = output_file.to_str().ok_or("Invalid output file path")?;
    let cmd_args = vec![
        CommandArg::Format(Format::Pdf),
        CommandArg::Output(output_file_path_str.to_string()),
    ];

    exec_dot(dot_string, cmd_args).map_err(|e| format!("Graphviz execution error: {}", e))?;

    info!("{} generated at: {}", description, output_file.display());
    Ok(())
}

fn emit_validator_traces(validator: &Validator, args: &TraceOutputArgs) -> Result<(), String> {
    if !args.requested() {
        return Ok(());
    }

    let trace_buf = validator.context().trace_events();
    let events = trace_buf
        .lock()
        .ok()
        .map(|guard| guard.clone())
        .unwrap_or_default();

    emit_trace_outputs(&events, args).map_err(|e| format!("Failed to emit trace outputs: {}", e))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    init_logging(cli.log_level, cli.verbose, cli.quiet);

    match cli.command {
        Commands::Visualize(args) => {
            let validator = get_validator(&args.common, None)?;
            let dot_string = validator.to_graphviz()?;
            let wants_graphviz = args.graphviz || args.pdf.is_none();

            if wants_graphviz {
                println!("{}", dot_string);
            } else if let Some(pdf_path) = args.pdf {
                write_pdf_from_dot(dot_string, &pdf_path, "PDF")?;
            }
        }
        Commands::Validate(args) => {
            if !args.run_inference
                && (args.inference_min_iterations.is_some()
                    || args.inference_max_iterations.is_some()
                    || args.inference_no_converge
                    || args.inference_error_on_blank_nodes
                    || args.inference_debug)
            {
                return Err(
                    "inference tuning flags require --run-inference=true (default true)".into(),
                );
            }
            let validator = get_validator(&args.common, args.shacl_ir.shacl_ir.as_ref())?;
            let (report, inference_outcome) = if args.run_inference {
                let config = build_inference_config(
                    args.inference_min_iterations,
                    args.inference_max_iterations,
                    args.inference_no_converge,
                    args.inference_error_on_blank_nodes,
                    args.inference_debug,
                );
                match validator.validate_with_inference(config) {
                    Ok((outcome, report)) => (report, Some(outcome)),
                    Err(err) => return Err(format!("Inference failed: {}", err).into()),
                }
            } else {
                (validator.validate(), None)
            };

            if let Some(outcome) = inference_outcome {
                info!(
                    "Inference added {} triple(s) in {} iteration(s); converged={}",
                    outcome.triples_added, outcome.iterations_executed, outcome.converged
                );
            }

            let report_options = ValidationReportOptions {
                follow_bnodes: args.follow_bnodes,
            };

            match args.format {
                ValidateOutputFormat::Turtle => {
                    let report_str = report.to_turtle_with_options(report_options)?;
                    println!("{}", report_str);
                }
                ValidateOutputFormat::Dump => {
                    report.dump();
                }
                ValidateOutputFormat::RdfXml => {
                    let report_str =
                        report.to_rdf_with_options(RdfFormat::RdfXml, report_options)?;
                    println!("{}", report_str);
                }
                ValidateOutputFormat::NTriples => {
                    let report_str =
                        report.to_rdf_with_options(RdfFormat::NTriples, report_options)?;
                    println!("{}", report_str);
                }
            }

            if args.graphviz {
                let dot_string = validator.to_graphviz()?;
                println!("{}", dot_string);
            }

            if let Some(pdf_path) = args.pdf_heatmap.as_ref() {
                let dot_string = validator.to_graphviz_heatmap(args.pdf_heatmap_all)?;
                write_pdf_from_dot(dot_string, pdf_path, "PDF heatmap")?;
            }

            if args.component_graph_calls {
                println!(
                    "SourceShape\tComponentID\tComponent\tinvocations\tquads_for_pattern\texecute_prepared\ttotal_ms\tmin_ms\tmax_ms\tmean_ms\tstddev_ms"
                );
                for stat in validator.component_graph_call_stats() {
                    println!(
                        "{}\t{}\t{}\t{}\t{}\t{}\t{:.3}\t{:.3}\t{:.3}\t{:.3}\t{:.3}",
                        stat.source_shape,
                        stat.component_id,
                        stat.component_label,
                        stat.component_invocations,
                        stat.quads_for_pattern_calls,
                        stat.execute_prepared_calls,
                        stat.runtime_total_ms,
                        stat.runtime_min_ms,
                        stat.runtime_max_ms,
                        stat.runtime_mean_ms,
                        stat.runtime_stddev_ms
                    );
                }
                println!();
                println!(
                    "SourceShape\tPhase\tinvocations\ttotal_ms\tmin_ms\tmax_ms\tmean_ms\tstddev_ms"
                );
                for stat in validator.shape_phase_timing_stats() {
                    println!(
                        "{}\t{}\t{}\t{:.3}\t{:.3}\t{:.3}\t{:.3}\t{:.3}",
                        stat.source_shape,
                        stat.phase,
                        stat.invocations,
                        stat.runtime_total_ms,
                        stat.runtime_min_ms,
                        stat.runtime_max_ms,
                        stat.runtime_mean_ms,
                        stat.runtime_stddev_ms
                    );
                }
                println!();
                println!(
                    "SourceShape\tComponentID\tConstraint\tquery_hash\tinvocations\trows_total\trows_min\trows_max\trows_mean\ttotal_ms\tmin_ms\tmax_ms\tmean_ms\tstddev_ms"
                );
                for stat in validator.sparql_query_call_stats() {
                    let constraint = stat.constraint_term.replace(['\n', '\t'], " ");
                    println!(
                        "{}\t{}\t{}\t{:016x}\t{}\t{}\t{}\t{}\t{:.3}\t{:.3}\t{:.3}\t{:.3}\t{:.3}\t{:.3}",
                        stat.source_shape,
                        stat.component_id,
                        constraint,
                        stat.query_hash,
                        stat.invocations,
                        stat.rows_returned_total,
                        stat.rows_returned_min,
                        stat.rows_returned_max,
                        stat.rows_returned_mean,
                        stat.runtime_total_ms,
                        stat.runtime_min_ms,
                        stat.runtime_max_ms,
                        stat.runtime_mean_ms,
                        stat.runtime_stddev_ms
                    );
                }
            }

            emit_validator_traces(&validator, &args.trace)?;
        }
        Commands::Infer(args) => {
            let validator = get_validator(&args.common, args.shacl_ir.shacl_ir.as_ref())?;
            let config = build_inference_config(
                args.min_iterations,
                args.max_iterations,
                args.no_converge,
                args.error_on_blank_nodes,
                args.debug,
            );
            let outcome = validator
                .run_inference_with_config(config)
                .map_err(|e| format!("Inference failed: {}", e))?;
            info!(
                "Inference added {} triple(s) in {} iteration(s); converged={}",
                outcome.triples_added, outcome.iterations_executed, outcome.converged
            );

            let quads_to_emit = if args.union {
                validator
                    .data_graph_quads()
                    .map_err(|e| format!("Failed to read data graph: {}", e))?
            } else {
                outcome.inferred_quads
            };

            let turtle_bytes = serialize_quads_to_turtle(&quads_to_emit)?;

            if let Some(path) = args.output_file {
                fs::write(&path, &turtle_bytes)
                    .map_err(|e| format!("Failed to write {}: {}", path.display(), e))?;
                info!(
                    "Wrote {} triple(s) to {}",
                    quads_to_emit.len(),
                    path.display()
                );
            } else {
                io::stdout().write_all(&turtle_bytes)?;
            }

            if args.graphviz {
                let dot_string = validator.to_graphviz()?;
                println!("{}", dot_string);
            }

            if let Some(pdf_path) = args.pdf_heatmap.as_ref() {
                let dot_string =
                    run_validation_then_get_heatmap_dot(&validator, args.pdf_heatmap_all)?;
                write_pdf_from_dot(dot_string, pdf_path, "PDF heatmap")?;
            }

            emit_validator_traces(&validator, &args.trace)?;
        }
        #[cfg(feature = "shacl-compiler")]
        Commands::Compile(args) => {
            let validator = get_validator_shapes_only_for_compile(&args)?;
            let shapes_file_is_ir = args
                .shapes
                .shapes_file
                .as_ref()
                .and_then(|path| path.extension().and_then(|ext| ext.to_str()))
                .map(|ext| ext.eq_ignore_ascii_case("ir"))
                .unwrap_or(false);
            let shape_ir = if shapes_file_is_ir {
                validator.shape_ir().clone()
            } else {
                validator
                    .shape_ir_with_imports(args.import_depth)
                    .map_err(|e| e.to_string())?
            };
            let plan = PlanIR::from_shape_ir(&shape_ir).map_err(|e| e.to_string())?;
            let plan_json = plan
                .to_json_pretty()
                .map_err(|e| format!("plan serialization error: {}", e))?;
            let generated = generate_rust_modules_from_plan(&plan)?;
            let generated_root = generated.root;
            let generated_files = generated.files;

            if let Some(plan_out) = &args.plan_out {
                fs::write(plan_out, &plan_json)?;
            }

            info!("Using compile backend: shacl-compiler");

            let out_dir = &args.out_dir;
            let src_dir = out_dir.join("src");
            let generated_dir = src_dir.join("generated");
            fs::create_dir_all(&src_dir)?;
            fs::create_dir_all(&generated_dir)?;
            fs::write(generated_dir.join("mod.rs"), generated_root)?;
            for (name, content) in generated_files {
                fs::write(generated_dir.join(name), content)?;
            }
            let shape_ir_json = serde_json::to_string(&shape_ir)
                .map_err(|e| format!("Failed to serialize SHACL-IR: {}", e))?;
            fs::write(generated_dir.join("shape_ir.json"), shape_ir_json)?;
            write_shape_graph_ttl(&shape_ir, &out_dir.join("shape_graph.ttl"))?;

            let workspace_root = std::env::current_dir()?
                .canonicalize()
                .map_err(|e| format!("Failed to canonicalize current dir: {}", e))?;
            let shifty_dep = if let Some(path) = args.shifty_path.as_ref() {
                format!(
                    "shifty = {{ path = \"{}\", package = \"shifty-shacl\" }}",
                    path.display()
                )
            } else {
                let repo = args
                    .shifty_git
                    .clone()
                    .unwrap_or_else(|| env!("CARGO_PKG_REPOSITORY").to_string());
                if repo.is_empty() {
                    let shifty_path = workspace_root.join("lib");
                    format!(
                        "shifty = {{ path = \"{}\", package = \"shifty-shacl\" }}",
                        shifty_path.display()
                    )
                } else {
                    let inferred_ref = std::process::Command::new("git")
                        .args(["rev-parse", "HEAD"])
                        .current_dir(&workspace_root)
                        .output()
                        .ok()
                        .and_then(|output| {
                            if output.status.success() {
                                let rev =
                                    String::from_utf8_lossy(&output.stdout).trim().to_string();
                                if rev.is_empty() {
                                    None
                                } else {
                                    Some(rev)
                                }
                            } else {
                                None
                            }
                        });
                    let git_ref = args.shifty_git_ref.clone().or(inferred_ref);
                    let mut dep =
                        format!("shifty = {{ git = \"{}\", package = \"shifty-shacl\"", repo);
                    if let Some(git_ref) = git_ref {
                        dep.push_str(&format!(", rev = \"{}\"", git_ref));
                    }
                    dep.push_str(" }");
                    dep
                }
            };
            let cargo_toml = format!(
                "[workspace]\n\n[package]\nname = \"{}\"\nversion = \"0.0.1\"\nedition = \"2021\"\n\n[dependencies]\noxigraph = {{ version = \"0.5\" }}\nrayon = \"1\"\nregex = \"1\"\nserde_json = \"1\"\noxsdatatypes = \"0.2.2\"\nfixedbitset = \"0.5\"\ndashmap = \"6\"\n{}\nontoenv = \"0.5.0-a5\"\nlog = \"0.4\"\nenv_logger = \"0.11\"\n\n[profile.release]\ndebug = 2\nstrip = \"none\"\n\n[profile.bench]\ndebug = 2\nstrip = \"none\"\n",
                args.bin_name,
                shifty_dep,
            );
            fs::write(out_dir.join("Cargo.toml"), cargo_toml)?;

            let main_rs = include_str!("compiled_binary_main.rs.tmpl");
            fs::write(src_dir.join("main.rs"), main_rs.trim_start())?;

            let mut cmd = Command::new("cargo");
            cmd.arg("build");
            if args.release {
                cmd.arg("--release");
            }
            cmd.arg("--manifest-path").arg(out_dir.join("Cargo.toml"));
            let status = cmd.status()?;
            if !status.success() {
                return Err("failed to build compiled executable".into());
            }

            let target_dir = out_dir
                .join("target")
                .join(if args.release { "release" } else { "debug" })
                .join(&args.bin_name);
            println!("Built executable: {}", target_dir.display());
        }
        Commands::Heat(args) => {
            let validator = get_validator(&args.common, None)?;
            let report = validator.validate();

            let frequencies: HashMap<(String, String, String), usize> =
                report.get_component_frequencies();

            let mut sorted_frequencies: Vec<_> = frequencies.into_iter().collect();
            sorted_frequencies.sort_by(|a, b| b.1.cmp(&a.1));

            println!("ID\tLabel\tType\tInvocations");
            for ((id, label, item_type), count) in sorted_frequencies {
                println!("{}\t{}\t{}\t{}", id, label, item_type, count);
            }
        }
        Commands::VisualizeHeatmap(args) => {
            let validator = get_validator(&args.common, None)?;
            let dot_string = run_validation_then_get_heatmap_dot(&validator, args.all)?;
            let wants_graphviz = args.graphviz || args.pdf.is_none();

            if wants_graphviz {
                println!("{}", dot_string);
            } else if let Some(pdf_path) = args.pdf {
                write_pdf_from_dot(dot_string, &pdf_path, "PDF heatmap")?;
            }
        }
        Commands::GenerateIr(args) => {
            let validator = get_validator_shapes_only(&args)?;
            let mut shape_ir = validator.context().shape_ir().clone();
            shape_ir.data_graph = None;
            ir_cache::write_shape_ir(&args.output_file, &shape_ir)
                .map_err(|e| format!("Failed to write SHACL-IR cache: {}", e))?;
            println!("Wrote SHACL-IR cache to {}", args.output_file.display());
        }
        Commands::Trace(args) => {
            let validator = get_validator(&args.common, None)?;
            // Run validation to populate execution traces
            let report = validator.validate();

            report.print_traces();
        }
    }
    Ok(())
}
#[derive(Parser, Debug, Clone, Default)]
struct TraceOutputArgs {
    /// Print collected trace events to stderr after completion
    #[arg(long)]
    trace_events: bool,

    /// Write collected trace events to a file (one debug-formatted event per line)
    #[arg(long, value_name = "FILE")]
    trace_file: Option<PathBuf>,

    /// Write collected trace events as newline-delimited JSON
    #[arg(long, value_name = "FILE")]
    trace_jsonl: Option<PathBuf>,
}

impl TraceOutputArgs {
    fn requested(&self) -> bool {
        self.trace_events || self.trace_file.is_some() || self.trace_jsonl.is_some()
    }
}

#[cfg(feature = "shacl-compiler")]
fn write_shape_graph_ttl(
    shape_ir: &ShapeIR,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = Graph::new();
    for quad in &shape_ir.shape_quads {
        let triple = Triple::new(
            quad.subject.clone(),
            quad.predicate.clone(),
            quad.object.clone(),
        );
        graph.insert(&triple);
    }

    let mut writer = Vec::new();
    let mut serializer = RdfSerializer::from_format(RdfFormat::Turtle)
        .with_prefix("sh", "http://www.w3.org/ns/shacl#")?
        .with_prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")?
        .with_prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#")?
        .for_writer(&mut writer);
    for triple in graph.iter() {
        serializer.serialize_triple(triple)?;
    }
    serializer.finish()?;
    fs::write(path, &writer)?;
    Ok(())
}
