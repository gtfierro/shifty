use clap::{ArgAction, Parser, ValueEnum};
use graphviz_rust::cmd::{CommandArg, Format};
use graphviz_rust::exec_dot;
use log::{info, LevelFilter};
use oxigraph::io::{RdfFormat, RdfSerializer};
use oxigraph::model::{Quad, Term, TripleRef};
use serde_json::json;
use shifty::ir_cache;
use shifty::trace::TraceEvent;
use shifty::{InferenceConfig, Source, ValidationReportOptions, Validator, ValidatorBuilder};
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

fn try_read_shape_ir_from_path(path: &Path) -> Option<Result<shacl_ir::ShapeIR, String>> {
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

    /// Disable resolving owl:imports for the shapes graph while generating the IR
    #[arg(long)]
    no_imports: bool,

    /// Maximum owl:imports recursion depth for shapes (-1 = unlimited, 0 = only the root graph)
    #[arg(long, default_value_t = -1)]
    import_depth: i32,

    /// Use a temporary OntoEnv workspace (set to false to reuse a local store if present)
    #[arg(long, default_value_t = false, value_parser = clap::value_parser!(bool))]
    temporary: bool,

    /// Output path for the serialized SHACL-IR file
    #[arg(short, long, value_name = "FILE")]
    output_file: PathBuf,
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

    /// Maximum owl:imports recursion depth (-1 = unlimited, 0 = only the root graph)
    #[arg(long, default_value_t = -1)]
    import_depth: i32,

    /// Use a temporary OntoEnv workspace (set to false to reuse a local store if present)
    #[arg(long, default_value_t = false, value_parser = clap::value_parser!(bool))]
    temporary: bool,
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

    /// Run SHACL rule inference before validation
    #[arg(long)]
    run_inference: bool,

    /// Minimum iterations for inference (requires --run-inference)
    #[arg(long, requires = "run_inference")]
    inference_min_iterations: Option<usize>,

    /// Maximum iterations for inference (requires --run-inference)
    #[arg(long, requires = "run_inference")]
    inference_max_iterations: Option<usize>,

    /// Disable convergence-based early exit (requires --run-inference)
    #[arg(long, requires = "run_inference")]
    inference_no_converge: bool,

    /// Fail if inference produces blank nodes (requires --run-inference)
    #[arg(long, requires = "run_inference")]
    inference_error_on_blank_nodes: bool,

    /// Enable verbose inference logging (requires --run-inference)
    #[arg(long, requires = "run_inference")]
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
        .with_do_imports(!common.no_imports)
        .with_temporary_env(common.temporary)
        .with_import_depth(common.import_depth)
        .with_shapes_data_union(!common.no_union_graphs);

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
    shapes: &ShapesSourceCli,
    skip_invalid_rules: bool,
    warnings_are_errors: bool,
    do_imports: bool,
    import_depth: i32,
    temporary: bool,
) -> Result<Validator, Box<dyn std::error::Error>> {
    let mut builder = ValidatorBuilder::new();
    if let Some(path) = &shapes.shapes_file {
        if let Some(shape_ir) = try_read_shape_ir_from_path(path) {
            builder = builder.with_shape_ir(shape_ir?);
        } else {
            builder = builder.with_shapes_source(Source::File(path.clone()));
        }
    } else if let Some(graph) = &shapes.shapes_graph {
        builder = builder.with_shapes_source(Source::Graph(graph.clone()));
    } else {
        return Err("shapes input must be provided via --shapes-file or --shapes-graph".into());
    }

    builder
        .with_data_source(Source::Empty)
        .with_skip_invalid_rules(skip_invalid_rules)
        .with_warnings_are_errors(warnings_are_errors)
        .with_do_imports(do_imports)
        .with_temporary_env(temporary)
        .with_import_depth(import_depth)
        .with_shapes_data_union(true)
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
            let validator = get_validator_shapes_only(
                &args.shapes,
                args.skip_invalid_rules,
                args.warnings_are_errors,
                !args.no_imports,
                args.import_depth,
                args.temporary,
            )?;
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
