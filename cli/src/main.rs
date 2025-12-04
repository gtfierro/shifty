use clap::{Parser, ValueEnum};
use graphviz_rust::cmd::{CommandArg, Format};
use graphviz_rust::exec_dot;
use oxigraph::io::{RdfFormat, RdfSerializer};
use oxigraph::model::{Quad, Term, TripleRef};
use serde_json::json;
use shacl::trace::TraceEvent;
use shacl::{InferenceConfig, Source, Validator, ValidatorBuilder};
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
#[clap(group(
    clap::ArgGroup::new("shapes_source")
        .required(true)
        .args(&["shapes_file", "shapes_graph"]),
))]
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

#[derive(Parser, Debug)]
struct CommonArgs {
    #[clap(flatten)]
    shapes: ShapesSourceCli,
    #[clap(flatten)]
    data: DataSourceCli,

    /// Skip invalid SHACL constructs (log and continue)
    #[arg(long)]
    skip_invalid_rules: bool,
}

#[derive(Parser)]
struct GraphvizArgs {
    #[clap(flatten)]
    common: CommonArgs,
}

#[derive(Parser)]
struct PdfArgs {
    #[clap(flatten)]
    common: CommonArgs,

    /// Path to the output PDF file
    #[arg(short, long, value_name = "FILE")]
    output_file: PathBuf,
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
struct ValidateArgs {
    #[clap(flatten)]
    common: CommonArgs,
    #[clap(flatten)]
    trace: TraceOutputArgs,

    /// The output format for the validation report
    #[arg(long, value_enum, default_value_t = ValidateOutputFormat::Turtle)]
    format: ValidateOutputFormat,

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
struct InferenceArgs {
    #[clap(flatten)]
    common: CommonArgs,
    #[clap(flatten)]
    trace: TraceOutputArgs,

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
struct GraphvizHeatmapArgs {
    #[clap(flatten)]
    common: CommonArgs,

    /// Include all shapes and components, even those not executed
    #[arg(long)]
    all: bool,
}

#[derive(Parser)]
struct PdfHeatmapArgs {
    #[clap(flatten)]
    common: CommonArgs,

    /// Path to the output PDF file
    #[arg(short, long, value_name = "FILE")]
    output_file: PathBuf,

    /// Include all shapes and components, even those not executed
    #[arg(long)]
    all: bool,
}

#[derive(Parser)]
struct TraceCmdArgs {
    #[clap(flatten)]
    common: CommonArgs,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Output the Graphviz DOT string of the shape graph
    Graphviz(GraphvizArgs),
    /// Generate a PDF of the shape graph using Graphviz
    Pdf(PdfArgs),
    /// Validate the data against the shapes and output a frequency table of component invocations
    Heat(HeatArgs),
    /// Validate the data and output a graphviz heatmap of the shape graph
    #[command(name = "graphviz-heatmap")]
    GraphvizHeatmap(GraphvizHeatmapArgs),
    /// Generate a PDF of the shape graph heatmap using Graphviz
    #[command(name = "pdf-heatmap")]
    PdfHeatmap(PdfHeatmapArgs),
    /// Validate the data against the shapes
    Validate(ValidateArgs),
    /// Run SHACL rule inference without performing validation
    Inference(InferenceArgs),
    /// Print the execution traces for debugging
    Trace(TraceCmdArgs),
}

fn get_validator(common: &CommonArgs) -> Result<Validator, Box<dyn std::error::Error>> {
    let shapes_source = if let Some(path) = &common.shapes.shapes_file {
        Source::File(path.clone())
    } else {
        Source::Graph(common.shapes.shapes_graph.clone().unwrap())
    };

    let data_source = if let Some(path) = &common.data.data_file {
        Source::File(path.clone())
    } else {
        Source::Graph(common.data.data_graph.clone().unwrap())
    };

    ValidatorBuilder::new()
        .with_shapes_source(shapes_source)
        .with_data_source(data_source)
        .with_skip_invalid_rules(common.skip_invalid_rules)
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
        eprintln!("--- trace events ({} entries) ---", events.len());
        for ev in events {
            eprintln!("{:?}", ev);
        }
    }

    if let Some(path) = args.trace_file.as_ref() {
        let mut lines = String::new();
        for ev in events {
            lines.push_str(&format!("{:?}\n", ev));
        }
        fs::write(path, lines)
            .map_err(|e| format!("Failed to write trace file {}: {}", path.display(), e))?;
        eprintln!("Wrote trace events to {}", path.display());
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
        eprintln!("Wrote trace events (JSONL) to {}", path.display());
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Graphviz(args) => {
            let validator = get_validator(&args.common)?;
            let dot_string = validator.to_graphviz()?;
            println!("{}", dot_string);
        }
        Commands::Pdf(args) => {
            let validator = get_validator(&args.common)?;
            let dot_string = validator.to_graphviz()?;

            let output_format = Format::Pdf;
            let output_file_path_str = args
                .output_file
                .to_str()
                .ok_or("Invalid output file path")?;

            let cmd_args = vec![
                CommandArg::Format(output_format),
                CommandArg::Output(output_file_path_str.to_string()),
            ];

            exec_dot(dot_string, cmd_args)
                .map_err(|e| format!("Graphviz execution error: {}", e))?;

            println!("PDF generated at: {}", args.output_file.display());
        }
        Commands::Validate(args) => {
            let validator = get_validator(&args.common)?;
            let trace_buf = validator.context().trace_events();
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
                eprintln!(
                    "Inference added {} triple(s) in {} iteration(s); converged={}",
                    outcome.triples_added, outcome.iterations_executed, outcome.converged
                );
            }

            match args.format {
                ValidateOutputFormat::Turtle => {
                    let report_str = report.to_turtle()?;
                    println!("{}", report_str);
                }
                ValidateOutputFormat::Dump => {
                    report.dump();
                }
                ValidateOutputFormat::RdfXml => {
                    let report_str = report.to_rdf(RdfFormat::RdfXml)?;
                    println!("{}", report_str);
                }
                ValidateOutputFormat::NTriples => {
                    let report_str = report.to_rdf(RdfFormat::NTriples)?;
                    println!("{}", report_str);
                }
            }

            if args.graphviz {
                let dot_string = validator.to_graphviz()?;
                println!("{}", dot_string);
            }

            if let Some(pdf_path) = args.pdf_heatmap.as_ref() {
                let dot_string = validator.to_graphviz_heatmap(args.pdf_heatmap_all)?;
                let output_file_path_str = pdf_path
                    .to_str()
                    .ok_or("Invalid heatmap output file path")?;
                let cmd_args = vec![
                    CommandArg::Format(Format::Pdf),
                    CommandArg::Output(output_file_path_str.to_string()),
                ];
                exec_dot(dot_string, cmd_args)
                    .map_err(|e| format!("Graphviz execution error: {}", e))?;
                println!("PDF heatmap generated at: {}", pdf_path.display());
            }

            if args.trace.requested() {
                let events = trace_buf
                    .lock()
                    .ok()
                    .map(|guard| guard.clone())
                    .unwrap_or_default();
                emit_trace_outputs(&events, &args.trace).map_err(io::Error::other)?;
            }
        }
        Commands::Inference(args) => {
            let validator = get_validator(&args.common)?;
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
            eprintln!(
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
                eprintln!(
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
                // Need a validation pass to populate execution traces for the heatmap
                let _report = validator.validate();
                let dot_string = validator.to_graphviz_heatmap(args.pdf_heatmap_all)?;
                let output_file_path_str = pdf_path
                    .to_str()
                    .ok_or("Invalid heatmap output file path")?;
                let cmd_args = vec![
                    CommandArg::Format(Format::Pdf),
                    CommandArg::Output(output_file_path_str.to_string()),
                ];
                exec_dot(dot_string, cmd_args)
                    .map_err(|e| format!("Graphviz execution error: {}", e))?;
                println!("PDF heatmap generated at: {}", pdf_path.display());
            }

            // Emit trace events if requested.
            if args.trace.requested() {
                let trace_buf = validator.context().trace_events();
                let events = trace_buf
                    .lock()
                    .ok()
                    .map(|guard| guard.clone())
                    .unwrap_or_default();
                emit_trace_outputs(&events, &args.trace).map_err(io::Error::other)?;
            }
        }
        Commands::Heat(args) => {
            let validator = get_validator(&args.common)?;
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
        Commands::GraphvizHeatmap(args) => {
            let validator = get_validator(&args.common)?;
            // Run validation first to populate execution traces used by graphviz_heatmap.
            // include_all_nodes == args.all: when true, include shapes/components that did not execute.
            let _report = validator.validate();

            let dot_string = validator.to_graphviz_heatmap(args.all)?;
            println!("{}", dot_string);
        }
        Commands::PdfHeatmap(args) => {
            let validator = get_validator(&args.common)?;
            // Run validation first to populate execution traces used by graphviz_heatmap.
            // include_all_nodes == args.all: when true, include shapes/components that did not execute.
            let _report = validator.validate();

            let dot_string = validator.to_graphviz_heatmap(args.all)?;

            let output_format = Format::Pdf;
            let output_file_path_str = args
                .output_file
                .to_str()
                .ok_or("Invalid output file path")?;

            let cmd_args = vec![
                CommandArg::Format(output_format),
                CommandArg::Output(output_file_path_str.to_string()),
            ];

            exec_dot(dot_string, cmd_args)
                .map_err(|e| format!("Graphviz execution error: {}", e))?;

            println!("PDF heatmap generated at: {}", args.output_file.display());
        }
        Commands::Trace(args) => {
            let validator = get_validator(&args.common)?;
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
