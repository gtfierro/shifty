use clap::{ArgAction, Parser, ValueEnum};
use graphviz_rust::cmd::{CommandArg, Format};
use graphviz_rust::exec_dot;
use log::{LevelFilter, info};
use oxigraph::io::{RdfFormat, RdfSerializer};
use oxigraph::model::{NamedOrBlankNode, Quad, Term, TripleRef};
use serde_json::json;
#[cfg(feature = "srcgen-compiler")]
use shacl_srcgen_compiler::{
    SrcGenBackend,
    generate_modules_from_ir_with_backend as generate_srcgen_modules_from_ir_with_backend,
    lower_shape_ir as lower_shape_ir_to_srcgen_ir,
};
use shifty::ir_cache;
use shifty::shacl_ir::{ComponentDescriptor, ShapeIR};
use shifty::trace::TraceEvent;
use shifty::{InferenceConfig, Source, ValidationReportOptions, Validator, ValidatorBuilder};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
#[cfg(feature = "srcgen-compiler")]
use std::process::Command;
use std::time::Instant;

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

    /// Write benchmark-oriented execution metadata as a JSON object to a file ("-" for stdout)
    #[arg(long, global = true, value_name = "FILE")]
    benchmark_json: Option<PathBuf>,

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
    /// Path to the shapes file (optional for data-bearing commands; defaults to the data graph when omitted)
    #[arg(short, long, value_name = "FILE")]
    shapes_file: Option<PathBuf>,

    /// URI of the shapes graph (optional for data-bearing commands; defaults to the data graph when omitted)
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

    /// Force-refresh ontology fetches instead of reusing cached copies
    #[arg(long)]
    force_refresh: bool,

    /// Maximum owl:imports recursion depth for shapes (-1 = unlimited, 0 = only the root graph)
    #[arg(long, default_value_t = -1)]
    import_depth: i32,

    /// Use a temporary OntoEnv workspace (set to false to reuse the local OntoEnv cache)
    #[arg(long, default_value_t = false, value_parser = clap::value_parser!(bool))]
    temporary: bool,

    /// Disable store optimization when building the SHACL-IR
    #[arg(long)]
    no_store_optimize: bool,

    /// Output path for the serialized SHACL-IR file
    #[arg(short, long, value_name = "FILE")]
    output_file: PathBuf,
}

#[cfg(feature = "srcgen-compiler")]
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

    /// Force-refresh ontology fetches instead of reusing cached copies
    #[arg(long)]
    force_refresh: bool,

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

    /// Optional output path for compiler IR JSON (SrcGenIR)
    #[arg(long, value_name = "FILE")]
    plan_out: Option<PathBuf>,

    /// Backend mode for srcgen compiler (`specialized` default, or `tables`)
    #[arg(long, value_enum)]
    backend: Option<CompileBackendArg>,

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

#[cfg(feature = "srcgen-compiler")]
#[derive(ValueEnum, Clone, Copy, Debug)]
enum CompileBackendArg {
    Specialized,
    Tables,
}

#[cfg(feature = "srcgen-compiler")]
enum ResolvedCompileBackend {
    Srcgen(SrcGenBackend),
}

#[cfg(feature = "srcgen-compiler")]
impl CompileArgs {
    fn resolve_backend(&self) -> Result<ResolvedCompileBackend, String> {
        match self.backend.unwrap_or(CompileBackendArg::Specialized) {
            CompileBackendArg::Specialized => {
                Ok(ResolvedCompileBackend::Srcgen(SrcGenBackend::Specialized))
            }
            CompileBackendArg::Tables => Ok(ResolvedCompileBackend::Srcgen(SrcGenBackend::Tables)),
        }
    }
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

    /// Force-refresh ontology fetches instead of reusing cached copies
    #[arg(long)]
    force_refresh: bool,

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
    #[cfg(feature = "srcgen-compiler")]
    Compile(CompileArgs),
    /// Validate the data against the shapes
    Validate(ValidateArgs),
    /// Run SHACL rule inference without performing validation
    Infer(InferenceArgs),
    /// Print the execution traces for debugging
    Trace(TraceCmdArgs),
}

impl Commands {
    fn name(&self) -> &'static str {
        match self {
            Commands::Visualize(_) => "visualize",
            Commands::Validate(_) => "validate",
            Commands::Infer(_) => "infer",
            #[cfg(feature = "srcgen-compiler")]
            Commands::Compile(_) => "compile",
            Commands::Heat(_) => "heat",
            Commands::VisualizeHeatmap(_) => "visualize-heatmap",
            Commands::GenerateIr(_) => "generate-ir",
            Commands::Trace(_) => "trace",
        }
    }
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
        .with_force_refresh(common.force_refresh)
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
        .with_force_refresh(args.force_refresh)
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

#[cfg(feature = "srcgen-compiler")]
fn get_validator_shapes_only_for_compile(
    args: &CompileArgs,
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
        .with_force_refresh(args.force_refresh)
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

use std::sync::Mutex;

static START_INSTANT: Mutex<Option<Instant>> = Mutex::new(None);

fn get_ts_nanos(ts: Instant) -> u64 {
    let mut guard = START_INSTANT.lock().unwrap();
    let start = *guard.get_or_insert(ts);
    if ts >= start {
        ts.duration_since(start).as_nanos() as u64
    } else {
        // if for some reason ts is before start, we update start
        *guard = Some(ts);
        0
    }
}

fn term_to_string(term: &Term) -> String {
    term.to_string()
}

use shifty::SourceShape;

fn readable_term_label(term: &Term) -> String {
    match term {
        Term::NamedNode(nn) => {
            let iri = nn.as_str();
            iri.rsplit(['#', '/'])
                .next()
                .filter(|segment| !segment.is_empty())
                .unwrap_or(iri)
                .to_string()
        }
        Term::BlankNode(bn) => format!("_:{}", bn.as_str()),
        _ => term.to_string(),
    }
}

fn shape_kind_label(source: &SourceShape) -> &'static str {
    match source {
        SourceShape::NodeShape(_) => "NodeShape",
        SourceShape::PropertyShape(_) => "PropertyShape",
    }
}

fn shape_term_for_source<'a>(source: &SourceShape, shape_ir: &'a ShapeIR) -> Option<&'a Term> {
    match source {
        SourceShape::NodeShape(id) => shape_ir.node_shape_terms.get(id),
        SourceShape::PropertyShape(id) => shape_ir.property_shape_terms.get(id),
    }
}

fn shape_term_for_node(id: shifty::shacl_ir::ID, shape_ir: &ShapeIR) -> Option<&Term> {
    shape_ir.node_shape_terms.get(&id)
}

fn shape_term_for_property(id: shifty::shacl_ir::PropShapeID, shape_ir: &ShapeIR) -> Option<&Term> {
    shape_ir.property_shape_terms.get(&id)
}

fn shape_frame_name(kind: &str, term: Option<&Term>, fallback: &str) -> String {
    term.map(|value| format!("{}:{}", kind, readable_term_label(value)))
        .unwrap_or_else(|| fallback.to_string())
}

fn component_shape_ref(id: shifty::shacl_ir::ID, shape_ir: &ShapeIR) -> String {
    shape_ir
        .node_shape_terms
        .get(&id)
        .map(readable_term_label)
        .unwrap_or_else(|| format!("NodeShape_{}", id.0))
}

fn component_property_ref(id: shifty::shacl_ir::PropShapeID, shape_ir: &ShapeIR) -> String {
    shape_ir
        .property_shape_terms
        .get(&id)
        .map(readable_term_label)
        .unwrap_or_else(|| format!("PropertyShape_{}", id.0))
}

fn component_kind_and_args(
    descriptor: &ComponentDescriptor,
    shape_ir: &ShapeIR,
) -> (String, Vec<String>) {
    match descriptor {
        ComponentDescriptor::Node { shape } => (
            "NodeConstraint".to_string(),
            vec![format!("shape={}", component_shape_ref(*shape, shape_ir))],
        ),
        ComponentDescriptor::Property { shape } => (
            "PropertyConstraint".to_string(),
            vec![format!(
                "shape={}",
                component_property_ref(*shape, shape_ir)
            )],
        ),
        ComponentDescriptor::QualifiedValueShape {
            shape,
            min_count,
            max_count,
            disjoint,
        } => {
            let mut args = vec![format!("shape={}", component_shape_ref(*shape, shape_ir))];
            if let Some(value) = min_count {
                args.push(format!("min_count={}", value));
            }
            if let Some(value) = max_count {
                args.push(format!("max_count={}", value));
            }
            if let Some(value) = disjoint {
                args.push(format!("disjoint={}", value));
            }
            ("QualifiedValueShape".to_string(), args)
        }
        ComponentDescriptor::Class { class } => (
            "ClassConstraint".to_string(),
            vec![format!("class={}", readable_term_label(class))],
        ),
        ComponentDescriptor::Datatype { datatype } => (
            "DatatypeConstraint".to_string(),
            vec![format!("datatype={}", readable_term_label(datatype))],
        ),
        ComponentDescriptor::NodeKind { node_kind } => (
            "NodeKindConstraint".to_string(),
            vec![format!("node_kind={}", readable_term_label(node_kind))],
        ),
        ComponentDescriptor::MinCount { min_count } => (
            "MinCount".to_string(),
            vec![format!("min_count={}", min_count)],
        ),
        ComponentDescriptor::MaxCount { max_count } => (
            "MaxCount".to_string(),
            vec![format!("max_count={}", max_count)],
        ),
        ComponentDescriptor::MinExclusive { value } => (
            "MinExclusiveConstraint".to_string(),
            vec![format!("value={}", readable_term_label(value))],
        ),
        ComponentDescriptor::MinInclusive { value } => (
            "MinInclusiveConstraint".to_string(),
            vec![format!("value={}", readable_term_label(value))],
        ),
        ComponentDescriptor::MaxExclusive { value } => (
            "MaxExclusiveConstraint".to_string(),
            vec![format!("value={}", readable_term_label(value))],
        ),
        ComponentDescriptor::MaxInclusive { value } => (
            "MaxInclusiveConstraint".to_string(),
            vec![format!("value={}", readable_term_label(value))],
        ),
        ComponentDescriptor::MinLength { length } => (
            "MinLengthConstraint".to_string(),
            vec![format!("length={}", length)],
        ),
        ComponentDescriptor::MaxLength { length } => (
            "MaxLengthConstraint".to_string(),
            vec![format!("length={}", length)],
        ),
        ComponentDescriptor::Pattern { pattern, flags } => {
            let mut args = vec![format!("pattern={}", pattern)];
            if let Some(value) = flags {
                args.push(format!("flags={}", value));
            }
            ("PatternConstraint".to_string(), args)
        }
        ComponentDescriptor::LanguageIn { languages } => (
            "LanguageInConstraint".to_string(),
            vec![format!("languages={}", languages.join(","))],
        ),
        ComponentDescriptor::UniqueLang { enabled } => (
            "UniqueLangConstraint".to_string(),
            vec![format!("enabled={}", enabled)],
        ),
        ComponentDescriptor::Equals { property } => (
            "EqualsConstraint".to_string(),
            vec![format!("property={}", readable_term_label(property))],
        ),
        ComponentDescriptor::Disjoint { property } => (
            "DisjointConstraint".to_string(),
            vec![format!("property={}", readable_term_label(property))],
        ),
        ComponentDescriptor::LessThan { property } => (
            "LessThanConstraint".to_string(),
            vec![format!("property={}", readable_term_label(property))],
        ),
        ComponentDescriptor::LessThanOrEquals { property } => (
            "LessThanOrEqualsConstraint".to_string(),
            vec![format!("property={}", readable_term_label(property))],
        ),
        ComponentDescriptor::Not { shape } => (
            "NotConstraint".to_string(),
            vec![format!("shape={}", component_shape_ref(*shape, shape_ir))],
        ),
        ComponentDescriptor::And { shapes } => (
            "AndConstraint".to_string(),
            vec![format!(
                "shapes={}",
                shapes
                    .iter()
                    .map(|shape| component_shape_ref(*shape, shape_ir))
                    .collect::<Vec<_>>()
                    .join(",")
            )],
        ),
        ComponentDescriptor::Or { shapes } => (
            "OrConstraint".to_string(),
            vec![format!(
                "shapes={}",
                shapes
                    .iter()
                    .map(|shape| component_shape_ref(*shape, shape_ir))
                    .collect::<Vec<_>>()
                    .join(",")
            )],
        ),
        ComponentDescriptor::Xone { shapes } => (
            "XoneConstraint".to_string(),
            vec![format!(
                "shapes={}",
                shapes
                    .iter()
                    .map(|shape| component_shape_ref(*shape, shape_ir))
                    .collect::<Vec<_>>()
                    .join(",")
            )],
        ),
        ComponentDescriptor::Closed {
            closed,
            ignored_properties,
        } => (
            "ClosedConstraint".to_string(),
            vec![
                format!("closed={}", closed),
                format!(
                    "ignored_properties={}",
                    ignored_properties
                        .iter()
                        .map(readable_term_label)
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            ],
        ),
        ComponentDescriptor::HasValue { value } => (
            "HasValueConstraint".to_string(),
            vec![format!("value={}", readable_term_label(value))],
        ),
        ComponentDescriptor::In { values } => (
            "InConstraint".to_string(),
            vec![format!(
                "values={}",
                values
                    .iter()
                    .map(readable_term_label)
                    .collect::<Vec<_>>()
                    .join(",")
            )],
        ),
        ComponentDescriptor::Sparql { constraint_node } => (
            "SPARQLConstraint".to_string(),
            vec![format!(
                "constraint={}",
                readable_term_label(constraint_node)
            )],
        ),
        ComponentDescriptor::Custom {
            definition,
            parameter_values,
        } => {
            let mut args = parameter_values
                .iter()
                .map(|(name, values)| {
                    format!(
                        "{}={}",
                        readable_term_label(&Term::NamedNode(name.clone())),
                        values
                            .iter()
                            .map(readable_term_label)
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                })
                .collect::<Vec<_>>();
            args.sort();
            (
                readable_term_label(&Term::NamedNode(definition.iri.clone())),
                args,
            )
        }
    }
}

fn component_frame_name(id: shifty::shacl_ir::ComponentID, shape_ir: &ShapeIR) -> String {
    shape_ir
        .components
        .get(&id)
        .map(|descriptor| {
            let (kind, args) = component_kind_and_args(descriptor, shape_ir);
            if args.is_empty() {
                format!("Component:{}", kind)
            } else {
                format!("Component:{}({})", kind, args.join(", "))
            }
        })
        .unwrap_or_else(|| format!("Component:{}", id.0))
}

fn annotate_shape_value(
    mut value: serde_json::Value,
    kind: &str,
    frame: String,
    term: Option<&Term>,
) -> serde_json::Value {
    if let Some(object) = value.as_object_mut() {
        object.insert("shape_kind".to_string(), json!(kind));
        object.insert("frame".to_string(), json!(frame));
        if let Some(term) = term {
            object.insert("shape_term".to_string(), json!(term.to_string()));
        }
    }
    value
}

fn trace_event_to_json(event: &TraceEvent, validator: &Validator) -> serde_json::Value {
    let shape_ir = validator.shape_ir();
    match event {
        TraceEvent::EnterShapeExecution(source, ts) => {
            let kind = shape_kind_label(source);
            let term = shape_term_for_source(source, shape_ir);
            let fallback = format!("{:?}", source);
            annotate_shape_value(
                json!({
                    "type": "EnterShapeExecution",
                    "source": fallback,
                    "ts": get_ts_nanos(*ts),
                }),
                kind,
                shape_frame_name(kind, term, &fallback),
                term,
            )
        }
        TraceEvent::ExitShapeExecution(source, ts) => {
            let kind = shape_kind_label(source);
            let term = shape_term_for_source(source, shape_ir);
            let fallback = format!("{:?}", source);
            annotate_shape_value(
                json!({
                    "type": "ExitShapeExecution",
                    "source": fallback,
                    "ts": get_ts_nanos(*ts),
                }),
                kind,
                shape_frame_name(kind, term, &fallback),
                term,
            )
        }
        TraceEvent::EnterNodeShape(id, ts) => {
            let term = shape_term_for_node(*id, shape_ir);
            annotate_shape_value(
                json!({
                    "type": "EnterNodeShape",
                    "node_shape_id": id.0,
                    "ts": get_ts_nanos(*ts),
                }),
                "NodeShape",
                shape_frame_name("NodeShape", term, &format!("NodeShape_{}", id.0)),
                term,
            )
        }
        TraceEvent::ExitNodeShape(id, ts) => {
            let term = shape_term_for_node(*id, shape_ir);
            annotate_shape_value(
                json!({
                    "type": "ExitNodeShape",
                    "node_shape_id": id.0,
                    "ts": get_ts_nanos(*ts),
                }),
                "NodeShape",
                shape_frame_name("NodeShape", term, &format!("NodeShape_{}", id.0)),
                term,
            )
        }
        TraceEvent::EnterPropertyShape(id, ts) => {
            let term = shape_term_for_property(*id, shape_ir);
            annotate_shape_value(
                json!({
                    "type": "EnterPropertyShape",
                    "property_shape_id": id.0,
                    "ts": get_ts_nanos(*ts),
                }),
                "PropertyShape",
                shape_frame_name("PropertyShape", term, &format!("PropertyShape_{}", id.0)),
                term,
            )
        }
        TraceEvent::ExitPropertyShape(id, ts) => {
            let term = shape_term_for_property(*id, shape_ir);
            annotate_shape_value(
                json!({
                    "type": "ExitPropertyShape",
                    "property_shape_id": id.0,
                    "ts": get_ts_nanos(*ts),
                }),
                "PropertyShape",
                shape_frame_name("PropertyShape", term, &format!("PropertyShape_{}", id.0)),
                term,
            )
        }
        TraceEvent::EnterComponent(id, ts) => json!({
            "type": "EnterComponent",
            "component_id": id.0,
            "frame": component_frame_name(*id, shape_ir),
            "ts": get_ts_nanos(*ts),
        }),
        TraceEvent::ExitComponent(id, ts) => json!({
            "type": "ExitComponent",
            "component_id": id.0,
            "frame": component_frame_name(*id, shape_ir),
            "ts": get_ts_nanos(*ts),
        }),
        TraceEvent::EnterRule(id, ts) => json!({
            "type": "EnterRule",
            "rule_id": id.0,
            "frame": format!("Rule:{}", id.0),
            "ts": get_ts_nanos(*ts),
        }),
        TraceEvent::ExitRule(id, inserted, ts) => json!({
            "type": "ExitRule",
            "rule_id": id.0,
            "frame": format!("Rule:{}", id.0),
            "inserted": inserted,
            "ts": get_ts_nanos(*ts),
        }),
        TraceEvent::ComponentPassed {
            component,
            focus,
            value,
            ts,
        } => json!({
            "type": "ComponentPassed",
            "component_id": component.0,
            "frame": component_frame_name(*component, shape_ir),
            "focus": term_to_string(focus),
            "value": value.as_ref().map(term_to_string),
            "ts": get_ts_nanos(*ts),
        }),
        TraceEvent::ComponentFailed {
            component,
            focus,
            value,
            message,
            ts,
        } => json!({
            "type": "ComponentFailed",
            "component_id": component.0,
            "frame": component_frame_name(*component, shape_ir),
            "focus": term_to_string(focus),
            "value": value.as_ref().map(term_to_string),
            "message": message,
            "ts": get_ts_nanos(*ts),
        }),
        TraceEvent::SparqlQuery { label, ts } => json!({
            "type": "SparqlQuery",
            "label": label,
            "ts": get_ts_nanos(*ts),
        }),
        TraceEvent::RuleApplied { rule, inserted, ts } => json!({
            "type": "RuleApplied",
            "rule_id": rule.0,
            "inserted": inserted,
            "ts": get_ts_nanos(*ts),
        }),
        TraceEvent::TargetCollectionStart(source, ts) => {
            let kind = shape_kind_label(source);
            let term = shape_term_for_source(source, shape_ir);
            let fallback = format!("{:?}", source);
            annotate_shape_value(
                json!({
                    "type": "TargetCollectionStart",
                    "source": fallback,
                    "ts": get_ts_nanos(*ts),
                }),
                kind,
                shape_frame_name(kind, term, &fallback),
                term,
            )
        }
        TraceEvent::TargetCollectionEnd(source, target_count, ts) => {
            let kind = shape_kind_label(source);
            let term = shape_term_for_source(source, shape_ir);
            let fallback = format!("{:?}", source);
            annotate_shape_value(
                json!({
                    "type": "TargetCollectionEnd",
                    "source": fallback,
                    "target_count": target_count,
                    "ts": get_ts_nanos(*ts),
                }),
                kind,
                shape_frame_name(kind, term, &fallback),
                term,
            )
        }
        TraceEvent::TargetCacheHit(source, cached_count) => {
            let kind = shape_kind_label(source);
            let term = shape_term_for_source(source, shape_ir);
            let fallback = format!("{:?}", source);
            annotate_shape_value(
                json!({
                    "type": "TargetCacheHit",
                    "source": fallback,
                    "cached_count": cached_count,
                }),
                kind,
                shape_frame_name(kind, term, &fallback),
                term,
            )
        }
        TraceEvent::ComponentExecutionStart(component_id, source, ts) => {
            let kind = shape_kind_label(source);
            let term = shape_term_for_source(source, shape_ir);
            let fallback = format!("{:?}", source);
            annotate_shape_value(
                json!({
                    "type": "ComponentExecutionStart",
                    "component_id": component_id.0,
                    "source": fallback,
                    "frame": component_frame_name(*component_id, shape_ir),
                    "ts": get_ts_nanos(*ts),
                }),
                kind,
                shape_frame_name(kind, term, &fallback),
                term,
            )
        }
        TraceEvent::ComponentExecutionEnd(component_id, source, ts) => {
            let kind = shape_kind_label(source);
            let term = shape_term_for_source(source, shape_ir);
            let fallback = format!("{:?}", source);
            annotate_shape_value(
                json!({
                    "type": "ComponentExecutionEnd",
                    "component_id": component_id.0,
                    "source": fallback,
                    "frame": component_frame_name(*component_id, shape_ir),
                    "ts": get_ts_nanos(*ts),
                }),
                kind,
                shape_frame_name(kind, term, &fallback),
                term,
            )
        }
        TraceEvent::ComponentCacheHit(component_id, source) => {
            let kind = shape_kind_label(source);
            let term = shape_term_for_source(source, shape_ir);
            let fallback = format!("{:?}", source);
            annotate_shape_value(
                json!({
                    "type": "ComponentCacheHit",
                    "component_id": component_id.0,
                    "source": fallback,
                    "frame": component_frame_name(*component_id, shape_ir),
                }),
                kind,
                shape_frame_name(kind, term, &fallback),
                term,
            )
        }
        TraceEvent::InferenceConditionCacheHit(shape_id, focus_node) => {
            let term = shape_term_for_node(*shape_id, shape_ir);
            annotate_shape_value(
                json!({
                    "type": "InferenceConditionCacheHit",
                    "shape_id": shape_id.0,
                    "focus_node": term_to_string(focus_node),
                }),
                "NodeShape",
                shape_frame_name("NodeShape", term, &format!("NodeShape_{}", shape_id.0)),
                term,
            )
        }
        TraceEvent::ParallelWaveStarted {
            wave_index,
            rules_count,
            ts,
        } => json!({
            "type": "ParallelWaveStarted",
            "wave_index": wave_index,
            "rules_count": rules_count,
            "ts": get_ts_nanos(*ts),
        }),
        TraceEvent::ParallelWaveCompleted {
            wave_index,
            rules_count,
            triples_added,
            ts,
        } => json!({
            "type": "ParallelWaveCompleted",
            "wave_index": wave_index,
            "rules_count": rules_count,
            "triples_added": triples_added,
            "ts": get_ts_nanos(*ts),
        }),
    }
}

fn trace_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let rendered = path.to_string_lossy();
    let base = rendered.strip_suffix(".jsonl").unwrap_or(&rendered);
    PathBuf::from(format!("{}{}", base, suffix))
}

fn subject_matches_term(subject: &NamedOrBlankNode, term: &Term) -> bool {
    match (subject, term) {
        (NamedOrBlankNode::NamedNode(left), Term::NamedNode(right)) => left == right,
        (NamedOrBlankNode::BlankNode(left), Term::BlankNode(right)) => left == right,
        _ => false,
    }
}

fn collect_shape_tooltip_fields(root: &Term, shape_ir: &ShapeIR) -> (Vec<String>, Vec<String>) {
    const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
    const SH_MESSAGE: &str = "http://www.w3.org/ns/shacl#message";

    let mut queue: VecDeque<Term> = VecDeque::from([root.clone()]);
    let mut seen_subjects: HashSet<Term> = HashSet::new();
    let mut labels: HashSet<String> = HashSet::new();
    let mut messages: HashSet<String> = HashSet::new();

    while let Some(subject_term) = queue.pop_front() {
        if !seen_subjects.insert(subject_term.clone()) {
            continue;
        }
        for quad in &shape_ir.shape_quads {
            if subject_matches_term(&quad.subject, &subject_term) {
                match quad.predicate.as_str() {
                    RDFS_LABEL => {
                        if let Term::Literal(literal) = &quad.object {
                            labels.insert(literal.value().to_string());
                        }
                    }
                    SH_MESSAGE => {
                        if let Term::Literal(literal) = &quad.object {
                            messages.insert(literal.value().to_string());
                        }
                    }
                    _ => {}
                }
                if let Term::BlankNode(bn) = &quad.object {
                    queue.push_back(Term::BlankNode(bn.clone()));
                }
            }
        }
    }

    let mut labels = labels.into_iter().collect::<Vec<_>>();
    labels.sort();
    let mut messages = messages.into_iter().collect::<Vec<_>>();
    messages.sort();
    (labels, messages)
}

fn write_traced_shape_sidecars(
    events: &[TraceEvent],
    validator: &Validator,
    trace_jsonl_path: &Path,
) -> Result<(), String> {
    let shape_ir = validator.shape_ir();
    let mut traced_shapes: HashMap<String, (String, Term)> = HashMap::new();
    let mut traced_components: HashSet<shifty::shacl_ir::ComponentID> = HashSet::new();

    for event in events {
        match event {
            TraceEvent::EnterShapeExecution(source, _)
            | TraceEvent::ExitShapeExecution(source, _) => {
                if let Some(term) = shape_term_for_source(source, shape_ir) {
                    let kind = shape_kind_label(source).to_string();
                    let frame = shape_frame_name(&kind, Some(term), &format!("{:?}", source));
                    traced_shapes.entry(frame).or_insert((kind, term.clone()));
                }
            }
            TraceEvent::EnterNodeShape(id, _) | TraceEvent::ExitNodeShape(id, _) => {
                if let Some(term) = shape_term_for_node(*id, shape_ir) {
                    let kind = "NodeShape".to_string();
                    let frame = shape_frame_name(&kind, Some(term), &format!("NodeShape_{}", id.0));
                    traced_shapes.entry(frame).or_insert((kind, term.clone()));
                }
            }
            TraceEvent::EnterPropertyShape(id, _) | TraceEvent::ExitPropertyShape(id, _) => {
                if let Some(term) = shape_term_for_property(*id, shape_ir) {
                    let kind = "PropertyShape".to_string();
                    let frame =
                        shape_frame_name(&kind, Some(term), &format!("PropertyShape_{}", id.0));
                    traced_shapes.entry(frame).or_insert((kind, term.clone()));
                }
            }
            TraceEvent::EnterComponent(id, _)
            | TraceEvent::ExitComponent(id, _)
            | TraceEvent::ComponentPassed { component: id, .. }
            | TraceEvent::ComponentFailed { component: id, .. } => {
                traced_components.insert(*id);
            }
            _ => {}
        }
    }

    if traced_shapes.is_empty() && traced_components.is_empty() {
        return Ok(());
    }

    let mut frame_entries: Vec<serde_json::Value> = traced_shapes
        .into_iter()
        .map(|(frame, (kind, term))| {
            let (labels, messages) = collect_shape_tooltip_fields(&term, shape_ir);
            json!({
                "frame": frame,
                "kind": kind,
                "term": term.to_string(),
                "labels": labels,
                "messages": messages,
            })
        })
        .collect();
    let metadata_path = trace_sidecar_path(trace_jsonl_path, ".shapes.json");

    let mut component_ids: Vec<_> = traced_components.into_iter().collect();
    component_ids.sort_by_key(|id| id.0);
    for component_id in component_ids {
        if let Some(descriptor) = shape_ir.components.get(&component_id) {
            let (kind, args) = component_kind_and_args(descriptor, shape_ir);
            frame_entries.push(json!({
                "frame": component_frame_name(component_id, shape_ir),
                "kind": "Component",
                "component_id": component_id.0,
                "component_kind": kind,
                "args": args,
            }));
        }
    }
    frame_entries.sort_by(|left, right| {
        left.get("frame")
            .and_then(|value| value.as_str())
            .cmp(&right.get("frame").and_then(|value| value.as_str()))
    });

    let metadata = json!({
        "frames": frame_entries,
    });
    fs::write(
        &metadata_path,
        serde_json::to_string_pretty(&metadata)
            .map_err(|e| format!("Failed to serialise shape metadata JSON: {}", e))?,
    )
    .map_err(|e| {
        format!(
            "Failed to write shape metadata {}: {}",
            metadata_path.display(),
            e
        )
    })?;

    Ok(())
}

fn emit_trace_outputs(
    events: &[TraceEvent],
    args: &TraceOutputArgs,
    validator: &Validator,
) -> Result<(), String> {
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
            let value = trace_event_to_json(ev, validator);
            let line = serde_json::to_string(&value)
                .map_err(|e| format!("Failed to serialise trace event to JSON: {}", e))?;
            buf.push_str(&line);
            buf.push('\n');
        }
        fs::write(path, buf)
            .map_err(|e| format!("Failed to write trace jsonl {}: {}", path.display(), e))?;
        write_traced_shape_sidecars(events, validator, path)?;
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

    emit_trace_outputs(&events, args, validator)
        .map_err(|e| format!("Failed to emit trace outputs: {}", e))
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn emit_benchmark_json(path: &Path, value: &serde_json::Value) -> Result<(), String> {
    let payload = serde_json::to_string_pretty(value)
        .map_err(|e| format!("Failed to serialise benchmark JSON: {}", e))?;
    if path.as_os_str() == "-" {
        println!("{}", payload);
        return Ok(());
    }
    fs::write(path, format!("{payload}\n"))
        .map_err(|e| format!("Failed to write benchmark JSON {}: {}", path.display(), e))
}

fn validator_stats_json(validator: &Validator) -> serde_json::Value {
    let component_graph_call_stats = validator
        .component_graph_call_stats()
        .into_iter()
        .map(|stat| {
            json!({
                "component_id": stat.component_id.0,
                "source_shape": stat.source_shape,
                "component_label": stat.component_label,
                "quads_for_pattern_calls": stat.quads_for_pattern_calls,
                "execute_prepared_calls": stat.execute_prepared_calls,
                "component_invocations": stat.component_invocations,
                "runtime_total_ms": stat.runtime_total_ms,
                "runtime_min_ms": stat.runtime_min_ms,
                "runtime_max_ms": stat.runtime_max_ms,
                "runtime_mean_ms": stat.runtime_mean_ms,
                "runtime_stddev_ms": stat.runtime_stddev_ms,
            })
        })
        .collect::<Vec<_>>();
    let shape_phase_timing_stats = validator
        .shape_phase_timing_stats()
        .into_iter()
        .map(|stat| {
            json!({
                "source_shape": stat.source_shape,
                "phase": stat.phase,
                "invocations": stat.invocations,
                "runtime_total_ms": stat.runtime_total_ms,
                "runtime_min_ms": stat.runtime_min_ms,
                "runtime_max_ms": stat.runtime_max_ms,
                "runtime_mean_ms": stat.runtime_mean_ms,
                "runtime_stddev_ms": stat.runtime_stddev_ms,
            })
        })
        .collect::<Vec<_>>();
    let sparql_query_call_stats = validator
        .sparql_query_call_stats()
        .into_iter()
        .map(|stat| {
            json!({
                "source_shape": stat.source_shape,
                "component_id": stat.component_id.0,
                "constraint_term": stat.constraint_term,
                "query_hash": stat.query_hash,
                "invocations": stat.invocations,
                "rows_returned_total": stat.rows_returned_total,
                "rows_returned_min": stat.rows_returned_min,
                "rows_returned_max": stat.rows_returned_max,
                "rows_returned_mean": stat.rows_returned_mean,
                "runtime_total_ms": stat.runtime_total_ms,
                "runtime_min_ms": stat.runtime_min_ms,
                "runtime_max_ms": stat.runtime_max_ms,
                "runtime_mean_ms": stat.runtime_mean_ms,
                "runtime_stddev_ms": stat.runtime_stddev_ms,
            })
        })
        .collect::<Vec<_>>();
    let trace_event_count = validator
        .context()
        .trace_events()
        .lock()
        .ok()
        .map(|events| events.len())
        .unwrap_or(0);

    json!({
        "component_graph_call_stats": component_graph_call_stats,
        "shape_phase_timing_stats": shape_phase_timing_stats,
        "sparql_query_call_stats": sparql_query_call_stats,
        "trace_event_count": trace_event_count,
    })
}

fn run_command(command: Commands) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    match command {
        Commands::Visualize(args) => {
            let fetch_start = Instant::now();
            let validator = get_validator(&args.common, None)?;
            let graph_fetching_ms = elapsed_ms(fetch_start);
            let dot_string = validator.to_graphviz()?;
            let wants_graphviz = args.graphviz || args.pdf.is_none();

            if wants_graphviz {
                println!("{}", dot_string);
            } else if let Some(pdf_path) = args.pdf.as_ref() {
                write_pdf_from_dot(dot_string, pdf_path, "PDF")?;
            }

            Ok(json!({
                "subcommand": "visualize",
                "phases_ms": {
                    "graph_fetching": graph_fetching_ms,
                    "validate": serde_json::Value::Null,
                    "inference": serde_json::Value::Null,
                    "report_assembly": 0.0,
                },
                "emitted_graphviz": wants_graphviz,
                "pdf_output": args.pdf.as_ref().map(|p| p.display().to_string()),
                "validator_stats": validator_stats_json(&validator),
            }))
        }
        Commands::Validate(args) => {
            if !args.run_inference
                && (args.inference_min_iterations.is_some()
                    || args.inference_max_iterations.is_some()
                    || args.inference_no_converge
                    || args.inference_error_on_blank_nodes
                    || args.inference_debug)
            {
                return Err("inference tuning flags require --run-inference".into());
            }
            let fetch_start = Instant::now();
            let validator = get_validator(&args.common, args.shacl_ir.shacl_ir.as_ref())?;
            let graph_fetching_ms = elapsed_ms(fetch_start);
            let inference_outcome = if args.run_inference {
                let config = build_inference_config(
                    args.inference_min_iterations,
                    args.inference_max_iterations,
                    args.inference_no_converge,
                    args.inference_error_on_blank_nodes,
                    args.inference_debug,
                );
                let inference_start = Instant::now();
                let outcome = validator
                    .run_inference_with_config(config)
                    .map_err(|err| format!("Inference failed: {}", err))?;
                let inference_ms = elapsed_ms(inference_start);
                Some((outcome, inference_ms))
            } else {
                None
            };
            let validate_start = Instant::now();
            let report = validator.validate();
            let validate_ms = elapsed_ms(validate_start);

            if let Some((outcome, _)) = &inference_outcome {
                info!(
                    "Inference added {} triple(s) in {} iteration(s); converged={}",
                    outcome.triples_added, outcome.iterations_executed, outcome.converged
                );
            }

            let report_options = ValidationReportOptions {
                follow_bnodes: args.follow_bnodes,
            };

            let report_assembly_start = Instant::now();
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
            let report_assembly_ms = elapsed_ms(report_assembly_start);

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

            Ok(json!({
                "subcommand": "validate",
                "phases_ms": {
                    "graph_fetching": graph_fetching_ms,
                    "validate": validate_ms,
                    "inference": inference_outcome.as_ref().map(|(_, ms)| *ms),
                    "report_assembly": report_assembly_ms,
                },
                "report": {
                    "conforms": report.conforms(),
                    "component_frequency_entries": report.get_component_frequencies().len(),
                    "format": match args.format {
                        ValidateOutputFormat::Turtle => "turtle",
                        ValidateOutputFormat::Dump => "dump",
                        ValidateOutputFormat::RdfXml => "rdfxml",
                        ValidateOutputFormat::NTriples => "ntriples",
                    },
                    "follow_bnodes": args.follow_bnodes,
                },
                "inference": inference_outcome.as_ref().map(|(outcome, _)| {
                    json!({
                        "iterations_executed": outcome.iterations_executed,
                        "triples_added": outcome.triples_added,
                        "converged": outcome.converged,
                        "inferred_quad_count": outcome.inferred_quads.len(),
                    })
                }),
                "validator_stats": validator_stats_json(&validator),
            }))
        }
        Commands::Infer(args) => {
            let fetch_start = Instant::now();
            let validator = get_validator(&args.common, args.shacl_ir.shacl_ir.as_ref())?;
            let graph_fetching_ms = elapsed_ms(fetch_start);
            let config = build_inference_config(
                args.min_iterations,
                args.max_iterations,
                args.no_converge,
                args.error_on_blank_nodes,
                args.debug,
            );
            let inference_start = Instant::now();
            let outcome = validator
                .run_inference_with_config(config)
                .map_err(|e| format!("Inference failed: {}", e))?;
            let inference_ms = elapsed_ms(inference_start);
            info!(
                "Inference added {} triple(s) in {} iteration(s); converged={}",
                outcome.triples_added, outcome.iterations_executed, outcome.converged
            );

            let quads_to_emit = if args.union {
                validator
                    .data_graph_quads()
                    .map_err(|e| format!("Failed to read data graph: {}", e))?
            } else {
                outcome.inferred_quads.clone()
            };

            let report_assembly_start = Instant::now();
            let turtle_bytes = serialize_quads_to_turtle(&quads_to_emit)?;

            if let Some(path) = args.output_file.as_ref() {
                fs::write(path, &turtle_bytes)
                    .map_err(|e| format!("Failed to write {}: {}", path.display(), e))?;
                info!(
                    "Wrote {} triple(s) to {}",
                    quads_to_emit.len(),
                    path.display()
                );
            } else {
                io::stdout().write_all(&turtle_bytes)?;
            }
            let report_assembly_ms = elapsed_ms(report_assembly_start);

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

            Ok(json!({
                "subcommand": "infer",
                "phases_ms": {
                    "graph_fetching": graph_fetching_ms,
                    "validate": serde_json::Value::Null,
                    "inference": inference_ms,
                    "report_assembly": report_assembly_ms,
                },
                "inference": {
                    "iterations_executed": outcome.iterations_executed,
                    "triples_added": outcome.triples_added,
                    "converged": outcome.converged,
                    "inferred_quad_count": outcome.inferred_quads.len(),
                    "emitted_quad_count": quads_to_emit.len(),
                    "union_output": args.union,
                    "output_file": args.output_file.as_ref().map(|p| p.display().to_string()),
                },
                "validator_stats": validator_stats_json(&validator),
            }))
        }
        #[cfg(feature = "srcgen-compiler")]
        Commands::Compile(args) => {
            let fetch_start = Instant::now();
            let validator = get_validator_shapes_only_for_compile(&args)?;
            let graph_fetching_ms = elapsed_ms(fetch_start);
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
            let (generated_root, generated_files, generated_binary_files, backend_name) =
                match args.resolve_backend()? {
                    ResolvedCompileBackend::Srcgen(backend) => {
                        let srcgen_ir =
                            lower_shape_ir_to_srcgen_ir(&shape_ir).map_err(|e| e.to_string())?;
                        if let Some(plan_out) = &args.plan_out {
                            fs::write(plan_out, srcgen_ir.to_json_pretty()?)?;
                        }
                        let backend_name = match backend {
                            SrcGenBackend::Specialized => "specialized",
                            SrcGenBackend::Tables => "tables",
                        };
                        info!("Using compiler track=srcgen backend={backend_name}");
                        let generated =
                            generate_srcgen_modules_from_ir_with_backend(&srcgen_ir, backend)?;
                        (
                            generated.root,
                            generated.files,
                            generated.binary_files,
                            backend_name,
                        )
                    }
                };

            let out_dir = &args.out_dir;
            let src_dir = out_dir.join("src");
            let generated_dir = src_dir.join("generated");
            fs::create_dir_all(&src_dir)?;
            fs::create_dir_all(&generated_dir)?;
            fs::write(generated_dir.join("mod.rs"), generated_root)?;
            for (name, content) in generated_files {
                let path = generated_dir.join(name);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(path, content)?;
            }
            for (name, content) in generated_binary_files {
                let path = generated_dir.join(name);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(path, content)?;
            }
            let shape_ir_json = match serde_json::to_string(&shape_ir) {
                Ok(json) => json,
                Err(err) => {
                    info!(
                        "Skipping embedded shape_ir.json (non-JSON map keys in ShapeIR): {}",
                        err
                    );
                    "null".to_string()
                }
            };
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
                                if rev.is_empty() { None } else { Some(rev) }
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
                "[workspace]\n\n[package]\nname = \"{}\"\nversion = \"0.0.1\"\nedition = \"2024\"\n\n[dependencies]\noxigraph = {{ version = \"0.5.5\" }}\nrayon = \"1\"\nregex = \"1\"\nserde_json = \"1\"\nbincode = {{ version = \"2\", features = [\"serde\"] }}\noxsdatatypes = \"0.2.2\"\nfixedbitset = \"0.5\"\ndashmap = \"6\"\n{}\nontoenv = \"0.5.1\"\nlog = \"0.4\"\nenv_logger = \"0.11\"\n\n[profile.release]\ndebug = 2\nstrip = \"none\"\n\n[profile.bench]\ndebug = 2\nstrip = \"none\"\n",
                args.bin_name, shifty_dep,
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

            Ok(json!({
                "subcommand": "compile",
                "phases_ms": {
                    "graph_fetching": graph_fetching_ms,
                    "validate": serde_json::Value::Null,
                    "inference": serde_json::Value::Null,
                    "report_assembly": 0.0,
                },
                "backend": backend_name,
                "out_dir": out_dir.display().to_string(),
                "bin_name": args.bin_name,
                "release": args.release,
                "binary_path": target_dir.display().to_string(),
                "plan_out": args.plan_out.as_ref().map(|p| p.display().to_string()),
                "shape_ir": {
                    "node_shapes": shape_ir.node_shapes.len(),
                    "property_shapes": shape_ir.property_shapes.len(),
                    "components": shape_ir.components.len(),
                    "rules": shape_ir.rules.len(),
                }
            }))
        }
        Commands::Heat(args) => {
            let fetch_start = Instant::now();
            let validator = get_validator(&args.common, None)?;
            let graph_fetching_ms = elapsed_ms(fetch_start);
            let validate_start = Instant::now();
            let report = validator.validate();
            let validate_ms = elapsed_ms(validate_start);

            let frequencies: HashMap<(String, String, String), usize> =
                report.get_component_frequencies();

            let mut sorted_frequencies: Vec<_> = frequencies.into_iter().collect();
            sorted_frequencies.sort_by(|a, b| b.1.cmp(&a.1));

            println!("ID\tLabel\tType\tInvocations");
            for ((id, label, item_type), count) in &sorted_frequencies {
                println!("{}\t{}\t{}\t{}", id, label, item_type, count);
            }

            Ok(json!({
                "subcommand": "heat",
                "phases_ms": {
                    "graph_fetching": graph_fetching_ms,
                    "validate": validate_ms,
                    "inference": serde_json::Value::Null,
                    "report_assembly": 0.0,
                },
                "report": {
                    "conforms": report.conforms(),
                    "frequency_rows": sorted_frequencies.len(),
                },
                "validator_stats": validator_stats_json(&validator),
            }))
        }
        Commands::VisualizeHeatmap(args) => {
            let fetch_start = Instant::now();
            let validator = get_validator(&args.common, None)?;
            let graph_fetching_ms = elapsed_ms(fetch_start);
            let validate_start = Instant::now();
            let dot_string = run_validation_then_get_heatmap_dot(&validator, args.all)?;
            let validate_ms = elapsed_ms(validate_start);
            let wants_graphviz = args.graphviz || args.pdf.is_none();

            if wants_graphviz {
                println!("{}", dot_string);
            } else if let Some(pdf_path) = args.pdf.as_ref() {
                write_pdf_from_dot(dot_string, pdf_path, "PDF heatmap")?;
            }

            Ok(json!({
                "subcommand": "visualize-heatmap",
                "phases_ms": {
                    "graph_fetching": graph_fetching_ms,
                    "validate": validate_ms,
                    "inference": serde_json::Value::Null,
                    "report_assembly": 0.0,
                },
                "emitted_graphviz": wants_graphviz,
                "pdf_output": args.pdf.as_ref().map(|p| p.display().to_string()),
                "include_all_nodes": args.all,
                "validator_stats": validator_stats_json(&validator),
            }))
        }
        Commands::GenerateIr(args) => {
            let fetch_start = Instant::now();
            let validator = get_validator_shapes_only(&args)?;
            let graph_fetching_ms = elapsed_ms(fetch_start);
            let mut shape_ir = validator.context().shape_ir().clone();
            shape_ir.data_graph = None;
            let report_assembly_start = Instant::now();
            ir_cache::write_shape_ir(&args.output_file, &shape_ir)
                .map_err(|e| format!("Failed to write SHACL-IR cache: {}", e))?;
            let report_assembly_ms = elapsed_ms(report_assembly_start);
            println!("Wrote SHACL-IR cache to {}", args.output_file.display());

            Ok(json!({
                "subcommand": "generate-ir",
                "phases_ms": {
                    "graph_fetching": graph_fetching_ms,
                    "validate": serde_json::Value::Null,
                    "inference": serde_json::Value::Null,
                    "report_assembly": report_assembly_ms,
                },
                "output_file": args.output_file.display().to_string(),
                "shape_ir": {
                    "node_shapes": shape_ir.node_shapes.len(),
                    "property_shapes": shape_ir.property_shapes.len(),
                    "components": shape_ir.components.len(),
                    "rules": shape_ir.rules.len(),
                }
            }))
        }
        Commands::Trace(args) => {
            let fetch_start = Instant::now();
            let validator = get_validator(&args.common, None)?;
            let graph_fetching_ms = elapsed_ms(fetch_start);
            let validate_start = Instant::now();
            let report = validator.validate();
            let validate_ms = elapsed_ms(validate_start);
            report.print_traces();

            Ok(json!({
                "subcommand": "trace",
                "phases_ms": {
                    "graph_fetching": graph_fetching_ms,
                    "validate": validate_ms,
                    "inference": serde_json::Value::Null,
                    "report_assembly": 0.0,
                },
                "report": {
                    "conforms": report.conforms(),
                },
                "validator_stats": validator_stats_json(&validator),
            }))
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    init_logging(cli.log_level, cli.verbose, cli.quiet);
    let command_name = cli.command.name();
    let benchmark_json_path = cli.benchmark_json.clone();
    let start = Instant::now();
    let result = run_command(cli.command);
    let wall_time_ms = start.elapsed().as_secs_f64() * 1000.0;

    if let Some(path) = benchmark_json_path.as_ref() {
        let benchmark = match &result {
            Ok(metrics) => json!({
                "command": command_name,
                "success": true,
                "wall_time_ms": wall_time_ms,
                "metrics": metrics,
            }),
            Err(err) => json!({
                "command": command_name,
                "success": false,
                "wall_time_ms": wall_time_ms,
                "error": err.to_string(),
            }),
        };
        emit_benchmark_json(path, &benchmark)?;
    }

    result.map(|_| ())
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

#[cfg(feature = "srcgen-compiler")]
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

#[cfg(all(test, feature = "srcgen-compiler"))]
mod tests {
    use super::*;

    fn parse_compile_args(extra: &[&str]) -> CompileArgs {
        let mut argv = vec!["shifty", "compile", "--shapes-file", "shapes.ttl"];
        argv.extend_from_slice(extra);
        let cli = Cli::try_parse_from(argv).expect("compile args should parse");
        match cli.command {
            Commands::Compile(args) => args,
            _ => panic!("expected compile command"),
        }
    }

    #[test]
    fn compile_defaults_to_srcgen_specialized_backend() {
        let args = parse_compile_args(&[]);
        assert!(matches!(
            args.resolve_backend().expect("backend should resolve"),
            ResolvedCompileBackend::Srcgen(SrcGenBackend::Specialized)
        ));
    }

    #[test]
    fn compile_tables_backend_is_supported() {
        let args = parse_compile_args(&["--backend", "tables"]);
        assert!(matches!(
            args.resolve_backend().expect("backend should resolve"),
            ResolvedCompileBackend::Srcgen(SrcGenBackend::Tables)
        ));
    }
}
