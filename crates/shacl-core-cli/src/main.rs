use clap::{Parser, Subcommand, ValueEnum};
use oxigraph::io::RdfFormat;
use serde::Serialize;
use shifty_shacl_core::source::{ShapeSource, SourceLoadOptions, source_from_str};
use shifty_shacl_core::{
    AnalysisSummary, BackendClosureMode, BackendViewOptions, BackendViews, DependencyClass,
    InferencePlan, InferenceView, NormalizeOptions, PlanningEstimationMode, RewriteOptions,
    RewriteSummary, SharedWorkUnitKind, SliceReason, SliceRoots, StaticAnalysisSummary,
    StaticCostHint, ValidationBackend, ValidationPlan, ValidationPlanningOptions, ValidationResult,
    ValidationView, analyze_program, analyze_static_with_roots, build_validation_report,
    derive_backend_logical_plans, derive_backend_views, derive_inference_logical_plan,
    derive_inference_view, derive_validation_logical_plan,
    derive_validation_logical_plan_with_options_detailed, derive_validation_view,
    load_and_parse_with_ontoenv, lower_to_program, normalize_program, parse_resolved,
    render_shape_program_dot, rewrite_program,
};
use shifty_shacl_core_inmemory::{InMemoryValidationBackend, compile_validation_plan};
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

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
    /// Run deeper static analysis over the lowered shape program
    StaticAnalyze(StaticAnalyzeArgs),
    /// Rewrite the lowered shape program using static-analysis-guided passes
    Rewrite(RewriteArgs),
    /// Derive backend-facing validation and inference views
    BackendView(BackendViewArgs),
    /// Derive backend-agnostic logical plans
    Plan(PlanArgs),
    /// Derive data-graph-aware validation plan annotations
    DataPlan(DataPlanArgs),
    /// Inspect the in-memory backend physical plan
    PhysicalPlan(PhysicalPlanArgs),
    /// Execute a narrow validation backend over data graphs
    Validate(ValidateArgs),
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

#[derive(ValueEnum, Debug, Clone, Copy)]
enum ValidateFormat {
    Text,
    Json,
    Turtle,
    NTriples,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum PlanningModeArg {
    Exact,
    Sampled,
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

    /// Drop deactivated shapes and rules before rendering
    #[arg(long)]
    prune_deactivated: bool,

    /// Write output to a file instead of stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct AnalyzeArgs {
    #[command(flatten)]
    shared: SharedArgs,

    /// Drop deactivated shapes and rules before analysis
    #[arg(long)]
    prune_deactivated: bool,

    /// Output format
    #[arg(long, value_enum, default_value_t = AnalyzeFormat::Text)]
    format: AnalyzeFormat,

    /// Write output to a file instead of stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct StaticAnalyzeArgs {
    #[command(flatten)]
    shared: SharedArgs,

    /// Drop deactivated shapes and rules before analysis
    #[arg(long)]
    prune_deactivated: bool,

    /// Slice from target-bearing root shapes
    #[arg(long, default_value_t = true)]
    slice_from_targets: bool,

    /// Explicit root shape selector, matched against a shape IRI or normalized key
    #[arg(long = "root-shape")]
    root_shapes: Vec<String>,

    /// Output format
    #[arg(long, value_enum, default_value_t = AnalyzeFormat::Text)]
    format: AnalyzeFormat,

    /// Write output to a file instead of stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct RewriteArgs {
    #[command(flatten)]
    shared: SharedArgs,

    /// Drop deactivated shapes and rules before rewriting
    #[arg(long)]
    prune_deactivated: bool,

    /// Explicit root shape selector, matched against a shape IRI or normalized key
    #[arg(long = "root-shape")]
    root_shapes: Vec<String>,

    /// Remove unreachable structure after root slicing
    #[arg(long, default_value_t = true)]
    prune_unreachable: bool,

    /// Remove unreferenced constraint components after rewriting
    #[arg(long, default_value_t = true)]
    prune_unreferenced_components: bool,

    /// Reorder constraints using static cost hints
    #[arg(long, default_value_t = true)]
    reorder_constraints: bool,

    /// Reorder rules using static cost hints
    #[arg(long, default_value_t = true)]
    reorder_rules: bool,

    /// Compute recursive-region annotations in the rewrite summary
    #[arg(long, default_value_t = true)]
    annotate_recursive_regions: bool,

    /// Output format
    #[arg(long, value_enum, default_value_t = AnalyzeFormat::Text)]
    format: AnalyzeFormat,

    /// Write output to a file instead of stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum BackendViewKind {
    Both,
    Validation,
    Inference,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum BackendClosureArg {
    TargetRoots,
    ValidationClosure,
    InferenceClosure,
    MixedClosure,
}

#[derive(Parser, Debug)]
struct BackendViewArgs {
    #[command(flatten)]
    shared: SharedArgs,

    /// Drop deactivated shapes and rules before deriving views
    #[arg(long)]
    prune_deactivated: bool,

    /// Explicit root shape selector, matched against a shape IRI or normalized key
    #[arg(long = "root-shape")]
    root_shapes: Vec<String>,

    /// Which backend-facing view to render
    #[arg(long, value_enum, default_value_t = BackendViewKind::Both)]
    kind: BackendViewKind,

    /// Closure mode used to derive backend-facing views
    #[arg(long, value_enum, default_value_t = BackendClosureArg::TargetRoots)]
    closure: BackendClosureArg,

    /// Output format
    #[arg(long, value_enum, default_value_t = AnalyzeFormat::Text)]
    format: AnalyzeFormat,

    /// Write output to a file instead of stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum PlanKind {
    Both,
    Validation,
    Inference,
}

#[derive(Parser, Debug)]
struct PlanArgs {
    #[command(flatten)]
    shared: SharedArgs,

    /// Drop deactivated shapes and rules before deriving plans
    #[arg(long)]
    prune_deactivated: bool,

    /// Explicit root shape selector, matched against a shape IRI or normalized key
    #[arg(long = "root-shape")]
    root_shapes: Vec<String>,

    /// Which logical plan to render
    #[arg(long, value_enum, default_value_t = PlanKind::Both)]
    kind: PlanKind,

    /// Closure mode used to derive backend-facing views before planning
    #[arg(long, value_enum, default_value_t = BackendClosureArg::TargetRoots)]
    closure: BackendClosureArg,

    /// Output format
    #[arg(long, value_enum, default_value_t = AnalyzeFormat::Text)]
    format: AnalyzeFormat,

    /// Write output to a file instead of stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct ValidateArgs {
    #[command(flatten)]
    shared: SharedArgs,

    /// Data files or URLs to validate
    #[arg(long = "data", value_name = "DATA", required = true)]
    data: Vec<String>,

    /// Drop deactivated shapes and rules before planning/execution
    #[arg(long)]
    prune_deactivated: bool,

    /// Explicit root shape selector, matched against a shape IRI or normalized key
    #[arg(long = "root-shape")]
    root_shapes: Vec<String>,

    /// Closure mode used to derive the validation view and plan
    #[arg(long, value_enum, default_value_t = BackendClosureArg::TargetRoots)]
    closure: BackendClosureArg,

    /// Output format
    #[arg(long, value_enum, default_value_t = ValidateFormat::Text)]
    format: ValidateFormat,

    /// Data-aware planning estimation mode
    #[arg(long = "planning", value_enum, default_value_t = PlanningModeArg::Exact)]
    planning: PlanningModeArg,

    /// Subject budget used when --planning sampled is selected
    #[arg(long = "planning-sample-subjects")]
    planning_sample_subjects: Option<usize>,

    /// Write output to a file instead of stdout
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Write stage timing benchmarks to a JSON file
    #[arg(long)]
    benchmark_json: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct DataPlanArgs {
    #[command(flatten)]
    shared: SharedArgs,

    /// Data files or URLs used to summarize selectivity and annotate the logical plan
    #[arg(long = "data", value_name = "DATA", required = true)]
    data: Vec<String>,

    /// Drop deactivated shapes and rules before deriving plans
    #[arg(long)]
    prune_deactivated: bool,

    /// Explicit root shape selector, matched against a shape IRI or normalized key
    #[arg(long = "root-shape")]
    root_shapes: Vec<String>,

    /// Closure mode used to derive backend-facing views before planning
    #[arg(long, value_enum, default_value_t = BackendClosureArg::TargetRoots)]
    closure: BackendClosureArg,

    /// Output format
    #[arg(long, value_enum, default_value_t = AnalyzeFormat::Text)]
    format: AnalyzeFormat,

    /// Data-aware planning estimation mode
    #[arg(long = "planning", value_enum, default_value_t = PlanningModeArg::Exact)]
    planning: PlanningModeArg,

    /// Subject budget used when --planning sampled is selected
    #[arg(long = "planning-sample-subjects")]
    planning_sample_subjects: Option<usize>,

    /// Write output to a file instead of stdout
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Write stage timing benchmarks to a JSON file
    #[arg(long)]
    benchmark_json: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct PhysicalPlanArgs {
    #[command(flatten)]
    shared: SharedArgs,

    /// Data files or URLs used to derive the data-aware validation plan
    #[arg(long = "data", value_name = "DATA", required = true)]
    data: Vec<String>,

    /// Drop deactivated shapes and rules before deriving plans
    #[arg(long)]
    prune_deactivated: bool,

    /// Explicit root shape selector, matched against a shape IRI or normalized key
    #[arg(long = "root-shape")]
    root_shapes: Vec<String>,

    /// Closure mode used to derive backend-facing views before planning
    #[arg(long, value_enum, default_value_t = BackendClosureArg::TargetRoots)]
    closure: BackendClosureArg,

    /// Output format
    #[arg(long, value_enum, default_value_t = AnalyzeFormat::Text)]
    format: AnalyzeFormat,

    /// Data-aware planning estimation mode
    #[arg(long = "planning", value_enum, default_value_t = PlanningModeArg::Exact)]
    planning: PlanningModeArg,

    /// Subject budget used when --planning sampled is selected
    #[arg(long = "planning-sample-subjects")]
    planning_sample_subjects: Option<usize>,

    /// Write output to a file instead of stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkStageRecord {
    name: String,
    elapsed_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkReport {
    command: String,
    stages: Vec<BenchmarkStageRecord>,
    total_ms: f64,
}

#[derive(Debug)]
struct BenchmarkRecorder {
    command: &'static str,
    started: Instant,
    stages: Vec<BenchmarkStageRecord>,
}

impl BenchmarkRecorder {
    fn new(command: &'static str) -> Self {
        Self {
            command,
            started: Instant::now(),
            stages: Vec::new(),
        }
    }

    fn measure<T, E, F>(&mut self, name: &str, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Result<T, E>,
    {
        let started = Instant::now();
        let result = f();
        self.stages.push(BenchmarkStageRecord {
            name: name.to_string(),
            elapsed_ms: elapsed_ms(started.elapsed()),
        });
        result
    }

    fn measure_value<T, F>(&mut self, name: &str, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        let started = Instant::now();
        let value = f();
        self.stages.push(BenchmarkStageRecord {
            name: name.to_string(),
            elapsed_ms: elapsed_ms(started.elapsed()),
        });
        value
    }

    fn extend_stages<I>(&mut self, stages: I)
    where
        I: IntoIterator<Item = BenchmarkStageRecord>,
    {
        self.stages.extend(stages);
    }

    fn finish(self) -> BenchmarkReport {
        BenchmarkReport {
            command: self.command.to_string(),
            stages: self.stages,
            total_ms: elapsed_ms(self.started.elapsed()),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Dump(args) => run_dump(args)?,
        Command::Analyze(args) => run_analyze(args)?,
        Command::StaticAnalyze(args) => run_static_analyze(args)?,
        Command::Rewrite(args) => run_rewrite(args)?,
        Command::BackendView(args) => run_backend_view(args)?,
        Command::Plan(args) => run_plan(args)?,
        Command::DataPlan(args) => run_data_plan(args)?,
        Command::PhysicalPlan(args) => run_physical_plan(args)?,
        Command::Validate(args) => run_validate(args)?,
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
    let program = normalize_program(
        &lower_to_program(&syntax),
        NormalizeOptions {
            prune_deactivated: args.prune_deactivated,
        },
    );
    let dot = render_shape_program_dot(&program);
    write_text(&dot, args.output.as_deref())?;
    Ok(())
}

fn run_analyze(args: AnalyzeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let resolved = load_shapes(&args.shared)?;
    let syntax = parse_resolved(&resolved);
    let program = normalize_program(
        &lower_to_program(&syntax),
        NormalizeOptions {
            prune_deactivated: args.prune_deactivated,
        },
    );
    let analysis = analyze_program(&program);
    match args.format {
        AnalyzeFormat::Json => {
            write_json(
                &serde_json::json!({
                    "analysis": analysis,
                    "template_inspection": collect_template_inspection(&program),
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

fn run_static_analyze(args: StaticAnalyzeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let resolved = load_shapes(&args.shared)?;
    let syntax = parse_resolved(&resolved);
    let program = normalize_program(
        &lower_to_program(&syntax),
        NormalizeOptions {
            prune_deactivated: args.prune_deactivated,
        },
    );
    let roots = if args.root_shapes.is_empty() {
        if args.slice_from_targets {
            SliceRoots::TargetShapes
        } else {
            SliceRoots::ExplicitShapes(Vec::new())
        }
    } else {
        SliceRoots::ExplicitSelectors(args.root_shapes.clone())
    };
    let summary = analyze_static_with_roots(&program, roots);
    let slice = summary.slice.clone();
    match args.format {
        AnalyzeFormat::Json => {
            write_json(
                &serde_json::json!({
                    "static_analysis": summary,
                    "slice": slice,
                    "template_inspection": collect_template_inspection(&program),
                    "program_diagnostics": program.diagnostics,
                    "syntax_diagnostics": syntax.diagnostics,
                }),
                args.output.as_deref(),
            )?;
        }
        AnalyzeFormat::Text => {
            let text = render_static_analysis_text(&summary, &slice, &program, &syntax);
            write_text(&text, args.output.as_deref())?;
        }
    }
    Ok(())
}

fn run_rewrite(args: RewriteArgs) -> Result<(), Box<dyn std::error::Error>> {
    let resolved = load_shapes(&args.shared)?;
    let syntax = parse_resolved(&resolved);
    let program = normalize_program(
        &lower_to_program(&syntax),
        NormalizeOptions {
            prune_deactivated: args.prune_deactivated,
        },
    );
    let roots = if args.root_shapes.is_empty() {
        None
    } else {
        Some(SliceRoots::ExplicitSelectors(args.root_shapes.clone()))
    };
    let rewritten = rewrite_program(
        &program,
        RewriteOptions {
            roots,
            prune_unreachable: args.prune_unreachable,
            prune_unreferenced_components: args.prune_unreferenced_components,
            reorder_constraints: args.reorder_constraints,
            reorder_rules: args.reorder_rules,
            annotate_recursive_regions: args.annotate_recursive_regions,
        },
    );
    match args.format {
        AnalyzeFormat::Json => write_json(
            &serde_json::json!({
                "rewrite_summary": rewritten.summary,
                "rewritten_program": rewritten.program,
                "program_diagnostics": program.diagnostics,
                "syntax_diagnostics": syntax.diagnostics,
            }),
            args.output.as_deref(),
        )?,
        AnalyzeFormat::Text => {
            let text = render_rewrite_text(&rewritten.summary, &rewritten.program);
            write_text(&text, args.output.as_deref())?;
        }
    }
    Ok(())
}

fn run_backend_view(args: BackendViewArgs) -> Result<(), Box<dyn std::error::Error>> {
    let resolved = load_shapes(&args.shared)?;
    let syntax = parse_resolved(&resolved);
    let program = normalize_program(
        &lower_to_program(&syntax),
        NormalizeOptions {
            prune_deactivated: args.prune_deactivated,
        },
    );
    let roots = if args.root_shapes.is_empty() {
        Some(SliceRoots::TargetShapes)
    } else {
        Some(SliceRoots::ExplicitSelectors(args.root_shapes.clone()))
    };
    let options = BackendViewOptions {
        rewrite: RewriteOptions {
            roots,
            prune_unreachable: true,
            ..RewriteOptions::default()
        },
        closure_mode: match args.closure {
            BackendClosureArg::TargetRoots => BackendClosureMode::TargetRoots,
            BackendClosureArg::ValidationClosure => BackendClosureMode::ValidationClosure,
            BackendClosureArg::InferenceClosure => BackendClosureMode::InferenceClosure,
            BackendClosureArg::MixedClosure => BackendClosureMode::MixedClosure,
        },
    };
    match args.kind {
        BackendViewKind::Both => {
            let views = derive_backend_views(&program, options);
            match args.format {
                AnalyzeFormat::Json => write_json(&views, args.output.as_deref())?,
                AnalyzeFormat::Text => {
                    let text = render_backend_views_text(&views);
                    write_text(&text, args.output.as_deref())?;
                }
            }
        }
        BackendViewKind::Validation => {
            let view = derive_validation_view(&program, options);
            match args.format {
                AnalyzeFormat::Json => write_json(&view, args.output.as_deref())?,
                AnalyzeFormat::Text => {
                    let text = render_validation_view_text(&view);
                    write_text(&text, args.output.as_deref())?;
                }
            }
        }
        BackendViewKind::Inference => {
            let view = derive_inference_view(&program, options);
            match args.format {
                AnalyzeFormat::Json => write_json(&view, args.output.as_deref())?,
                AnalyzeFormat::Text => {
                    let text = render_inference_view_text(&view);
                    write_text(&text, args.output.as_deref())?;
                }
            }
        }
    }
    Ok(())
}

fn run_plan(args: PlanArgs) -> Result<(), Box<dyn std::error::Error>> {
    let resolved = load_shapes(&args.shared)?;
    let syntax = parse_resolved(&resolved);
    let program = normalize_program(
        &lower_to_program(&syntax),
        NormalizeOptions {
            prune_deactivated: args.prune_deactivated,
        },
    );
    let roots = if args.root_shapes.is_empty() {
        Some(SliceRoots::TargetShapes)
    } else {
        Some(SliceRoots::ExplicitSelectors(args.root_shapes.clone()))
    };
    let options = BackendViewOptions {
        rewrite: RewriteOptions {
            roots,
            prune_unreachable: true,
            ..RewriteOptions::default()
        },
        closure_mode: match args.closure {
            BackendClosureArg::TargetRoots => BackendClosureMode::TargetRoots,
            BackendClosureArg::ValidationClosure => BackendClosureMode::ValidationClosure,
            BackendClosureArg::InferenceClosure => BackendClosureMode::InferenceClosure,
            BackendClosureArg::MixedClosure => BackendClosureMode::MixedClosure,
        },
    };
    match args.kind {
        PlanKind::Both => {
            let plans = derive_backend_logical_plans(&program, options);
            match args.format {
                AnalyzeFormat::Json => write_json(&plans, args.output.as_deref())?,
                AnalyzeFormat::Text => {
                    let text = render_backend_plans_text(&plans);
                    write_text(&text, args.output.as_deref())?;
                }
            }
        }
        PlanKind::Validation => {
            let plan = derive_validation_logical_plan(&program, options);
            match args.format {
                AnalyzeFormat::Json => write_json(&plan, args.output.as_deref())?,
                AnalyzeFormat::Text => {
                    let text = render_validation_plan_text(&plan);
                    write_text(&text, args.output.as_deref())?;
                }
            }
        }
        PlanKind::Inference => {
            let plan = derive_inference_logical_plan(&program, options);
            match args.format {
                AnalyzeFormat::Json => write_json(&plan, args.output.as_deref())?,
                AnalyzeFormat::Text => {
                    let text = render_inference_plan_text(&plan);
                    write_text(&text, args.output.as_deref())?;
                }
            }
        }
    }
    Ok(())
}

fn run_validate(args: ValidateArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut benchmark = args
        .benchmark_json
        .as_ref()
        .map(|_| BenchmarkRecorder::new("validate"));
    let resolved = if let Some(recorder) = benchmark.as_mut() {
        recorder.measure("load.shapes", || load_shapes(&args.shared))?
    } else {
        load_shapes(&args.shared)?
    };
    let syntax = if let Some(recorder) = benchmark.as_mut() {
        recorder.measure_value("parse.syntax", || parse_resolved(&resolved))
    } else {
        parse_resolved(&resolved)
    };
    let lowered = if let Some(recorder) = benchmark.as_mut() {
        recorder.measure_value("lower.program", || lower_to_program(&syntax))
    } else {
        lower_to_program(&syntax)
    };
    let program = if let Some(recorder) = benchmark.as_mut() {
        recorder.measure_value("optimize.normalize", || {
            normalize_program(
                &lowered,
                NormalizeOptions {
                    prune_deactivated: args.prune_deactivated,
                },
            )
        })
    } else {
        normalize_program(
            &lowered,
            NormalizeOptions {
                prune_deactivated: args.prune_deactivated,
            },
        )
    };
    let roots = if args.root_shapes.is_empty() {
        Some(SliceRoots::TargetShapes)
    } else {
        Some(SliceRoots::ExplicitSelectors(args.root_shapes.clone()))
    };
    let options = BackendViewOptions {
        rewrite: RewriteOptions {
            roots,
            prune_unreachable: true,
            ..RewriteOptions::default()
        },
        closure_mode: match args.closure {
            BackendClosureArg::TargetRoots => BackendClosureMode::TargetRoots,
            BackendClosureArg::ValidationClosure => BackendClosureMode::ValidationClosure,
            BackendClosureArg::InferenceClosure => BackendClosureMode::InferenceClosure,
            BackendClosureArg::MixedClosure => BackendClosureMode::MixedClosure,
        },
    };
    let data_sources = args
        .data
        .iter()
        .map(|value| source_from_str(value))
        .collect::<Vec<_>>();
    let data = if same_shape_and_data_sources(&shape_sources(&args.shared), &data_sources) {
        if let Some(recorder) = benchmark.as_mut() {
            recorder.measure_value("load.data", || resolved.clone())
        } else {
            resolved.clone()
        }
    } else {
        if let Some(recorder) = benchmark.as_mut() {
            recorder.measure("load.data", || {
                shifty_shacl_core::source::load_with_ontoenv(
                    &data_sources,
                    &load_options(&args.shared),
                )
            })?
        } else {
            shifty_shacl_core::source::load_with_ontoenv(
                &data_sources,
                &load_options(&args.shared),
            )?
        }
    };
    let execution_data = if let Some(recorder) = benchmark.as_mut() {
        recorder.measure_value("prepare.execution_data", || resolved.merged_with(&data))
    } else {
        resolved.merged_with(&data)
    };
    let planning_options = planning_options(args.planning, args.planning_sample_subjects);
    let plan = if let Some(recorder) = benchmark.as_mut() {
        let planning = derive_validation_logical_plan_with_options_detailed(
            &program,
            options,
            &execution_data,
            planning_options,
        );
        recorder.extend_stages(planning.benchmark.stages.into_iter().map(|stage| {
            BenchmarkStageRecord {
                name: stage.name,
                elapsed_ms: stage.elapsed_ms,
            }
        }));
        planning.plan
    } else {
        derive_validation_logical_plan_with_options_detailed(
            &program,
            options,
            &execution_data,
            planning_options,
        )
        .plan
    };
    let backend = InMemoryValidationBackend;
    let (result, backend_benchmark) = if benchmark.is_some() {
        let (result, benchmark) = backend
            .execute_profiled(&plan, &execution_data)
            .map_err(io::Error::other)?;
        (result, Some(benchmark))
    } else {
        (
            backend
                .execute(&plan, &execution_data)
                .map_err(io::Error::other)?,
            None,
        )
    };
    if let (Some(recorder), Some(backend_benchmark)) = (benchmark.as_mut(), backend_benchmark) {
        recorder.extend_stages(backend_benchmark.stages.into_iter().map(|stage| {
            BenchmarkStageRecord {
                name: stage.name,
                elapsed_ms: stage.elapsed_ms,
            }
        }));
    }
    if let Some(recorder) = benchmark.as_mut() {
        recorder.measure("output.render_write", || {
            match args.format {
                ValidateFormat::Json => write_json(&result, args.output.as_deref())?,
                ValidateFormat::Text => {
                    let text = render_validation_result_text(&result);
                    write_text(&text, args.output.as_deref())?;
                }
                ValidateFormat::Turtle | ValidateFormat::NTriples => {
                    let report = build_validation_report(&result, &plan.view.program);
                    let rdf = report
                        .serialize(match args.format {
                            ValidateFormat::Turtle => RdfFormat::Turtle,
                            ValidateFormat::NTriples => RdfFormat::NTriples,
                            ValidateFormat::Text | ValidateFormat::Json => unreachable!(),
                        })
                        .map_err(io::Error::other)?;
                    write_text(&rdf, args.output.as_deref())?;
                }
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        })?;
    } else {
        match args.format {
            ValidateFormat::Json => write_json(&result, args.output.as_deref())?,
            ValidateFormat::Text => {
                let text = render_validation_result_text(&result);
                write_text(&text, args.output.as_deref())?;
            }
            ValidateFormat::Turtle | ValidateFormat::NTriples => {
                let report = build_validation_report(&result, &plan.view.program);
                let rdf = report
                    .serialize(match args.format {
                        ValidateFormat::Turtle => RdfFormat::Turtle,
                        ValidateFormat::NTriples => RdfFormat::NTriples,
                        ValidateFormat::Text | ValidateFormat::Json => unreachable!(),
                    })
                    .map_err(io::Error::other)?;
                write_text(&rdf, args.output.as_deref())?;
            }
        }
    }
    if let Some(path) = args.benchmark_json.as_deref() {
        if let Some(recorder) = benchmark {
            write_json(&recorder.finish(), Some(path))?;
        }
    }
    Ok(())
}

fn run_data_plan(args: DataPlanArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut benchmark = args
        .benchmark_json
        .as_ref()
        .map(|_| BenchmarkRecorder::new("data-plan"));
    let resolved = if let Some(recorder) = benchmark.as_mut() {
        recorder.measure("load.shapes", || load_shapes(&args.shared))?
    } else {
        load_shapes(&args.shared)?
    };
    let syntax = if let Some(recorder) = benchmark.as_mut() {
        recorder.measure_value("parse.syntax", || parse_resolved(&resolved))
    } else {
        parse_resolved(&resolved)
    };
    let lowered = if let Some(recorder) = benchmark.as_mut() {
        recorder.measure_value("lower.program", || lower_to_program(&syntax))
    } else {
        lower_to_program(&syntax)
    };
    let program = if let Some(recorder) = benchmark.as_mut() {
        recorder.measure_value("optimize.normalize", || {
            normalize_program(
                &lowered,
                NormalizeOptions {
                    prune_deactivated: args.prune_deactivated,
                },
            )
        })
    } else {
        normalize_program(
            &lowered,
            NormalizeOptions {
                prune_deactivated: args.prune_deactivated,
            },
        )
    };
    let roots = if args.root_shapes.is_empty() {
        Some(SliceRoots::TargetShapes)
    } else {
        Some(SliceRoots::ExplicitSelectors(args.root_shapes.clone()))
    };
    let options = BackendViewOptions {
        rewrite: RewriteOptions {
            roots,
            prune_unreachable: true,
            ..RewriteOptions::default()
        },
        closure_mode: match args.closure {
            BackendClosureArg::TargetRoots => BackendClosureMode::TargetRoots,
            BackendClosureArg::ValidationClosure => BackendClosureMode::ValidationClosure,
            BackendClosureArg::InferenceClosure => BackendClosureMode::InferenceClosure,
            BackendClosureArg::MixedClosure => BackendClosureMode::MixedClosure,
        },
    };
    let data_sources = args
        .data
        .iter()
        .map(|value| source_from_str(value))
        .collect::<Vec<_>>();
    let data = if same_shape_and_data_sources(&shape_sources(&args.shared), &data_sources) {
        if let Some(recorder) = benchmark.as_mut() {
            recorder.measure_value("load.data", || resolved.clone())
        } else {
            resolved.clone()
        }
    } else {
        if let Some(recorder) = benchmark.as_mut() {
            recorder.measure("load.data", || {
                shifty_shacl_core::source::load_with_ontoenv(
                    &data_sources,
                    &load_options(&args.shared),
                )
            })?
        } else {
            shifty_shacl_core::source::load_with_ontoenv(
                &data_sources,
                &load_options(&args.shared),
            )?
        }
    };
    let execution_data = if let Some(recorder) = benchmark.as_mut() {
        recorder.measure_value("prepare.execution_data", || resolved.merged_with(&data))
    } else {
        resolved.merged_with(&data)
    };
    let planning_options = planning_options(args.planning, args.planning_sample_subjects);
    let plan = if let Some(recorder) = benchmark.as_mut() {
        let planning = derive_validation_logical_plan_with_options_detailed(
            &program,
            options,
            &execution_data,
            planning_options,
        );
        recorder.extend_stages(planning.benchmark.stages.into_iter().map(|stage| {
            BenchmarkStageRecord {
                name: stage.name,
                elapsed_ms: stage.elapsed_ms,
            }
        }));
        planning.plan
    } else {
        derive_validation_logical_plan_with_options_detailed(
            &program,
            options,
            &execution_data,
            planning_options,
        )
        .plan
    };
    if let Some(recorder) = benchmark.as_mut() {
        recorder.measure("output.render_write", || {
            match args.format {
                AnalyzeFormat::Json => write_json(
                    &serde_json::json!({
                        "data_summary": plan.data_summary,
                        "validation_plan": plan,
                    }),
                    args.output.as_deref(),
                )?,
                AnalyzeFormat::Text => {
                    let text = render_data_plan_text(&plan);
                    write_text(&text, args.output.as_deref())?;
                }
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        })?;
    } else {
        match args.format {
            AnalyzeFormat::Json => write_json(
                &serde_json::json!({
                    "data_summary": plan.data_summary,
                    "validation_plan": plan,
                }),
                args.output.as_deref(),
            )?,
            AnalyzeFormat::Text => {
                let text = render_data_plan_text(&plan);
                write_text(&text, args.output.as_deref())?;
            }
        }
    }
    if let Some(path) = args.benchmark_json.as_deref() {
        if let Some(recorder) = benchmark {
            write_json(&recorder.finish(), Some(path))?;
        }
    }
    Ok(())
}

fn run_physical_plan(args: PhysicalPlanArgs) -> Result<(), Box<dyn std::error::Error>> {
    let resolved = load_shapes(&args.shared)?;
    let syntax = parse_resolved(&resolved);
    let program = normalize_program(
        &lower_to_program(&syntax),
        NormalizeOptions {
            prune_deactivated: args.prune_deactivated,
        },
    );
    let roots = if args.root_shapes.is_empty() {
        Some(SliceRoots::TargetShapes)
    } else {
        Some(SliceRoots::ExplicitSelectors(args.root_shapes.clone()))
    };
    let options = BackendViewOptions {
        rewrite: RewriteOptions {
            roots,
            prune_unreachable: true,
            ..RewriteOptions::default()
        },
        closure_mode: match args.closure {
            BackendClosureArg::TargetRoots => BackendClosureMode::TargetRoots,
            BackendClosureArg::ValidationClosure => BackendClosureMode::ValidationClosure,
            BackendClosureArg::InferenceClosure => BackendClosureMode::InferenceClosure,
            BackendClosureArg::MixedClosure => BackendClosureMode::MixedClosure,
        },
    };
    let data_sources = args
        .data
        .iter()
        .map(|value| source_from_str(value))
        .collect::<Vec<_>>();
    let data = if same_shape_and_data_sources(&shape_sources(&args.shared), &data_sources) {
        resolved.clone()
    } else {
        shifty_shacl_core::source::load_with_ontoenv(&data_sources, &load_options(&args.shared))?
    };
    let execution_data = resolved.merged_with(&data);
    let plan = derive_validation_logical_plan_with_options_detailed(
        &program,
        options,
        &execution_data,
        planning_options(args.planning, args.planning_sample_subjects),
    )
    .plan;
    let physical = compile_validation_plan(&plan).inspect();
    match args.format {
        AnalyzeFormat::Json => write_json(
            &serde_json::json!({
                "validation_plan": plan,
                "physical_plan": physical,
            }),
            args.output.as_deref(),
        )?,
        AnalyzeFormat::Text => {
            let text = render_physical_plan_text(&physical);
            write_text(&text, args.output.as_deref())?;
        }
    }
    Ok(())
}

fn planning_options(
    mode: PlanningModeArg,
    sample_subject_budget: Option<usize>,
) -> ValidationPlanningOptions {
    let mut options = ValidationPlanningOptions {
        estimation_mode: match mode {
            PlanningModeArg::Exact => PlanningEstimationMode::Exact,
            PlanningModeArg::Sampled => PlanningEstimationMode::Sampled,
        },
        ..ValidationPlanningOptions::default()
    };
    if let Some(sample_subject_budget) = sample_subject_budget {
        options.subject_sample_budget = sample_subject_budget.max(1);
    }
    options
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

fn same_shape_and_data_sources(shapes: &[ShapeSource], data: &[ShapeSource]) -> bool {
    shapes.len() == data.len()
        && shapes
            .iter()
            .zip(data)
            .all(|(left, right)| match (left, right) {
                (ShapeSource::File(left), ShapeSource::File(right)) => left == right,
                (ShapeSource::Url(left), ShapeSource::Url(right)) => left == right,
                (
                    ShapeSource::Quads {
                        graph_iri: left_graph,
                        quads: left_quads,
                    },
                    ShapeSource::Quads {
                        graph_iri: right_graph,
                        quads: right_quads,
                    },
                ) => left_graph == right_graph && left_quads == right_quads,
                _ => false,
            })
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

fn elapsed_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn render_analysis_text(
    analysis: &AnalysisSummary,
    program: &shifty_shacl_core::algebra::ShapeProgram,
    syntax: &shifty_shacl_core::syntax::ShapeSyntaxDocument,
) -> String {
    let template_inspection = collect_template_inspection(program);
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
    out.push_str(&format!("  roots: {}\n", analysis.root_shapes.len()));
    out.push_str(&format!(
        "  reachable: {}\n",
        analysis.reachable_shapes.len()
    ));
    out.push_str(&format!(
        "  unreachable: {}\n",
        analysis.unreachable_shapes.len()
    ));
    out.push_str(&format!("  rules: {}\n", analysis.inference_rule_count));
    out.push_str(&format!(
        "  reachable_rules: {}\n",
        analysis.reachable_rules.len()
    ));
    out.push_str(&format!(
        "  deactivated_rules: {}\n",
        analysis.deactivated_rule_count
    ));
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

    out.push_str("\nTargets\n");
    let mut target_kinds: Vec<_> = analysis.target_kind_counts.iter().collect();
    target_kinds.sort_by_key(|(name, _)| *name);
    for (name, count) in target_kinds {
        out.push_str(&format!("  {}: {}\n", name, count));
    }

    out.push_str("\nConstraints\n");
    let mut constraint_kinds: Vec<_> = analysis.constraint_kind_counts.iter().collect();
    constraint_kinds.sort_by_key(|(name, _)| *name);
    for (name, count) in constraint_kinds {
        out.push_str(&format!("  {}: {}\n", name, count));
    }

    out.push_str("\nDependencies\n");
    let mut dependency_kinds: Vec<_> = analysis.dependency_kind_counts.iter().collect();
    dependency_kinds.sort_by_key(|(name, _)| *name);
    for (name, count) in dependency_kinds {
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
            .filter_map(|shape_id| analysis.shape_labels.get(shape_id).cloned())
            .collect::<Vec<_>>();
        if !labels.is_empty() {
            out.push_str(&format!("     {}\n", labels.join(", ")));
        }
    }

    if !analysis.unreachable_shapes.is_empty() {
        out.push_str("\nUnreachable Shapes\n");
        for shape_id in &analysis.unreachable_shapes {
            if let Some(label) = analysis.shape_labels.get(shape_id) {
                out.push_str(&format!("  {}\n", label));
            }
        }
    }

    if !template_inspection.is_empty() {
        out.push_str("\nTemplates\n");
        for instance in &template_inspection {
            out.push_str(&format!(
                "  shape={} predicate={}\n",
                instance.owner_shape, instance.predicate
            ));
            if !instance.bindings.is_empty() {
                out.push_str(&format!(
                    "    bindings: {}\n",
                    instance
                        .bindings
                        .iter()
                        .map(|binding| format!(
                            "{}={}{}",
                            binding.name,
                            binding.values.join(", "),
                            if binding.from_default {
                                " [default]"
                            } else {
                                ""
                            }
                        ))
                        .collect::<Vec<_>>()
                        .join("; ")
                ));
            }
            for message in &instance.messages {
                out.push_str(&format!(
                    "    message: {}\n",
                    message.rendered.as_deref().unwrap_or(message.raw.as_str())
                ));
            }
            if let Some(label) = &instance.label_template {
                out.push_str(&format!(
                    "    label: {}\n",
                    label.rendered.as_deref().unwrap_or(label.raw.as_str())
                ));
            }
        }
    }

    if !analysis.diagnostics_by_severity.is_empty() {
        out.push_str("\nDiagnostic Summary\n");
        let mut diagnostics: Vec<_> = analysis.diagnostics_by_severity.iter().collect();
        diagnostics.sort_by_key(|(severity, _)| *severity);
        for (severity, count) in diagnostics {
            out.push_str(&format!("  {}: {}\n", severity, count));
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

fn render_static_analysis_text(
    summary: &StaticAnalysisSummary,
    slice: &shifty_shacl_core::ProgramSlice,
    program: &shifty_shacl_core::algebra::ShapeProgram,
    syntax: &shifty_shacl_core::syntax::ShapeSyntaxDocument,
) -> String {
    let template_inspection = collect_template_inspection(program);
    let mut out = String::new();
    out.push_str("Static Analysis\n");
    out.push_str(&format!("  roots: {}\n", slice.roots.len()));
    if !slice.requested_root_selectors.is_empty() {
        out.push_str(&format!(
            "  requested selectors: {}\n",
            slice.requested_root_selectors.join(", ")
        ));
    }
    if !slice.unresolved_root_selectors.is_empty() {
        out.push_str(&format!(
            "  unresolved selectors: {}\n",
            slice.unresolved_root_selectors.join(", ")
        ));
    }
    out.push_str(&format!(
        "  retained shapes: {}\n",
        slice.retained_shape_ids.len()
    ));
    out.push_str(&format!(
        "  dropped shapes: {}\n",
        slice.dropped_shape_ids.len()
    ));
    out.push_str(&format!(
        "  retained rules: {}\n",
        slice.retained_rule_ids.len()
    ));
    out.push_str(&format!(
        "  dropped rules: {}\n",
        slice.dropped_rule_ids.len()
    ));
    out.push_str(&format!(
        "  retained constraints: {}\n",
        slice.retained_constraint_ids.len()
    ));
    out.push_str(&format!(
        "  retained components: {}\n",
        slice.retained_component_ids.len()
    ));

    out.push_str("\nPer-Root Slice\n");
    for root in &slice.root_summaries {
        let label = summary
            .baseline
            .shape_labels
            .get(&root.root)
            .cloned()
            .unwrap_or_else(|| format!("shape:{}", root.root.0));
        out.push_str(&format!(
            "  {}: shapes={} rules={} constraints={} targets={} components={}\n",
            label,
            root.retained_shapes,
            root.retained_rules,
            root.retained_constraints,
            root.retained_targets,
            root.retained_components
        ));
    }

    out.push_str("\nSlice Reasons\n");
    let dropped_shape_examples = slice
        .dropped_shape_reasons
        .iter()
        .take(5)
        .filter_map(|(shape_id, reason)| {
            summary.baseline.shape_labels.get(shape_id).map(|label| {
                format!(
                    "{label}: {}",
                    render_slice_reason(reason, &summary.baseline)
                )
            })
        })
        .collect::<Vec<_>>();
    if dropped_shape_examples.is_empty() {
        out.push_str("  no dropped shapes\n");
    } else {
        for example in dropped_shape_examples {
            out.push_str(&format!("  {example}\n"));
        }
    }

    out.push_str("\nContext Footprint\n");
    let mut histogram: Vec<_> = summary.context.histogram.iter().collect();
    histogram.sort_by_key(|(name, _)| *name);
    for (name, count) in histogram {
        out.push_str(&format!("  {}: {}\n", name, count));
    }
    let interesting_context = summary
        .context
        .shape_footprints
        .iter()
        .filter(|(_, footprint)| {
            matches!(
                footprint,
                shifty_shacl_core::ContextFootprint::BoundedTraversal
                    | shifty_shacl_core::ContextFootprint::ShapeReferenceTraversal
                    | shifty_shacl_core::ContextFootprint::GlobalSparql
            )
        })
        .take(5)
        .collect::<Vec<_>>();
    if !interesting_context.is_empty() {
        out.push_str("  examples:\n");
        for (shape_id, footprint) in interesting_context {
            let label = summary
                .baseline
                .shape_labels
                .get(shape_id)
                .cloned()
                .unwrap_or_else(|| format!("shape:{}", shape_id.0));
            let reasons = summary
                .context
                .shape_reasons
                .get(shape_id)
                .cloned()
                .unwrap_or_default()
                .join("; ");
            out.push_str(&format!(
                "    {} => {} ({})\n",
                label,
                render_context_footprint(footprint),
                reasons
            ));
        }
    }

    out.push_str("\nRecursive Components\n");
    for component in summary
        .baseline
        .dependency_components
        .iter()
        .filter(|component| component.recursive)
    {
        let labels = component
            .shapes
            .iter()
            .filter_map(|shape_id| summary.baseline.shape_labels.get(shape_id))
            .cloned()
            .collect::<Vec<_>>();
        out.push_str(&format!(
            "  size={} {}\n",
            component.shapes.len(),
            labels.join(", ")
        ));
    }
    if !summary
        .baseline
        .dependency_components
        .iter()
        .any(|component| component.recursive)
    {
        out.push_str("  none\n");
    }

    out.push_str("\nSPARQL / Global Touch\n");
    let mut global_shapes = summary
        .context
        .shape_footprints
        .iter()
        .filter(|(_, footprint)| {
            matches!(footprint, shifty_shacl_core::ContextFootprint::GlobalSparql)
        })
        .filter_map(|(shape_id, _)| summary.baseline.shape_labels.get(shape_id))
        .cloned()
        .collect::<Vec<_>>();
    global_shapes.sort();
    if global_shapes.is_empty() {
        out.push_str("  none\n");
    } else {
        for label in global_shapes {
            out.push_str(&format!("  {}\n", label));
        }
    }

    out.push_str("\nDuplicate Fingerprints\n");
    out.push_str(&format!(
        "  constraints: {}\n",
        summary.fingerprints.duplicate_constraints.len()
    ));
    out.push_str(&format!(
        "  targets: {}\n",
        summary.fingerprints.duplicate_targets.len()
    ));
    out.push_str(&format!(
        "  rules: {}\n",
        summary.fingerprints.duplicate_rules.len()
    ));

    out.push_str("\nShared Work Candidates\n");
    out.push_str(&format!(
        "  custom component constraints: {}\n",
        summary
            .shared_work
            .duplicate_custom_component_constraints
            .len()
    ));
    out.push_str(&format!(
        "  sparql constraints: {}\n",
        summary.shared_work.duplicate_sparql_constraints.len()
    ));
    out.push_str(&format!(
        "  sparql rules: {}\n",
        summary.shared_work.duplicate_sparql_rules.len()
    ));
    if let Some(group) = summary
        .shared_work
        .duplicate_custom_component_constraints
        .first()
    {
        let labels = group
            .owner_shapes
            .iter()
            .filter_map(|shape_id| summary.baseline.shape_labels.get(shape_id))
            .cloned()
            .collect::<Vec<_>>();
        out.push_str(&format!(
            "  example custom-component owners: {}\n",
            labels.join(", ")
        ));
    }

    out.push_str("\nStatic Cost Hints\n");
    let mut hint_histogram: Vec<_> = summary.cost_hints.histogram.iter().collect();
    hint_histogram.sort_by_key(|(name, _)| *name);
    for (name, count) in hint_histogram {
        out.push_str(&format!("  {}: {}\n", name, count));
    }
    let interesting_hints = summary
        .cost_hints
        .shape_hints
        .iter()
        .filter(|(_, hints)| {
            hints.iter().any(|hint| {
                matches!(
                    hint,
                    StaticCostHint::HasSparql
                        | StaticCostHint::Recursive
                        | StaticCostHint::DuplicateConstraintWork
                        | StaticCostHint::DuplicateRuleWork
                )
            })
        })
        .take(5)
        .collect::<Vec<_>>();
    if !interesting_hints.is_empty() {
        out.push_str("  examples:\n");
        for (shape_id, hints) in interesting_hints {
            let label = summary
                .baseline
                .shape_labels
                .get(shape_id)
                .cloned()
                .unwrap_or_else(|| format!("shape:{}", shape_id.0));
            out.push_str(&format!(
                "    {} => {}\n",
                label,
                hints
                    .iter()
                    .map(render_cost_hint)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }

    if !template_inspection.is_empty() {
        out.push_str("\nTemplates\n");
        for instance in &template_inspection {
            out.push_str(&format!(
                "  shape={} predicate={}\n",
                instance.owner_shape, instance.predicate
            ));
            if !instance.bindings.is_empty() {
                out.push_str(&format!(
                    "    bindings: {}\n",
                    instance
                        .bindings
                        .iter()
                        .map(|binding| format!(
                            "{}={}{}",
                            binding.name,
                            binding.values.join(", "),
                            if binding.from_default {
                                " [default]"
                            } else {
                                ""
                            }
                        ))
                        .collect::<Vec<_>>()
                        .join("; ")
                ));
            }
        }
    }

    out.push_str("\nDiagnostics\n");
    out.push_str(&format!("  syntax: {}\n", syntax.diagnostics.len()));
    out.push_str(&format!("  program: {}\n", program.diagnostics.len()));
    out
}

#[derive(serde::Serialize)]
struct TemplateInspection {
    owner_shape: String,
    predicate: String,
    component: Option<u64>,
    bindings: Vec<TemplateBindingView>,
    messages: Vec<RenderedTemplate>,
    label_template: Option<RenderedTemplate>,
}

#[derive(serde::Serialize)]
struct TemplateBindingView {
    name: String,
    values: Vec<String>,
    from_default: bool,
}

#[derive(serde::Serialize)]
struct RenderedTemplate {
    raw: String,
    rendered: Option<String>,
}

fn collect_template_inspection(
    program: &shifty_shacl_core::algebra::ShapeProgram,
) -> Vec<TemplateInspection> {
    program
        .constraints
        .iter()
        .filter_map(|constraint| {
            let shifty_shacl_core::algebra::ConstraintExpr::CustomComponent {
                predicate,
                component,
                bindings,
                message_templates,
                label_template,
                ..
            } = &constraint.expr
            else {
                return None;
            };
            if bindings.is_empty() && message_templates.is_empty() && label_template.is_none() {
                return None;
            }
            let owner_shape = program
                .shapes
                .iter()
                .find(|shape| shape.id == constraint.owner)
                .map(|shape| shape.normalized_key.clone())
                .unwrap_or_else(|| format!("shape:{}", constraint.owner.0));
            Some(TemplateInspection {
                owner_shape,
                predicate: predicate.to_string(),
                component: component.map(|id| id.0),
                bindings: bindings
                    .iter()
                    .map(|binding| TemplateBindingView {
                        name: binding.name.clone(),
                        values: binding
                            .values
                            .iter()
                            .map(|value| value.to_string())
                            .collect(),
                        from_default: binding.from_default,
                    })
                    .collect(),
                messages: message_templates
                    .iter()
                    .map(|template| RenderedTemplate {
                        raw: template.raw.clone(),
                        rendered: render_template(template, bindings),
                    })
                    .collect(),
                label_template: label_template.as_ref().map(|template| RenderedTemplate {
                    raw: template.raw.clone(),
                    rendered: render_template(template, bindings),
                }),
            })
        })
        .collect()
}

fn render_template(
    template: &shifty_shacl_core::algebra::Template,
    bindings: &[shifty_shacl_core::algebra::TemplateBinding],
) -> Option<String> {
    let mut out = String::new();
    let mut saw_slot = false;
    for part in &template.parts {
        match part {
            shifty_shacl_core::algebra::TemplatePart::Text(text) => out.push_str(text),
            shifty_shacl_core::algebra::TemplatePart::Slot { name, .. } => {
                saw_slot = true;
                if let Some(binding) = bindings.iter().find(|binding| binding.name == *name) {
                    out.push_str(
                        &binding
                            .values
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", "),
                    );
                } else {
                    out.push_str(&format!("{{unbound:{name}}}"));
                }
            }
        }
    }
    saw_slot.then_some(out)
}

fn render_slice_reason(reason: &SliceReason, analysis: &AnalysisSummary) -> String {
    match reason {
        SliceReason::Root { roots } => {
            format!("root ({})", render_root_labels(roots, analysis))
        }
        SliceReason::ReachableFromRoots { roots } => {
            format!("reachable from {}", render_root_labels(roots, analysis))
        }
        SliceReason::OwnedByRetainedShape { owner, roots } => format!(
            "owned by {} from {}",
            analysis
                .shape_labels
                .get(owner)
                .cloned()
                .unwrap_or_else(|| format!("shape:{}", owner.0)),
            render_root_labels(roots, analysis)
        ),
        SliceReason::ReferencedByRetainedConstraints { constraints } => format!(
            "referenced by retained constraints {}",
            constraints
                .iter()
                .map(|id| id.0.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        SliceReason::UnreachableFromRoots => "unreachable from selected roots".to_string(),
        SliceReason::OwnerShapeDropped { owner } => format!(
            "owner {} dropped",
            analysis
                .shape_labels
                .get(owner)
                .cloned()
                .unwrap_or_else(|| format!("shape:{}", owner.0))
        ),
        SliceReason::UnreferencedAfterSlicing => "unreferenced after slicing".to_string(),
    }
}

fn render_root_labels(
    roots: &[shifty_shacl_core::algebra::ShapeId],
    analysis: &AnalysisSummary,
) -> String {
    roots
        .iter()
        .map(|root| {
            analysis
                .shape_labels
                .get(root)
                .cloned()
                .unwrap_or_else(|| format!("shape:{}", root.0))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_context_footprint(footprint: &shifty_shacl_core::ContextFootprint) -> &'static str {
    match footprint {
        shifty_shacl_core::ContextFootprint::TargetOnly => "target_only",
        shifty_shacl_core::ContextFootprint::NodeLocal => "node_local",
        shifty_shacl_core::ContextFootprint::SingleHopPath => "single_hop_path",
        shifty_shacl_core::ContextFootprint::BoundedTraversal => "bounded_traversal",
        shifty_shacl_core::ContextFootprint::ShapeReferenceTraversal => "shape_reference_traversal",
        shifty_shacl_core::ContextFootprint::RecursiveNeighborhood => "recursive_neighborhood",
        shifty_shacl_core::ContextFootprint::GlobalSparql => "global_sparql",
    }
}

fn render_cost_hint(hint: &StaticCostHint) -> &'static str {
    match hint {
        StaticCostHint::EntryShape => "entry_shape",
        StaticCostHint::HelperShape => "helper_shape",
        StaticCostHint::HasSparql => "has_sparql",
        StaticCostHint::Recursive => "recursive",
        StaticCostHint::ClosedShape => "closed_shape",
        StaticCostHint::QualifiedValueShape => "qualified_value_shape",
        StaticCostHint::HasRules => "has_rules",
        StaticCostHint::DuplicateConstraintWork => "duplicate_constraint_work",
        StaticCostHint::DuplicateTargetWork => "duplicate_target_work",
        StaticCostHint::DuplicateRuleWork => "duplicate_rule_work",
        StaticCostHint::SingleHopPath => "single_hop_path",
        StaticCostHint::BoundedTraversal => "bounded_traversal",
        StaticCostHint::ShapeReferenceTraversal => "shape_reference_traversal",
    }
}

fn render_rewrite_text(
    summary: &RewriteSummary,
    program: &shifty_shacl_core::algebra::ShapeProgram,
) -> String {
    let mut out = String::new();
    out.push_str("Rewrite Summary\n");
    out.push_str(&format!("  shapes: {}\n", program.shapes.len()));
    out.push_str(&format!("  constraints: {}\n", program.constraints.len()));
    out.push_str(&format!("  targets: {}\n", program.targets.len()));
    out.push_str(&format!("  rules: {}\n", program.rules.len()));
    out.push_str(&format!(
        "  components: {}\n",
        program.constraint_components.len()
    ));
    out.push_str(&format!("  shapes removed: {}\n", summary.shapes_removed));
    out.push_str(&format!(
        "  constraints removed: {}\n",
        summary.constraints_removed
    ));
    out.push_str(&format!("  targets removed: {}\n", summary.targets_removed));
    out.push_str(&format!("  rules removed: {}\n", summary.rules_removed));
    out.push_str(&format!(
        "  components removed: {}\n",
        summary.components_removed
    ));

    out.push_str("\nPasses\n");
    for pass in &summary.passes {
        out.push_str(&format!("  {} changed={}\n", pass.pass, pass.changed));
        for detail in &pass.details {
            out.push_str(&format!("    {}\n", detail));
        }
    }

    out.push_str("\nRecursive Regions\n");
    if summary.recursive_regions.is_empty() {
        out.push_str("  none\n");
    } else {
        for region in &summary.recursive_regions {
            let labels = region
                .shapes
                .iter()
                .filter_map(|shape_id| {
                    program
                        .shapes
                        .iter()
                        .find(|shape| shape.id == *shape_id)
                        .map(|shape| shape.normalized_key.clone())
                })
                .collect::<Vec<_>>();
            out.push_str(&format!("  {}: {}\n", region.id, labels.join(", ")));
        }
    }

    out
}

fn render_backend_views_text(views: &BackendViews) -> String {
    let mut out = String::new();
    out.push_str(&render_validation_view_text(&views.validation));
    out.push('\n');
    out.push_str(&render_inference_view_text(&views.inference));
    out
}

fn render_validation_view_text(view: &ValidationView) -> String {
    let mut out = String::new();
    out.push_str("Validation View\n");
    out.push_str(&format!(
        "  closure_mode: {}\n",
        render_backend_closure_mode(&view.closure_mode)
    ));
    out.push_str(&format!("  shapes: {}\n", view.program.shapes.len()));
    out.push_str(&format!(
        "  constraints: {}\n",
        view.program.constraints.len()
    ));
    out.push_str(&format!("  targets: {}\n", view.program.targets.len()));
    out.push_str(&format!("  rules: {}\n", view.program.rules.len()));
    out.push_str(&format!("  entry_shapes: {}\n", view.entry_shapes.len()));
    out.push_str(&format!("  helper_shapes: {}\n", view.helper_shapes.len()));
    out.push_str(&format!(
        "  recursive_regions: {}\n",
        view.recursive_regions.len()
    ));
    out.push_str(&format!(
        "  shared_work_units: {}\n",
        view.shared_work_units.len()
    ));
    out.push_str("\n  Buckets\n");
    for (bucket, count) in &view.partition.histogram {
        out.push_str(&format!("    {}: {}\n", bucket, count));
    }
    out.push_str("\n  Dependency Classes\n");
    for (class, count) in dependency_class_histogram(&view.dependencies) {
        out.push_str(&format!("    {}: {}\n", class, count));
    }
    out.push_str("\n  Work Inventory\n");
    out.push_str(&format!(
        "    local_checks: {}\n",
        view.work_inventory.local_checks
    ));
    out.push_str(&format!(
        "    traversal_checks: {}\n",
        view.work_inventory.traversal_checks
    ));
    out.push_str(&format!(
        "    global_checks: {}\n",
        view.work_inventory.global_checks
    ));
    out.push_str(&format!(
        "    shared_work_units: {}\n",
        view.work_inventory.shared_work_units
    ));
    if !view.shared_work_units.is_empty() {
        out.push_str("\n  Shared Work Units\n");
        for unit in &view.shared_work_units {
            out.push_str(&format!(
                "    {} {} owners={} members={}\n",
                unit.id,
                render_shared_work_unit_kind(&unit.kind),
                unit.owner_shapes.len(),
                unit.member_ids.len()
            ));
        }
    }
    out
}

fn render_inference_view_text(view: &InferenceView) -> String {
    let mut out = String::new();
    out.push_str("Inference View\n");
    out.push_str(&format!(
        "  closure_mode: {}\n",
        render_backend_closure_mode(&view.closure_mode)
    ));
    out.push_str(&format!("  shapes: {}\n", view.program.shapes.len()));
    out.push_str(&format!(
        "  constraints: {}\n",
        view.program.constraints.len()
    ));
    out.push_str(&format!("  targets: {}\n", view.program.targets.len()));
    out.push_str(&format!("  rules: {}\n", view.program.rules.len()));
    out.push_str(&format!(
        "  rule_owner_shapes: {}\n",
        view.rule_owner_shapes.len()
    ));
    out.push_str(&format!(
        "  condition_shapes: {}\n",
        view.condition_shapes.len()
    ));
    out.push_str(&format!(
        "  target_seed_shapes: {}\n",
        view.target_seed_shapes.len()
    ));
    out.push_str(&format!(
        "  recursive_regions: {}\n",
        view.recursive_regions.len()
    ));
    out.push_str(&format!(
        "  shared_work_units: {}\n",
        view.shared_work_units.len()
    ));
    out.push_str("\n  Shape Buckets\n");
    for (bucket, count) in &view.partition.histogram {
        out.push_str(&format!("    {}: {}\n", bucket, count));
    }
    out.push_str("\n  Rule Buckets\n");
    let mut histogram = std::collections::BTreeMap::<String, usize>::new();
    for bucket in view.rule_buckets.values() {
        *histogram
            .entry(render_backend_bucket(bucket).to_string())
            .or_insert(0) += 1;
    }
    if histogram.is_empty() {
        out.push_str("    none\n");
    } else {
        for (bucket, count) in histogram {
            out.push_str(&format!("    {}: {}\n", bucket, count));
        }
    }
    out.push_str("\n  Dependency Classes\n");
    for (class, count) in dependency_class_histogram(&view.dependencies) {
        out.push_str(&format!("    {}: {}\n", class, count));
    }
    out.push_str("\n  Work Inventory\n");
    out.push_str(&format!(
        "    seed_shapes: {}\n",
        view.work_inventory.seed_shapes
    ));
    out.push_str(&format!(
        "    rule_clusters: {}\n",
        view.work_inventory.rule_clusters
    ));
    out.push_str(&format!(
        "    recursive_regions: {}\n",
        view.work_inventory.recursive_regions
    ));
    out.push_str(&format!(
        "    local_rules: {}\n",
        view.work_inventory.local_rules
    ));
    out.push_str(&format!(
        "    traversal_rules: {}\n",
        view.work_inventory.traversal_rules
    ));
    out.push_str(&format!(
        "    global_rules: {}\n",
        view.work_inventory.global_rules
    ));
    out.push_str(&format!(
        "    shared_work_units: {}\n",
        view.work_inventory.shared_work_units
    ));
    if !view.shared_work_units.is_empty() {
        out.push_str("\n  Shared Work Units\n");
        for unit in &view.shared_work_units {
            out.push_str(&format!(
                "    {} {} owners={} members={}\n",
                unit.id,
                render_shared_work_unit_kind(&unit.kind),
                unit.owner_shapes.len(),
                unit.member_ids.len()
            ));
        }
    }
    out
}

fn render_backend_bucket(bucket: &shifty_shacl_core::BackendBucket) -> &'static str {
    match bucket {
        shifty_shacl_core::BackendBucket::LocalOnly => "local_only",
        shifty_shacl_core::BackendBucket::BoundedTraversal => "bounded_traversal",
        shifty_shacl_core::BackendBucket::ShapeReference => "shape_reference",
        shifty_shacl_core::BackendBucket::GlobalSparql => "global_sparql",
        shifty_shacl_core::BackendBucket::Recursive => "recursive",
    }
}

fn render_backend_closure_mode(mode: &BackendClosureMode) -> &'static str {
    match mode {
        BackendClosureMode::TargetRoots => "target_roots",
        BackendClosureMode::ValidationClosure => "validation_closure",
        BackendClosureMode::InferenceClosure => "inference_closure",
        BackendClosureMode::MixedClosure => "mixed_closure",
    }
}

fn render_shared_work_unit_kind(kind: &SharedWorkUnitKind) -> &'static str {
    match kind {
        SharedWorkUnitKind::CustomComponentConstraint => "custom_component_constraint",
        SharedWorkUnitKind::SparqlConstraint => "sparql_constraint",
        SharedWorkUnitKind::RuleBody => "rule_body",
    }
}

fn dependency_class_histogram(
    dependencies: &[shifty_shacl_core::ClassifiedDependency],
) -> Vec<(String, usize)> {
    let mut histogram = std::collections::BTreeMap::<String, usize>::new();
    for dependency in dependencies {
        *histogram
            .entry(match dependency.class {
                DependencyClass::ValidationOnly => "validation_only".to_string(),
                DependencyClass::InferenceOnly => "inference_only".to_string(),
                DependencyClass::Shared => "shared".to_string(),
            })
            .or_insert(0) += 1;
    }
    histogram.into_iter().collect()
}

fn render_backend_plans_text(plans: &shifty_shacl_core::BackendPlans) -> String {
    let mut out = String::new();
    out.push_str(&render_validation_plan_text(&plans.validation));
    out.push('\n');
    out.push_str(&render_inference_plan_text(&plans.inference));
    out
}

fn render_validation_plan_text(plan: &ValidationPlan) -> String {
    let mut out = String::new();
    out.push_str("Validation Plan\n");
    out.push_str(&format!(
        "  planning_mode: {}\n",
        render_planning_mode(&plan.planning.estimation_mode)
    ));
    out.push_str(&format!(
        "  exact_target_estimates: {}\n",
        plan.planning.exact_target_estimates
    ));
    out.push_str(&format!(
        "  sampled_target_estimates: {}\n",
        plan.planning.sampled_target_estimates
    ));
    if let Some(sample_subject_budget) = plan.planning.sample_subject_budget {
        out.push_str(&format!(
            "  sample_subject_budget: {}\n",
            sample_subject_budget
        ));
    }
    if let Some(sampled_subjects) = plan.planning.sampled_subjects {
        out.push_str(&format!("  sampled_subjects: {}\n", sampled_subjects));
    }
    if let Some(sampled_quads) = plan.planning.sampled_quads {
        out.push_str(&format!("  sampled_quads: {}\n", sampled_quads));
    }
    out.push_str(&format!(
        "  indexed_shapes: {}\n",
        plan.planning.indexed_shapes
    ));
    out.push_str(&format!(
        "  indexed_constraints: {}\n",
        plan.planning.indexed_constraints
    ));
    out.push_str(&format!(
        "  indexed_targets: {}\n",
        plan.planning.indexed_targets
    ));
    out.push_str(&format!(
        "  indexed_direct_path_shapes: {}\n",
        plan.planning.indexed_direct_path_shapes
    ));
    out.push_str(&format!(
        "  closure_mode: {}\n",
        render_backend_closure_mode(&plan.view.closure_mode)
    ));
    out.push_str(&format!("  nodes: {}\n", plan.nodes.len()));
    for (kind, count) in &plan.summary.node_counts {
        out.push_str(&format!("  {}: {}\n", kind, count));
    }
    if !plan.nodes.is_empty() {
        out.push_str("\n  Steps\n");
        for node in &plan.nodes {
            match node {
                shifty_shacl_core::ValidationPlanNode::TargetScan { shape, targets } => {
                    let hint = plan.annotations.target_scans.get(&shape.0);
                    out.push_str(&format!(
                        "    target_scan shape={} targets={}",
                        shape.0,
                        targets.len()
                    ));
                    if let Some(hint) = hint {
                        out.push_str(&format!(
                            " estimated_focus={} empty={}",
                            hint.estimated_focus_nodes
                                .map(|count| count.to_string())
                                .unwrap_or_else(|| "?".to_string()),
                            hint.empty_scan
                                .map(|value| value.to_string())
                                .unwrap_or_else(|| "?".to_string())
                        ));
                    }
                    out.push('\n');
                }
                shifty_shacl_core::ValidationPlanNode::ConstraintBatch {
                    shape,
                    bucket,
                    constraints,
                } => {
                    let hint = plan.annotations.constraint_batches.get(&shape.0);
                    out.push_str(&format!(
                        "    constraint_batch shape={} bucket={} constraints={}",
                        shape.0,
                        render_backend_bucket(bucket),
                        constraints.len()
                    ));
                    if let Some(hint) = hint {
                        if !hint.fanout_hints.is_empty() {
                            let fanout = hint
                                .fanout_hints
                                .iter()
                                .map(|(predicate, class)| {
                                    format!("{predicate}={}", render_fanout_class(class))
                                })
                                .collect::<Vec<_>>()
                                .join(", ");
                            out.push_str(&format!(" fanout=[{}]", fanout));
                        }
                        if !hint.dead_constraint_candidates.is_empty() {
                            out.push_str(&format!(
                                " dead_candidates={}",
                                hint.dead_constraint_candidates.len()
                            ));
                        }
                        if !hint.vacuous_constraint_candidates.is_empty() {
                            out.push_str(&format!(
                                " vacuous_candidates={}",
                                hint.vacuous_constraint_candidates.len()
                            ));
                        }
                    }
                    out.push('\n');
                }
                shifty_shacl_core::ValidationPlanNode::SharedWorkUnit { unit_id } => {
                    out.push_str(&format!("    shared_work_unit {}\n", unit_id));
                }
                shifty_shacl_core::ValidationPlanNode::RecursiveRegion { region_id, shapes } => {
                    out.push_str(&format!(
                        "    recursive_region {} shapes={}\n",
                        region_id,
                        shapes.len()
                    ));
                }
            }
        }
    }
    out
}

fn render_data_plan_text(plan: &ValidationPlan) -> String {
    let mut out = String::new();
    out.push_str("Data Graph Summary\n");
    if let Some(summary) = &plan.data_summary {
        out.push_str(&format!("  quads: {}\n", summary.total_quads));
        out.push_str(&format!("  subjects: {}\n", summary.distinct_subjects));
        out.push_str(&format!("  predicates: {}\n", summary.distinct_predicates));
        out.push_str(&format!("  objects: {}\n", summary.distinct_objects));
        out.push_str(&format!(
            "  estimation_mode: {}\n",
            render_planning_mode(&summary.metadata.estimation_mode)
        ));
        out.push_str(&format!(
            "  exact_target_estimates: {}\n",
            summary.metadata.exact_target_estimates
        ));
        out.push_str(&format!(
            "  sampled_target_estimates: {}\n",
            summary.metadata.sampled_target_estimates
        ));
        if let Some(sample_subject_budget) = summary.metadata.sample_subject_budget {
            out.push_str(&format!(
                "  sample_subject_budget: {}\n",
                sample_subject_budget
            ));
        }
        if let Some(sampled_subjects) = summary.metadata.sampled_subjects {
            out.push_str(&format!("  sampled_subjects: {}\n", sampled_subjects));
        }
        if let Some(sampled_quads) = summary.metadata.sampled_quads {
            out.push_str(&format!("  sampled_quads: {}\n", sampled_quads));
        }
        out.push_str(&format!(
            "  target_estimates: {}\n",
            summary.target_estimates.len()
        ));
        let empty_scans = summary
            .shape_summaries
            .iter()
            .filter(|shape| shape.empty_target_scan == Some(true))
            .count();
        out.push_str(&format!("  empty_target_scans: {}\n", empty_scans));

        out.push_str("\nShape Data Hints\n");
        for shape in &summary.shape_summaries {
            out.push_str(&format!(
                "  {} focus={} empty={}\n",
                shape.label,
                shape
                    .estimated_focus_nodes
                    .map(|count| count.to_string())
                    .unwrap_or_else(|| "?".to_string()),
                shape
                    .empty_target_scan
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "?".to_string())
            ));
            if shape.sampled_estimate {
                out.push_str(" sampled=true");
            }
            out.push('\n');
            if !shape.fanout_hints.is_empty() {
                out.push_str(&format!(
                    "    fanout: {}\n",
                    shape
                        .fanout_hints
                        .iter()
                        .map(|(predicate, class)| format!(
                            "{predicate}={}",
                            render_fanout_class(class)
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            if !shape.dead_constraint_candidates.is_empty() {
                out.push_str(&format!(
                    "    dead_constraints: {}\n",
                    shape.dead_constraint_candidates.len()
                ));
            }
            if !shape.vacuous_constraint_candidates.is_empty() {
                out.push_str(&format!(
                    "    vacuous_constraints: {}\n",
                    shape.vacuous_constraint_candidates.len()
                ));
            }
        }
        out.push('\n');
    } else {
        out.push_str("  unavailable\n\n");
    }
    out.push_str(&render_validation_plan_text(plan));
    out
}

fn render_physical_plan_text(plan: &shifty_shacl_core_inmemory::InspectablePhysicalPlan) -> String {
    let mut out = String::new();
    out.push_str("Physical Plan\n");
    out.push_str(&format!("  shapes: {}\n", plan.shape_count));
    out.push_str(&format!("  constraints: {}\n", plan.constraint_count));
    out.push_str(&format!("  target_scans: {}\n", plan.target_scans.len()));
    out.push_str(&format!(
        "  constraint_batches: {}\n",
        plan.constraint_batches.len()
    ));
    out.push_str(&format!("  has_sparql_work: {}\n", plan.has_sparql_work));
    out.push_str(&format!(
        "  has_advanced_targets: {}\n",
        plan.has_advanced_targets
    ));
    out.push_str(&format!("  has_sparql_rules: {}\n", plan.has_sparql_rules));
    out.push('\n');
    out.push_str("  Target Scans\n");
    for scan in &plan.target_scans {
        out.push_str(&format!(
            "    shape={} targets={} empty={} cacheable={}\n",
            scan.shape.0,
            scan.targets.len(),
            scan.empty_scan,
            scan.cacheable
        ));
    }
    out.push('\n');
    out.push_str("  Constraint Batches\n");
    for batch in &plan.constraint_batches {
        out.push_str(&format!(
            "    shape={} bucket={} constraints={} dead={} vacuous={}\n",
            batch.shape.0,
            render_backend_bucket(&batch.bucket),
            batch.constraints.len(),
            batch.dead_constraints.len(),
            batch.vacuous_constraints.len()
        ));
    }
    out.push('\n');
    out.push_str("  Compiled Constraints\n");
    for op in &plan.constraint_ops {
        out.push_str(&format!(
            "    constraint={} kind={} compiled_as={} precompiled_regex={}\n",
            op.constraint.0, op.kind, op.compiled_kind, op.precompiled_regex
        ));
    }
    out.push('\n');
    out.push_str("  Compiled Rules\n");
    for rule in &plan.rule_plans {
        out.push_str(&format!(
            "    rule={} owner={} kind={} mode={} uses_conditions={} focus_stable={} condition_global={}",
            rule.rule_id.0,
            rule.owner_shape.0,
            rule.kind,
            rule.mode,
            rule.uses_conditions,
            rule.focus_stable,
            rule.condition_dependencies_global
        ));
        if !rule.dependency_predicates.is_empty() {
            out.push_str(&format!(
                " deps=[{}]",
                rule.dependency_predicates.join(", ")
            ));
        }
        if !rule.condition_dependencies.is_empty() {
            out.push_str(&format!(
                " condition_deps=[{}]",
                rule.condition_dependencies.join(", ")
            ));
        }
        out.push('\n');
    }
    out
}

fn render_fanout_class(class: &shifty_shacl_core::FanoutClass) -> &'static str {
    match class {
        shifty_shacl_core::FanoutClass::Empty => "empty",
        shifty_shacl_core::FanoutClass::Single => "single",
        shifty_shacl_core::FanoutClass::Bounded => "bounded",
        shifty_shacl_core::FanoutClass::Broad => "broad",
        shifty_shacl_core::FanoutClass::Unknown => "unknown",
    }
}

fn render_planning_mode(mode: &PlanningEstimationMode) -> &'static str {
    match mode {
        PlanningEstimationMode::Exact => "exact",
        PlanningEstimationMode::Sampled => "sampled",
    }
}

fn render_inference_plan_text(plan: &InferencePlan) -> String {
    let mut out = String::new();
    out.push_str("Inference Plan\n");
    out.push_str(&format!(
        "  closure_mode: {}\n",
        render_backend_closure_mode(&plan.view.closure_mode)
    ));
    out.push_str(&format!("  nodes: {}\n", plan.nodes.len()));
    for (kind, count) in &plan.summary.node_counts {
        out.push_str(&format!("  {}: {}\n", kind, count));
    }
    if !plan.nodes.is_empty() {
        out.push_str("\n  Steps\n");
        for node in &plan.nodes {
            match node {
                shifty_shacl_core::InferencePlanNode::SeedScan { shape, targets } => {
                    out.push_str(&format!(
                        "    seed_scan shape={} targets={}\n",
                        shape.0,
                        targets.len()
                    ));
                }
                shifty_shacl_core::InferencePlanNode::RuleBatch {
                    shape,
                    bucket,
                    rules,
                    condition_shapes,
                } => {
                    out.push_str(&format!(
                        "    rule_batch shape={} bucket={} rules={} condition_shapes={}\n",
                        shape.0,
                        render_backend_bucket(bucket),
                        rules.len(),
                        condition_shapes.len()
                    ));
                }
                shifty_shacl_core::InferencePlanNode::SharedWorkUnit { unit_id } => {
                    out.push_str(&format!("    shared_work_unit {}\n", unit_id));
                }
                shifty_shacl_core::InferencePlanNode::RecursiveRegion { region_id, shapes } => {
                    out.push_str(&format!(
                        "    recursive_region {} shapes={}\n",
                        region_id,
                        shapes.len()
                    ));
                }
            }
        }
    }
    out
}

fn render_validation_result_text(result: &ValidationResult) -> String {
    let mut out = String::new();
    out.push_str("Validation Result\n");
    out.push_str(&format!("  conforms: {}\n", result.conforms));
    out.push_str(&format!(
        "  focus_nodes_evaluated: {}\n",
        result.focus_nodes_evaluated
    ));
    out.push_str(&format!("  violations: {}\n", result.violations.len()));
    out.push_str(&format!("  unsupported: {}\n", result.unsupported.len()));
    out.push_str(&format!("  trace_events: {}\n", result.trace.len()));
    out.push_str("\nCoverage\n");
    out.push_str(&format!(
        "  executed_constraints: {}\n",
        result.coverage.executed_constraints
    ));
    out.push_str(&format!(
        "  executed_rules: {}\n",
        result.coverage.executed_rules
    ));
    out.push_str(&format!(
        "  unsupported_constraints: {}\n",
        result.coverage.unsupported_constraints
    ));
    out.push_str(&format!(
        "  deferred_recursions: {}\n",
        result.coverage.deferred_recursions
    ));
    out.push_str(&format!(
        "  inference_iterations: {}\n",
        result.coverage.inference_iterations
    ));
    out.push_str(&format!(
        "  inferred_triples: {}\n",
        result.coverage.inferred_triples
    ));
    if !result.coverage.unsupported_by_kind.is_empty() {
        out.push_str("  unsupported_by_kind:\n");
        for (kind, count) in &result.coverage.unsupported_by_kind {
            out.push_str(&format!("    {}: {}\n", kind, count));
        }
    }
    out.push_str("\nHeatmap\n");
    out.push_str(&format!(
        "  shapes_evaluated: {}\n",
        result.heatmap.shape_hits.len()
    ));
    out.push_str(&format!(
        "  constraints_evaluated: {}\n",
        result.heatmap.constraint_hits.len()
    ));
    out.push_str(&format!(
        "  rules_evaluated: {}\n",
        result.heatmap.rule_hits.len()
    ));
    if !result.violations.is_empty() {
        out.push_str("\nViolations\n");
        for violation in &result.violations {
            out.push_str(&format!(
                "  shape={} constraint={} focus={}\n",
                violation.shape.0,
                violation
                    .constraint
                    .map(|constraint| constraint.0.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                violation.focus_node
            ));
            out.push_str(&format!("    severity={:?}\n", violation.severity));
            if let Some(value_node) = &violation.value_node {
                out.push_str(&format!("    value={}\n", value_node));
            }
            if let Some(source) = &violation.source {
                out.push_str(&format!(
                    "    source={} locator={}\n",
                    source.graph_iri,
                    source.locator.as_deref().unwrap_or("-")
                ));
            }
            out.push_str(&format!("    {}\n", violation.message));
        }
    }
    if !result.unsupported.is_empty() {
        out.push_str("\nUnsupported\n");
        for unsupported in &result.unsupported {
            out.push_str(&format!(
                "  shape={} constraint={} kind={} focus={}\n",
                unsupported.shape.0,
                unsupported
                    .constraint
                    .map(|constraint| constraint.0.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                unsupported.kind,
                unsupported.focus_node.as_deref().unwrap_or("-")
            ));
            out.push_str(&format!("    severity={:?}\n", unsupported.severity));
            if let Some(source) = &unsupported.source {
                out.push_str(&format!(
                    "    source={} locator={}\n",
                    source.graph_iri,
                    source.locator.as_deref().unwrap_or("-")
                ));
            }
            out.push_str(&format!("    {}\n", unsupported.reason));
        }
    }
    if !result.trace.is_empty() {
        out.push_str("\nTrace\n");
        for event in result.trace.iter().take(20) {
            out.push_str(&format!(
                "  {:?} shape={} constraint={} focus={} {}\n",
                event.event,
                event
                    .shape
                    .map(|shape| shape.0.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                event
                    .constraint
                    .map(|constraint| constraint.0.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                event.focus_node.as_deref().unwrap_or("-"),
                event.message
            ));
        }
        if result.trace.len() > 20 {
            out.push_str(&format!("  ... {} more events\n", result.trace.len() - 20));
        }
    }
    out
}
