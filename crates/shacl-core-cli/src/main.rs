use clap::{Parser, Subcommand, ValueEnum};
use shifty_shacl_core::source::{ShapeSource, SourceLoadOptions, source_from_str};
use shifty_shacl_core::{
    AnalysisSummary, NormalizeOptions, RewriteOptions, RewriteSummary, SliceReason, SliceRoots,
    StaticAnalysisSummary, StaticCostHint, analyze_program, analyze_static_with_roots,
    load_and_parse_with_ontoenv, lower_to_program, normalize_program, parse_resolved,
    render_shape_program_dot, rewrite_program,
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
    /// Run deeper static analysis over the lowered shape program
    StaticAnalyze(StaticAnalyzeArgs),
    /// Rewrite the lowered shape program using static-analysis-guided passes
    Rewrite(RewriteArgs),
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Dump(args) => run_dump(args)?,
        Command::Analyze(args) => run_analyze(args)?,
        Command::StaticAnalyze(args) => run_static_analyze(args)?,
        Command::Rewrite(args) => run_rewrite(args)?,
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
