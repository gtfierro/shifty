//! CLI for the formalism-first SHACL engine.
//!
//! `inspect` visualizes how a shapes graph is transformed through the layers,
//! one `--stage` at a time. As later layers land (normalized, planned, …), they
//! become additional stages here.

use clap::{Args, Parser, Subcommand, ValueEnum};
use std::error::Error;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "shacl", about = "Formalism-first SHACL/SHACL-AF engine")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show a layer's view of a shapes graph.
    Inspect(InspectArgs),
    /// Validate a data graph against a shapes graph (normalized planned evaluator).
    Validate(ValidateArgs),
    /// Run SHACL-AF rule inference (forward chaining to a fixpoint).
    Infer(InferArgs),
    /// Show symbolic-repair structures for a data graph's violations.
    Repair(RepairArgs),
}

#[derive(Args)]
struct RepairArgs {
    /// Turtle shapes file(s) or URL(s) (repeatable).
    #[arg(long, value_name = "SHAPES", required = true, action = clap::ArgAction::Append)]
    shapes: Vec<String>,
    /// Turtle data file(s) or URL(s) (repeatable; defaults to shapes).
    #[arg(long, value_name = "DATA", action = clap::ArgAction::Append)]
    data: Vec<String>,
    /// Base IRI for parsing.
    #[arg(long)]
    base: Option<String>,
    /// Which repair structure to print.
    #[arg(long, value_enum, default_value_t = RepairStage::Tree)]
    stage: RepairStage,
    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,
    /// Skip SHACL-AF rule inference before witnessing.
    #[arg(long)]
    no_infer: bool,
    /// Run the fixpoint driver and emit the repaired data graph (N-Triples)
    /// instead of inspecting structures. Overrides `--stage`.
    #[arg(long)]
    apply: bool,
}

#[derive(Clone, Copy, ValueEnum)]
enum RepairStage {
    /// The witness tree per failing focus node (why each violates).
    Witness,
    /// The synthesized RepairTree per failing focus node (how to fix it).
    Tree,
    /// A concrete repair (ΔG) the enumeration driver finds for each focus.
    Solve,
}

#[derive(Args)]
struct InferArgs {
    /// Turtle shapes file(s) or URL(s) (repeatable).
    #[arg(long, value_name = "SHAPES", required = true, action = clap::ArgAction::Append)]
    shapes: Vec<String>,
    /// Turtle data file(s) or URL(s) (repeatable; defaults to shapes).
    #[arg(long, value_name = "DATA", action = clap::ArgAction::Append)]
    data: Vec<String>,
    /// Base IRI for parsing.
    #[arg(long)]
    base: Option<String>,
    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,
    /// Print shape, cache, and SPARQL execution telemetry after inference.
    #[arg(long)]
    profile: bool,
}

#[derive(Args)]
struct ValidateArgs {
    /// Turtle shapes file(s) or URL(s) (repeatable).
    #[arg(long, value_name = "SHAPES", required = true, action = clap::ArgAction::Append)]
    shapes: Vec<String>,
    /// Turtle data file(s) or URL(s) (repeatable; defaults to shapes).
    #[arg(long, value_name = "DATA", action = clap::ArgAction::Append)]
    data: Vec<String>,
    /// Base IRI for parsing.
    #[arg(long)]
    base: Option<String>,
    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,
    /// Emit a W3C `sh:ValidationReport` graph (N-Triples) instead of a summary.
    #[arg(long)]
    report: bool,
    /// Skip SHACL-AF rule inference before validation.
    #[arg(long)]
    no_infer: bool,
    /// RDF graph scope used during validation.
    #[arg(long, visible_alias = "graph-scope", value_enum, default_value_t = GraphMode::Union)]
    graph_mode: GraphMode,
    /// Lowest result severity that makes validation non-conforming.
    #[arg(long, value_enum, default_value_t = SeverityLevel::Info)]
    minimum_severity: SeverityLevel,
    /// Print shape, cache, and SPARQL execution telemetry after validation.
    #[arg(long)]
    profile: bool,
}

#[derive(Args)]
struct InspectArgs {
    /// Turtle shapes file.
    file: PathBuf,
    /// Which layer's representation to print.
    #[arg(long, value_enum, default_value_t = Stage::Algebra)]
    stage: Stage,
    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,
    /// Base IRI for parsing.
    #[arg(long)]
    base: Option<String>,
}

#[derive(Clone, Copy, ValueEnum)]
enum Stage {
    /// The raw parsed RDF triples (input to lowering).
    Rdf,
    /// The lowered formalism IR (Layer 2 output).
    Algebra,
    /// The normalized IR (Layer 4: CSE + simplification).
    Normalized,
    /// The recursion/stratification analysis (Layer 4).
    Strata,
    /// The physical plan (Layer 5: focus sources + cost-ordered checks).
    Plan,
    /// SPARQL capability classification: which constraint queries lower to the
    /// native executor vs. fall back to Spareval.
    Capability,
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Text,
    Json,
    /// Graphviz DOT (algebra-ast stage only).
    Dot,
}

#[derive(Clone, Copy, ValueEnum)]
enum GraphMode {
    /// Focus nodes and evaluation use only the data graph.
    Data,
    /// Focus nodes come from data; evaluation uses data + shapes.
    Union,
    /// Focus nodes and evaluation both use data + shapes.
    UnionAll,
}

#[derive(Clone, Copy, ValueEnum)]
enum SeverityLevel {
    Info,
    Warning,
    Violation,
}

impl From<SeverityLevel> for shifty_algebra::Severity {
    fn from(value: SeverityLevel) -> Self {
        match value {
            SeverityLevel::Info => Self::Info,
            SeverityLevel::Warning => Self::Warning,
            SeverityLevel::Violation => Self::Violation,
        }
    }
}

impl From<GraphMode> for shifty_engine::ValidationGraphMode {
    fn from(mode: GraphMode) -> Self {
        match mode {
            GraphMode::Data => Self::Data,
            GraphMode::Union => Self::Union,
            GraphMode::UnionAll => Self::UnionAll,
        }
    }
}

fn main() -> ExitCode {
    env_logger::init();
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn Error>> {
    match cli.command {
        Command::Inspect(args) => inspect(args),
        Command::Validate(args) => validate(args),
        Command::Infer(args) => infer(args),
        Command::Repair(args) => repair(args),
    }
}

fn fetch_bytes(src: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    if src.starts_with("http://") || src.starts_with("https://") {
        let response = ureq::get(src).call()?;
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut response.into_reader(), &mut bytes)?;
        Ok(bytes)
    } else {
        Ok(std::fs::read(src)?)
    }
}

fn load_sources(
    sources: &[String],
    base: Option<&str>,
) -> Result<shifty_parse::Loaded, Box<dyn Error>> {
    let mut merged: Option<shifty_parse::Loaded> = None;
    for src in sources {
        let bytes = fetch_bytes(src)?;
        let loaded = shifty_parse::load_turtle(&bytes, base)?;
        match merged.as_mut() {
            None => merged = Some(loaded),
            Some(m) => m.merge_from(&loaded),
        }
    }
    merged.ok_or_else(|| "no sources provided".into())
}

fn infer(args: InferArgs) -> Result<(), Box<dyn Error>> {
    if args.profile {
        shifty_engine::profile::enable();
    }
    let base = args.base.as_deref();
    let shapes = load_sources(&args.shapes, base)?;
    let parsed = shifty_parse::parse_loaded(&shapes);
    let normalized = shifty_opt::normalize(&parsed.schema);
    for d in &parsed.diagnostics {
        eprintln!("{d}");
    }

    let outcome = if args.data.is_empty() {
        shifty_engine::infer(&shapes.graph, &normalized)
    } else {
        let data = load_sources(&args.data, base)?;
        shifty_engine::infer_graphs(&data.graph, &shapes.graph, &normalized)
    };
    let outcome = match outcome {
        Ok(o) => o,
        Err(e) => return Err(format!("{e}; cannot infer (see `inspect --stage strata`)").into()),
    };
    for d in &outcome.diagnostics {
        eprintln!("warning: {d}");
    }

    match args.format {
        Format::Dot => return Err("--format dot is not supported for infer".into()),
        Format::Json => {
            let triples: Vec<_> = outcome
                .inferred
                .iter()
                .map(|t| {
                    serde_json::json!({
                        "subject": t.subject.to_string(),
                        "predicate": t.predicate.to_string(),
                        "object": t.object.to_string(),
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&triples)?);
        }
        Format::Text => {
            println!("inferred {} triple(s):", outcome.inferred.len());
            let mut lines: Vec<String> = outcome.inferred.iter().map(|t| t.to_string()).collect();
            lines.sort();
            for line in lines {
                println!("  {line}");
            }
        }
    }
    if args.profile
        && let Some(col) = shifty_engine::profile::take()
    {
        col.print_summary();
    }
    Ok(())
}

fn render_reason(r: &shifty_engine::Reason, indent: usize) -> Vec<String> {
    let pad = " ".repeat(indent);
    let header = match &r.path {
        Some(p) => format!("{pad}- [{}] ({p}) {} → {}", r.severity, r.value, r.message),
        None => format!("{pad}- [{}] {}", r.severity, r.message),
    };
    let mut lines = vec![header];
    for sub in &r.sub_reasons {
        lines.extend(render_reason(sub, indent + 4));
    }
    lines
}

fn validate(args: ValidateArgs) -> Result<(), Box<dyn Error>> {
    if args.profile {
        shifty_engine::profile::enable();
    }
    let base = args.base.as_deref();
    let shapes_loaded = load_sources(&args.shapes, base)?;
    let parsed = shifty_parse::parse_loaded(&shapes_loaded);
    let normalized = shifty_opt::normalize(&parsed.schema);
    for d in &parsed.diagnostics {
        eprintln!("{d}");
    }
    let graph_mode = args.graph_mode.into();
    let threshold: shifty_algebra::Severity = args.minimum_severity.into();
    let validation_options = shifty_engine::ValidationOptions {
        minimum_severity: threshold.clone(),
        sort_results: true,
    };

    let data_loaded = if args.data.is_empty() {
        None
    } else {
        Some(load_sources(&args.data, base)?)
    };
    let inference = if args.no_infer {
        None
    } else {
        let outcome = match data_loaded.as_ref() {
            Some(data) => {
                shifty_engine::infer_graphs(&data.graph, &shapes_loaded.graph, &normalized)
            }
            None => shifty_engine::infer(&shapes_loaded.graph, &normalized),
        };
        match outcome {
            Ok(outcome) => Some(outcome),
            Err(e) => {
                return Err(format!(
                    "{e}; cannot infer before validation (see `inspect --stage strata`)"
                )
                .into());
            }
        }
    };
    if let Some(inference) = &inference {
        for d in &inference.diagnostics {
            eprintln!("warning: {d}");
        }
    }
    let data_graph = inference.as_ref().map_or_else(
        || {
            data_loaded
                .as_ref()
                .map_or(&shapes_loaded.graph, |data| &data.graph)
        },
        |inference| &inference.graph,
    );

    // W3C report mode: component-granular validator + RDF report output.
    if args.report {
        let report = if data_loaded.is_some() {
            shifty_engine::validate_report_graphs_with_mode_and_options(
                &shapes_loaded,
                data_graph,
                graph_mode,
                &validation_options,
            )
        } else {
            shifty_engine::validate_report_with_options(
                &shapes_loaded,
                data_graph,
                &validation_options,
            )
        };
        let graph = shifty_engine::report_to_graph(&report);
        // Collect prefixes from shapes + data, deduplicating by name.
        // Fall back to standard entries for sh:/rdf:/xsd: if not declared.
        let mut prefixes: Vec<(&str, &str)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (name, iri) in shapes_loaded.prefixes.iter().chain(
            data_loaded
                .as_ref()
                .map(|d| d.prefixes.iter())
                .into_iter()
                .flatten(),
        ) {
            if seen.insert(name.as_str()) {
                prefixes.push((name.as_str(), iri.as_str()));
            }
        }
        for (name, iri) in [
            ("sh", "http://www.w3.org/ns/shacl#"),
            ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
            ("xsd", "http://www.w3.org/2001/XMLSchema#"),
        ] {
            if seen.insert(name) {
                prefixes.push((name, iri));
            }
        }
        let mut ser = oxttl::TurtleSerializer::new();
        for (name, iri) in &prefixes {
            ser = ser.with_prefix(*name, *iri).unwrap();
        }
        let bytes = graph
            .iter()
            .try_fold(ser.for_writer(Vec::new()), |mut s, triple| {
                s.serialize_triple(triple).map(|()| s)
            })?
            .finish()?;
        print!("{}", String::from_utf8_lossy(&bytes));
        return Ok(());
    }

    let physical = shifty_opt::plan(&normalized);
    let mut outcome = match if data_loaded.is_some() {
        shifty_engine::validate_plan_graphs_with_mode_and_options(
            data_graph,
            &shapes_loaded.graph,
            &physical,
            graph_mode,
            &validation_options,
        )
    } else {
        shifty_engine::validate_plan_with_options(data_graph, &physical, &validation_options)
    } {
        Ok(o) => o,
        Err(e) => {
            return Err(format!("{e}; cannot validate (see `inspect --stage strata`)").into());
        }
    };

    // The engine retains every finding; `--minimum-severity` scopes both
    // `conforms` (already applied) and what we display/serialize here. Drop
    // findings below the threshold; a violation's own severity is the max of its
    // reasons, so any violation that survives keeps at least its top reason.
    outcome.violations.retain(|v| v.severity.meets(&threshold));
    for v in &mut outcome.violations {
        v.reasons.retain(|r| r.severity.meets(&threshold));
    }

    match args.format {
        Format::Dot => return Err("--format dot is not supported for validate".into()),
        Format::Json => println!("{}", serde_json::to_string_pretty(&outcome)?),
        Format::Text => {
            println!("conforms: {}", outcome.conforms);
            if !outcome.violations.is_empty() {
                println!("violations: {}", outcome.violations.len());
                for v in &outcome.violations {
                    let st = &parsed.schema.statements[v.statement];
                    println!(
                        "  {}  [severity: {}; target: {}]",
                        v.focus,
                        v.severity,
                        shifty_algebra::render::selector_to_string_in(
                            &st.selector,
                            &parsed.schema.arena
                        )
                    );
                    let mut groups: Vec<Vec<String>> =
                        v.reasons.iter().map(|r| render_reason(r, 6)).collect();
                    groups.sort_by(|a, b| a[0].cmp(&b[0]));
                    for group in groups {
                        for line in group {
                            println!("{line}");
                        }
                    }
                }
            }
        }
    }

    if args.profile
        && let Some(col) = shifty_engine::profile::take()
    {
        col.print_summary();
    }
    Ok(())
}

fn repair(args: RepairArgs) -> Result<(), Box<dyn Error>> {
    let base = args.base.as_deref();
    let shapes_loaded = load_sources(&args.shapes, base)?;
    let parsed = shifty_parse::parse_loaded(&shapes_loaded);
    for d in &parsed.diagnostics {
        eprintln!("{d}");
    }
    let schema = &parsed.schema;

    let data_loaded = if args.data.is_empty() {
        None
    } else {
        Some(load_sources(&args.data, base)?)
    };

    // Witness/synthesize against the (optionally inferred) data graph.
    let inference = if args.no_infer {
        None
    } else {
        let outcome = match data_loaded.as_ref() {
            Some(data) => shifty_engine::infer_graphs(&data.graph, &shapes_loaded.graph, schema),
            None => shifty_engine::infer(&shapes_loaded.graph, schema),
        };
        match outcome {
            Ok(o) => Some(o),
            Err(e) => {
                return Err(format!("{e}; cannot infer (see `inspect --stage strata`)").into());
            }
        }
    };
    let data_graph = inference.as_ref().map_or_else(
        || {
            data_loaded
                .as_ref()
                .map_or(&shapes_loaded.graph, |d| &d.graph)
        },
        |inf| &inf.graph,
    );

    // --apply: run the fixpoint driver and emit the repaired graph.
    if args.apply {
        let result = match shifty_engine::repair_to_fixpoint(
            data_graph,
            schema,
            shifty_engine::EnumOptions::default(),
        ) {
            Ok(r) => r,
            Err(e) => {
                return Err(format!("{e}; cannot repair (see `inspect --stage strata`)").into());
            }
        };
        let mut lines: Vec<String> = result.graph.iter().map(|t| t.to_string()).collect();
        lines.sort();
        for line in lines {
            println!("{line}");
        }
        eprintln!(
            "repaired: applied {} repair(s) over {} iteration(s); {} violation(s) remain",
            result.applied.len(),
            result.iterations,
            result.remaining,
        );
        return Ok(());
    }

    let witnesses = match shifty_engine::witness_violations(data_graph, schema) {
        Ok(ws) => ws,
        Err(e) => {
            return Err(format!("{e}; cannot witness (see `inspect --stage strata`)").into());
        }
    };

    if matches!(args.format, Format::Dot) {
        return Err("--format dot is not supported for repair".into());
    }

    let target = |statement: usize| {
        shifty_algebra::render::selector_to_string_in(
            &schema.statements[statement].selector,
            &schema.arena,
        )
    };

    match args.stage {
        RepairStage::Witness => match args.format {
            Format::Json => println!("{}", serde_json::to_string_pretty(&witnesses)?),
            Format::Text => {
                if witnesses.is_empty() {
                    println!("conforms: no violations to witness");
                }
                for fw in &witnesses {
                    println!("{}  [target: {}]", fw.focus, target(fw.statement));
                    for line in render_witness(&fw.failure, 2) {
                        println!("{line}");
                    }
                }
            }
            Format::Dot => unreachable!(),
        },
        RepairStage::Tree => {
            let trees: Vec<(&shifty_engine::FocusWitness, shifty_repair::RepairTree)> = witnesses
                .iter()
                .map(|fw| (fw, shifty_engine::synthesize(&schema.arena, fw)))
                .collect();
            match args.format {
                Format::Json => {
                    let arr: Vec<_> = trees
                        .iter()
                        .map(|(fw, t)| {
                            serde_json::json!({
                                "focus": fw.focus.to_string(),
                                "statement": fw.statement,
                                "tree": t,
                            })
                        })
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&arr)?);
                }
                Format::Text => {
                    if trees.is_empty() {
                        println!("conforms: no violations to repair");
                    }
                    for (fw, t) in &trees {
                        println!("{}  [target: {}]", fw.focus, target(fw.statement));
                        for line in render_tree(t, &schema.arena, 2) {
                            println!("{line}");
                        }
                    }
                }
                Format::Dot => unreachable!(),
            }
        }
        RepairStage::Solve => {
            let opts = shifty_engine::EnumOptions::default();
            let mut json_items = Vec::new();
            if witnesses.is_empty() {
                match args.format {
                    Format::Json => println!("[]"),
                    _ => println!("conforms: no violations to repair"),
                }
            }
            for fw in &witnesses {
                let tree = shifty_engine::synthesize(&schema.arena, fw);
                let sol = match shifty_engine::enumerate_repair(&tree, data_graph, schema, opts) {
                    Ok(s) => s,
                    Err(e) => return Err(format!("{e}; cannot solve").into()),
                };
                match args.format {
                    Format::Text => {
                        println!("{}  [target: {}]", fw.focus, target(fw.statement));
                        match &sol {
                            Some(s) => {
                                println!(
                                    "  repair (fixes {}, introduces {}):",
                                    s.outcome.fixed.len(),
                                    s.outcome.introduced.len()
                                );
                                for t in &s.delta.delete {
                                    println!("    del  {t}");
                                }
                                for t in &s.delta.add {
                                    println!("    add  {t}");
                                }
                            }
                            None => println!("  no repair found within budget"),
                        }
                    }
                    Format::Json => json_items.push(serde_json::json!({
                        "focus": fw.focus.to_string(),
                        "statement": fw.statement,
                        "repair": sol.as_ref().map(|s| serde_json::json!({
                            "add": s.delta.add.iter().map(|t| t.to_string()).collect::<Vec<_>>(),
                            "delete": s.delta.delete.iter().map(|t| t.to_string()).collect::<Vec<_>>(),
                            "fixed": s.outcome.fixed.len(),
                            "introduced": s.outcome.introduced.len(),
                        })),
                    })),
                    Format::Dot => unreachable!(),
                }
            }
            if matches!(args.format, Format::Json) {
                println!("{}", serde_json::to_string_pretty(&json_items)?);
            }
        }
    }
    Ok(())
}

fn path_str(p: &shifty_algebra::Path) -> String {
    shifty_algebra::render::path_to_string(p)
}

fn render_witness(w: &shifty_engine::Witness, indent: usize) -> Vec<String> {
    use shifty_engine::Witness as W;
    let pad = " ".repeat(indent);
    let mut out = Vec::new();
    match w {
        W::Atom {
            node,
            reached_by,
            produced_by,
            ..
        } => out.push(format!(
            "{pad}Atom at {node} via {}{}",
            path_str(reached_by),
            if produced_by.is_some() {
                " [cuttable]"
            } else {
                ""
            }
        )),
        W::Relational {
            kind, offending, ..
        } => out.push(format!(
            "{pad}Relational {kind:?}: {} offending pair(s)",
            offending.len()
        )),
        W::Closed { offenders, .. } => {
            out.push(format!(
                "{pad}Closed: {} disallowed triple(s)",
                offenders.len()
            ));
            for (p, o) in offenders {
                out.push(format!("{pad}  - {p} {o}"));
            }
        }
        W::Not { inner, .. } => {
            out.push(format!("{pad}Not — falsify the inner shape:"));
            out.extend(render_sat(inner, indent + 2));
        }
        W::All { failed, .. } => {
            out.push(format!("{pad}All — fix every:"));
            for f in failed {
                out.extend(render_witness(f, indent + 2));
            }
        }
        W::Any { branches, .. } => {
            out.push(format!("{pad}Any — fix any one of:"));
            for b in branches {
                out.extend(render_witness(b, indent + 2));
            }
        }
        W::CountLow {
            path, have, min, ..
        } => out.push(format!(
            "{pad}CountLow along {}: have {have}, need {min}",
            path_str(path)
        )),
        W::CountHigh {
            path,
            matched,
            max,
            per_value,
            ..
        } => {
            out.push(format!(
                "{pad}CountHigh along {}: {} match(es), max {max}",
                path_str(path),
                matched.len()
            ));
            for (v, sub) in per_value {
                out.push(format!("{pad}  value {v}:"));
                out.extend(render_witness(sub, indent + 4));
            }
        }
        W::Opaque { .. } => out.push(format!("{pad}Opaque (SPARQL) — no algebraic witness")),
    }
    out
}

fn render_sat(s: &shifty_engine::SatTrace, indent: usize) -> Vec<String> {
    use shifty_engine::SatTrace as S;
    let pad = " ".repeat(indent);
    let mut out = Vec::new();
    match s {
        S::Irrefutable { .. } => out.push(format!("{pad}Irrefutable (⊤)")),
        S::Atom { node, .. } => out.push(format!("{pad}Atom holds at {node} [cut to break]")),
        S::AllHeld { children, .. } => {
            out.push(format!("{pad}AllHeld — break any one:"));
            for c in children {
                out.extend(render_sat(c, indent + 2));
            }
        }
        S::AnyHeld { satisfied, .. } => {
            out.push(format!("{pad}AnyHeld — break every:"));
            for c in satisfied {
                out.extend(render_sat(c, indent + 2));
            }
        }
        S::CountHeld { matches, .. } => {
            out.push(format!("{pad}CountHeld: {} match(es)", matches.len()))
        }
        S::NotHeld { inner_fails, .. } => {
            out.push(format!("{pad}NotHeld — make the inner shape hold:"));
            out.extend(render_witness(inner_fails, indent + 2));
        }
        S::Blocked { reason, .. } => out.push(format!("{pad}Blocked: {reason:?}")),
        S::Coinductive { .. } => out.push(format!("{pad}Coinductive (gfp back-edge)")),
    }
    out
}

fn render_tree(
    t: &shifty_repair::RepairTree,
    arena: &shifty_algebra::ShapeArena,
    indent: usize,
) -> Vec<String> {
    use shifty_repair::RepairTree as T;
    let pad = " ".repeat(indent);
    let mut out = Vec::new();
    match t {
        T::Noop(_) => out.push(format!("{pad}Noop")),
        T::Blocked(_, r) => out.push(format!("{pad}Blocked: {r:?}")),
        T::Edits { edits, holes, .. } => {
            out.push(format!("{pad}Edits:"));
            for e in edits {
                out.push(format!("{pad}  {}", edit_str(e)));
            }
            for (h, c) in holes {
                out.push(format!("{pad}  ?{} : {}", h.0, constraint_str(c, arena)));
            }
        }
        T::All { children, .. } => {
            out.push(format!("{pad}All — do all:"));
            for c in children {
                out.extend(render_tree(c, arena, indent + 2));
            }
        }
        T::Any { children, .. } => {
            out.push(format!("{pad}Any — choose one:"));
            for c in children {
                out.extend(render_tree(c, arena, indent + 2));
            }
        }
        T::Repeat { body, min, max, .. } => {
            let hi = max.map_or_else(|| "∞".to_string(), |m| m.to_string());
            out.push(format!("{pad}Repeat [{min}..{hi}]:"));
            out.extend(render_tree(body, arena, indent + 2));
        }
    }
    out
}

fn edit_str(e: &shifty_repair::Edit) -> String {
    use shifty_repair::EditOp;
    let (sign, p) = match &e.op {
        EditOp::Add(p) => ("add", p),
        EditOp::Delete(p) => ("del", p),
    };
    format!(
        "{sign} {} {} {}",
        slot_str(&p.s),
        slot_str(&p.p),
        slot_str(&p.o)
    )
}

fn slot_str(s: &shifty_repair::Slot) -> String {
    match s {
        shifty_repair::Slot::Bound(t) => t.to_string(),
        shifty_repair::Slot::Open(h) => format!("?{}", h.0),
    }
}

fn constraint_str(c: &shifty_repair::HoleConstraint, arena: &shifty_algebra::ShapeArena) -> String {
    use shifty_repair::HoleConstraint as H;
    match c {
        H::AnyNode => "any node".to_string(),
        H::Fresh => "fresh node".to_string(),
        H::Const(t) => format!("= {t}"),
        H::Typed(_) => "typed value".to_string(),
        H::Kind(_) => "nodeKind".to_string(),
        H::OneOf(v) => format!("one of {} value(s)", v.len()),
        H::ConformsTo(s) => describe_shape(*s, arena),
        H::ConformsToAll(ss) => describe_shapes(ss, arena),
    }
}

/// Human-readable description of several shapes a hole must *all* satisfy,
/// joined by “and” — so a multi-constraint value reads as every applicable
/// constraint, not just one.
fn describe_shapes(ids: &[shifty_algebra::ShapeId], arena: &shifty_algebra::ShapeArena) -> String {
    if ids.is_empty() {
        return "any node".to_string();
    }
    ids.iter()
        .map(|id| describe_shape(*id, arena))
        .collect::<Vec<_>>()
        .join(" and ")
}

/// Human-readable description of a shape for hole-constraint display. Recognises
/// `sh:class`, unwraps `sh:severity`, and expands a conjunction into its parts so
/// every constraint shows; other shapes render in the formalism's notation.
fn describe_shape(id: shifty_algebra::ShapeId, arena: &shifty_algebra::ShapeArena) -> String {
    describe_shape_within(id, arena, 4)
}

fn describe_shape_within(
    id: shifty_algebra::ShapeId,
    arena: &shifty_algebra::ShapeArena,
    depth: u8,
) -> String {
    use shifty_algebra::Shape;
    // ∃≥1 (rdf:type/rdfs:subClassOf*).test(C) — the encoding of sh:class C
    if let Some(class) = shifty_algebra::render::class_target_shape(id, arena) {
        return format!("instance of {class}");
    }
    match arena.get(id) {
        // Guard against recursive shapes: stop descending and name the slot.
        _ if depth == 0 => format!("conforms to @{}", id.0),
        // sh:severity is transparent — describe the wrapped shape.
        Shape::Annotated { shape, .. } => describe_shape_within(*shape, arena, depth - 1),
        // A conjunction reads as each conjunct joined by “and”.
        Shape::And(cs) => cs
            .iter()
            .map(|c| describe_shape_within(*c, arena, depth - 1))
            .collect::<Vec<_>>()
            .join(" and "),
        Shape::Top | Shape::Pending => "any node".to_string(),
        _ => shifty_algebra::render::shape_to_string(arena, id),
    }
}

fn inspect(args: InspectArgs) -> Result<(), Box<dyn Error>> {
    let bytes = std::fs::read(&args.file)?;
    let base = args.base.as_deref();

    match args.stage {
        Stage::Rdf => {
            let loaded = shifty_parse::load_turtle(&bytes, base)?;
            match args.format {
                Format::Text => {
                    let mut lines: Vec<String> =
                        loaded.graph.iter().map(|t| t.to_string()).collect();
                    lines.sort();
                    for line in lines {
                        println!("{line}");
                    }
                }
                Format::Json => {
                    let triples: Vec<_> = loaded
                        .graph
                        .iter()
                        .map(|t| {
                            serde_json::json!({
                                "subject": t.subject.to_string(),
                                "predicate": t.predicate.to_string(),
                                "object": t.object.to_string(),
                            })
                        })
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&triples)?);
                }
                Format::Dot => {
                    return Err(
                        "--format dot is only supported for --stage algebra or --stage normalized"
                            .into(),
                    );
                }
            }
        }
        Stage::Algebra => {
            let out = shifty_parse::parse_turtle(&bytes, base)?;
            match args.format {
                Format::Text => print!("{}", shifty_algebra::render::schema_to_text(&out.schema)),
                Format::Json => println!("{}", serde_json::to_string_pretty(&out.schema)?),
                Format::Dot => print!("{}", shifty_algebra::render::schema_to_dot(&out.schema)),
            }
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
        Stage::Normalized => {
            let out = shifty_parse::parse_turtle(&bytes, base)?;
            let schema = shifty_opt::normalize(&out.schema);
            match args.format {
                Format::Text => print!("{}", shifty_algebra::render::schema_to_text(&schema)),
                Format::Json => println!("{}", serde_json::to_string_pretty(&schema)?),
                Format::Dot => print!("{}", shifty_algebra::render::schema_to_dot(&schema)),
            }
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
        Stage::Strata => {
            let out = shifty_parse::parse_turtle(&bytes, base)?;
            let strat = shifty_opt::analyze(&out.schema.arena);
            match args.format {
                Format::Json => println!("{}", serde_json::to_string_pretty(&strat)?),
                Format::Text => print_strata(&strat),
                Format::Dot => {
                    return Err(
                        "--format dot is only supported for --stage algebra or --stage normalized"
                            .into(),
                    );
                }
            }
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
        Stage::Plan => {
            let out = shifty_parse::parse_turtle(&bytes, base)?;
            let normalized = shifty_opt::normalize(&out.schema);
            let physical = shifty_opt::plan(&normalized);
            match args.format {
                Format::Text => print!("{}", shifty_opt::plan::plan_to_text(&physical)),
                Format::Json => println!("{}", serde_json::to_string_pretty(&physical)?),
                Format::Dot => return Err("--format dot is not supported for --stage plan".into()),
            }
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
        Stage::Capability => {
            if !matches!(args.format, Format::Text) {
                return Err("--stage capability only supports --format text".into());
            }
            let out = shifty_parse::parse_turtle(&bytes, base)?;
            let normalized = shifty_opt::normalize(&out.schema);
            print_capability(&normalized);
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
    }
    Ok(())
}

fn print_strata(strat: &shifty_opt::Stratification) {
    let recursive = strat.recursive().count();
    println!(
        "strata: stratifiable = {}; {} shape(s) in {} stratum(strata); {} recursive component(s)",
        strat.stratifiable,
        strat.shape_count(),
        strat.strata.len(),
        recursive,
    );
    let fmt = |shapes: &[shifty_algebra::ShapeId]| {
        shapes
            .iter()
            .map(|s| format!("@{}", s.0))
            .collect::<Vec<_>>()
            .join(" ")
    };
    if recursive > 0 {
        println!("recursive components (in dependency order):");
        for (level, s) in strat.strata.iter().enumerate() {
            if !s.recursive {
                continue;
            }
            let tag = if s.stratifiable {
                "positive recursion, ok"
            } else {
                "NON-STRATIFIABLE: recursion through negation"
            };
            println!("  stratum {level}: {}  ({tag})", fmt(&s.shapes));
        }
    }
}

fn print_capability(schema: &shifty_algebra::Schema) {
    use shifty_algebra::Shape;
    use shifty_opt::lower_query;
    use spargebra::SparqlParser;

    let mut sparql_queries: Vec<String> = Vec::new();
    for i in 0..schema.arena.len() {
        let id = shifty_algebra::ShapeId(i as u32);
        if let Shape::Sparql(c) = schema.arena.get(id) {
            sparql_queries.push(c.query.clone());
        }
    }

    // `lower_query` is the routing gate: a query runs on the native executor iff
    // it lowers to a native plan, otherwise it falls back to Spareval. This
    // reports what actually happens, not the broader designed subset (which lives
    // in docs/05-sparql-execution.md §129-141).
    let lowered_count = sparql_queries
        .iter()
        .filter(|q| {
            SparqlParser::new()
                .parse_query(q)
                .map(|parsed| lower_query(&parsed).is_ok())
                .unwrap_or(false)
        })
        .count();

    println!(
        "capability: {} SPARQL constraint query/queries ({} native, {} fall back)",
        sparql_queries.len(),
        lowered_count,
        sparql_queries.len() - lowered_count,
    );

    for (i, q) in sparql_queries.iter().enumerate() {
        match SparqlParser::new().parse_query(q) {
            Ok(parsed) => {
                let tag = match lower_query(&parsed) {
                    Ok(_) => "NATIVE".to_string(),
                    Err(reason) => format!("FALLBACK ({reason})"),
                };
                println!("  [{i}] {tag}:\n{q}");
            }
            Err(e) => println!("  [{i}] PARSE ERROR: {e}"),
        }
    }
}
