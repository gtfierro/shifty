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
    /// Validate a data graph against a shapes graph (reference evaluator).
    Validate(ValidateArgs),
    /// Run SHACL-AF rule inference (forward chaining to a fixpoint).
    Infer(InferArgs),
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
    /// RDF graph scope used during validation.
    #[arg(long, visible_alias = "graph-scope", value_enum, default_value_t = GraphMode::Union)]
    graph_mode: GraphMode,
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

impl From<GraphMode> for shacl_engine::ValidationGraphMode {
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

fn load_sources(sources: &[String], base: Option<&str>) -> Result<shacl_parse::Loaded, Box<dyn Error>> {
    let mut merged: Option<shacl_parse::Loaded> = None;
    for src in sources {
        let bytes = fetch_bytes(src)?;
        let loaded = shacl_parse::load_turtle(&bytes, base)?;
        match merged.as_mut() {
            None => merged = Some(loaded),
            Some(m) => m.merge_from(&loaded),
        }
    }
    merged.ok_or_else(|| "no sources provided".into())
}

fn infer(args: InferArgs) -> Result<(), Box<dyn Error>> {
    let base = args.base.as_deref();
    let shapes = load_sources(&args.shapes, base)?;
    let parsed = shacl_parse::parse_loaded(&shapes);
    for d in &parsed.diagnostics {
        eprintln!("{d}");
    }

    let data = if args.data.is_empty() {
        shapes
    } else {
        load_sources(&args.data, base)?
    };

    let outcome = match shacl_engine::infer(&data.graph, &parsed.schema) {
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
    Ok(())
}

fn validate(args: ValidateArgs) -> Result<(), Box<dyn Error>> {
    let base = args.base.as_deref();
    let shapes_loaded = load_sources(&args.shapes, base)?;
    let graph_mode = args.graph_mode.into();

    let data_loaded = if args.data.is_empty() {
        None
    } else {
        Some(load_sources(&args.data, base)?)
    };
    let data_graph = data_loaded.as_ref().map(|d| &d.graph).unwrap_or(&shapes_loaded.graph);

    // W3C report mode: component-granular validator + RDF report output.
    // Uses only the raw loaded graph, so lowering is skipped entirely.
    if args.report {
        let report = shacl_engine::validate_report_graphs_with_mode(
            &shapes_loaded,
            data_graph,
            graph_mode,
        );
        let graph = shacl_engine::report_to_graph(&report);
        // Collect prefixes from shapes + data, deduplicating by name.
        // Fall back to standard entries for sh:/rdf:/xsd: if not declared.
        let mut prefixes: Vec<(&str, &str)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (name, iri) in shapes_loaded.prefixes.iter()
            .chain(data_loaded.as_ref().map(|d| d.prefixes.iter()).into_iter().flatten())
        {
            if seen.insert(name.as_str()) {
                prefixes.push((name.as_str(), iri.as_str()));
            }
        }
        for (name, iri) in [
            ("sh",   "http://www.w3.org/ns/shacl#"),
            ("rdf",  "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
            ("xsd",  "http://www.w3.org/2001/XMLSchema#"),
        ] {
            if seen.insert(name) {
                prefixes.push((name, iri));
            }
        }
        let mut ser = oxttl::TurtleSerializer::new();
        for (name, iri) in &prefixes {
            ser = ser.with_prefix(*name, *iri).unwrap();
        }
        let bytes = graph.iter().try_fold(ser.for_writer(Vec::new()), |mut s, triple| {
            s.serialize_triple(triple).map(|()| s)
        })?.finish()?;
        print!("{}", String::from_utf8_lossy(&bytes));
        return Ok(());
    }

    let parsed = shacl_parse::parse_loaded(&shapes_loaded);
    for d in &parsed.diagnostics {
        eprintln!("{d}");
    }

    let outcome = match shacl_engine::validate_graphs_with_mode(
        data_graph,
        &shapes_loaded.graph,
        &parsed.schema,
        graph_mode,
    ) {
        Ok(o) => o,
        Err(e) => {
            return Err(format!("{e}; cannot validate (see `inspect --stage strata`)").into());
        }
    };

    match args.format {
        Format::Dot => return Err("--format dot is not supported for validate".into()),
        Format::Json => println!("{}", serde_json::to_string_pretty(&outcome)?),
        Format::Text => {
            println!("conforms: {}", outcome.conforms);
            if !outcome.conforms {
                println!("violations: {}", outcome.violations.len());
                let mut violations = outcome.violations.clone();
                violations.sort_by_key(|v| v.focus.to_string());
                for v in &violations {
                    let st = &parsed.schema.statements[v.statement];
                    println!(
                        "  {}  [target: {}]",
                        v.focus,
                        shacl_algebra::render::selector_to_string(&st.selector)
                    );
                    let mut reasons: Vec<String> = v
                        .reasons
                        .iter()
                        .map(|r| match &r.path {
                            Some(p) => format!("      - ({p}) {} → {}", r.value, r.message),
                            None => format!("      - {}", r.message),
                        })
                        .collect();
                    reasons.sort();
                    for line in reasons {
                        println!("{line}");
                    }
                }
            }
        }
    }
    Ok(())
}

fn inspect(args: InspectArgs) -> Result<(), Box<dyn Error>> {
    let bytes = std::fs::read(&args.file)?;
    let base = args.base.as_deref();

    match args.stage {
        Stage::Rdf => {
            let loaded = shacl_parse::load_turtle(&bytes, base)?;
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
                Format::Dot => return Err("--format dot is only supported for --stage algebra or --stage normalized".into()),
            }
        }
        Stage::Algebra => {
            let out = shacl_parse::parse_turtle(&bytes, base)?;
            match args.format {
                Format::Text => print!("{}", shacl_algebra::render::schema_to_text(&out.schema)),
                Format::Json => println!("{}", serde_json::to_string_pretty(&out.schema)?),
                Format::Dot => print!("{}", shacl_algebra::render::schema_to_dot(&out.schema)),
            }
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
        Stage::Normalized => {
            let out = shacl_parse::parse_turtle(&bytes, base)?;
            let schema = shacl_opt::normalize(&out.schema);
            match args.format {
                Format::Text => print!("{}", shacl_algebra::render::schema_to_text(&schema)),
                Format::Json => println!("{}", serde_json::to_string_pretty(&schema)?),
                Format::Dot => print!("{}", shacl_algebra::render::schema_to_dot(&schema)),
            }
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
        Stage::Strata => {
            let out = shacl_parse::parse_turtle(&bytes, base)?;
            let strat = shacl_opt::analyze(&out.schema.arena);
            match args.format {
                Format::Json => println!("{}", serde_json::to_string_pretty(&strat)?),
                Format::Text => print_strata(&strat),
                Format::Dot => return Err("--format dot is only supported for --stage algebra or --stage normalized".into()),
            }
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
        Stage::Plan => {
            let out = shacl_parse::parse_turtle(&bytes, base)?;
            let normalized = shacl_opt::normalize(&out.schema);
            let physical = shacl_opt::plan(&normalized);
            match args.format {
                Format::Text => print!("{}", shacl_opt::plan::plan_to_text(&physical)),
                Format::Json => println!("{}", serde_json::to_string_pretty(&physical)?),
                Format::Dot => return Err("--format dot is not supported for --stage plan".into()),
            }
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
    }
    Ok(())
}

fn print_strata(strat: &shacl_opt::Stratification) {
    let recursive = strat.recursive().count();
    println!(
        "strata: stratifiable = {}; {} shape(s) in {} stratum(strata); {} recursive component(s)",
        strat.stratifiable,
        strat.shape_count(),
        strat.strata.len(),
        recursive,
    );
    let fmt = |shapes: &[shacl_algebra::ShapeId]| {
        shapes.iter().map(|s| format!("@{}", s.0)).collect::<Vec<_>>().join(" ")
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
