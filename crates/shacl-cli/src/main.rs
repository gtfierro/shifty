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
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Text,
    Json,
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
    }
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
            }
        }
        Stage::Algebra => {
            let out = shacl_parse::parse_turtle(&bytes, base)?;
            match args.format {
                Format::Text => print!("{}", shacl_algebra::render::schema_to_text(&out.schema)),
                Format::Json => println!("{}", serde_json::to_string_pretty(&out.schema)?),
            }
            for d in &out.diagnostics {
                eprintln!("{d}");
            }
        }
    }
    Ok(())
}
