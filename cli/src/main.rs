use clap::{Parser, ValueEnum};
use env_logger;
use graphviz_rust::cmd::{CommandArg, Format};
use graphviz_rust::exec_dot;
use oxigraph::io::RdfFormat;
use shacl::Validator;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser)]
struct GraphvizArgs {
    /// Path to the shapes file
    #[arg(short, long, value_name = "FILE")]
    shapes_file: PathBuf,

    /// Path to the data file
    #[arg(short, long, value_name = "FILE")]
    data_file: PathBuf,
}

#[derive(Parser)]
struct PdfArgs {
    /// Path to the shapes file
    #[arg(short, long, value_name = "FILE")]
    shapes_file: PathBuf,

    /// Path to the data file
    #[arg(short, long, value_name = "FILE")]
    data_file: PathBuf,

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
    /// Path to the shapes file
    #[arg(short, long, value_name = "FILE")]
    shapes_file: PathBuf,

    /// Path to the data file
    #[arg(short, long, value_name = "FILE")]
    data_file: PathBuf,

    /// The output format for the validation report
    #[arg(long, value_enum, default_value_t = ValidateOutputFormat::Turtle)]
    format: ValidateOutputFormat,
}

#[derive(Parser)]
struct HeatArgs {
    /// Path to the shapes file
    #[arg(short, long, value_name = "FILE")]
    shapes_file: PathBuf,

    /// Path to the data file
    #[arg(short, long, value_name = "FILE")]
    data_file: PathBuf,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Output the Graphviz DOT string of the shape graph
    Graphviz(GraphvizArgs),
    /// Generate a PDF of the shape graph using Graphviz
    Pdf(PdfArgs),
    /// Validate the data against the shapes and output a heatmap of component invocations
    Heat(HeatArgs),
    /// Validate the data against the shapes
    Validate(ValidateArgs),
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Graphviz(args) => {
            let validator = Validator::from_files(
                args.shapes_file
                    .to_str()
                    .ok_or_else(|| "Invalid shapes file path")?,
                args.data_file
                    .to_str()
                    .ok_or_else(|| "Invalid data file path")?,
            )
            .map_err(|e| format!("Error loading files: {}", e))?;
            let dot_string = validator.to_graphviz()?;
            println!("{}", dot_string);
        }
        Commands::Pdf(args) => {
            let validator = Validator::from_files(
                args.shapes_file
                    .to_str()
                    .ok_or_else(|| "Invalid shapes file path")?,
                args.data_file
                    .to_str()
                    .ok_or_else(|| "Invalid data file path")?,
            )
            .map_err(|e| format!("Error loading files: {}", e))?;
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
            let validator = Validator::from_files(
                args.shapes_file
                    .to_str()
                    .ok_or_else(|| "Invalid shapes file path")?,
                args.data_file
                    .to_str()
                    .ok_or_else(|| "Invalid data file path")?,
            )
            .map_err(|e| format!("Error loading files: {}", e))?;
            let report = validator.validate();

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
        }
        Commands::Heat(args) => {
            let validator = Validator::from_files(
                args.shapes_file
                    .to_str()
                    .ok_or_else(|| "Invalid shapes file path")?,
                args.data_file
                    .to_str()
                    .ok_or_else(|| "Invalid data file path")?,
            )
            .map_err(|e| format!("Error loading files: {}", e))?;

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
    }
    Ok(())
}
