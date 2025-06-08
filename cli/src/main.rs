use clap::Parser;
use graphviz_rust::cmd::{CommandArg, Format};
use graphviz_rust::exec_dot;
use shacl::context::ValidationContext;
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

#[derive(Parser)]
struct ValidateArgs {
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
    /// Validate the data against the shapes
    Validate(ValidateArgs),
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Graphviz(args) => {
            let ctx = ValidationContext::from_files(
                args.shapes_file
                    .to_str()
                    .ok_or_else(|| "Invalid shapes file path")?,
                args.data_file
                    .to_str()
                    .ok_or_else(|| "Invalid data file path")?,
            )
            .map_err(|e| format!("Error loading files: {}", e))?;
            let dot_string = ctx.graphviz();
            println!("{}", dot_string);
        }
        Commands::Pdf(args) => {
            let ctx = ValidationContext::from_files(
                args.shapes_file
                    .to_str()
                    .ok_or_else(|| "Invalid shapes file path")?,
                args.data_file
                    .to_str()
                    .ok_or_else(|| "Invalid data file path")?,
            )
            .map_err(|e| format!("Error loading files: {}", e))?;
            let dot_string = ctx.graphviz();

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
            let ctx = ValidationContext::from_files(
                args.shapes_file
                    .to_str()
                    .ok_or_else(|| "Invalid shapes file path")?,
                args.data_file
                    .to_str()
                    .ok_or_else(|| "Invalid data file path")?,
            )
            .map_err(|e| format!("Error loading files: {}", e))?;
            ctx.validate();
            println!("Validation completed successfully.");
        }
    }
    Ok(())
}
