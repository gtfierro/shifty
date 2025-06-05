use shacl::context::ValidationContext;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: {} <shapes_file> <data_file>", args[0]);
        std::process::exit(1);
    }

    let shapes_file = &args[1];
    let data_file = &args[2];

    let ctx = ValidationContext::from_files(shapes_file, data_file).unwrap_or_else(|e| {
        eprintln!("Error loading files: {}", e);
        std::process::exit(1);
    });

    ctx.graphviz();
}
