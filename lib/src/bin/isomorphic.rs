use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::{Graph, Triple};
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;

fn load_graph(path: &str) -> Result<Graph, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let parser = RdfParser::from_format(RdfFormat::Turtle).without_named_graphs();
    let mut graph = Graph::new();
    for quad in parser.for_reader(reader) {
        let triple: Triple = quad?.into();
        graph.insert(triple.as_ref());
    }
    Ok(graph)
}

fn collect_diff(g1: &Graph, g2: &Graph) -> Vec<Triple> {
    let mut extras = Vec::new();
    for t in g1.iter() {
        if !g2.contains(t) {
            extras.push(t.into());
        }
    }
    extras
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <file1> <file2>", args[0]);
        std::process::exit(1);
    }
    let g1 = load_graph(&args[1])?;
    let g2 = load_graph(&args[2])?;
    let iso = shifty::canonicalization::are_isomorphic(&g1, &g2);
    println!("isomorphic: {}", iso);
    println!("g1 triples: {}", g1.len());
    println!("g2 triples: {}", g2.len());
    if !iso {
        for (label, extras) in [
            ("g1 not in g2", collect_diff(&g1, &g2)),
            ("g2 not in g1", collect_diff(&g2, &g1)),
        ] {
            if !extras.is_empty() {
                println!("{label}:");
                for t in extras {
                    println!("  {} {} {} .", t.subject, t.predicate, t.object);
                }
            }
        }
    }
    Ok(())
}
