//! Micro-benchmark: reference evaluator vs. planned executor on a class target.
//!
//! Run with: `cargo run --release -p shifty-engine --example bench`
//!
//! The graph has a small population of targeted `ex:Person` instances buried in
//! a large pile of untargeted decoy nodes. The reference evaluator selects the
//! class target's focus nodes by scanning *every* node and testing
//! `∃(rdf:type/subClassOf*).test(Person)`; the planned executor seeds backward
//! from the `ex:Person` constant, touching only the instances.

use std::time::Instant;

fn main() {
    let persons = 1_000;
    let decoys = 20_000;
    let runs = 5;

    let mut ttl = String::new();
    ttl.push_str(
        "@prefix ex: <http://ex/> .\n\
         @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\
         @prefix sh: <http://www.w3.org/ns/shacl#> .\n\
         ex:PersonShape a sh:NodeShape ; sh:targetClass ex:Person ;\n\
           sh:property [ sh:path ex:name ; sh:minCount 1 ; sh:datatype xsd:string ] ;\n\
           sh:property [ sh:path ex:age ; sh:datatype xsd:integer ] .\n",
    );
    for i in 0..persons {
        ttl.push_str(&format!("ex:p{i} a ex:Person ; ex:name \"P{i}\" ; ex:age {i} .\n"));
    }
    for i in 0..decoys {
        ttl.push_str(&format!("ex:d{i} a ex:Thing ; ex:label \"D{i}\" .\n"));
    }

    let parsed = shifty_parse::parse_turtle(ttl.as_bytes(), None).unwrap();
    let loaded = shifty_parse::load_turtle(ttl.as_bytes(), None).unwrap();
    let normalized = shifty_opt::normalize(&parsed.schema);
    let physical = shifty_opt::plan(&normalized);

    let mut reference_conforms = true;
    let t = Instant::now();
    for _ in 0..runs {
        reference_conforms = shifty_engine::validate(&loaded.graph, &parsed.schema).unwrap().conforms;
    }
    let reference = t.elapsed() / runs;

    let mut planned_conforms = true;
    let t = Instant::now();
    for _ in 0..runs {
        planned_conforms = shifty_engine::validate_plan(&loaded.graph, &physical).unwrap().conforms;
    }
    let planned = t.elapsed() / runs;

    assert_eq!(reference_conforms, planned_conforms, "executors disagree");

    println!(
        "graph: {persons} Person instances + {decoys} decoys = {} nodes",
        persons + decoys
    );
    println!("reference validate: {reference:>10.2?}/run (conforms={reference_conforms})");
    println!("planned   validate: {planned:>10.2?}/run (conforms={planned_conforms})");
    println!(
        "speedup: {:.1}x",
        reference.as_secs_f64() / planned.as_secs_f64().max(1e-9)
    );
}
