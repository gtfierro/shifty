//! Session lifecycle benchmark with two data graphs sharing one compilation.
//!
//! Run from the repository root, for example:
//! `cargo run --release -p shifty-engine --example bench_sessions -- \
//!   benchmark/brick/Brick-closure.ttl benchmark/brick/models/bldg1.ttl \
//!   benchmark/brick/models/bldg2.ttl`
//!
//! Each invocation is a fresh process. Use `/usr/bin/time -l` on macOS or
//! `/usr/bin/time -v` on Linux around the binary to record peak resident memory.

use shifty_engine::{CompiledShapes, FindingOptions, SessionData, SessionOptions};
use shifty_parse::{Loaded, RdfFormat};
use shifty_repair::GraphDelta;
use std::env;
use std::hint::black_box;
use std::path::Path;
use std::process::Command;
use std::time::Instant;

fn load(path: &str) -> Loaded {
    Loaded::from_path(Path::new(path), RdfFormat::Turtle, None).expect("valid Turtle input")
}

fn rss_kib() -> Option<usize> {
    let pid = std::process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()?;
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

fn main() {
    let args: Vec<_> = env::args().skip(1).collect();
    assert!(
        args.len() == 3 || (args.len() == 4 && args[3] == "--infer"),
        "usage: bench_sessions SHAPES DATA_A DATA_B [--infer]"
    );
    let options = SessionOptions {
        inference: args.len() == 4,
        ..SessionOptions::default()
    };
    let findings = FindingOptions::default();

    let start = Instant::now();
    let shapes = load(&args[0]);
    let load_shapes_ms = start.elapsed().as_secs_f64() * 1000.0;
    let start = Instant::now();
    let compiled = CompiledShapes::compile(shapes).expect("compilable shapes");
    let compile_ms = start.elapsed().as_secs_f64() * 1000.0;
    let compiled_rss_kib = rss_kib();

    let data_a = load(&args[1]).graph;
    let data_b = load(&args[2]).graph;
    let delta = GraphDelta {
        delete: data_a
            .iter()
            .next()
            .map(|triple| triple.into_owned())
            .into_iter()
            .collect(),
        ..GraphDelta::default()
    };

    let start = Instant::now();
    let first = compiled
        .session(SessionData::Separate(data_a), options)
        .expect("first session");
    let first_session_ms = start.elapsed().as_secs_f64() * 1000.0;
    let start = Instant::now();
    black_box(first.validate(&findings).expect("first validation"));
    let first_validate_ms = start.elapsed().as_secs_f64() * 1000.0;
    let first_rss_kib = rss_kib();

    let start = Instant::now();
    for _ in 0..3 {
        black_box(first.validate(&findings).expect("repeated validation"));
    }
    let repeated_validate_ms = start.elapsed().as_secs_f64() * 1000.0 / 3.0;

    let start = Instant::now();
    let second = compiled
        .session(SessionData::Separate(data_b), options)
        .expect("second session");
    let second_session_ms = start.elapsed().as_secs_f64() * 1000.0;
    let start = Instant::now();
    black_box(second.validate(&findings).expect("second validation"));
    let second_validate_ms = start.elapsed().as_secs_f64() * 1000.0;
    let second_rss_kib = rss_kib();

    let start = Instant::now();
    let edited = first.with_delta(&delta).expect("edited session");
    let edit_session_ms = start.elapsed().as_secs_f64() * 1000.0;
    let start = Instant::now();
    black_box(edited.validate(&findings).expect("edited validation"));
    let edit_validate_ms = start.elapsed().as_secs_f64() * 1000.0;
    let edited_rss_kib = rss_kib();

    println!(
        "shapes={},data_a={},data_b={},infer={},load_shapes_ms={load_shapes_ms:.2},compile_ms={compile_ms:.2},first_session_ms={first_session_ms:.2},first_validate_ms={first_validate_ms:.2},repeated_validate_ms={repeated_validate_ms:.2},second_session_ms={second_session_ms:.2},second_validate_ms={second_validate_ms:.2},edit_session_ms={edit_session_ms:.2},edit_validate_ms={edit_validate_ms:.2},compiled_rss_kib={compiled_rss_kib:?},first_rss_kib={first_rss_kib:?},second_rss_kib={second_rss_kib:?},edited_rss_kib={edited_rss_kib:?}",
        args[0], args[1], args[2], options.inference,
    );
}
