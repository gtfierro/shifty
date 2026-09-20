//! Measure memory retained by many live sessions sharing one compilation.
//!
//! Run in a fresh process under `/usr/bin/time -l` on macOS (or `-v` on
//! Linux). The caller alternates release binaries and repeats samples.

use shifty_engine::{CompiledShapes, FindingOptions, SessionData, SessionOptions};
use shifty_parse::{Loaded, RdfFormat};
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
        "usage: bench_many_sessions SHAPES DATA COUNT [--infer]"
    );
    let count: usize = args[2].parse().expect("COUNT must be an integer");
    assert!(count > 0, "COUNT must be positive");
    let options = SessionOptions {
        inference: args.len() == 4,
        ..SessionOptions::default()
    };
    let findings = FindingOptions::default();

    let started = Instant::now();
    let compiled = CompiledShapes::compile(load(&args[0])).expect("compilable shapes");
    let compile_ms = started.elapsed().as_secs_f64() * 1000.0;
    let data = load(&args[1]).graph;
    let compiled_rss_kib = rss_kib();

    let mut sessions = Vec::with_capacity(count);
    let started = Instant::now();
    let mut first_rss_kib = None;
    for index in 0..count {
        let session = compiled
            .session(SessionData::Separate(data.clone()), options)
            .expect("session");
        black_box(session.validate(&findings).expect("validation"));
        sessions.push(session);
        if index == 0 {
            first_rss_kib = rss_kib();
        }
    }
    let sessions_ms = started.elapsed().as_secs_f64() * 1000.0;
    let final_rss_kib = rss_kib();
    black_box(&sessions);
    println!(
        "shapes={},data={},count={count},infer={},compile_ms={compile_ms:.2},sessions_ms={sessions_ms:.2},compiled_rss_kib={compiled_rss_kib:?},first_rss_kib={first_rss_kib:?},final_rss_kib={final_rss_kib:?}",
        args[0], args[1], options.inference,
    );
}
