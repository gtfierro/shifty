//! Where does evidence tracing spend its time? A counting probe, not a timer.
//!
//! Runs both arms over one model from one prepared snapshot and reports the
//! work each does, so the latency gap in `bench_evidence` can be attributed:
//!
//!   * memo hits/misses/entries — how much sharing the conformance evaluator
//!     gets from its `(ShapeId, Term) -> bool` cache;
//!   * evidence node visits — how many nodes `evaluate_node` *enters*, versus
//!     how many survive into the retained evidence.
//!
//! Visits far above both the retained node count and the memo's entry count is
//! the signature of a shared shape DAG being re-expanded into a tree, per pair,
//! with no cache to collapse it.
//!
//! Usage:
//!   cargo run --release -p shifty-engine --example probe_evidence_cost -- \
//!       --shapes benchmark/brick/Brick-closure.ttl \
//!       --data benchmark/brick/models/bldg1.ttl

use shifty_engine::{PreparedEvidenceValidator, ValidationGraphMode, ValidationOptions, profile};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

fn main() {
    let mut shapes_path = PathBuf::new();
    let mut data_path = PathBuf::new();
    let mut no_infer = false;
    let mut top = 15usize;
    let mut dump_json: Option<PathBuf> = None;
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut value = || {
            argv.next()
                .unwrap_or_else(|| panic!("{flag} needs a value"))
        };
        match flag.as_str() {
            "--shapes" => shapes_path = PathBuf::from(value()),
            "--data" => data_path = PathBuf::from(value()),
            "--no-infer" => no_infer = true,
            "--top" => top = value().parse().expect("--top must be an integer"),
            "--dump-json" => dump_json = Some(PathBuf::from(value())),
            other => panic!("unknown flag {other}"),
        }
    }
    assert!(!shapes_path.as_os_str().is_empty(), "--shapes is required");
    assert!(!data_path.as_os_str().is_empty(), "--data is required");

    let shapes_bytes = fs::read(&shapes_path).expect("cannot read --shapes");
    let shapes = shifty_parse::load_turtle(&shapes_bytes, None).expect("cannot parse --shapes");
    let parsed = shifty_parse::parse_loaded(&shapes);
    let raw_schema = parsed.schema.clone();
    let inference_schema = shifty_opt::normalize(&raw_schema);

    let data_bytes = fs::read(&data_path).expect("cannot read --data");
    let data = shifty_parse::load_turtle(&data_bytes, None).expect("cannot parse --data");
    let data_graph = if no_infer {
        data.graph.clone()
    } else {
        shifty_engine::infer_graphs(&data.graph, &shapes.graph, &inference_schema)
            .expect("inference")
            .graph
    };

    let prepared = PreparedEvidenceValidator::with_graphs(
        &data_graph,
        &shapes.graph,
        &raw_schema,
        ValidationGraphMode::Union,
    )
    .expect("stratifiable");
    let options = ValidationOptions::default();

    println!("model: {}", data_path.display());
    println!("normalized arena nodes: {}", prepared.schema().arena.len());
    println!(
        "normalized statements:  {}",
        prepared.schema().statements.len()
    );
    println!();

    // ---- arm A: conformance only ------------------------------------------
    profile::enable();
    let _ = profile::take_evidence_visits();
    let start = Instant::now();
    let conformance = prepared.validate_conformance(&options);
    let conformance_ms = start.elapsed().as_secs_f64() * 1e3;
    let conformance_visits = profile::take_evidence_visits();
    let conformance_probes = profile::take_path_probes();
    let conformance_cache = profile::take().expect("profiling enabled");
    let conformance_cache = conformance_cache.shape_cache().clone();

    // ---- arm B: dual evidence ----------------------------------------------
    profile::enable();
    let start = Instant::now();
    let run = prepared.validate(&options);
    let evidence_ms = start.elapsed().as_secs_f64() * 1e3;
    let evidence_visits = profile::take_evidence_visits();
    let evidence_probes = profile::take_path_probes();
    let collector = profile::take().expect("profiling enabled");
    let evidence_cache = collector.shape_cache().clone();

    // ---- arm C: on-demand ---------------------------------------------------
    // What a caller pays who wants explanations only for what failed: one
    // conformance pass that also records the failing pairs, then evidence
    // materialized for those pairs alone.
    profile::enable();
    let start = Instant::now();
    let (_, failures) = prepared.find_failures(&options);
    let find_ms = start.elapsed().as_secs_f64() * 1e3;
    let start = Instant::now();
    let explained: usize = failures
        .iter()
        .map(|pair| prepared.explain(pair).len())
        .sum();
    let explain_ms = start.elapsed().as_secs_f64() * 1e3;
    let _ = profile::take();
    let _ = profile::take_evidence_visits();
    let _ = profile::take_path_probes();

    let pairs = conformance.selected_pairs.max(1) as f64;
    let retained = run.walk().len();

    // ---- compact encoding --------------------------------------------------
    let full_json = run.to_json().expect("evidence run serializes");
    // Split so the encoding's own cost is separable from the intermediate
    // `Value` tree it currently has to be handed: compaction is worth doing
    // inline only if the total stays under the validation it describes.
    let to_value_start = Instant::now();
    let run_value = serde_json::to_value(&run).expect("run serializes");
    let to_value_ms = to_value_start.elapsed().as_secs_f64() * 1e3;
    let intern_start = Instant::now();
    let interned = shifty_engine::compact_value(run_value, true);
    let intern_ms = intern_start.elapsed().as_secs_f64() * 1e3;
    let emit_start = Instant::now();
    let compact_json = serde_json::to_string(&interned).expect("compact encoding serializes");
    let emit_ms = emit_start.elapsed().as_secs_f64() * 1e3;
    let compact_ms = to_value_ms + intern_ms + emit_ms;
    drop(interned);
    let compact_no_catalog =
        shifty_engine::to_compact_json(&run, false).expect("compact encoding succeeds");
    println!();
    println!(
        "serialized full:           {:>10.2} MB  ({:.0} bytes/pair)",
        full_json.len() as f64 / 1e6,
        full_json.len() as f64 / pairs
    );
    println!(
        "compact (with catalog):    {:>10.2} MB  ({:.0} bytes/pair, {:.1}% smaller, {compact_ms:.0} ms \
         = {to_value_ms:.0} to_value + {intern_ms:.0} intern + {emit_ms:.0} emit)",
        compact_json.len() as f64 / 1e6,
        compact_json.len() as f64 / pairs,
        100.0 * (full_json.len() - compact_json.len()) as f64 / full_json.len() as f64
    );
    println!(
        "compact (catalog elided):  {:>10.2} MB  ({:.0} bytes/pair, {:.1}% smaller)",
        compact_no_catalog.len() as f64 / 1e6,
        compact_no_catalog.len() as f64 / pairs,
        100.0 * (full_json.len() - compact_no_catalog.len()) as f64 / full_json.len() as f64
    );

    if let Some(path) = &dump_json {
        fs::write(path, run.to_json().expect("evidence run serializes"))
            .expect("cannot write --dump-json");
        eprintln!("wrote {}", path.display());
    }

    println!("                        conformance        evidence");
    println!("  wall ms               {conformance_ms:>11.1}  {evidence_ms:>14.1}");
    println!(
        "  memo lookups          {:>11}  {:>14}",
        conformance_cache.hits + conformance_cache.misses,
        evidence_cache.hits + evidence_cache.misses
    );
    println!(
        "  memo hits             {:>11}  {:>14}",
        conformance_cache.hits, evidence_cache.hits
    );
    println!(
        "  memo misses           {:>11}  {:>14}",
        conformance_cache.misses, evidence_cache.misses
    );
    println!(
        "  memo entries (peak)   {:>11}  {:>14}",
        conformance_cache.peak_entries, evidence_cache.peak_entries
    );
    println!(
        "  back-edges            {:>11}  {:>14}",
        conformance_cache.recursion_back_edges, evidence_cache.recursion_back_edges
    );
    println!(
        "  uncacheable results   {:>11}  {:>14}",
        conformance_cache.non_cacheable_results, evidence_cache.non_cacheable_results
    );
    println!("  evidence visits       {conformance_visits:>11}  {evidence_visits:>14}");
    println!("  path_support probes   {conformance_probes:>11}  {evidence_probes:>14}");
    println!();
    println!(
        "path probes per visit:     {:.1}",
        evidence_probes as f64 / evidence_visits.max(1) as f64
    );

    println!("selected pairs:            {}", conformance.selected_pairs);
    println!("retained evidence nodes:   {retained}");
    println!(
        "visited / retained:        {:.1}x",
        evidence_visits as f64 / retained.max(1) as f64
    );
    println!(
        "visits per pair:           {:.0}",
        evidence_visits as f64 / pairs
    );
    println!(
        "distinct (shape,node) that a cache could collapse to at most: {}",
        evidence_cache.peak_entries
    );
    println!(
        "visits / distinct pairs:   {:.1}x",
        evidence_visits as f64 / evidence_cache.peak_entries.max(1) as f64
    );
    println!();
    println!(
        "per-visit cost:            {:.2} us",
        evidence_ms * 1e3 / evidence_visits.max(1) as f64
    );
    println!(
        "per-memo-lookup cost:      {:.2} us",
        conformance_ms * 1e3 / (conformance_cache.hits + conformance_cache.misses).max(1) as f64
    );

    // ---- attribution: which statements own the time ------------------------
    let (selection, materialization): (Vec<_>, Vec<_>) = collector
        .shape_records()
        .iter()
        .partition(|record| record.label.starts_with("select:"));
    let selection_us: u64 = selection.iter().map(|record| record.total_us).sum();
    let materialization_us: u64 = materialization.iter().map(|record| record.total_us).sum();

    println!();
    println!(
        "target selection total:    {:.1} ms over {} statement(s)",
        selection_us as f64 / 1e3,
        selection.len()
    );
    println!(
        "materialization total:     {:.1} ms over {} statement(s)",
        materialization_us as f64 / 1e3,
        materialization.len()
    );
    println!();
    println!(
        "on-demand: {} failing pair(s) of {} selected -> {} explanation(s)",
        failures.len(),
        conformance.selected_pairs,
        explained
    );
    println!(
        "  find_failures {find_ms:.1} ms + explain {explain_ms:.1} ms = {:.1} ms  \
         ({:.2}x the full evidence run, {:.2}x conformance alone)",
        find_ms + explain_ms,
        (find_ms + explain_ms) / evidence_ms.max(f64::MIN_POSITIVE),
        (find_ms + explain_ms) / conformance_ms.max(f64::MIN_POSITIVE)
    );

    let mut ranked = materialization;
    ranked.sort_by_key(|record| std::cmp::Reverse(record.total_us));
    println!();
    println!("top statements by evidence-materialization time:");
    println!(
        "  {:>8}  {:>7}  {:>6}  {:>10}  {:>12}  {:>10}  shape",
        "ms", "share", "foci", "us/focus", "visits", "visits/foci"
    );
    for record in ranked.iter().take(top) {
        println!(
            "  {:>8.1}  {:>6.1}%  {:>6}  {:>10.1}  {:>12}  {:>10.0}  {}",
            record.total_us as f64 / 1e3,
            100.0 * record.total_us as f64 / materialization_us.max(1) as f64,
            record.invocations,
            record.total_us as f64 / record.invocations.max(1) as f64,
            record.visits,
            record.visits as f64 / record.invocations.max(1) as f64,
            record.label,
        );
    }

    let mut ranked_selection = selection;
    ranked_selection.sort_by_key(|record| std::cmp::Reverse(record.total_us));
    println!();
    println!("top statements by target-selection time:");
    for record in ranked_selection.iter().take(5) {
        println!(
            "  {:>8.1} ms  {}",
            record.total_us as f64 / 1e3,
            record.label
        );
    }
}
