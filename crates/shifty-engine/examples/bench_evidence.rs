//! Evidence-interface overhead over one prepared validation snapshot.
//!
//! All workflows run from the *same* `PreparedEvidenceValidator` snapshot, so parse,
//! SHACL-AF inference, normalization, stratification, dataset indexing, and
//! SPARQL-executor construction are paid once and excluded from every timing.
//! What the timer brackets is only:
//!
//!   A) `validate_conformance()` — target selection + one short-circuiting
//!      satisfaction test per selected (statement, focus) pair.
//!   B) `validate_canonical()`   — the same selection, but each pair also
//!      materializes its applicable evidence polarity (satisfaction trace or
//!      failure witness). Optional authored-statement progress is excluded.
//!   C) `find_failures()` followed by `explain_canonical()` for each failed
//!      pair — the workflow for consumers that only request failures.
//!
//! B − A is the cost of complete evidence tracing. C measures whether on-demand
//! materialization avoids that cost when failures are sparse. Serialization is
//! timed in a separate loop, since a caller that never leaves the process never
//! pays it and its allocation traffic must not perturb the validation arms.
//!
//! Usage:
//!   cargo run --release -p shifty-engine --example bench_evidence -- \
//!       --shapes benchmark/brick/Brick-closure.ttl \
//!       --models benchmark/brick/models \
//!       --suite brick --iters 5
//!
//! Emits one CSV row per model on stdout; progress and errors on stderr.

use shifty_engine::{
    ConformanceOptions, PreparedEvidenceValidator, ValidationGraphMode, ValidationOptions,
};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

struct Args {
    shapes: PathBuf,
    models: PathBuf,
    suite: String,
    iters: usize,
    min_iters: usize,
    budget_ms: f64,
    header: bool,
    no_infer: bool,
}

#[derive(Clone, Copy)]
enum Arm {
    Conformance,
    FullEvidence,
    OnDemand,
}

const ARM_ORDERS: [[Arm; 3]; 3] = [
    [Arm::Conformance, Arm::FullEvidence, Arm::OnDemand],
    [Arm::FullEvidence, Arm::OnDemand, Arm::Conformance],
    [Arm::OnDemand, Arm::Conformance, Arm::FullEvidence],
];

fn parse_args() -> Args {
    let mut args = Args {
        shapes: PathBuf::new(),
        models: PathBuf::new(),
        suite: "suite".to_string(),
        iters: 5,
        min_iters: 3,
        budget_ms: 0.0,
        header: true,
        no_infer: false,
    };
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut value = || {
            argv.next()
                .unwrap_or_else(|| panic!("{flag} needs a value"))
        };
        match flag.as_str() {
            "--shapes" => args.shapes = PathBuf::from(value()),
            "--models" => args.models = PathBuf::from(value()),
            "--suite" => args.suite = value(),
            "--iters" => args.iters = value().parse().expect("--iters must be an integer"),
            "--budget-ms" => {
                args.budget_ms = value().parse().expect("--budget-ms must be a number")
            }
            "--min-iters" => {
                args.min_iters = value().parse().expect("--min-iters must be an integer")
            }
            "--no-header" => args.header = false,
            "--no-infer" => args.no_infer = true,
            other => panic!("unknown flag {other}"),
        }
    }
    assert!(!args.shapes.as_os_str().is_empty(), "--shapes is required");
    assert!(!args.models.as_os_str().is_empty(), "--models is required");
    assert!(args.iters > 0, "--iters must be positive");
    args
}

const COLUMNS: &str = "suite,model,data_triples,iters,\
conformance_ms_median,conformance_ms_mad,\
full_evidence_ms_median,full_evidence_ms_mad,\
full_overhead_median,full_overhead_mad,\
failure_discovery_ms_median,failure_discovery_ms_mad,\
failure_explanation_ms_median,failure_explanation_ms_mad,\
on_demand_ms_median,on_demand_ms_mad,\
on_demand_overhead_median,on_demand_overhead_mad,\
serialize_ms_median,serialize_ms_mad,\
conforms_conformance,conforms_evidence,conforms_on_demand,\
evaluated_pairs,pass_pairs,fail_pairs,fail_fraction,\
authored_pairs,authored_pass_pairs,authored_fail_pairs,\
statements,\
evidence_nodes,evidence_nodes_per_authored_pair,\
evidence_bytes,evidence_bytes_per_authored_pair,\
full_run_bytes_with_catalog,full_run_bytes_no_catalog,\
compact_run_bytes_with_catalog,compact_run_bytes_no_catalog,\
node_occurrences,distinct_nodes,node_redundancy,\
result_occurrences,distinct_results,result_redundancy,\
support_occurrences,distinct_support,support_redundancy,support_share,\
term_occurrences,distinct_terms,term_redundancy,\
normalized_requests,duplicate_records,divergent_duplicates,\
canonical_occurrences,shared_payloads,shared_payload_fraction,\
shared_canonical_occurrences,shared_occurrence_fraction,\
request_reaches,requests_per_payload,max_payload_requests,\
distinct_keys,key_redundancy,multi_occurrence_keys,\
divergent_keys,divergence_fraction,divergent_occurrences,\
distinct_payloads_per_key,keys_over_payload_cap,both_polarity_addresses,\
conformance_ms_samples,full_evidence_ms_samples,\
failure_discovery_ms_samples,failure_explanation_ms_samples,on_demand_ms_samples";

fn main() {
    let args = parse_args();

    eprintln!("loading shapes {}…", args.shapes.display());
    let shapes_bytes = fs::read(&args.shapes).expect("cannot read --shapes");
    let shapes = shifty_parse::load_turtle(&shapes_bytes, None).expect("cannot parse --shapes");
    let parsed = shifty_parse::parse_loaded(&shapes);
    let raw_schema = parsed.schema.clone();
    // Inference runs against the normalized schema, exactly as the CLI does.
    let inference_schema = shifty_opt::normalize(&raw_schema);
    drop(shapes_bytes);

    if args.header {
        println!("{COLUMNS}");
    }

    for model in models_in(&args.models) {
        let name = model
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("?")
            .to_string();
        eprint!("{name}… ");

        // ---- untimed setup: everything common to all workflows --------------
        let data_bytes = fs::read(&model).expect("cannot read model");
        let data = shifty_parse::load_turtle(&data_bytes, None).expect("cannot parse model");
        drop(data_bytes);
        let data_graph = if args.no_infer {
            data.graph.clone()
        } else {
            shifty_engine::infer_graphs(&data.graph, &shapes.graph, &inference_schema)
                .unwrap_or_else(|error| panic!("{name}: inference failed: {error}"))
                .graph
        };
        let data_triples = data_graph.len();

        let prepared = PreparedEvidenceValidator::with_graphs(
            &data_graph,
            &shapes.graph,
            &raw_schema,
            ValidationGraphMode::Union,
        )
        .unwrap_or_else(|error| panic!("{name}: schema is non-stratifiable: {error}"));
        let options = ValidationOptions::default();
        let scan_options = ConformanceOptions::default();

        // ---- timed: validation only -----------------------------------------
        // One warm-up round, discarded: it faults in the caches all arms share.
        // Its duration is only consulted when `--budget-ms` is in play.
        let probe = Instant::now();
        let _ = prepared.validate_conformance(&scan_options);
        let _ = prepared.validate_canonical(&options);
        let (_, warm_failures) = prepared.find_failures(&scan_options);
        for pair in &warm_failures {
            std::hint::black_box(prepared.explain_canonical(pair));
        }
        let probe_ms = probe.elapsed().as_secs_f64() * 1e3;
        // Every model gets the same iteration count. `--budget-ms` only caps the
        // count for a corpus slow enough that a fixed count is impractical; it
        // is off by default, and the emitted `iters` column records what was used.
        let iters = if args.budget_ms > 0.0 {
            ((args.budget_ms / probe_ms.max(f64::MIN_POSITIVE)) as usize)
                .clamp(args.min_iters.min(args.iters), args.iters)
        } else {
            args.iters
        };

        let mut conformance_samples = Vec::with_capacity(iters);
        let mut evidence_samples = Vec::with_capacity(iters);
        let mut discovery_samples = Vec::with_capacity(iters);
        let mut explanation_samples = Vec::with_capacity(iters);
        let mut on_demand_samples = Vec::with_capacity(iters);
        let mut last_run = None;
        let mut last_conformance = None;
        let mut last_on_demand_conformance = None;
        let mut last_failures = Vec::new();
        let mut last_explanations = Vec::new();

        // Rotate A/B/C so no arm systematically inherits the cache, allocator,
        // scheduler, or thermal state left by another arm.
        for iteration in 0..iters {
            for arm in ARM_ORDERS[iteration % ARM_ORDERS.len()] {
                match arm {
                    Arm::Conformance => {
                        let start = Instant::now();
                        let conformance_run = prepared.validate_conformance(&scan_options);
                        conformance_samples.push(start.elapsed());
                        std::hint::black_box(conformance_run.selected_pairs);
                        last_conformance = Some(conformance_run);
                    }
                    Arm::FullEvidence => {
                        let start = Instant::now();
                        let run = prepared.validate_canonical(&options);
                        evidence_samples.push(start.elapsed());
                        std::hint::black_box(run.statements.len());
                        last_run = Some(run);
                    }
                    Arm::OnDemand => {
                        let start = Instant::now();
                        let (on_demand_conformance, failures) =
                            prepared.find_failures(&scan_options);
                        let discovery = start.elapsed();

                        let start = Instant::now();
                        let explanations: Vec<_> = failures
                            .iter()
                            .flat_map(|pair| prepared.explain_canonical(pair))
                            .collect();
                        let explanation = start.elapsed();

                        discovery_samples.push(discovery);
                        explanation_samples.push(explanation);
                        on_demand_samples.push(discovery + explanation);
                        std::hint::black_box(explanations.len());
                        last_on_demand_conformance = Some(on_demand_conformance);
                        last_failures = failures;
                        last_explanations = explanations;
                    }
                }
            }
        }

        // ---- untimed: shape of the evidence produced -------------------------
        // Reuse the final timed results rather than paying for another run.
        let conformance = last_conformance.expect("at least one iteration runs");
        let on_demand_conformance =
            last_on_demand_conformance.expect("at least one iteration runs");
        let run = last_run.expect("at least one iteration runs");
        let authored_pairs: usize = run
            .statements
            .iter()
            .map(|statement| statement.selected_foci.len())
            .sum();
        let authored_fail_pairs: usize = run
            .statements
            .iter()
            .flat_map(|statement| &statement.selected_foci)
            .filter(|focus| focus.status() == shifty_engine::EvaluationStatus::Fail)
            .count();

        // The three interfaces must describe the same normalized selection and
        // polarity partition. Compare exact failed identities, not just the
        // report-wide conformance bit.
        assert_eq!(
            conformance, on_demand_conformance,
            "{name}: on-demand counts"
        );
        assert_eq!(
            conformance.conforms, run.conforms,
            "{name}: conformance bit"
        );
        let full_selected: HashSet<_> = run
            .statements
            .iter()
            .flat_map(|statement| {
                statement.selected_foci.iter().filter_map(|focus| {
                    statement
                        .normalized_statement_id
                        .map(|id| (id, focus.focus.clone()))
                })
            })
            .collect();
        let full_failures: HashSet<_> = run
            .statements
            .iter()
            .flat_map(|statement| {
                statement
                    .selected_foci
                    .iter()
                    .filter(|focus| focus.status() == shifty_engine::EvaluationStatus::Fail)
                    .filter_map(|focus| {
                        statement
                            .normalized_statement_id
                            .map(|id| (id, focus.focus.clone()))
                    })
            })
            .collect();
        let discovered_failures: HashSet<_> = last_failures
            .iter()
            .map(|pair| (pair.normalized_statement(), pair.focus().clone()))
            .collect();
        assert_eq!(
            full_selected.len(),
            conformance.selected_pairs,
            "{name}: selected pairs"
        );
        assert_eq!(
            full_failures.len(),
            conformance.failed,
            "{name}: failed pairs"
        );
        assert_eq!(
            full_selected.len() - full_failures.len(),
            conformance.passed,
            "{name}: passed pairs"
        );
        assert_eq!(
            full_failures, discovered_failures,
            "{name}: failed identities"
        );

        let full_by_authored: HashMap<_, _> = run
            .statements
            .iter()
            .flat_map(|statement| {
                statement.selected_foci.iter().map(|focus| {
                    (
                        (statement.source_statement_id, focus.focus.clone()),
                        &focus.evidence,
                    )
                })
            })
            .collect();
        let mut explained_foci = 0;
        for statement in &last_explanations {
            for focus in &statement.selected_foci {
                explained_foci += 1;
                assert_eq!(
                    focus.status(),
                    shifty_engine::EvaluationStatus::Fail,
                    "{name}: on-demand explanation polarity"
                );
                let key = (statement.source_statement_id, focus.focus.clone());
                assert_eq!(
                    &focus.evidence,
                    *full_by_authored
                        .get(&key)
                        .unwrap_or_else(|| panic!("{name}: unexplained full-run pair {key:?}")),
                    "{name}: full and on-demand evidence differ for {key:?}"
                );
            }
        }
        assert_eq!(
            explained_foci, authored_fail_pairs,
            "{name}: authored explanations"
        );

        let evidence_nodes = run.walk().len();
        let evidence_bytes: usize = run
            .statements
            .iter()
            .flat_map(|statement| &statement.selected_foci)
            .map(|focus| focus.evidence.to_json().expect("evidence serializes").len())
            .sum();
        let full_json = run.to_json().expect("evidence run serializes");
        let full_run_bytes_with_catalog = full_json.len();
        let mut full_value_no_catalog =
            serde_json::to_value(&run).expect("evidence run converts to JSON");
        full_value_no_catalog
            .as_object_mut()
            .expect("evidence run is a JSON object")
            .remove("constraints");
        let full_run_bytes_no_catalog = serde_json::to_string(&full_value_no_catalog)
            .expect("catalog-free evidence run serializes")
            .len();

        // Compare complete runs with the same catalog policy on each side, and
        // verify both encodings reconstruct the exact typed result.
        let encoded_with_catalog =
            shifty_engine::compact(&run, true).expect("compact encoding succeeds");
        let compact_run_bytes_with_catalog = serde_json::to_string(&encoded_with_catalog)
            .expect("compact encoding serializes")
            .len();
        assert_eq!(
            shifty_engine::expand(&encoded_with_catalog).expect("compact run expands"),
            run,
            "{name}: compact run with catalog is not lossless"
        );
        let encoded_no_catalog =
            shifty_engine::compact(&run, false).expect("compact encoding succeeds");
        let compact_run_bytes_no_catalog = serde_json::to_string(&encoded_no_catalog)
            .expect("catalog-free compact encoding serializes")
            .len();
        let catalog = serde_json::to_value(&run.constraints).expect("catalog serializes");
        assert_eq!(
            shifty_engine::expand_with_catalog(&encoded_no_catalog, catalog)
                .expect("catalog-free compact run expands"),
            run,
            "{name}: compact run without catalog is not lossless"
        );
        // Sharing is reported over the evidence alone. The node and term tables
        // of `encoded` also hold catalog entries, which would understate the
        // redundancy by adding distinct entries with no evidence occurrences.
        let sharing = shifty_engine::sharing(&run).expect("sharing measures");
        let results = shifty_engine::result_sharing(&run);

        // The two passes reach the same judgment nodes by routes that share no
        // code: `sharing` interns serialized JSON, `result_sharing` walks the
        // typed evidence. Agreement on both counts is what makes either number
        // reportable; a mismatch means the node families are misclassified or a
        // traversal is missing a variant.
        assert_eq!(
            sharing.result_occurrences + sharing.support_occurrences,
            sharing.node_occurrences,
            "{name}: node families do not partition occurrences"
        );
        assert_eq!(
            sharing.distinct_results + sharing.distinct_support,
            sharing.distinct_nodes,
            "{name}: node families do not partition the table"
        );
        assert_eq!(
            sharing.result_occurrences, evidence_nodes,
            "{name}: interned judgments disagree with the typed walk"
        );
        assert_eq!(
            results.occurrences, evidence_nodes,
            "{name}: typed sharing pass disagrees with the typed walk"
        );
        assert_eq!(
            results.distinct_payloads, sharing.distinct_results,
            "{name}: distinct judgments disagree between the two passes"
        );
        // Structural equality implies key equality, never the reverse.
        assert!(
            results.distinct_keys <= results.distinct_payloads,
            "{name}: more keys than payloads"
        );
        // Requests are the normalized selection the conformance pass counts;
        // authored records are the source-preserving view over them.
        assert_eq!(
            results.normalized_requests, conformance.selected_pairs,
            "{name}: requests disagree with the conformance selection"
        );
        assert_eq!(
            results.authored_records, authored_pairs,
            "{name}: authored records disagree with the run"
        );

        // Serialization is measured separately so its allocation traffic does
        // not perturb the next validation arm.
        let mut serialize_samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let start = Instant::now();
            let json = run.to_json().expect("evidence run serializes");
            serialize_samples.push(start.elapsed());
            std::hint::black_box(json.len());
        }

        // Each ratio compares the corresponding raw samples from one rotated
        // A/B/C round. The CSV retains those samples for independent checking.
        let full_ratios: Vec<f64> = conformance_samples
            .iter()
            .zip(evidence_samples.iter())
            .map(|(conformance, evidence)| {
                evidence.as_secs_f64() / conformance.as_secs_f64().max(f64::MIN_POSITIVE)
            })
            .collect();
        let on_demand_ratios: Vec<f64> = conformance_samples
            .iter()
            .zip(on_demand_samples.iter())
            .map(|(conformance, on_demand)| {
                on_demand.as_secs_f64() / conformance.as_secs_f64().max(f64::MIN_POSITIVE)
            })
            .collect();

        let conformance_ms = summarize_durations(&conformance_samples);
        let evidence_ms = summarize_durations(&evidence_samples);
        let discovery_ms = summarize_durations(&discovery_samples);
        let explanation_ms = summarize_durations(&explanation_samples);
        let on_demand_ms = summarize_durations(&on_demand_samples);
        let serialize_ms = summarize_durations(&serialize_samples);
        let full_ratio = summarize_values(&full_ratios);
        let on_demand_ratio = summarize_values(&on_demand_ratios);
        let evaluated_pairs = conformance.selected_pairs.max(1) as f64;
        let authored_pairs_denominator = authored_pairs.max(1) as f64;

        println!(
            "{suite},{name},{data_triples},{iters},\
{c_med:.4},{c_mad:.4},{e_med:.4},{e_mad:.4},{full_ratio:.4},{full_ratio_mad:.4},\
{find_med:.4},{find_mad:.4},{explain_med:.4},{explain_mad:.4},\
{ondemand_med:.4},{ondemand_mad:.4},{ondemand_ratio:.4},{ondemand_ratio_mad:.4},\
{serialize_med:.4},{serialize_mad:.4},\
{conforms_c},{conforms_e},{conforms_o},\
{pairs_eval},{pass_pairs},{fail_pairs},{fail_fraction:.6},\
{authored_pairs},{authored_pass},{authored_fail},\
{statements},\
{evidence_nodes},{nodes_per_authored_pair:.3},\
{evidence_bytes},{bytes_per_authored_pair:.1},\
{full_with_catalog},{full_no_catalog},{compact_with_catalog},{compact_no_catalog},\
{node_occurrences},{distinct_nodes},{node_redundancy:.3},\
{result_occurrences},{distinct_results},{result_redundancy:.3},\
{support_occurrences},{distinct_support},{support_redundancy:.3},{support_share:.4},\
{term_occurrences},{distinct_terms},{term_redundancy:.3},\
{normalized_requests},{duplicate_records},{divergent_duplicates},\
{canonical_occurrences},{shared_payloads},{shared_payload_fraction:.4},\
{shared_canonical_occurrences},{shared_occurrence_fraction:.4},\
{request_reaches},{requests_per_payload:.3},{max_payload_requests},\
{distinct_keys},{key_redundancy:.3},{multi_occurrence_keys},\
{divergent_keys},{divergence_fraction:.4},{divergent_occurrences},\
{distinct_payloads_per_key},{keys_over_payload_cap},{both_polarity_addresses},\
{conformance_raw},{evidence_raw},{discovery_raw},{explanation_raw},{ondemand_raw}",
            suite = args.suite,
            node_occurrences = sharing.node_occurrences,
            distinct_nodes = sharing.distinct_nodes,
            node_redundancy = sharing.node_redundancy(),
            result_occurrences = sharing.result_occurrences,
            distinct_results = sharing.distinct_results,
            result_redundancy = sharing.result_redundancy(),
            support_occurrences = sharing.support_occurrences,
            distinct_support = sharing.distinct_support,
            support_redundancy = sharing.support_redundancy(),
            support_share = sharing.support_share(),
            term_occurrences = sharing.term_occurrences,
            distinct_terms = sharing.distinct_terms,
            term_redundancy = sharing.term_redundancy(),
            normalized_requests = results.normalized_requests,
            duplicate_records = results.duplicate_records,
            divergent_duplicates = results.divergent_duplicates,
            canonical_occurrences = results.canonical_occurrences,
            shared_payloads = results.shared_payloads,
            shared_payload_fraction = results.shared_payload_fraction(),
            shared_canonical_occurrences = results.shared_canonical_occurrences,
            shared_occurrence_fraction = results.shared_occurrence_fraction(),
            request_reaches = results.request_reaches,
            requests_per_payload = results.requests_per_payload(),
            max_payload_requests = results.max_payload_requests,
            distinct_keys = results.distinct_keys,
            key_redundancy = results.key_redundancy(),
            multi_occurrence_keys = results.multi_occurrence_keys,
            divergent_keys = results.divergent_keys,
            divergence_fraction = results.divergence_fraction(),
            divergent_occurrences = results.divergent_occurrences,
            distinct_payloads_per_key = results.distinct_payloads_per_key,
            keys_over_payload_cap = results.keys_over_payload_cap,
            both_polarity_addresses = results.both_polarity_addresses,
            c_med = conformance_ms.median,
            c_mad = conformance_ms.mad,
            e_med = evidence_ms.median,
            e_mad = evidence_ms.mad,
            full_ratio = full_ratio.median,
            full_ratio_mad = full_ratio.mad,
            find_med = discovery_ms.median,
            find_mad = discovery_ms.mad,
            explain_med = explanation_ms.median,
            explain_mad = explanation_ms.mad,
            ondemand_med = on_demand_ms.median,
            ondemand_mad = on_demand_ms.mad,
            ondemand_ratio = on_demand_ratio.median,
            ondemand_ratio_mad = on_demand_ratio.mad,
            serialize_med = serialize_ms.median,
            serialize_mad = serialize_ms.mad,
            conforms_c = conformance.conforms,
            conforms_e = run.conforms,
            conforms_o = on_demand_conformance.conforms,
            pairs_eval = conformance.selected_pairs,
            pass_pairs = conformance.passed,
            fail_pairs = conformance.failed,
            fail_fraction = conformance.failed as f64 / evaluated_pairs,
            authored_pass = authored_pairs - authored_fail_pairs,
            authored_fail = authored_fail_pairs,
            statements = run.statements.len(),
            nodes_per_authored_pair = evidence_nodes as f64 / authored_pairs_denominator,
            bytes_per_authored_pair = evidence_bytes as f64 / authored_pairs_denominator,
            full_with_catalog = full_run_bytes_with_catalog,
            full_no_catalog = full_run_bytes_no_catalog,
            compact_with_catalog = compact_run_bytes_with_catalog,
            compact_no_catalog = compact_run_bytes_no_catalog,
            conformance_raw = samples_string(&conformance_samples),
            evidence_raw = samples_string(&evidence_samples),
            discovery_raw = samples_string(&discovery_samples),
            explanation_raw = samples_string(&explanation_samples),
            ondemand_raw = samples_string(&on_demand_samples),
        );

        eprintln!(
            "median: {:.1}ms conformance, {:.1}ms full ({:.2}x), {:.1}ms on-demand ({:.2}x); {} failures / {} pairs; {iters} rounds",
            conformance_ms.median,
            evidence_ms.median,
            full_ratio.median,
            on_demand_ms.median,
            on_demand_ratio.median,
            conformance.failed,
            conformance.selected_pairs,
        );
    }
}

struct Stats {
    median: f64,
    mad: f64,
}

/// Median and median absolute deviation. Both are directly recomputable from
/// the raw samples retained in the CSV and resist an occasional descheduled run.
fn summarize_durations(samples: &[Duration]) -> Stats {
    let values: Vec<f64> = samples
        .iter()
        .map(|duration| duration.as_secs_f64() * 1e3)
        .collect();
    summarize_values(&values)
}

fn summarize_values(samples: &[f64]) -> Stats {
    let mut values = samples.to_vec();
    values.sort_by(f64::total_cmp);
    let median = median_of(&values);
    let mut deviations: Vec<f64> = values.iter().map(|value| (value - median).abs()).collect();
    deviations.sort_by(f64::total_cmp);
    Stats {
        median,
        mad: median_of(&deviations),
    }
}

fn samples_string(samples: &[Duration]) -> String {
    samples
        .iter()
        .map(|duration| format!("{:.4}", duration.as_secs_f64() * 1e3))
        .collect::<Vec<_>>()
        .join(";")
}

fn median_of(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

/// Models in stable, locale-independent order so repeated runs line up.
fn models_in(dir: &Path) -> Vec<PathBuf> {
    let mut models: Vec<PathBuf> = fs::read_dir(dir)
        .expect("cannot read --models")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "ttl"))
        .collect();
    models.sort();
    models
}
