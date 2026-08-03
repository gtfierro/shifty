//! Evidence-tracing overhead: prepared conformance-only vs. prepared dual evidence.
//!
//! Both arms run from the *same* `PreparedEvidenceValidator` snapshot, so parse,
//! SHACL-AF inference, normalization, stratification, dataset indexing, and
//! SPARQL-executor construction are paid once and excluded from every timing.
//! What the timer brackets is only:
//!
//!   A) `validate_conformance()` — target selection + one short-circuiting
//!      satisfaction test per selected (statement, focus) pair.
//!   B) `validate()`             — the same selection, but each pair also
//!      materializes its applicable evidence polarity (satisfaction trace or
//!      failure witness) plus authored-statement progress.
//!
//! B − A is the cost of evidence tracing. Serialization is timed and reported
//! separately, since a caller that never leaves the process never pays it.
//!
//! Usage:
//!   cargo run --release -p shifty-engine --example bench_evidence -- \
//!       --shapes benchmark/brick/Brick-closure.ttl \
//!       --models benchmark/brick/models \
//!       --suite brick --iters 5
//!
//! Emits one CSV row per model on stdout; progress and errors on stderr.

use shifty_engine::{PreparedEvidenceValidator, ValidationGraphMode, ValidationOptions};
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
        let mut value = || argv.next().unwrap_or_else(|| panic!("{flag} needs a value"));
        match flag.as_str() {
            "--shapes" => args.shapes = PathBuf::from(value()),
            "--models" => args.models = PathBuf::from(value()),
            "--suite" => args.suite = value(),
            "--iters" => args.iters = value().parse().expect("--iters must be an integer"),
            "--budget-ms" => args.budget_ms = value().parse().expect("--budget-ms must be a number"),
            "--min-iters" => args.min_iters = value().parse().expect("--min-iters must be an integer"),
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
conformance_ms_mean,conformance_ms_sd,conformance_cv,\
conformance_ms_median,conformance_ms_min,conformance_ms_mad,\
evidence_ms_mean,evidence_ms_sd,evidence_cv,\
evidence_ms_median,evidence_ms_min,evidence_ms_mad,\
overhead_ratio_mean,overhead_ratio_sd,overhead_ratio_median,overhead_ms,\
serialize_ms_median,\
conforms_conformance,conforms_evidence,\
evaluated_pairs,pass_pairs,fail_pairs,\
authored_pairs,authored_pass_pairs,authored_fail_pairs,\
statements,\
evidence_nodes,evidence_nodes_per_pair,\
evidence_bytes,evidence_bytes_per_pair,run_bytes,\
compact_bytes,compact_bytes_no_catalog,compact_nodes,compact_terms";

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

        // ---- untimed setup: everything common to both arms ------------------
        let data_bytes = fs::read(&model).expect("cannot read model");
        let data = shifty_parse::load_turtle(&data_bytes, None).expect("cannot parse model");
        drop(data_bytes);
        let data_graph = if args.no_infer {
            data.graph.clone()
        } else {
            match shifty_engine::infer_graphs(&data.graph, &shapes.graph, &inference_schema) {
                Ok(outcome) => outcome.graph,
                Err(error) => {
                    eprintln!("skipped (inference: {error})");
                    continue;
                }
            }
        };
        let data_triples = data_graph.len();

        let prepared = match PreparedEvidenceValidator::with_graphs(
            &data_graph,
            &shapes.graph,
            &raw_schema,
            ValidationGraphMode::Union,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                eprintln!("skipped (non-stratifiable: {error})");
                continue;
            }
        };
        let options = ValidationOptions::default();

        // ---- timed: validation only -----------------------------------------
        // One warm-up pair, discarded: it faults in the caches both arms share.
        // Its duration is only consulted when `--budget-ms` is in play.
        let probe = Instant::now();
        let _ = prepared.validate_conformance(&options);
        let _ = prepared.validate(&options);
        let probe_ms = probe.elapsed().as_secs_f64() * 1e3;
        // Every model gets the same iteration count, so a reported mean and SD
        // mean the same thing in every row. `--budget-ms` only caps the count
        // for a corpus slow enough that a fixed count is impractical; it is off
        // by default, and the emitted `iters` column records what was used.
        let iters = if args.budget_ms > 0.0 {
            ((args.budget_ms / probe_ms.max(f64::MIN_POSITIVE)) as usize)
                .clamp(args.min_iters.min(args.iters), args.iters)
        } else {
            args.iters
        };

        let mut conformance_samples = Vec::with_capacity(iters);
        let mut evidence_samples = Vec::with_capacity(iters);
        let mut serialize_samples = Vec::with_capacity(iters);
        let mut last: Option<_> = None;
        let mut conformance = None;
        // Interleaved A/B so thermal or scheduler drift hits both arms alike.
        for _ in 0..iters {
            let start = Instant::now();
            let conformance_run = prepared.validate_conformance(&options);
            conformance_samples.push(start.elapsed());
            std::hint::black_box(conformance_run.selected_pairs);
            conformance = Some(conformance_run);

            let start = Instant::now();
            let run = prepared.validate(&options);
            evidence_samples.push(start.elapsed());

            let start = Instant::now();
            let json = run.to_json().expect("evidence run serializes");
            serialize_samples.push(start.elapsed());
            std::hint::black_box(json.len());
            last = Some(run);
        }

        // ---- untimed: shape of the evidence produced -------------------------
        // Reuse the final timed run rather than paying for another one.
        let conformance = conformance.expect("at least one iteration runs");
        let run = last.expect("at least one iteration runs");
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
        let evidence_nodes = run.walk().len();
        let evidence_bytes: usize = run
            .statements
            .iter()
            .flat_map(|statement| &statement.selected_foci)
            .map(|focus| focus.evidence.to_json().expect("evidence serializes").len())
            .sum();
        let run_bytes = run.to_json().expect("evidence run serializes").len();
        // The compact encoding is lossless; `no_catalog` additionally assumes
        // the consumer already holds the schema the run was validated against.
        let encoded = shifty_engine::compact(&run, true).expect("compact encoding succeeds");
        let compact_bytes = serde_json::to_string(&encoded)
            .expect("compact encoding serializes")
            .len();
        let compact_bytes_no_catalog = shifty_engine::to_compact_json(&run, false)
            .expect("compact encoding succeeds")
            .len();
        let table_len = |key: &str| {
            encoded
                .get(key)
                .and_then(|value| value.as_array())
                .map_or(0, Vec::len)
        };
        let compact_nodes = table_len("nodes");
        let compact_terms = table_len("terms");

        // Paired per-iteration ratios, computed before `summarize` sorts the
        // samples. The arms are interleaved, so each iteration compares them
        // under the same machine conditions: pairing removes the drift that a
        // ratio of independently-summarized arms would leave in.
        let paired_ratios: Vec<f64> = conformance_samples
            .iter()
            .zip(evidence_samples.iter())
            .map(|(conformance, evidence)| {
                evidence.as_secs_f64() / conformance.as_secs_f64().max(f64::MIN_POSITIVE)
            })
            .collect();
        let ratio_mean = mean_of(&paired_ratios);
        let ratio_sd = sd_of(&paired_ratios);

        let conformance_ms = summarize(&mut conformance_samples);
        let evidence_ms = summarize(&mut evidence_samples);
        let serialize_ms = summarize(&mut serialize_samples);
        // Per-pair figures use the pairs actually evaluated, not the authored
        // fan-out, so they do not double-count shared evidence.
        let pairs = conformance.selected_pairs.max(1) as f64;

        println!(
            "{suite},{name},{data_triples},{iters},\
{c_mean:.4},{c_sd:.4},{c_cv:.4},\
{c_med:.4},{c_min:.4},{c_mad:.4},\
{e_mean:.4},{e_sd:.4},{e_cv:.4},\
{e_med:.4},{e_min:.4},{e_mad:.4},\
{ratio_mean:.4},{ratio_sd:.4},{ratio:.4},{delta:.4},\
{s_med:.4},\
{conforms_c},{conforms_e},\
{pairs_eval},{pass_pairs},{fail_pairs},\
{authored_pairs},{authored_pass},{authored_fail},\
{statements},\
{evidence_nodes},{nodes_per_pair:.3},\
{evidence_bytes},{bytes_per_pair:.1},{run_bytes},\
{compact_bytes},{compact_bytes_no_catalog},{compact_nodes},{compact_terms}",
            suite = args.suite,
            c_mean = conformance_ms.mean,
            c_sd = conformance_ms.sd,
            c_cv = conformance_ms.cv(),
            c_med = conformance_ms.median,
            c_min = conformance_ms.min,
            c_mad = conformance_ms.mad,
            e_mean = evidence_ms.mean,
            e_sd = evidence_ms.sd,
            e_cv = evidence_ms.cv(),
            e_med = evidence_ms.median,
            e_min = evidence_ms.min,
            e_mad = evidence_ms.mad,
            ratio = evidence_ms.median / conformance_ms.median.max(f64::MIN_POSITIVE),
            delta = evidence_ms.median - conformance_ms.median,
            s_med = serialize_ms.median,
            conforms_c = conformance.conforms,
            conforms_e = run.conforms,
            pairs_eval = conformance.selected_pairs,
            pass_pairs = conformance.passed,
            fail_pairs = conformance.failed,
            authored_pass = authored_pairs - authored_fail_pairs,
            authored_fail = authored_fail_pairs,
            statements = run.statements.len(),
            nodes_per_pair = evidence_nodes as f64 / pairs,
            bytes_per_pair = evidence_bytes as f64 / pairs,
        );

        if conformance.conforms != run.conforms {
            eprint!("CONFORMANCE MISMATCH! ");
        }
        eprintln!(
            "{:.1}±{:.1}ms → {:.1}±{:.1}ms ({ratio_mean:.2}±{ratio_sd:.2}x), {} pairs, {iters} iter(s)",
            conformance_ms.mean,
            conformance_ms.sd,
            evidence_ms.mean,
            evidence_ms.sd,
            conformance.selected_pairs
        );
    }
}

struct Stats {
    median: f64,
    min: f64,
    mad: f64,
    mean: f64,
    /// Sample standard deviation (n-1). `NaN` for a single sample, which is
    /// reported rather than passed off as zero spread.
    sd: f64,
}

impl Stats {
    /// Relative spread. Scale-free, so it compares across a corpus whose
    /// per-model times span four orders of magnitude.
    fn cv(&self) -> f64 {
        self.sd / self.mean
    }
}

/// Location and spread, in milliseconds, reported both ways: the mean with its
/// sample standard deviation, and the median with its median absolute
/// deviation. The mean answers "what does a run cost on average", the median
/// resists the occasional descheduled run, and disagreement between them is
/// itself the signal that a model's timings are contaminated.
fn summarize(samples: &mut [Duration]) -> Stats {
    let mut ms: Vec<f64> = samples.iter().map(|d| d.as_secs_f64() * 1e3).collect();
    ms.sort_by(f64::total_cmp);
    let median = median_of(&ms);
    let mut deviations: Vec<f64> = ms.iter().map(|value| (value - median).abs()).collect();
    deviations.sort_by(f64::total_cmp);
    Stats {
        median,
        min: ms[0],
        mad: median_of(&deviations),
        mean: mean_of(&ms),
        sd: sd_of(&ms),
    }
}

fn mean_of(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

/// Sample standard deviation; `NaN` when a single sample gives no estimate.
fn sd_of(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return f64::NAN;
    }
    let mean = mean_of(values);
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    variance.sqrt()
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
