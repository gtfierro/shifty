# Benchmarks

Timing benchmarks for inference and validation against real building models.

## Running

Each script builds the release binary automatically, then benchmarks every
model in the corresponding `models/` directory:

```sh
./benchmark/bench_brick.sh   # 45 Brick models
./benchmark/bench_s223.sh    # 19 ASHRAE 223P models
```

Each script prints an aligned table of mean ± stddev wall-clock times (ms) for
three operations across every model in the corresponding `models/` directory:

- `infer`    — SHACL-AF rule inference (`shifty infer`)
- `infer+val` — inference + validation summary (`shifty validate`)
- `report`   — inference + validation + W3C `sh:ValidationReport` output
               (`shifty validate --report`)

Set `BENCH_ITERS` to control the number of timed runs (default: 3). Models are
iterated in a stable, locale-independent (sorted) order so repeated runs line up
row-for-row.

## Evidence-tracing overhead

`bench_evidence.sh` measures what evidence tracing costs on top of deciding
conformance, across all 45 Brick and 19 223P models.

### Reproducing the paper numbers on another machine

Requirements: a Rust toolchain (`cargo`, stable), `bash`, and — only for the
figures — [`uv`](https://docs.astral.sh/uv/), which installs matplotlib from the
script header. Nothing else needs installing; the ontology closures and models
are checked into this repository. Expect roughly 20–30 minutes end to end on a
laptop, nearly all of it SHACL-AF inference during setup rather than the timed
sections.

```sh
git clone <this repo> && cd shifty

# 1. Measure. Builds the release binary itself; writes one CSV row per model.
#    Progress goes to stderr, so redirecting stdout keeps the CSV clean.
./benchmark/bench_evidence.sh > results.csv

# 2. Report: console table, LaTeX tables, and figures.
uv run benchmark/summarize_evidence.py results.csv \
    --per-model \
    --latex paper/evidence.tex \
    --figures paper/figures
```

`--latex` writes two `booktabs` tables (`\usepackage{booktabs}` required):
`tab:evidence-overhead` (the suite summary) and `tab:evidence-per-model` (the
appendix table). `--figures` writes each figure as both `.pdf` (vector, for
`\includegraphics`) and `.png` (for previewing):

| Figure | Shows |
|---|---|
| `evidence_latency` | conformance vs. evidence latency per model, log-log, against a `y=x` "no overhead" line |
| `evidence_overhead_ecdf` | the distribution of per-model overhead, as an ECDF per suite |
| `evidence_overhead_vs_size` | whether overhead grows with the number of selected pairs |
| `evidence_compaction` | full vs. compact bytes per pair, against a `y=x` "no saving" line |

Figures are sized for a single column (3.4 × 2.5 in), use a serif face to match
body text, and take their colors from a colorblind-safe categorical palette
validated for all-pairs separation. Text tables need no dependencies at all —
run `python3 benchmark/summarize_evidence.py results.csv` and omit `--figures`.

Two properties make a run trustworthy, and both are reported: `conforms` is
cross-checked between the arms for every model (any divergence is printed to
stderr and invalidates the comparison), and per-model coefficients of variation
are summarized so measurement noise is visible next to the effect. On a quiet
machine expect a median CV of 1–3%; if yours is far higher, the machine was
busy and the run should be repeated.

### Running one suite

```sh
./benchmark/bench_evidence.sh brick > brick.csv     # or s223, or lubm
```

Unlike the scripts above it does not shell out to the CLI, because the CLI
timing is dominated by parsing an 11–18 MB ontology closure. Instead the
`bench_evidence` example builds **one** `PreparedEvidenceValidator` snapshot per
model and runs both arms from it, so parsing, SHACL-AF inference, normalization,
stratification, dataset indexing, and SPARQL-executor construction are paid once
and excluded from every timing. The timer brackets only:

- `validate_conformance()` — target selection, then one short-circuiting
  satisfaction test per selected `(statement, focus)` pair;
- `validate()` — the same selection, but every pair also materializes its
  applicable evidence polarity plus authored-statement progress.

The two arms are interleaved within each iteration so thermal and scheduler
drift hits both alike, and `conforms` is cross-checked between them per model —
a divergence is reported loudly and means the comparison is invalid.
JSON serialization is timed as its own column rather than folded into the
evidence arm, since an in-process caller never pays it.

Per-pair figures divide by *evaluated* pairs (normalized statements), not by the
authored fan-out: several authored statements that normalize together share one
evidence tree, and the summary reports that fan-out factor separately.
`run_bytes` covers the whole `EvidenceRun` including its two constraint
catalogs, which are a fixed per-run cost independent of corpus size;
`evidence_bytes` is the per-pair evidence payload alone.

Every model is timed for the same number of iterations — `BENCH_ITERS`, default
5 — after one discarded warm-up pair, so a mean and standard deviation mean the
same thing in every row. `BENCH_BUDGET_MS` is an escape hatch for a corpus too
slow to afford a fixed count: set it to a millisecond budget and the count
adapts down to `BENCH_MIN_ITERS` (default 3, never 1 — a lone sample gives no
spread at all). It is off by default, and the `iters` column always records what
was actually used.

Each row carries both a mean with its sample standard deviation and a median
with its median absolute deviation; disagreement between the two is itself the
signal that a model's timings were contaminated. Overhead is measured as a
*paired* per-iteration ratio, since the arms are interleaved — that cancels
drift a ratio of separately-summarized arms would keep. Across models the
summary uses the geometric mean with a geometric standard deviation
(`geomean ×/÷ gsd`), which is the correct dispersion for ratios.

Size columns cover both encodings: `run_bytes` is the full `EvidenceRun`,
`compact_bytes` the hash-consed encoding, and `compact_bytes_no_catalog` the
same with the constraint catalog elided for consumers that hold the schema.

### Attribution

`probe_evidence_cost` explains a single model rather than timing the corpus:
memo hits/misses, evidence nodes visited versus retained, `path_support` probes,
and a per-statement ranking of materialization time. Use it when a suite's
overhead needs a cause:

```sh
cargo run --release -p shifty-engine --example probe_evidence_cost -- \
    --shapes benchmark/brick/Brick-closure.ttl \
    --data benchmark/brick/models/bldg1.ttl
```

`analyze_evidence_size.py` does the same for bytes, pricing each candidate
compaction against a dumped run (`--dump-json`).

## Comparing two runs

`benchcmp.sh` compares two bench outputs, benchcmp-style:

```sh
./benchmark/bench_brick.sh > old.txt
# …change something, rebuild…
./benchmark/bench_brick.sh > new.txt
./benchmark/benchcmp.sh old.txt new.txt
```

For each column (infer, validate, report) it prints a per-model table of old ms,
new ms, and the percentage delta (new relative to old; negative is faster),
followed by a geometric-mean delta. Rows are matched by model name, and models
present in only one file are reported on stderr.

## Data

```
benchmark/
  brick/
    Brick.ttl              # Brick ontology
    Brick-closure.ttl      # Brick + all transitive OWL imports (used by scripts)
    models/                # 45 real building models
  s223/
    223p.ttl               # ASHRAE 223P ontology
    223p-closure.ttl       # 223P + all transitive OWL imports (used by scripts)
    models/                # 19 real building models (NIST, LBNL, NREL, PNNL)
```

The `*-closure.ttl` files are pre-computed and passed as the `--shapes` argument
so the engine does not need to fetch remote imports at benchmark time.
