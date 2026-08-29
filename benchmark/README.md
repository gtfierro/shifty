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

`bench_evidence.sh` measures complete and failure-only evidence costs on top of deciding conformance across all 45 Brick and 19 223P models.

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
BENCH_METADATA=results-machine.txt ./benchmark/bench_evidence.sh > results.csv

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
| `evidence_latency` | conformance vs. full-evidence latency per model, log-log, against a `y=x` "no overhead" line |
| `evidence_overhead_ecdf` | the distribution of per-model overhead, as an ECDF per suite |
| `evidence_on_demand` | on-demand overhead against the fraction of evaluated pairs that fail |
| `evidence_compaction` | complete full vs. compact runs with both catalogs elided, against a `y=x` "no saving" line |

Figures are sized for a single column (3.4 × 2.5 in), use a serif face to match
body text, and take their colors from a colorblind-safe categorical palette
validated for all-pairs separation. Text tables need no dependencies at all —
run `python3 benchmark/summarize_evidence.py results.csv` and omit `--figures`.

The benchmark checks the report-wide result, selected/pass/fail counts, exact failed pair identities, and full-versus-on-demand evidence for every failure.
It also expands both compact encodings and requires exact equality with the original run.
Any disagreement stops the benchmark.

### Running one suite

```sh
./benchmark/bench_evidence.sh brick > brick.csv     # or s223
```

Unlike the scripts above it does not shell out to the CLI because the CLI timing is dominated by parsing an 11–18 MB ontology closure.
The `bench_evidence` example builds one `PreparedEvidenceValidator` snapshot per model, so parsing, SHACL-AF inference, normalization, stratification, dataset indexing, and SPARQL-executor construction are paid once and excluded from every timing.
The timer brackets three workflows:

- `validate_conformance()` — target selection, then one short-circuiting
  satisfaction test per selected `(statement, focus)` pair;
- `validate_canonical()` — the same selection, but every pair also materializes
  its applicable satisfaction trace or failure witness;
- `find_failures()` followed by `explain_canonical()` for every returned pair —
  the failure-only on-demand workflow.

The benchmark rotates the three execution orders across iterations so no workflow always inherits the cache, allocator, scheduler, or thermal state left by another workflow.
JSON serialization runs in a separate loop so its allocation traffic cannot perturb validation timing.

Runtime counts use normalized evaluated pairs because those are the pairs the engine executes.
Serialized-size counts use authored pairs because those are the records the consumer receives after source fanout.

Every model is timed for the same number of iterations, `BENCH_ITERS`, which defaults to 10, after one discarded warm-up round.
Each row reports the median and median absolute deviation and retains the ten raw samples as semicolon-separated fields.
The overhead value is the median of ten paired per-round ratios and is therefore directly recomputable from those fields.
The summarizer recomputes every median, median absolute deviation, and paired overhead from the raw fields and rejects inconsistent rows.
`BENCH_BUDGET_MS` remains an off-by-default escape hatch for a corpus too slow to afford a fixed count.

Size columns report the complete full and compact run both with and without the constraint catalog.
Every comparison uses the same catalog policy on both sides.

`node_redundancy` and `term_redundancy` say *why* the compact encoding is
smaller: occurrences per distinct entry, for tagged nodes and for RDF terms.
Both are measured over the evidence alone, excluding the constraint catalog —
the catalog is interned into the same tables, so counting it would add distinct
entries that no evidence occurrence refers to and understate the sharing. They
come from `shifty_engine::sharing()`, which counts against the same predicates
the encoder interns by, so the reported ratio cannot drift from what compaction
collapses.

### `node_redundancy` is not sharing between validation results

Three enums serialize as `{"type", "details"}` and the encoder interns all
three: `Witness`, `SatTrace`, and `PathSupport`. The first two are validation
judgments; the third is a path certificate saying how a value was reached, which
is not a judgment about anything. Across the corpus, path support is roughly
three quarters of tagged-node occurrences — and its share differs sharply
between suites, so the Brick/223P contrast in `node_redundancy` partly tracks
certificate density rather than how much the suites share.

Read `result_redundancy` for judgments and `support_redundancy` for
certificates; `support_share` gives the mixture. `node_redundancy` remains the
right number for *what compaction collapses*, and only for that.

### Sharing across independently addressable results

`shifty_engine::result_sharing()` measures the other question, over the typed
evidence rather than over JSON. Its judgment counts must equal the encoder's, so
the two implementations check each other; `bench_evidence` asserts this per
model and fails the run on a mismatch.

Two things it settles that the byte-level numbers cannot:

- **The unit.** A run holds one record per *authored* `(statement, focus)` pair
  because a report must name the statement its reader wrote, but two authored
  statements that normalize together are one *request*. Sharing is counted
  across `normalized_requests`; `duplicate_records` is what source traceability
  costs on top, and `divergent_duplicates` should be 0 — records answering one
  request must agree.
- **Whether the key is an address.** `(constraint, node, polarity)` is the shape
  memo's key plus its outcome. It addresses evidence only if it determines
  evidence, which is not obvious: `Witness::Atom` carries `reached_by` and
  `produced_by`, which describe the derivation rather than the judgment.
  `divergent_keys` over `multi_occurrence_keys` is how often one key holds
  several payloads, and it is what decides whether evidence can be memoized the
  way conformance already is.

The two together give a bracket. `result_redundancy` is what hash-consing
collapses losslessly today; `key_redundancy` is what a judgment-keyed memo could
collapse if divergence were zero. Quote the first as achieved and the second
only alongside the divergence figure.

`both_polarity_addresses` should be 0: a `(constraint, node)` has one truth
value per run. It is measured rather than assumed, and the summarizer warns on
it.

### Check the ratio's denominator before reading it as a trend

`overhead_ratio_*` sorted by `data_triples` rises steeply — roughly 1.4x on the
smallest models to ~5x on the largest — which reads as evidence tracing getting
more expensive on bigger graphs. In the last run it was not: decomposing both
arms by `evaluated_pairs` showed evidence at a near-constant cost per pair while
*conformance* got several times cheaper per pair as models grew. The ratio moved
because the denominator fell.

Worth re-checking against fresh numbers rather than assumed, since it decides
what the ratio can be claimed to show. Divide `conformance_ms_median` and
`evidence_ms_median` by `evaluated_pairs` and look at each arm separately. For
reference, `results/evidence/evidence.csv` as of that run gave, smallest to
largest model:

| | conformance µs/pair | evidence µs/pair |
|---|---|---|
| Brick | 125 → 23 | 176 → 130 |
| 223P | 51 → 15 | 70 → 43 |

`pnnl-bdg2-1` is the useful check from the other direction — the largest 223P
model, but the *lowest* ratio in its suite (1.78x), because its conformance is
anomalously expensive per pair rather than because its evidence is cheap.

Bytes behave differently from time and the two corpora disagreed, which is the
part most worth confirming: `evidence_bytes_per_pair` grew about 19x across
Brick (995 → 19,110) while staying flat on 223P (~2,000) over a comparable size
range. If that holds it is a statement about structure — Brick's larger models
having deeper `subClassOf*` chains and more path branching per focus — and not
about triple count, so it should not be reported as a size effect without
checking `evidence_nodes_per_pair` alongside it.

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
