# Performance comparison benchmark

This directory contains `compare_engines.py`, a benchmark driver for comparing
pyshifty, pySHACL, and TopQuadrant SHACL rule execution on the Brick and
ASHRAE 223 model suites.

The benchmark measures two operations for each selected model:

- SHACL-AF rule inference to a fixed point.
- Inference plus validation against the suite ontology/shapes graph.

For each engine/model pair, the raw per-run timings are written to CSV and the
best successful timing is used for the summary CSV and plots.

## Basic usage

Run the default benchmark over all Brick and 223 models:

```bash
uv run compare_engines.py
```

Run a smaller comparison with one run per model:

```bash
uv run compare_engines.py --runs 1
```

Run only one suite or one engine:

```bash
uv run compare_engines.py --suite brick
uv run compare_engines.py --engine pyshifty
```

Each individual engine/model/run has a 10 minute wall-time timeout by default.
If a run exceeds the timeout, it is recorded as an error. With `--keep-going`,
the benchmark continues to the next run; without it, the script writes the
partial raw CSV and exits.

Change or disable the timeout:

```bash
uv run compare_engines.py --run-timeout-seconds 300
uv run compare_engines.py --run-timeout-seconds 0
```

Write outputs to another directory:

```bash
uv run compare_engines.py --out-dir /tmp/shifty-performance
```

## Log-scale model sampling

The benchmark suites contain many models with similar triple counts. Use
log-scale sampling to select a deterministic subset that spans different orders
of magnitude in model size.

```bash
uv run compare_engines.py --sample-models log --models-per-decade 3
```

The sampling method is applied independently to each suite:

1. Count triples for every candidate model.
2. Group models by `floor(log10(triples))`.
3. Select evenly spaced representatives inside each populated decade.
4. Always include the smallest and largest model in the suite.
5. Sort the selected models by triple count before benchmarking.

For example, with `--models-per-decade 3`, the tool keeps up to low, middle, and
high representatives from each populated order of magnitude. If a decade has
three or fewer models, all models in that decade are kept.

Preview the selected models without running benchmarks:

```bash
uv run compare_engines.py \
  --sample-models log \
  --models-per-decade 3 \
  --dry-run-models
```

## Model manifests

Write the exact selected models to a manifest:

```bash
uv run compare_engines.py \
  --sample-models log \
  --models-per-decade 3 \
  --write-model-manifest selected_models.csv \
  --dry-run-models
```

Run benchmarks from a saved manifest:

```bash
uv run compare_engines.py --model-manifest selected_models.csv
```

Manifest files are CSVs with these required columns:

```csv
suite,model,triples
brick,bldg24.ttl,16
s223,guideline36-2021-A-1.ttl,102
```

Only `suite` and `model` are required when reading a manifest. The `triples`
column is written for auditability and ignored when reading.

## Common invocations

Sample models, run pyshifty three times per selected model, and save the exact
selection:

```bash
uv run compare_engines.py \
  --sample-models log \
  --models-per-decade 3 \
  --engine pyshifty \
  --runs 3 \
  --write-model-manifest selected_models.csv
```

Run all engines on the saved selection, but only run pySHACL once:

```bash
uv run compare_engines.py \
  --model-manifest selected_models.csv \
  --runs 3 \
  --engine-runs pySHACL=1 \
  --run-timeout-seconds 600 \
  --keep-going
```

## Outputs

By default, outputs are written in this directory:

- `performance_comparison_raw.csv`: every engine/model/run result.
- `performance_comparison_best.csv`: best successful timing per engine/model.
- `inference_only.png`: inference timing plot.
- `inference_plus_validation.png`: inference plus validation timing plot.
