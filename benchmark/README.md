# Benchmarks

Times the Shifty CLI on the checked-in Brick (45 models) and ASHRAE 223P
(19 models) corpora. Results appear on the docs [benchmark page](../docs/benchmarks.rst).

## Run

From the repository root, with Python 3.10+ and Rust:

```sh
python3 benchmark/run.py                   # current checkout
python3 benchmark/run.py v0.6.0-alpha.4    # one release
python3 benchmark/run.py v0.5.0 v0.5.1     # several releases
python3 benchmark/run.py --history --list  # preview every release
python3 benchmark/run.py --history         # measure releases not yet recorded
```

| Option | Effect |
| --- | --- |
| `--suite brick` / `--suite s223` | Run one corpus |
| `--iterations 5` | Take more samples (default 3) |
| `--force` | Re-measure a release that is already recorded |
| `--binary PATH` | Use an existing binary for a single version instead of building |
| `--output PATH` | Write results somewhere else |

Releases are built in temporary Git worktrees. Recorded releases are skipped
unless you pass `--force`. `HEAD` is always measured again.

## What is measured

- Each model runs `shifty infer` and `shifty validate`, each in a fresh process.
- The time covers parsing, preparation, computation, and output. Build time is excluded.
- The median of the samples is recorded.
- `validate` includes inference, so the two times overlap.
- Models use `*-closure.ttl` files with their ontology imports included, so
  no network access is needed. Cargo may still download dependencies when
  building a release.

Compare results only from the same machine, inputs, and toolchain.

## Result files

Each version gets a folder, `benchmark/results/<version>/`:

- `brick.json`, `s223.json`: every sample, plus hashes and machine details.
- `brick.txt`, `s223.txt`: readable summaries.
- `revisions.json` (in `benchmark/results/`): maps release tags to commits.

Older recordings are text only, with no machine details.

If you change the models or shapes, re-run with `--force`. Otherwise old and
new results measure different work.

## Build the chart

```sh
cd docs
make html
```

The docs build reads the recorded results and draws the chart. It never runs
benchmarks. For a JSON summary instead, run `python3 benchmark/process_results.py`.
