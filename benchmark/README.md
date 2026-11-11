# Benchmarks

The documentation's [benchmark page](../docs/benchmarks.rst) tracks fresh-process
CLI inference and validation on the checked-in Brick and ASHRAE 223P corpora.

## Collect measurements

From the repository root with Python 3.10+ and Rust installed:

```sh
python3 benchmark/run.py                               # current checkout, both corpora
python3 benchmark/run.py v0.6.0-alpha.4                  # one tagged release
python3 benchmark/run.py v0.5.0 v0.5.1                   # selected releases
python3 benchmark/run.py --history --list                # preview the full history
python3 benchmark/run.py --history                      # measure missing tags
```

Use `--suite brick` or `--suite s223` for one corpus, `--iterations 5` for more
samples, and `--force` to replace tagged results. Complete tagged recordings
are reused before a build is started. `HEAD` is measured again on each run.
`--binary PATH` uses an existing binary for one version. `--output PATH` writes
to a separate results directory.

Tagged releases are built in temporary Git worktrees. Inputs always come from
the current checkout. There are 45 Brick models and 19 ASHRAE 223P models. Their
`*-closure.ttl` files include ontology imports, so measurement needs no ontology
network requests. Cargo may need to download dependencies while building.

## Methodology and files

Each model runs `shifty infer` and `shifty validate` three times by default.
Each sample uses a fresh process and includes parsing, preparation, computation,
and output formatting. Build time is excluded. Medians and maximum deviations
are retained. Validation includes inference, so the two measurements overlap.
The OS file cache can stay warm across processes.

Use the same machine, inputs, sample count, and toolchain when comparing
versions. New runs select `--on-unsupported ignore` where the CLI supports it,
matching the best-effort behavior of older releases. A validation verdict of
nonconformance is valid output. An execution error fails collection.

Results live under `benchmark/results/<version>/`:

- `brick.json` and `s223.json` contain provenance, input and binary hashes,
  per-model summaries, and every sample. A suite is published only after all
  its models finish successfully.
- `brick.txt` and `s223.txt` contain readable summaries. The reader also
  accepts historical tables with extra report columns.
- `revisions.json` maps release tags to their commit IDs, including annotated
  tags. The collector refreshes it from Git so docs builds can resolve earlier
  text recordings in a shallow checkout.
- Older text-only recordings have no machine or sample metadata. Their source
  revision is resolved from the release tag. An old `HEAD` capture needs to be
  measured again to establish its revision.

The collector checks model coverage and input hashes before reusing a new
recording. After changing fixtures, remeasure versions with `--force` so their
results describe the same workload. Historical text-only recordings cannot
verify their input hashes.

## Build the page

```sh
cd docs
make html
```

Sphinx runs `process_results.py` to read recordings and generate the chart data
inside `_build/`. It does not run measurements or change checked-in assets.
For a standalone JSON summary:

```sh
python3 benchmark/process_results.py
```

The page includes every recorded version, including prereleases. Its per-model
table can compare any two recordings and uses shared model names for the
summary ratio.

## Other measurements

These serve separate implementation and research questions:

- [Evidence overhead](evidence.md) uses `bench_evidence.sh` and
  `summarize_evidence.py` to measure prepared-session conformance and evidence.
- [Shared datasets](shared-dataset-results.md) records the storage experiments
  driven by `compare_shared_datasets.py` and `compare_shared_sessions.py`.
