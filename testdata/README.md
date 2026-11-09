# Test fixtures

`regressions/` contains project-owned cases shared across language bindings.
The split shapes/data files under `regressions/witnesses/` are consumed by
Python's `test_witnesses.py` and Rust's `property_witnesses.rs`; keep each
pair together when updating a scenario.

`w3c-shacl/tests/` contains the unmodified SHACL 1.0 Core/SPARQL suite from
[`w3c/data-shapes`](https://github.com/w3c/data-shapes), commit
`40231dfb97c5b606d22ae733947c0d14b4f45c07`, directory
`data-shapes-test-suite/tests/`. Its license is in `w3c-shacl/LICENSE.md`.
The runner verifies every file against `w3c-shacl/upstream.json`.

Run from `python/`:

```sh
uv sync --dev --frozen --reinstall-package pyshifty
uv run python -m conformance --include-unlisted
```

The root manifest includes 120 approved tests. `--include-unlisted` adds the
proposed `sparql/component/nodeValidator-001` test and reports it separately.
See the [runner guide](../docs/how-to/conformance.rst) for results and CI options.

## Update the upstream copy

Retrieve the pinned source with:

```sh
curl -fsSL https://codeload.github.com/w3c/data-shapes/tar.gz/40231dfb97c5b606d22ae733947c0d14b4f45c07 -o /tmp/data-shapes.tar.gz
tar -xzf /tmp/data-shapes.tar.gz -C /tmp
```

The tests are under `data-shapes-test-suite/tests/` in the extracted directory.
When adopting a newer revision, update the copy, checksums, inventory tests,
and baseline together. Keep upstream fixtures unchanged.

## Older fixtures

`test-suite/` and `data-shapes/data-shapes-test-suite/tests/` contain locally
adapted fixtures used by Rust and AF/DASH regression tests. Their pass/skip
counts are separate from the W3C report. The
[file audit](w3c-shacl/local-fixture-audit.json) records how they differ from
the pinned source.
