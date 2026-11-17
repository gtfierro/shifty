# Test fixtures

| Folder | What it is |
| --- | --- |
| `w3c-shacl/` | The official W3C SHACL 1.0 test suite, unmodified |
| `regressions/` | Shifty's own regression cases, shared by Rust and Python |
| `test-suite/`, `data-shapes/` | Older, locally adapted fixtures for Rust and SHACL-AF tests |

## W3C suite

`w3c-shacl/tests/` is a copy of `data-shapes-test-suite/tests/` from
[`w3c/data-shapes`](https://github.com/w3c/data-shapes) at commit
`40231dfb97c5b606d22ae733947c0d14b4f45c07`. The license is in
`w3c-shacl/LICENSE.md`.

Don't edit these files. The runner checks each one against the checksums in
`w3c-shacl/upstream.json`.

To run it, from `python/`:

```sh
uv sync --dev --frozen --reinstall-package pyshifty
uv run python -m conformance --include-unlisted
```

See the [runner guide](../docs/how-to/conformance.rst) for options and CI.

### Update to a newer revision

Download the pinned source:

```sh
curl -fsSL https://codeload.github.com/w3c/data-shapes/tar.gz/40231dfb97c5b606d22ae733947c0d14b4f45c07 -o /tmp/data-shapes.tar.gz
tar -xzf /tmp/data-shapes.tar.gz -C /tmp
```

When moving to a new revision, update all of these at once:

- the copied tests
- the checksums in `upstream.json`
- the inventory tests
- `baseline.json`

## Regressions

`regressions/witnesses/` holds shapes and data files in pairs. Python's
`test_witnesses.py` and Rust's `property_witnesses.rs` both read them. Keep
each pair together.

## Older fixtures

`test-suite/` and `data-shapes/data-shapes-test-suite/tests/` were adapted
locally. Their results don't count toward the W3C report.
[`local-fixture-audit.json`](w3c-shacl/local-fixture-audit.json) records how
they differ from upstream.
