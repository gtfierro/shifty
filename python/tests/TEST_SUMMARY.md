# Tests

Rust and Python each test their own layer:

- **Rust** covers the engine: parsing, normalization, planning, execution,
  evidence, and repair.
- **Python** covers the public API: arguments, result objects, RDFLib
  conversion, serialization, and errors.

Scenarios both languages need live in `testdata/regressions/`. For example,
`testdata/regressions/witnesses/` is read by both `test_witnesses.py` and
Rust's `property_witnesses.rs`.

## Where Python tests go

| File | Tests |
| --- | --- |
| `test_results.py` | Validation and inference results, reasons, multiple violations |
| `test_shifty.py` | W3C reports, embedded shapes, inference deltas, in-place operations, blank nodes |
| `test_errors.py` | Bad input, parse errors, missing files, empty inputs |
| `test_convert.py` | RDF conversion: syntax, namespaces, literals, blank nodes |
| `test_prepared.py` | Reusing a prepared validator |
| `test_shacl_features.py` | SHACL components and custom components |
| `test_w3c_suite.py` | Verdicts on the locally adapted test suite |
| `test_conformance.py`, `test_interface_parity.py` | The W3C runner and the CLI/Python comparison |
| `test_interface_agreement.py`, `test_execution_failures.py` | Errors and diagnostics across interfaces |
| `test_evidence.py`, `test_repair.py`, `test_witnesses.py` | Evidence, repair, and witnesses |
| `test_shapemap*.py` | ShapeMaps |
| `test_file_inputs.py`, `test_sparql_graph_input.py` | File bases and SPARQL prefixes |
| `test_typing.py`, `test_sdist.py` | Type stubs and packaging |

## Writing tests

- Same behavior, another input or entry point: add a parameter case.
- Different setup or a different way of failing: write a separate test.
- Check related result fields against a single run.
- Create mutable graphs and sessions inside each test. Don't share them.

Rust test helpers live in `crates/shifty-engine/tests/common/` and
`crates/shifty-cli/tests/common/`. Full W3C report comparison happens only in
the conformance runner (see `testdata/README.md`).

## Run

See [Contributing](../../docs/contributing.rst#run-the-tests) for the full set
of checks. Python only, from `python/`:

```sh
uv sync --dev --frozen --reinstall-package pyshifty
uv run pytest -q
```
