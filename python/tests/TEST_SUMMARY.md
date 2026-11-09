# Python and Rust test coverage

The suites share regression data where they exercise the same scenario, while
keeping assertions at the layer that owns the behavior.

- Rust unit tests cover algebra, parsing, normalization, planning, execution,
  evidence, and repair semantics. Engine corpus tests also compare reference,
  normalized, and planned execution; these comparisons remain independent of
  Python interface tests.
- Python tests cover public argument handling, result objects, RDFLib conversion,
  serialization, in-place mutation, and exception propagation.
- `testdata/regressions/witnesses/` holds split shapes/data fixtures consumed by
  both `test_witnesses.py` and Rust's `property_witnesses.rs`. Each suite retains
  its own assertions about its public interface.

## Python test ownership

| Tests | Responsibility |
| --- | --- |
| `test_results.py` | Passing/failing algebra result contracts, inference result contracts, reason provenance, multiple violations |
| `test_shifty.py` | W3C report contract, embedded shapes, inference deltas, in-place operations, blank-node identity, combined graph inputs |
| `test_errors.py` | Parameterized input admission, parse errors, missing files, empty inputs, and explicit bases across entry points |
| `test_convert.py` | Conversion-specific syntax, namespaces, literals, blank nodes, and unsupported types |
| `test_prepared.py` | Prepared-validator reuse, parity with one-shot validation, in-place behavior, lazy deltas, and GIL release |
| `test_shacl_features.py` | SHACL component smoke tests and custom-component boundary cases |
| `test_w3c_suite.py` | Adapted local corpus verdicts, once per case and validation interface |
| `test_conformance.py`, `test_interface_parity.py` | Upstream manifest runner and CLI comparison infrastructure |
| `test_interface_agreement.py`, `test_execution_failures.py` | Admission, diagnostics, and failure propagation across interfaces |
| `test_evidence.py`, `test_repair.py`, `test_witnesses.py` | Evidence, repair, and witness APIs |
| `test_shapemap*.py` | ShapeMap construction, projection, ordering, components, and value recovery |
| `test_file_inputs.py`, `test_sparql_graph_input.py` | File bases and SPARQL prefixes through graph conversion |
| `test_typing.py`, `test_sdist.py` | Stub completeness and source-distribution packaging |

When extending coverage, add a named parameter case for another input or public
entry point with the same contract. Combine related result-field assertions over
one execution. Keep distinct regressions separate when their setup or failure
mode differs, particularly session availability, graph scope, normalization, and
blank-node ownership. Keep mutable graphs and sessions local to each test.

Rust's adapted report corpus tests share helpers in
`crates/shifty-engine/tests/common/`; CLI fixtures and command helpers live in
`crates/shifty-cli/tests/common/`. The adapted report helpers retain their local
anonymous-shape matching rules. Full upstream report comparison belongs to the
stricter manifest runner described in `testdata/README.md`.

## Verification

Run from `python/`, rebuilding the extension after Rust changes:

```sh
uv sync --dev --frozen --reinstall-package pyshifty
uv run ruff check .
uv run ruff format --check .
uv run ty check shifty
uv run pytest -q
```

Run Rust checks from the repository root:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```
