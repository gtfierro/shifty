# shifty Python bindings

This crate packages the `shifty` validator as a CPython extension using
[`PyO3`](https://pyo3.rs). It exposes the `shacl_rs` module for RDFlib-centric workflows
while reusing the same Rust engine that powers the CLI.

## Installation

From the repository root you can develop locally with

```bash
cd python
uv sync  # or `pip install -r requirements.txt` if you prefer
uvx maturin develop --extras rdflib --release
```

The GitHub Actions workflow (`.github/workflows/python.yml`) runs `maturin` with this
README as the PyPI long description, so keep it up to date when the bindings change.

## Usage

The extension exports two entrypoints that mirror the CLI subcommands:

```python
import shacl_rs

# Validation. When you request diagnostics, a fourth element is returned.
conforms, results_graph, report_text, diag = shacl_rs.validate(
    data_graph,
    shapes_graph,
    run_inference=True,
    inference={"min_iterations": 1, "max_iterations": 8},
    graphviz=True,
    heatmap=True,
    trace_events=True,
    return_inference_outcome=True,
)
print(diag["graphviz"])        # DOT for the shapes graph
print(diag["heatmap"])         # DOT for the execution heatmap
print(diag["trace_events"][0]) # First trace event (dict)
print(diag["inference_outcome"])

# Inference-only. Diagnostics are returned as a second element when requested.
inferred_graph, diag = shacl_rs.infer(
    data_graph,
    shapes_graph,
    min_iterations=1,
    max_iterations=4,
    graphviz=True,
    return_inference_outcome=True,
)
print(diag["inference_outcome"]["triples_added"])
```

Key options (mirroring the CLI flags):

- `skip_invalid_rules` (default: `False`), `warnings_are_errors`, `do_imports`
- Inference knobs: `min_iterations`, `max_iterations`, `run_until_converged`/`no_converge`,
  `error_on_blank_nodes`, `debug`; the `inference={...}` dict still works and aliases like
  `inference_min_iterations` remain.
- Diagnostics: `graphviz` (DOT for shapes), `heatmap` + `heatmap_all` (execution heatmap, triggers
  a validation pass), `trace_events`, `trace_file`, `trace_jsonl`, `return_inference_outcome`
  (adds iteration/insert counts).

If you omit all diagnostics, `validate` returns `(conforms, results_graph, report_text)` and
`infer` returns the inferred `rdflib.Graph` just like before.

See `example.py` and `brick.py` in this directory for full RDFlib/OntoEnv examples.

## Packaging notes

- `Cargo.toml` declares `readme = "README.md"`, so this file must be checked into the repo.
- The sdist job in GitHub Actions uses the same path when stamping metadata via `maturin`.
- Any new runtime assets (fixtures, schemas, etc.) should be listed in `pyproject.toml`
  under `tool.maturin.sdist.include` so they reach PyPI.
