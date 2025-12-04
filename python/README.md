# shacl-rs Python bindings

This crate packages the `shacl-rs` validator as a CPython extension using
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

The extension currently exports two entrypoints that mirror the CLI subcommands:

```python
import shacl_rs

conforms, results_graph, report = shacl_rs.validate(
    data_graph,
    shapes_graph,
    inference={
        "min_iterations": 1,
        "max_iterations": 8,
        "debug": True,
    },
)

inferred_graph = shacl_rs.infer(
    data_graph,
    shapes_graph,
    min_iterations=1,
    max_iterations=4,
)

Passing `inference=True` runs rule inference before validation, while a dict lets you tweak
the same knobs as the CLI without repeating every keyword (aliases like
`inference_max_iterations` remain for backward compatibility).
```

See `example.py` and `brick.py` in this directory for full RDFlib/OntoEnv examples.

## Packaging notes

- `Cargo.toml` declares `readme = "README.md"`, so this file must be checked into the repo.
- The sdist job in GitHub Actions uses the same path when stamping metadata via `maturin`.
- Any new runtime assets (fixtures, schemas, etc.) should be listed in `pyproject.toml`
  under `tool.maturin.sdist.include` so they reach PyPI.
