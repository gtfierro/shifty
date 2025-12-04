*Note*:  This is a nearly 100% GenAI generated codebase. The initial work was done with Gemini 2.5 Pro, and the later work was done with ChatGPT 5 with the Codex tool.
This is an experiment to see how well I could create a SHACL/SHACL-AF implementation in Rust using AI tools.

---

# shacl-rs

`shacl-rs` is a Rust implementation of the Shapes Constraint Language (SHACL) with two crates:

- `lib/`: reusable validation engine
- `cli/`: end-user binary that wraps the engine with visualization and debugging tools

The workspace also ships with Python bindings (`python/`) so the same validator can run inside a notebook or existing RDFlib pipeline.

## Building

```bash
cargo build --workspace
```

Format, lint, and test when contributing:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features
cargo test --workspace
```

## CLI Overview

Run `cargo run -p cli -- --help` to see every subcommand. The most common entry points are:

- `validate`: run SHACL validation (optionally with rule inference)
- `inference`: emit only the triples inferred by SHACL rules
- `graphviz` / `graphviz-heatmap`: output DOT graphs for shapes or execution counts
- `pdf` / `pdf-heatmap`: render the DOT graphs directly to PDF

You can now request the visualization artifacts directly from `validate` or `inference` by appending:

- `--graphviz` to print the DOT description after execution
- `--pdf-heatmap heatmap.pdf [--pdf-heatmap-all]` to write the heatmap PDF (the inference command will trigger a validation pass when this flag is set)

### Validation example

```bash
cargo run -p cli -- \
  validate \
  --shapes-file examples/shapes.ttl \
  --data-file examples/data.ttl \
  --format turtle \
  --run-inference \
  --inference-min-iterations 1 \
  --inference-max-iterations 8 \
  --inference-debug
```

- `--format` chooses the report output (`turtle`, `rdf-xml`, `ntriples`, or `dump`).
- Inference flags mirror the standalone `inference` subcommand (`--inference-no-converge`, `--inference-error-on-blank-nodes`, etc.).

### Inference example

```bash
cargo run -p cli -- \
  inference \
  --shapes-file examples/shapes.ttl \
  --data-file examples/data.ttl \
  --min-iterations 1 \
  --max-iterations 12 \
  --debug \
  --output-file inferred.ttl
```

Use `--union` to emit the original data plus inferred triples.

## Python API

Install the extension module with `uvx maturin develop` (or `maturin develop --release`) inside `python/`. The module exposes two functions:

```python
import shacl_rs

shacl_rs.infer(
    data_graph: rdflib.Graph,
    shapes_graph: rdflib.Graph,
    *,
    min_iterations=None,
    max_iterations=None,
    run_until_converged=None,
    no_converge=None,
    error_on_blank_nodes=None,
    enable_af=True,
    enable_rules=True,
    debug=None,
    skip_invalid_rules=None,
) -> rdflib.Graph

shacl_rs.validate(
    data_graph: rdflib.Graph,
    shapes_graph: rdflib.Graph,
    *,
    run_inference=False,
    inference=None,
    min_iterations=None,
    max_iterations=None,
    run_until_converged=None,
    no_converge=None,
    inference_min_iterations=None,
    inference_max_iterations=None,
    inference_no_converge=None,
    error_on_blank_nodes=None,
    inference_error_on_blank_nodes=None,
    enable_af=True,
    enable_rules=True,
    debug=None,
    inference_debug=None,
    skip_invalid_rules=None,
) -> (bool, rdflib.Graph, str)
```

- `infer` returns only the new triples.
- `validate` returns `(conforms, results_graph, report_turtle)`.
- `inference` can be `True`/`False` or a dict that groups inference options (e.g. `{"min_iterations": 2, "debug": True}`) so you don't have to repeat CLI-style flags in Python. Explicit keyword arguments still work and continue to accept their `inference_*` aliases for backward compatibility.

Example:

```python
conforms, results_graph, report_text = shacl_rs.validate(
    data_graph,
    shapes_graph,
    inference={"min_iterations": 2, "max_iterations": 6, "debug": True},
)
```

### Python example (adapted from `python/brick.py`)

```python
from rdflib import Graph
from ontoenv import OntoEnv
import shacl_rs

env = OntoEnv()
model_iri = env.add("https://example.com/model.ttl")
data_graph = env.get_graph(model_iri)
shapes_graph, imports = env.get_closure(model_iri)

print(f"SHACL graph imports: {imports}")

inferred = shacl_rs.infer(data_graph, shapes_graph, debug=True)
print(inferred.serialize(format="turtle"))

conforms, results_graph, report_text = shacl_rs.validate(
    data_graph,
    shapes_graph,
    inference={"max_iterations": 12, "debug": True},
)
print(f"Model conforms: {conforms}")
print(report_text)
```

## Repository layout

```
lib/      # core validator crate (exported as `shacl`)
cli/      # command-line interface
python/   # PyO3 bindings and RDFlib examples
docs/     # additional design docs and profiles
```

Need help? Open an issue or discussion in this repo with the failing SHACL shapes and data.
