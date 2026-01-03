*Note*:  This is a nearly 100% GenAI generated codebase. The initial work was done with Gemini 2.5 Pro, and the later work was done with ChatGPT 5 with the Codex tool.
This is an experiment to see how well I could create a SHACL/SHACL-AF implementation in Rust using AI tools. The "sh" in "shifty" comes from the fact it is a SHACL 
implementation, and the overall name is a pun on how shifty it is to vibecode a gigantic Rust project and use that for something as critical as "validation."

---

# shifty

`shifty` is a Rust implementation of the Shapes Constraint Language (SHACL) with two crates:

- `lib/`: reusable validation engine
- `cli/`: end-user binary that wraps the engine with visualization and debugging tools

The workspace also ships with Python bindings (`python/`) so the same validator can run inside a notebook or existing RDFlib pipeline.

## Highlights

- `generate-ir` writes a `shacl-ir` cache so every invocation of `validate` or `infer` can skip reparsing the shapes graph and reuse the cached `ShapeIR`; the cache now includes the shapes graph (and resolved imports) so downstream runs don't need to reload shapes separately.
- `heat`, `trace`, `visualize-heatmap`, and `pdf-heatmap` commands expose component frequencies, execution traces, and heatmap diagnostics for validation runs.
- Validation and inference run against the union of the data graph **and** shapes graph by default; disable with `--no-union-graphs` if you want to keep them separate.
- All CLI subcommands support `--skip-invalid-rules`, `--warnings-are-errors`, `--no-imports`, and `--no-union-graphs`; the Graphviz/PDF helpers can run against shapes-only inputs while `validate`/`infer` can load the cached `--shacl-ir` artifact to avoid repeated parsing.
- `ARCHITECTURE.md` documents the validation pipeline end-to-end, and `AGENTS.md` captures the repository contribution guidelines.

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

- `validate`: run SHACL validation (optionally with rule inference) and optionally emit reports, DOT graphs, or traces.
- `infer`: emit the triples inferred by SHACL rules (Graphviz and PDF outputs are also supported).
- `visualize`: dump the DOT for the shapes (add `--pdf <FILE>` to render that DOT directly to PDF instead).
- `visualize-heatmap`: dump the execution heatmap DOT (add `--pdf <FILE>` to render the heatmap to PDF, optionally including non-executed nodes via `--all`).
- `heat`: validate the data and print a table of component/node/property invocation frequencies.
- `trace`: validate the data and dump every execution trace collected during validation.
- `generate-ir`: parse a shapes graph and write the `shacl-ir` artifact that other commands can reuse via `--shacl-ir path/to/cache`.

You can now request the visualization artifacts directly from `validate` or `infer` by appending:

- `--graphviz` to print the DOT description after execution
- `--pdf-heatmap heatmap.pdf [--pdf-heatmap-all]` to write the heatmap PDF (the `infer` command will trigger a validation pass when this flag is set)

All commands accept the shared `--skip-invalid-rules`, `--warnings-are-errors`, and `--no-imports` flags so you can skip problematic constructs, treat warnings as failures, or avoid resolving `owl:imports` when working in offline environments.
`validate`, `infer`, and other data-bearing commands additionally accept `--no-union-graphs` to keep shapes and data separate (the default is to union them so targets and rules can see shapes triples alongside data).

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

## SHACL-IR caching

The CLI ships with a `generate-ir` subcommand that parses the shapes graph, serializes the resulting `ShapeIR`, and writes it to disk. This makes repeated validations much faster because `validate`, `inference`, `heat`, and `trace` can all consume the `--shacl-ir cache.ttl` artifact instead of reparsing the shapes every time. The helper crate under `shacl-ir/` defines the serde-friendly IR data structures, so the cache can also be shared between the CLI and other embedders.

```bash
cargo run -p cli -- generate-ir --shapes-file examples/shapes.ttl --output-file /tmp/shape-cache.ttl
cargo run -p cli -- validate --shacl-ir /tmp/shape-cache.ttl --data-file examples/data.ttl --run-inference
```

When you reuse a cached IR, validation and inference still run over the union of the shapes graph (captured in the cache) and the supplied data graph unless you pass `--no-union-graphs`.

## Diagnostics & tracing

The CLI offers several commands to inspect validation behavior without rerunning validation from scratch:

- `heat` prints a tab-separated table of component/node/property invocation counts so you can find the hot spots that fired most frequently.
- `trace` dumps every execution trace recorded during validation, which prints the per-shape/component path that led to each failure.
`visualize-heatmap` reuses the same execution trace buffer to visualize how shapes were hit across the validator; add `--pdf <FILE>` to the command to render the heatmap to a PDF instead of printing the DOT.

Both `visualize` and `visualize-heatmap` expose a `--pdf` option so you can produce PDFs from the same DOT stream (the Graphviz output is still the default when `--pdf` is not provided). Every command still respects the shared `--skip-invalid-rules`, `--warnings-are-errors`, and `--no-imports` flags so you can treat warnings as failures or run without resolving `owl:imports`.

Both `validate` and `infer` can emit Graphviz (`--graphviz`) or PDF heatmaps (`--pdf-heatmap`) on demand.

## Python API

Install the extension module from PyPI as `pyshifty` (import it as `shifty`), or use `uvx maturin develop` (or `maturin develop --release`) inside `python/`. The module mirrors the CLI workflow:

- `generate_ir(shapes_graph, ...)` parses the shapes once and returns a `CompiledShapeGraph` Python object.
- `CompiledShapeGraph.validate` / `.infer` reuse the cached IR and accept the same flags as the CLI `validate`/`infer` commands.
- One-off helpers `shifty.validate` and `shifty.infer` still exist for quick runs when you don't need caching.

```python
import shifty

cache = shifty.generate_ir(
    shapes_graph,
    skip_invalid_rules=True,
    warnings_are_errors=False,
    do_imports=True,
)

conforms, report_graph, report_text, diag = cache.validate(
    data_graph,
    run_inference=True,
    inference={"min_iterations": 1, "max_iterations": 8},
    graphviz=True,
    heatmap=True,
    trace_events=True,
)
cached_inferred, cached_diag = cache.infer(
    data_graph,
    run_until_converged=True,
    graphviz=True,
    return_inference_outcome=True,
)
```

The standalone functions expose the same signatures:

```python
import shifty

# Requesting diagnostics returns a second item; otherwise you get just the graph.
inferred_graph, diag = shifty.infer(
    data_graph,
    shapes_graph,
    min_iterations=None,
    max_iterations=None,
    run_until_converged=None,
    no_converge=None,
    error_on_blank_nodes=None,
    enable_af=True,
    enable_rules=True,
    debug=None,
    skip_invalid_rules=False,
    warnings_are_errors=False,
    do_imports=True,
    graphviz=True,
    heatmap=False,
    heatmap_all=False,
    trace_events=False,
    trace_file=None,
    trace_jsonl=None,
    return_inference_outcome=True,
)

conforms, results_graph, report_text, diag = shifty.validate(
    data_graph,
    shapes_graph,
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
    skip_invalid_rules=False,
    warnings_are_errors=False,
    do_imports=True,
    graphviz=False,
    heatmap=False,
    heatmap_all=False,
    trace_events=False,
    trace_file=None,
    trace_jsonl=None,
    return_inference_outcome=False,
)
```

- `infer` still returns only the new triples unless you request diagnostics, in which case you get `(graph, diag)` where `diag` may contain `graphviz`, `heatmap`, `trace_events`, and/or `inference_outcome`.
- `validate` returns `(conforms, results_graph, report_turtle)` by default or a fourth diagnostics dict when any of the diagnostic flags are set.
- `inference` can be `True`/`False` or a dict that groups inference options (e.g. `{"min_iterations": 2, "debug": True}`) so you don't have to repeat CLI-style flags in Python. Explicit keyword arguments still work and continue to accept their `inference_*` aliases for backward compatibility.

Example:

```python
conforms, results_graph, report_text, diag = shifty.validate(
    data_graph,
    shapes_graph,
    inference={"min_iterations": 2, "max_iterations": 6, "debug": True},
    graphviz=True,
    heatmap=True,
    trace_events=True,
    return_inference_outcome=True,
)
print(diag["graphviz"])
print(diag["heatmap"])
print(diag["inference_outcome"]["triples_added"])
```

### Python example (adapted from `python/brick.py`)

```python
from rdflib import Graph
from ontoenv import OntoEnv
import shifty

env = OntoEnv()
model_iri = env.add("https://example.com/model.ttl")
data_graph = env.get_graph(model_iri)
shapes_graph, imports = env.get_closure(model_iri)

print(f"SHACL graph imports: {imports}")

inferred = shifty.infer(data_graph, shapes_graph, debug=True)
print(inferred.serialize(format="turtle"))

conforms, results_graph, report_text = shifty.validate(
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
shacl-ir/ # serde-backed ShapeIR crate for caching parsed shapes
scripts/  # helper scripts (Python tooling, benchmarks, etc.)
```

Need help? Open an issue or discussion in this repo with the failing SHACL shapes and data.

## Docs & guidelines

- `ARCHITECTURE.md` describes the full validation lifecycle, the component wiring, and the instrumentation pipeline.
- `AGENTS.md` captures the current repository guidelines, coding expectations, and testing commands for contributors.
