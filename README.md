# Shifty

Shifty validates RDF graphs against SHACL shapes and runs SHACL-AF rules to a
fixed point. It compiles shapes into a path and shape algebra before evaluating
data. The engine is available through a command-line tool, Python package,
C++17 SDK, Rust crates, and WebAssembly module.

[Documentation](https://shifty.gtf.fyi/) · [Feature support](https://shifty.gtf.fyi/reference/feature-support.html) · [Changelog](CHANGELOG.md)

## Quick start

Install the Python package and its optional `rdflib` dependency when you want a
W3C `sh:ValidationReport` graph:

```sh
pip install "pyshifty[rdflib]"
```

```python
import shifty

shapes = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .

ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"""
data = """
@prefix ex: <http://example.org/> .
ex:alice a ex:Person ; ex:name "Alice" .
ex:bob a ex:Person .
"""

result = shifty.validate_algebra(data, shapes)
assert not result.conforms
assert len(result.violations) == 1

conforms, report_graph, results_text = shifty.validate(data, shapes)
assert not conforms
```

`validate_algebra()` returns structured violations. `validate()` returns the
W3C report as an `rdflib.Graph`. The [Python reference](https://shifty.gtf.fyi/reference/python.html)
lists their options and the other graph input forms.

For the CLI, clone this repository and install the binary with Rust:

```sh
cargo install --path crates/shifty-cli
shifty validate --shapes shapes.ttl --data data.ttl
shifty infer --shapes rules.ttl --data data.ttl
```

The CLI's default validation output groups failures by finding and lists the
affected focus nodes. `--report` emits a W3C report in Turtle. See the
[CLI reference](https://shifty.gtf.fyi/reference/cli.html) for flags and output
formats.

## Interfaces

| Interface | Use |
| --- | --- |
| [Python](python/README.md) | Validation, inference, evidence, shape maps, and prepared schemas. |
| [CLI](https://shifty.gtf.fyi/reference/cli.html) | Validation and inference from files or URLs. |
| [C++17](cpp/README.md) | Static library with prepared validation and typed results. |
| [Rust](https://docs.rs/shifty-engine) | Engine crates and reusable sessions. |
| [WebAssembly](crates/shifty-wasm/README.md) | Local browser validation and inference; try the [playground](https://shifty.gtf.fyi/playground/). |

SHACL Core and much of SHACL-AF are supported. JavaScript constraints and
functions are unsupported; some calls to SHACL functions from SPARQL have
limited graph access. Check the [feature matrix](https://shifty.gtf.fyi/reference/feature-support.html)
before relying on an advanced feature. Symbolic repair is experimental.

## Development

The [contribution guide](https://shifty.gtf.fyi/contributing.html) has build and
quality checks. Python development uses the locked uv environment in `python/`:

```sh
cd python
uv sync --dev --frozen --reinstall-package pyshifty
uv run ruff check .
uv run ruff format --check .
uv run ty check shifty
uv run pytest -q
```

Reinstall the editable package after changing Rust sources so tests use the
newly compiled extension. For performance measurements and implementation
comparisons, see [benchmark/README.md](benchmark/README.md) and
[`scripts/compare_implementations.py`](scripts/compare_implementations.py).

The `archive-first-attempt` branch preserves the older implementation published
as Shifty 0.0.7. The current project was developed predominantly with coding
assistants, including Claude Opus 4.8, Sonnet 4.6, and ChatGPT 5.5.

## License

BSD-3-Clause.
