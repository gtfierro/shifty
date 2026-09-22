# pyshifty

`pyshifty` is the Python package for [Shifty](https://github.com/gtfierro/shifty),
a SHACL validation and SHACL-AF inference engine for RDF graphs. Install the
distribution as `pyshifty` and import it as `shifty`.

[Documentation](https://shifty.gtf.fyi/) · [Python API](https://shifty.gtf.fyi/reference/python.html) · [Feature support](https://shifty.gtf.fyi/reference/feature-support.html)

## Install

```sh
pip install pyshifty
```

The core validation, inference, and evidence paths do not require `rdflib`.
Install the extra when using `validate()` or an API that returns an
`rdflib.Graph`:

```sh
pip install "pyshifty[rdflib]"
```

Python 3.9 or newer is supported. Published wheels include the Rust engine.

## Validate

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
for violation in result.violations:
    for reason in violation.reasons:
        print(violation.focus_node, reason.constraint_kind, reason.path)

# With the rdflib extra installed:
conforms, report_graph, results_text = shifty.validate(data, shapes)
```

The first call returns typed violations and reasons. The second returns a
W3C `sh:ValidationReport` graph and text. Both take data first and shapes
second. See [validation interfaces](https://shifty.gtf.fyi/explanation/validation-interfaces.html)
for the reporting tradeoffs and the [API reference](https://shifty.gtf.fyi/reference/python.html)
for accepted arguments.

Inputs may be Turtle text or bytes, local paths, HTTP(S) URLs, `pathlib.Path`,
or `rdflib.Graph`. Lists and tuples merge sources before evaluation. Omit the
shapes argument when shapes and data are in one graph.

## Infer

```python
import shifty

rules = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .

ex:RectangleShape a sh:NodeShape ;
    sh:targetClass ex:Rectangle ;
    sh:rule [
        a sh:TripleRule ;
        sh:subject sh:this ;
        sh:predicate ex:area ;
        sh:object [ sh:path ex:width ] ;
    ] .
"""
rectangles = """
@prefix ex: <http://example.org/> .
ex:r1 a ex:Rectangle ; ex:width 3 .
"""

inferred = shifty.infer(rectangles, rules)
assert inferred.inferred_count == 1
print(inferred.inferred_ntriples)
```

`inferred.graph_ntriples` contains the input and derived triples.
`inferred.graph()` returns that graph as `rdflib.Graph` when the extra is
installed. See the [inference guide](https://shifty.gtf.fyi/how-to/infer.html)
for in-place updates and inference during validation.

## Reuse shapes and inspect results

`PreparedValidator(shapes)` compiles a schema once for use with many data
graphs. `EvidenceSession` records passing and failing focus nodes and their
derivations. `shape_map()` extracts typed property bindings, including partial
bindings for a focus node that fails another constraint. Symbolic repair is
experimental. Start with the [tutorials](https://shifty.gtf.fyi/tutorials/index.html)
or look up these interfaces in the [reference](https://shifty.gtf.fyi/reference/index.html).

## Develop from a checkout

From `python/`, use the locked uv environment:

```sh
uv sync --dev --frozen --reinstall-package pyshifty
uv run ruff check .
uv run ruff format --check .
uv run ty check shifty
uv run pytest -q
```

Reinstall after changing Rust sources so the editable extension is rebuilt.
The [contribution guide](https://shifty.gtf.fyi/contributing.html) has the other
frontend build instructions.

## License

BSD-3-Clause.
