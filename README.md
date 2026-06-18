# shifty

A formalism-first SHACL validation and SHACL-AF inference engine written in Rust, grounded in the algebraic treatment of *Common Foundations for SHACL, ShEx, and PG-Schema* (arXiv:2502.01295). Available as a command-line tool, Python bindings (`pyshifty`), and a C++17 static-library SDK.

See the `archive-first-attempt` branch for an older attempt, which was published as shifty 0.0.7

**Disclosure: This project was nearly 100% "vibe coded" with Claude Opus 4.8, Sonnet 4.6, and ChatGPT 5.5**

## Features

- **Full SHACL Core validation** — node and property shapes, all standard constraint components
- **SHACL-AF inference** — forward-chaining `sh:rule` evaluation (Triple Rules, SPARQL Construct Rules) to a fixed point, with stratification analysis for recursive rulesets
- **Algebraic IR** — shapes are lowered to a path algebra (π) and shape grammar (φ) before evaluation; the same IR drives both validation and inference
- **Native SPARQL execution** — a subset of `sh:sparql` constraints and SPARQL Construct rules runs directly over an indexed dataset without a full SPARQL engine, with automatic fallback to Spareval for unsupported constructs
- **Multi-layer pipeline** — parsing → algebraic lowering → normalization/CSE → physical planning → execution; each layer is independently inspectable
- **pyshifty-compatible Python API** — `validate()` returns `(conforms, report_graph, results_text)` matching pyshifty's interface

## Installation

### CLI

```sh
cargo install --path crates/shifty-cli
```

Or build from source:

```sh
cargo build --release -p shifty-cli
# binary at target/release/shifty
```

### Python

```sh
pip install pyshifty
```

The package installs as `pyshifty` but is imported as `shifty`:

```python
import shifty
```

To build from source (requires Rust and [maturin](https://github.com/PyO3/maturin)):

```sh
cd python
pip install maturin
maturin develop
```

### C++

The C++17 SDK embeds Shifty as a Rust static library and provides RAII wrappers
for RDF loading, SPARQL queries, and reusable SHACL validators:

```sh
cmake -S cpp -B build/cpp
cmake --build build/cpp
ctest --test-dir build/cpp --output-on-failure
```

```cpp
#include <shifty/shifty.hpp>

shifty::Dataset dataset;
dataset.load_file("data.ttl");

auto rows = dataset.query("SELECT ?s WHERE { ?s ?p ?o }");
auto validator = shifty::PreparedValidator::from_file("shapes.ttl");
auto report = validator.validate(dataset);
```

See [`cpp/README.md`](cpp/README.md) for installation and CMake package usage.

## CLI usage

### Validate

```sh
shifty validate --shapes shapes.ttl --data data.ttl
```

```
conforms: false
violations: 1
  <http://example.org/bob>  [target: ∃ rdf:type .⊤]
      - (ex:name) 123 → expected datatype xsd:string
```

Emit a W3C `sh:ValidationReport` in Turtle:

```sh
shifty validate --shapes shapes.ttl --data data.ttl --report
```

JSON output:

```sh
shifty validate --shapes shapes.ttl --data data.ttl --format json
```

Validation runs SHACL-AF rules to a fixed point by default. Skip rule inference
when validating shapes directly:

```sh
shifty validate --shapes shapes.ttl --no-infer
```

Graph mode controls which triples are visible to path traversal and SPARQL evaluation:

```sh
# default: focus nodes from data; paths/SPARQL use data ∪ shapes
shifty validate --shapes shapes.ttl --data data.ttl --graph-mode union

# focus nodes and evaluation use data only
shifty validate --shapes shapes.ttl --data data.ttl --graph-mode data

# focus nodes and evaluation both use data ∪ shapes
shifty validate --shapes shapes.ttl --data data.ttl --graph-mode union-all
```

### Infer

Run SHACL-AF rules to a fixed point, then print the derived triples:

```sh
shifty infer --shapes rules.ttl --data data.ttl
```

```
inferred 3 triple(s):
  <http://example.org/r1> <http://example.org/area> "6"^^<http://www.w3.org/2001/XMLSchema#integer>
  ...
```

### Inspect

Inspect how a shapes graph looks at each stage of the pipeline:

```sh
# Raw triples after parsing
shifty inspect --stage rdf shapes.ttl

# Lowered algebraic IR (φ/π notation)
shifty inspect --stage algebra shapes.ttl

# After normalization and common-subexpression elimination
shifty inspect --stage normalized shapes.ttl

# Stratification analysis (recursion detection)
shifty inspect --stage strata shapes.ttl

# Physical plan: focus sources + cost-ordered shape checks
shifty inspect --stage plan shapes.ttl

# SPARQL constraint capability: which queries run native vs. Spareval
shifty inspect --stage capability shapes.ttl
```

All stages support `--format text` (default), `--format json`; the `algebra` and `normalized` stages also support `--format dot` for Graphviz output.

Shapes files and data files may be local paths or HTTP/HTTPS URLs. Both `--shapes` and `--data` are repeatable to merge multiple files.

## Python usage

```python
import shifty
```

### Validate (pyshifty-compatible)

```python
shapes = """
@prefix sh:  <http://www.w3.org/ns/shifty#> .
@prefix ex:  <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [
        sh:path ex:name ;
        sh:minCount 1 ;
        sh:datatype xsd:string ;
    ] ;
    sh:property [
        sh:path ex:age ;
        sh:maxCount 1 ;
        sh:datatype xsd:integer ;
    ] .
"""

data = """
@prefix ex: <http://example.org/> .

ex:Alice a ex:Person ; ex:name "Alice" ; ex:age 30 .
ex:Bob   a ex:Person .
"""

conforms, report_graph, results_text = shifty.validate(data, shapes)
# conforms → False
# report_graph → rdflib.Graph with sh:ValidationReport
# results_text → human-readable summary
```

Graph inputs can be a string, `bytes`, `pathlib.Path`, or `rdflib.Graph`. If `shacl_graph` is omitted or passed as `None`, shapes are expected to be embedded in the data graph. Do not pass an empty `rdflib.Graph()` for embedded shapes; that is treated as an explicit empty shapes graph.

To validate a shapes graph against itself, pass it once. The embedded path
parses and plans one graph without constructing separate data and shapes
graphs:

```python
result = shifty.validate_algebra("shapes.ttl", infer=False)
conforms, report_graph, results_text = shifty.validate("shapes.ttl", infer=False)
```

For repeated validation, prepare the shapes graph once:

```python
validator = shifty.PreparedValidator(shapes)
result = validator.validate_algebra(data, infer=False)
conforms, report_graph, results_text = validator.validate(data)
```

`pathlib.Path` inputs are parsed directly by Rust. `rdflib.Graph` inputs use
N-Triples for the Python-to-Rust transfer to avoid rdflib's slower Turtle
serializer.

### Validate with structured result

`validate_algebra` returns an `AlgebraResult` with typed `Violation` objects instead of an RDF report graph:

```python
result = shifty.validate_algebra(data, shapes)
print(result.conforms)        # False
for v in result.violations:
    print(v.focus)            # IRI of the failing focus node
    for r in v.reasons:
        print(r.message)      # human-readable failure description
        print(r.path)         # path that was checked, if applicable
        print(r.value)        # the offending value node
```

Set `infer=False` when validation should not first run embedded SHACL-AF rules
to a fixed point.

### Infer

Run SHACL-AF rules to a fixed point:

```python
rules = """
@prefix sh: <http://www.w3.org/ns/shifty#> .
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

data = """
@prefix ex: <http://example.org/> .
ex:r1 a ex:Rectangle ; ex:width 3 ; ex:height 2 .
"""

result = shifty.infer(data, rules)
print(result.inferred_count)    # number of newly derived triples
g = result.graph()              # rdflib.Graph with original + inferred data
```

If rules are embedded in the data graph, omit the second argument or pass
`None`:

```python
result = shifty.infer(combined_data_and_rules)
result = shifty.infer(combined_data_and_rules, None)
```

Passing `rdflib.Graph()` as the second argument means “run with an explicit
empty rules graph,” so no embedded rules will be parsed.

### Repair (drive your own loop)

**Warning: this is very experimental and prelimary; not fully tested or documented yet. API subject to change.**

`RepairSession` exposes the symbolic-repair primitives so you can build your own
repair driver: enumerate the violation horizon by focus node, inspect each
violation's repair tree (holes + decision points), enumerate candidate bindings,
fold your own choices into a delta, gate it, and apply it. The library computes
and gates; **every choice is yours** — no repair loop is made for you.

```python
session = shifty.RepairSession(shapes, data)   # SHACL-AF inference runs first

while True:
    witnesses = session.witnesses()            # the horizon, by focus node
    if not witnesses:
        break                                  # conforms
    fw = witnesses[0]                          # your focus-ordering policy
    print(fw.target)                           # what targeted this node
    for atom in fw.summary():                  # flat failing leaves
        print(atom.kind, atom.detail)
    # fw.explain()                             # the full witness tree, as text

    tree = fw.repair_tree()                    # the repair space
    plan = shifty.RepairPlan()
    for choice in tree.choices():              # Any branches / Repeat counts
        if choice.kind == "repeat":
            plan.count(choice.node_id, choice.min)

    for hole in tree.instantiate(plan).open_holes:
        plan.bind(hole.id, hole.candidates(limit=8)[0])   # your binding policy

    inst = tree.instantiate(plan)              # fold choices → ΔG
    outcome = session.gate(inst.delta)         # sound? progress? introduces what?
    if outcome.is_progress:
        session = session.advance(inst.delta)  # accept, re-witness from G ⊕ ΔG
    else:
        break                                  # reject; choose differently

repaired = session.to_graph()                  # rdflib.Graph with accepted repairs
```

`Hole.candidates()` returns reuse-first options drawn from the data graph, in
N-Triples term syntax; bind one straight back with `plan.bind(hole.id, value)`.
A `RepairTree` may be `is_blocked` (opaque SPARQL / identity / coinductive),
meaning no data repair is possible in scope. Two reference drivers ship as
examples: `python/examples/repair.py` (non-interactive: takes the first option
for each violation) and `python/examples/repair_interactive.py` (prints a
numbered menu of gated options per violation and applies the one you pick).

### graph_mode

`validate()` and `validate_algebra()` accept a `graph_mode` keyword argument:

```python
shifty.validate(data, shapes, graph_mode="union")      # default
shifty.validate(data, shapes, graph_mode="data")
shifty.validate(data, shapes, graph_mode="union-all")
```

When `shacl_graph` is omitted, all three modes are equivalent because focus
discovery and evaluation use the same embedded graph. `infer()` does not accept
`graph_mode`.

### File inputs

```python
import pathlib

conforms, report, text = shifty.validate(
    pathlib.Path("data.ttl"),
    pathlib.Path("shapes.ttl"),
)
```

## Crate structure

| crate | role |
|---|---|
| `shifty-algebra` | path algebra π, shape grammar φ, schema arena, rendering |
| `shifty-parse` | Turtle/RDF → algebraic IR lowering |
| `shifty-opt` | normalization, stratification, physical planning, native SPARQL lowering |
| `shifty-engine` | validation + AF inference execution, SPARQL executor |
| `shifty-cli` | `shifty` binary |
| `pyshifty` (python/) | PyO3 bindings, published as `pyshifty` on PyPI |

## Development

### 3-way implementation comparison

`scripts/compare_implementations.py` runs shifty, [TopQuadrant SHACL](https://github.com/topquadrant/shacl), and [pySHACL](https://github.com/RDFLib/pySHACL) against the same shapes + data graphs and produces a 3-way diff of their `sh:ValidationResult` sets.

```sh
uv run scripts/compare_implementations.py \
    -s benchmark/brick/Brick-closure.ttl \
    -d benchmark/brick/models/bldg1.ttl
```

Requires shifty to be importable from the local build:

```sh
cd python && maturin develop
```

TopQuadrant requires the `brick-tq-shacl` Python package (installed automatically by `uv run`). To skip it:

```sh
uv run scripts/compare_implementations.py \
    -s shapes.ttl -d data.ttl \
    --skip topquadrant
```

**How results are compared:** each `sh:ValidationResult` is content-hashed by its full predicate/object subtree (blank nodes are recursively canonicalized by content, not identity). `sh:resultMessage` and `sh:sourceConstraint` are excluded from the hash — message phrasing varies across implementations, and `sh:sourceConstraint` carries implementation-specific blank-node bookkeeping. Results are then grouped into buckets: all-agree, only-in-X, only-in-X+Y, etc.

Flags:
- `--exact-message` — include `sh:resultMessage` in the hash
- `--max-show N` — show N signatures per bucket (default: 5)
- `--verbose` — print full canonical JSON signature per result instead of one-line summaries

## License

BSD-3-Clause
