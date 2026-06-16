# shifty

A formalism-first SHACL validation and SHACL-AF inference engine written in Rust, grounded in the algebraic treatment of *Common Foundations for SHACL, ShEx, and PG-Schema* (arXiv:2502.01295). Available as a command-line tool and as Python bindings (`pyshifty`).

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

Graph inputs can be a string, `bytes`, `pathlib.Path`, or `rdflib.Graph`. If `shacl_graph` is omitted, shapes are expected to be embedded in the data graph.

To validate a shapes graph against itself, pass it once. The embedded path
parses and plans one graph without constructing separate data and shapes
graphs:

```python
result = shifty.validate_algebra("shapes.ttl", infer=False)
conforms, report_graph, results_text = shifty.validate("shapes.ttl", infer=False)
```

`pathlib.Path` inputs are parsed directly in Rust. `rdflib.Graph` inputs are
transferred as N-Triples.

### Reuse prepared shapes

For multiple data graphs using the same shapes, cache parsing, normalization,
and planning with `PreparedValidator`:

```python
validator = shifty.PreparedValidator(shapes)

result = validator.validate_algebra(data, infer=False)
conforms, report_graph, results_text = validator.validate(data)
```

### Validate with structured result

`validate_algebra` returns an `AlgebraResult` with typed `Violation` objects instead of an RDF report graph:

```python
result = shifty.validate_algebra(data, shapes)
print(result.conforms)        # False
print(result.results_text)    # human-readable summary (built and cached on first access)
for v in result.violations:
    print(v.focus_node)       # IRI of the failing focus node
    print(v.shape_name)       # IRI of the violated shape, or None
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

## Witnesses (symbolic repair)

`RepairSession` exposes the *witnessing* layer: for each statement it reports why
a focus node fails (a `FocusWitness`) or why it holds (a `FocusSatisfaction`),
the structured input to repair synthesis. The session is immutable; it computes
and gates but decides nothing.

```python
shapes = """
@prefix sh:  <http://www.w3.org/ns/shacl#> .
@prefix ex:  <http://example.org/> .

ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:name ; sh:minCount 1 ] .
"""
data = """
@prefix ex: <http://example.org/> .
ex:carol a ex:Person ; ex:name "Carol" .   # passes ex:PersonShape
ex:dan   a ex:Person .                      # fails: no ex:name
"""

session = shifty.RepairSession(shapes, data, infer=False)
```

### The whole horizon

`witnesses()` returns one `FocusWitness` per `(focus node, failed statement)`
across the entire schema. Empty ⟺ the graph conforms.

```python
for w in session.witnesses():
    print(w.focus)        # '<http://example.org/dan>'
    print(w.statement)    # 0 — index into the schema's statements
    print(w.target)       # 'class(<http://example.org/Person>)' — rendered selector
```

### Structured access (strings *and* objects)

Everything that has a readable string also has a structured, inspectable form, so
you can branch and process externally instead of parsing text. `w.target` is the
rendered selector; `w.selector` is the same thing decomposed:

```python
sel = w.selector
print(sel.kind)      # TargetKind.Class — an enumerated discriminant
print(sel.value)     # '<http://example.org/Person>' — N-Triples, round-trips
print(sel.render)    # 'class(<http://example.org/Person>)' == w.target
print(str(sel))      # same rendered string

if sel.kind == shifty.TargetKind.Class:
    ...              # dispatch on the kind, not on a substring
```

`kind` fields are real enums, not bare strings — so the valid set is discoverable
at runtime and usable in `match`/comparisons:

```python
shifty.TargetKind   # Class | SubjectsOf | ObjectsOf | Node | Path | Sparql
shifty.WitnessKind  # Atom | Relational | Closed | CountLow | CountHigh | Not | Opaque
shifty.SatKind      # Atom | Match | Not | Blocked | Coinductive
shifty.ChoiceKind   # Any | Repeat
```

### Scope to one shape

`witnesses_for(shape_iri)` narrows the horizon to the statements that target a
single shape, matched against the schema's shape IRIs (angle brackets optional).
It raises `ValueError` if no shape is named `shape_iri`.

```python
for w in session.witnesses_for("http://example.org/PersonShape"):
    # flat bag of failing leaves (AND/OR structure dropped)
    for a in w.summary():       # a is a WitnessAtom
        print(a.kind, a.path, a.detail)   # WitnessKind.CountLow <…/name> have 0, need 1
        if a.kind == shifty.WitnessKind.CountLow:
            ...

    print(w.explain())          # indented witness tree:
                                # CountLow along <…/name>: have 0, need 1

    tree = w.repair_tree()      # synthesize the repair space for this violation
    print(tree.is_blocked)      # False — a data repair exists in scope
```

### Passing nodes and the values that satisfied them

`satisfactions_for(shape_iri)` is the dual: one `FocusSatisfaction` per *passing*
focus node for that shape. Each records why the node conforms, including the
values matched along every checked path — the satisfaction-side mirror of
`witnesses_for`.

```python
for fs in session.satisfactions_for("http://example.org/PersonShape"):
    print(fs.focus)             # '<http://example.org/carol>'
    print(fs.statement)         # 0
    print(fs.target)            # same rendered selector as the witness side
    print(fs.selector.kind)     # TargetKind.Class — same structured selector too

    for a in fs.summary():      # a is a SatAtom
        # one Match leaf per value that satisfied a checked path
        if a.kind == shifty.SatKind.Match:
            print(a.path, a.value)        # <…/name> "Carol"

    print(fs.explain())         # CountHeld: 1 match(es)
```

`witnesses_for` and `satisfactions_for` partition the targeted focus nodes:
every node that fails appears in one, every node that holds in the other. For
`closed`, relational (`sh:equals`/`sh:lessThan`/…), and opaque-SPARQL
constraints a satisfaction leaf is reported as `SatKind.Blocked` — the node
holds, but no enumerable value set is exposed.

## Crate structure

| crate | role |
|---|---|
| `shifty-algebra` | path algebra π, shape grammar φ, schema arena, rendering |
| `shifty-parse` | Turtle/RDF → algebraic IR lowering |
| `shifty-opt` | normalization, stratification, physical planning, native SPARQL lowering |
| `shifty-engine` | validation + AF inference execution, SPARQL executor |
| `shifty-cli` | `shifty` binary |
| `pyshifty` (python/) | PyO3 bindings, published as `pyshifty` on PyPI |

## Design docs

The `docs/` directory contains the full design:

- [`docs/00-formalism.md`](docs/00-formalism.md) — path algebra π, shape grammar φ, selectors, reference semantics
- [`docs/01-gap-analysis.md`](docs/01-gap-analysis.md) — W3C SHACL/SHACL-AF coverage and known gaps
- [`docs/02-roadmap.md`](docs/02-roadmap.md) — layered build plan (Layer 0 → 7)

## License

BSD-3-Clause
