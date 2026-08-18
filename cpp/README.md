# Shifty C++ SDK

The C++ SDK embeds Shifty as a Rust static library. RDF parsing, SPARQL query
execution, SHACL-AF inference, and SHACL validation all use the same Rust RDF
implementation.

The public C++17 API is in `include/shifty/shifty.hpp`. It wraps the stable C
ABI in `include/shifty/shifty.h` with move-only RAII types and C++ exceptions.
Rust implementation types and allocators do not cross the ABI boundary.

## Build and test

```sh
cmake -S cpp -B build/cpp
cmake --build build/cpp
ctest --test-dir build/cpp --output-on-failure
```

The CMake build invokes Cargo and links the resulting `shifty_cpp` static
library into the C++ test executable.

## Use from C++

```cpp
#include <shifty/shifty.hpp>

#include <iostream>

shifty::Dataset dataset;
dataset.load_file("data.ttl");

auto rows = dataset.query(R"(
    SELECT ?s WHERE { ?s a <http://example.com/Person> }
)");

auto validator = shifty::PreparedValidator::from_file("shapes.ttl");
auto report = validator.validate(dataset);
if (!report.conforms()) {
    std::cerr << report.results_text();
}
```

### Multiple shapes / data graphs

Several RDF sources can be unioned (merged at the triple level) before they
reach the engine — the C++ analogue of the CLI's repeatable `--shapes` /
`--data`.

For **data**, call `Dataset::load` / `Dataset::load_file` repeatedly; triples
accumulate into one dataset:

```cpp
shifty::Dataset dataset;
dataset.load_file("data1.ttl");
dataset.load_file("data2.ttl");  // unioned with data1
```

For **shapes**, use `PreparedValidator::from_files` (multiple files) or
`PreparedValidator::from_memory` (multiple in-memory documents). Each source is
parsed in its own context (so per-document `@prefix`es resolve correctly) and
the resulting triples are merged into one shapes graph before planning:

```cpp
std::vector<std::filesystem::path> shape_files{"shapes1.ttl", "shapes2.ttl"};
auto validator = shifty::PreparedValidator::from_files(shape_files);

std::vector<std::string_view> shape_docs{doc_a, doc_b};
auto validator = shifty::PreparedValidator::from_memory(shape_docs);
```

`SELECT` results are returned as SPARQL Results JSON. `CONSTRUCT` and
`DESCRIBE` results are returned as N-Triples. `ASK` results provide both a
Boolean accessor and SPARQL Results JSON.

The initial API stores one RDF default graph and executes read-only SPARQL
queries over it. Named graphs, N-Quads, and SPARQL Update are not yet exposed.

### Severity threshold

Both `validate()` and `validate_algebra()` accept a `minimum_severity` on
`ValidationOptions` — the lowest result severity that makes `conforms()`
false. Findings below the threshold are still reported (they appear in the
W3C report graph / `AlgebraResult::violations()`); they just don't fail
validation. This mirrors the `minimum_severity` option of the Python / WASM /
CLI APIs.

```cpp
shifty::ValidationOptions opts;
opts.minimum_severity = shifty::Severity::Warning;  // Info no longer fails
if (!validator.validate(dataset, opts).conforms()) { /* … */ }
```

The three levels are `Severity::Info` (the default — any finding fails),
`Severity::Warning`, and `Severity::Violation` (only Violations fail).

### Scope to named shapes

Set `ValidationOptions::shape_names` to validate only selected named shapes as
top-level entry points. Dependencies referenced from those shapes are still
evaluated normally, so helper shapes reached through `sh:node`, `sh:property`,
qualified value shapes, and boolean shape expressions keep their usual
semantics.

```cpp
shifty::ValidationOptions opts;
opts.shape_names = {"http://example.org/PersonShape"};

auto report = validator.validate(dataset, opts);
auto algebra = validator.validate_algebra(dataset, opts);
```

Shape names may be bare IRIs or wrapped in angle brackets. An empty list (the
default) validates every target-bearing shape.

### Algebra-path validation

`validate()` returns a W3C `sh:ValidationReport`, serialized as Turtle.
`PreparedValidator::validate_algebra()` runs the same underlying conformance
oracle but returns the finding as a structured violation/reason tree instead
of an RDF report graph — useful when the caller wants to inspect results
programmatically rather than parse Turtle.

```cpp
auto algebra = validator.validate_algebra(dataset);
if (!algebra.conforms()) {
    for (const auto &violation : algebra.violations()) {
        std::cerr << violation.severity << " at " << violation.focus_node;
        if (!violation.shape_name.empty()) {
            std::cerr << " (" << violation.shape_name << ")";
        }
        std::cerr << "\n";
        for (const auto &reason : violation.reasons) {
            std::cerr << "  " << reason.message << "\n";
        }
    }
}
```

Each `AlgebraViolation` groups the reasons that failed at one focus node for
one shape; `shape_name` is empty for anonymous (blank-node) shapes.
`AlgebraResult::results_text()` gives a pre-formatted human-readable summary,
same as `ValidationResult::results_text()`.

### Property witnesses

`validate()` reports violations. `PreparedValidator::witnesses()` is its
inverse: for every focus node that *conforms* to a target/profile node shape,
it returns the values each `sh:property` shape's `sh:path` resolved to. When a
property shape uses `sh:qualifiedValueShape` (e.g. to disambiguate several
same-typed sensors), the witness is narrowed to the value(s) satisfying the
qualifier rather than every raw path value.

```cpp
shifty::ValidationOptions options;
options.key_path = "zea:roleName";

for (const auto &w : validator.witnesses(dataset, options)) {
    std::cout << w.focus_node << " " << w.key << " =";
    for (const auto &value : w.value_nodes) {
        std::cout << " " << value;
    }
    std::cout << "\n";
}
```

`key_path` is a SPARQL 1.1 property path expression (sequence `/`, alternation
`|`, inverse `^`, and the Kleene forms `*`/`+`/`?` are all supported)
evaluated from each `sh:property` shape's own node, over the shapes graph, to
produce a stable key. `zea:roleName` above is the direct-annotation case; if
the key instead lives one hop further away — say, through an intermediate
role-descriptor node — the same mechanism reaches it with e.g.
`"zea:role/zea:roleName"` or, if the descriptor points *at* the property shape
rather than the other way around, `"^zea:describes/zea:roleName"`. Prefixes
resolve against the shapes document's declared `@prefix`es. Property shapes
where the path resolves to no value fall back to their own IRI/blank-node id
as the key. `value_nodes` entries are rendered in full (`<iri>`, `_:label`,
`"lit"`, `"lit"@lang`, `"lit"^^<datatype>`) so IRI and literal bindings stay
distinguishable.

### Evidence-carrying validation

`validate()` reports what failed. `EvidenceSession` reports what was *decided*:
every authored statement, every focus node its selector chose, and exactly one
evidence polarity per pair — a satisfaction trace where the shape held, a
failure witness where it did not. Statements that selected nothing are reported
with an empty focus list, so a run is a coverage horizon over the schema rather
than a list of findings.

A session prepares one immutable snapshot. Inference, normalization,
stratification, indexing, and SPARQL preparation happen once in the constructor
and are reused by every call, so `graph_mode` and `run_inference` are read there
and ignored afterwards; `minimum_severity` and `shape_names` stay per-call.

```cpp
shifty::EvidenceSession evidence(validator, dataset);

for (const auto &statement : evidence.validate().statements()) {
    for (const auto &focus : statement.selected_foci) {
        std::cout << (focus.passed() ? "pass " : "fail ")
                  << focus.focus_node << " " << statement.target << "\n"
                  << focus.explanation << "\n";
    }
}
```

Each `FocusEvidence` carries the evidence tree as JSON (`evidence_json`) and a
human-readable rendering of the same (`explanation`). Constraint ids inside the
JSON resolve against `EvidenceSession::constraints_json()`, which holds the
source and normalized catalogs. That catalog is fixed per snapshot — on a small
model it is the majority of a run's serialized bytes — so take it once rather
than per run.

#### Scan, then explain

Materializing evidence for every pair is the expensive path. When failures are a
small fraction of selected pairs, scan for them and explain only those:
`find_failures()` decides each pair with one short-circuiting satisfaction test,
paying only a term clone per *failing* pair, and `explain()` materializes
evidence for one pair without re-running target selection.

```cpp
const auto failures = evidence.find_failures();
std::cout << failures.conformance().failed << " of "
          << failures.conformance().selected_pairs << " pairs failed\n";

for (std::size_t i = 0; i < failures.size(); ++i) {
    const auto run = evidence.explain(failures, i);
    std::cout << run.statements().front().selected_foci.front().explanation;
}
```

`validate_conformance()` is the same scan without retaining the pairs: a verdict
with counts, and the baseline that isolates what evidence tracing costs. Neither
honors `minimum_severity` — with no failure evidence there is no per-constraint
severity to weigh, so any failing pair makes the run non-conforming. Their
counts are over *normalized* pairs, before authored statements that normalize
together fan the same evidence back out.

A run from `explain()` carries an empty constraint catalog, since the catalog
belongs to the snapshot rather than the pair. It is otherwise shaped exactly
like one from `validate()`.

#### Compact encoding

`EvidenceRun::compact_json()` writes the run with its evidence nodes and RDF
terms hash-consed into shared tables and referenced by index; `expand_evidence()`
restores exactly what `json()` returned. Passing `include_catalog = false`
elides the constraint catalog for a consumer that already holds the schema,
which then supplies it when expanding:

```cpp
const auto wire = run.compact_json(/*include_catalog=*/false);
const auto restored =
    shifty::expand_evidence(wire, evidence.constraints_json());
```

Expanding a catalog-less encoding without supplying one throws, rather than
yielding a silently truncated run.

### Shape maps: typed key -> value bindings

One level above the evidence trees is the shape-map view: for every selected
`(shape, focus)` pair, a mapping of the shape's property obligations — bound
keys carry the values the data supplied as typed `Term`s (exact even on
partially-conforming foci), unbound keys carry the shortfall count and
near-misses. This is the C++ port of the Python `shifty.shape_map()`.

```cpp
shifty::ShapeMapOptions opts;
opts.name_path = "sh:name";            // author's name per slot, shapes graph
opts.value_paths = {{"ts", "demo:hasTimeseriesId"}};  // annotate each value

const shifty::EvidenceSession session(validator, dataset);
const auto smap = session.shape_map(session.validate(), opts);

for (const auto &name : smap.shape_names()) {
    for (const auto &mapping : smap.mappings(name)) {
        for (const auto &binding : mapping.successful()) {
            std::cout << binding->key().str() << ":";
            for (const auto &value : binding->values()) {
                std::cout << " " << value.n3();
            }
            std::cout << "\n";
        }
        for (const auto &binding : mapping.unsuccessful()) {
            std::cout << binding->key().str() << ": missing "
                      << binding->missing() << "\n";
        }
    }
}
```

Keys are typed (`Key` with a `Path` plus an optional `Qualifier` —
`QualifierKind::Cls`/`Const`/`Datatype`/`ShapeRef`), values are typed
`Term`s (`TermKind::Iri`/`Literal`/`BNode`), and bindings carry cardinality
(`min`/`max`/`observed`/`expects_single`) and `severity` read from the source
constraint so they are present even when evidence was not materialized.
`name_path` (default `sh:name`; set `ShapeMapOptions::name_path` empty to
skip) carries the author's name for each slot, evaluated from the property
shape's own node over the shapes graph; `value_paths` annotates each bound
*value* from the data graph, resolved in one batched call per label
(`Binding::annotated_values()` / `annotations()`).

A partially-conforming focus yields both sides: its failing keys report
`missing()`/`rejected_values()` and the `evidence_json()` witness subtree,
while its passing keys are materialized on demand (the raw failure witness
elides them) so a repair driver sees every value the focus can already
supply.

`Mapping` also offers `for_focus()` (via `ShapeMap`), `by_name()`, `find()`,
`value_map()` / `value_map_by_name()`, and `ShapeMap::to_json()` for a
plain-JSON summary. `Path::parse_json()` round-trips the `key_path_json`
encoding into a typed `Path` for pattern matching.

Three session helpers back the features directly, mirroring the Python
`EvidenceSession`:

- `binding_names(name_path)` — raw source constraint id -> reached names over
  the shapes graph (used for `name_path`);
- `shape_name_of(constraint_id)` — the named shape IRI a source constraint
  was lowered from;
- `resolve_path(nodes, path)` — batch-evaluate a SPARQL property path from
  N-Triples nodes over the session's evaluation graph (used for
  `value_paths`).

## Install

```sh
cmake --install build/cpp --prefix /desired/prefix
```

Consumers can then use:

```cmake
find_package(Shifty CONFIG REQUIRED)
target_link_libraries(my_target PRIVATE Shifty::shifty)
```

The static library contains Rust and all Rust crate dependencies. The generated
CMake package adds the required platform threading, dynamic-loader, and math
libraries.
