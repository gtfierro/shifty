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
