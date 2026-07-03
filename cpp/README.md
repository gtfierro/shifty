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

`SELECT` results are returned as SPARQL Results JSON. `CONSTRUCT` and
`DESCRIBE` results are returned as N-Triples. `ASK` results provide both a
Boolean accessor and SPARQL Results JSON.

The initial API stores one RDF default graph and executes read-only SPARQL
queries over it. Named graphs, N-Quads, and SPARQL Update are not yet exposed.

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
