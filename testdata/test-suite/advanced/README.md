# Local SHACL-AF/DASH regression fixtures

This directory is populated. Run its Rust harness from the repository root:

```sh
cargo test -p shifty-engine --test w3c_advanced -- --nocapture
```

The harness runs inference, validation, and function cases; no environment
toggle is required. `cases.txt` fixes the vendored inventory; category counts
are 7 inference, 97 validation, and 2 function cases. Missing files, unexpected
diagnostics, execution failures, missing expectations, and comparison failures
fail the test rather than becoming skips. SHACL-JS is outside the profile and
requires a reviewed explicit exclusion if JS fixtures are added.

Inference comparisons cover the entire derived delta, including unexpected
triples. Fresh blank-node labels are compared by graph isomorphism while
preserving existing input-node identities. Report checks include severity,
authored expected messages, and source constraints when specified, in addition
to focus, value, path, component, and source shape. Annotation values and
cardinality are compared, including unexpected annotations.

The harness also runs project-owned cases in `testdata/regressions/af/`, covering
function expressions, property value scope, empty and mixed results, optional
arguments, ASK results, path inputs, fresh CONSTRUCT blank nodes, hierarchical
decimal ordering, nested graph-reading functions, reusable target types, and
result annotations. These
cases are kept separate from the vendored inventory.

It resolves imported helper ontologies locally. Metadata-only DASH/schema
imports and declarations already inlined by a fixture are permitted; unresolved
data imports fail. Some
document IRIs have been rewritten, so these fixtures are retained as local
regressions rather than included in the SHACL 1.0 implementation report.

The pinned Core/SPARQL fixtures and their supported manifest runner are
documented in [testdata/README.md](../../README.md).
