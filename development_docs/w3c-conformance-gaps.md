# SHACL 1.0 results and remaining work

The 2026-11-08 working-tree run passed all 120 approved tests and one proposed
test, with no partial results or runner errors. Algebraic conformance agreed
for every case expecting a report. The suite is pinned to W3C data-shapes
commit `40231dfb97c5b606d22ae733947c0d14b4f45c07`.

Reproduce from `python/`:

```sh
uv sync --dev --frozen --reinstall-package pyshifty
uv run python -m conformance --include-unlisted \
  --baseline ../testdata/w3c-shacl/baseline.json
```

For one case, use `--test urn:x-shacl-test:/<path>` with a path below.
See the [runner guide](../docs/how-to/conformance.rst) for output and exit codes.

## Fixed

The first strict run found 27 affected tests: 26 matched only the conformance
boolean, and one returned the wrong boolean. Four fixes resolved them:

| ID | Defect | Fix |
| --- | --- | --- |
| G1 | Complex result paths had no RDF structure. | Include the path triples, describing only the path actually evaluated. |
| G2 | Python report decoding changed literal values. | Preserve lexical forms, including invalid typed literals. |
| G3 | Report and algebra validation disagreed on `uniqueLang-002`. | Activate this constraint only for the exact `true` RDF term. |
| G4 | Direct SPARQL results omitted `sh:sourceConstraint`. | Include the authored constraint in each result. |

References: [property paths](https://www.w3.org/TR/shacl/#property-paths),
[validation results](https://www.w3.org/TR/shacl/#results-validation-result),
[uniqueLang](https://www.w3.org/TR/shacl/#UniqueLangConstraintComponent), and
[W3C's comparison rules](https://w3c.github.io/data-shapes/data-shapes-test-suite/).

All cases below are approved and now pass full report comparison.

| Test path after urn:x-shacl-test:/ | Initial outcome | Root cause | Final outcome |
| --- | --- | --- | --- |
| `core/path/path-alternative-001` | partial | G1 | passed |
| `core/path/path-complex-001` | partial | G1 | passed |
| `core/path/path-complex-002` | partial | G1 | passed |
| `core/path/path-inverse-001` | partial | G1 | passed |
| `core/path/path-oneOrMore-001` | partial | G1 | passed |
| `core/path/path-sequence-001` | partial | G1 | passed |
| `core/path/path-sequence-002` | partial | G1 | passed |
| `core/path/path-sequence-duplicate-001` | partial | G1 | passed |
| `core/path/path-strange-001` | partial | G1 | passed |
| `core/path/path-strange-002` | partial | G1 | passed |
| `core/path/path-zeroOrMore-001` | partial | G1 | passed |
| `core/path/path-zeroOrOne-001` | partial | G1 | passed |
| `core/property/datatype-001` | partial | G2 | passed |
| `core/property/or-datatypes-001` | partial | G2 | passed |
| `core/property/uniqueLang-002` | failed | G3 | passed |
| `sparql/node/prefixes-001` | partial | G4 | passed |
| `sparql/node/sparql-001` | partial | G4 | passed |
| `sparql/node/sparql-002` | partial | G4 | passed |
| `sparql/node/sparql-003` | partial | G4 | passed |
| `sparql/pre-binding/pre-binding-001` | partial | G4 | passed |
| `sparql/pre-binding/pre-binding-002` | partial | G4 | passed |
| `sparql/pre-binding/pre-binding-003` | partial | G4 | passed |
| `sparql/pre-binding/pre-binding-004` | partial | G4 | passed |
| `sparql/pre-binding/pre-binding-005` | partial | G4 | passed |
| `sparql/pre-binding/pre-binding-007` | partial | G4 | passed |
| `sparql/pre-binding/shapesGraph-001` | partial | G4 | passed |
| `sparql/property/sparql-001` | partial | G4 | passed |

## Test counts

The root manifest includes 98 Core and 22 SPARQL tests. Seven expect a
validation failure. The proposed `sparql/component/nodeValidator-001` test is
outside the root manifest and runs with `--include-unlisted`.

Older harnesses used adapted fixtures, counted helper files, and checked only
selected report fields. They remain regression tests. The new runner follows
manifest entries and checks the full reports; CI detects missing tests and
regressions. See [fixture sources](../testdata/README.md).

## Remaining work

This suite demonstrates no remaining missing SHACL 1.0 feature. It does not
prove complete specification conformance. Separate work remains:

- Regenerate the submission report from reviewed, committed code. The current
  [draft](w3c-submission/README.md) records uncommitted changes.
- Support data-graph access for SHACL functions called from SPARQL; currently
  partial. Add dedicated AF tests for that calling context.
- Implement and test SHACL-JS if JavaScript support is wanted.
- Assess the separate SHACL 1.2 suite before making a 1.2 support claim.
