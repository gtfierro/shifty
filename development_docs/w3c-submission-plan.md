# W3C submission status

As of 2026-11-08, the pinned SHACL 1.0 Core/SPARQL suite passes all 120
approved tests and one separately reported proposed test. The
[submission draft](w3c-submission/README.md) is ready for review.

## Completed

1. Simplified public docs and preserved the original implementation text,
   diagrams, and manuscript in [implementation-notes/](implementation-notes/README.md).
2. Pinned unmodified upstream fixtures, recorded checksums, and audited the
   older local copies. See [testdata/README.md](../testdata/README.md).
3. Added a manifest runner with full report comparison, per-test diagnostics,
   EARL output, and a CI baseline. See [the runner guide](../docs/how-to/conformance.rst).
4. Ran the full suite and recorded the [gaps and fixes](w3c-conformance-gaps.md).
5. Fixed four defects and passed Python, Rust, documentation, C++, and Wasm checks.
6. Prepared the EARL report, proposed W3C listing changes, and PR description.

## Before submission

Review and commit the implementation changes, then rebuild and regenerate the
report from that revision. The current draft records uncommitted changes.
Follow the [submission instructions](w3c-submission/README.md).

The report covers SHACL 1.0 Core and SPARQL. SHACL-AF/DASH, SHACL-JS, and
SHACL 1.2 remain separate work.

## W3C format

[W3C's instructions](https://w3c.github.io/data-shapes/data-shapes-test-suite/)
request an EARL Turtle report, preferably as a PR to `w3c/data-shapes`, branch
`gh-pages`, under `data-shapes-test-suite/reports/`. Full report matches use
`earl:passed`; boolean-only matches use `sht:partial`. Failures can be disclosed.

The draft follows the submitted [TopBraid][topbraid], [dotNetRDF][dotnetrdf],
and [pySHACL][pyshacl] examples: software metadata, one assertion per test,
canonical `urn:x-shacl-test:/...` identifiers, and diagnostics where needed.
Those reports describe historical releases.

[topbraid]: https://github.com/w3c/data-shapes/blob/gh-pages/data-shapes-test-suite/reports/topbraid-shacl-earl.ttl
[dotnetrdf]: https://github.com/w3c/data-shapes/blob/gh-pages/data-shapes-test-suite/reports/dotnetrdf-shacl-earl.ttl
[pyshacl]: https://github.com/w3c/data-shapes/blob/gh-pages/data-shapes-test-suite/reports/pyshacl-earl.ttl
