# SHACL 1.0 results for fea00b5

Generated from clean Shifty commit `fea00b5a66c510dc14cc658d5aa5f2baed627ace` on 2026-11-08.
The W3C suite revision is `40231dfb97c5b606d22ae733947c0d14b4f45c07`.

- 120 approved cases and one proposed case passed.
- Native CLI and Python file inputs passed all 121 checks: 114 matching reports
  and seven expected validation failures.
- Both interfaces used the data graph, disabled inference, and rejected
  unsupported features. Process-local blank-node labels were compared by RDF
  graph isomorphism.

| File | Contents |
| --- | --- |
| [shifty-earl.ttl](shifty-earl.ttl) | EARL implementation report |
| [results.json](results.json) | Manifest-run results and source/build metadata |
| [parity.json](parity.json) | Native file-input results and binary hashes |

Verify the files with `sha256sum -c SHA256SUMS` in this directory.

To reproduce, check out the commit above and run:

```sh
cargo build --locked -p shifty-cli
cd python
uv sync --dev --frozen --reinstall-package pyshifty
uv run python -m conformance --include-unlisted \
  --baseline ../testdata/w3c-shacl/baseline.json
uv run python -m conformance.parity --cli ../target/debug/shifty
```

The clean checkout also passed 476 Rust tests and 1072
Python tests. Timestamps, blank-node labels, and binary hashes can change on
reruns. This snapshot covers SHACL 1.0 Core/SPARQL; it makes no SHACL-AF,
SHACL-JS, or SHACL 1.2 claim. Nothing has been submitted online.
