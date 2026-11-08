# SHACL 1.0 results for 54e2a40

Generated from a clean checkout of Shifty commit
`54e2a40bf851336553d89d99f10851556f197e0d` on 2026-11-08.

- 120 approved Core/SPARQL cases passed.
- One proposed node-validator case passed and is reported separately.
- The EARL and JSON files agree on all 121 test IDs and outcomes.
- The suite revision is `40231dfb97c5b606d22ae733947c0d14b4f45c07`.

[shifty-earl.ttl](shifty-earl.ttl) contains the EARL report;
[results.json](results.json) includes settings, diagnostics, source and binary
hashes. `SHA256SUMS` records the hashes of these preserved files. Verify them
with `sha256sum -c SHA256SUMS` from this directory.

To reproduce, check out the Shifty revision above and run from `python/`:

```sh
uv sync --dev --frozen --reinstall-package pyshifty
uv run python -m conformance --include-unlisted \
  --baseline ../testdata/w3c-shacl/baseline.json \
  --output ../target/conformance
```

Options: inference disabled, data-graph validation, unsupported features treated
as errors. Matching reports use `earl:passed`; expected validation failures must
have a diagnosed SHACL-SPARQL restriction. No manual overrides were used.
Timestamps, binary hashes, and generated blank-node labels can differ on reruns.

This report covers the pinned SHACL 1.0 test cases. It makes no SHACL-AF,
SHACL-JS, or SHACL 1.2 claim. It has not been submitted or published.
