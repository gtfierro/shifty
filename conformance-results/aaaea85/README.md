# SHACL 1.0 results for aaaea85

Recorded by the [W3C conformance workflow](https://github.com/gtfierro/shifty/actions/runs/43076972974)
on 2026-11-11 from clean commit `aaaea85bd406021f1fd9056a53d5dca7c3b9a53c`,
using Shifty `0.6.0-alpha.4` (Python `0.6.0a4`).
The pinned W3C suite revision is `40231dfb97c5b606d22ae733947c0d14b4f45c07`.

- All 120 approved cases passed.
- The one proposed case also passed and is reported separately.
- Structured validation passed 114 checks; seven expected validation failures
  are diagnosed before structured validation runs.
- CLI and Python file-input parity passed all 121 cases.

The workflow artifact supplied these files unchanged:

| File | Contents |
| --- | --- |
| [shifty-earl.ttl](shifty-earl.ttl) | EARL implementation report |
| [results.json](results.json) | Per-test report/algebra outcomes and source/build metadata |
| [parity.json](parity.json) | CLI/Python report comparisons and binary hashes |

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

Both interfaces used the data graph, disabled inference, and rejected unsupported
features. Reports were compared by RDF graph isomorphism. This snapshot covers
SHACL 1.0 Core/SPARQL cases; it makes no SHACL-AF, SHACL-JS, or SHACL 1.2 claim
and is not a W3C endorsement. Timestamps, blank-node labels, and binary hashes
may change on reruns.
