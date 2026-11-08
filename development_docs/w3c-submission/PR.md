# Add Shifty SHACL 1.0 implementation report

Add [Shifty](https://github.com/gtfierro/shifty) to the implementation list.
All 120 approved Core/SPARQL tests and the separate proposed node-validator
test pass full report comparison or produce the expected validation failure.
No manual pass overrides were used.

The report uses unmodified fixtures from W3C data-shapes commit
`40231dfb97c5b606d22ae733947c0d14b4f45c07`. The runner follows manifest actions
and compares reports using W3C's rules. Inference is disabled, validation uses
the data graph, and unsupported features are errors. SHACL-AF, SHACL-JS, and
SHACL 1.2 are outside this report's scope.

To reproduce from the submitted Shifty revision:

```sh
cd python
uv sync --dev --frozen --reinstall-package pyshifty
uv run python -m conformance --include-unlisted \
  --baseline ../testdata/w3c-shacl/baseline.json
```

Before submitting, regenerate the report from the reviewed, committed code
and link that revision here.
