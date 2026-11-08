# Draft W3C submission

The draft passes 120 approved SHACL 1.0 Core/SPARQL tests and one proposed
test. It has not been submitted.

| File | Contents |
| --- | --- |
| [shifty-earl.ttl](shifty-earl.ttl) | EARL implementation report |
| [results.json](results.json) | Per-test results, settings, revisions, and hashes |
| [upstream-index.patch](upstream-index.patch) | Proposed W3C listing and report-template changes |
| [PR.md](PR.md) | Draft PR title and description |

## Prepare the final report

The draft includes uncommitted changes. Review and commit the intended code,
then regenerate from that revision:

```sh
cd python
uv sync --dev --frozen --reinstall-package pyshifty
uv run python -m conformance --include-unlisted \
  --baseline ../testdata/w3c-shacl/baseline.json \
  --output ../target/w3c-submission
```

Replace the draft report and results with the new files. Check that all 121
tests pass, and update the date and revision in the submission.

For `w3c/data-shapes`, branch `gh-pages`, copy the report to
`data-shapes-test-suite/reports/shifty-earl.ttl`. Review the index patch
against the current upstream files and use the draft PR description.

The patch updates the static results table and its TopBraid SWP template.
All 121 test IDs already exist in `reports/alltests.ttl`. W3C maintainers may
prefer to regenerate the table themselves.

See [gaps and scope limits](../w3c-conformance-gaps.md) for supporting details.
