# SHACL-AF runtime regression investigation

The 0.6.1 AF implementation introduced avoidable inference overhead. This
investigation uses the existing CLI collector on 45 Brick models and 19 ASHRAE
223P models, with three fresh-process samples per command. Validation includes
inference. The reported changes compare geometric means of per-model ratios;
the benchmark page's bars instead show arithmetic means of per-model medians.

## Recordings

The recordings before these fixes are [Brick](before-brick.json) and
[ASHRAE 223P](before-s223.json), measured at clean commit
`e72a1f739f0169a7263b18a5d48b4a6cc6180a2d`. The recordings after the fixes are
the refreshed [HEAD Brick](../HEAD/brick.json) and
[HEAD ASHRAE 223P](../HEAD/s223.json) files. These were rerun after committing
the fixes, at clean commit `6b56a89b8db0b32eff2605950632a815ed4ed1db`, and
identify the built binary by its hash.
Machine, Rust toolchain, input hashes, sample count, and policy match between
the recordings. Timings remain subject to machine load and sampling noise.

## Full-corpus results

| Corpus | Command | Mean median before (ms) | Mean median after (ms) | Geometric-mean time change |
| --- | --- | ---: | ---: | ---: |
| Brick | Inference | 2,559.3 | 1,297.6 | -44.0% |
| Brick | Inference + validation | 2,697.9 | 1,440.0 | -41.4% |
| ASHRAE 223P | Inference | 1,074.4 | 611.0 | -31.4% |
| ASHRAE 223P | Inference + validation | 1,350.4 | 872.8 | -24.5% |

Relative to the preceding alpha HEAD recording, the final 223P recording is
6.0% faster for inference and 4.4% faster for inference plus validation. Brick
remains 14.6% and 12.8% slower, respectively. That comparison spans all intervening
changes, including corrected ordering and dependency tracking; it does not
isolate the overhead of any one feature.

The alpha and current Brick runs also produce different graphs. On `bldg1`,
the alpha infers 2,122 triples and the current build infers 2,693, a strict
superset. The 571 additional triples comprise 190 `hasTag`, 190 `isTagOf`,
169 `hasQuantity`, 20 `skos:narrower`, and two `skos:related` triples. Thus the
remaining alpha timing gap compares different amounts of inference work.

The committed-code rerun is within 0.5% of the initial post-fix recordings on
both corpora and commands, using geometric means of per-model timing ratios.

## Causes and changes

1. Inference cloned a session dictionary for each tied shape, even if the shape
   had no intermediate rule-order groups. Brick has roughly 12,000 rules.
   Shapes with one rule order now share a read snapshot and stage their outputs
   together. Runs of these shapes inside mixed groups share one branch; shapes
   that need intermediate additions retain their private snapshots.
2. Compilation scanned the entire access catalog twice per rule to recover its
   transitive reads. A rule-index lookup replaces this quadratic work.
3. Correct dependency tracking schedules 223P's fallback label-generating rule
   on later passes. Each focus evaluation rebuilt its regexes. A narrow batch
   rewrite now binds named focus nodes below deterministic STR/REPLACE
   expressions and correlated BGP EXISTS guards. Fresh template nodes, custom
   and volatile functions, aggregates, slicing, and other query forms retain
   their existing execution paths. Blank-node foci retain per-focus execution.

For `pnnl-bdg3-2.ttl`, profiling after the snapshot optimization found about
2.12 seconds in the label rule across six calls. Batching reduced that to
29 milliseconds across the same six calls; session/inference time dropped
from 2.55 seconds to 0.48 seconds. Both runs inferred 19,355 triples. These
individual profiles explain the hotspot; use the full recordings for timing
comparisons.

## Correctness checks

The strict AF harness, workspace Rust tests, Clippy, Python quality gate, and
pinned conformance baseline pass. Python reports 1,518 passed and one skipped.
New regressions compare batched CONSTRUCT output with per-focus output on
both dataset backends, verify correlation and mixed named/blank focus sets,
reject unsafe batching boundaries, and preserve isolation between tied shapes
with and without intermediate additions.

Direct comparisons of the pre-fix and optimized binaries also preserve the
complete inferred graphs for Brick `bldg1` (2,693 triples) and 223P
`pnnl-bdg3-2` (19,355 triples), allowing blank-node relabeling. WASM and C++
smoke checks and the strict documentation build pass.

## Reproduce

```sh
python3 benchmark/run.py HEAD
cd docs
make html SPHINXOPTS="-W --keep-going"
```

To reproduce the earlier measurements, build commit `e72a1f7` in a separate
checkout and run the same collector and fixtures. The earlier alpha HEAD
recording used a dirty working tree, so comparisons with it do not isolate a
single source change or establish performance of the corresponding release tag.
