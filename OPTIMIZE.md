# SHACL Optimization Backlog

This document tracks optimization work aimed at reducing repeated store scans,
target discovery overhead, and whole-graph rule sweeps in Shifty.

## Implemented First Pass

- Data-dependent target pruning for `sh:targetClass`
- Data-dependent target pruning for `sh:targetSubjectsOf` when the predicate is absent
- Data-dependent target pruning for `sh:targetObjectsOf` when the predicate is absent
- Safe pruning of top-level property-shape targets when:
  - the path is a simple predicate
  - that predicate is absent from the data graph
  - the property shape has no constraint that can fail on an empty value set
- Lazy per-focus outgoing-predicate summaries for the data graph
- Property-shape value selection now skips simple-path lookups when the current
  focus node definitely lacks the required outgoing predicate
- The same summary now tracks:
  - incoming predicate presence
  - per-focus outgoing predicate counts
  - `predicate -> subjects` and `predicate -> objects` lookup sets
- `sh:targetSubjectsOf` and `sh:targetObjectsOf` now resolve from the summary
  instead of via SPARQL
- simple and inverse-simple property paths now resolve value nodes from cached
  adjacency instead of SPARQL
- cardinality-only property shapes on simple and inverse-simple paths now use
  cached counts without materializing value nodes
- mixed simple/inverse-simple property shapes now use cached counts to run
  count constraints first and skip conservative zero-value vacuous validators
- SPARQL constraints now use a conservative `$this` required-predicate prefilter
  derived from parsed query algebra before executing the full query

## Next High-Yield Passes

### 1. Expand Per-Focus Predicate Summaries

Current state:

- outgoing and incoming predicate presence is cached lazily per focus node
- per-focus outgoing predicate counts are cached
- per-focus incoming predicate counts are cached
- `predicate -> subjects` / `predicate -> objects` are cached
- simple and inverse-simple property paths resolve directly from the cache
- cardinality-only simple/inverse-simple property shapes use cached counts
- `targetSubjectsOf` / `targetObjectsOf` use it to avoid SPARQL

Next extensions:

- use predicate counts more aggressively for mixed constraint sets
- extend the SPARQL prefilter beyond direct `$this` predicate requirements

### 2. SPARQL Mandatory-Pattern Prefilters

Current state:

- direct `$this`-anchored named-predicate requirements are extracted from parsed
  algebra
- `JOIN` unions those requirements
- `UNION` intersects them
- `OPTIONAL` / `MINUS` only contribute requirements from their mandatory side
- the validator skips the full query when the focus node cannot satisfy those
  required incoming/outgoing predicates

Next extensions:

- add required-class signatures
- extend extraction to more mandatory path forms
- reuse the same signature logic for SPARQL rules and custom constraints

### 3. Batched SPARQL Execution with `VALUES`

Current hotspot:

- slow 223p benchmark models are dominated by repeated zero-row SPARQL
  constraints executed once per focus node
- representative cases include the same query running hundreds or thousands of
  times with no matches

Plan:

- execute `$this`-anchored SPARQL constraints in chunks rather than one focus at
  a time
- inject batched focus candidates with:
  - `VALUES ?this { ... }`
- collect result rows keyed by `?this` and then materialize failures per focus
- use adaptive chunk sizes rather than one global batch size

Initial scope:

- only for SPARQL constraints that:
  - are anchored on `$this`
  - do not require per-focus query text changes
  - already pass pre-binding validation
- keep the existing per-focus path as a fallback

Heuristics:

- start with fixed chunking (for example 64 or 128 focus nodes)
- shrink chunks when query runtime or result size spikes
- skip batching for tiny target sets where per-focus dispatch is cheaper

Why this matters:

- this directly attacks the dominant profile signature in `NIST-IBAL`,
  `lbnl-bdg3-1`, `pnnl-bdg2-1`, and `pnnl-bdg3-2`
- it should reduce repeated planning, dispatch, and full-graph scans for
  negative SPARQL checks

### 4. Query-Class Lowering for Common SHACL SPARQL Shapes

Goal:

- optimize classes of queries, not individual query strings

Current state:

- generic adjacency-whitelist `FILTER NOT EXISTS` queries over a `$this`-anchored
  path can now be recognized from SPARQL algebra and executed from cached
  adjacency summaries instead of the generic SPARQL engine
- this recovers part of the old closed-world/whitelist-style optimization
  surface without any ontology-specific shape or predicate references

Approach:

- inspect parsed SPARQL algebra and detect recurring SHACL query patterns
- lower recognized patterns to native indexed joins instead of generic SPARQL

Promising classes:

- missing-related-node checks
- closed-world relation checks
- path/class existence checks
- “if related node exists, then related property must exist” checks

Why this matters:

- many expensive SHACL SPARQL constraints are really regular graph joins in
  disguise
- a small number of query classes will likely cover a large fraction of the
  runtime on 223p-style models

### 5. Path Evaluation Cache

Current state:

- simple and inverse-simple single-step paths already use cached adjacency

Next step:

- cache `(focus, path)` results for short reusable paths
- target:
  - inverse paths
  - short sequences
  - paths reused across multiple constraints or sibling shapes

Requirements:

- path cache should be built after inference or invalidated when inference adds
  triples
- cache should be bounded or interned enough to avoid unbounded growth on large
  graphs

Why this matters:

- the slow benchmark models still spend substantial time in
  `property_value_selection`
- a path-result cache is the next natural step after the current predicate
  summary

### 6. Shape Scheduling and Traversal Order

Inspired by Trav-SHACL:

- prioritize shapes that are likely to invalidate large candidate sets early
- prefer:
  - explicit target classes with small candidate sets
  - shapes with selective predicates
  - shapes that feed many downstream constraints

Possible signals:

- target set size
- required-predicate rarity
- historical benchmark cost
- downstream dependency count

Why this matters:

- validation order can reduce the amount of later work even without changing
  semantics
- this is a lower-risk stepping stone toward fuller candidate propagation

### 7. Candidate-Set Propagation Across Shapes

Goal:

- carry knowledge from already-processed shapes into later scheduling

Examples:

- if a later shape only matters for nodes reached from a failing earlier shape,
  shrink or skip that later candidate set
- if a relation/path precondition is already known to be absent, avoid grounding
  downstream SPARQL depending on it

This is the closest analogue to Trav-SHACL traversal-based pruning and likely
requires explicit shape-dependency metadata.

### 8. Batched Native Property Validation

Goal:

- validate groups of property shapes over shared path results instead of
  materializing values independently per shape

Approach:

- bucket by:
  - target class
  - path
  - constraint family
- fetch values once per bucket and reuse them across validators

Why this matters:

- repeated path extraction is still a measurable hotspot even after the current
  summary work
- this complements path caching and reduces allocator churn

## Inference Optimizations

### 9. Delta-Driven Rule Scheduling

Current state:

- every inference round walks every rule

Desired state:

- maintain an agenda keyed by newly-added predicates/classes
- only rerun rules whose target selection or rule body can observe the new facts

This is the biggest algorithmic win available in inference.

### 10. Semi-Naive Inference

Run rules against the delta from the previous round rather than the entire graph.

This is especially useful for:

- inverse-property propagation
- connectivity closure rules
- property-copy rules that repeatedly rediscover old facts

### 11. Rule Dependency Graph

For each rule, record:

- predicates/classes read by the rule body
- predicates emitted by the rule head
- target classes and target predicates

Then use that graph to:

- wake only downstream rules after a delta
- skip whole rule families for unrelated datasets

## Validation Optimizations

### 12. Constraint Family Partitioning

Partition property-shape constraints into:

- missing-value-sensitive constraints
- value-only constraints
- expensive query/custom constraints

When a path yields zero values:

- run only the missing-value-sensitive subset
- skip the rest

### 13. Shape Bucketing by Target Class and First Predicate

Group shapes by:

- target class
- first selective predicate

Then validate by bucket rather than shape-by-shape, reducing repeated scans over
the same focus sets.

## Report/Diagnostics Optimizations

### 14. Lazy Report Materialization

Separate:

- validation result computation
- rich report graph construction

Only build full source-shape/source-constraint neighborhoods and formatted
messages when the caller requests a report serialization.

### 15. Logging Discipline During Profiling

Profile with `INFO` logging disabled by default.

The current validation path emits a large number of target-selection logs, which
adds noise and measurable overhead during profiling runs.

## Immediate Execution Plan

The next implementation sequence should be:

1. Batched SPARQL execution with `VALUES`
2. Stronger SPARQL mandatory-pattern prefilters
3. Path evaluation cache for short reusable paths
4. Shape scheduling heuristics
5. Candidate-set propagation across dependent shapes

This ordering matches the current benchmark evidence:

- the largest slowdowns are repeated negative SPARQL executions
- the second-largest slowdowns are repeated property value selection passes
