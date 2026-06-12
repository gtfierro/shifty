# Backlog — deferred work, one place

Single source of truth for things we've consciously deferred. Per-area detail
lives in the layer docs (linked); this is the index so nothing is lost. Tags:
**[next]** likely soon · **[do]** queued · **[todo]** someday / lower value.

## Parsing & features (Layer 2) — see [`01-gap-analysis.md`](01-gap-analysis.md)
- **[done]** SHACL-AF **rules** *parsing*: `sh:TripleRule` / `sh:SPARQLRule` +
  node expressions (`sh:this`, constants, paths) lowered into the IR (AF-R,
  AF-E). Richer node expressions (functions, filterShape) still diagnosed.
- **[done]** SPARQL-based **targets** (`sh:target`+`sh:select`) →
  `Selector::Sparql` (AF-T), parsed and canonicalized with Spargebra.
- **[done]** SPARQL-based **constraints** (`sh:sparql`) → `Shape::Sparql`
  (AF-C), including `sh:prefixes` resolution and query-kind validation.
- **[todo]** Custom constraint **components** (`sh:parameter`+`sh:validator`) (AF-CC).
- **[todo]** SHACL **functions** (`sh:SPARQLFunction`) (AF-F); JS features unsupported.
- **[done]** `sh:qualifiedValueShapesDisjoint` is lowered into the algebra count
  qualifier as the qualified shape conjoined with the negation of every sibling
  qualified shape. The RDF-driven report path uses the same parent-based sibling
  definition, including sibling property shapes with different paths.
- **[done]** `sh:disjoint` is supported on property shapes and node shapes; node
  shapes lower through the identity path so the focus node is compared with the
  values of the named predicate.
- **[done]** `sh:equals` is supported on property shapes and node shapes; node
  shapes lower through the identity path so the focus node singleton must equal
  the values of the named predicate.

## Engine / semantics (Layers 3, 6)
- **[done]** Rule **execution** (Layer 6): lfp forward chaining, `sh:order`
  priority groups, global fixpoint (`shacl_engine::infer`, `shacl infer` CLI),
  and conservative predicate-delta scheduling at rule granularity. Later-order
  output can reactivate earlier rules. Oxigraph `CONSTRUCT` execution supports
  `$this` prebinding; CONSTRUCT blank-node output is rejected to preserve
  termination for the supported subset. Still to do: **[do]** finer focus-node
  / relational-delta evaluation; **[do]** function node expressions;
  **[done]** CLI validation runs inference first; **[do]** predicate-level
  logical stratification for non-monotone rule conditions (the current analyzer
  stratifies shape SCCs).
- **[done]** Initial SPARQL execution backend: one Oxigraph store per validation
  or inference run, cached parsed/prepared queries, and Spargebra AST
  substitution for `$this` and simple-predicate `$PATH` prebinding. Split
  data/shapes validation defaults to union evaluation with data-only focus
  discovery; the API and CLI also expose data-only and full-union modes.
- **[done]** Report-path SPARQL uses frozen dataset: `validate_report_context` now
  calls `.with_frozen(FrozenIndexedDataset::from_graph(context))` (or
  `from_graphs` when a shapes graph is needed), matching what the algebra path
  does in `validate_with_context`. SPARQL constraints that lower to the native
  subset run through `native_exec`; fallback queries run over
  `QueryableDataset` instead of the mutable Oxigraph Store. **[done]** The
  report-path `succ()`/`pred()` now run over the indexed `FrozenIndexedDataset`
  too, via the `path::PathBackend` trait (see Layer 5).
- **[done]** Full SHACL-SPARQL prebinding: `$shapesGraph` / `$currentShape`
  were already substituted as named-node terms. Complex `$PATH` (non-predicate
  paths) now rewritten via `path_to_property_path` + `rewrite_path_pattern`:
  `Path::Alt/Seq/Star/Inverse` → `PropertyPathExpression`; BGP triples with
  `?PATH` predicate replaced by `GraphPattern::Path`. `Path::Id`/unsupported
  variants fall through to an error (fail-closed). Tests cover
  alternative, sequence, inverse, and zero-or-more paths.
- **[done]** SPARQL constraints and targets on the RDF-driven W3C report path:
  `collect_sparql` and SPARQL-based `focus_nodes` were already implemented;
  removed the stale skip guards in the advanced harness. Now executes the
  `advanced/sparql/` and `advanced/target/` cases (5 new passes).
- **[done]** W3C `sh:ValidationReport` output: component-granular, RDF-driven
  validator (`shacl_engine::validate_report`, `validate --report` CLI) +
  `report_to_graph`. data-shapes core: **98/113 pass, 0 fail, 15 skip**.
  Report coverage includes `closed`, property pairs, term ordering,
  `languageIn`, `uniqueLang`, qualified counts/disjointness, and ill-formed
  datatype literals.
- **[done]** Exact blank-node comparison in both report harnesses: focus/value
  blank nodes compare exactly (same file → same IDs); complex `sh:resultPath`
  blank-node sub-graphs are canonicalized via `parse_path` + `path_to_string`;
  `sh:sourceShape` blank nodes are still wildcarded (expected results use empty
  `[ ]` placeholders for anonymous shapes).
- **[done]** W3C SHACL-AF **advanced** suite harness
  (`crates/shifty-engine/tests/w3c_advanced.rs`) over
  `testdata/test-suite/advanced/`, decoding the DASH test vocabulary:
  `dash:InferencingTestCase` → `infer` + expected-triple check;
  `sht:Validate` / `dash:GraphValidationTestCase` → `validate_report` result-set
  match. Advanced files use relative IRIs, so a `file://` base is supplied.
  Current: **96 pass (4 inferencing + 92 validation), 0 fail, 9 skip (of 105)**.
  The 9 skips are the still-unsupported features below:
  **[todo]** SHACL functions (AF-F, `sh:SPARQLFunction`) → 3 inferencing skips +
  the `function/` cases; plus custom components / `sh:expression` → 6 validation
  skips.
- the algebra path's `Violation`/`Reason` reports stay focus-node + `@id` level
  (the report validator is the W3C-faithful path).
- **[done]** Term ordering extended: `compare_terms` now handles `xsd:date`,
  `xsd:time`, `xsd:duration`, `xsd:yearMonthDuration`, and
  `xsd:dayTimeDuration` via `oxsdatatypes` `PartialOrd` (which uses the XSD
  four-reference-datetime algorithm for durations, correctly returning `None`
  for genuinely incomparable pairs like `P1M` vs `P30D`). `valid_lexical_form`
  validates the same types. 9 unit tests added in `value::tests`.
- **[todo]** Well-founded fallback for non-stratifiable schemas (we diagnose &
  refuse today) — see [`03-recursion-semantics.md`](03-recursion-semantics.md).

## Normalization (Layer 4) — see [`04-normalization.md`](04-normalization.md)
- **[done]** Value-type tightening + unsat (`ValueType::normalize` + `mk_and`
  `merge_value_types`): same-family range/length bound merge, empty-range /
  `len.min>max` / distinct-datatype → ⊥, `test(any)` → ⊤. Numeric ordering only;
  cross-facet unsat still **[todo]**.
- **[done]** Path converse-normalization (`(π₁·π₂)⁻ = π₂⁻·π₁⁻`, inverse on `Pred` only):
  `push_inverse`/`normalize_path` in `shacl_opt::normalize`; called from `simplify`,
  `rebuild_cyclic`, and `selector`. Also applied to `Eq`/`Disj`/`Lt`/`Le`/`UniqueLang` leaves.
- **[done]** `id`-path collapse (`∃≥1 id.φ = φ`, `∃[0..0] id.φ = ¬φ`, `∃≥2 id.φ = ⊥`) and
  qualifier-⊥ count collapse (`∃≥1 π.⊥ = ⊥`, `∃[0..m] π.⊥ = ⊤`) in `mk_count`.
- **[todo]** Absorption, complementary atoms, cross-facet unsat, selector
  canonicalization, target merging.

## Planning / execution (Layer 5)
- **[done]** **Path compilation**: index-aware reachability plans for
  `Seq/Alt/Star/Inverse`; SPO/POS/OSP order choice (replace naive re-walking).
  `ReachStep` IR pre-interns predicate `TermId`s and folds `Inverse` into step
  direction at compile time; `apply_closure` dispatches `Star/Plus/Opt` over
  compiled steps; `compile_bwd` reverses `Seq` order statically.
- **[done]** Storage-backend trait for path evaluation: `path::PathBackend`
  exposes `objects`/`subjects`/`out_predicates`, and `succ`/`pred` are generic
  over it (`B: PathBackend + ?Sized`). Two impls: `oxrdf::Graph` (linear B-tree
  scans, kept for inference's growing graph) and `FrozenIndexedDataset` (the
  `u32`-dictionary sorted indexes). The report path (`Reporter::frozen`) and both
  algebra entry points (via `SparqlExecutor::frozen()`, `Graph` fallback) now
  traverse `sh:path` / `rdfs:subClassOf*` over the indexed snapshot built once
  per run; inference keeps the `Graph` backend. A `Graph`-vs-`Frozen`
  differential unit test pins agreement across `Pred/Inverse/Seq/Alt/Star`.
  Still naive `Term`-space (re-interns each frontier node); folding into
  `TermId`-space compiled `ReachStep` plans is the staged native SPARQL design
  below.
- **[next]** **Data-aware planning**: consume the predicate cardinality and
  distinct subject/object statistics already collected by
  `FrozenIndexedDataset`. Reorder native BGP joins and shape conjunctions using
  estimated rows/fanout instead of the current static cost proxy. Expose
  estimated versus actual rows in plan/profile output and benchmark index-build
  time, execution time, and peak memory on Brick and 223P.
- **[done]** Shape evaluation is memoized per immutable graph snapshot by
  `(ShapeId, Term)` and shared across statements, focus nodes, target filters,
  qualified counts, and rule conditions/node-expression filters. Results that
  observe a coinductive recursion back-edge remain uncached because their
  provisional truth is call-context-dependent; inference creates a fresh cache
  for each rule firing so graph mutations cannot leave stale entries.
- **[next]** Add shape-cache telemetry and memory controls: hits/misses,
  cycle-dependent non-cacheable evaluations, entry count, and estimated bytes.
  Use benchmark evidence to choose an optional entry/byte budget or selective
  admission policy for high-cardinality workloads.
- **[done]** Memoize repeated native property-path closures by
  `(start TermId, compiled ReachStep, closure kind, graph)` within an immutable
  `FrozenIndexedDataset`. Cached endpoint sets are shared across probes, bounded
  to one million stored endpoint IDs, and invalidated when inference extends the
  dataset.
- **[next]** Consume `PathDemand` to choose between traversal, per-start
  memoization, whole-relation materialization, and SCC closure for repeated
  transitive paths.
- **[done]** Staged native SPARQL design, stages 1–4
  ([`05-sparql-execution.md`](05-sparql-execution.md)):
  capability analysis (`sparql_native/capability.rs`), per-query profiling
  (`profile.rs`), path demand extraction (`sparql_native/demand.rs`),
  `FrozenIndexedDataset` + `QueryableDataset` impl (`frozen.rs`), native BGP
  executor with batched `$this`, safe-boolean `ExprPlan`, property paths in all
  4 binding modes, compiled `ReachStep` traversal, correlated
  `EXISTS`/`NOT EXISTS`, native `STR`/`STRSTARTS`, and debug-build differential
  testing.
  Unsupported queries fall back to Spareval over the same frozen dataset.
- **[next]** Native SPARQL stage 5 — data-aware planning: `DatasetStatistics`
  (`predicate_cardinality`) is collected in `frozen.rs` but never consulted;
  joins are left-deep with no selectivity ordering; `PathDemand` structs are
  extracted but have no consumer; bounded per-start memoization exists, but
  there is no demand-driven strategy selection or Materialized/SccClosure path
  index implementation.
- **[todo]** Native SPARQL stage 6 — broaden coverage: OPTIONAL, MINUS,
  aggregates, arithmetic/comparison expressions, `IN`, `ORDER BY`/`LIMIT`,
  negated property sets, and remaining functions. Deferred until stage 5 lands
  and measured demand drives priorities.
- **[do]** Finer inference deltas: replace rule-level predicate invalidation
  with affected-focus and relational deltas where dependency analysis can prove
  them sound; retain wildcard rescheduling for opaque/domain-sensitive rules.
- **[todo]** Incremental validation / focus-delta scheduling across graph
  updates, including cache invalidation by changed predicate and affected path.

## Layer 7 (not started)
- **[todo]** Batch/vectorized shape evaluation over `TermId` columns to reduce
  RDF-term cloning, hashing, and repeated dictionary translation.
- **[todo]** Compilation / JIT of plans after profiling shows dispatch or
  expression evaluation is a material bottleneck.

## Tooling / scripts
- **[done]** `benchmark/bench_brick.sh` and `benchmark/bench_s223.sh`: aligned
  table output — split `mean±stddev` into separate integer columns so
  `printf` width calculations are exact (the `±` multi-byte character was
  off by one). Header uses `+/-`; data rows use `%d` for both columns.
