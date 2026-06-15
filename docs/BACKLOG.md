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
  **[done]** CLI validation runs inference first with a `--no-infer` opt-out;
  **[do]** predicate-level
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
- **[done]** **Data-aware planning**: BGP scan reordering by predicate
  cardinality (`reorder_bgp`) and whole-join-tree flattening + greedy ordering
  (`collect_join_leaves` / `lower_join_leaves`). `Join(Join(A, B), C)` is
  flattened to three leaves and ordered globally rather than as two nested
  binary decisions; the two-arm case is subsumed. `estimate_pattern_cost` tracks
  the propagating bound set across leaves so each cost estimate uses the
  variables already committed by prior leaves. `PlanStats` built from
  `FrozenIndexedDataset` and passed to `lower_query_with_stats` in both
  compilation paths in `sparql.rs`. **[todo]** expose estimated vs. actual rows
  in profiler output; shape-conjunction reordering at the SHACL algebra level.
- **[done]** Shape evaluation is memoized per immutable graph snapshot by
  `(ShapeId, Term)` and shared across statements, focus nodes, target filters,
  qualified counts, and rule conditions/node-expression filters. Results that
  observe a coinductive recursion back-edge remain uncached because their
  provisional truth is call-context-dependent; inference creates a fresh cache
  for each rule firing so graph mutations cannot leave stale entries.
- **[done]** Shape-cache telemetry is exposed through the existing opt-in
  profiler: evaluator count, hits/misses and hit rate, insertions, recursion
  back-edges, cycle-dependent non-cacheable results, peak entries, and
  approximate peak bytes. Evaluators aggregate locally and publish once on
  drop, avoiding a thread-local operation per cache lookup.
- **[do]** Benchmark cache retention on Brick and 223P, then use the evidence
  to choose an optional entry/byte budget or selective admission policy for
  high-cardinality workloads.
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
- **[do]** Native SPARQL stage 5 — data-aware planning (remaining): stats are
  now consumed for BGP and join-tree reordering; `PathDemand` structs are
  extracted but have no consumer; bounded per-start memoization exists, but
  there is no demand-driven strategy selection or Materialized/SccClosure path
  index implementation.
- **[todo]** Native SPARQL stage 6 — broaden coverage: OPTIONAL, MINUS,
  aggregates, arithmetic/comparison expressions, `IN`, `ORDER BY`/`LIMIT`,
  negated property sets, and remaining functions. Deferred until stage 5 lands
  and measured demand drives priorities.
- **[done]** Blank-node focus term dispatch in CONSTRUCT fallback: `sparopt`
  converts `TermPattern::BlankNode` to a fresh query variable (SPARQL semantics:
  blank nodes in patterns are existential), so pre-binding `?this` to a blank
  node focus via algebra substitution degrades to a full predicate scan instead
  of an SP lookup.  For named-node foci the per-focus algebra substitution
  works correctly; blank-node foci now fall back to one global CONSTRUCT run
  (without `?this` bound) whose triples are filtered by blank-node subject
  identity.  Measured speedup on `water-closure.ttl`:
  `ns3:QuantifiablePropertyShape` rule (GROUP BY / COUNT DISTINCT) 33 s → 1.8 s;
  total 39 s → 7.6 s.
- **[done]** Bulk fallback SELECT validation — the SELECT analog of the global
  CONSTRUCT fix. `prefetch_constraint` (in `sparql.rs`) evaluates a constraint
  over a whole focus set in one Spareval run and caches the per-focus violations
  on the compiled entry (`BatchedResults`); `constraint_violations` then serves
  covered foci from the cache instead of cloning/substituting/executing once per
  node. Wired into both algebra validators (`validate_with_frozen`,
  `validate_plan_with_frozen` walk the statement shape through `∧`/`∨`/`¬` to the
  reachable `Shape::Sparql` constraints) and the W3C report path
  (`prefetch_sparql` over each shape's focus set). Caching is a pure memo
  (violations depend only on focus + immutable frozen dataset), so it is sound in
  any operator context. The implemented strategy is **free-`?this`**, *not* the
  originally-sketched `VALUES` table: a measured `VALUES (?this){…}` build was
  *slower* than the per-focus baseline (3.6 s vs 3.1 s) because the table join
  costs more than it saves. Instead, when `?this` is positively bound by the
  required (non-OPTIONAL/MINUS/FILTER) part of the WHERE clause, the query runs
  unmodified (just projecting `?this`) and the foci are recovered from the
  solutions — one selective scan replaces N substituted scans. **Coverage is
  named-node foci only**: a blank-node focus cannot be matched back from query
  results (SPARQL relabels result blank nodes), so blank foci fall through to the
  per-focus path where `$this` is substituted as a constant. Gated off for
  aggregation (`GROUP BY`), slicing (`LIMIT`/`OFFSET`), and non-positively-bound
  `?this`. Measured on `water-closure.ttl` (`validate --no-infer`): 3.1 s → 2.5 s
  (~18%). The win is bounded here because `DontReferToDeprecatedConceptConstraint`
  targets `qudt:Concept`, whose instances are mostly *blank* value structures, so
  the dominant ~19 k blank foci still go per-focus. **[todo]** a stable-blank-id
  path (or a `QueryableDataset` that preserves dataset blank labels through
  Spareval results) would let blank foci batch too and unlock the rest.
- **[done]** BGP scan reordering by predicate cardinality in the native
  executor: within a `GraphPattern::Bgp` block, scans are greedily reordered
  by estimated output cardinality (`reorder_bgp` in `plan.rs`; bound predicate
  → cardinality lookup, bound subject → S-range size, free → full-scan cost).
  Cross-`Join` arm reordering also lands in the same feature: when both arms
  of a `Join` are BGP or Path patterns, `join_arm_cost` estimates each arm's
  output given only `?this` as the initial binding; the cheaper arm runs first.
  This handles the `ClosedWorld223Shape` pattern `?p a/rdfs:subClassOf*
  s223:Relation . $this ?p ?o` — the compact path closure runs first, then the
  BGP probes with `?p` bound (SP+pred range scan instead of a full SP scan).
- **[do]** OSP-index prefilter for `$this`-as-object constraints: when the
  first meaningful triple pattern in a constraint's WHERE clause is `?s ?p
  $this` (focus node appears as the object), use the OSP index to restrict the
  focus set to nodes that have at least one inbound edge before entering the
  full constraint evaluation. Removes the overhead of evaluating all focus nodes
  (e.g. all `qudt:Concept` instances) when only a small subset are referenced
  as objects.
- **[do]** Whole-relation transitive-closure index: materialize `rdfs:subClassOf*`
  (and other frequently-queried transitive predicates) as a
  `HashMap<TermId, HashSet<TermId>>` once per frozen snapshot. The per-start
  memoization cache is the building block; promote to whole-relation when
  `PathDemand` shows the full relation is probed from > T distinct starts (T
  tunable). Eliminates repeated BFS/DFS inside `ClosedWorld` and deprecated-
  concept constraints that both walk `rdfs:subClassOf*`.
- **[do]** Hash-join for large probe sets in the native executor: `extend_scan`
  is a nested-loop probe against the sorted triple indexes. When the build side
  (bound variable range) is large and the probe side is small (e.g. a filtered
  predicate set), build a hash set from the smaller side and use it as a
  membership filter during the scan rather than relying on sort-merge range
  lookups. Pairs naturally with BGP reordering above.
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
