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
- **[todo]** Lower `sh:qualifiedValueShapesDisjoint` into the algebra path; the
  RDF-driven report validator already implements it and passes its core cases.

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
  (`crates/shacl-engine/tests/w3c_advanced.rs`) over
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
- **[do]** **Data-aware statistics**: predicate counts → real selectivity
  ordering (cost is a static proxy today).
- **[do]** Memoize / share shape evaluation across focus nodes within a run.
- **[do]** Implement the staged native SPARQL design in
  [`05-sparql-execution.md`](05-sparql-execution.md): per-query instrumentation
  and capability analysis; immutable `QueryableDataset` indexes; native BGP
  execution with batched `$this`; indexed paths and correlated anti-joins; then
  data-aware join planning. Unsupported whole queries continue through
  Oxigraph/Spareval.
- **[todo]** Incremental / focus-delta scheduling.

## Layer 7 (not started)
- **[todo]** Compilation / JIT of plans; batch/vectorized evaluation.

## Tooling / scripts
- **[done]** `scripts/bench_brick.sh` and `scripts/bench_s223.sh`: aligned
  table output — split `mean±stddev` into separate integer columns so
  `printf` width calculations are exact (the `±` multi-byte character was
  off by one). Header uses `+/-`; data rows use `%d` for both columns.
