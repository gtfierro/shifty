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
- **[do]** Full SHACL-SPARQL prebinding: complex `$PATH` AST replacement,
  `$shapesGraph` / `$currentShape`, and explicit execution-error propagation.
- **[do]** Add SPARQL constraints and targets to the RDF-driven W3C report path;
  the algebra conformance path executes them today. This blocks the
  `advanced/sparql/` and `advanced/target/` validation cases, which the advanced
  harness skips for now (see below).
- **[done]** W3C `sh:ValidationReport` output: component-granular, RDF-driven
  validator (`shacl_engine::validate_report`, `validate --report` CLI) +
  `report_to_graph`. data-shapes core: **98/113 pass, 0 fail, 15 skip**.
  Report coverage includes `closed`, property pairs, term ordering,
  `languageIn`, `uniqueLang`, qualified counts/disjointness, and ill-formed
  datatype literals. Full blank-node graph isomorphism in the harness remains
  **[do]** (blank nodes are wildcarded today).
- **[done]** W3C SHACL-AF **advanced** suite harness
  (`crates/shacl-engine/tests/w3c_advanced.rs`) over
  `testdata/test-suite/advanced/`, decoding the DASH test vocabulary:
  `dash:InferencingTestCase` → `infer` + expected-triple check;
  `sht:Validate` / `dash:GraphValidationTestCase` → `validate_report` result-set
  match. Advanced files use relative IRIs, so a `file://` base is supplied.
  Current: **91 pass (4 inferencing + 87 validation), 0 fail, 14 skip (of 105)**.
  The 14 skips are the still-unsupported features below:
  **[todo]** SHACL functions (AF-F, `sh:SPARQLFunction`) → 3 inferencing skips +
  the `function/` cases; **[do]** SPARQL constraints/targets on the report path →
  `advanced/sparql/` + `advanced/target/`; plus custom components / `sh:expression`.
- the algebra path's `Violation`/`Reason` reports stay focus-node + `@id` level
  (the report validator is the W3C-faithful path).
- **[do]** Term ordering beyond numeric, `xsd:string`, and `xsd:dateTime`
  (additional date/time and duration datatypes) for ranges / `sh:lessThan`.
- **[todo]** Well-founded fallback for non-stratifiable schemas (we diagnose &
  refuse today) — see [`03-recursion-semantics.md`](03-recursion-semantics.md).

## Normalization (Layer 4) — see [`04-normalization.md`](04-normalization.md)
- **[done]** Value-type tightening + unsat (`ValueType::normalize` + `mk_and`
  `merge_value_types`): same-family range/length bound merge, empty-range /
  `len.min>max` / distinct-datatype → ⊥, `test(any)` → ⊤. Numeric ordering only;
  cross-facet unsat still **[todo]**.
- **[do]** Path converse-normalization (`(π₁·π₂)⁻ = π₂⁻·π₁⁻`, inverse on `Pred` only).
- **[do]** `id`-path collapse (`∃≥1 id.φ = φ`); qualifier-⊥ count collapse.
- **[todo]** Absorption, complementary atoms, cross-facet unsat, selector
  canonicalization, target merging.

## Planning / execution (Layer 5)
- **[do]** **Path compilation**: index-aware reachability plans for
  `Seq/Alt/Star/Inverse`; SPO/POS/OSP order choice (replace naive re-walking).
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
