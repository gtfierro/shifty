# Backlog — deferred work, one place

Single source of truth for things we've consciously deferred. Per-area detail
lives in the layer docs (linked); this is the index so nothing is lost. Tags:
**[next]** likely soon · **[do]** queued · **[todo]** someday / lower value.

## Parsing & features (Layer 2) — see [`01-gap-analysis.md`](01-gap-analysis.md)
- **[done]** SHACL-AF **rules** *parsing*: `sh:TripleRule` / `sh:SPARQLRule` +
  node expressions (`sh:this`, constants, paths) lowered into the IR (AF-R,
  AF-E). Richer node expressions (functions, filterShape) still diagnosed.
- **[do]** SPARQL-based **targets** (`sh:target`+`sh:select`) → `Selector::Sparql` (AF-T).
- **[do]** SPARQL-based **constraints** (`sh:sparql`) → `Shape::Sparql` (AF-C).
- **[todo]** Custom constraint **components** (`sh:parameter`+`sh:validator`) (AF-CC).
- **[todo]** SHACL **functions** (`sh:SPARQLFunction`) (AF-F); JS features unsupported.
- **[todo]** `sh:qualifiedValueShapesDisjoint` flag (currently skipped in the suite).

## Engine / semantics (Layers 3, 6)
- **[done]** Rule **execution** (Layer 6): lfp forward chaining, `sh:order`
  strata, naive fixpoint (`shacl_engine::infer`, `shacl infer` CLI). Still to do:
  **[do]** semi-naive (delta) evaluation; **[do]** `sh:SPARQLRule` via oxigraph
  CONSTRUCT; **[do]** function node expressions; **[do]** `validate --infer`.
- **[done]** W3C `sh:ValidationReport` output: component-granular, RDF-driven
  validator (`shacl_engine::validate_report`, `validate --report` CLI) +
  `report_to_graph`. data-shapes core: **75/113 pass, 23 fail, 15 skip**.
  Remaining report components **[do]**: `closed`, `equals`/`disjoint`,
  `lessThan(OrEquals)`, `languageIn`, `uniqueLang`, `qualifiedValueShape`(+counts);
  ill-formed-literal datatype validation; full blank-node graph isomorphism in
  the harness (wildcarded today).
- the algebra path's `Violation`/`Reason` reports stay focus-node + `@id` level
  (the report validator is the W3C-faithful path).
- **[do]** Term ordering beyond numeric + `xsd:string` (dates, etc.) for
  ranges / `sh:lessThan`.
- **[todo]** Well-founded fallback for non-stratifiable schemas (we diagnose &
  refuse today) — see [`03-recursion-semantics.md`](03-recursion-semantics.md).

## Normalization (Layer 4) — see [`04-normalization.md`](04-normalization.md)
- **[do]** Value-type tightening + unsat (range/length merge, datatype conflict).
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
- **[todo]** Incremental / focus-delta scheduling.

## Layer 7 (not started)
- **[todo]** Compilation / JIT of plans; batch/vectorized evaluation.
