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
- **[next]** Rule **execution**: lfp semi-naive fixpoint, `sh:order` strata,
  recursion via the stratified framework (Layer 6).
- **[do]** Wire `Schema.names` provenance into validation **reports** (real
  `sh:sourceShape`; reports are focus-node + `@id` level today).
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
