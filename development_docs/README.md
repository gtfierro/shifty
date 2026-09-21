# shifty — development docs

A clean-room SHACL + SHACL-AF engine (inference + validation) grounded in the
SHACL fragment of *Common Foundations for SHACL, ShEx, and PG-Schema*
(arXiv:2502.01295), specialized to RDF and built to be heavily optimized.

These are internal design and implementation notes.  The public documentation
lives in `docs/`.

## Implementation-comment convention

The source is meant to be readable as an implementation, not merely as a
translation of the formalism. Keep comments at the boundaries where a reader
would otherwise have to reconstruct intent from several functions:

- Start a deep module with its contract: the representation it owns, the work
  it deliberately centralizes, and the semantics its callers may rely on.
- Explain invariants, semantic translations, ownership/provenance, cache
  invalidation, and performance tradeoffs. Do not narrate syntax the code
  already states.
- At an algorithmic branch, record why it is correct and why that shape of
  algorithm was selected (especially when it protects allocation, ordering,
  or fixpoint behavior).
- Keep the public interface small; put the detailed explanation beside the
  private data structure or algorithm that makes the interface deep.

When a comment makes a semantic claim, retain a focused test or a link to the
corresponding design note. Comments are the guide to the decision; tests remain
the executable guarantee.

Read in order:

1. [`00-formalism.md`](00-formalism.md) — the formal core: path algebra `π`,
   shape grammar `φ`, selectors, schema, and their Rust IR sketch.
2. [`01-gap-analysis.md`](01-gap-analysis.md) — where W3C SHACL / SHACL-AF
   diverge from the paper, every hole, and the fix (stable ids `D0`, `C1`, …).
3. [`02-roadmap.md`](02-roadmap.md) — the layered build, Layer 0 → 7.
4. [`03-recursion-semantics.md`](03-recursion-semantics.md) — the pinned
   recursion semantics (stratified; gfp validation / lfp inference).
5. [`04-normalization.md`](04-normalization.md) — semantics-preserving Layer 4
   rewrites and their correctness boundaries.
6. [`05-sparql-execution.md`](05-sparql-execution.md) — the implemented
   Spargebra-native query subset, shared indexed dataset, and Spareval fallback,
   with future join and path-index options separated from current behavior.
7. [`06-repair.md`](06-repair.md) — symbolic repair, the **library API**:
   witness violations, expose the repair space as `RepairTree` templates (typed
   holes, variadic blocks) by abduction over `φ`, instantiate a caller's `Plan`,
   and gate a candidate. The library decides nothing; an external driver does.
8. [`07-repair-drivers.md`](07-repair-drivers.md) — **reference drivers** over
   the 06 API (monomorphism / enumeration / ASP / LLM) and a reference
   fixpoint loop, as worked examples a real integration may use or replace.
9. [`08-repair-witness-validation-inference-reference.md`](08-repair-witness-validation-inference-reference.md)
   — implementation reference for the repair, validation, and inference seams.
10. [`09-evidence.md`](09-evidence.md) — the current unified evidence behavior,
    compact encoding, guarantees, and blocked cases.
11. [`10-evidence-architecture.md`](10-evidence-architecture.md) — the
    publication-oriented architecture contract: vocabulary, polarity duality,
    identities, executable invariants, and performance boundaries.
12. [`11-compiled-shapes-and-sessions.md`](11-compiled-shapes-and-sessions.md) —
    implemented 0.5 ownership boundaries, APIs, graph semantics, and historical
    migration plan for reusable compiled shapes and evaluation sessions, following the
    [architecture review](0.5-architecture-review.md).
13. [`12-shared-datasets-and-demand-driven-indexes.md`](12-shared-datasets-and-demand-driven-indexes.md)
    — implemented continuation of the compiled-session design: shared source
    storage, graph views, one dataset for native and fallback execution, and
    index selection from compiled access requirements and observed demand.
    [Measured results](../benchmark/shared-dataset-results.md) cover Brick,
    s223, generated workloads, and many-session reuse.

> `static-analysis-plan.md` predates this branch (old spec-shaped `shacl-core`
> approach) and is kept only as historical reference.
