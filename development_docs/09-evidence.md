# Unified validation evidence

`PreparedEvidenceValidator` normalizes, indexes, selects targets, and creates a
single evaluator for a graph snapshot. Its result is grouped by authored
statement; a statement with no selected focus nodes is represented by an empty
`selected_foci` list.

Satisfaction and failure are produced by one recursive dispatcher. Boolean
children and qualified-count candidates are evaluated once, after which the
dispatcher partitions the results into the applicable polarity. Canonical
failure evidence remains pruned, while `EvaluationProgress` reports immediate
authored children in the raw schema's stable order.

## Guarantees

- Positive recursion uses the validator's greatest-fixed-point semantics. An
  evidence recursion back-edge is a finite `Coinductive` satisfaction leaf.
- `PathSupport` is one concrete positive reachability certificate. Alternative
  paths retain the first successful syntactic alternative. It is neither all
  routes nor a complete deletion cut.
- Statements are ordered by raw statement id; focus nodes use lexical
  N-Triples order; evidence traversal is pre-order; projections deduplicate by
  first occurrence.

## Opaque or blocked variants

- A passing SPARQL constraint is `Blocked(OpaqueSparql)` because Shifty cannot
  synthesize a sound data-deletion repair for arbitrary queries.
- A failing SPARQL constraint is `Opaque` and retains declared messages and the
  cached query diagnostic.
- SHACL-AF expression failures remain `Opaque`; passing expressions are
  `Blocked(Unsupported)` because expression-level repair provenance is not yet
  available.
- Passing closed and relational constraints are blocked only in the deletive
  repair direction. Their validation status is still exact.
