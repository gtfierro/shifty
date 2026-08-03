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

## Conformance-only baseline

`PreparedEvidenceValidator::validate_conformance` runs the same snapshot,
target selection, and evaluator, but decides each selected pair with one
short-circuiting satisfaction test instead of materializing evidence. It exists
so evidence-tracing cost can be measured against an otherwise identical
execution (`benchmark/bench_evidence.sh`), and for callers that want only a
verdict from an already-prepared snapshot. It reports counts over *normalized*
pairs, before authored fan-out, and does not honor `minimum_severity` — with no
failure evidence there is no per-constraint severity to weigh, so any failing
pair makes the run non-conforming.

## Compact encoding

`compact::to_compact_json` writes the same run with evidence nodes and RDF
terms hash-consed into tables and referenced by index; `expand` reconstructs it
exactly. Two redundancies dominate the full form, on different corpora: the
same `(constraint, node)` conclusion is reached through many parents and is
written out at each occurrence, and the constraint catalog is dumped on every
run whatever the findings. Passing `include_catalog: false` elides the catalog
for consumers that already hold the schema; those runs decode through
`expand_with_catalog`.

The encoding is structural — it interns any tagged `{type, details}` object and
any RDF term — so it tracks the evidence vocabulary without mirroring every
variant. `compact_value`/`expand_value` operate on already-serialized runs, so a
caller holding one only as JSON encodes it without a typed round-trip; that is
how the Python bindings expose it, as `EvidenceRun.to_compact_json()` and
`shifty.expand_evidence()`.

## Guarantees

- Positive recursion uses the validator's greatest-fixed-point semantics. An
  evidence recursion back-edge is a finite `Coinductive` satisfaction leaf.
- `PathSupport` is one concrete positive reachability certificate. Alternative
  paths retain the first successful syntactic alternative. It is neither all
  routes nor a complete deletion cut. Certificates are derived during the value
  traversal (`succ_with_support`), so which route a multi-route value cites
  follows traversal order and is not part of the contract; that every cited
  triple exists is.
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
