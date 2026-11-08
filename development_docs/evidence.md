# Unified validation evidence

Implementation: [evidence.rs](../crates/shifty-engine/src/evidence.rs),
[witness.rs](../crates/shifty-engine/src/witness.rs), and
[compact.rs](../crates/shifty-engine/src/compact.rs).

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
`shifty.expand_evidence()`, and how the C++ SDK does, as
`EvidenceRun::compact_json()` and `shifty::expand_evidence()`.

## Bindings

The Python and C++ surfaces cover the same operations: the whole horizon
(`validate`), the conformance baseline (`validate_conformance`), scan-then-explain
(`find_failures` + `explain`), the per-snapshot catalog (`constraints`), and the
compact codec.

The C++ SDK draws the typed/JSON line differently, because the C ABI hands over
opaque handles and length-delimited UTF-8 rather than live RDF terms. The
statement and focus structure — ids, constraint kind, rendered selector, focus
term, polarity — is typed; the evidence tree itself crosses as JSON, per focus
(`FocusEvidence::evidence_json`) or per run (`EvidenceRun::json()`). Mirroring
the evidence variants through a C ABI would fix a deeply recursive tagged union
into a flat accessor surface that the vocabulary would outgrow; the compact
codec already treats the serialized form as the interchange, so the binding
follows it.

`explain` returns statements without a catalog, so the C++ binding wraps them in
an `EvidenceRun` whose catalog is empty rather than introducing a second result
shape. One JSON shape means the compact codec and every accessor work on either
entry point.

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
- SHACL-AF expression failures are `Opaque`; passing expressions are
  `Blocked(Unsupported)` because expression-level repair provenance is
  unavailable.
- Passing closed and relational constraints are blocked only in the deletive
  repair direction. Their validation status is still exact.

## Evidence grammar and traversal

`EvidenceNodeRef` is the common read-only view of both polarities. It owns the
structural contract used by traversal and projections:

- every node exposes its polarity, kind, normalized constraint id, and judgment
  node where one exists;
- `children()` returns immediate evidence children in stable semantic order;
- `walk()` is pre-order over that child relation;
- `Not` is the polarity-crossing edge;
- count matches may cross into satisfaction and rejected candidates into
  failure;
- path certificates are support attached to a judgment, not judgments
  themselves.

Code that merely traverses evidence should use this interface. Direct matches
on `Witness` or `SatTrace` are reserved for operations that need variant
payloads, such as synthesis or collecting count values. This prevents a new
variant from being silently omitted by several independent recursive walkers.

Python's `WitnessKind` and `SatKind` are deliberately retained as flattened
summary classifications: for example, `SatKind.Match` is a matched-value row,
not an evidence node. Every such row also exposes its exact
`evidence_kind`, linking it to `CountHeld` or `AllValuesHeld`. Structured
`EvidenceNode` and `RepairOrigin` values expose `EvidenceKind` directly; their
old snake-case `kind` strings are compatibility projections of that enum.

## Identity and provenance

The following identities must not be conflated:

| Identity | Meaning | Stability |
|---|---|---|
| source statement id | authored selector/constraint statement | stable for one parsed source schema |
| normalized statement id | request evaluated after normalization | stable for one normalized schema |
| source constraint id | authored arena node | source-schema local |
| normalized constraint id | executable algebra node | normalized-schema local |
| focus | top-level node selected by a statement | RDF term identity |
| judgment node | node an interior constraint is about | RDF term identity |
| evidence occurrence | one position in one derivation | traversal-local |
| derivation payload | structural evidence value | hash/equality by complete payload |
| repair node id | address of a repair-tree operator | stable within one synthesized tree |

An authored result may share its normalized request and derivation payload with
another authored result. That duplication is required provenance and must not
be reported as evaluation sharing.

`synthesize_with_origins` attaches an `EvidenceOrigin` to each retained repair
node. It records the schema-local statement, a child-index path from that
statement's root evidence, normalized constraint, judgment node, polarity, and
typed evidence kind. The statement is absent for evidence requested directly
against a node and sub-constraint, where no authored statement exists. A joint
synthetic node may cite several origins. The
intended chain is:

```text
source statement → normalized request → evidence occurrence
                 → repair operator → plan choice → graph edit → gate outcome
```

The child-index path is interpreted through `EvidenceNodeRef::children()`, so
origin addressing and public traversal have one grammar rather than two
positionally-coupled walks. The origin map lives in the engine's
`SynthesizedRepair`; `shifty-repair::RepairTree` remains a pure repair IR with no
validation dependency.

## Executable invariants

Tests, rather than comments alone, must enforce these laws:

1. **Partition**: every selected request has exactly one polarity.
2. **Agreement**: evidence polarity equals conformance evaluation for the same
   normalized request.
3. **Dual crossing**: failure of `Not(q)` contains satisfaction of `q`, and
   satisfaction of `Not(q)` contains failure of `q`.
4. **Boolean duality**: failed `And` retains every failed child; failed `Or`
   retains every branch; holding `And` retains all children; holding `Or`
   retains every holding branch needed by the deletive fold.
5. **Count partition**: each qualified candidate appears exactly once as a
   qualifying match or rejected candidate.
6. **Traversal**: `walk()` equals pre-order closure of `children()` and is
   deterministic across validators and repeated runs.
7. **Provenance fan-out**: normalized deduplication retains all authored
   statements and gives duplicates equal evidence.
8. **Synthesis coverage**: every failure and satisfaction variant maps to a
   repair operator or an explicit blocked reason.
9. **Gate soundness**: an accepted delta fixes at least one requested result and
   introduces none under the driver's acceptance policy.
10. **Serialization**: full and compact encodings round-trip without changing
    identity, polarity, ordering, or evidence payload.
