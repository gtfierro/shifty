# 10 — Evidence architecture and duality contract

This document is the architectural contract for validation evidence and its
use by repair. It fixes the vocabulary, boundaries, identities, and laws that
the implementation and publication should share. `09-evidence.md` records the
current behavior; this document says which parts of that behavior are design
invariants and where future optimization is allowed.

## 1. Architectural pipeline

```text
authored SHACL
      │ parse/lower
      ▼
source Schema ── normalize_with_mapping ──► normalized Schema + provenance
                                                   │
                                                   ▼
                                      one polarity-aware evaluator
                                                   │
                              ┌────────────────────┴────────────────────┐
                              ▼                                         ▼
                    Failure (`Witness`)                    Satisfaction (`SatTrace`)
                     additive repair                         deletive repair
                              └────────────────────┬────────────────────┘
                                                   ▼
                                              RepairTree
                                                   │ driver Plan
                                                   ▼
                                              GraphDelta
                                                   │
                                              gate/revalidate
```

The boundaries are deliberate:

1. **Lowering** preserves authored statements and source constraints.
2. **Normalization** may merge algebra but must retain the many-to-one source
   mapping.
3. **Evaluation** decides one normalized `(statement, focus)` request and
   constructs evidence for its polarity.
4. **Reporting** fans that result back out to every authored statement. This is
   source traceability, not repeated evaluation.
5. **Synthesis** folds evidence into a repair space. It makes no driver choice.
6. **Instantiation** applies a driver's choices to obtain a graph delta.
7. **The gate** evaluates that delta and returns its effects; applying it is a
   driver decision.

Parsing, provenance, evaluation, evidence, synthesis, search, and acceptance
must remain separate concepts even when an API offers a convenient session that
composes them.

## 2. Vocabulary

Use these terms consistently in Rust, Python, documentation, benchmarks, and
the paper:

- **Judgment**: whether one normalized constraint holds for one node. Its
  address is `(constraint, node, polarity)`; `Top` has no node.
- **Request**: one normalized `(statement, focus)` selected for evaluation.
- **Authored result**: one source `(statement, focus)` record. Several authored
  results may report the same normalized request after common-subexpression
  elimination.
- **Evidence**: the polarity-tagged derivation of a judgment.
- **Failure**: evidence that a constraint does not hold. `Witness` is the
  current repair-facing Rust spelling; `Failure` is the public evidence
  vocabulary.
- **Satisfaction**: evidence that a constraint holds. `SatTrace` is the current
  Rust spelling.
- **Evidence kind**: a variant of failure or satisfaction evidence. This is not
  a driver enumeration. Rust and Python expose the same exhaustive
  `EvidenceKind`; adding a Rust variant makes the Python conversion
  non-exhaustive at compile time.
- **Repair operator**: `All`, `Any`, `Repeat`, `Edits`, `Noop`, or `Blocked` in
  `RepairTree`.
- **Repair choice**: a branch, repeat count, or typed-hole binding supplied in
  a `Plan`.
- **Candidate enumeration**: a driver strategy that searches repair choices.
  It is downstream of evidence and repair-tree construction.
- **Derivation context**: path and parent information such as `reached_by` and
  `produced_by` that can make two payloads differ even when their judgment
  address is equal.

`Failure` and `Satisfaction` are the canonical public Rust enum definitions.
`Witness` and `SatTrace` are compatibility aliases retained at the repair
boundary; they do not name different concepts.

## 3. Polarity duality

Failure and satisfaction are semantic duals, not isomorphic data structures.
Both are produced by one recursive evaluation. They cross polarity at `Not`
and inside qualified counts, where a matched candidate carries satisfaction and
a rejected candidate carries failure.

| Algebra constructor | Failure evidence | Satisfaction evidence | Repair fold |
|---|---|---|---|
| `Top` / `Pending` | unreachable | `Irrefutable` | deletive repair is `Blocked`; no data deletion falsifies `Top` |
| value test | `Atom` with the observed value and optional producing path | `Atom` with a concrete producing path | replace/add value vs. cut its support |
| relational | `Relational` with compared sets and offenders | `Blocked` in the deletive direction | kind-specific repair vs. explicit unsupported deletion |
| `Closed` | `Closed` with disallowed edges | `Blocked(ClosedNeedsAdd)` | delete offenders vs. add a forbidden edge, outside the current deletive scope |
| `Not(q)` | `Not { inner: Satisfaction(q) }` | `NotHeld { inner_fails: Failure(q) }` | switch from repair to break, or break to repair |
| `And(q...)` | `All` of failed children | `AllHeld` for all children | repair all failures vs. break any holding child |
| `Or(q...)` | `Any` of failed branches | `AnyHeld` for every branch that held | repair any branch vs. break all holding branches |
| count below `min` | `CountLow`, including matches and rejected candidates | `CountHeld` when within bounds | add enough qualifying values vs. remove enough support |
| count above `max` | `CountHigh`, including excess values | `CountHeld` when within bounds | remove enough matches vs. add enough matches |
| universal encoding | `CountHigh` with per-value failures where applicable | `ForAllHeld` with every checked value | repair failing values vs. break a holding value/support |
| SPARQL/expression | `Opaque` with diagnostic | `Blocked` where a sound deletion proof is unavailable | explicit blocked reason, never an invented repair |
| positive recursion back-edge | ordinary finite failure if one exists | `Coinductive` | no finite deletive certificate is claimed |

The asymmetries in this table are part of the model. Do not introduce dummy
variants merely to make the two Rust enums look alike.

## 4. Evidence grammar and traversal

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
not an evidence node. Every such row now also exposes its exact
`evidence_kind`, linking it to `CountHeld` or `AllValuesHeld`. Structured
`EvidenceNode` and `RepairOrigin` values expose `EvidenceKind` directly; their
old snake-case `kind` strings are compatibility projections of that enum.

## 5. Identity and provenance

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
typed evidence kind. A joint synthetic node may cite several origins. The
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

## 6. Executable invariants

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

## 7. Performance boundaries

Optimization must preserve the identities and laws above. In particular:

- cache boolean conformance by judgment address;
- do not cache evidence by judgment address alone, because derivation context
  can change `reached_by` and `produced_by`;
- evaluate normalized requests once and perform authored fan-out afterward;
- prefer on-demand failure discovery plus pair explanation when satisfaction
  evidence for the entire coverage horizon is not required;
- measure evidence construction, progress construction, source fan-out,
  serialization, and emission separately;
- measure peak memory as well as elapsed time and output size;
- treat streaming serialization and context-aware evidence sharing as distinct
  experiments with semantic equivalence gates.

Every publication-facing optimization should record its hypothesis, semantic
test, benchmark command, corpus checksum, raw samples, machine metadata, and
negative result where the hypothesis fails.

## 8. Implementation sequence

1. **Done:** centralize evidence child traversal in `EvidenceNodeRef` and remove
   parallel recursive walkers.
2. **In progress:** add the duality and traversal law tests above.
3. **Done:** make `Failure` and `Satisfaction` canonical, retaining `Witness`
   and `SatTrace` as compatibility aliases.
4. **Done:** add typed evidence origins to synthesized repairs and expose them
   through Rust and Python.
5. **Done:** expose the exact `EvidenceKind` in Python through an exhaustive
   Rust conversion, and link legacy flattened summaries to it.
6. Implement streaming compact serialization and measure peak memory.
7. Prototype context-aware evidence sharing behind an experimental boundary;
   retain it only if the measured benefit justifies its additional identity.
