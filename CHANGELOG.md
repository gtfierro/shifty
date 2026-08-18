# Changelog

## Unreleased

### Added

- Added configuration-oriented shape maps to the C++ SDK through
  `PreparedValidator::shape_map(dataset, options)`. It returns typed
  `ShapeMap` / `Mapping` / `Binding` values with typed keys, RDF terms, paths,
  qualifiers, binding status, cardinality, authored names, rejected values,
  and per-value annotations. The backing C API exposes one direct shape-map
  operation and an opaque `ShiftyShapeMap`; evidence and property-witness
  handles are intentionally not part of the C or C++ surface.
- Added `shifty.shape_map()` / `shifty.ShapeMap` to the Python bindings: a
  ShEx-shapemap-style view one level above the evidence trees. Each selected
  `(shape, focus)` pair becomes a `Mapping` — a `collections.abc.Mapping` from
  a typed, hashable, pattern-matchable `Key` (`path` + `Qualifier` — `Cls`,
  `Const`, `Datatype`, or `ShapeRef`) to a `Binding`. Bound keys carry the
  values the data supplied as typed `Term`s (`Iri`/`Literal`/`BNode`, exact
  even on partially-conforming foci); unbound keys carry the witness subtree,
  the shortfall count, and near-miss candidates. `Binding` also exposes
  cardinality (`min`/`max`/`observed`/`expects_single`) and `severity`. Pass
  `name_path` (default `sh:name`) to carry the author's name for each slot,
  evaluated from the property shape's own node over the shapes graph, and
  `value_paths` to annotate each bound *value* from the data graph
  (`Binding.annotations`/`.annotated_values`, resolved lazily and batched).
  `ShapeMap.for_focus()` looks up every mapping for a focus node across
  shapes; `Mapping.value_map()` projects bound keys for application
  configuration. Added `Schema::sources` (`shifty-algebra`) to record the
  originating shapes-graph node for arena slots lowered from an RDF node, and
  `EvidenceSession.binding_names()` / `.shape_name_of()` / `.resolve_path()`
  (Python and the underlying Rust primitives) to support `name_path` and
  `value_paths`.
- Added `PreparedEvidenceValidator::explain_constraint()`: evidence for one
  focus against any *normalized* constraint id, not just a statement's top
  shape. Exposed in Python as `EvidenceSession.evidence_for()`. This is the
  drill-down for the passes a failing conjunction's witness elides — the
  run's `EvaluationProgress` says a child passed; this materializes why.
- Added `shape_name` to `StatementEvaluation`, `Failure`, and `Satisfaction`
  in the Python bindings: the statement's source shape IRI, when named.

- Added `PreparedEvidenceValidator::validate_conformance()`: the conformance-only
  counterpart of `validate()` over the same prepared snapshot, so evidence
  tracing can be measured against an otherwise identical execution.
- Added `shifty_engine::compact`, a lossless encoding of an `EvidenceRun` that
  hash-conses evidence nodes and RDF terms into shared tables and can elide the
  constraint catalog for consumers that already hold the schema.
  `compact_value`/`expand_value` encode and decode an already-serialized run
  without a typed round-trip.
- Added `PathBackend::contains()` for direct triple-existence checks.
- Added the compact encoding to the Python bindings:
  `EvidenceRun.to_compact_json()`, `EvidenceRun.to_compact_dict()`, and
  `shifty.expand_evidence()`.
- Added `benchmark/bench_evidence.sh`, `benchmark/summarize_evidence.py`, and
  `benchmark/analyze_evidence_size.py` covering evidence latency and size across
  the Brick and 223P corpora.
### Fixed

- Fixed quadratic evidence materialization. Certificates are now derived during
  the value traversal instead of re-probing `path_support` per candidate, which
  re-ran a class-hierarchy walk for every value. On a Brick model this cut
  evidence validation from 19.5 s to 0.38 s (8.6M path probes to none).

### Changed

- Which route a multi-route value cites in `PathSupport` now follows traversal
  order. Every cited triple still exists in the evaluation graph; validation
  verdicts, evidence structure, and failure content are unchanged.
- The C ABI version is now 4. Its surface only grew, but a `SHIFTY_ABI_VERSION`
  of 3 no longer matches: the C++ header checks for equality, so headers and the
  static library must be updated together.

## 0.3.0

### Added

- Added algebraic provenance to validation reasons:
  - `Reason.constraint`
  - `Reason.constraint_kind`
  - `Reason.constraint_id`
  - `Reason.statement_id`
- Added Python `Constraint` and `ConstraintKind` types for stable programmatic
  branching on algebraic operators such as cardinality, class membership,
  datatype/value type, node kind, conjunction, disjunction, and SPARQL.
- Added statement-level identity on Python algebra violations and repair
  witnesses so callers can join:
  `(focus_node, statement_id, constraint_id)`.
- Added `shifty_opt::normalize_with_mapping()` to preserve raw-statement to
  normalized-statement provenance across normalization deduplication.
- Added `python/examples/provenance.py` and expanded validation examples/docs.

### Changed

- `FocusWitness.statement_id` is now the normalized provenance statement id used
  for validation/repair correlation. `FocusWitness.statement` remains the raw
  repair-schema statement index for compatibility/debugging.
- `RepairSession.witnesses()` and `witnesses_for()` deduplicate repair witnesses
  that normalize to the same semantic `(focus, statement_id)` pair.
- Internal workspace dependency requirements now track the `0.3.0` release line.

### Fixed

- Fixed a panic and bad provenance when raw repair statements were indexed
  directly into the normalized schema after normalization deduplicated
  statements.

### Breaking

- Rust `shifty_engine::Reason` gained public fields. Downstream Rust code using
  struct literals may need to initialize the new fields.
