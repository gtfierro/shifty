# Changelog

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
