# Changelog

## 0.5.0

The 0.5.0 release consolidates the three alpha releases. Install the Python
package with `pip install pyshifty==0.5.0`. There are no engine or API changes
since 0.5.0-alpha.3; the alpha entries below give the full change history.

### Highlights

- Reuse compiled shapes across data snapshots with Rust `CompiledShapes` and
  `EvaluationSession`, and get consistent schema admission from W3C reports,
  algebraic validation, and evidence.
- Inspect inference and validation with diagnostics, input telemetry, graph
  dumps, and the CLI's access-stage view. Python can write inferred triples
  back to a caller-owned `rdflib.Graph` with `in_place=True`.
- Preserve blank-node identities across separately parsed data and shapes,
  inference, reports, evidence, and repair. SPARQL rules may reuse existing
  blank nodes while fresh ones remain rejected.
- Improve the CLI's human-readable validation report and expose resolvable
  constraint definitions in JSON. Scripts consuming the text report should
  switch to `--format json`.

### Compatibility

- Rust inference now runs through `CompiledShapes::session`; the legacy
  free-function inference APIs and `InferenceOutcome` were removed.
- The C ABI version is 6. Rebuild C/C++ consumers against the 0.5.0 library
  and headers.
- Strict `on_unsupported="error"` now applies to inference that runs before
  validation, so previously ignored unsupported rules can raise an error.

## 0.5.0-alpha.3

Third alpha for 0.5.0. To try the Python package, run
`pip install pyshifty==0.5.0a3`.

### Fixed

- Separately parsed data and shapes blank nodes remain distinct when their
  labels match. Validation reports, algebra results, evidence, inferred graphs,
  and repair results expose stable identities; reports built from an
  `rdflib.Graph` reuse the caller's data blank nodes.
- `RepairSession.gate()` no longer reports the same blank-node violation as
  both fixed and introduced for a no-op or unrelated delta.
- SPARQL function calls correctly bind blank-node arguments, including nodes
  shared with the inference and validation inputs.

### Changed

- SPARQL rules can reuse existing data or shapes blank nodes while still
  rejecting fresh blank nodes that could prevent inference from terminating.
- Rust inference now runs through `CompiledShapes::session`; the legacy
  free-function inference APIs and `InferenceOutcome` have been removed.
- Session output relabels only colliding nodes, projects inferred data in one
  pass, and avoids a duplicate inference-delta copy. W3C reports are serialized
  only when requested, and shape-map path lookups reuse their combined graph.
- Release metadata and wheel smoke checks were tightened; the public guides
  and benchmark documentation were refreshed.

## 0.5.0-alpha.2

Second alpha for 0.5.0.
To try the Python package, run `pip install pyshifty==0.5.0a2`.
The three validation interfaces — W3C, algebra, and evidence — now share one
compilation boundary, so a schema they previously disagreed about is admitted
or rejected the same way by all of them. Two of those agreements are reached by
*rejecting* input that used to produce an answer; see **Changed** below.

### Added

- Added reusable Rust `CompiledShapes` and `EvaluationSession` APIs. Sessions
  share compiled shapes across data snapshots, expose validation, reports,
  evidence, diagnostics, and asserted-data edits, and reject evidence handles
  from another snapshot. Document-based Python, C++, CLI, and Wasm paths now
  use this compilation boundary.
- Added `--dump-data <PATH>` and `--dump-shapes <PATH>` to `validate`, writing
  the graphs it evaluated as Turtle to a path or to stdout with `-`. Neither is
  the file on disk: the data graph carries whatever SHACL-AF inference derived,
  and the shapes graph is every `--shapes` source merged, so a constraint that
  fails on a triple appearing in no input document could not be seen before.
- Added input telemetry to `--profile` on `validate` and `infer`: each
  `--shapes` and `--data` source is reported with the format that parsed it and
  the triples it contributed, plus the merged total (and any duplicates dropped)
  when a graph has several sources. `validate` also reports how many triples
  rule inference added, or that it was skipped. Until now nothing in a run
  described its own inputs, so a vacuous `conforms: true` — a shape whose target
  predicate never occurs in the data — was indistinguishable from a document
  that had not been read. The format reported is the one that succeeded, not the
  one the extension suggests: a literate-Turtle `.md` document reports `turtle`.
- Added `shifty inspect --stage access`, which prints the data-independent read
  demand a shapes document places on a graph: per consumer (statement, rule, or
  function) the predicates and probe directions it reads from the default and
  shapes graphs, the query and path identities it shares, and what it writes.
  This is what index selection is now driven from, so it is also how to see why
  a given index was or was not built. `--format json` is supported;
  `--format dot` is not.
- Added inference diagnostics to every validation result, not just `infer()`.
  Python `AlgebraResult.diagnostics` and `W3cResult.diagnostics`, Wasm
  `validate()` and `validateW3c()`, the C ABI's
  `shifty_validation_result_diagnostics_json`, and C++
  `ValidationResult::diagnostics_json()` all report the unsupported-feature and
  rule-execution diagnostics raised while validating. A run whose rules silently
  did nothing previously looked identical to one whose rules had nothing to do,
  unless the caller made a separate `infer()` call to find out.
- Added `shifty.ShaclDiagnosticWarning`. Python's `validate()` and
  `PreparedValidator.validate()` return pyshacl's
  `(conforms, report_graph, results_text)` tuple, which has nowhere to put a
  diagnostic, so they now warn once per diagnostic instead of dropping it — a
  skipped rule no longer reads as a clean `conforms=True`. Silence it with
  `warnings.filterwarnings("ignore", category=shifty.ShaclDiagnosticWarning)`,
  or use `validate_algebra()`, whose `.diagnostics` is a plain list. The tuple
  is unchanged.

### Changed

- `$shapesGraph` now always names the authored shapes source. In combined
  shapes/data input, triples derived later by inference remain in the data
  graph but no longer appear in `$shapesGraph`. With separate inputs, function
  definitions come from the compiled shapes source; only the low-level legacy
  inference API still discovers functions in the combined context graph.
- `RepairSession.gate()` evaluates the whole candidate snapshot with the
  session's inference policy. `RepairSession.advance()` continues to patch its
  materialized graph without rerunning inference.
- Updated the Python bindings to pyo3 0.29 (from 0.23). `requires-python` is
  unchanged at 3.9 and the extension is still built abi3. The `#[pyclass]`
  types no longer carry an automatic `FromPyObject`: pyo3 is making it opt-in,
  nothing in the crate extracted these types from Python, and they are values
  the bindings hand out rather than accept.
- The W3C report path now applies the same schema admission rule as algebraic
  validation and evidence. A schema with recursion through negation is rejected
  by all three; previously `validate()` returned a conformance verdict for a
  schema the other two interfaces refused as non-stratifiable, which is the more
  dangerous of the two answers because nothing said the result was unsound.
- SHACL functions are now compiled once alongside the schema and registered with
  every executor. A constraint calling a `sh:SPARQLFunction` previously passed
  through W3C validation and failed through algebra and evidence, where the
  reason given was that the function was unsupported.
- `on_unsupported` now reaches the inference that runs automatically before
  validation. `validate(..., on_unsupported="error")` and its variants
  previously ran that inference under the default lenient policy, so a rule
  calling an unsupported function could derive and write triples in a call that
  had asked to fail instead. Strict callers that relied on the lenient behavior
  will now see `ValueError: strict inference failed: …`.
- Validation and inference now share one indexed dataset across a compiled
  shapes document and select secondary indexes from compiled access demand and
  observed probes, under a byte budget, instead of building a fixed set. Every
  triple stays in a complete predicate-partitioned primary index and a declined
  index falls back to a correct scan, so this changes cost rather than answers.
  Against 0.4.4 on the recorded suites, Brick end-to-end geomean went from
  3311.6 ms to 2232.5 ms over 45 models and s223 from 2308.9 ms to 1383.6 ms
  over 19; `benchmark/shared-dataset-results.md` has the measurements and their
  limits.
- Bumped `SHIFTY_ABI_VERSION` to 6. The C ABI gained
  `shifty_validation_result_diagnostics_json` without a bump, and the C++
  header compares the header's constant against the library's
  `shifty_abi_version()` for exact equality — so a 0.5 header against a 0.4.4
  library reported a match and then failed to link, with nothing saying why.
  Both definitions now carry a comment pointing at the other.
- Published crates now carry the workspace README. `readme` was declared in
  `[workspace.package]` but no member inherited it, so every crates.io page was
  blank.

### Fixed

- Fixed `shifty validate --profile` printing no telemetry at all. The call that
  prints the collected summary was dropped in "Label the text report, and make
  JSON pointers resolve" (0.5.0-alpha.1); profiling was still enabled, and the
  data still collected, but never shown. `infer --profile` was unaffected.

## 0.5.0-alpha.1

Alpha release from [PR #22](https://github.com/gtfierro/shifty/pull/22).
To try the Python package, run `pip install pyshifty==0.5.0a1`.
The new Python `in_place` option is opt-in; existing calls retain their behavior.
The CLI's text report has a new layout, so scripts should consume JSON output.

### Fixed

- Fixed `sh:xone` reporting. It lowers to `⋁ᵢ (φᵢ ∧ ⋀_{j≠i} ¬φⱼ)`, and reported
  as that disjunction a node satisfying *two* alternatives — the usual way to
  fail a xone — was told that none were satisfied, the opposite of the finding.
  The rewrite is now recognized (`render::xone_alternatives`), so the message
  counts what actually holds and the constraint renders as
  `exactly one of (…)` rather than as its expansion.
- Fixed `sh:not` rendering a double negative when the negated shape is itself
  negative: `not (∄ p)` now reads `∃[1..] p`. A boolean combination is left as
  `not (a and b)`, where De Morgan would trade one clear form for a longer
  disjunction. The `sh:not` failure message is now the positive requirement
  instead of "negated shape unexpectedly held".
- Fixed typed literals rendering their datatype as an absolute IRI
  (`"10"^^<http://www.w3.org/2001/XMLSchema#integer>` → `"10"^^xsd:integer`),
  including numeric bounds inside a value type. A plain string keeps its
  implicit `xsd:string` unspelled.
- Fixed the notation key listing the inverse-path symbol for a report that used
  none: it matched the `^^` of a typed literal.
- Fixed validation messages that leaked internal arena slot labels such as
  `@257`. Constraint descriptions were cut at a fixed recursion depth and fell
  back to the slot id, which is meaningless outside a debugging dump and could
  elide the very term that distinguished two conjuncts. Descriptions now expand
  in full, stopping only at a genuinely self-referential shape (named as such)
  or a size cap (elided with an ellipsis).

### Added

- Added `in_place=True` to `infer()`, `validate()`, `validate_algebra()`,
  and `PreparedValidator.validate()` / `.validate_algebra()`: writes
  triples derived by SHACL-AF inference directly into a caller-owned
  `rdflib.Graph` passed as `data_graph`, instead of returning a separate
  copy. Only the inferred delta crosses back from Rust for in-place
  validation. `infer()` exposes that delta through the new
  `InferResult.inferred_ntriples` property. Triples derived about a blank
  node land on the blank node the caller's graph already holds.
- Added a top-level `shapes` map to `validate --format json`: the transitive
  closure of every reported constraint, keyed by the same ids the algebra's own
  `constraint_id` and `qualifier` fields use, so those pointers resolve inside
  the document. Previously a JSON consumer hit the same dead end a reader of the
  text report hit with `@257`. It is the closure rather than the whole arena
  deliberately — for the s223 shapes that is 19 slots against 2412, a 2.2x
  payload instead of 172x; `inspect --stage plan --format json` still dumps the
  arena in full. Each reason also gains `definition` and `definition_pretty`,
  and each violation `target` and `shape_name`.
- Added `Shape::child_shapes`, the shape's direct references including the
  `sh:filterShape` ids inside a node expression. `render`'s reachability walk
  now uses it, so a shape reachable only through an expression no longer drops
  out of a schema dump.
- Added `Reason.observed_count`: for a cardinality constraint, how many values
  along the path satisfied the qualifier. The bound is already in the constraint
  algebra, so this is the one number a report needs that the algebra does not
  carry — a renderer can now state the shortfall without parsing `message`.
  Exposed in Python as `Reason.observed_count`.
- Added an indented layout for constraint descriptions, for the nested ones that
  are unreadable on a single line. `render::describe_shape_pretty` breaks and
  indents by nesting depth, and returns the one-line form byte-identical when it
  already fits, so callers can use it unconditionally. Surfaced as
  `Constraint.definition_pretty` and `RepairSession.describe_shape_pretty` in
  Python, and printed by `shifty validate` as a `constraint:` block under a
  reason whose description does not fit. `Reason.message` and the
  `sh:resultMessage` literal stay single-line: consumers embed them mid-line and
  serialize them as RDF.

### Changed

- An `rdflib.Graph` input is now serialized as its namespace declarations
  followed by an N-Triples body. This is valid Turtle and carries the same
  bindings SHACL-SPARQL resolves prefixed names against, while also giving
  every blank node an explicit label; it is cheaper to produce than
  rdflib's Turtle serializer, which groups triples by subject, counts blank
  node references to decide what to nest, and compacts every IRI against
  the namespace manager. Namespace bindings and blank node labels that RDF
  syntax cannot spell are handled rather than emitted: an undeclarable
  binding is skipped, and such a blank node is written under a reversible
  encoding, so no name a caller's graph happens to carry can make the
  document unparseable.
- `validate()` and `validate_algebra()` discard the inferred delta after
  validation unless `in_place=True` needs it for write-back, and do not
  render an unused N-Triples string.
- `shifty validate --format text` now reports *findings* rather than violations:
  reasons that fail the same statement with the same rendered explanation are
  grouped, the explanation printed once, and the focus nodes listed under it,
  each with the value node that failed on it. On a sample s223 run this took a
  report from 1230 lines to 161 — the same constraint failing on 59 nodes is one
  thing wrong with the graph, and repeating its explanation 59 times buried the
  two other things that were also wrong. The summary line counts both
  (`61 violations in 3 findings`). The unit is one reason rather than one
  violation so that each grouped node carries exactly one value node, which can
  be named in the heading instead of left as a bare parenthesised IRI.
- The generated message is labelled `failure` rather than `details`, and keeps
  that label whether or not the shape carried an `sh:message`. It previously
  appeared as `message` when there was no author text and `details` when there
  was, so one field had two names and neither said where it came from. It is
  also suppressed when it is exactly ``must satisfy `<the requirement>` ``.
- A count over a `⊤` qualifier drops the vacuous `. any node` clause:
  `∃[1..] ex:p` rather than `∃[1..] ex:p . any node`, matching `∄ p`.
- `shifty validate --format text` now prints labelled fields — `focus node`,
  `value node`, `path`, `found`, `requirement` — instead of packing a reason
  onto one line. The two nodes in a reason are what a first-time reader
  confuses: the focus node was selected for checking, the value node was reached
  from it along the path and is what failed. Unlabelled, the value node reads as
  the subject. A report that uses `∀`/`∃`/`∄` now also ends with a key glossing
  only the symbols it actually used. Scripts should read `--format json`.
- `shifty validate` now spells data-graph nodes using the data document's own
  `@prefix` declarations, layered over the shapes document's. A focus node in the
  s223 sample goes from 89 characters to 37, and it appears twice per reason.
  This is the text report only. Node identity in the Python API
  (`Violation.focus_node`, `Reason.value`) stays absolute: callers match those
  against IRIs they hold, and a compacted form is not resolvable without the
  prefix table beside it.
- `shifty validate` no longer repeats a cardinality reason's generated message
  inline when it also prints the constraint block. The block says everything the
  message did — the bound is in the constraint, the count is in its label — so
  the two together restated a 300-character sentence three lines above its own
  readable form. Reasons that are not cardinality failures keep their message,
  where the prose is the finding rather than a restatement.
- Validation messages now state a `∃[..0] π . φ` count as the universal it is,
  `∀ π . ¬φ`, inverting the qualifier. The lowered form of a universal carries a
  negated qualifier, so the old rendering presented a double negative: what read
  as two stacked "zero or fewer" quantifiers actually says "every value along
  the path *is* an instance of C".
- Descriptions now bracket nested `and`/`or` groups, so a message parses
  unambiguously without knowing the connectives' precedence.
- Report messages, paths, and rendered targets now compact IRIs using the
  `@prefix` declarations of the document the shapes were loaded from, not just
  the five well-known W3C namespaces. `Schema` and `PhysicalPlan` carry those
  declarations as display metadata; new `*_in`/`*_px` rendering entry points
  take them, and the existing prefix-less functions are unchanged.

## 0.4.4

### Added

- Added complete type declarations for the native Python extension, so the
  existing `py.typed` marker now provides useful validation, evidence,
  inference, and repair types to editors and static type checkers.
- Added locked Ruff formatting/linting and `ty` type-checking gates for Python
  development, pull requests, and releases.

### Changed

- Stripped debug symbols from Python extension artifacts while retaining them
  in the workspace release profile for Rust profiling. The Linux wheel is now
  approximately 3.7 MB instead of 197 MB.
- Applied Ruff formatting and import cleanup across the Python package, tests,
  examples, and benchmarks.

## 0.4.3

### Fixed

- Fixed SHACL-SPARQL custom SELECT validators so `?value` is a result variable,
  rather than being incorrectly treated as pre-bound. This accepts DASH property
  validators such as `SELECT $this ($this AS ?value)` and reports that binding
  as `sh:value`.
- Aligned `$PATH` handling with SHACL: it is substituted for SELECT property
  validators, while `$value` is pre-bound only for ASK validators.

## 0.4.2

### Fixed

- Fixed Python shape-map projection for a lone property shape that combines
  `sh:class` or `sh:datatype` with cardinality. It now emits one named binding,
  places conforming values on that binding, and reports type-rejected values in
  `rejected_values`.
- Preserved authored property boundaries in `ShapeMap.from_run(run)` without an
  accompanying session, and corrected mixed qualified/unqualified-count value
  projection and recovered literal rendering.

## 0.4.1

### Fixed

- Fixed Python shape-map slot names for node shapes with exactly one property
  shape. `name_path` now follows the transparent authored wrapper to the
  property shape instead of losing its `sh:name` when conjunction lowering
  elides the sole child.
- Fixed extraction from optional `sh:qualifiedValueShape` slots. Although an
  unbounded qualified count correctly normalizes to a vacuous constraint for
  validation, `Binding.values` now returns the property values that satisfy
  the qualified value shape.
- Fixed the default `name_path="sh:name"` for `rdflib.Graph` inputs whose
  N-Triples serialization has no namespace declarations. `sh:` is now a
  standard fallback for property-path resolution while explicit document
  bindings continue to take precedence.
- Fixed SHACL-SPARQL constraints and SPARQL rules supplied as an
  `rdflib.Graph`. Graph inputs now serialize as Turtle so query prefix
  declarations survive; malformed or unresolved query prefixes now raise an
  invalid-shapes-graph error instead of silently dropping the constraint or
  rule.
- Fixed ambiguous string graph inputs. Long or multiline Turtle is no longer
  probed as a filesystem path, and a missing filename with a recognized RDF
  suffix now raises `FileNotFoundError` instead of a misleading Turtle syntax
  error.
- Made the same string-input policy apply to every list/tuple member, including
  directories, missing RDF filenames, URLs, and inline Turtle.
- Made invalid shapes-graph diagnostics fatal in the Python, CLI, C++, and
  WASM APIs; malformed SPARQL can no longer be lowered as an absent constraint.
- Brought C++ shape maps in line with Python for singleton-property names and
  optional qualified-slot value extraction.

## 0.4.0

### Added

- Added `EvidenceNodeRef::children()`, the common immediate-child relation for
  failure and satisfaction evidence. `Evidence::walk()` now derives its
  pre-order traversal from this shared grammar, including polarity crossings at
  negation and qualified counts.
- Added the typed `EvidenceKind` discriminant and made `Failure` and
  `Satisfaction` the canonical Rust evidence enum definitions. `Witness` and
  `SatTrace` remain source-compatible aliases for repair callers. Python now
  exposes the same exhaustive `EvidenceKind` on `EvidenceNode`, `RepairOrigin`,
  `WitnessAtom`, and `SatAtom`; legacy kind strings and flattened
  `WitnessKind`/`SatKind` categories remain available.
- Added `synthesize_with_origins()` / `synthesize_focus_with_origins()`. Their
  `SynthesizedRepair` links every retained repair node to the exact typed
  evidence occurrence that justified it: statement, child-index path,
  constraint, judgment node, polarity, and evidence kind. Python `RepairTree`
  exposes the same links through `root_id` and `origins()`.
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
  originating shapes-graph node for arena slots lowered from an RDF node.
  Shape maps use internal evidence-session operations to support `name_path`
  and `value_paths` without exposing that plumbing on the public session API.
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
- Added per-focus projections over an `EvidenceRun` in the Python bindings.
  `results_for(focus)`, `failures_for(focus)`, and `satisfactions_for(focus)`
  answer from a focus index rather than a scan over every statement;
  `failure_for(focus, statement=None)` and `satisfaction_for(...)` are strict
  lookups that raise on a miss and on an ambiguous match instead of guessing.
- Exposed the on-demand evidence API in Python, which until now was Rust-only
  even though the performance guide recommends it: `EvidenceSession` gains
  `validate_conformance()`, `find_failures()`, `explain(pair)`,
  `explain_canonical(pair)`, and `constraints()`, with new `ConformanceRun` and
  `SelectedPair` types. `explain` returns an `EvidenceRun` holding just that
  pair, so every projection works on it. `SelectedPair` names its
  `normalized_statement` and `source_statements` separately rather than
  carrying a bare `statement`, which elsewhere in the API means an authored id.
  `constraints()` makes `to_compact_json(include_catalog=False)` usable without
  materializing a full run to obtain a catalog.
- Added `PreparedEvidenceValidator::source_statements()`, the authored
  statements that normalize to a given normalized statement.
- Added `EvidenceSession.revalidate(delta, infer=None)`: the run `validate()`
  would produce over `G ⊕ ΔG`, so an evidence-driven driver can check a proposed
  edit without building a second session. Pure — the session keeps its own
  snapshot. `infer` re-runs SHACL-AF rules over the patched graph and defaults
  to the session's own setting; with inference on the rules re-run over the
  pre-inference graph, so a deletion takes its derivations with it rather than
  stranding them.
- Extended `MissingObligation` with the `node` the deficit is about, the `path`
  its values were counted along, and the `qualifier` each counted value must
  satisfy, so a cardinality deficit describes the edge that would close it
  without a caller reading `explain()`. In Python `qualifier` is a structured
  `Constraint` and `path` is rendered in the spelling `values_for_path` accepts.
  `MissingObligation` is a computed projection, so no serialized run changes.
- Added shape identity and shape-scoped projections to the Python evidence API.
  `StatementEvaluation`, `Failure`, and `Satisfaction` expose `shape_iri` (`None`
  for a shape written as a blank node), and an `EvidenceRun` answers
  `covered_shapes()`, `results_for_shape(iri)`, `failures_for_shape(iri)`, and
  `satisfactions_for_shape(iri)` from a shape index. An IRI naming no shape in
  the schema raises rather than returning empty; a named shape the run holds no
  statements for projects empty.
- Added `Evidence::matched_values_by_path()` and `Evidence::values_for_path()`,
  which read matched values per path from the structured match records, exposed
  in Python as `values_for_path(path)` on both evidence polarities.
- Added `benchmark/bench_evidence.sh`, `benchmark/summarize_evidence.py`, and
  `benchmark/analyze_evidence_size.py` covering evidence latency and size across
  the Brick and 223P corpora.

### Deprecated

- Python `FocusWitness` and `FocusSatisfaction` remain available as
  warning-producing aliases for `Failure` and `Satisfaction`. The old names
  were part of the 0.3.0 release and will be removed in 1.0.

### Fixed

- Fixed scoped on-demand explanation fanning a normalized failure back out to
  authored statements the caller did not select. `SelectedPair` now retains
  the exact selected source-statement ids and `explain` honors them.
- Fixed direct node/sub-shape repairs exposing `usize::MAX` as a statement
  provenance id. `EvidenceOrigin.statement` / Python
  `RepairOrigin.statement_id` are now optional and are absent for such roots.
- Fixed compact JSON expansion silently replacing dangling or forward table
  references with `null`; malformed references now return `InvalidReference`.
- Fixed `shape_names` / `--shape` scoping silently dropping shapes that
  normalization collapsed, and doing so *nondeterministically*. `Schema.names`
  held one name per arena slot, so when common-subexpression elimination merged
  two named shapes stating the same constraint, one name was overwritten and
  which one survived depended on hash iteration order. Scoping by the lost name
  validated nothing; scoping by the surviving one pulled in the other shape's
  statements as well. The same table backs `shape_id_for_iri`, so
  `witnesses_for`/`satisfactions_for` could fail to find a real shape from one
  run to the next. A slot now carries every authored name that reached it,
  sorted, with `Schema::name_of` for display and `Schema::names_of` for
  matching; authored statements are filtered by their own names, so a scoped run
  contains exactly the statements asked for.
- Fixed quadratic evidence materialization. Certificates are now derived during
  the value traversal instead of re-probing `path_support` per candidate, which
  re-ran a class-hierarchy walk for every value. On a Brick model this cut
  evidence validation from 19.5 s to 0.38 s (8.6M path probes to none).

### Changed

- The Python `shape_map` convenience function now follows the other one-shot
  operations and takes data first: `shape_map(data_graph, shacl_graph=None)`.
  Omitting the shapes argument uses one combined graph for both roles.
- `EvidenceSession.evidence_for` now returns a typed `EvidenceNode` instead
  of a raw dictionary. Shape-map-only graph traversal and source-provenance
  helpers are no longer part of the public `EvidenceSession` interface.
- Conformance-only `validate_conformance` and `find_failures` now take
  `ConformanceOptions`, which exposes only entry-shape selection. The old
  `ValidationOptions` parameter suggested that severity and result sorting were
  honored even though a no-evidence scan cannot implement either.
- Which route a multi-route value cites in `PathSupport` now follows traversal
  order. Every cited triple still exists in the evaluation graph; validation
  verdicts, evidence structure, and failure content are unchanged.
- The C ABI version is now 4. Its surface only grew, but a `SHIFTY_ABI_VERSION`
  of 3 no longer matches: the C++ header checks for equality, so headers and the
  static library must be updated together.

### Breaking

- Validation rejects an explicitly supplied zero-triple shapes graph instead
  of reporting vacuous conformance. Omit the Python shapes argument or pass
  `None` to use shapes embedded in the data graph. Inference continues to
  accept an empty rules graph.

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
