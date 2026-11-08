# Sessions and storage

`CompiledShapes` owns the products of compiling one shapes document.
`EvaluationSession` combines those products with one immutable data snapshot
and its inference and graph policies. Both live in `shifty-engine`.

Implementation: [compiled.rs](../crates/shifty-engine/src/compiled.rs),
[session.rs](../crates/shifty-engine/src/session.rs), and
[context.rs](../crates/shifty-engine/src/context.rs).

## Compilation and ownership

Compilation consumes the parser's `Loaded` source and performs:

1. Lowering and `require_valid()` admission.
2. Polarity-aware recursion analysis of the authored arena.
3. Normalization with source-to-normalized shape and statement maps, followed
   by recursion analysis of the executable arena.
4. Collection of canonical function definitions, access requirements, parsed
   query bodies, and ordered rule metadata with conservative dependencies.

Authored admission precedes normalization: an invalid recursive authored shape
is rejected even if normalization could erase its use. Every document-based
validation interface therefore admits the same schema.

| Resource | Owner | Lifetime |
| --- | --- | --- |
| Loaded source, authored and normalized schemas, provenance | `CompiledShapes` | Until the shapes change |
| Functions, access catalog, parsed query bodies, rule schedule | `CompiledShapes` | Until the shapes change |
| Physical validation plan and encoded source storage | Lazy products under `CompiledShapes` | Shared across sessions |
| Asserted data, inferred additions, graph views | `EvaluationSession` | One snapshot |
| Dataset indexes and prepared query caches | Session/prepared evaluator | One snapshot |
| Boolean memo and active recursion set | Evaluation operation | One scan or explanation |
| Findings and evidence trees | Returned result | Caller-controlled |

`CompiledShapes` is a cheap `Arc` clone and is `Send + Sync`. Session executor
state uses `Rc`/`RefCell` and remains thread-confined. Workers share compiled
shapes and create separate sessions. Operation-local evaluators avoid
self-referential borrowed structures; a scan does not promise to cache all
evidence for a later explanation.

## Session preparation

`CompiledShapes::session(data, options)` accepts `SessionData::Separate(Graph)`
or `SessionData::Embedded`. Separate input requires a nonempty shapes source;
embedded input explicitly takes initial data from that source. An empty shapes
graph does not implicitly select embedded mode.

`SessionOptions` fixes graph mode, inference, and engine policy for the snapshot.
Preparation admits unsupported-feature policy and performs inference when
enabled. Strict inference failures reject construction; best-effort diagnostics
remain available from the session.

Source encoding and physical validation planning are lazy. Active compiled
inference builds one indexed dataset, commits rule batches to it, and hands it
to the session. First validation moves it into `PreparedEvidenceValidator`.
Native and fallback SPARQL use that same dataset. Public `Graph` getters
materialize compatibility projections on demand. The storage layout is described below.

## Graph roles

Let `S` be the fixed shapes source and `E` the data after optional inference.

| Input | Mode | Focus candidates | Default read graph | Named `$shapesGraph` |
| --- | --- | --- | --- | --- |
| Separate | Data | E | E | S |
| Separate | Union | E | E union S | S |
| Separate | UnionAll | E union S | E union S | S |
| Embedded | Any | E | E | S |

Explicit `targetNode` terms can be selected even when absent from the candidate
graph. Focus discovery and evaluation reads are distinct roles.

Separate-input inference discovers foci in evolving data and reads data union
S, independently of validation graph mode. Embedded inference reads evolving
data. The named shapes graph always remains the compiled source: inferred
triples and data edits do not modify it or recompile shapes.

Separate documents have separate blank-node scopes even when labels collide.
Dataset encoding records aliases; session outputs map them to stable public
identities consistently across findings, reports, evidence, and inferred data.

## Evaluation products

The following methods return domain values directly on a constructed session:

| Method | Product |
| --- | --- |
| `validate` | Algebraic findings |
| `report` | Component-level W3C validation report |
| `evidence` | Failure or satisfaction evidence, optionally with progress |
| `conformance` | Logical verdict and normalized-pair counts without evidence |
| `find_failures` | Conformance scan plus selected-pair handles |
| `property_witnesses` | Source-oriented property bindings |

`FindingOptions` controls entry shapes, minimum severity, and sorting.
`EvidenceOptions` adds progress selection. `ConformanceOptions` selects entry
shapes but does not apply severity thresholds, so a conformance scan can fail
while a severity-filtered findings run conforms.

Entry-shape selection uses authored identities before normalized fan-out. CSE
must not make selection of one named shape select another.

`explain` and `explain_canonical` return `Result` because their `SelectedPair`
handle must belong to the exact session. The handle carries an owned snapshot
token; another session sharing the same compiled shapes still rejects it.
Handles and arena IDs are local identities, not portable serialized addresses.

## Edits and repair

`with_delta` patches asserted data, recomputes inference under the same policy,
and returns a new session sharing the compiled shapes. The old snapshot and its
results remain valid. Deleting the last asserted support removes a derived fact
when inference runs again; edits never mutate the shapes source.

In union mode, removing an overlapping data triple leaves its shapes copy
visible. Embedded reads use edited data directly, avoiding accidental
reintroduction from the original source.

`with_delta_and_inference` also accepts an inference setting. When it changes
from enabled to disabled, the patch is applied to evaluated data, retaining
previously inferred facts. With inference enabled it patches asserted data and
derives again. This distinction is part of the compatibility edit contract.

`session.gate(delta)` validates the whole baseline and candidate, then compares
failures. It inherits snapshot policy and does not inherit a UI's entry-shape
filter. The gate returns effects; the driver decides whether to apply them.

Python's sendable `RepairSession` retains compiled shapes and asserted data,
preparing baseline and candidate sessions for gating while releasing the GIL.
Its unsendable `EvidenceSession` can retain the prepared session. Thread
ownership therefore affects adapter preparation cost without requiring locks
in engine hot paths.

## Reports and diagnostics

W3C reporting borrows the prepared executor and graph context while walking
authored RDF component boundaries. It remains a separate source-oriented
reporting traversal; normalization maps alone cannot recover all original
paths, nested property ownership, messages, and component boundaries.

Compile errors cover invalid lowering and recursion. Session errors cover
empty separate shapes, unsupported-feature admission, and strict inference.
Explanation errors identify foreign handles. Ordinary nonconformance is a
result, and SPARQL execution diagnostics remain visible rather than becoming
silent success.

## Term identity and membership

`TermId` is a checked `u32` dictionary index. Source IDs form an immutable prefix;
session-local terms extend it. Query-only constants can be interned locally
without changing the RDF snapshot or shared source dictionary.

Equal RDF terms reuse IDs within a session. Separately parsed data blank nodes
that collide with source labels are alpha-renamed, preserving document scope.
Session output maps those identities back consistently. Local IDs cannot be
compared across sessions and are not persistent external identifiers.

Source triples are stored once. Data membership in source rows is represented
as all rows, an included subset, or all rows with exclusions. Additional data
triples live in local partitions. A triple shared by shapes and data has both
memberships; deleting its data membership leaves the shapes copy intact.

Union reads emit overlapping triples once. SPARQL solution multiplicity is a
separate query-level property and is preserved. Node domains are graph-scoped
subjects and objects, including literal objects; predicate-only and query-only
terms do not enter zero-length path or focus domains.

The named shapes graph reads the same source index. It remains available even
without optional indexes and never observes inference or data edits. Embedded
default views respect data exclusions instead of unioning the original source
back into edited data.

## Access catalog

Compilation records possible graph reads independently of data size and native
query admission. `AccessCatalog` associates authored statements, rules,
functions, and conservative source-reporting consumers with:

- Default/shapes graph requirements and known or wildcard predicates.
- Forward, reverse, membership, and open-scan probe modes.
- Node-domain sensitivity and incomplete analysis.
- Compilation-local query/path identities, function calls, and possible writes.

An unknown read is distinct from a proven empty read set. SPARQL AST analysis
also describes fallback queries. Variable predicates, dynamic calls, and
unhandled constructs retain conservative requirements. Source reporting has an
explicit conservative consumer rather than pretending its reads are absent.

Query and path identities use structural keys. Parsed query bodies can be
shared across scheduling, access analysis, and execution preparation. Static
SHACL substitutions and session term lowering still have their own context.
SPARQL paths retain bag semantics where required; SHACL path set identities
cannot be applied indiscriminately to SPARQL sequences and alternatives.

Rule scheduling dependencies remain a separate product: a predicate read set
alone does not describe graph scope, direction, bindings, or node domains.

## Primary storage and optional indexes

Every triple is retained in a predicate-partitioned PSO primary representation,
with sorted `(subject, object)` pairs inside each predicate. This supports exact
membership, predicate scans, and forward probes for a known predicate.

| Access pattern | Optional acceleration |
| --- | --- |
| Reverse probe for a known predicate | Per-predicate `(object, subject)` pairs |
| Subject probe with unknown predicate | General subject directory |
| Object probe with unknown predicate | General object directory |
| Repeated native path endpoint | Bounded reachability-result cache |

Compiled demand identifies candidates. Dataset size and observed probes inform
admission. Shared source and session indexes have independent byte budgets
(currently 64 MiB and 32 MiB). Source index slots use immutable `OnceLock`
products; session indexes use local cells. General directories and reverse
indexes do not replace the complete primary representation.

An unavailable or declined index leaves a correct scan fallback. Unknown demand
does not require every secondary index. `BaseOnly` and `AllIndexes` are internal
comparison policies; production uses `DemandDriven`. No ontology names or
benchmark-specific predicates appear in policy.

Published iterators borrow stable allocations. Lazy index construction must not
invalidate their slices; RDF mutation occurs only after query iterators have
been consumed.

## Commits and cache lifecycle

`extend_triples` commits new facts, merges affected predicate partitions, updates
instantiated indexes and statistics, advances the dataset revision, and clears
derived reachability results. A no-op batch is not a new commit. Inference
execution also invalidates data-dependent query/focus results at commit
boundaries. Parsed/static query products can survive when they do not depend
on changed data.

Rules with equal order read the same snapshot and publish their additions
together. Duplicate suppression uses the inference read view, including
shapes facts already visible there.

Validation selects its own graph view over the published storage. Inference
may read data union shapes while Data-mode validation reads only evaluated
data. Changing the default view clears view-sensitive caches; storage reuse
does not mean reuse of an incompatible read view.

Native repeated paths cache endpoint results by term, compiled reachability
step, closure kind, and graph selection. Admission is bounded to one million
result IDs. The engine does not materialize all-pairs path relations or SCC
closures. See [SPARQL execution](sparql-execution.md).
