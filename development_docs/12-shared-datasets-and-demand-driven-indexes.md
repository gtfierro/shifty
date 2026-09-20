# 12 — Shared datasets and demand-driven indexes

Status: proposed continuation of `feature/compiled-shapes-sessions`, based on
the implementation at `c6d0b1d`, 2026-09-19. This document is an implementation
plan; the proposed storage and planning types below do not exist yet.

## 1. Outcome and scope

Keep the public `CompiledShapes` / `EvaluationSession` model from
[11](11-compiled-shapes-and-sessions.md). Complete its ownership model inside
the engine:

- Reuse shapes storage and shapes-derived analysis across sessions.
- Give native execution and Spareval fallback access to the same dataset.
- Express data, shapes, and their union as graph views.
- Carry inference storage into validation instead of rebuilding it.
- Use compiled access requirements, dataset statistics, and observed work to
  decide which additional indexes to construct.

The complete graph stays available. Selecting an index must never remove
triples from the queryable dataset or change the supported query language.
The improvements must apply to workloads described by their access patterns,
without ontology names or benchmark-specific predicates in planning policy.

This plan develops the index-demand direction in
[05](05-sparql-execution.md#path-demand-and-index-planning). It supersedes the
internal storage paragraph in document 11 that gives inference a mutable
Oxigraph Store and makes `SparqlExecutor` own the frozen dataset. It preserves
document 11's public APIs, graph-role table, admission, provenance, feature
policy, snapshot semantics, and thread-confinement requirements.

Full W3C reporter unification, new SPARQL features, incremental truth
maintenance after deletions, persistent compiled artifacts, and all-pairs path
materialization are separate work. The metadata needed to support them should
be retained where inexpensive; this plan does not make them prerequisites.

| Phase | Reviewable outcome |
| --- | --- |
| 0 | Baselines, work counters, and graph-semantics regression coverage |
| 1 | Compiled access catalog and reusable query/path identities |
| 2 | One dataset for inference's native and fallback queries; no full-data Store |
| 3 | Shared source storage, explicit graph views, and inference-to-validation reuse |
| 4 | Complete primary storage with selective secondary indexes |
| 5 | All consumers migrated, inspection output, and full benchmark acceptance |

## 2. Current implementation and redundant work

| Current representation | Current use | Proposed treatment |
| --- | --- | --- |
| Authored `Loaded` shapes | Parsing, inspection, report metadata, compatibility | Retain under `CompiledShapes`; share an encoded source representation lazily |
| Asserted data | Input snapshot and the starting point for edits | Preserve its identity independently of inferred facts |
| Evaluated data `Graph` | Targets, returned data, validation consumers | Replace internal consumers with views; retain a lazy compatibility projection |
| Materialized data/shapes union | Inference paths, conditions, function lookup | Replace reads with a union view and compiled function lookup |
| Mutable Oxigraph Store | Inference SPARQL and duplicate checks | Remove from the session execution path after differential verification |
| Inference `FrozenIndexedDataset` | Native SPARQL rule execution | Become the shared dataset used by both evaluators |
| Validation `FrozenIndexedDataset` | Validation, reporting, evidence | Reuse inference's storage, or build it on first demand without inference |

In `infer.rs`, inferred batches currently update evaluated data, the union
graph, an Oxigraph Store, and an optional indexed dataset. `sparql.rs` already
executes validation fallback queries with `on_queryable_dataset`. The same
dependency API supports CONSTRUCT; a Store is not required to use that query
evaluator. Inference still explicitly calls `on_store` in its CONSTRUCT paths.

The indexed dataset currently encodes RDF terms as `u32` IDs and builds SPO,
POS, and OSP arrays for every default-graph triple. Named graphs use separate
SPO-sorted arrays but currently answer patterns with linear filtering. Committed
inference batches append to and sort each complete default-graph array. At the
end of inference, the index is dropped; validation later constructs another.

The diagnostic experiment that motivated this plan removed named-shapes
availability in an isolated build. Median inference-session construction fell
from approximately 1,047 to 720 ms for Brick and 581 to 377 ms for s223. That
experiment establishes substantial preparation cost; disabling a graph is not
a correct optimization. These two cases do not establish performance for the
whole benchmark suite, nor isolate all differences from `v0.5.0-alpha.1`.

## 3. Semantic invariants

Let `S` be the fixed shapes source, `D` asserted data, and `E` evaluated data.

| Input / operation | Focus candidates | Default read graph | Named shapes graph |
| --- | --- | --- | --- |
| Separate inference | Evolving data | Evolving data union S | S |
| Separate validation, Data | E | E | S |
| Separate validation, Union | E | E union S | S |
| Separate validation, UnionAll | E union S | E union S | S |
| Embedded inference / validation | Evolving data / E | Evolving data / E | S |

Explicit `targetNode` retains its existing behavior for terms absent from the
graph. Focus candidates and evaluation reads must remain separate inputs.

Required invariants:

1. Admission, authored and normalized stratification, unsupported-feature
   handling, diagnostics, and source provenance remain centralized.
2. Each logical graph is a set of triples. Union views deduplicate overlap;
   query solution multiplicity is preserved according to SPARQL semantics.
3. `$shapesGraph` always exposes S. Inference and data edits never mutate it.
   Graph identity, existence, and variable-graph queries work even when no
   optional named-graph index has been built.
4. Rule groups with equal order read the same snapshot. Their additions become
   visible together before the next order group, preserving the existing
   fixpoint and scheduling behavior. Preserve existing duplicate suppression
   against the inference read graph, including facts already present in S.
5. `with_delta` edits asserted data and recomputes inference. It must not keep
   conclusions whose support was deleted. The old session stays usable.
6. Embedded edits may remove a triple from data while the same triple remains
   in the original named shapes graph. An embedded default view must not
   reintroduce that triple by automatically unioning the source back in.
7. SHACL value sets, SPARQL bags, path endpoint semantics, graph node domains,
   blank-node identity, and diagnostics survive the storage change.
8. Performance policy affects preparation and execution cost only. Missing
   analysis, exhausted budgets, and missing indexes have correct scan/traversal
   fallbacks.

## 4. Ownership and internal interfaces

### 4.1 Owners

```text
CompiledShapes (Send + Sync)
  authored source, schemas, provenance, functions, rule schedule
  access catalog: QueryId, PathId, read requirements, capabilities
  lazily encoded source storage and reusable immutable source indexes

SessionBuilder (private, mutable)
  asserted-data membership
  inferred additions
  session term extension and dataset indexes
  query execution state and committed dataset revision

EvaluationSession (published immutable RDF snapshot)
  shared CompiledShapes
  asserted/evaluated dataset memberships and graph views
  reusable indexes, statistics, and thread-confined query caches
  lazy Graph projections required by compatibility APIs
```

Mutable query caches do not change the published RDF snapshot. Source storage
contains no `Rc` or `RefCell`; session state keeps the current thread confinement.
Use `OnceLock` for immutable compiled products and `OnceCell`/`RefCell` for
session-local lazy products. Do not add a global term interner or hot-path locks.

### 4.2 Interfaces

Use one graph-view implementation beneath these consumers:

- `PathBackend`: forward/reverse adjacency, outgoing predicates, membership.
- An internal focus interface: subjects/objects for a predicate, node-domain
  enumeration and membership. Replace current `&Graph` requirements in target
  enumeration rather than constructing a Graph to satisfy them.
- `QueryableDataset`: native and fallback SPARQL read identical graph views.
- Native ID-based scans and path traversal: use the same storage, IDs, and
  graph selector as the fallback adapter.

`SparqlExecutor` should own preparation/plan caches and function policy. Supply
the dataset/view when executing a query; it should not own an independent
dataset or Store. Prefer short-lived borrows during each operation over a
self-referential executor/session structure.

The builder consumes all query iterators and evaluation borrows before committing
a batch. A builder method is the single place that changes data membership,
updates existing indexes and statistics, and invalidates derived results.
Publishing the session moves these allocations; it does not re-encode the graph.

Keep current public `Graph` getters and result types. Materialize a requested
data projection once in a session-local cell; share it through existing Arc
getters. A compatibility export must not allocate a shapes union unless that
specific API promises one. Public legacy functions can use transient adapters
while retaining their documented discovery and graph semantics.

### 4.3 Code boundaries and concrete ownership

Keep the existing crates. Suggested implementation locations:

| Location | Responsibility |
| --- | --- |
| `shifty-opt/src/access.rs` | Access descriptions, AST traversal, consumer/query/path identities |
| `shifty-opt/src/rule_deps.rs` | Scheduling dependencies, reusing common read analysis where sound |
| `shifty-engine/src/compiled.rs` | Compiled catalog, source dictionary/storage/index owner |
| Private `shifty-engine/src/dataset/` module | Dictionaries, memberships, views, index policy, mutable builder |
| `shifty-engine/src/frozen.rs` | Compatibility facade over the new representation during migration |
| `sparql.rs`, `native_exec.rs`, `path_plan.rs` | Query preparation and execution over explicit dataset views |
| `infer.rs` | Rule scheduling and commits through the builder |
| `session.rs`, `context.rs`, `evidence.rs` | Session publication, graph-role selection, and prepared consumers |
| `profile.rs` and benchmark examples/scripts | Work counters, cost attribution, and comparison harness |

The builder owns mutable session storage directly. On publication, move it into
an `Rc<SessionDataset>` shared by the session and prepared consumers. Create
short-lived borrowed `DatasetView` values for evaluation; prepared consumers
retain the dataset handle and view descriptor, never a reference into the
enclosing session. Source storage is separately shared with `Arc`. This keeps
the immutable compiled owner thread-safe and avoids changing the existing
thread-confined session contract. Retain the public `FrozenIndexedDataset`
constructor surface through adapters while its internals migrate.

## 5. Shared term identity and graph membership

### 5.1 Term dictionary

Choose a two-part dictionary:

1. An immutable source dictionary under `CompiledShapes`, assigning IDs
   `0..source_term_count` to terms present in S.
2. A session dictionary extension for other data terms, inferred terms, and
   query constants. Look up source terms first so equal RDF terms always have
   equal IDs inside a session.

The source dictionary is built once on first encoded-dataset demand. Keep
query-only constants out of the source dictionary; query preparation must not
mutate shared shapes storage. Preserve the existing term equality and document
blank-node scoping established by the loaders; lexical ID sharing must not
merge unrelated blank nodes from separate source documents.

IDs remain stable for a builder and its published snapshot. `TermId` remains
`u32` initially, with checked capacity handling at the allocation boundary.
Term IDs and row locations are internal, not persistent external identifiers.
Source IDs can be reused between sessions of the same compilation; local IDs
cannot be compared across sessions. Cache keys include owner/snapshot identity.
A later session may inherit an immutable local dictionary prefix, but copying
only a data-sized local dictionary is an acceptable first implementation.

### 5.2 Physical partitions and logical membership

Store source triples once in source partitions. Session data can refer to
source triples and store additional triples in session partitions. Represent
source participation in asserted data explicitly:

- Separate input: a subset of source rows for overlapping asserted triples,
  plus local rows.
- Embedded input: all source rows initially participate in asserted data.
  An all-members representation plus exclusions avoids copying every row ID.
- Inference: additions change evaluated membership, never source membership.

Source row IDs are stable because source partitions are immutable. A triple
shared by S and D retains both memberships. Removing its data membership leaves
its source membership intact. Union scans emit it once, while scans of either
individual graph remain correct. Query deduplication does not replace this
storage-level set union.

Do not carry row offsets into caches across session-partition compaction; use
term/triple identity or tag those caches with the relevant revision. For the
first version, edits may rebuild session-sized membership and local storage;
they must reuse the source dictionary and source indexes.

Maintain graph-scoped node domains: subjects and objects, including literal
objects, with existing semantics for explicit constants. Predicate-only or
query-only terms must not accidentally enter focus enumeration or free-endpoint
path domains. Zero-length paths make this important even when a query appears
to mention only one predicate.

## 6. Access catalog: compile once, choose indexes per dataset

### 6.1 Static descriptions

Add data-independent analysis in `shifty-opt`, owned by `CompiledShapes`.
Suggested internal vocabulary:

```rust,ignore
struct AccessCatalog {
    queries: Vec<QueryAccess>,
    paths: Vec<PathAccess>,
    consumers: Vec<ConsumerAccess>,
}

struct AccessRequirement {
    scope: ReadScope, // data, shapes, default evaluation view, or unknown
    predicates: PredicateDemand, // known set or any predicate
    probes: ProbeModes, // bound S/P/O, forward, reverse, membership, open scan
    reads_node_domain: bool,
    coverage: AnalysisCoverage, // complete or conservative/unknown
}
```

These are sketches, not a new public API. An empty proven read set and unknown
reads must be distinguishable. Compile-time demand is a description of possible
access; expected frequency comes from estimates or observations and should be
labeled accordingly.

Record requirements from:

- Authored and normalized constraints that public operations can evaluate,
  including closed shapes, logical branches, and nested property paths.
- Targets and their actual planned access direction. A forward class path may
  be executed backward from a known class.
- Rule targets, guards, node expressions, and SPARQL WHERE clauses.
- SPARQL constraints, custom components, targets, and callable functions,
  including transitive function-call dependencies.
- Source-driven report/property operations still outside normalized execution.
  Mark coverage conservatively until their reads have an explicit description.

Analyze the parsed SPARQL AST independently of native-lowering success. A
fallback query can still expose useful constant-predicate access. Variable
predicates/graphs, negated property sets, dynamic calls, domain-sensitive paths,
and unhandled constructs produce conservative requirements. Parse or capability
analysis here must not change existing admission/diagnostic behavior.

Reuse the traversal behind `rule_deps.rs`, but keep scheduling dependencies and
index requirements as distinct products: a predicate read set lacks graph,
direction, binding, and node-domain information. Preserve conservative rule
scheduling while extending the analysis. Do not infer requirements from regexes
or from queries that happen to lower to the native subset.

### 6.2 Query and path identity

Assign compilation-local `QueryId` and `PathId` values with full-key equality;
a diagnostic fingerprint alone is not a collision-safe cache identity.

Retain canonical parsed query templates and resolved prefixes/base where their
types permit immutable sharing. Account for static SHACL substitutions, shape
and component bindings, function environment, and graph policy in instantiated
query keys. Keep `$this` dynamic. Dataset statistics and term-ID lowering remain
session-specific. Share definition parsing across scheduling, access analysis,
inspection, and execution preparation instead of parsing the same body anew.

This compilation work must stay proportionate: start with cheap structural
access analysis and cache parsed bodies already needed by compilation. Make
additional operation-specific query preparation lazy. Measure catalog build
time separately so index selection does not recreate the eager-preparation
regression in a different form.

Path identity records operator semantics, including zero/one/many repetition,
direction, and whether a relation has set or bag behavior. Do not normalize
SPARQL fixed sequences/alternatives using SHACL set identities. Their underlying
edge access can be shared while their solution multiplicity remains distinct.
Graph scope and snapshot revision belong in index/cache keys as applicable.

### 6.3 Ownership of future-ready metadata

| Information retained now | Immediate use | Enables later |
| --- | --- | --- |
| Consumer-to-query/path provenance | Explain why an index exists | Cost attribution to authored shapes and rules |
| Graph-scoped read dependencies and domain sensitivity | Conservative invalidation | Selective invalidation and affected-focus discovery |
| Possible rule writes, including unknown predicate writes | Changed-partition accounting | Better scheduling and write/read interaction analysis |
| Endpoint binding modes and static constants | Direction selection | Join selection and batched probes |
| Canonical function registry and call graph | Consistent calls and access analysis | Safe function-result caching for proven pure functions |
| Predicate cardinalities and demand-triggered degree estimates | Index benefit estimates | Skew-aware join planning |
| Actual probes, scanned rows, cache hits, build bytes/time | Runtime promotion and diagnostics | Better cost calibration across workloads |
| Monotonic dataset revisions and committed deltas | Correct cache lifecycle | Incremental maintenance where separately proven sound |
| Authored-to-normalized provenance | Existing report/evidence behavior | Operation-specific preparation and reporting improvements |

Keep uncertainty explicit. Do not label unknown functions pure or unknown rules
monotone. Avoid building degree distributions or large per-query counters when
there is no consumer for them.

## 7. Index design and policy

### 7.1 Complete base representation

The initial migration keeps the existing SPO/POS/OSP representation so removal
of duplicate backends can be measured separately. The target selective layout
is predicate partitions with sorted `(subject, object)` pairs: one complete
primary PSO representation, a predicate directory, and compact graph-membership
information. It provides exact membership, predicate scans, and forward probes
for a known predicate.

Always retain every triple and a correct scan implementation for every pattern.
Also retain the node-domain information required by targets and path semantics.
One full primary representation is mandatory; additional acceleration is
selective. The retained authored `Loaded.graph` remains an explicit compatibility
and metadata cost during this migration. It is shared once, not rebuilt per
session; removing it requires a separate public/source-consumer decision.

### 7.2 Additional indexes

| Demand | Candidate acceleration |
| --- | --- |
| Reverse lookup for predicate p | Per-predicate `(object, subject)` pairs |
| Frequent subject scans with unknown predicate; closed shapes | Subject directory across predicate partitions |
| Frequent object scans with unknown predicate | Object directory across predicate partitions |
| Repeated path from the same endpoint | Bounded endpoint-result cache |
| Repeated path from many endpoints | Future budgeted path relation or SCC index |

Source index slots are immutable lazy products under the compiled source owner,
for example per-predicate `OnceLock` reverse arrays. Session index slots use
session-local cells. A union probe combines the appropriate source and data
indexes and deduplicates graph overlap. Named-shapes probes use that same source
index, without a second named-graph copy.

The builder updates only instantiated indexes affected by a committed batch.
Sort the new pairs and merge them into affected predicate partitions instead of
sorting the entire graph's permutations. Many tiny batches may still make
repeated merging expensive; measure this before adding buffered sorted runs.
Do not introduce a storage tree or LSM implementation without evidence that
partition merging is the remaining bottleneck.

### 7.3 Selecting and building indexes

Use the following order of decisions:

1. Compiled demand identifies candidates and the operations that could use them.
2. Cheap statistics estimate graph size, candidate size, and likely scan work.
3. First use builds a selected index when estimated reuse justifies its cost.
   Observed expensive scans can promote an index that static analysis missed.
4. A byte budget limits additional indexes and result caches. Falling back to
   scans remains correct. Unknown demand does not automatically build every
   possible index.

Known bound-variable patterns inform index choice but do not promise a fixed
execution direction for all invocations. Fallback query probes also feed demand;
native execution must not be the only path that gets useful indexes.

Start with an internal policy object and deterministic test settings: `BaseOnly`,
`AllIndexes`, and `DemandDriven`. Keep these out of the public session API until
measurements justify user-facing controls. Runtime telemetry is disabled or
aggregated cheaply by default; avoid a string-keyed map update per triple.

Separate the lifetime budgets: shared source indexes have one compiled-owner
budget, while data indexes and result caches have a per-session budget. Account
for shared bytes once. Initially use admission with no eviction for immutable
source indexes; use bounded admission for result caches. Inspect output should
explain when an index was declined, not silently exceed the budget.

Building a lazy index must not invalidate slices held by an active iterator.
Use append-only index slots with stable owned allocations; release temporary
cell borrows before yielding iterators. Mutating RDF batches is restricted to
the builder boundary after all iterators have been consumed.

Whole-path materialization remains a later, measured addition. The number of
reachable pairs can be quadratic even when the edge set is small. The initial
implementation retains bounded endpoint caching and never chooses a closure
index merely because it recognizes a particular ontology predicate.

## 8. Inference, cache lifecycle, and publication

Give the mutable builder a monotonically increasing committed revision. Query
parse trees and static substitutions can survive revisions; query results,
prefetched constraint results, focus caches, and reachability caches must have
explicit validity rules.

First implement conservative invalidation of data-dependent results on every
committed batch. Source-only results may survive when their dependencies are
proven source-only. Index contents and statistics are updated by the same batch
method before new queries execute. Later use the catalog's read dependencies to
narrow invalidation, with wildcard and node-domain dependencies preserved.

Do not cache missing-index plans as permanently unoptimizable. A compiled plan
should select an access method through the dataset interface, or be keyed by
the index/statistics revision when its physical strategy depends on them.
Stale estimates may affect cost; stale results must never affect answers.

Inference and validation can select different default views over the same
partitions. A Data-mode validation following union-based inference selects E;
it must not reuse an E-union-S *view* by accident. Reuse the underlying storage,
then create the correct view and operation caches. Drop inference-only query
state when it has no later consumer; preserve parsed/static products where
useful, without retaining intermediate graphs through cache ownership.

Node-expression functions currently rediscover definitions and parameter order
from the working RDF graph in `infer.rs`. Route the session path through the
compiled canonical function registry, retaining graph access through the
dataset for the function's actual query. Preserve documented legacy discovery
through compatibility adapters. This removes another reason to retain a
materialized union and makes function reads analyzable.

When no rules can execute, the builder can publish shared asserted/evaluated
membership after admission without inference setup. This is a small general
cleanup, not the expected explanation or remedy for all benchmark regressions.

## 9. Implementation sequence and review gates

Each phase should land as a reviewable change on the current branch. Internal
test configurations provide comparison paths; avoid permanent competing public
preparation APIs. Every production phase retains the semantic invariants above.

### Phase 0 — Establish attribution and correctness baselines

- Extend the benchmark harness to separate load, compile, storage preparation,
  inference, first validation, repeated validation, and export.
- Add opt-in counters for Store allocation/loading, source encoding, dataset
  builds, index builds, triple materialization, and committed update work.
- Record the current branch and pre-session `4118547` using the same dependency
  versions. Keep the release-tag comparison separately labeled because its
  lockfile differs. Earlier semantic fixes can legitimately change results;
  pin those cases explicitly instead of treating the old version as an oracle.
- Add semantic tests for graph overlap, graph roles, mutation visibility, and
  fallback CONSTRUCT before changing storage.

Gate: repeatable stage timings and exact expected outputs, not only matching
counts or `conforms` values. Structural counters can enforce ownership contracts
without flaky timing assertions.

### Phase 1 — Compile access descriptions and retain query identities

- Add the access catalog and conservative AST analysis in `shifty-opt`.
- Attach it to `CompiledShapes`; share canonical functions, parsed query
  templates, and borrowed rule metadata where their ownership permits.
- Preserve authored consumers and fallback-query demand. Initially use this
  catalog for inspection and tests while the existing full indexes remain.
- Expose internal inspection records linking demand to shapes, rules, and
  query IDs; avoid committing to serialized persistent plan formats.

Gate: expected demands for forward/reverse paths, wildcard queries, graph
variables, closed shapes, node domains, and called functions; unknown cases
remain explicitly conservative. No new compilation rejection policy.

### Phase 2 — Execute inference and fallback over one dataset

- Introduce `DatasetBuilder` / published dataset roles around the existing
  indexed representation. Separate executor caches from dataset ownership.
- Route CONSTRUCT, targets, constraints, and graph-reading node functions
  through `QueryableDataset`; use dataset membership for duplicate checks.
- Route native rule execution to that same dataset. Remove production Store
  construction and Store synchronization from session inference.
- Commit batches through one mutation method and invalidate result caches.
- Return the prepared dataset internally from inference. Where the current
  monolithic layout cannot express a different validation view yet, keep that
  rebuild explicit and instrumented for phase 3.
- Retain an independent Store-backed evaluator in tests as a differential
  oracle. Empty computational-function evaluation can use an empty queryable
  dataset; it must not justify a full data Store.

Gate: native/fallback inference matches expected graphs and diagnostics,
including multi-round fallback rules. No full-data Store allocation in session
inference. Measure fallback-heavy workloads before claiming a speedup; the
current named-graph linear scans are a concrete risk to address in phase 3.

### Phase 3 — Share source storage and implement graph views

- Add source/local dictionaries, stable source rows, and explicit memberships.
- Implement the graph-role table as views over source and session partitions.
- Introduce the focus interface and adapt `ShapeEvaluator`, node expressions,
  reporting lookups, and native/fallback scans to the shared view boundary.
- Remove materialized inference unions and UnionAll focus unions from the
  session path. Use compiled function lookup for session node expressions.
- Have named-shapes scans reuse source indexes, replacing their linear-list
  special case. Initially retain broad source/data indexes for easy comparison.
- Publish and reuse the inference dataset for every validation graph mode.
  Retain lazy Graph projections at public compatibility boundaries.

Gate: encoding and source-index counters stay constant across multiple sessions
with the same compiled shapes. Inference-to-validation performs no second full
dataset build. Embedded deletion, separate overlap, union deduplication, and
graph-variable queries pass. Source-only reads cannot observe data mutations.

### Phase 4 — Implement selective predicate and direction indexes

- Replace mandatory full permutations with the complete PSO partition base.
- Add per-predicate reverse indexes and demand-driven general subject/object
  directories, with scan fallbacks for every unindexed pattern.
- Select indexes from phase 1's catalog plus dataset statistics and observed
  probes. Implement shared-source and session budget accounting.
- Update only affected instantiated indexes using sorted batch merges; preserve
  revisions and graph-scoped node-domain bookkeeping.
- Keep base-only and full-index modes in the test/benchmark harness to isolate
  planner quality from storage correctness.

Gate: identical answers across index policies and both evaluators. Unqueried
predicates remain accessible without automatically receiving all secondary
indexes. Build bytes and time improve on selective workloads; wildcard and
fallback workloads have a documented, measured policy rather than accidental
full scans. No predicate-name special cases.

### Phase 5 — Finish integration, inspectability, and branch acceptance

- Run every facade consumer against the shared storage path: validation,
  reports, evidence, conformance, witnesses, gates, and language adapters.
- Audit Python repair gates that reconstruct the baseline session: reuse a
  prepared snapshot where thread ownership permits, or measure and document
  the boundary. Do not make all caches Send/Sync just to move them through
  `py.detach`.
- Remove obsolete production storage branches and stale comments. Preserve
  legacy API behavior with adapters; keep reference implementations in tests.
- Document `inspect`/profile output for query/path demand, selected indexes,
  graph scope, estimated versus actual costs, and budget decisions.
- Publish full-suite before/after results and remaining tradeoffs. Update
  documents 05 and 11 to reflect the implemented lifecycle.

Gate: acceptance criteria below pass, compatibility projections do not become
hidden eager copies, and no unresolved performance regression is obscured by
an aggregate average.

## 10. Validation and performance acceptance

### Correctness matrix

- All graph modes; separate and embedded inputs; empty named shapes where the
  public contract permits them; overlapping source/data triples.
- Forward, inverse, sequence, alternative, zero-length, repeated, and cyclic
  paths; literals, blank nodes, absent constants, and graph node domains.
- Closed shapes and variable predicates/graphs, including fallback queries,
  supported dataset clauses, and named-shapes reads during inference.
- Native and fallback CONSTRUCT, guards, functions, tied rule orders,
  multi-round visibility, existing-triple filtering, and unsupported blank-node
  construction diagnostics.
- Authored reporting/evidence, normalized validation, strict policy, provenance,
  and operation-specific bindings.
- Inference followed by Data-mode validation; edits deleting the final support
  for a conclusion; source/data overlap deletions; original sessions remain
  unchanged; legacy repair advance behavior remains materialized-data editing.
- Tiny/no optional-index budgets and unknown demand produce the same results.

Differentially compare against the current engine and the independent evaluator
where semantics are intended to match. Compare RDF graphs up to blank-node
isomorphism and query bags with their required multiplicities. Check findings,
reports, evidence, and diagnostics at their semantic boundaries; triple counts
alone do not establish correctness.

### Performance matrix

Run the full existing benchmark collection, plus generated workloads that vary:

| Dimension | Cases |
| --- | --- |
| Source/data size ratio | Tiny shapes with large data; large shapes with small data; both large |
| Predicate demand | Few referenced predicates among many; most predicates referenced; wildcard reads |
| Direction | Forward, reverse, mixed, and open scans |
| Graph structure | Chains, stars/high-degree hubs, branching graphs, cycles |
| Query execution | SHACL-only, native SPARQL, fallback-heavy, named graph reads |
| Inference | None, no active rules, triple rules, SPARQL rules, many small rounds |
| Reuse | One-shot, repeated operations, many datasets per compilation, edited snapshots |
| Memory | One live session and many live sessions sharing shapes |

Use optimized builds, fixed dependencies and fixtures, sequential alternating
versions, a warmup, and at least five samples per condition. Keep cold-process
and warm-session results separate. Report per-workload medians and spread,
phase timings, peak RSS, source/session/index bytes, and work counters. Include
compilation and preparation in end-to-end numbers. Do not benchmark competing
versions concurrently or infer causation from single-run percentages.

Acceptance requires semantic equality, the structural reuse gates, and measured
improvement in the redundant-work dimensions. Investigate repeatable workload
regressions outside observed noise individually; no fixed speedup is promised.
If removing the Store exposes a missing efficient access pattern, improve the
shared dataset's index policy before restoring a duplicate production backend.

Run Rust formatting, workspace tests, and Clippy gates. Run adapter smoke and
integration coverage for affected Python, C/C++, and Wasm paths. For Python
changes, follow `AGENTS.md`: the frozen uv environment, Ruff checks and formatting
check, `ty check shifty`, and pytest. Do not weaken checks to accommodate the
refactor. Documentation-only planning does not require runtime test execution.

## 11. Decisions fixed here and decisions validated during implementation

| Decision | Direction |
| --- | --- |
| Shared storage for native and fallback execution | Required; remove full-data Store from session inference |
| Graph semantics and public snapshot contracts | Preserve document 11's contracts |
| Source term identity | Immutable source prefix plus session extension |
| Physical graph roles | Explicit membership and views, including embedded exclusions |
| Base representation | Complete predicate-partitioned PSO storage; stage behind existing indexes first |
| Additional indexes | Demand-driven per-predicate/direction and general scan directories |
| Compilation versus runtime planning | Compile possible access; select physical indexes using session statistics and usage |
| Inference mutations | One batch commit boundary with revisions and conservative invalidation |
| Public index controls | Defer until internal policies and measurements stabilize |
| Budget sizes and promotion thresholds | Calibrate on the workload matrix; retain inspectable decisions |
| Sorted merges versus buffered runs | Begin with affected-partition merges; change only if measured batch cost warrants it |
| Full path/SCC materialization | Later extension using retained demand, identity, and cost metadata |

The first implementation milestone is phases 0–2: observable preparation and
one dataset serving both evaluators. The branch's full design milestone includes
phases 3–5: shared source storage, correct graph views, inference-index reuse,
and selective indexing. Do not call this plan complete after making the Store
lazy or adding a no-rules shortcut.
