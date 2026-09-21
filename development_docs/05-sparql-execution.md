# 05 - Spargebra-Native SPARQL Execution

This document records the implemented native SPARQL subset and shared indexed
dataset, then identifies possible extensions. Queries are parsed with Spargebra.
Supported queries run through the native executor; other queries run through
Spareval over the same dataset. The original 223P/NIST profile found that
validation-side SPARQL evaluation dominated runtime, particularly repeated
quad scans and property-path traversal.

The decision is to compile a useful Spargebra subset into a native physical
plan over immutable, specialized indexes. Queries outside that subset execute
through Spareval over the same indexed dataset via `QueryableDataset`.

## Motivation

The initial 223P/NIST profile is sufficiently concentrated to justify a
specialized execution path:

- the shapes lower to 18 SPARQL constraint leaves;
- about 99% of sampled validation time is below SPARQL constraint evaluation;
- Oxigraph quad iteration, range checks, and pattern lookup account for most
  self time;
- the source shapes repeatedly use `rdfs:subClassOf*`,
  `rdf:type/rdfs:subClassOf*`, `s223:cnx+`, `s223:contains+`, and
  `s223:mapsTo+`.

These figures identify the first optimization targets, not permanent workload
assumptions. Per-query instrumentation must precede execution changes so plans
and indexes can be selected from observed demand.

## Goals and boundaries

The native path should:

- preserve SHACL-SPARQL prebinding semantics;
- evaluate a constraint for a batch of focus nodes instead of once per node;
- make property paths and correlated `EXISTS` / `NOT EXISTS` explicit physical
  operators;
- use graph statistics and reusable path indexes;
- stop after the first violation when only conformance is required;
- fall back for the entire query whenever native execution is unsupported.

It is not intended to become a complete SPARQL 1.1 implementation. `SERVICE`,
arbitrary dataset clauses, unsupported expressions, and initially aggregates
or subqueries remain on the fallback path. A query is never partly evaluated
by both engines: capability analysis chooses one executor before evaluation.

## Execution pipeline

```
canonical SPARQL text
        |
        v
Spargebra Query AST
        |
        +--> apply static SHACL substitutions
        |
        v
capability + demand analysis
        |
        +--> NativeQueryPlan ------> native executor
        |
        `--> PreparedSparqlQuery --> Spareval fallback
                                      |
                                      v
                              FrozenIndexedDataset
```

Parsing and prefix resolution remain in `shifty-parse`. Planning belongs in
`shifty-opt`; indexed storage and execution belong in `shifty-engine`.

The compiled shapes owner retains parsed canonical query bodies that access
analysis found. Each session reuses those bodies where available and caches
operation-specific static substitutions and native lowering locally. The
constraint cache key includes the canonical query, path, and shape bindings;
the session's dataset and graph policy fix the remaining context. `$this`
remains dynamic. Runtime statistics and term-ID lowering are session-specific.

## SHACL prebindings

SHACL prebinding is substitution throughout the query, not an ordinary initial
solution binding. The compiler therefore represents substituted values as
operands in the plan rather than prepending `VALUES`.

Bindings split into two classes:

- Static per constraint: `$PATH`, `$currentShape`, `$shapesGraph`, and future
  custom-component parameters. These are applied once before capability
  analysis. A complex `$PATH` is replaced in property-path position, not encoded
  as an RDF term.
- Dynamic per invocation: `$this`. Native plans use a `FocusId` and `TermId`
  input column so one plan can evaluate many focus nodes together.

The fallback path uses the fully substituted Spargebra AST where required for
SHACL semantics. Oxigraph's initial-variable substitution API is used only
where differential tests prove it equivalent.

## Native physical plan

The native physical IR is deliberately small (shown here without its fields):

```rust
enum NativeOp {
    InputFocus,
    Scan { input: OpId, pattern: TripleScan },
    PathScan { input: OpId, scan: PathScan },
    Union { left: OpId, right: OpId },
    Filter { input: OpId, expr: ExprPlan },
    Extend { input: OpId, var: VarId, expr: ExprPlan },
    Project { input: OpId, vars: Vec<VarId> },
    Distinct { input: OpId },
}
```

BGP joins lower by threading the output of one scan into the next; correlated
`EXISTS` is an expression node evaluated against a subplan.

`TripleScan` supports constants, variables, and parameter operands in subject,
predicate, object, and graph positions. `PathScan` contains a canonical path and
supports all endpoint binding modes:

- bound start and bound end: membership probe;
- bound start: forward lookup;
- bound end: reverse lookup;
- open endpoints: relation scan.

The implemented native executor uses a left-deep pipeline of indexed scans.
`EXISTS` and `NOT EXISTS` evaluate correlated subplans and stop after the first
match. Hash joins and dedicated semi-/anti-join operators remain possible
extensions. Binding batches retain
`FocusId`, allowing results from many `$this` values to be evaluated together
without losing their owning focus node.

The implemented native capability set includes:

- `SELECT` and `ASK`;
- basic graph patterns and fixed named-graph patterns;
- joins, unions, projection, and `DISTINCT`;
- property paths: predicate, reverse, sequence, alternative, `*`, `+`, and `?`;
- correlated `EXISTS` and `NOT EXISTS`;
- boolean connectives, `BOUND`, safe equality, `STR`, and `STRSTARTS`;
- simple `BIND` expressions.

Aggregates, subqueries, ordering, service calls, ordered comparisons, `IN`, and
unsupported functions select fallback execution. Lowering (`lower_query`) is the
gate: a query runs natively iff it lowers to a native plan, otherwise it falls
back, recording the first unsupported construct as the fallback reason for
inspection and telemetry. The set above is the design target; the authoritative
statement of what lowers today is `lower_query` itself.

## Indexed session dataset

`FrozenIndexedDataset` now starts during inference, accepts committed rule
batches, and is handed directly to validation. The name reflects its published
read snapshot; the builder phase is mutable. It implements Spareval's
`QueryableDataset`, so native and fallback queries read the same graph views.

`TermId` is the dataset's `QueryableDataset::InternalTerm`. A lazily encoded
source dictionary and index belong to `CompiledShapes`; each session extends
that dictionary for data, inferred facts, and query-only constants. Source
rows and local rows retain separate membership, allowing a Data validation
view after union-based inference without rebuilding the dataset. The named
shapes graph reads the same source index and remains independent of data edits.

Every triple is kept in a predicate-partitioned PSO primary index, sorted by
`(subject, object)` within each predicate. Exact membership, predicate scans,
and known-predicate forward probes use that base. Optional per-predicate
reverse indexes and general subject/object directories are built from compiled
demand or observed probes under separate source and session byte budgets.
Patterns without an optional index scan the complete primary representation;
index selection cannot change answers. Virtual path relations are not exposed
as RDF predicates or wildcard scan rows. Native path traversal uses the shared
dataset, while fallback property paths remain evaluated by Spareval.

Literal operations still use the dataset trait's externalization where needed.
Moving more expression work onto dictionary entries is a possible extension.

## Path demand and index planning

Compilation analyzes paths from both authored SHACL and parsed SPARQL queries.
Equal paths share a compilation-local identity. The following demand sketch
describes the information retained, rather than a literal public Rust type:

```rust
struct PathDemand {
    path: PathId,
    graph_scope: GraphScope,
    forward_probes: u64,
    reverse_probes: u64,
    membership_probes: u64,
    open_scans: u64,
    expected_focuses: u64,
}
```

The current dataset tracks predicate cardinality and graph node domains. Native
path execution traverses selected graph views and keeps a bounded cache of
endpoint results. The following richer strategies are future options:

- `Traverse`: use base indexes directly for cheap or rarely used paths;
- `Memoized`: cache forward and reverse result bitmaps per endpoint;
- `Materialized`: store the complete path relation in forward and reverse CSR
  form;
- `SccClosure`: condense a transitive graph into SCCs and store component
  reachability bitmaps.

The current executor caches native `*`, `+`, and `?` endpoint results. Keys
include the start term, compiled reachability step, closure kind, and graph
selection. The cache admits at most one million result IDs and is cleared on a
committed data batch or default-view change. It does not materialize whole-path
relations or SCC closures, and has no ontology-specific predicate cases.

Optional *triple* indexes are selected from compiled demand, dataset size, and
observed probes under separate source and session byte budgets. A declined
index leaves a correct primary-index scan fallback. Path-result caching has a
separate bounded admission rule.

## Query planning and execution

The native planner substitutes static SHACL parameters, lowers its supported
query subset, and uses dataset statistics to greedily order BGP scans. It keeps
each next scan connected to an already bound variable when possible. A
predicate absent from initial statistics is estimated from the dataset average
rather than assigned zero rows: inference may introduce that predicate after
the plan is compiled. This prevents a free scan from being repeated for every
focus node solely because its initial cardinality was zero. The current
executor runs the resulting left-deep plan with indexed probes and handles
supported property paths separately.

A worst-case-optimal join could be added for dense cyclic BGPs if measurements
show large intermediate joins. It would still require a variable order and
appropriate indexed access, so it would complement rather than replace access
planning. Hash, semi-join, and anti-join operators are also possible later
extensions; the current native executor does not implement them.

Constraint execution has two modes:

- `Conforms`: return as soon as any violation exists for a focus node.
- `Violations`: retain projected `?value` and `?path` values for reporting.

Targets return focus nodes. Rules return constructed triples and remain in the
inference fixed-point scheduler. Native rule execution reads the indexed dataset;
committed rule batches update it before the next order group reads.

## Correctness and fallback

Oxigraph/Spareval remains the semantic oracle for SPARQL behavior. A query uses
fallback if capability analysis cannot prove that every AST node, expression,
graph operation, and result form is supported.

During development, a differential mode executes native-capable queries through
both engines and compares:

- `ASK` booleans;
- `SELECT` multisets after projection, including unbound variables;
- RDF term identity and literal semantics;
- SHACL violation values and paths;
- errors and unsupported prebindings.

Native planning or execution errors are not silently converted into fallback
after partial evaluation. They are surfaced as engine errors. Fallback is a
planning decision, which keeps behavior deterministic and debuggable.

## Instrumentation

Current opt-in telemetry reports per-query executor and execution time,
per-shape/rule work, source/session encoding and commit time, optional-index
admissions, scan candidate rows, and shape/reach-cache activity. These proposed
additional details are not yet recorded per physical operator:

- source shape or rule and stable query fingerprint;
- native or fallback executor and fallback reason;
- invocation and focus-node counts;
- planning, index-build, and execution time;
- rows entering and leaving each operator;
- triple and path probe counts;
- path-cache hits, misses, and materialized relation sizes;
- early exits in conformance mode.

`inspect --stage capability` reports native/fallback admission, and
`inspect --stage access` reports static graph scope, predicates, probe direction,
query/path identities, function calls, and conservative coverage. Runtime
`--profile` output reports the selected indexes with estimated and actual
bytes, budgets, build time, and scan work. There is no data-dependent index
selection in `inspect`, because that command reads only shapes.

## Historical implementation stages

1. **Measure and classify.** Add per-query timing, AST capability reports, path
   demand extraction, and differential-test infrastructure.
2. **Shared indexed storage.** Implement `FrozenIndexedDataset`,
   `QueryableDataset`, base statistics, and verify fallback conformance.
3. **Native BGP subset.** Add scans, joins, projection, filters, batched
   `$this`, and conformance early exit.
4. **Paths and anti-joins.** Add `PathScan`, subclass/type indexes, correlated
   `EXISTS` / `NOT EXISTS`, measured string functions, and lazy transitive
   caches.
5. **Data-aware planning.** Add join selection, budgeted materialization, plan
   inspection, and adaptive promotion based on measured demand.
6. **Broaden coverage.** Add expressions and operators in measured priority
   order while preserving whole-query fallback.

The native subset, shared dataset, and demand-driven triple indexes described
above are implemented. Whole-path materialization, SCC indexes, hash joins,
and additional native SPARQL features remain future work. The release
measurements and semantic comparisons for the current storage design are in
[`benchmark/shared-dataset-results.md`](../benchmark/shared-dataset-results.md).
