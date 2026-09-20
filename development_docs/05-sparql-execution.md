# 05 - Spargebra-Native SPARQL Execution

This document defines the planned optimized execution path for SHACL-SPARQL
constraints, targets, and rules. The current engine parses queries with
Spargebra and executes them with Oxigraph/Spareval. That remains the correctness
fallback, but profiling of the 223P/NIST workload shows that validation-side
SPARQL evaluation dominates runtime, particularly repeated quad scans and
property-path traversal.

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

Each canonical query is parsed once. The compilation cache key contains the
canonical query, static SHACL substitutions, graph mode, and index-plan version.
The cached result is either a native plan or a prepared fallback query.

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

The initial physical IR is deliberately small:

```rust
enum NativeOp {
    InputFocus,
    Scan(TripleScan),
    PathScan(PathScan),
    Join { left: OpId, right: OpId, kind: JoinKind },
    Exists { input: OpId, probe: OpId, negated: bool },
    Union(Vec<OpId>),
    Filter { input: OpId, expression: ExprPlan },
    Extend { input: OpId, variable: VarId, expression: ExprPlan },
    Project { input: OpId, variables: Vec<VarId> },
    Distinct(OpId),
}
```

`TripleScan` supports constants, variables, and parameter operands in subject,
predicate, object, and graph positions. `PathScan` contains a canonical path and
supports all endpoint binding modes:

- bound start and bound end: membership probe;
- bound start: forward lookup;
- bound end: reverse lookup;
- open endpoints: relation scan.

Joins use indexed nested-loop execution when one side binds an indexed probe
and hash joins otherwise. `Exists` is a correlated semi-join; negated `Exists`
is an anti-join and stops its probe at the first match. Binding batches retain
`FocusId`, allowing results from many `$this` values to be evaluated together
without losing their owning focus node.

The first native capability set is:

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

Literal operations may initially use the trait's externalization defaults.
Frequently used effective-boolean-value, numeric comparison, and string
operations should later operate directly on dictionary entries.

## Path demand and index planning

Planning analyzes paths from both native SHACL algebra and Spargebra queries.
Equivalent paths share one canonical `PathId`.

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

Dataset statistics include predicate cardinality, distinct subjects and
objects, degree distributions, and class/subclass counts. The planner chooses
one strategy per path:

- `Traverse`: use base indexes directly for cheap or rarely used paths;
- `Memoized`: cache forward and reverse result bitmaps per endpoint;
- `Materialized`: store the complete path relation in forward and reverse CSR
  form;
- `SccClosure`: condense a transitive graph into SCCs and store component
  reachability bitmaps.

The initial specializations are:

1. `rdfs:subClassOf*`: SCC condensation plus component reachability.
2. `rdf:type/rdfs:subClassOf*`: materialized instance-to-class and reverse
   class-to-instance relations.
3. `s223:contains+`, `s223:mapsTo+`, and `s223:cnx+`: memoized endpoint
   closures first; promote to materialized relations only when observed demand
   and estimated relation size justify it.
4. Small fixed sequences such as `sh:property/sh:path`: materialize when the
   estimated output is bounded and reused.

The current executor implements bounded per-start memoization for native `*`
and `+` closures. Cache keys include the start term, compiled reachability step,
closure kind, and graph selection. Endpoint sets are shared between repeated
probes, capped at one million stored endpoint IDs, and cleared if inference
extends the dataset. Demand-driven promotion to materialized or SCC indexes
remains planned work.

Index selection is budgeted. The planner estimates construction work and
relation size, ranks candidates by avoided traversal work per byte, and admits
them until the configured memory budget is exhausted. `Traverse` is always the
correct fallback.

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

Per-query telemetry should report:

- source shape or rule and stable query fingerprint;
- native or fallback executor and fallback reason;
- invocation and focus-node counts;
- planning, index-build, and execution time;
- rows entering and leaving each operator;
- triple and path probe counts;
- path-cache hits, misses, and materialized relation sizes;
- early exits in conformance mode.

`inspect` should expose capability decisions, the native physical plan, path
demands, selected indexes, estimated cardinalities, and memory estimates.

## Implementation stages

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

Every stage must preserve W3C conformance and the 223P validation result. Speed
claims require release benchmarks that report index-build time and peak memory,
not only steady-state query execution.
