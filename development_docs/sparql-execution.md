# SPARQL execution

Queries are parsed with Spargebra. Supported queries lower to a native
physical plan; other queries execute through Spareval over the same indexed
dataset. Capability admission selects one executor for the whole query before
evaluation.

Implementation: [native planning](../crates/shifty-opt/src/sparql_native/mod.rs),
[native execution](../crates/shifty-engine/src/native_exec.rs),
[SPARQL integration](../crates/shifty-engine/src/sparql.rs), and
[path execution](../crates/shifty-engine/src/path_plan.rs).

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

- Static per constraint: `$PATH`, `$currentShape`, `$shapesGraph`, and
  custom-component parameters. These are applied once before capability
  analysis. A complex `$PATH` is replaced in property-path position, not encoded
  as an RDF term.
- Dynamic per invocation: `$this`. Native plans use a `FocusId` and `TermId`
  input column so one plan can evaluate many focus nodes together.

The fallback path uses the fully substituted Spargebra AST where required for
SHACL semantics. Oxigraph's initial-variable substitution API is used only
where differential tests prove it equivalent.

## Native physical plan

The native physical IR contains these operators (field details are in the
implementation):

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

`TripleScan` supports constants and variables in subject, predicate, and object
positions, with a default or fixed named-graph selector. Variable graph names
select fallback execution. `PathScan` contains a canonical path and supports all
endpoint binding modes:

- bound start and bound end: membership probe;
- bound start: forward lookup;
- bound end: reverse lookup;
- open endpoints: relation scan.

The implemented native executor uses a left-deep pipeline of indexed scans.
`EXISTS` and `NOT EXISTS` evaluate correlated subplans and stop after the first
match. Binding batches retain `FocusId`, allowing results from many `$this`
values to be evaluated together
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
inspection and telemetry.

## Indexed session dataset

`FrozenIndexedDataset` starts during inference, accepts committed rule
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

Literal operations use the dataset trait's externalization where needed.

## Path demand and index planning

Compilation analyzes paths from authored SHACL and parsed SPARQL queries,
retaining graph scope, known/wildcard predicates, probe directions, and
node-domain sensitivity in the access catalog. Equal paths share a
compilation-local identity. Native execution traverses the selected graph view.

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

Opt-in telemetry reports per-query executor and execution time,
per-shape/rule work, source/session encoding and commit time, optional-index
admissions, scan candidate rows, and shape/reach-cache activity.

`inspect --stage capability` reports native/fallback admission, and
`inspect --stage access` reports static graph scope, predicates, probe direction,
query/path identities, function calls, and conservative coverage. Runtime
`--profile` output reports the selected indexes with estimated and actual
bytes, budgets, build time, and scan work. There is no data-dependent index
selection in `inspect`, because that command reads only shapes.
