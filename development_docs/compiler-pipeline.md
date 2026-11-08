# Compiler pipeline and inspection

Shifty lowers shapes RDF into an arena algebra, checks recursion, normalizes
with provenance, and builds physical plans. Inspection reads only shapes;
dataset-dependent index decisions occur during session evaluation.

## Module boundaries

| Crate | Responsibility |
| --- | --- |
| `shifty-algebra` | Paths, arena shapes, selectors, rules, node expressions, metadata |
| `shifty-parse` | RDF loading, lowering, source diagnostics, canonical SPARQL/functions |
| `shifty-opt` | Dependency analysis, normalization, physical plans, query lowering, access catalog |
| `shifty-engine` | Session preparation, storage, inference, validation, evidence, synthesis, gating |
| `shifty-repair` | Pure repair IR and plan instantiation |
| CLI and language bindings | Input conversion, transport, presentation, caller-owned write-back |

`CompiledShapes` retains both authored and normalized schemas. Admission checks
the authored arena before optimization and the normalized arena afterward.
Operation-specific planning and source encoding remain lazy. The lifetime
contract is in [compiled shapes and sessions](sessions-and-storage.md).

## Algebra and lowering

The executable core is an arena graph of shape formulas over RDF paths:

```text
π ::= id | q | π⁻ | π · π′ | π ∪ π′ | π*
φ ::= ⊤ | test(c) | test(τ) | closed(Q) | eq(π,p) | disj(π,p)
    | ¬φ | φ ∧ φ′ | φ ∨ φ′ | ∃≥n π.φ | ∃≤n π.φ
```

Paths relate arbitrary RDF terms, including literals. SHACL counts distinct
reachable values. A `Count` fuses lower and upper bounds on one path/qualifier;
`∀π.φ` lowers to `∃≤0 π.¬φ`. `sh:hasValue` requires a specified value to exist,
while `sh:in` constrains every value to the declared set.

Node kind, ordered comparisons, unique language, SPARQL, and expression tests
extend this core. Class membership uses `rdf:type/rdfs:subClassOf*` with a
constant class test. Selectors represent explicit nodes, predicate subjects or
objects, path/qualifier targets, and SPARQL targets.

Shape children use arena-local `ShapeId` references, supporting sharing and
cycles. `Pending` is a construction marker that must not survive finalization.
`Annotated` preserves severity and messages without changing logical truth.
See [the algebra types](../crates/shifty-algebra/src/shape.rs) and
[parser](../crates/shifty-parse/src/lib.rs) for exact representations.

## Normalization

[normalize.rs](../crates/shifty-opt/src/normalize.rs) rebuilds acyclic nodes
through structural interning and retains source-to-normalized shape and
statement maps. Recursive SCCs preserve their reference structure without
unfolding. Admission runs before and after normalization.

Implemented rewrites flatten Boolean operators, eliminate identities and
complements, push supported negations inward, merge compatible counts, and
collapse identity/empty paths. Path normalization folds inverses, reverses
inverse sequences, deduplicates alternatives, and simplifies star identities.
Value predicates tighten compatible ranges and lengths and detect proven
contradictions conservatively.

These rewrites preserve satisfaction, not numeric arena identity or rendered
message text. Authored results use provenance maps. SHACL set-reachability laws
cannot indiscriminately rewrite SPARQL paths with bag semantics.

## Target seeding

[plan.rs](../crates/shifty-opt/src/plan.rs) lowers selectors to `FocusSource`:
subjects/objects of a predicate, an explicit node, a path to a constant, a
scan/filter, or a SPARQL target.

For a class target, the logical selector asks whether each candidate can reach
class C via `rdf:type/rdfs:subClassOf*`. `PathToConst` traverses backward from C
to enumerate matching foci directly, avoiding a full candidate scan. General
qualifiers retain `ScanFilter`. Explicit target constants remain eligible even
when absent from graph storage.

## Short-circuit ordering

A static recursive cost model estimates relative shape/path work. Atomic tests
are cheap; repeated paths, recursive references, and opaque query/expression
checks cost more. `And` and `Or` children are sorted by estimated cost and ID
inside a cloned physical arena that preserves shape IDs.

This ordering changes early exits, not truth or authored order. It is a static
heuristic, not a data-driven selectivity model. Native BGP planning separately
uses dataset predicate statistics and currently bound variables. See
[SPARQL execution](sparql-execution.md).

## Reachability compilation

[path_plan.rs](../crates/shifty-engine/src/path_plan.rs) compiles paths to
`ReachStep` trees. Predicate IRIs become dataset term IDs once. Inverses fold
into forward/backward predicate steps; backward sequences reverse their order.
Evaluation then uses dataset probes, set composition, alternatives, and visited
sets for repeated reachability without redispatching on inverse syntax.

Repeated native paths use bounded endpoint caching. Optional triple indexes
come from compiled demand and observed runtime work; the complete graph remains
queryable when no optional index is admitted. See
[shared storage](sessions-and-storage.md).

## Inspection stages

Run from a checkout with a built CLI:

```sh
cargo build --locked -p shifty-cli
target/debug/shifty inspect --stage algebra docs/examples/quick-start/shapes.ttl
```

| Stage | What it reveals |
| --- | --- |
| `rdf` | Loaded triples and parsing |
| `algebra` | Authored arena, selectors, and lowering |
| `strata` | Signed recursion analysis and offending components |
| `normalized` | Simplification, shared expressions, normalized selectors |
| `plan` | Target seeds and static Boolean ordering |
| `capability` | Native or fallback SPARQL admission |
| `access` | Static graph scope, predicates, directions, query/path identities, conservative demand |

For example, a property datatype check appears as an upper count of zero
values failing that datatype. Class targets become path/count selectors.
Source and normalized IDs can differ, so compare the provenance maps rather
than assuming numeric identity across stages.

Inspecting capabilities identifies query admission before execution. Inspecting
access shows potential reads even for fallback queries. Neither can select
runtime indexes without data. Validation's `--profile` reports actual encoding,
index, query, inference, and cache work; exact CLI formats and flags are in
[the CLI reference](../docs/reference/cli.rst).
