# Validation, inference, and witnessing algorithms

The [compiler algebra](compiler-pipeline.md) supplies one semantic basis for validation,
rule conditions, evidence, and repair. Compilation fixes admission and source
provenance; a session fixes graph roles and execution policy. This reference
explains the algorithms at those boundaries.

Implementation: [validate.rs](../crates/shifty-engine/src/validate.rs),
[infer.rs](../crates/shifty-engine/src/infer.rs),
[witness.rs](../crates/shifty-engine/src/witness.rs), and
[evidence.rs](../crates/shifty-engine/src/evidence.rs).

## Validation

For each selector/shape statement, enumerate target foci from the focus view,
then evaluate the shape against the evaluation view. These roles differ in
union mode: data supplies candidates while data union shapes supplies reads.
The complete graph-role table is in
[compiled shapes and sessions](sessions-and-storage.md).

`ShapeEvaluator::holds(node, shape)` dispatches over arena constructors:

- Constants, node kinds, and value types test the focus term.
- Boolean operators short-circuit.
- Counts traverse distinct path endpoints and test the qualifier on each.
- Closed and relational atoms inspect outgoing or compared value sets.
- SPARQL and expression leaves delegate to their execution boundary.
- Admitted positive recursion uses provisional coinductive truth on active
  back-edges.

Completed judgments are memoized by `(shape, term)` within the operation.
Active judgments are separate: an answer that depends on a provisional
back-edge is not cached as context-independent. The recursion admission rule is described below.

For findings, failed judgments are explained with observed values, paths,
severity, and messages. W3C reporting additionally walks authored RDF component
boundaries to construct report results. Structured evidence retains a different,
more detailed proof grammar for traversal and repair.

### Execution failures

A SPARQL target or constraint can fail to execute. That failure has no Boolean
verdict. Recursive evaluation still memoizes Boolean judgments internally, but
the executor retains the first query failure separately. Public evaluation
boundaries start a fresh failure scope and return `EvaluationError` instead of
findings if any executed query failed. Negation, disjunction, target filtering,
and severity policy cannot turn that failure into a completed verdict. Reusing
a session starts a new scope; an earlier failed call does not poison it.

`UnsupportedPolicy::Error` is the default. Before executing a SPARQL query, the
engine checks its parsed function calls for graph-reading SHACL functions that
cannot use the query dataset. This check precedes native and fallback execution:
SPARQL expression-error semantics alone can discard a failing FILTER row.
Explicit `Ignore` permits best effort, but execution errors still propagate.

## Physical planning

A physical plan changes access and order while preserving logical satisfaction:

```text
holds(plan(normalize(S)), G) = holds(S, G)
```

Class targets can seed backward from their class constant rather than scanning
every graph node. Boolean children are ordered by static cost to improve early
exit. Native query scans use dataset statistics and selected indexes. Authored
provenance remains separate from execution order, so sharing a normalized
request does not erase source results.

Conformance-only scans avoid evidence materialization. Findings and evidence
apply their documented severity policy, while bare conformance counts logical
failures. These products need not have identical verdicts under a severity
threshold that permits some failures.

## Recursion polarity

The arena graph records references between shapes. Dependency edges describe
how a child's truth affects its parent:

| Constructor | Edge polarity |
| --- | --- |
| `And`, `Or`, transparent annotation | Positive |
| `Not` | Negative |
| Count lower bound | Positive with respect to the qualifier |
| Count upper bound | Negative with respect to the qualifier |

A count with both bounds contributes both dependencies. Constraints that inspect
graph facts but reference no child shape do not introduce recursive shape edges.

A property-shape universal lowers to `∀π.φ = ∃≤0 π.¬φ`. The upper count and the
negation each reverse polarity, so the whole dependency is positive. Rejecting
any SCC containing a negative edge would incorrectly reject this encoding.

## Recursion admission

1. Compute signed dependency edges and Tarjan strongly connected components.
2. Order components by dependency depth, dependees before dependants.
3. Within each recursive SCC, assign sign potentials satisfying
   `σ(b) = σ(a) × sign(a → b)` for every internal edge.
4. Reject a component if two paths require contradictory potentials. This is
   the sign-balance test: a cycle with net negative polarity is inadmissible.

`CompiledShapes` checks both authored and normalized arenas. Failure reports
identify the offending shapes; choosing a different validation product cannot
bypass admission.

For example, `S := ¬∃p.S` over a p-self-loop has no consistent two-valued answer
and is rejected. `S := ∀p.S` has two negative arena edges composing to positive
and is admitted.

## Ordered least-fixpoint inference

Compilation retains active rules, their order, and conservative read/guard
dependencies. Execution starts from asserted data:

1. Discover rule foci from the data view.
2. Visit rule groups in ascending `sh:order`.
3. Evaluate each group's conditions and heads against one shared snapshot.
4. Collect new triples, suppressing duplicates against the inference read view.
5. Commit the group's additions together before the next group reads.
6. Repeat passes until no additions remain.

Separate-input rules read evolving data union shapes; embedded rules read
evolving data. Triple-rule node expressions compute terms from focus, paths,
filters, set operations, and supported functions. SPARQL rules evaluate
CONSTRUCT. Existing input blank nodes can be reused, but fresh blank-node
construction is rejected to prevent unbounded creation on repeated passes.

Changed predicates and conservative dependencies avoid unnecessary rule and
guard work. Unknown reads retain wildcard dependencies. This scheduling is
semi-naive in its use of changed facts; it does not imply a general relational
delta engine for every SPARQL operator.

Native and fallback queries read the same indexed dataset. A commit updates
indexes and invalidates data-dependent results before further rule evaluation.
After convergence that dataset moves into validation with the requested read
view. See [shared storage](sessions-and-storage.md).

## Witnessing

A polarity-aware dispatcher evaluates Boolean children and qualified candidates,
then constructs failure or satisfaction evidence for the judgment. A failed
conjunction retains failed children; a failed disjunction retains each branch.
Satisfaction retains the children required to explain holding conjunctions and
disjunctions. Negation crosses polarity. Counts retain qualifying matches and
rejected candidates with their corresponding evidence.

Canonical failure evidence is pruned. Optional `EvaluationProgress` separately
reports immediate authored children in source order, including passing siblings.
Normalization maps allow one normalized request to fan out to multiple authored
results without repeated evaluation.

`PathSupport` is one concrete positive route. A coinductive back-edge is a finite
satisfaction leaf. Opaque constraints retain diagnostics, and unsupported repair
directions remain blocked instead of fabricating proof or edits. Ordering,
traversal, identities, and codec contracts are specified in
[the evidence architecture](evidence.md).

## Repair composition

Failure and satisfaction feed the two synthesis folds described in
[symbolic repair](repair.md). Synthesis describes edits; a
[driver](repair.md) chooses a plan; instantiation resolves it; the
whole-schema gate reports fixed, remaining, and introduced violations.

The resulting dependency chain is:

```text
source SHACL → algebra + provenance → admitted normalized schema
            → session inference → validation/evidence
            → repair tree → chosen plan → graph delta → gate outcome
```

Each stage has a separate correctness boundary. A report rendering is not a
repair proof, a candidate binding is not validation, and a gate outcome leaves
application policy to the caller.
