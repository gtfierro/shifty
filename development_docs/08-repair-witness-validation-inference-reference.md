# Repair, Witnessing, Validation, and Inference Reference

This document is a proposal-oriented consolidation of the formalism and
algorithmic approach behind Shifty's validation, inference, witness, and repair
machinery. It intentionally avoids centering the current Rust/Python API. The
API is an implementation surface; the important design is the algebra, the
chosen fixpoint semantics, and the separation between what the engine can
compute and what an external repair driver must decide.

The short version:

- SHACL is lowered to a small algebra of paths, shapes, selectors, and rules.
- Validation is satisfaction of that algebra over an RDF graph, with
  stratified greatest-fixpoint semantics for positive recursive shapes.
- SHACL-AF inference is least-fixpoint forward chaining over rule heads and
  conditions, scheduled by rule order and graph deltas.
- Witnessing is the structured, lossless failure proof produced by the same
  evaluator as validation.
- Repair synthesis is the abductive dual of validation: from a failure witness,
  construct a parametric repair tree describing edits that could make the failed
  shape hold.
- Instantiation only applies a driver's choices to a repair tree. It never
  chooses terms, branches, or counts.
- The gate validates `G (+/-) DeltaG` against the whole schema and reports fixed,
  remaining, and introduced violations. The driver decides whether to accept.

## 1. Data and Algebraic Core

The formal core is the SHACL fragment of the common graph formalism specialized
to RDF. RDF removes the paper's edge/property distinction:

```text
Term = IRI | Blank | Literal
Node = IRI | Blank
Pred = IRI
G    subset Node x Pred x Term
```

Path evaluation is relational:

```text
[[pi]]^G subset Term x Term
```

The path grammar is:

```text
pi ::= id | q | pi^- | pi . pi' | pi union pi' | pi*
```

where `q` is an RDF predicate. SHACL `+` and `?` are parser sugar:

```text
pi+ = pi . pi*
pi? = pi union id
```

Shapes are formulas over paths and value predicates:

```text
phi ::= top
      | test(c)
      | test(tau)
      | closed(Q)
      | eq(pi, p)
      | disj(pi, p)
      | not phi
      | phi and phi'
      | phi or phi'
      | exists >= n pi . phi
      | exists <= n pi . phi
```

The implementation extends this core with SHACL-native atoms such as node kind,
less-than, less-than-or-equals, unique language, SPARQL constraints, and
expressions. Cardinality constraints are represented as one bounded count:

```text
Count { path = pi, min = optional n, max = optional m, qualifier = phi }
```

This single primitive covers `sh:minCount`, `sh:maxCount`,
`sh:qualifiedMinCount`, `sh:qualifiedMaxCount`, `sh:node`, nested property
shapes, and the universal encoding:

```text
forall pi . phi  ==  exists <= 0 pi . not phi
```

A schema is a finite set of target/constraint statements:

```text
S = { (selector_i, shape_i) }

G |= S iff for every statement (sel, phi) and every term v:
  if G, v |= sel, then G, v |= phi
```

Selectors are the algebraic form of SHACL targets: target node, subjects of a
predicate, objects of a predicate, class targets expressed as a path/count shape,
and SPARQL target escape hatches.

## 2. Normalization

Lowering produces a shape arena: a DAG of reusable shape expressions. Before
planning and execution, the arena is normalized by semantics-preserving rewrites:

- Boolean simplification: flatten `and`/`or`, remove identities, absorb `top` and
  bottom, deduplicate children, push negation inward, and canonicalize children.
- Count simplification: remove trivial lower bounds, detect impossible bounds,
  collapse empty paths, merge counts over the same path and qualifier, and
  simplify identity-path counts.
- Path simplification: flatten sequence/alternative, eliminate identity,
  canonicalize inverse, deduplicate alternatives, and simplify star identities.
- Value-type simplification: merge numeric and string bounds, drop `any`, and
  detect definitely unsatisfiable facet combinations.
- Hash-consing/CSE: structurally identical nodes share one arena ID.

Normalization matters for all later phases:

- validation gets fewer distinct shapes to evaluate;
- witnessing references stable arena IDs instead of duplicate subformulas;
- repair trees can refer to a compact constraint DAG;
- driver-side solvers see smaller search spaces.

Normalization must preserve the chosen recursion semantics. It does not unfold
recursive SCCs or perform rewrites that change stratification.

## 3. Recursion and Fixpoint Semantics

SHACL recursion is not fully specified by the W3C spec, so Shifty makes an
explicit decision:

1. The shape/rule dependency graph must be stratifiable.
2. Cycles through net negation are rejected with a diagnostic.
3. Positive recursive validation uses the greatest fixpoint.
4. Rule inference uses the least fixpoint.

Polarity is semantic, not syntactic. Because the IR encodes `forall pi . phi` as
`exists <= 0 pi . not phi`, a syntactic `not` can appear inside a positive
universal constraint. Dependency analysis tracks monotonicity through the
operators rather than just counting `not` nodes.

For validation, greatest fixpoint gives the coinductive reading:

```text
a node conforms if no reachable concrete counterexample exists
```

This makes positive cycles valid when no finite violation can be found. For
example, two nodes that mutually satisfy a recursive "all neighbors conform"
shape conform under gfp.

For inference, least fixpoint gives the constructive reading:

```text
a triple is inferred only when its rule body has finite support
```

This prevents rules from deriving facts solely because the facts would justify
themselves.

Repair inherits both choices:

- witnesses agree with gfp validation because they use the same evaluator and
  back-edge behavior;
- constructive repair drivers, especially ASP-style drivers, should reason over
  finite violation/support facts rather than trying to rely on coinductive
  self-support;
- every candidate repair still passes through the real gfp validation gate.

## 4. Validation Algorithm

Validation is the oracle all optimized and repair-facing machinery must agree
with.

Inputs:

- data graph `D`;
- optional shapes/ontology graph `H`;
- schema `S`;
- graph mode selecting which graph is used for focus discovery and which graph is
  used for path/class/SPARQL evaluation.

The important distinction is:

```text
focus graph   = where target nodes are discovered
context graph = where paths, class hierarchy, and SPARQL constraints are read
```

For split data/shapes use, the default is data-only focus discovery and union
context evaluation. This means shape targets do not accidentally include ontology
nodes, but class hierarchy and shape-side support triples remain visible.

The algorithm:

1. Check stratifiability of the shape arena.
2. Build a frozen indexed dataset over the context graph.
3. Build a SPARQL executor over the same frozen dataset.
4. For each schema statement, enumerate focus nodes from its selector.
5. For each focus, evaluate `holds(focus, shape)`.
6. If it fails, run `explain` to produce validation reasons.
7. Return one grouped violation per `(focus, statement)`.

Shape evaluation is memoized by `(shape_id, term)`. An active stack detects
positive recursive back-edges; under gfp these are treated as provisionally true.
Results that depend on such provisional back-edges are not blindly reused outside
their active context.

`explain` is human/report oriented. It records failed atoms, paths, severity, and
messages, but it can flatten or render structure in ways unsuitable for repair.
That is why witness generation is separate.

A planned physical validator preserves the same semantics while changing access
paths:

- target-class focus enumeration can seed backward from the class instead of
  scanning all nodes;
- shape checks can be cost ordered;
- SPARQL and path operations can run over native indexed operators;
- memoization and CSE reduce repeated sub-shape work.

The semantic contract remains:

```text
validate(plan(S), G) == validate(S, G)
```

up to expected reporting/provenance differences introduced by normalization.

## 5. SHACL-AF Inference Algorithm

Inference is a separate pre-validation phase. Rules are algebraic objects:

```text
rule = selector + conditions + head
```

The selector chooses focus nodes. Conditions are shapes that must hold at the
focus. The head emits triples via triple-rule templates or SPARQL `CONSTRUCT`.

Inference semantics:

```text
G_0 = asserted data graph
G_{i+1} = G_i union all rule heads whose bodies hold in G_i
stop when G_{i+1} = G_i
```

The implementation is least-fixpoint forward chaining with scheduling:

- rules are grouped by `sh:order`;
- tied rules observe the same snapshot;
- additions from an earlier order group are visible to later groups in the same
  pass;
- later passes reactivate only rules whose dependencies can observe changed
  predicates;
- focus sets are cached per selector and invalidated when relevant predicates
  change;
- SPARQL construct rules may use a frozen indexed snapshot for efficient reads,
  with controlled updates after candidate triples are committed.

Termination is preserved for the supported subset:

- inferred triples are deduplicated against the context;
- triple rules combine existing terms;
- SPARQL `CONSTRUCT` outputs with fresh blank nodes are rejected in the supported
  path;
- unsupported function features are reported according to policy rather than
  silently trusted.

The default validation pipeline runs inference first, then validates the
materialized graph.

## 6. Witnessing

Witnessing answers a different question from validation reports:

```text
Why did this algebraic shape fail, structurally, at this node?
```

It is the lossless sibling of `explain`. It traverses the same shape arena and
uses the same satisfaction oracle, but retains repair-relevant information:

- the failed sub-DAG of the shape;
- structured paths, not rendered strings;
- count gaps: path, qualifier, have, min/max;
- path support: which concrete triples make a value reachable;
- relational value sets and offending pairs;
- failed disjunction branches as alternatives;
- failed conjunction branches as joint obligations;
- a dual satisfaction trace for `not` repair.

The main failure witness forms are:

```text
Atom(shape, node, reached_by, produced_by)
Relational(shape, node, kind, lhs, rhs, offending)
Closed(shape, node, offenders)
Not(shape, node, inner_sat_trace)
All(shape, node, failed_children)
Any(shape, node, failed_branches)
CountLow(shape, node, path, qualifier, have, min, sibling_qualifiers)
CountHigh(shape, node, path, qualifier, matched, max, per_value)
Opaque(shape, node)
```

`SatTrace` is the dual: why a shape currently holds, so that a failed `not phi`
can be repaired by breaking `phi`.

Important witness insights:

- A failed `and` means all failed children must be repaired.
- A failed `or` means every branch failed, but repairing any one branch suffices.
- A failed `not phi` means `phi` currently holds; repair crosses to a deletion or
  break-support problem.
- A too-low count is a construction problem.
- A too-high count is a deletion or value-fix problem.
- A failed universal, encoded as `exists <= 0 pi . not phi`, drills into each
  offending value and witnesses the inner `phi` failure.
- Universals conjoined above a count can be vacuously satisfied while a path is
  empty. When a new value is added for the count, that value must also satisfy
  the sibling universals on the same path. Witnessing carries these as
  `sibling_qualifiers`.

`PathSupport` is a concrete positive reachability certificate:

```text
Empty        reflexive identity support; nothing to cut
Edge(t)      one triple supports the reachability
Chain([...]) one sequence/star route through existing triples
Alt([...])   retained certificate branches, when explicitly constructed
```

It is not guaranteed to enumerate every route, especially for alternatives and
cyclic closures. It bridges logical evidence to candidate graph edits; the
repair gate remains responsible for proving that a proposed cut actually fixes
the violation.

## 7. Repair as Abduction

Validation asks:

```text
G, v |= phi ?
```

Repair asks for the edit space:

```text
repair(phi, v) = { DeltaG | (G (+/-) DeltaG), v |= phi }
```

The engine does not enumerate that set as concrete graphs. It produces a
parametric repair tree:

```text
RepairTree =
    Noop
  | Blocked(reason)
  | Edits(edits, holes)
  | All(children)
  | Any(children)
  | Repeat(body, min, max)
```

The tree mirrors the proof structure of satisfaction:

- `Noop`: already satisfied.
- `Blocked`: no data-graph repair is known in scope.
- `Edits`: add/delete triple patterns, possibly with holes.
- `All`: all children must be satisfied.
- `Any`: choose one repair branch.
- `Repeat`: instantiate a body multiple times, usually for count deficits or
  surplus deletions.

Holes are typed constraints on terms:

```text
AnyNode
Fresh
Const(term)
Typed(value_type)
Kind(node_kind_set)
OneOf(term_set)
ConformsTo(shape_id)
ConformsToAll(shape_ids)
```

This is a deliberate driver boundary. The library can say "a value is needed
here and it must satisfy this datatype/shape/class"; it does not decide whether
to reuse an existing term, mint a new blank node, ask a human, query an external
catalog, or call a model.

## 8. Synthesis: Witness to RepairTree

Synthesis is two mutually recursive folds:

```text
repair(Witness)  -> RepairTree   additive direction
break(SatTrace) -> RepairTree   deletive direction
```

They cross at negation:

```text
repair(Not(inner_holds)) = break(inner_holds)
break(NotHeld(inner_fails)) = repair(inner_fails)
```

Conceptual mapping:

- Failed value atom on a reached value: delete the bad produced edge and add a
  replacement edge to a typed hole.
- Failed focus-scoped atom: blocked, because data edits cannot turn an IRI focus
  into a literal focus or change focus identity.
- Failed `closed(Q)`: delete every offending predicate/object pair not allowed by
  `Q`.
- Failed conjunction: `All` over child repairs.
- Failed disjunction: `Any` over branch repairs.
- Too-low count: `Repeat` a path-materialization body `min - have` times.
- Too-high plain count: repeat deletion of selected matched values.
- Universal failure: repair each offending value in place.
- Failed `disjoint`: delete the shared value from one side or the other.
- Failed `uniqueLang`: delete one value from each duplicated language pair.
- Failed `lt`/`le`: currently delete offending left-side values; value-shrinking
  is deferred.
- Failed `equals`: currently blocked/unsupported for full reconciliation.
- Opaque SPARQL/expression: blocked.

Path materialization is how logical paths become add edits:

- `Pred(p)`: add `(subject, p, ?value)`.
- `Inverse(Pred(p))`: add `(?value, p, subject)`.
- `Seq`: add a chain, using fresh holes for intermediate nodes.
- trailing `Star` in a sequence may be satisfied reflexively;
- `Star(inner)` can be materialized by one inner hop;
- arbitrary alternatives/interior stars are left to future expansion or drivers.

For class constraints, this matters because `sh:class C` lowers to a path like:

```text
rdf:type / rdfs:subClassOf*
```

The star can be satisfied reflexively, so adding `node rdf:type C` is a valid
repair even though the lowered path contains a closure.

Blocked propagation is normalized:

- `All` with a blocked child becomes blocked.
- `Any` drops blocked branches.
- `Any` with all branches blocked becomes blocked.

So a satisfiable branch does not contain a hidden dead subtree.

## 9. Plans and Instantiation

A repair tree is not an edit. It becomes an edit only after a driver supplies a
plan:

```text
Plan =
  branch[node_id]  -> child index for Any
  count[node_id]   -> instance count for Repeat
  binding[hole_id] -> concrete RDF term
```

Instantiation is a pure fold:

```text
instantiate(tree, plan) -> {
  delta: { add: triples, delete: triples },
  open_holes: holes still needing terms,
  open_choices: Any/Repeat nodes still needing choices
}
```

It never validates, never picks a branch, and never chooses a term.

Partial plans are useful. A driver can set repeat counts first, instantiate to
discover per-instance holes, bind those holes, then instantiate again. Repeat
hole renaming is deterministic for a given tree/count plan so this two-pass
workflow is stable.

RDF position constraints are enforced during instantiation: subjects must be
IRIs or blank nodes, predicates must be IRIs, and objects may be any term.
Ill-typed triple patterns remain unresolved rather than being emitted as invalid
triples.

## 10. The Gate

Repair candidates are sound only if they do not trade one violation for another.
The gate is whole-graph by design.

Given a candidate `DeltaG`:

```text
baseline = validate(D, context, S)
D'       = apply(D, DeltaG)
C'       = apply(context, DeltaG)
patched  = validate(D', C', S)
outcome  = diff(baseline.violations, patched.violations)
```

Edits are applied as deletes first, then adds, so re-add wins if both mention the
same triple.

The outcome is:

```text
fixed       violations present before and gone after
introduced  violations absent before and present after
remaining   violations present both before and after
```

Identity is currently `(focus, statement)`, matching validation's grouped
violation granularity.

The gate decides nothing beyond classification:

```text
sound    iff introduced is empty
progress iff sound and fixed is non-empty
```

The driver decides whether to accept, reject, backtrack, ask a user, expand the
frontier, or terminate.

A future optimized gate can replace full revalidation with affected-set
revalidation, but the semantic contract is the same diff over validation
outcomes.

## 11. Driver Strategies

The library computes the repair space and validates candidates. A driver owns
policy:

- which focus to repair first;
- whether to repair one violation or a whole focus group;
- branch selection;
- repeat counts;
- hole bindings;
- cost/minimality preferences;
- acceptance criteria;
- backtracking and termination.

A minimal fixpoint loop:

```text
loop:
  witnesses = witness_violations(G, S)
  if witnesses empty: success

  choose focus group W
  tree = synthesize_focus(W)
  plan = choose branches, counts, bindings
  inst = instantiate(tree, plan)
  outcome = gate(G, S, inst.delta)

  if outcome.progress:
    G = G (+/-) inst.delta
  else:
    try a different plan or mark blocked
```

Per-violation greedy repair is simple but incomplete. It can stall when fixing
one node introduces a violation on another node that would be fixed by a joint
edit. Per-focus joint repair is stronger because `synthesize_focus` builds an
`All` over all statement failures for a focus.

A practical middle ground is frontier expansion:

1. propose a repair for the current focus;
2. if the gate reports introduced violations, pull those witnesses into the same
   planning group;
3. re-plan jointly;
4. bound the expansion to avoid runaway cascades.

### Monomorphism / Reuse Driver

The reuse driver treats a repair template as a graph pattern. It seeks a maximum
partial embedding into existing data:

- mapped holes reuse existing nodes;
- `Fresh` holes are forced unmapped;
- unmapped non-fresh holes are minted or delegated to another source;
- sidecar hole constraints check that reused nodes satisfy datatype/shape/kind
  obligations;
- each embedding becomes a plan and is gated.

This is valuable for asset/model ingestion workflows where the best repair is
often "wire this shape to an existing node that already almost matches."

### Enumeration Driver

Enumeration is useful for finite choices:

- `Const`;
- `OneOf`;
- small repeat counts;
- small candidate lists for typed/kind constraints.

It is a good correctness harness for the repair IR and gate, but it is not a
general repair strategy over infinite literal domains or large graphs.

### LLM / Human Driver

Semantic holes such as "invent a plausible label" or "choose an existing
equipment node from context" can be delegated to a human or model. The important
rule is that generated choices are untrusted:

```text
model/human proposal -> instantiate -> gate -> accept only if policy allows
```

The gate is the safety boundary.

### ASP / Joint Solver Driver

An ASP-style driver can encode the repair tree and validation semantics as a
bounded generate-define-test problem:

- Generate: choose branches, repeat instances, hole bindings, and deletions from
  the template-bounded repair space.
- Define: represent post-repair triples, path reachability, and shape violation
  predicates.
- Test: require every root shape in the joint group to conform.
- Optimize: minimize weighted edits, prefer reuse over minting, or encode
  domain-specific costs.

The key semantic trick is to lower finite failure/violation rather than trying to
derive coinductive conformance directly:

```text
conf(S, V)  :- node(V), not viol(S, V).
viol(and, V)   :- viol(child, V).
viol(or, V)    :- viol(child1, V), viol(child2, V).
viol(not S, V) :- conf(S, V).
viol(count, V) :- count reachable conforming values below min or above max.
```

This gives a finite constructive repair problem while the final decoded answer
set still passes the gfp validation gate. Non-logical leaf checks such as
datatype, regex, node kind, and term ordering can be precomputed by the engine
and emitted as facts.

ASP is strongest for joint minimal repairs over finite candidate pools. It still
needs help for infinite fresh literal domains and opaque SPARQL constraints.

## 12. Current Boundaries and Proposal Hooks

The design leaves several clear proposal directions:

- Affected-set gate: replace full revalidation with dependency-guided
  revalidation while preserving the same `fixed/introduced/remaining` contract.
- Cost model: standardize edit, reuse, mint, deletion, and semantic-invention
  costs so drivers can compare plans.
- Provenance side table: connect normalized arena IDs and repair edits back to
  source SHACL components and author messages.
- Structural expansion for `ConformsTo`: let synthesis recursively build a
  sub-shape for a fresh value instead of leaving all structural obligations to
  the driver.
- Better relational repair: synthesize reconciliation for `equals`, value edits
  for `lt`/`le`, and less coarse choices for pairwise constraints.
- Schema repair mode: optionally propose shape edits such as relaxing a bound or
  widening `closed(Q)`, clearly separated from data repair.
- SPARQL repair: characterize safe subsets of SPARQL constraints where repair
  can be synthesized from the query plan; keep arbitrary SPARQL blocked.
- Joint solving: make frontier expansion and ASP-style planning first-class
  reference drivers.
- Candidate providers: define a pluggable candidate interface for catalogs,
  vector search, ontology lookup, human review, or model-generated literals.
- Recursive repair policy: distinguish data edits that construct finite support
  from situations that only depend on coinductive support.

## 13. End-to-End Mental Model

The complete flow is:

```text
RDF data + shapes
  -> parse/lower to algebra
  -> normalize shape/rule arena
  -> stratification analysis
  -> lfp SHACL-AF inference
  -> gfp validation
  -> structured witnesses for failures
  -> repair-tree synthesis
  -> driver chooses plan
  -> instantiate plan to DeltaG
  -> whole-graph gate
  -> driver accepts/rejects/replans
```

The main architectural insight is the separation of concerns:

- The algebra gives a compact formal object shared by validation, inference,
  witnessing, repair, optimization, and solver lowering.
- Validation and inference have explicit, different fixpoint semantics.
- Witnesses are proofs of failure, not user-facing reports.
- Repair trees describe a space of possible edits, not a chosen repair.
- The gate is the semantic arbiter for proposed edits.
- Drivers are policy modules outside the core engine.

That separation is what makes the system proposal-friendly: one can improve
planning, solving, candidate generation, UI, or cost models without changing the
meaning of validation or the soundness boundary of repair.
