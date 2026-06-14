# 07 — Repair Drivers (reference)

The repair *library* ([`06-repair.md`](06-repair.md)) decides nothing: it
witnesses violations, exposes the repair space (`RepairTree`), turns a caller's
`Plan` into edits (`instantiate`), and validates a candidate (`gate`). A
**driver** supplies the data sources, makes the choices, and runs the loop.

This document gives **reference drivers** — worked examples built entirely on the
06 API. They are illustrative, not privileged: a real integration brings its own
data sources and policy and may use, adapt, or ignore any of them. Nothing here
is invoked by the library on its own.

All drivers share one contract from doc 06: every candidate `ΔG` is `gate`-d, and
*the driver* decides what the verdict means and whether to apply.

---

## 1. The loop you own — a reference fixpoint driver

Repairs interact, and a witness computed against `G` goes stale the moment `G`
changes, so repair is iterative. A minimal driver:

1. `witness_violations(G, S)` → current violations, grouped per focus.
2. Pick a focus; `synthesize_focus` → its `RepairTree`.
3. Choose: walk the tree, fill holes / pick `Any` branches / set `Repeat` counts
   into a `Plan` (using your data sources — §3–§6 below show strategies).
4. `instantiate(tree, plan)` → `ΔG`.
5. `gate(G, S, ΔG)` → `RepairOutcome`. If `introduced` is empty and `fixed` is
   non-empty, **you** may accept: `G := G ⊕ ΔG`. Otherwise try another `Plan`.
6. Re-witness and repeat until `G ⊨ S` or no plan makes progress.

**Termination** is the driver's responsibility, but it is easy to guarantee:
require each accepted `ΔG` to shrink the violation set (`gate` reports this), so
the loop is monotone and finite; report a focus with no progress-making plan as
*unrepairable in scope* rather than retrying it forever.

**Choosing focus order, accept criteria, and backtracking is policy** — a CLI
might prompt a human at step 3; a batch job might take the first sound plan; an
optimizer might enumerate plans and keep the cheapest.

---

## 2. Per-focus joint vs. per-violation greedy

`synthesize_focus` hands you an `All` over everything a focus violates, so you
*can* fill one `Plan` that fixes them together. Two driver stances:

- **Greedy / per-branch** (drivers §3, §4-enum): fill each part independently,
  lean on `gate` to reject a `ΔG` that breaks a sibling, and `Any`-backtrack.
  Simple and sound, but **incomplete**: it can stall where a repair exists only by
  fixing a focus *and* the node its `ΔG` would break *together*.
- **Joint** (driver §5, ASP): encode the whole group as one problem and solve it
  in a single shot, so cross-constraint cascades are handled by construction.

Closing the greedy gap without a full joint solver is the optional
**frontier-expansion** strategy: when `gate` reports `introduced` violations at
node `w`, pull `w`'s witness into the *same* `synthesize_focus` group and re-plan,
iterating until the joint `ΔG` is sound. It is a strictly more powerful driver
behind the same gate, at the cost of a possibly-cascading frontier (bound it).

---

## 3. Monomorphism / reuse driver

The reuse-oriented driver consumes `render(&RepairTree) → RepairProgram` (06 §7)
and binds holes to **existing** nodes by graph matching — the BuildingMOTIF
ingest mechanic.

It does **not** seek a full embedding of a pattern's `add` graph into `G` — if
the whole add-graph already matched, the focus would not be violating. It seeks
the **maximum partial embedding**: an injective map from a *subset* of hole nodes
to existing `G` nodes such that (a) every mapped edge of `add` exists in `G`, and
(b) each mapped hole's bound node conforms to its sidecar shape (06 §7). Unmapped
holes are **minted fresh**. That is "find the subgraph of the data that overlaps
the template," reuse maximized, remainder constructed.

- **Reuse vs. mint** is the knob: prefer embeddings binding more holes to existing
  nodes; `Fresh` holes are forced unmapped.
- **`Repeat` blocks**: bind up to `max` existing matching instances, mint enough
  fresh ones to reach `min`.
- Each embedding becomes a `Plan`; `instantiate` → `ΔG`; `gate` confirms.

This is the recommended first concrete driver: it reuses machinery the project
already understands and yields immediately useful "reuse this existing node"
repairs.

---

## 4. Enumeration and LLM drivers

- **Naive enumeration.** For finite holes (`Const`, `OneOf`, small `Repeat`
  counts) enumerate `Plan`s directly and keep the first that `gate`s clean.
  Cheapest driver; primarily a correctness check on the IR and the gate.
- **LLM.** Serialize the `RepairTree` (or its `RepairProgram` rendering) plus
  surrounding graph context, and ask the model to fill the *semantic* holes a
  `Typed(pattern)` or a plausible label that enumeration and monomorphism cannot
  invent. Non-deterministic, so the `gate` is load-bearing: discard any `ΔG` whose
  `RepairOutcome` is unsound. The driver, not the library, owns the prompt, the
  retries, and the acceptance bar.

---

## 5. ASP driver — joint solving + minimality

The ASP driver takes a focus's *joint* group (§2) and lowers it to a single
answer-set program whose **answer sets are exactly the sound repairs**. It is the
driver that closes cross-constraint cascades and finds *minimal* repairs by
construction: integrity constraints for "must conform," `#minimize` for parsimony.

The lowering is mechanical for one reason: the recursion semantics (doc 03) is
**stratified negation**, and the engine already runs it as least-fixpoint forward
chaining (`infer.rs`) with a computed stratum order (`shifty-opt::analyze`). So
`φ` *is already a logic program* — lowering it reuses the same strata, not a
reimplementation of validation. (Even so, decoded answer sets still pass the 06
§8 `gate` — belt and suspenders, and it catches the deferred-value cases below.)

### 5.1 Generate–define–test

- **Generate** — choose hole bindings, repeat-instance counts, and deletions,
  *restricted to what the `RepairTree` proposes* (not "any triple in the
  universe"), so the search space is small and finite.
- **Define** — the post-repair graph `holds/3`, the path relations, and the
  violation predicate `viol/2` (= the witness, 06 §5) as Datalog over `holds`.
- **Test** — `:- not conf(root, focus).` for every root shape in the focus group.
- **Optimize** — `#minimize` over the chosen edits.

### 5.2 Facts

`G` becomes `triple/3` over interned term constants (a symbol table both ways).
The *non-logical* leaf checks ASP cannot express — `sh:datatype`, numeric range,
`sh:pattern`, `sh:nodeKind`, `sh:in`, and `Lt`/`Le`/`Eq` comparisons — are
**precomputed by the engine** and emitted as facts (`sat(LeafShape, X)`,
`lt(X,Y)`, …), reusing `value_type_holds`/`compare_terms`.

### 5.3 Generate (template-bounded)

```prolog
{ bind(H, X) : cand(H, X) } = 1 :- hole(H).      % one binding per hole
cand(H, X) :- reuse(H, X).        % existing node passing the 06 §7 sidecar shape
cand(H, X) :- fresh(X), open(H).  % a minted node, unless the hole is Fresh-only
add(focus, p, X) :- bind(hv, X).  % one per Add(TriplePattern), holes substituted
{ del(S,P,O) : delcand(S,P,O) }.  % deletions from the break-options / delete-set
```

A `Repeat{min,max}` block becomes instance choice — `{ use(B,I) : inst(B,I) }`
with fresh hole copies per instance; `min`/`max` are enforced by the matching
`Count` violation rule (§5.4), so the solver and `#minimize` pick the smallest
conforming instantiation. The `fresh` pool is sized from the template's demand.

### 5.4 Define — and why we lower `viol`, not `conf`

```prolog
holds(S,P,O) :- triple(S,P,O), not del(S,P,O).
holds(S,P,O) :- add(S,P,O).
reach(pred_p, X, Y) :- holds(X, p, Y).                      % π = Pred(p)
reach(seq_ab, X, Z) :- reach(a, X, Y), reach(b, Y, Z).      % π = Seq
reach(star_b, X, X) :- node(X).                             % π = Star (closure)
reach(star_b, X, Z) :- reach(star_b, X, Y), reach(b, Y, Z).

conf(S, V)         :- node(V), not viol(S, V).
viol(s_type, V)    :- node(V), not sat(s_type, V).
viol(s_and, V)     :- viol(C, V).                  % some conjunct fails
viol(s_or, V)      :- viol(c1,V), viol(c2,V).      % all disjuncts fail
viol(s_not, V)     :- conf(c, V).                  % ¬φ fails iff φ holds
viol(s_count, V)   :- #count{ U : reach(pi,V,U), conf(q,U) } < min.
viol(s_count, V)   :- #count{ U : reach(pi,V,U), conf(q,U) } > max.
```

**Validation is gfp but ASP stable models are lfp.** For positive recursion
(`S := ∃knows.S`) gfp lets a cycle conform coinductively; stable-model semantics
demands finite justification and would not. Lowering `viol` (the
finitely-justified failure — exactly the witness, 06 §5) and defining `conf` as
its stratified negation makes the reading **lfp/finitely-justified — which is the
*correct* semantics for repair**: a repair must actually *construct* supporting
structure, not lean on a coinductive cycle. The divergence is contained because
the driver only runs on genuine witnesses (nodes that fail the real gfp
validator) and every answer set passes the gfp `gate`. Stratification of `viol`
is guaranteed by the same sign-balance test the engine enforces
(`shifty-opt::strata`; the `∀`-encoding's paired negative edges compose to
positive).

### 5.5 Test, optimize, decode

```prolog
:- not conf(root_a, focus).        % every root shape in the focus group — jointly
:- not conf(root_b, focus).
#minimize { Wa,S,P,O : add(S,P,O) ; Wd,S,P,O : del(S,P,O) }.
```

The conjoined integrity constraints are the joint solve: one model satisfies
*all* of the focus's shapes at once, so adding a value to fix a `minCount` is
forced to also pass the `datatype`/`closed` rules that constrain it. Edit weights
encode the preference order (reuse < mint, add/delete bias), so the optimum is the
minimal, most-reuse repair. Each answer set decodes (`add`/`del` → terms →
`GraphDelta`) into a `Plan`-equivalent and passes the 06 §8 `gate`.

### 5.6 Boundaries

- **Opaque SPARQL.** `Shape::Sparql` cannot be lowered: freeze its current truth
  as a fact and forbid edits to its footprint, or treat repairs touching it as
  `Blocked`.
- **Infinite literal domains.** A `Typed(τ)` *fresh* hole (e.g. "any integer ≥ 0")
  has no finite candidate set. ASP decides **structure, reuse, and counts**; the
  concrete novel literal is left open for the enumeration/LLM drivers. ASP is
  strongest at "which existing nodes to wire, how many, which to drop,"
  complementary to monomorphism (reuse) and the LLM (invention).

---

## 6. Choosing a driver

| Driver | Fills holes by | Strength | Joint? |
|---|---|---|---|
| Monomorphism (§3) | matching existing subgraphs | reuse, structural overlap | no (greedy) |
| Enumeration (§4) | brute force over finite domains | simplicity, IR oracle | no (greedy) |
| LLM (§4) | model proposal | semantic/infinite holes | no (greedy) |
| ASP (§5) | constraint solving | minimality, cascades | yes |

A real integration will often combine them: ASP or monomorphism for structure
and counts, the LLM for the leftover semantic literals, all behind the one `gate`.
