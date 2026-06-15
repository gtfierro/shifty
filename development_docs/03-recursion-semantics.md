# 03 — Recursion Semantics (decision)

The paper ([`00-formalism.md`](00-formalism.md)) is non-recursive and the W3C
spec leaves recursion **undefined** (gap-analysis **D**). SHACL shapes can
reference each other cyclically (`sh:node` / `sh:property` /
`sh:qualifiedValueShape`), and our `ShapeArena` represents that natively. This
document pins down the semantics we evaluate against. It is load-bearing:
**every optimization in Layer 4+ is only sound relative to this choice**, and it
replaces the provisional cycle-breaking stack guard used in Layer 3.

## The decision

1. **Stratified semantics** over the shape (and rule) dependency graph, with a
   clear **diagnostic** when a schema is not stratifiable. No silent guessing,
   no well-founded 3-valued machinery for now (revisitable if we later need
   totality).
2. Within each stratum:
   - **Validation** uses the **greatest fixpoint (gfp)** for positive
     recursion — *coinductive* "no reachable counterexample."
   - **Inference (AF rules)** uses the **least fixpoint (lfp)** — you only
     derive finitely-justified triples.

These run in separate phases (infer → then validate), so the different fixpoint
directions never conflict.

## Why stratified (decision a)

Pure positive recursion has clean fixpoints; **negation through a cycle** does
not. The minimal paradox:

```
S := ¬ ∃p. S          "v conforms iff no p-successor conforms"
```

over a `p`-self-loop has no consistent 2-valued answer. Stratification forbids
exactly this: a schema is **stratifiable** iff its shape-dependency graph has no
cycle through a *negative* edge. We evaluate stratum by stratum (lower strata
fully decided first). This is deterministic, 2-valued, optimizer-friendly, and —
crucially — **the same machinery serves AF rule inference** (Datalog with
stratified negation), unifying recursive validation and inference under one
engine. Non-stratifiable schemas are reported, not guessed.

### Polarity must be computed semantically, not syntactically

Our IR encodes `∀π.φ` as `∃≤0 π.¬φ` (a `Count` with `max:0` over `¬φ`). So a
*positive* SHACL constraint (`sh:node S` in a property shape) looks
*syntactically negative* in the IR. But two anti-monotone operators compose to
monotone, so the **semantic** polarity is positive. The dependency analysis must
therefore track monotonicity, not surface `¬`:

| construct | polarity of the referenced shape |
|---|---|
| `sh:node` / `sh:property` / `minCount` / `∃≥ⁿ` (qualifier under a lower bound) | **positive** (monotone) |
| `sh:not` | **negative** |
| `maxCount` / `∃≤ⁿ` / qualified-max (qualifier under an upper bound) | **negative** (anti-monotone) |
| `closed` / `disjoint` | **negative** |

Because `Count{min,max}` carries one qualifier constrained by *both* bounds,
for analysis we **un-fuse** it: the `min` part contributes a positive edge, the
`max` part a negative edge. A qualifier governed by both (a genuine
`qualifiedValueShape` with min *and* max) yields a negative (non-monotone) edge.

## Why gfp for validation, lfp for inference (decision b)

lfp and gfp **only differ on cyclic data**; on DAGs they coincide. The contrast,
for `conforms(v) ⟺ isPerson(v) ∧ every knows-neighbour conforms` over a mutual
`knows` cycle of two Persons:

- **lfp** (build up from grounded base cases): the cycle never grounds out →
  both nodes **fail**. This is the *inductive / finiteness* reading ("must
  bottom out").
- **gfp** (whittle away provable violators): no reachable concrete violation →
  both nodes **conform**. This is the *coinductive / safety* reading ("no
  reachable counterexample").

For validation, the coinductive reading is what universal constraints usually
*mean* ("everyone I transitively follow is verified"), and it avoids flagging
legitimate cyclic data — decisive for cyclic social graphs (Bluesky). Hierarchy
constraints (Brick part/feeds DAGs) are acyclic, so the choice is moot there. We
therefore default validation to **gfp**.

Inference must be **lfp**: a rule fires only when its body is *actually*
satisfied by derived/asserted triples; you cannot materialize a fact because it
justifies itself. lfp is also the standard semi-naive rule-evaluation fixpoint.

The trade-off: an *inductive* "this structure must be acyclic/finite" constraint
is not expressible by default under gfp (it would need an explicit acyclicity
check). Stratification supports either fixpoint direction per positive stratum,
so this is **not a one-way door** — gfp is the documented default, overridable
later if a concrete need appears.

## Implications for Layer 4+

- Build the shape/rule **dependency graph** with **polarity-aware** edges
  (un-fusing `Count` min/max as above).
- **SCC + stratification**: condense SCCs; a schema is stratifiable iff no SCC
  contains a negative internal edge. Emit a diagnostic naming the offending
  cycle otherwise.
- Evaluate strata bottom-up; within a positive stratum compute the **gfp**
  (validation) by iterating downward from "all candidate (shape,node) pairs
  conform," removing on a concrete violation. Memoize per `(shape, node)`.
- This **replaces** the Layer 3 stack guard (which returned `true` on revisit —
  an ad-hoc gfp that happened to be right for positive cycles but had no
  principled footing under negation).
- The same stratified fixpoint framework drives **AF rule inference** (lfp) in
  Layer 6.
