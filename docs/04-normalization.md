# 04 — Normalization checklist (Layer 4)

Semantics-preserving rewrites of the IR, validated against the Layer 3 oracle
(`validate(normalize(S), G) ≡ validate(S, G)` at the conforms + violation-focus
level — see the W3C harness, which now cross-checks this on every core test).

Tags: **[done]** implemented · **[do]** sound, high-value, queued · **[todo]**
needs more machinery or lower value. Soundness is under the gfp validation
semantics ([`03-recursion-semantics.md`](03-recursion-semantics.md)); all rules
here are per-node truth-functional / relational, hence gfp-safe.

## 0. Enabler
- **Hash-consing / CSE** — dedup structurally-identical arena nodes. **[done]**
  (`shacl_opt::normalize`; acyclic nodes are interned through a cons-table,
  recursive SCCs are rebuilt preserving sharing.)
- **Arena compaction** — only reachable, deduped nodes survive. **[done]**
  (free: the rebuild interns from the schema roots, so no orphans.)

## 1. Boolean `φ`
- Flatten `And`/`Or`, `¬¬φ = φ`, `And([])=⊤`, `Or([])=⊥`. **[done]**
- ⊤/⊥ absorption: `φ∨⊤=⊤`, `φ∧⊥=⊥`, `φ∨⊥=φ`, `φ∧⊤=φ`. **[done]**
- Idempotent dedup `φ∧φ=φ`, `φ∨φ=φ` (sort+dedup child ids). **[done]**
- Complementation `φ∧¬φ=⊥`, `φ∨¬φ=⊤` (via CSE ids). **[done]**
- Canonical child ordering (sort by id) so equal And/Or dedup. **[done]**
- NNF — push `¬` inward; `¬(∃≥n π.φ)=∃≤(n−1) π.φ`, `¬(∃≤m π.φ)=∃≥(m+1) π.φ`
  (flip bounds, keep qualifier positive). **[done]** (negation is not pushed
  into recursive nodes, which stay as `¬`-literals).
- Absorption `φ∧(φ∨ψ)=φ`. **[todo]**
- Complementary atoms `¬nodeKind(IRI)=nodeKind(Blank|Literal)`. **[todo]**

## 2. Count `∃[min..max] π.φ`
- Trivial `∃≥0 / unbounded ⇒ ⊤`. **[done]**
- Unsat bounds `min > max ⇒ ⊥`. **[done]**
- Qualifier collapse `∃≥1 π.⊥=⊥`, `∃≤m π.⊥=⊤`. **[do]**
- Merge counts on same `(π,φ)`: `∃≥a ∧ ∃≥b = ∃≥max`, `∃≤a ∧ ∃≤b = ∃≤min`;
  fuse a separate min/max count into one node (one path eval). **[done]**
  (conjunction context only)
- `id`-path collapse `∃≥1 id.φ=φ`, `∃≤0 id.φ=¬φ`. **[do]**
- Empty-path `∃≥1 Alt([]).φ=⊥`. **[todo]**

## 3. Path `π` (Kleene + converse)
- Flatten `Seq`/`Alt`, `π·id=π`, `(π⁻)⁻=π`, `id*=id`, `(π*)*=π*`. **[done]**
  (smart constructors)
- Converse push-down to canonical (`(π₁·π₂)⁻=π₂⁻·π₁⁻`, …, inverse only wraps a
  `Pred`). **[do]**
- `Alt` ACI (idempotence + ordering); empty-relation zero laws. **[do]**
- Star laws `(π∪id)*=π*`, `π*·π*=π*`. **[todo]**
- Distributivity / prefix factoring. **[todo → Layer 5 plan-time]**

## 4. Value types `τ`
- Flatten `And`, drop `any`. **[done]** (smart constructor)
- Range/length tightening (merge bounds). **[done]** (`ValueType::normalize`
  folds same-family numeric/length bounds to the tighter pair; conjoined
  `test(τ)` siblings are fused in `mk_and` via `merge_value_types`. Numeric
  ordering covers xsd numeric only — incomparable bounds are kept, never
  merged.)
- Facet unsat → ⊥ (empty range, `len.min>max`, conflicting datatypes). **[done]**
  (`ValueType::normalize` returns `None`; the normalizer maps it to ⊥, which the
  Boolean layer then absorbs. `test(any)` ⇒ ⊤.)
- Cross-facet unsat (`datatype(xsd:string) ∧ NumericRange`). **[todo]**
  (needs a datatype↔facet compatibility table; same blocker as non-numeric
  bound ordering for dates/durations.)

## 5. Schema level
- Statement/selector dedup. **[do]**
- Selector canonicalization (`HasPath(Pred(q),⊤) ⇒ HasOut(q)`). **[todo]**
- Target merging (same selector ⇒ conjoin) — changes reporting granularity.
  **[todo]**

## Caveats
- **Reporting drift**: normalization changes `@id`s/structure, so per-constraint
  *messages* change; oracle equivalence is conforms + violation-focus, not
  message-identical (ties to the deferred provenance side-table).
- **Recursion**: CSE across cycles is id-based and safe; inlining/unfolding into
  cycles is not (not attempted). Recursive SCCs get only light rebuilding (no
  collapse) to preserve the cycle. Normalization preserves stratifiability.
- **Ordering** (And/Or by selectivity, short-circuit) is a *plan* concern
  (Layer 5), not a normal form.
