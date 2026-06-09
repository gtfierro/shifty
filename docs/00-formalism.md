# 00 — The SHACL Formalism (Core IR)

This document fixes the formal core our implementation is built on. It is the
SHACL fragment of the *Common Foundations for SHACL, ShEx, and PG-Schema* paper
(Ahmetaj et al., arXiv:2502.01295), **specialized to RDF** and stripped of all
property-graph / ShEx / PG-Schema material. Everything downstream (parser,
optimizer, engine) targets the types sketched here.

The companion docs are [`01-gap-analysis.md`](01-gap-analysis.md) (where real
SHACL/SHACL-AF diverges from this core and how we patch the holes) and
[`02-roadmap.md`](02-roadmap.md) (the layered build).

---

## 1. Data model

The paper works over *common graphs* `G = (E, ρ)` that abstract both RDF and
property graphs: `E ⊆ N × P × N` are node-to-node edges and `ρ : N × K ⇀ V` is a
finite partial map from node/key pairs to values. It deliberately keeps
predicates `P` (edges) and keys `K` (properties) distinct, so paths navigate only
node→node and never "through" a value (Appendix A.1 / A.2).

**For RDF this distinction is artificial** (Appendix A.1: "a common graph can be
seen as a finite set of triples"). We therefore re-unify it — this is our first
intentional deviation, see gap-analysis **D0**:

```
Term   = IRI | Blank | Literal
N      = IRI ∪ Blank            -- "nodes"
V      = Literal                -- "values"
P = K  = IRI                    -- predicates and keys unified
G      ⊆ N × IRI × Term         -- a set of RDF triples
```

A path may traverse any triple regardless of the object's term kind (RDF imposes
no edge/value wall), so path evaluation is a relation over `Term × Term` rather
than `N × N`.

### Value types `T`

The paper assumes an enumerable set of **value types** `τ ∈ T`, each denoting a
set `⟦τ⟧ ⊆ V`, with a top type `any` (`⟦any⟧ = V`). This single abstraction
absorbs datatypes, numeric interval bounds, string length bounds, and regex
patterns (Appendix B). We adopt it as a *composable decidable predicate on
literals*:

```rust
/// A decidable predicate over literal values, ⟦τ⟧ ⊆ V.
pub enum ValueType {
    Any,
    Datatype(IriBuf),                       // sh:datatype
    NumericRange { lo: Option<Bound>, hi: Option<Bound> }, // sh:min/maxIn/Exclusive
    Length { min: Option<u64>, max: Option<u64> },          // sh:min/maxLength
    Pattern { regex: String, flags: String },               // sh:pattern
    // extensions (gap-analysis): language membership lives here too
    LangIn(Vec<String>),                    // sh:languageIn  (deviation L1)
    And(Vec<ValueType>),                    // intersection (for stacked facets)
}

pub struct Bound { pub value: Literal, pub inclusive: bool }
```

---

## 2. Path expressions (Def. 3, Table 1)

```
π ::= id | q | π⁻ | π · π′ | π ∪ π′ | π*           (q ∈ P)
```

Denotation `⟦π⟧^G ⊆ Term × Term`:

| `π`        | `⟦π⟧^G`                                                        |
|------------|----------------------------------------------------------------|
| `id`       | `{ (v, v) }`                                                   |
| `q`        | `{ (v, u) | (v, q, u) ∈ G }`                                   |
| `π⁻`       | `{ (v, u) | (u, v) ∈ ⟦π⟧ }`                                    |
| `π · π′`   | `{ (v, w) | ∃u. (v,u) ∈ ⟦π⟧ ∧ (u,w) ∈ ⟦π′⟧ }`                 |
| `π ∪ π′`   | `⟦π⟧ ∪ ⟦π′⟧`                                                   |
| `π*`       | reflexive-transitive closure of `⟦π⟧`                          |

```rust
pub enum Path {
    Id,                                 // identity / empty word
    Pred(IriBuf),                       // q
    Inverse(Box<Path>),                 // π⁻
    Seq(Vec<Path>),                     // π · π′
    Alt(Vec<Path>),                     // π ∪ π′
    Star(Box<Path>),                    // π*
}
```

The paper omits `π⁺` (one-or-more) and `π?` (zero-or-one) but notes both are
definable: `π⁺ = π · π*` and `π? = π ∪ id`. We keep them as *normalizing sugar*
in the parser, not as IR constructors (gap-analysis **P1**).

---

## 3. Shapes (Def. 4, Table 2)

```
φ ::= ⊤ | test(c) | test(τ) | closed(Q) | eq(π,p) | disj(π,p)
    | ¬φ | φ ∧ φ′ | φ ∨ φ′ | ∃≥ⁿ π.φ | ∃≤ⁿ π.φ
```

with `c ∈ V`, `τ ∈ T`, `Q ⊆fin P∪K`, `p ∈ P`, `n ∈ ℕ`. Satisfaction
`G, v ⊨ φ` for a focus node/value `v ∈ N ∪ V`:

| `φ`            | `G, v ⊨ φ` iff                                                          |
|----------------|-------------------------------------------------------------------------|
| `⊤`            | always                                                                  |
| `test(c)`      | `v = c`                            (equality with a value constant)      |
| `test(τ)`      | `v ∈ ⟦τ⟧`                          (value-type / facet membership)       |
| `closed(Q)`    | `∀ p ∈ (P∪K)\Q : ¬(G, v ⊨ ∃≥¹ p.⊤)` (no triples outside the allowed set) |
| `eq(π,p)`      | `{u | (v,u) ∈ ⟦π⟧} = {u | (v,u) ∈ ⟦p⟧}`                                  |
| `disj(π,p)`    | `{u | (v,u) ∈ ⟦π⟧} ∩ {u | (v,u) ∈ ⟦p⟧} = ∅`                              |
| `¬φ`           | `not (G, v ⊨ φ)`                                                         |
| `φ ∧ φ′`       | `G,v ⊨ φ and G,v ⊨ φ′`                                                   |
| `φ ∨ φ′`       | `G,v ⊨ φ or G,v ⊨ φ′`                                                    |
| `∃≥ⁿ π.φ`      | `#{ u | (v,u) ∈ ⟦π⟧ ∧ G,u ⊨ φ } ≥ n`                                     |
| `∃≤ⁿ π.φ`      | `#{ u | (v,u) ∈ ⟦π⟧ ∧ G,u ⊨ φ } ≤ n`                                     |

Derived sugar (kept in parser, normalized away): `∃π.φ ≜ ∃≥¹π.φ`, and
`∀π.φ ≜ ∃≤⁰π.¬φ`.

```rust
pub enum Shape {
    Top,
    TestConst(Term),          // test(c)  — extended to Term, see gap-analysis V1
    TestType(ValueType),      // test(τ)
    Closed(BTreeSet<IriBuf>), // closed(Q): Q is the *allowed* predicate set
    Eq(Path, IriBuf),         // eq(π, p)
    Disj(Path, IriBuf),       // disj(π, p)
    Not(Box<Shape>),
    And(Vec<Shape>),
    Or(Vec<Shape>),
    /// ∃≥ⁿ π.φ  (lo) and  ∃≤ⁿ π.φ (hi); both bounds optional, qualified count.
    Count { path: Path, min: Option<u64>, max: Option<u64>, qualifier: Box<Shape> },
    // ---- extensions (gap-analysis), not in the paper's core ----
    TestKind(NodeKindSet),    // sh:nodeKind            (deviation K1)
    Lt(Path, IriBuf),         // sh:lessThan            (deviation O1)
    Le(Path, IriBuf),         // sh:lessThanOrEquals    (deviation O1)
    UniqueLang(Path),         // sh:uniqueLang          (deviation L1)
}
```

We fuse `∃≥ⁿ` and `∃≤ⁿ` into one `Count` node because real SHACL emits them as a
pair (`sh:qualifiedMinCount` + `sh:qualifiedMaxCount`, or `sh:minCount` +
`sh:maxCount`) on a shared path and qualifier; keeping them separate would force
the optimizer to re-pair them. The two-row table semantics is recovered by
treating `min`/`max` independently.

---

## 4. Selectors (Def. 5) and schemas

```
sel ::= ∃q.⊤ | ∃q⁻.⊤ | test(c)
```

A **schema** `S` is a finite set of pairs `(sel, φ)`. The selector is the
"target" (which nodes/values to check); `φ` is the constraint they must satisfy.

```
G ⊨ S   iff   ∀ v ∈ N∪V. ∀ (sel, φ) ∈ S.  (G, v ⊨ sel) ⟹ (G, v ⊨ φ)
```

```rust
pub enum Selector {
    HasOut(IriBuf),      // ∃q.⊤        — sh:targetSubjectsOf
    HasIn(IriBuf),       // ∃q⁻.⊤       — sh:targetObjectsOf
    IsConst(Term),       // test(c)     — sh:targetNode
    // ---- extension: class targets need a path-shaped selector (deviation C1) ----
    HasPath(Path, Box<Shape>),  // ∃≥¹ π.φ, used for sh:targetClass / implicit class target
    Sparql(SparqlTarget),       // AF SPARQL target escape hatch (deviation AF-T)
}

pub struct Schema {
    pub statements: Vec<(Selector, Shape)>,
    // shape references are resolved into a shared arena; cyclic refs allowed,
    // recursion semantics fixed in a later doc (see roadmap Layer 4).
}
```

Note the paper's selectors are *unary* and very restricted; the
`HasPath`/`Sparql` variants are our additions to reach W3C target coverage.

---

## 5. What this core gives us

* A **closed, denotational** definition of validation: `G ⊨ S` is decidable by
  structural recursion on `φ` plus relational evaluation of `π`. This is the
  reference oracle the optimized engine must agree with.
* **Algebraic laws** to exploit later: `π` is a Kleene algebra with converse
  (`(π·π′)⁻ = π′⁻·π⁻`, `id` unit, `∪` idempotent-commutative-associative, `*`
  fixpoints); `φ` is a Boolean algebra over the `test`/`Count`/`eq`/`disj`/
  `closed` atoms. Normalization and simplification (Layer 4) ride on these.
* A **single counting primitive** (`Count`) that subsumes cardinality, qualified
  cardinality, `sh:node`/`sh:property` nesting, and the `∀`/`∃` sugar — so the
  planner optimizes one construct, not a dozen vocabulary terms.

## 6. Out of scope for the core (handled as extensions)

The paper's core is *validation only* and *non-recursive*. Two whole machines
live outside it and are specified in the gap analysis + roadmap, not here:

* **SHACL-AF inference (rules)** — a separate rule algebra (body = condition
  shapes + selector, head = triples built from *node expressions*) evaluated to a
  fixpoint. See gap-analysis **AF-R** and roadmap Layer 6.
* **Recursion** — shape references may be cyclic; the paper assumes they are not.
  Choosing a semantics (stratified / well-founded / supported) is a deliberate
  design decision deferred to roadmap Layer 4.
