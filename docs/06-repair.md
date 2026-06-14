# 06 — Symbolic Repair (template synthesis)

When `G ⊭ S`, validation tells you *that* a focus node violates a shape and —
through the report path ([`report.rs`](../crates/shifty-engine/src/report.rs)) —
*which component* failed. This document specifies the dual machine: given a
violation, synthesize the **set of ways the data graph could be edited so the
focus node satisfies the shape**.

The output is not a single repair. It is a **repair template**: a parametric
description of the *entire* repair space for one violation, carrying typed holes
and variadic blocks. Concrete repairs are produced by *filling the holes*, and
the hole-filler is pluggable — subgraph monomorphism (reuse existing nodes),
naive enumeration, answer-set programming, or an LLM. One representation, many
solvers.

Companion docs: the IR is [`00-formalism.md`](00-formalism.md); the recursion
semantics this leans on is [`03-recursion-semantics.md`](03-recursion-semantics.md);
the normalized `φ` we recurse over is the product of
[`04-normalization.md`](04-normalization.md). An earlier, informal sketch of
data-fix suggestions lives in
[`human-readable-output.md`](human-readable-output.md) and is subsumed here.

> **Scope decision (this cut): data-graph repairs only.** A template adds and
> deletes *data* triples; the schema `S` is ground truth. The IR is kept general
> enough that schema edits (widen a `closed` set, lower a `min`, drop a
> statement) could be expressed later, but they are not generated now.

---

## 1. Repair as abduction over `φ`

Validation is `G, v ⊨ φ` decided by structural recursion on `φ` (doc 00 §5).
Repair is its abductive dual:

```
repair(φ, v)  =  { ΔG : (G ⊕ ΔG), v ⊨ φ }              -- the repair space
```

where `ΔG` is a set of triple additions and deletions and `⊕` applies them.
`repair` is computed by the *same* structural recursion on `φ`, so the small
core IR pays off twice: the `Shape` enum
([`shape.rs`](../crates/shifty-algebra/src/shape.rs)) has ~15 variants, it is
already in NNF with negation pushed to the leaves (recursive `¬` stays a
literal — doc 04 §1), and it is a DAG we can fold over.

**Generate from the algebra, not from the report.** The W3C report walker
deliberately treats `sh:and`/`sh:or`/`sh:not`/`sh:node` as opaque units (it does
not drill into sub-failures — `report.rs` module docs). Repairs *must* drill in:
to repair `φ₁ ∧ φ₂` you need the repair spaces of both conjuncts. So the repair
generator recurses over the `ShapeArena`, using the report only to seed *which*
statements failed at *which* focus nodes.

---

## 2. The template IR

Four constructs, mirroring `φ` on purpose — a repair tree is the skeleton of a
satisfaction proof.

```rust
/// A typed placeholder. Solvers bind holes to concrete terms.
pub struct Hole(pub u32);

pub enum HoleConstraint {
    AnyNode,                  // any term
    Fresh,                    // a newly-minted node (mint, don't reuse)
    Const(Term),             // must equal this term (sh:hasValue / test(c))
    Typed(ValueType),        // datatype / numeric range / length / pattern
    Kind(NodeKindSet),       // IRI | blank | literal (sh:nodeKind)
    OneOf(BTreeSet<Term>),   // finite domain (sh:in)
    ConformsTo(ShapeId),     // value node must itself satisfy a sub-shape
}

/// A triple whose slots may be holes or fixed terms.
pub struct TriplePattern { s: Slot, p: Slot, o: Slot }   // Slot = Bound(Term) | Open(Hole)

pub enum Edit { Add(TriplePattern), Delete(TriplePattern) }

/// The repair space for one (sub)shape, as an AND/OR/repeat tree.
pub enum RepairTree {
    /// Already satisfied — the empty repair. (⊤, or a witness-pruned branch.)
    Noop,
    /// Unrepairable in scope (e.g. opaque SPARQL). Carries a reason.
    Blocked(BlockReason),
    /// A concrete set of edits + the holes they introduce.
    Edits { edits: Vec<Edit>, holes: Vec<(Hole, HoleConstraint)> },
    /// Satisfy every child (conjunction): merge one repair from each.
    All(Vec<RepairTree>),
    /// Satisfy any one child (disjunction): each is an alternative template.
    Any(Vec<RepairTree>),
    /// Variadic: instantiate `body` between `min` and `max` times.
    /// This is the home of cardinality repair and "variadic elements".
    Repeat { body: Box<RepairTree>, min: u64, max: Option<u64> },
}
```

Everything the request named has a home: **variadic elements** are `Repeat`;
**deep / nested elements** come from `ConformsTo` holes and from path
materialization (§3); the **variety of patterns** is the `Any` branching; and a
hole with `HoleConstraint::Typed`/`OneOf` is exactly the seam a solver fills.

We will also render templates **to RDF** (a parameterized graph in the
BuildingMOTIF sense), because that rendering is what the monomorphism backend
searches with and what we show to a human.

---

## 3. The `φ → RepairTree` mapping

This is the altitude view: each `Shape` constructor has a canonical, finite
family of satisfaction strategies. §5 gives the concrete, witness-driven dispatch
(`repair`/`break_`/`build`); the table below is the conceptual map it implements.

| `Shape` | `RepairTree` | notes |
|---|---|---|
| `Top` | `Noop` | |
| `TestConst(c)` | bind value-hole to `Const(c)`, or `Add` the triple yielding `c` along the governing path | finite |
| `TestType(τ)` | `Edits` introducing a `Typed(τ)` hole for the value node | **infinite/semantic hole** — sampler or LLM |
| `TestKind(K)` | `Kind(K)` hole | |
| `Closed(Q)` | `All` of `Delete` edits, one per offending predicate ∉ Q | data-only stance |
| `Eq/Disj/Lt/Le` | pairwise `Add`/`Delete` to establish/break the relation | |
| `UniqueLang(π)` | `Delete` all-but-one value per duplicated language tag | choose which |
| `Not(φ′)` | *falsify* φ′ via its satisfaction trace (§4.2) — flips add → delete | recursive `¬` is a literal under NNF, so this mostly hits leaves |
| `And(φᵢ)` | `All([synthesize(φᵢ)])` | conflict-prune merges (see §5) |
| `Or(φᵢ)` | `Any([synthesize(φᵢ)])` | where templates branch |
| `Count{π,min,..}` (under min) | `Repeat{ body, min = min − have, max }` where `body` adds one π-reachable value satisfying `q` | the variadic add |
| `Count{π,..,max}` (over max) | choose `have − max` matching π-values to `Delete` | variadic delete |
| `Sparql(_)` | `Blocked(OpaqueSparql)` | escape-hatch — defer to LLM |

**Path materialization.** When the governing path `π` is a `Seq` (doc 00 §2),
adding "a π-reachable value" means minting the intermediate nodes too: each
interior step becomes a `Fresh` hole and a chained `Add`. That is how a single
`Count` over a multi-step path produces a deep repair block. `Inverse` flips
subject/object in the pattern; `Alt` becomes an `Any`; `Star` is bounded (§4).

---

## 4. Witnessing — the first building block

To avoid synthesizing repairs for parts of `φ` that already hold, `synthesize`
must know *which* sub-shape failed at *which* value node, reached by *which*
path. The engine already has 80% of this: [`explain`](../crates/shifty-engine/src/validate.rs)
walks the arena, prunes satisfied branches (via the `holds` fast-path and the
gfp back-edge guard), and returns a `Reason` tree with `sub_reasons`. But
`explain` is tuned for humans and loses exactly what repair needs:

1. **paths are rendered to `String`** (`path_to_string`) — repair must
   materialize/cut intermediate nodes, so it needs the structured `Path`;
2. **`And` is flattened** — repair needs the conjunction preserved as a node to
   map onto `RepairTree::All`;
3. **`Count` gaps are stringly-typed** in a message — repair needs
   `(path, have, min, max, qualifier)` structured to build the `Repeat` block.

So the witness is the **structured, lossless sibling of `Reason`**: the failed
sub-DAG of `φ`, pruned to exactly what did not hold, carrying at each node the
`ShapeId` (so `synthesize` recovers the constraint from the arena) and the
structural gap.

### 4.1 The witness (additive direction: what to *add*)

```rust
/// Why one focus node failed one statement: the failed sub-structure of φ,
/// pruned to exactly the parts that did not hold. The input to repair synthesis.
pub struct FocusWitness {
    pub focus: Term,
    pub statement: usize,     // index into Schema::statements (matches Violation)
    pub failure: Witness,
}

pub enum Witness {
    /// A leaf atom failed at `node`, reached from the focus by `reached_by`.
    /// `shape` recovers the exact constraint from the arena
    /// (TestConst / TestType / TestKind / Eq / Disj / Lt / Le / UniqueLang).
    /// `produced_by` (the same PathSupport the deletive side uses, §4.2) names
    /// the triples that made `node` a value — `Some` for value-scoped atoms,
    /// `None` when the atom is on the focus itself (`reached_by = Id`). Synthesis
    /// needs it for *replace-in-place* (delete the offending value, add a good
    /// one); without it only the pure-add strategy is available.
    Atom { shape: ShapeId, node: Term, reached_by: Path, produced_by: Option<PathSupport> },

    /// `closed(Q)` failed: these (predicate, object) pairs are not allowed.
    Closed { shape: ShapeId, node: Term, offenders: Vec<(NamedNode, Term)> },

    /// `¬φ` failed because φ *holds* at `node` — it must be falsified. The child
    /// is φ's satisfaction trace (§4.2): the delete-direction support to break.
    /// This crossing flips add → delete.
    Not { shape: ShapeId, node: Term, inner: Box<SatTrace> },

    /// Conjunction: every child failed and ALL must be repaired. → RepairTree::All
    All { shape: ShapeId, node: Term, failed: Vec<Witness> },

    /// Disjunction: no branch held; repairing ANY ONE suffices. → RepairTree::Any
    Any { shape: ShapeId, node: Term, branches: Vec<Witness> },

    /// `∃≥min π.q` under-satisfied: `have` values match, `min` required.
    /// The variadic-add case. → RepairTree::Repeat { min: min - have, .. }
    CountLow {
        shape: ShapeId, node: Term, path: Path,
        qualifier: ShapeId, have: u64, min: u64,
    },

    /// `∃≤max π.q` over-satisfied. `per_value` is populated only for the
    /// ∀-encoding (`∃≤0 π.¬inner`): the inner failure at each matched value, so
    /// repair can fix-in-place instead of deleting. Otherwise delete down to max.
    CountHigh {
        shape: ShapeId, node: Term, path: Path, qualifier: ShapeId,
        matched: Vec<Term>, max: u64, per_value: Vec<(Term, Witness)>,
    },

    /// Opaque SPARQL — no algebraic witness. Carries reported value/path so a
    /// non-algebraic backend (LLM) still has context. → RepairTree::Blocked
    Opaque { shape: ShapeId, node: Term, value: Option<Term>, path: Option<Path> },
}
```

### 4.2 The satisfaction trace (deletive direction: what to *delete*)

A `Not(φ)` failure can only be repaired by **breaking φ** — which needs the dual
of a failure trace: a record of *why φ currently holds*, organized so repair can
pick a minimal set of deletions that falsifies it. Crossing a `Not` flips the
direction; `Count` is the other flip point (it is self-dual — break the lower
bound by *deleting* matches, the upper bound by *adding* them). So `Witness` and
`SatTrace` are **mutually recursive**, and every `Not` toggles add ↔ delete.

```rust
/// Why φ currently *holds* at a node — its grounded support, organized so repair
/// can pick a minimal set of edits that falsifies it. The dual of `Witness`.
pub enum SatTrace {
    /// ⊤ — vacuously true; no graph edit falsifies it.
    Irrefutable { shape: ShapeId },

    /// A leaf atom holds at `node`, which became a value via `reached_by`.
    /// `produced_by` names the existing triples that make `node` a π-successor;
    /// cutting them removes it from the value set. (See PathSupport below.)
    Atom { shape: ShapeId, node: Term, reached_by: Path, produced_by: PathSupport },

    /// Conjunction holds because ALL children hold ⟹ break ANY ONE.
    /// (dual of Witness::All) — an OR of break-options.
    AllHeld { shape: ShapeId, node: Term, children: Vec<SatTrace> },

    /// Disjunction holds because these branches hold ⟹ break EVERY one.
    /// (dual of Witness::Any) — an AND of break-options.
    AnyHeld { shape: ShapeId, node: Term, satisfied: Vec<SatTrace> },

    /// `∃[min..max] π.q` holds ⟹ drop below min (delete matches) OR exceed max
    /// (add matches — flips to the additive side). `matches` carries each counted
    /// value with its q-support, so a match can be broken by cutting the π-edge
    /// OR by falsifying q at that value.
    CountHeld {
        shape: ShapeId, node: Term, path: Path, qualifier: ShapeId,
        matches: Vec<(Term, SatTrace)>, min: Option<u64>, max: Option<u64>,
    },

    /// `¬φ` holds because φ fails ⟹ make φ hold. Flips back to the additive side.
    NotHeld { shape: ShapeId, node: Term, inner_fails: Box<Witness> },

    /// `closed(Q)` holds (falsifying needs *adding* a disallowed predicate) or
    /// opaque SPARQL holds — both outside the data-deletion scope.
    Blocked { shape: ShapeId, node: Term, reason: BlockReason },

    /// Support reached only through a gfp back-edge: coinductively assumed true,
    /// with no finite set of facts to delete. Falsifying requires breaking the
    /// cycle elsewhere. The deletion-direction analog of the fuel problem (§4.4).
    Coinductive { shape: ShapeId, node: Term },
}
```

`PathSupport` is the deletion-side dual of path *materialization*: instead of
minting a chain to a fresh value, it enumerates the *existing* triples that made
`node` a π-successor. For `Pred(p)` it is the single triple `(focus, p, node)`;
for a `Seq` it is an OR over the chain (cut any one edge); `Alt`/`Star` give
multiple parallel witnessing chains, OR'd together.

### 4.3 The interlocked signatures

Two sibling folds over the arena, parallel to `explain`, sharing the same `holds`
oracle and back-edge guard. They are **exact complements**: for a given
`(node, id)` exactly one returns `Some` (modulo the back-edge convention).

```rust
/// Reasons φ fails at `node`. `None` ⟺ φ holds.
fn witness(eval: &mut ShapeEvaluator, node: &Term, id: ShapeId,
           reached_by: &Path, stack: &mut HashSet<(ShapeId, Term)>) -> Option<Witness>;

/// Support for why φ holds at `node`. `None` ⟺ φ fails.
fn sat_trace(eval: &mut ShapeEvaluator, node: &Term, id: ShapeId,
             reached_by: &Path, stack: &mut HashSet<(ShapeId, Term)>) -> Option<SatTrace>;

/// Public driver, mirroring `validate` (same statement × focus loop, same
/// stratifiability check). `_graphs` / `_with_context` / `_plan` variants follow.
pub fn witness_violations(data: &Graph, schema: &Schema)
    -> Result<Vec<FocusWitness>, NonStratifiable>;
```

At a `Not(c)` the two cross over:
`witness` ⇒ `sat_trace(c).map(Witness::Not)`; `sat_trace` ⇒ `witness(c).map(SatTrace::NotHeld)`.

Four things must be carried over verbatim from `explain`, or repairs corrupt
silently: the **back-edge ⇒ `None`/`Coinductive`** convention; the **`holds`
fast-path** (which is what prunes satisfied branches for free); the
**∀-encoding drill-in** (`∃≤0 π.¬inner` → `CountHigh.per_value`, the one place a
witness looks *through* the qualifier); and **structured path threading** (at a
`Count`, recurse into value nodes with `reached_by · path`).

### 4.4 Termination, minimality, and the honest limits

- **Back-edges and fuel.** `witness` terminates exactly like `explain` (the stack
  guard); it is finite. The **fuel** concern is *`synthesize`'s*, not
  witnessing's: unfolding a recursive `ConformsTo` hole could diverge, so
  `synthesize` caps depth and, at the cap, leaves the hole for a solver to bind
  to an *existing* conforming node (monomorphism's job) rather than minting an
  infinite chain.
- **Minimality is the solver's job (both directions).** `AllHeld`→OR /
  `AnyHeld`→AND nest into a support DAG; the smallest falsifying deletion is a
  **minimum hitting set** over it — NP-hard in general, trivial in practice, and
  exactly `#minimize` for the ASP backend. The trace hands over the AND/OR DAG;
  the backend picks the cut, mirroring the additive side.
- **Deletion-direction repair is genuinely incomplete through positive
  recursion.** A gfp support reached on a back-edge (`Coinductive`) is assumed
  true *without* a grounded justification, so there is no finite set of facts to
  delete. This is the formal content of the "Not / deletion completeness" caveat
  (§11): the first cut keeps falsification shallow (it falsifies leaf atoms, which
  is most of what survives NNF) and reports `Coinductive` otherwise.

---

## 5. Synthesis — from witness to template

`synthesize` turns a `FocusWitness` into a `RepairTree`. It is not one function:
the witness/trace duality forces **three mutually-recursive** ones, plus a third
direction the additive side demands.

- **`repair(Witness)`** — additive: make a *failing existing node* hold.
- **`break_(SatTrace)`** — deletive: falsify a *holding existing node*.
- **`build(ShapeId, Hole)`** — additive, *hypothetical*: constrain a
  *not-yet-existing* node to satisfy a shape.

The third exists because `Witness::CountLow` says "add `min − have` new values,
each satisfying `qualifier`" — and those values **don't exist yet**, so there is
nothing to witness against. `build` therefore walks the **`Shape`** (everything
must be constructed; nothing to prune), whereas `repair`/`break_` walk the
already-pruned witness/trace. That difference is also where **fuel** lives:
`build` on a recursive shape is what can diverge; `repair`/`break_` are finite.

```rust
struct Synth<'a> { arena: &'a ShapeArena, next_hole: u32, fuel: u32 }

impl Synth<'_> {
    fn fresh(&mut self) -> Hole { let h = Hole(self.next_hole); self.next_hole += 1; h }

    /// Additive: edits making a failing node satisfy φ. Walks the (pruned)
    /// witness ⟹ finite, no fuel.
    fn repair(&mut self, w: &Witness) -> RepairTree;

    /// Deletive: edits that falsify a holding φ. Walks the satisfaction trace.
    fn break_(&mut self, s: &SatTrace) -> RepairTree;

    /// Additive, hypothetical: constrain a fresh `hole` to satisfy shape `id`.
    /// Walks the *shape* ⟹ fuel-bounded against recursion.
    fn build(&mut self, id: ShapeId, hole: Hole) -> RepairTree;
}

/// One focus, one statement.
pub fn synthesize(arena: &ShapeArena, w: &FocusWitness) -> RepairTree;

/// Per-focus grouping (§8): All(...) over a focus's statement-witnesses, so a
/// backend can solve the focus's constraints jointly.
pub fn synthesize_focus(arena: &ShapeArena, ws: &[FocusWitness]) -> RepairTree;
```

Two properties hold by construction. **`synthesize` is graph-free**: every
contact with `G` already happened during witnessing (the witness/trace carry the
failing nodes, counts, and `PathSupport` triples), so `synthesize` is a pure
function of `(witness/trace/shape, arena)`. And it **does not validate**: it
emits the template; the backend fills holes and re-validates (the gate in §7).

### 5.1 `repair` (additive) — walks the witness

| `Witness` | `RepairTree` |
|---|---|
| `Atom` (value-scoped, has `produced_by`) | `Any([ replace-in-place, … ])` — delete the offender, `build` a good value |
| `Atom` (focus identity, e.g. nodeKind on focus) | `Blocked(CannotMutateIdentity)` |
| `Closed{offenders}` | `All([ Delete(node,p,o) … ])` |
| `All{failed}` | `All(failed.map(repair))` |
| `Any{branches}` | `Any(branches.map(repair))` — each is one alternative way |
| `CountLow{path,qualifier,have,min}` | `Repeat{ min: min−have, max: None, body: build_value(path, qualifier) }` |
| `CountHigh` (∀-encoding, `per_value`) | `All(per_value.map(\|(v,inner)\| repair(inner) @ v))` — fix each offender in place |
| `CountHigh` (plain max) | `Repeat{ min: have−max, body: Delete(node, p, OneOf(matched)) }` |
| `Not{inner}` | `break_(inner)` — **flip to deletive** |
| `Opaque` | `Blocked(OpaqueSparql)` |

### 5.2 `break_` (deletive) — walks the satisfaction trace, the De Morgan mirror

| `SatTrace` | `RepairTree` |
|---|---|
| `Atom{produced_by}` | `Any([ Delete(edge) … ])` over the `PathSupport` cut-options |
| `AllHeld{children}` | `Any(children.map(break_))` — break any one |
| `AnyHeld{satisfied}` | `All(satisfied.map(break_))` — break all |
| `CountHeld{min,max}` | `Any([ drop-below-min (deletes), exceed-max (builds) ])` |
| `NotHeld{inner_fails}` | `repair(inner_fails)` — **flip to additive** |
| `Irrefutable` / `Blocked` / `Coinductive` | `Blocked(…)` |

### 5.3 `build` (hypothetical) — walks the shape, attaches hole constraints

| `Shape` | `build(id, hole)` |
|---|---|
| `Top` | `Noop` |
| `TestConst(c)` / `TestType(τ)` / `TestKind(K)` | `Edits{ edits:[], holes:[(hole, Const(c)/Typed(τ)/Kind(K))] }` |
| `And(cs)` / `Or(cs)` | `All` / `Any` of `build(c, hole)` — same hole, combined constraints |
| `Count{path,min,..,q}` | `Repeat{ min, body: build_value(path, q) }` — **fuel − 1** |
| recursive `id` at **fuel 0** | `Edits{ holes:[(hole, ConformsTo(id))] }` — defer to the solver (monomorphism reuse) |
| `Closed(Q)` | `Noop` — a freshly built node simply omits disallowed predicates |
| `Sparql` | `Blocked(OpaqueSparql)` |

`build_value(path, q)` mints the interior chain of a `Seq` path as `Fresh` holes,
ends at a value-hole `vh`, and calls `build(q, vh)` — that is how a single
`CountLow` over a multi-step path yields a deep repair block.

### 5.4 Three sharp points

- **`maxCount` deletion reuses `Repeat`.** "Delete `k` of these `n` matches" is
  the exact dual of "add `k` values": `Repeat{ min: k, body: Delete(node, p,
  OneOf(matched)) }`, where the `OneOf` hole lets the solver *choose which* to
  drop. The same construct counts up (add) or down (delete) — no new variant.
- **Fuel is per-`build`-entry.** `repair`/`break_` are finite; when one spawns a
  `build` (via `CountLow`, or `CountHeld`'s exceed-max), that build gets a fresh
  depth budget. At zero, the recursive hole becomes `ConformsTo(id)` and the
  monomorphism backend closes it — reuse over invention.
- **Replace-in-place is the witness's one demand on the layer below.** It needs
  the offending value's `produced_by` provenance, which is why value-scoped
  `Witness::Atom` carries `PathSupport` (§4.1). This is the sole place
  `synthesize` reaches back into the witnessing contract; everywhere else it is
  self-contained.

---

## 6. Rendering a template to RDF (the monomorphism format)

Two consumers need a `RepairTree` *as an RDF graph*: the monomorphism backend
(§7), which searches `G` with it, and human display. This is the BuildingMOTIF
representation — a parameterized graph in which some nodes are holes. The ASP and
LLM backends consume the `RepairTree` directly and need no RDF, so this rendering
is specifically the monomorphism + display path.

### 6.1 Holes as parameter nodes

Each `Hole` renders to a reserved-namespace IRI — `urn:shifty:hole#<id>` (the
analogue of BuildingMOTIF's `urn:___param___#`). A `TriplePattern` renders to a
triple with bound slots as their terms and open slots as the hole IRI. The
*additive* edits of one branch thus form a graph `B` over real predicates and
hole IRIs.

### 6.2 Hole constraints as a self-hosted SHACL sidecar

A `HoleConstraint` is exactly "what the bound node must satisfy" — i.e. a shape.
So render it as a node shape on the hole IRI, **in the engine's own algebra**:

| `HoleConstraint` | rendered shape |
|---|---|
| `Const(c)` | `test(c)` (`sh:hasValue`) |
| `Typed(τ)` | the τ value-type (`sh:datatype` / range / `sh:pattern` / length) |
| `Kind(K)` | `sh:nodeKind` |
| `OneOf(S)` | `sh:in (…)` |
| `ConformsTo(q)` | `sh:node q` — reference the existing arena shape |
| `Fresh` | marker: must **not** bind to an existing node (mint-only) |
| `AnyNode` | no constraint |

This is self-hosting: a repair template's side-conditions are checked by the same
validator the repair targets — no second constraint language, and the sidecar is
itself a `Schema` over the hole IRIs.

### 6.3 Flattening: `RepairTree` → a set of conjunctive patterns

RDF is flat, so the tree's logic is pushed outward into a DNF-shaped program:

- `Any([…])` → **alternatives**: each branch renders to a separate pattern.
- `All([…])` → **merge** children's add-graphs, sidecars, and delete-sets into
  one pattern; contradictory hole constraints prune the branch.
- `Repeat{ body, min, max }` → a **count-parameterized block**: the body pattern
  tagged `min..max`; the solver instantiates it `k` times with a fresh copy of
  the block's holes per instance.
- `Edits` → triples into the add-graph (`Add`) or delete-set (`Delete`); holes
  into the sidecar.
- `Noop` → empty; `Blocked` → drop the alternative (or surface "no data repair").

The result is a `RepairProgram` — a set of alternative patterns:

```rust
struct RepairPattern {
    add:    Graph,                    // real predicates + hole IRIs
    params: Schema,                   // §6.2 sidecar over the hole IRIs
    repeat: Vec<(BlockId, u64, Option<u64>)>,   // count-parameterized sub-blocks
    delete: Vec<TriplePattern>,       // applied, not searched
}
```

### 6.4 The overlap search (maximum partial embedding)

The backend does **not** seek a full embedding of `add` into `G` — if the whole
add-graph already matched, the node would not be violating. It seeks the
**maximum partial embedding**: an injective map from a *subset* of hole nodes to
existing `G` nodes such that (a) every mapped edge of `add` exists in `G`, and
(b) each mapped hole's bound node conforms to its sidecar shape (§6.2). Unmapped
holes are **minted fresh**. That is precisely "find the subgraph of the data that
overlaps the template," reuse maximized, remainder constructed — the BuildingMOTIF
ingest mechanic.

- **Reuse vs. mint** is the preference knob: prefer embeddings binding more holes
  to existing nodes; `Fresh` holes are forced unmapped.
- **`Repeat` blocks**: bind up to `max` existing matching instances, mint enough
  fresh ones to reach `min`.
- Each embedding yields a concrete `ΔG` (minted add-triples + the deletes),
  confirmed by the global re-validation gate (§10).

---

## 7. Filling the holes — pluggable backends

The template is solver-agnostic. A backend consumes a `RepairTree` + `G` and
emits concrete `ΔG`s.

- **Monomorphism / reuse — _first concrete cut_.** Render the *positive* (add)
  part of a template as an RDF graph pattern and search for monomorphic images
  in `G`, exactly as BuildingMOTIF matches templates against a data graph. A
  match binds holes to **existing** nodes that already partially satisfy the
  template — so the repair *reuses* graph structure instead of minting fresh
  nodes. This is the "find subgraphs that overlap the template" mechanism the
  design calls for. Reuse is preferred over `Fresh` minting; `Fresh` holes are
  the fallback when no monomorphic image exists.
- **Naive enumeration.** For finite holes (`Const`, `OneOf`, small `Repeat`
  counts) enumerate directly. Cheapest backend; primarily a correctness oracle
  for the template IR.
- **ASP / clingo.** Lower template + `G` to answer-set programming: holes →
  choice atoms, `Repeat{min,max}` → cardinality rules `{...} = n`, `Not`/maxCount
  → integrity constraints, parsimony → `#minimize`. Answer sets *are* the
  repairs. The only backend that solves **all of a focus node's constraints
  jointly** (§8), so it handles cross-constraint interaction natively. Lowering
  detail in §9.
- **LLM.** Serialize the template + surrounding graph context; the model fills
  the *semantic* holes (`Typed(pattern)`, a plausible label) that enumeration and
  monomorphism cannot. Non-deterministic; gated behind validation re-check.

Every emitted `ΔG` passes the **global re-validation gate** (§10) before being
offered — the reference oracle keeps every backend honest, mirroring the Layer-3
oracle discipline (doc 02 guiding principle 1).

---

## 8. Grouping by focus node

Repairs are grouped per **focus node** (the `focus: Term` that already keys every
`ValidationResult`). That is the right unit because a focus node's violations
*interact*: satisfying a `minCount` by adding a value may trip a `datatype` or
`closed` constraint, and two violations may share a hole. Per-focus, the
generator combines the violations into one `All([...])` problem so a backend can
search for a single `ΔG` that satisfies everything at once. Pure template
enumeration cannot see these interactions; joint solving (ASP) can — which is the
main argument for that backend later.

---

## 9. ASP lowering — the joint-solving backend

The ASP backend takes a focus's *joint* problem (§8) and lowers it to a single
answer-set program whose **answer sets are exactly the valid repairs**. It is the
only backend that closes cross-constraint cascades and finds *minimal* repairs,
because both fall out of the solver: integrity constraints for "must conform,"
`#minimize` for parsimony.

The lowering is mechanical for one reason established earlier: the recursion
semantics (doc 03) is **stratified negation**, and the engine already runs it as
least-fixpoint forward chaining (`infer.rs`) with a computed stratum order
(`shifty-opt::analyze`). So `φ` *is already a logic program* — lowering it to ASP
rules reuses the same strata, not a reimplementation of validation.

### 9.1 Generate–define–test

The classic ASP skeleton, with the template bounding the search:

- **Generate** — choose hole bindings, repeat-instance counts, and deletions,
  *restricted to what the `RepairTree` proposes* (not "any triple in the universe"),
  so the search space is small and finite.
- **Define** — the post-repair graph `holds/3`, the path relations, and the
  violation predicate `viol/2` (= the witness, §4) as Datalog over `holds`.
- **Test** — `:- not conf(root, focus).` for every root shape in the focus group.
- **Optimize** — `#minimize` over the chosen edits.

### 9.2 Facts

`G` becomes `triple/3` over interned term constants (a symbol table both ways).
The *non-logical* leaf checks ASP cannot express — `sh:datatype`, numeric range,
`sh:pattern`, `sh:nodeKind`, `sh:in`, and `Lt`/`Le`/`Eq` comparisons — are
**precomputed by the engine** and emitted as facts (`sat(LeafShape, X)`,
`lt(X,Y)`, …), evaluated by reusing `value_type_holds`/`compare_terms`.

### 9.3 Generate (template-bounded)

```prolog
{ bind(H, X) : cand(H, X) } = 1 :- hole(H).      % one binding per hole
cand(H, X) :- reuse(H, X).        % existing node passing the §6.2 sidecar shape
cand(H, X) :- fresh(X), open(H).  % a minted node, unless the hole is Fresh-only
add(focus, p, X) :- bind(hv, X).  % one per Add(TriplePattern), holes substituted
{ del(S,P,O) : delcand(S,P,O) }.  % deletions from the break-options / delete-set
```

A `Repeat{min,max}` block becomes instance choice — `{ use(B,I) : inst(B,I) }`
with fresh hole copies per instance; the `min`/`max` are *not* asserted here but
enforced by the matching `Count` violation rule (§9.4), so the solver and
`#minimize` pick the smallest instantiation that conforms. The `fresh` pool is
sized from the template's `Fresh`/`Repeat` demand.

### 9.4 Define — and why we lower `viol`, not `conf`

The post-repair graph and path algebra:

```prolog
holds(S,P,O) :- triple(S,P,O), not del(S,P,O).
holds(S,P,O) :- add(S,P,O).
reach(pred_p, X, Y) :- holds(X, p, Y).                      % π = Pred(p)
reach(seq_ab, X, Z) :- reach(a, X, Y), reach(b, Y, Z).      % π = Seq
reach(star_b, X, X) :- node(X).                             % π = Star (closure)
reach(star_b, X, Z) :- reach(star_b, X, Y), reach(b, Y, Z).
```

Conformance is encoded **through its complement**: lower the *violation* (the
finitely-justified failure — exactly the witness, §4) and define conformance as
its stratified negation.

```prolog
conf(S, V)         :- node(V), not viol(S, V).
viol(s_type, V)    :- node(V), not sat(s_type, V).
viol(s_and, V)     :- viol(C, V).                  % some conjunct fails
viol(s_or, V)      :- viol(c1,V), viol(c2,V).      % all disjuncts fail
viol(s_not, V)     :- conf(c, V).                  % ¬φ fails iff φ holds
viol(s_count, V)   :- #count{ U : reach(pi,V,U), conf(q,U) } < min.
viol(s_count, V)   :- #count{ U : reach(pi,V,U), conf(q,U) } > max.
```

This matters because **validation is gfp but ASP stable models are lfp.** For
positive recursion (`S := ∃knows.S`) gfp lets a cycle conform coinductively;
stable-model semantics demands finite justification and would not. Lowering
`viol` makes the encoding's reading **lfp/finitely-justified — which is the
*correct* semantics for repair**: a repair must actually *construct* supporting
structure, not lean on a coinductive cycle. The divergence is contained because
(a) the backend only ever runs on genuine witnesses (nodes that fail the real
gfp validator), and (b) every answer set is decoded and re-checked by the real
validator (§7). Stratification of `viol` is guaranteed by the same sign-balance
test the engine already enforces (`shifty-opt::strata`; the `∀`-encoding's paired
negative edges compose to positive).

### 9.5 Test, optimize, decode

```prolog
:- not conf(root_a, focus).        % every root shape in the focus group — jointly
:- not conf(root_b, focus).
#minimize { Wa,S,P,O : add(S,P,O) ; Wd,S,P,O : del(S,P,O) }.
```

The conjoined integrity constraints are the joint solve: one model must satisfy
*all* of the focus's shapes at once, so adding a value to fix a `minCount` is
forced to also pass the `datatype`/`closed` rules that constrain it. Edit weights
encode the preference order — reuse < mint, and any add/delete bias — so the
optimum is the minimal, most-reuse repair. Each answer set decodes (`add`/`del`
→ terms → `ΔG`) and passes the global re-validation gate (§10).

### 9.6 Boundaries

- **Opaque SPARQL.** `Shape::Sparql` cannot be lowered. Either freeze its current
  truth as a fact and forbid edits to its footprint, or mark repairs touching it
  `Blocked` — the honest default.
- **Infinite literal domains.** A `Typed(τ)` *fresh* hole (e.g. "any integer ≥ 0")
  has no finite candidate set. ASP decides **structure, reuse, and counts**; the
  concrete novel literal is left as a hole for a sampler or the LLM backend. ASP
  is strongest at "which existing nodes to wire, how many, which to drop,"
  complementary to monomorphism (reuse) and the LLM (invention).

---

## 10. Applying repairs — the global gate and the fixpoint driver

A repair is sound only if it does not trade one violation for another. The gate
is therefore **whole-graph**, not focus-local: a candidate `ΔG` is accepted iff

```
violations(G ⊕ ΔG, S)  ⊆  violations(G, S) \ { the targeted violation(s) }
```

— the target is fixed *and no new violation appears anywhere*. This reuses the
existing `validate` unchanged. The gate's interface is this **delta-of-violations**
check rather than a bare `v ⊨ φ`, so a cheaper *affected-set* re-validation (only
the nodes `ΔG` can touch — computable from `shifty-opt`'s dependency/demand
analysis) can drop in later as a pure performance optimization with identical
semantics.

**The driver loop.** Repairs interact, and a witness computed against `G` goes
stale the moment `G` changes, so repair is iterative:

1. `witness_violations(G, S)` → the current violations, grouped per focus (§8).
2. Pick a focus; `synthesize_focus` → its `RepairTree`; a backend proposes a `ΔG`.
3. **Gate** `ΔG`. On accept, `G := G ⊕ ΔG`. On reject, **retry** the next
   candidate (next embedding / answer set / `Any` branch).
4. Re-witness and repeat until `G ⊨ S`, or no candidate makes progress.

**Termination** rests on strict progress: an accepted `ΔG` shrinks the violation
set (the subset criterion guarantees it never grows), so the loop is monotone and
finite. A focus with no progress-making candidate is reported **unrepairable in
scope** and skipped — never retried forever.

**Reject-and-retry is incomplete on purpose.** Fixing one focus at a time, it can
stall where a repair exists *only* by fixing a focus and the node its `ΔG` would
break *together*. That joint case is deferred to an opt-in **frontier-expansion**
driver — pull the broken node into the same problem and re-solve, natural for the
ASP backend (§9) — a strictly more powerful driver behind the same gate.

---

## 11. Open questions / limitations

- **Minimality.** Many `ΔG` satisfy a constraint. We need a preference order
  (delete-vs-add bias, reuse-vs-mint bias, edit count). Policy lives in the
  backend (`#minimize`, monomorphism-prefers-reuse, LLM judgment); the template
  should carry enough cost annotation to drive it (the `#minimize` weights, §9.5).
- **Confluence.** The global gate (§10) makes every backend *sound* under
  collateral — a `ΔG` that breaks a sibling is rejected. But reject-and-retry is
  *incomplete*: it can stall where a repair exists only by fixing a focus and its
  collateral jointly. Closing that needs the frontier-expansion driver (§10) or
  the joint ASP solve (§9).
- **`Not` and deletion completeness.** Falsification rides the satisfaction
  trace (§4.2). It is sound but *incomplete through positive recursion*: a gfp
  support reached on a back-edge (`SatTrace::Coinductive`) has no grounded fact
  to delete (§4.4). The first cut falsifies leaf atoms and reports `Coinductive`
  otherwise.
- **Schema repairs** are deliberately out of scope for this cut (see the scope
  note) but the IR does not preclude them.

## 12. Build order

1. **Witnessing evaluator** over the arena (§4) — `witness` + its `sat_trace`
   dual, as sibling folds beside `explain`, exporting `FocusWitness`.
2. **Template IR** (§2) with serde.
3. **`synthesize`** (§5) — `repair`/`break_`/`build`, fuel-bounded (§5.4).
4. **RDF rendering** (§6) — `RepairTree` → `RepairProgram` with the self-hosted
   sidecar.
5. **Global gate + reject-retry driver** (§10) — the delta-of-violations check
   and the iterate-to-fixpoint loop; needed before any backend's `ΔG` is trusted.
6. **Monomorphism backend** (§7) — the overlap search (§6.4).
7. Per-focus grouping (§8).
8. **ASP backend** (§9) — joint solving + `#minimize`; then enumeration / LLM
   backends, and the frontier-expansion driver (§10), as needed.
