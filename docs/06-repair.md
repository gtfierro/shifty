# 06 — Symbolic Repair: the API

When `G ⊭ S`, validation tells you *that* a focus node violates a shape and —
through the report path ([`report.rs`](../crates/shifty-engine/src/report.rs)) —
*which component* failed. This document specifies the **library API** for the
dual question: given a violation, expose the *set of ways the data graph could be
edited so the focus node satisfies the shape*.

> **This is a library, not an autonomous repairer.** The API *computes* — it
> witnesses violations, enumerates the repair space, turns a caller's choices
> into concrete edits, and validates a candidate. It makes **no decisions**:
> which term fills a hole, which alternative to take, how many values to add,
> whether to accept a candidate, which focus to fix, and when to stop are all the
> **driver's** to make. An external driver, with its own data sources and policy,
> drives the loop. Reference drivers (monomorphism, enumeration, ASP, an LLM)
> live in [`07-repair-drivers.md`](07-repair-drivers.md) as worked examples over
> this API — use or replace them freely.

The central artifact is a **repair template** (`RepairTree`): a parametric,
*inspectable* description of the entire repair space for one violation, with
typed holes and variadic blocks. A driver fills the holes; the library turns that
into edits.

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

where `ΔG` is a set of triple additions and deletions and `⊕` applies them. The
library *describes* this set as a `RepairTree`; it does not pick a member. The
description is computed by the *same* structural recursion on `φ`, so the small
core IR pays off twice: the `Shape` enum
([`shape.rs`](../crates/shifty-algebra/src/shape.rs)) has ~15 variants, it is
already in NNF with negation pushed to the leaves (recursive `¬` stays a
literal — doc 04 §1), and it is a DAG we can fold over.

**Compute from the algebra, not from the report.** The W3C report walker
deliberately treats `sh:and`/`sh:or`/`sh:not`/`sh:node` as opaque units (it does
not drill into sub-failures — `report.rs` module docs). Repair *must* drill in:
to describe how to repair `φ₁ ∧ φ₂` you need the repair spaces of both conjuncts.
So the API recurses over the `ShapeArena`, using the report only to seed *which*
statements failed at *which* focus nodes.

---

## 2. The driver boundary

The library and the driver split cleanly: the library is a set of pure functions
that decide nothing; the driver supplies data, choices, and control flow.

| Library provides (pure, decides nothing) | Driver provides |
|---|---|
| `witness_violations(G, S) -> Vec<FocusWitness>` — *what's wrong* (§5) | which focus, in what order |
| `synthesize_focus(arena, &[FocusWitness]) -> RepairTree` — the inspectable space of fixes (§6) | how to fill holes / pick branches / set counts |
| `render(&RepairTree) -> RepairProgram` — the parameterized-graph view (§7) | its own data sources, ML, human input, ASP, … |
| `candidates(&HoleConstraint, source) -> Vec<Term>` — *optional* helper (§3.3) | the `Plan` of choices |
| `instantiate(&RepairTree, &Plan) -> Instantiated` — choices → concrete edits (§3.2) | whether to apply, and to which store |
| `gate(G, S, &ΔG) -> RepairOutcome` — what it fixes / would break, **returned, not acted on** (§8) | accept? apply? re-witness? loop? terminate? |

Everything below specifies these types and functions. The control loop that
chains them is the driver's; a reference implementation is in doc 07.

---

## 3. The template IR

### 3.1 `RepairTree`

Four constructs, mirroring `φ` on purpose — a repair tree is the skeleton of a
satisfaction proof. **Every node carries a stable `NodeId`** (assigned by
`synthesize`) so a `Plan` (§3.2) can reference it.

```rust
/// Stable identity for a RepairTree node. Holes carry their own ids; this
/// extends addressing to the Any/Repeat/Edits nodes a Plan must name.
pub struct NodeId(pub u32);

/// A typed placeholder. Drivers bind holes to terms.
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

/// The repair space for one (sub)shape, as an AND/OR/repeat tree. The leading
/// `NodeId` is the node's stable address.
pub enum RepairTree {
    Noop(NodeId),                              // already satisfied — empty repair
    Blocked(NodeId, BlockReason),              // unrepairable in scope (e.g. opaque SPARQL)
    Edits  { id: NodeId, edits: Vec<Edit>, holes: Vec<(Hole, HoleConstraint)> },
    All    { id: NodeId, children: Vec<RepairTree> },   // satisfy every child
    Any    { id: NodeId, children: Vec<RepairTree> },   // satisfy any one child
    Repeat { id: NodeId, body: Box<RepairTree>, min: u64, max: Option<u64> },
}
```

Everything the design named has a home: **variadic elements** are `Repeat`;
**deep / nested elements** come from `ConformsTo` holes and path materialization
(§4); the **variety of patterns** is the `Any` branching; and a hole with
`HoleConstraint::Typed`/`OneOf` is exactly the seam a driver fills.

`BlockReason` enumerates why a branch admits no data repair (`OpaqueSparql`,
`CannotMutateIdentity`, `Coinductive`, …); it is also surfaced so a driver can
explain *why* nothing is offered. (Its full set, and an optional per-edit cost
annotation to drive driver-side minimality, are open — §9.)

### 3.2 `Plan` and `instantiate`

A `Plan` is the driver's choices as data — keyed by `NodeId`/`Hole` so it is
serializable and position-stable. Partial plans are allowed.

```rust
pub struct Plan {
    pub branch:  HashMap<NodeId, usize>,   // which child to take at an `Any`
    pub count:   HashMap<NodeId, u64>,     // how many instances at a `Repeat`
    pub binding: HashMap<Hole, Term>,      // chosen term per hole
}

/// Apply a (possibly partial) Plan: emit the resolved edits and report whatever
/// is still open, so a driver can fill in one shot or interactively (the
/// BuildingMOTIF `template.evaluate(partial)` shape).
pub fn instantiate(tree: &RepairTree, plan: &Plan) -> Instantiated;

pub struct GraphDelta { pub add: Vec<Triple>, pub delete: Vec<Triple> }

pub struct Instantiated {
    pub delta: GraphDelta,                          // edits resolved so far
    pub open_holes:   Vec<(Hole, HoleConstraint)>,  // still need a term
    pub open_choices: Vec<NodeId>,                  // Any/Repeat still needing a pick
}
```

A fully-resolved plan yields `open_holes`/`open_choices` empty and a complete
`delta`. `instantiate` never validates and never chooses — it is a pure fold of
the plan over the tree.

### 3.3 Candidate enumeration (optional helper)

```rust
/// Existing terms in `source` that satisfy a hole's constraint. `source` is any
/// store the driver brings (not necessarily `G`); the constraint is the §7
/// self-hosted sidecar shape, so a driver may equally evaluate candidates itself
/// and skip this helper entirely.
pub fn candidates(c: &HoleConstraint, source: &dyn PathBackend) -> Vec<Term>;
```

This is a convenience for reuse-oriented drivers; the contract places no
requirement on *where* a binding's term comes from.

---

## 4. The `φ → RepairTree` mapping

The altitude view: each `Shape` constructor has a canonical, finite family of
satisfaction strategies. §6 gives the concrete, witness-driven dispatch
(`repair`/`break_`/`build`); the table below is the conceptual map it implements.

| `Shape` | `RepairTree` | notes |
|---|---|---|
| `Top` | `Noop` | |
| `TestConst(c)` | bind value-hole to `Const(c)`, or `Add` the triple yielding `c` along the governing path | finite |
| `TestType(τ)` | `Edits` introducing a `Typed(τ)` hole for the value node | **infinite/semantic hole** |
| `TestKind(K)` | `Kind(K)` hole | |
| `Closed(Q)` | `All` of `Delete` edits, one per offending predicate ∉ Q | data-only stance |
| `Eq/Disj/Lt/Le` | pairwise `Add`/`Delete` to establish/break the relation | see §9 |
| `UniqueLang(π)` | `Delete` all-but-one value per duplicated language tag | see §9 |
| `Not(φ′)` | *falsify* φ′ via its satisfaction trace (§5.2) — flips add → delete | recursive `¬` is a literal under NNF |
| `And(φᵢ)` | `All([…])` | conflict-prune merges (§6) |
| `Or(φᵢ)` | `Any([…])` | where templates branch |
| `Count{π,min,..}` (under min) | `Repeat{ body, min = min − have, max }` | the variadic add |
| `Count{π,..,max}` (over max) | choose `have − max` matching π-values to `Delete` | variadic delete |
| `Sparql(_)` | `Blocked(OpaqueSparql)` | escape-hatch |

**Path materialization.** When the governing path `π` is a `Seq` (doc 00 §2),
adding "a π-reachable value" means minting the intermediate nodes too: each
interior step becomes a `Fresh` hole and a chained `Add`. That is how a single
`Count` over a multi-step path produces a deep repair block. `Inverse` flips
subject/object in the pattern; `Alt` becomes an `Any`; `Star` is bounded (§5).

---

## 5. Witnessing — the input to synthesis

To describe repairs only for the parts of `φ` that actually failed, the API must
know *which* sub-shape failed at *which* value node, reached by *which* path. The
engine already has 80% of this: [`explain`](../crates/shifty-engine/src/validate.rs)
walks the arena, prunes satisfied branches (via the `holds` fast-path and the gfp
back-edge guard), and returns a `Reason` tree. But `explain` is tuned for humans
and loses what repair needs: paths rendered to `String` (repair needs the
structured `Path`), `And` flattened (repair needs the conjunction as a node), and
`Count` gaps as message text (repair needs `(path, have, min, max, qualifier)`).

So the witness is the **structured, lossless sibling of `Reason`**: the failed
sub-DAG of `φ`, pruned to exactly what did not hold, carrying at each node the
`ShapeId` (so synthesis recovers the constraint from the arena) and the
structural gap.

### 5.1 The witness (additive direction: what to *add*)

```rust
pub struct FocusWitness {
    pub focus: Term,
    pub statement: usize,     // index into Schema::statements (matches Violation)
    pub failure: Witness,
}

pub enum Witness {
    /// A leaf atom failed at `node`, reached from the focus by `reached_by`.
    /// `shape` recovers the exact constraint from the arena
    /// (TestConst / TestType / TestKind / Eq / Disj / Lt / Le / UniqueLang).
    /// `produced_by` (the same PathSupport the deletive side uses, §5.2) names
    /// the triples that made `node` a value — `Some` for value-scoped atoms,
    /// `None` when the atom is on the focus itself (`reached_by = Id`); it is
    /// what `repair` needs for *replace-in-place*.
    Atom { shape: ShapeId, node: Term, reached_by: Path, produced_by: Option<PathSupport> },

    /// `closed(Q)` failed: these (predicate, object) pairs are not allowed.
    Closed { shape: ShapeId, node: Term, offenders: Vec<(NamedNode, Term)> },

    /// `¬φ` failed because φ *holds* at `node` — it must be falsified. The child
    /// is φ's satisfaction trace (§5.2). This crossing flips add → delete.
    Not { shape: ShapeId, node: Term, inner: Box<SatTrace> },

    /// Conjunction: every child failed and ALL must be repaired. → All
    All { shape: ShapeId, node: Term, failed: Vec<Witness> },

    /// Disjunction: no branch held; repairing ANY ONE suffices. → Any
    Any { shape: ShapeId, node: Term, branches: Vec<Witness> },

    /// `∃≥min π.q` under-satisfied: `have` values match, `min` required. → Repeat
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
    /// non-algebraic driver still has context. → Blocked
    Opaque { shape: ShapeId, node: Term, value: Option<Term>, path: Option<Path> },
}
```

### 5.2 The satisfaction trace (deletive direction: what to *delete*)

A `Not(φ)` failure can only be repaired by **breaking φ** — which needs the dual
of a failure trace: a record of *why φ currently holds*. Crossing a `Not` flips
the direction; `Count` is the other flip point (self-dual — break the lower bound
by *deleting* matches, the upper bound by *adding* them). So `Witness` and
`SatTrace` are **mutually recursive**, and every `Not` toggles add ↔ delete.

```rust
pub enum SatTrace {
    /// ⊤ — vacuously true; no graph edit falsifies it.
    Irrefutable { shape: ShapeId },

    /// A leaf atom holds at `node`. `produced_by` names the existing triples that
    /// make `node` a π-successor; cutting them removes it from the value set.
    Atom { shape: ShapeId, node: Term, reached_by: Path, produced_by: PathSupport },

    /// Conjunction holds because ALL children hold ⟹ break ANY ONE (dual of All).
    AllHeld { shape: ShapeId, node: Term, children: Vec<SatTrace> },

    /// Disjunction holds because these branches hold ⟹ break EVERY one (dual of Any).
    AnyHeld { shape: ShapeId, node: Term, satisfied: Vec<SatTrace> },

    /// `∃[min..max] π.q` holds ⟹ drop below min (delete) OR exceed max (add — flips
    /// to the additive side). `matches` carries each counted value with its q-support.
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
    /// with no finite set of facts to delete (§5.4).
    Coinductive { shape: ShapeId, node: Term },
}
```

`PathSupport` is the deletion-side dual of path materialization: instead of
minting a chain to a fresh value, it enumerates the *existing* triples that made
`node` a π-successor. For `Pred(p)` it is the single triple `(focus, p, node)`;
for a `Seq` it is an OR over the chain (cut any one edge); `Alt`/`Star` give
multiple parallel witnessing chains, OR'd together.

### 5.3 The interlocked signatures

Two sibling folds over the arena, parallel to `explain`, sharing the same `holds`
oracle and back-edge guard — **exact complements**: for a given `(node, id)`
exactly one returns `Some` (modulo the back-edge convention).

```rust
fn witness(eval: &mut ShapeEvaluator, node: &Term, id: ShapeId,
           reached_by: &Path, stack: &mut HashSet<(ShapeId, Term)>) -> Option<Witness>;
fn sat_trace(eval: &mut ShapeEvaluator, node: &Term, id: ShapeId,
             reached_by: &Path, stack: &mut HashSet<(ShapeId, Term)>) -> Option<SatTrace>;

/// Public driver entry, mirroring `validate` (same statement × focus loop, same
/// stratifiability check). `_graphs` / `_with_context` / `_plan` variants follow.
pub fn witness_violations(data: &Graph, schema: &Schema)
    -> Result<Vec<FocusWitness>, NonStratifiable>;
```

At a `Not(c)` the two cross: `witness` ⇒ `sat_trace(c).map(Witness::Not)`;
`sat_trace` ⇒ `witness(c).map(SatTrace::NotHeld)`. Four behaviors must be carried
over verbatim from `explain`, or repairs corrupt silently: the **back-edge ⇒
`None`/`Coinductive`** convention; the **`holds` fast-path** (prunes satisfied
branches); the **∀-encoding drill-in** (`∃≤0 π.¬inner` → `CountHigh.per_value`);
and **structured path threading** (at a `Count`, recurse with `reached_by · path`).

### 5.4 Termination and the honest limit

`witness` terminates exactly like `explain` (the stack guard); it is finite. A
gfp support reached on a back-edge (`SatTrace::Coinductive`) is assumed true
*without* a grounded justification, so there is no finite set of facts to delete
— deletion-direction repair is genuinely **incomplete through positive
recursion** (§9). The first cut falsifies leaf atoms and reports `Coinductive`
otherwise.

---

## 6. Synthesis — witness → `RepairTree`

`synthesize_focus` is the public entry; internally it is three mutually-recursive
folds, mirroring the witness/trace duality plus one direction the additive side
forces:

- **`repair(Witness)`** — additive: make a *failing existing node* hold.
- **`break_(SatTrace)`** — deletive: falsify a *holding existing node*.
- **`build(ShapeId, Hole)`** — additive, *hypothetical*: constrain a
  *not-yet-existing* node to satisfy a shape.

`build` exists because `CountLow` says "add `min − have` new values satisfying
`qualifier`" — values that don't exist yet, so there is nothing to witness
against. It walks the **`Shape`** (everything must be constructed), whereas
`repair`/`break_` walk the already-pruned witness/trace. That is where **fuel**
lives: `build` on a recursive shape can diverge; `repair`/`break_` are finite.

```rust
struct Synth<'a> { arena: &'a ShapeArena, next_node: u32, next_hole: u32, fuel: u32 }

impl Synth<'_> {
    fn repair(&mut self, w: &Witness)  -> RepairTree;   // walks the witness  (finite)
    fn break_(&mut self, s: &SatTrace) -> RepairTree;   // walks the trace
    fn build(&mut self, id: ShapeId, hole: Hole) -> RepairTree;  // walks the shape (fuel-bounded)
}

/// One focus, one statement.
pub fn synthesize(arena: &ShapeArena, w: &FocusWitness) -> RepairTree;

/// Per-focus grouping: `All` over a focus's statement-witnesses, so a driver can
/// fill one Plan that fixes everything a focus violates at once. (Whether a
/// driver solves the group jointly or branch-by-branch is its choice — doc 07.)
pub fn synthesize_focus(arena: &ShapeArena, ws: &[FocusWitness]) -> RepairTree;
```

**`synthesize` is graph-free**: all contact with `G` happened during witnessing,
so it is a pure function of `(witness/trace/shape, arena)`. It **does not validate
and does not choose** — it emits the inspectable space; the driver fills it (§3.2)
and gates it (§8).

### 6.1 Dispatch

`repair` (additive) — walks the witness:

| `Witness` | `RepairTree` |
|---|---|
| `Atom` (value-scoped, has `produced_by`) | `Any([ replace-in-place, … ])` — delete the offender, `build` a good value |
| `Atom` (focus identity, e.g. nodeKind on focus) | `Blocked(CannotMutateIdentity)` |
| `Closed{offenders}` | `All([ Delete(node,p,o) … ])` |
| `All{failed}` | `All(failed.map(repair))` |
| `Any{branches}` | `Any(branches.map(repair))` |
| `CountLow{path,qualifier,have,min}` | `Repeat{ min: min−have, max: None, body: build_value(path, qualifier) }` |
| `CountHigh` (∀-encoding) | `All(per_value.map(\|(v,inner)\| repair(inner) @ v))` |
| `CountHigh` (plain max) | `Repeat{ min: have−max, body: Delete(node, p, OneOf(matched)) }` |
| `Not{inner}` | `break_(inner)` — flip to deletive |
| `Opaque` | `Blocked(OpaqueSparql)` |

`break_` (deletive) — the De Morgan mirror:

| `SatTrace` | `RepairTree` |
|---|---|
| `Atom{produced_by}` | `Any([ Delete(edge) … ])` over the `PathSupport` cut-options |
| `AllHeld` | `Any(children.map(break_))` — break any one |
| `AnyHeld` | `All(satisfied.map(break_))` — break all |
| `CountHeld{min,max}` | `Any([ drop-below-min (deletes), exceed-max (builds) ])` |
| `NotHeld{inner_fails}` | `repair(inner_fails)` — flip to additive |
| `Irrefutable` / `Blocked` / `Coinductive` | `Blocked(…)` |

`build` (hypothetical) — walks the shape, attaches hole constraints:

| `Shape` | `build(id, hole)` |
|---|---|
| `Top` | `Noop` |
| `TestConst/TestType/TestKind` | `Edits{ holes:[(hole, Const/Typed/Kind)] }` |
| `And(cs)` / `Or(cs)` | `All` / `Any` of `build(c, hole)` — same hole, combined |
| `Count{path,min,..,q}` | `Repeat{ min, body: build_value(path, q) }` — **fuel − 1** |
| recursive `id` at **fuel 0** | `Edits{ holes:[(hole, ConformsTo(id))] }` — defer to the driver |
| `Closed(Q)` | `Noop` — a freshly built node simply omits disallowed predicates |
| `Sparql` | `Blocked(OpaqueSparql)` |

`build_value(path, q)` mints the interior chain of a `Seq` path as `Fresh` holes,
ends at a value-hole `vh`, and calls `build(q, vh)` — how a single `CountLow` over
a multi-step path yields a deep repair block. At fuel 0 a recursive hole becomes
`ConformsTo(id)` — left for the driver (e.g. the monomorphism driver, doc 07) to
bind to an existing conforming node.

---

## 7. Rendering — `render(&RepairTree) -> RepairProgram`

Drivers that match against a graph (the monomorphism driver, doc 07) or display
to a human need the template *as an RDF graph*: a parameterized graph in which
some nodes are holes — the BuildingMOTIF representation. (The ASP and enumeration
drivers consume the `RepairTree`/`Plan` directly and need no RDF.)

- **Holes as parameter nodes.** Each `Hole` renders to a reserved-namespace IRI,
  `urn:shifty:hole#<id>` (the analogue of BuildingMOTIF's `urn:___param___#`); a
  `TriplePattern` renders to a triple with bound slots as terms and open slots as
  the hole IRI.
- **Constraints as a self-hosted SHACL sidecar.** A `HoleConstraint` *is* a shape,
  so it renders as a node shape on the hole IRI **in the engine's own algebra**
  (`Const`→`test(c)`, `Typed`→value-type, `Kind`→`sh:nodeKind`, `OneOf`→`sh:in`,
  `ConformsTo(q)`→`sh:node q`, `Fresh`→a mint-only marker, `AnyNode`→nothing). No
  second constraint language; the sidecar is itself a `Schema` over the holes,
  checked by the same validator the repair targets.
- **Flattening to DNF.** `Any`→alternatives (one pattern each); `All`→merge
  add-graphs/sidecars/delete-sets (contradictions prune the branch);
  `Repeat{min,max}`→a count-parameterized block (fresh hole copies per instance);
  `Blocked`→drop the alternative.

```rust
pub struct RepairProgram { pub patterns: Vec<RepairPattern> }   // the alternatives
pub struct RepairPattern {
    pub add:    Graph,                            // real predicates + hole IRIs
    pub params: Schema,                           // self-hosted sidecar over the holes
    pub repeat: Vec<(NodeId, u64, Option<u64>)>,  // count-parameterized sub-blocks
    pub delete: Vec<TriplePattern>,               // applied, not searched
}
```

This is a *view*, not a decision: it restates the same repair space in a form a
graph-matching or display driver can consume.

---

## 8. The gate — `gate(G, S, &ΔG) -> RepairOutcome`

A repair is sound only if it does not trade one violation for another, so the
gate is **whole-graph**, not focus-local. It re-validates and returns a structured
verdict; it **never applies the delta and never decides** — the driver does.

```rust
pub struct RepairOutcome {
    pub fixed:      Vec<Violation>,   // violations this delta removes
    pub introduced: Vec<Violation>,   // NEW violations it would cause
    pub remaining:  Vec<Violation>,   // pre-existing, still unfixed
}
/// A delta is *sound* iff `introduced.is_empty()`. Soundness + `fixed` non-empty
/// is *progress*. The driver chooses what to require.
pub fn gate(data: &Graph, schema: &Schema, delta: &GraphDelta) -> RepairOutcome;
```

The verdict is exactly the set difference of `violations(G ⊕ ΔG, S)` against
`violations(G, S)`, computed by reusing `validate` unchanged. Because the
contract is this delta — not a bare `v ⊨ φ` — a cheaper *affected-set*
re-validation (only the nodes `ΔG` can touch, from `shifty-opt`'s
dependency/demand analysis) can replace the implementation later with identical
semantics.

---

## 9. Open questions / limitations

- **`BlockReason` and cost are under-specified.** `BlockReason`'s full set is
  TBD, and the IR carries no per-edit **cost** annotation — drivers that want
  minimality (fewest edits, reuse-over-mint) need one to rank `Plan`s. Both are
  IR additions, not just docs.
- **Relational atoms are lumped into `Witness::Atom`** (`Eq`/`Disj`/`Lt`/`Le`/
  `UniqueLang`), but §6.1's `Atom` dispatch only does value-replace. These are
  *relational* (set-equality across two paths; retag/drop a duplicate-language
  pair) and need their own dispatch — or to be explicitly scoped out of the first
  cut. The mapping table (§4) currently over-promises here.
- **`CountHigh` deletion assumes a single-predicate path.** §6.1 emits
  `Delete(node, p, OneOf(matched))`, but the `Count` path can be a `Seq`/`Star`;
  `CountHigh` needs per-matched-value `PathSupport` (the same enrichment `Atom`
  got) to cut a complex path correctly.
- **`Blocked` propagation is unspecified.** `All([Blocked, X])` should collapse to
  `Blocked`; `Any([Blocked, X])` to `Any([X])`. The IR algebra for this needs
  pinning down (and affects what `synthesize` returns vs. what a driver sees).
- **`Not` and deletion completeness.** Falsification rides the satisfaction trace
  (§5.2); it is *incomplete through positive recursion* (`Coinductive`, §5.4).
- **Schema repairs** are out of scope for this cut, but the IR does not preclude
  them.

## 10. Build order (the API surface)

1. **Witnessing evaluator** (§5) — `witness` + `sat_trace`, sibling folds beside
   `explain`, exporting `FocusWitness` / `witness_violations`.
2. **Template IR** (§3) with serde — `RepairTree` (+`NodeId`), `Plan`,
   `instantiate`, `RepairOutcome`.
3. **`synthesize` / `synthesize_focus`** (§6) — `repair`/`break_`/`build`,
   fuel-bounded.
4. **`gate`** (§8) — whole-graph delta-of-violations over `validate`.
5. **`render`** (§7) — `RepairTree` → `RepairProgram` with the self-hosted sidecar.

Reference drivers built on this surface — monomorphism, enumeration, ASP, LLM,
and the fixpoint loop — are specified in
[`07-repair-drivers.md`](07-repair-drivers.md).
