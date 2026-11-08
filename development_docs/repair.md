# Symbolic data repair

Repair synthesis exposes candidate data edits as a `RepairTree`. A driver
chooses branches, repetition counts, and terms; instantiation produces a
`GraphDelta`; whole-schema validation checks its effects. Repair is experimental
and intentionally incomplete. The schema remains fixed.

Implementation: [repair IR](../crates/shifty-repair/src/ir.rs),
[instantiation](../crates/shifty-repair/src/instantiate.rs),
[synthesis](../crates/shifty-engine/src/synthesize.rs), and
[gate](../crates/shifty-engine/src/gate.rs).
See [evidence](evidence.md) for synthesis inputs and
[enumerate.rs](../crates/shifty-engine/src/enumerate.rs) for the reference driver.

## Abduction and the gate

For a failed formula, the semantic repair problem is:

```text
repair(φ,v) = { ΔG : (G ⊕ ΔG),v ⊨ φ }
```

The tree describes a bounded candidate space derived from evidence. It is not
an exhaustive characterization of every possible graph repair. Instantiation
alone establishes neither soundness nor progress; the gate checks the concrete
delta against the full schema.

## Engine and driver boundary

| Engine/library operation | Driver decision |
| --- | --- |
| Witness selected failures | Which focus to address |
| `synthesize` or `synthesize_focus` | How to search the tree |
| Expose typed hole constraints | Candidate terms and external data sources |
| `instantiate(tree, plan)` | Branch, count, and binding choices |
| Gate the candidate | Whether to accept and apply it |

Synthesis consumes the algebra and structured evidence. W3C result messages do
not retain the Boolean and polarity structure required to synthesize repairs.

## Template IR and plans

Every tree node has a tree-local `NodeId`:

| Operator | Meaning |
| --- | --- |
| `Noop` | Empty repair |
| `Blocked` | No repair supported in this branch, with a reason |
| `Edits` | Add/delete triple patterns with declared typed holes |
| `All` | Combine every child |
| `Any` | Choose one child |
| `Repeat` | Instantiate a body a chosen number of times |

Triple-pattern slots are either fixed RDF terms or `Hole` identifiers.
`HoleConstraint` includes `AnyNode`, `Fresh`, `Const`, `Typed`, `Kind`, `OneOf`,
`ConformsTo`, and `ConformsToAll`. The last two carry arena shape obligations
for candidate values. `ConformsToAll` keeps multiple requirements on the same
fresh value together, including a qualified count and universal siblings on
that path.

`Edit.cost` is a default driver ranking hint, not a proof of global minimality.
Synthesis normalizes composition: a blocked child blocks an `All`; blocked
alternatives are discarded from an `Any` when another branch is available.

A `Plan` has three maps:

- `branch: NodeId → child index` for `Any`.
- `count: NodeId → instance count` for `Repeat`.
- `binding: Hole → Term` for open triple slots.

Partial plans are allowed. `instantiate` returns resolved edits plus
`open_holes` and `open_choices`. Repeated bodies receive fresh per-instance hole
IDs deterministically for a fixed tree and counts. A driver can set counts,
instantiate to discover the renamed holes, bind them, then instantiate again.

Instantiation resolves choices and RDF triple positions; it does not validate
hole constraints or enforce repeat bounds. Invalid RDF position combinations
emit no triple. Drivers must check their choices and gate the result rather
than treating an empty open-hole list as acceptance.

## Failure and satisfaction folds

`Synth::repair` folds failure evidence into ways to make a constraint hold.
`Synth::break_` folds satisfaction evidence into ways to make it fail. They
cross at negation:

```text
repair(Not-held-failure) → break_(inner satisfaction)
break_(NotHeld)         → repair(inner failure)
```

| Evidence case | Candidate composition |
| --- | --- |
| Failed conjunction | Repair every failed child (`All`) |
| Failed disjunction | Repair one branch (`Any`) |
| Holding conjunction | Break one holding child (`Any`) |
| Holding disjunction | Break every holding branch (`All`) |
| Count below minimum | Materialize missing qualifying values with endpoint obligations |
| Count above maximum | Remove enough qualifying support or break qualifiers where supported |
| Closed-shape failure | Delete forbidden outgoing triples |
| Produced value fails an atom | Replace its producing value with a constrained hole where editable |
| Focus identity fails | `Blocked(CannotMutateIdentity)` |
| Opaque SPARQL or unsupported expression repair | Explicit blocked branch |

Relational leaves have kind-specific strategies for equality, disjointness,
ordering, and unique language. They remain bounded candidates rather than a
complete solver for arbitrary value-set reconciliation.

## Path materialization and support

Additive synthesis expands a path to bounded predicate words. Inverses reverse
both hop order and direction; sequences concatenate; alternatives offer
branches. Stars are first taken reflexively. If this leaves no concrete hop,
synthesis tries one application. Empty words are discarded because they add no
support, and expansion retains at most 16 words.

For `rdf:type/rdfs:subClassOf*`, this permits the minimal `rdf:type` assertion.
Interior path nodes become fresh holes. The endpoint carries its value/type or
shape obligations. Composite qualifier shapes remain `ConformsTo` or
`ConformsToAll` constraints; synthesis does not recursively construct arbitrary
fresh subgraphs satisfying them.

`PathSupport` contains one positive reachability certificate, not every route
or a complete cut. Cutting one cited edge may leave another route. Deletion
candidates therefore require revalidation. Positive recursion can yield a
`Coinductive` satisfaction leaf with no finite deletion certificate; synthesis
blocks that direction explicitly.

The folds terminate over finite evidence. They do not unfold recursive shape
requirements indefinitely or rely on a structural-build fuel counter.

## Origins

`synthesize_with_origins` and `synthesize_focus_with_origins` return
`SynthesizedRepair`: the tree plus origins for retained nodes. An origin
identifies the optional statement, child-index evidence path, normalized shape,
judgment node, polarity, and exact evidence kind. A joint synthetic root can cite
multiple origins.

Origin paths use `EvidenceNodeRef::children()`, sharing the public traversal
grammar. The repair crate stays independent of engine evidence types. See
[the evidence contract](evidence.md).

## Whole-schema gate

The low-level `gate(data, context, schema, delta)` validates the baseline and
patched graphs and compares violation identities `(focus, statement)`:

```text
fixed      = baseline failures − candidate failures
introduced = candidate failures − baseline failures
remaining  = candidate failures ∩ baseline failures
```

Deletes apply before additions, so a re-add wins on conflict. `is_sound()` means
no introduced failures; `is_progress()` additionally requires at least one fixed
failure. These are whole-statement identities, not a comparison of every
interior failing atom.

`EvaluationSession::gate` also preserves snapshot inference and graph-role
policy, rebuilding from patched asserted data. The low-level gate directly
patches its supplied materialized data/context. Applications must use the gate
matching their session contract. Neither gate applies edits to caller storage.

## Candidate sources

The reference `candidates` helper truncates each pool to `max_candidates`:

| Hole constraint | Candidate pool |
| --- | --- |
| `Const` | The required term |
| `OneOf` | The finite declared domain |
| `Fresh` | A newly minted blank node |
| `AnyNode`, `ConformsTo`, `ConformsToAll` | Existing graph nodes plus a fresh node |
| `Typed` | Existing terms passing the value predicate |
| `Kind` | Existing terms of the requested kind |

Composite shape requirements are not solved by candidate collection. The gate
checks the chosen terms in the patched graph. The helper cannot invent a novel
literal satisfying an infinite datatype, regex, or numeric domain; an external
candidate source is needed when no existing value suffices.

## Enumeration algorithm

`enumerate_repair` performs depth-first plan completion:

1. Instantiate the current partial plan.
2. Branch over the first open `Any`, or set an open `Repeat` to its minimum.
3. Bind the first open hole from its candidate pool.
4. When choices and holes are resolved, decrement the gate budget and validate.
5. Return the first candidate whose outcome is progress; otherwise backtrack.

`EnumOptions` bounds gated candidates and terms per hole. Repeats use only their
minimum count; enumeration does not search every legal repetition count.
Default bounds are 10,000 gated candidates and 64 terms per hole.

The result retains the chosen plan, concrete delta, and gate outcome. It makes
no global minimality claim and can miss repairs requiring larger repetitions,
fresh literals, structural construction, or cross-focus coordination.

## Repair loop

`repair_to_fixpoint` witnesses, synthesizes, enumerates, gates, and applies one
accepted delta before re-witnessing. Acceptance requires no new failure and at
least one fixed failure, so the violation set strictly shrinks. Search failure
does not prove no repair exists. Greedy per-focus search can miss repairs that
require coordinated changes across several foci.

The reference loop edits supplied materialized data/context graphs. A caller
requiring inference to be recomputed after each edit uses session-aware gating.
External solvers and candidate providers consume the same IR and still gate the
concrete delta before applying it.
