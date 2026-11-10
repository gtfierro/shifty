# Violation generation

`shifty_experimental::vio` (crate `shifty-vio`) generates repair benchmarks. It
injects SHACL violations with known inverse edits into a conforming reference
graph. It reimplements the generator of Lin et al.
([arXiv 2507.22419](https://arxiv.org/abs/2507.22419)) and its multi-violation
composition ([arXiv 2611.07342](https://arxiv.org/abs/2611.07342)) over
Shifty's typed evidence. It covers test-case generation only, not the repair
agent. Python's `shifty.experimental.vio` is a thin wrapper.

Implementation: [shifty-vio](../crates/experimental/shifty-vio/src/), with
[expand.rs](../crates/experimental/shifty-vio/src/expand.rs) for the rewrite
system and [universe.rs](../crates/experimental/shifty-vio/src/universe.rs)
for targets, sampling, and composition. Binding:
[vio.rs](../python/src/vio.rs). Wrapper:
[vio.py](../python/shifty/experimental/vio.py). Tests:
[generate.rs](../crates/experimental/shifty-vio/tests/generate.rs) and
[test_vio.py](../python/tests/test_vio.py). User guide:
[generate-violations](../docs/how-to/generate-violations.rst).

## API

Three public nouns, the same in Rust and Python:

- `Universe`: every injection the expansion finds. It is immutable. A
  private `Core` in an `Rc` holds the session, the baseline evidence, the
  targets, and the caches. A universe is that core, an optional filter
  closure, and `Weights`. `filter` (Python `where`) composes another
  closure, and `weighted` replaces the weights. Both share the core, so
  views are cheap and verification results carry across them.
- `Injection`: one member, identified by `key`. The normalized ids it
  carries (`component`, and each operation's `constraint` and `requires`)
  are crate-private, since arena ids are not stable across processes.
- `Case`: what `draw(k, seed)` returns. It holds the injections, the
  `CaseFailure`s, the delta as a `GraphDelta`, and an `Rc` of the reference
  triples for `graph_ntriples`.

`draw` and `draws` are the only ways to compose: verification and
revalidation are always on, the salt is `seed:k`, and the reservoir holds
`max(8k, 64)`. The Python wrapper keeps one `Injection` dataclass per key,
so case injections are the universe's objects.

## Streaming

`Core::expand` streams injections to a callback, target by target. It keeps
a set of 64-bit key digests to drop duplicates, so the first target that
reaches a delta owns it, exactly as an eager expansion would. `draws` runs
one pass and offers each member that passes the filter to one `Reservoir`
per request. Composition starts once the pass ends.

`len`, `get`, and `iter` call `Core::all`, which runs the pass into a
`OnceCell<Vec<Injection>>`. A view's member indices are another
`OnceCell`. Once the cache exists, draws iterate it instead of
re-expanding. A reservoir keeps the smallest keys whatever the arrival
order, so both paths return the same case.

Python filters run in Rust. `shape`, `predicate`, and `focus` become set
lookups. A callable is wrapped as a closure that calls back with the
injection's tuple, under `Python::attach`. A Rust filter returns a bool, so
the first exception is parked on the view and raised when the call that ran
the filter returns.

## Inputs

`Universe::new` compiles the shapes and takes the asserted data as the
reference; with `infer` (the default) every session over it runs the rules.
When the reference has blank nodes, they are relabeled with
oxrdf's canonicalization (`canonicalize_blank_nodes`, on by default), since
parsers label anonymous nodes afresh on every parse. An `EvaluationSession`
over that graph, with `SessionData::Separate`, keeps the labels. Evidence
terms, edit deltas, and emitted files therefore share one set of labels.

A target is a passing `(focus, statement)` pair from `session.evidence()`.
Pairs that already fail are left alone and reported as `baseline_failures`.
Foci with no outgoing reference triples are skipped by default.

Terms are N-Triples strings throughout ([nt.rs](../crates/experimental/shifty-vio/src/nt.rs)).
Strings order totally, so every set the expansion iterates has one order. They
are parsed back only to probe the engine and to build a `GraphDelta`.

## Rewrite system

The paper's `VIO(x, φ)` is a rewrite over the shape grammar. Here it is a
fold over the `Satisfaction` evidence of the normalized algebra. The dual
`SAT(x, φ)` folds `Failure` evidence. They cross at negation, mirroring
`Synth::repair`/`Synth::break_` in [repair](repair.md).

| Evidence | VIO alternatives |
| --- | --- |
| `AllHeld` | break any one child (`∪`) |
| `AnyHeld` | break every satisfied disjunct (`×`, merged) |
| `NotHeld` | `SAT` of the failing inner shape |
| `CountHeld`, `min=n` | knock out `observed − n + 1` matches; each is cut, substituted with a value failing `q`, or broken internally (`VIO(v, q)`), as in Rule 2 of the paper |
| `CountHeld`, `max=n` | make `n − observed + 1` more reached values satisfy `q`: add new values, swap existing ones, or fix them in place (`SAT(v, q)`) |
| `ForAllHeld` | substitute or internally break one value, or add a value failing `q` |
| `Blocked(ClosedNeedsAdd)` | add a predicate outside the closed set |
| atoms, `Irrefutable`, `Coinductive`, other `Blocked` | none |

| Evidence | SAT alternatives |
| --- | --- |
| `All` / `Any` | repair every failed child / any one branch |
| `Not` | `VIO` of the holding inner shape |
| `CountLow` | add qualifying values, or fix rejected candidates (substitute or `SAT`) |
| `CountHigh` | cut matches, or break `q` at them |

Atoms have no rule of their own. Changing an atom means changing *which value*
sits on an edge, and only the enclosing count or universal knows that edge and
the qualifier the new value must fail or satisfy. Substitution is therefore a
value-level operation. Candidate terms come from the facets, constants, and
classes under the qualifier (`Core::leaves`), mutations of the current
value, other values of the predicate, typed nodes, and a fresh IRI
([propose.rs](../crates/experimental/shifty-vio/src/propose.rs)). The engine
checks each one with `PreparedEvidenceValidator::explain_constraint(term,
qualifier)` before use, with results cached per `(term, qualifier)`.
Candidates of the current value's kind come first.

When `max` is broken by adding values, the universals registered on the same
`(node, path)` are preferred (`Expander::register`). A new `feeds` value then
satisfies the sibling `sh:class Zone`, and the case stays a cardinality error.
This is the generation-side counterpart of `CountLow.sibling_qualifiers`.

Path edits use `minimal_word`: stars are taken reflexively and the first
alternative is used. `rdf:type/rdfs:subClassOf*` therefore becomes a single
`rdf:type` edge. Interior nodes of longer words are fresh IRIs.

## Normal form and caps

An alternative is a list of `Operation`s. `merge` rejects a joint choice when
one part deletes what another adds, when two parts demand opposite polarities
for one `(node, constraint)`, or when two parts replace one value differently.
An injection's identity, `key`, is its sorted net delta, and duplicates are
dropped.

Every choice point is capped (`ExpansionLimits`). Alternatives are computed
eagerly and truncated. `union` interleaves round-robin, so a capped prefix
still samples every branch. Products and subset combinations also stop after a
fixed number of attempts, so a run of contradictory joint choices cannot spin.

## Determinism

Normalized arena ids are not stable across processes: the same shapes graph
can number its arena differently. `std` hashers are randomly keyed too.
Expansion order depends on neither:

- Sibling evidence is ordered by a content key (`Expander::sat_key`,
  `fail_key`), a digest in which each arena id is replaced by
  `Core::shape_key`, a structural digest of the shape. Ignoring ids
  altogether is not enough. Two counts on one path that differ only in their
  qualifier would tie, and the expansion caps would keep a different prefix.
- Every collection is a sorted `Vec` or a `BTree*` before it is sampled.
  Per-target RNGs (SplitMix64) are seeded from `(focus, shape name)`.
- Hashes are FNV-1a with a SplitMix64 finisher
  ([hash.rs](../crates/experimental/shifty-vio/src/hash.rs)), never `std`'s.
- Fresh IRIs are hashed from terms, never from ids.

`test_universe_and_draws_are_reproducible_across_processes` expands and
draws in three processes over data with an anonymous blank node. The NIST-IBAL
universe was also checked byte-for-byte across processes.

## Sampling and composition

A draw keeps the `M = max(8k, 64)` smallest `−ln h(v) / w(v)`, where `h`
hashes the salt `seed:k` and the key into `(0, 1]`. By default `w` is 1 and
every predicate is treated alike. `Weights::predicates` multiplies in the
geometric mean of the weights of the predicates edited. `Weights::kinds`
multiplies in the geometric mean of the kind weights of the operations.
Missing entries weigh 1. Predicates carry no built-in grouping: any emphasis
comes from the caller's mapping. `filter` is the hard filter, since weights
cannot exclude anything. Only members that pass the filter reach the
reservoir.

Composition scans the reservoir. A candidate is skipped when it conflicts
with a chosen injection or fails `verify`. Conflict is the paper's rule
(Sec. 6.3), implemented by the same `merge` that joins choices inside one
injection: one deletes what the other adds, both substitute one edge
differently, or they need opposite polarities of one `(node, constraint)`.
Shared edits are allowed, and so are several injections on one
`(focus, statement, component)`. There is no limit on blast radius either.
`verify` applies the injection's own delta with `with_delta`, finds the
target pair with `EvaluationSession::select_pair`, and explains only that
pair. `select_pair` runs one statement's target selection, so the pair
must still be selected. It caches the result by key in the shared core. The component
is the top-level conjunct of the statement that the injection breaks. It is
taken from the root `AllHeld` child the expansion started from. An injection
is *realized* when that component's shape id appears among the failing
children of its target's failure evidence. After filling up to `k`, the union
delta is revalidated: `find_failures` decides every pair without building
evidence, and only failing pairs outside the baseline are explained. Masked injections are rejected and the remainder
refilled until nothing is masked. Every `CaseFailure` therefore comes from an
engine run over the final `G_test`.

Realization is checked per component, so with several injections on one
component it proves that the component fails, not that each injection is
separately visible: one could be redundant given another. A leave-one-out
revalidation would check that, at about `k` more revalidations per draw.

Composition dominates the cost of a draw. A candidate check is one target
selection and one explanation. A case check is one conformance pass over
every statement, about 0.15 s on NIST-IBAL, so it grows with the graph.
Sessions are neither `Send` nor `Sync` (the engine holds `Rc` and
`std::cell::OnceCell`), so `generate` parallelizes with processes: `jobs`
splits the requests round-robin, and each worker builds its own universe
from a `_Recipe` of the inputs. Graph objects and input lists are turned
into the Turtle bytes `Universe` would build (`_portable`) before they are
pickled. Workers save their cases and return summary rows; the first also
writes `reference.nt`, and the parent writes `suite.json`. The CLI only
parses arguments and calls `generate`.

## Known gaps

- Relational atoms (`Eq`, `Disj`, `Lt`, `Le`, `UniqueLang`), opaque SPARQL,
  and `sh:expression` are never broken. They sit under `Blocked` or have no
  generation rule.
- `infer` (the default) re-runs rules on every revalidation, and edits target
  asserted triples only. Values whose sole support is inferred cannot be cut;
  that would take tracing the rule back to its premises. An earlier
  `materialize` mode broke the inferred closure and validated it without
  rules; it was removed because on NIST-IBAL 59 of 60 sampled injections
  that deleted an inferred triple stopped failing once the rules ran again.
- Edits are not checked against shapes the target does not mention. Extra
  failures are reported as amplification, not prevented.
- `Sparql` and `Expression` shapes are keyed by their debug form, which can
  include arena ids. Ordering among siblings of those kinds may vary across
  processes.
