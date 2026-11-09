# Violation generation

`shifty.multivio` generates repair benchmarks. It injects SHACL violations with
known inverse edits into a conforming reference graph. It reimplements the
generator of Lin et al. ([arXiv 2507.22419](https://arxiv.org/abs/2507.22419))
and its multi-violation composition
([arXiv 2611.07342](https://arxiv.org/abs/2611.07342)) over Shifty's evidence
API. It covers test-case generation only, not the repair agent.

Implementation: [multivio.py](../python/shifty/multivio.py), tests in
[test_multivio.py](../python/tests/test_multivio.py). User guide:
[generate-violations](../docs/how-to/generate-violations.rst).

## Inputs

The reference graph is normalized to N-Triples once, through
`RepairSession.current_ntriples`, or through `infer` when `materialize=True`.
An `EvidenceSession` is then built over that text. Blank-node labels from
N-Triples survive loading, so the evidence terms, the edit deltas, and the
emitted files all use one set of labels.

A target is a passing `(focus, statement)` pair from `EvidenceSession.validate()`.
Pairs that already fail are left alone and reported as `baseline_failures`.
Foci with no outgoing reference triples are skipped by default.

## Rewrite system

The paper's `VIO(x, φ)` is a rewrite over the shape grammar. Here it is a
fold over the satisfaction evidence of the normalized algebra. The dual
`SAT(x, φ)` folds failure evidence. They cross at negation, mirroring
`Synth::repair`/`Synth::break_` in [repair](repair.md).

| Evidence | VIO alternatives |
| --- | --- |
| `all_held` | break any one child (`∪`) |
| `any_held` | break every satisfied disjunct (`×`, merged) |
| `not_held` | `SAT` of the failing inner shape |
| `count_held`, `min=n` | knock out `observed − n + 1` matches; each is cut, substituted with a value failing `q`, or broken internally (`VIO(v, q)`), as in Rule 2 of the paper |
| `count_held`, `max=n` | make `n − observed + 1` more reached values satisfy `q`: add new values, swap existing ones, or fix them in place (`SAT(v, q)`) |
| `for_all_held` | substitute or internally break one value, or add a value failing `q` |
| `blocked(ClosedNeedsAdd)` | add a predicate outside the closed set |
| atoms, `irrefutable`, `coinductive`, other `blocked` | none |

| Evidence | SAT alternatives |
| --- | --- |
| `all` / `any` | repair every failed child / any one branch |
| `not` | `VIO` of the holding inner shape |
| `count_low` | add qualifying values, or fix rejected candidates (substitute or `SAT`) |
| `count_high` | cut matches, or break `q` at them |

Atoms have no rule of their own. Changing an atom means changing *which value*
sits on an edge, and only the enclosing count or universal knows that edge and
the qualifier the new value must fail or satisfy. Substitution is therefore a
value-level operation. Candidate terms come from the facets, constants, and
classes under the qualifier, mutations of the current value, other values of
the predicate, typed nodes, and a fresh IRI. The engine checks each one with
`EvidenceSession.evidence_for(term, qualifier)` before use. Candidates of the
current value's kind come first.

When `max` is broken by adding values, the universals registered on the same
`(node, path)` are preferred (`_register`/`_universals`). A new `feeds` value
then satisfies the sibling `sh:class Zone`, and the case stays a cardinality
error. This is the generation-side counterpart of `CountLow.sibling_qualifiers`.

Path edits use `_minimal_word`: stars are taken reflexively and the first
alternative is used. `rdf:type/rdfs:subClassOf*` therefore becomes a single
`rdf:type` edge. Interior nodes of longer words are fresh IRIs.

## Normal form and caps

A leaf is a tuple of `Operation`s. `_merge` rejects a joint choice when one
part deletes what another adds, when two parts demand opposite polarities for
one `(node, constraint)`, or when two parts replace one value differently. An
expression's identity, `key`, is its sorted net delta, and duplicates are
dropped.

Every choice point is capped (`ExpansionLimits`). `_union` interleaves
round-robin, so a capped prefix still samples every branch.

## Determinism

Normalized arena ids are not stable across processes. The same shapes graph
can number its arena differently, and Python string hashing is randomized.
Expansion order must not depend on either:

- Sibling evidence is ordered by `_content_key`, a digest in which each arena
  id is replaced by `_shape_key`, a structural digest of the catalog shape.
  Ignoring ids altogether is not enough. Two counts on one path that differ
  only in their qualifier would tie, and the expansion caps would keep a
  different prefix.
- Every set is sorted before sampling. Per-target RNGs are seeded from
  `(seed, focus, shape name)`.
- Fresh IRIs are hashed from terms, never from ids.

`test_seeded_universe_is_reproducible_across_processes` runs the generator
under three `PYTHONHASHSEED`s. The NIST-IBAL universe was also checked
byte-for-byte across processes.

## Sampling and composition

`sample` takes the `M` smallest `−ln h(v) / w(v)`. `h` is BLAKE2b of the salt
and the key, mapped into `(0, 1]`. `w` is the geometric mean of the predicate
weights edited, times the geometric mean of the kind weights when any are set.
`generate` salts each case with `seed:K:i` over one materialized universe.

`compose` scans the reservoir. A candidate is skipped when its
`(focus, statement, component)` slot is taken, when it conflicts with a chosen
expression, or when it fails `verify`. A conflict is an overlapping footprint
or opposite requirements. `verify` revalidates the expression's own delta.
The component is the top-level conjunct of the statement that the expression
breaks. It is taken from the root `all_held` child the expansion started
from. An expression is *realized* when that component's shape id appears among
the failing children of its target's failure evidence. After filling up to
`K`, the union delta is revalidated. Masked expressions are rejected and the
remainder refilled until nothing is masked. Every recorded violation therefore
comes from an engine run over the final `G_test`.

## Known gaps

- Relational atoms (`Eq`, `Disj`, `Lt`, `Le`, `UniqueLang`), opaque SPARQL,
  and `sh:expression` are never broken. They sit under `blocked` or have no
  generation rule.
- `infer=True` re-runs rules on every revalidation, but edits target asserted
  triples only. Values whose sole support is inferred cannot be cut.
  `materialize=True` avoids this by treating the closure as asserted.
- Edits are not checked against shapes the target does not mention. Extra
  failures are reported as amplification, not prevented.
