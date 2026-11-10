Generating violations
=====================

.. note::

   Violation generation is experimental. Its API and output format may
   change.

``shifty.experimental.vio`` builds repair benchmarks: graphs with known
violations and known repairs. It implements the violation-injection
framework of Lin et al. [LIN25]_, extended to multiple violations per case as
in [LIN26]_. This page defines the terms, states the framework in the papers'
terms, and then lists what Shifty does beyond it and why. For usage, see
:doc:`../how-to/generate-violations`.

Terms
-----

Reference graph ``G_ref``
   A data graph that conforms to the shapes.
Pair
   A focus node together with a statement that selects it, where a
   statement is a shape with its targets. A pair passes or fails.
Property
   One top-level conjunct of a statement's shape, usually one property
   shape. Validation reports each failing property as its own result.
Delta ``Δ``
   A set of triple additions and deletions. ``G_test = G_ref ⊕ Δ`` is the
   graph after applying it, and its inverse ``Δ⁻¹`` restores ``G_ref``.
Injection
   A set of edits that makes one passing pair fail, by breaking one of its
   properties. The papers call it a *normalized violation expression*. Its
   net delta identifies it.
Case
   A ``G_test`` built from several injections, together with ``Δ⁻¹`` as the
   ground-truth repair.
Failure
   A pair that fails in ``G_test``, together with the injections that break
   it.

The framework
-------------

The papers build a case in three steps.

1. **Expansion.** ``VIO(x, φ)`` rewrites the task "make focus node ``x``
   violate shape ``φ``" into alternatives, down to edit operations. The
   leaves of this expansion graph are injections. The universe ``U`` is the
   set of distinct leaves ([LIN25]_; [LIN26]_ §6.1).
2. **Sampling.** Each injection ``v`` gets a weight ``w(v)``, the geometric
   mean of the weights of the predicates it edits. A reservoir keeps the
   ``M`` injections with the smallest keys ``−ln h(v) / w(v)``, where ``h``
   is a stable hash into ``(0, 1]`` (Efraimidis–Spirakis weighted sampling;
   [LIN26]_ §6.2).
3. **Composition.** Injections are taken from the reservoir in key order.
   Each one states requirements: which nodes must violate or satisfy which
   constraints. An injection is admitted if its requirements are compatible
   with those of the injections already chosen. Two requirements conflict
   when they demand incompatible violations on one node. Selection stops at
   the budget ``K`` or when the reservoir runs out ([LIN26]_ §6.3).

``K`` counts injections, not failures. One injection may cause several
validation results ([LIN26]_ §6.1).

In Shifty, ``U`` is a ``Universe``, an injection is an ``Injection``, and a
case is a ``Case``. ``Universe.draw(k, seed)`` runs sampling and composition
with ``K = k``.

What Shifty adds, and why
-------------------------

**Each injection must break its target alone.** Before admitting an
injection, ``draw`` applies its delta alone to ``G_ref`` and revalidates.
The injection's pair must then fail on the property it targets. *Why:* the
expansion is approximate, so a leaf can fail to violate anything. It caps
every choice point, follows each path along one shortest sequence of
predicates, and does not see support that rules re-infer.

**The composed case is revalidated.** After composition, ``draw`` applies
the union of the chosen deltas and revalidates. An injection whose targeted
property no longer fails is replaced with the next one from the reservoir.
*Why:* compatible requirements do not guarantee a joint effect, since one
injection can restore support that another removed. After this step, every
failure in a case comes from an engine run on ``G_test``.

**Revalidation checks properties, not injections.** When several injections
break one property, the check confirms that the property fails. It does not
confirm that each injection matters, and one may be redundant. A
leave-one-out check would find redundant injections. It would cost about
``k`` extra validations per draw, so Shifty does not run one.

**Edits must not contradict.** The papers' requirement rule is kept. In
addition, ``draw`` rejects an injection that deletes a triple a chosen one
adds, adds a triple a chosen one deletes, or replaces a value that a chosen
one replaces differently. *Why:* requirements do not describe edits, so
they cannot catch edits that cancel, and a cancelled edit makes ``Δ⁻¹``
wrong. Injections may share an edit.

**Nothing limits overlap between failures.** As in the papers, several
injections may break one property, and an injection may cause failures at
pairs it does not target. A failure that no injection targets is reported
with no injections.

**A case holds up to** ``k`` **injections.** [LIN26]_ §6.1 states
``|S| = K``, while §6.3 says "up to ``K``". Shifty follows §6.3, so a small
universe can yield fewer than ``k`` injections. A case can have more
failures than ``k``, because one edit can fail several pairs. It can also
have fewer, because several injections can share one failure.

**Weights are 1 unless given at call time.** By default every injection has
``w(v) = 1``, so each is equally likely to enter the reservoir.
``universe.weighted(predicates={iri: weight})`` returns a view whose draws
use the papers' ``w(v)``: the geometric mean of the given weights over the
predicates the injection edits. A predicate without an entry weighs 1, so
only ratios matter. *Why:* [LIN26]_ up-weights a class of predicates to test
a hypothesis about that class. That weighting belongs to the experiment, not
to the generator.

**Operation kinds can be weighted too.** ``weighted(kinds={kind: weight})``
multiplies ``w(v)`` by the geometric mean of the given weights over the
kinds of the injection's operations: ``remove``, ``substitute``,
``add_value``, or ``add_forbidden``. *Why:* on ASHRAE 223P models,
``add_value`` makes up most of the universe, because nearly every property
has both ``sh:class`` and ``sh:maxCount``.

**Filters are separate from weights.** ``universe.where(shape=...,
predicate=..., focus=...)`` returns a view of the matching injections, and
it also accepts a callable. Draws from the view sample only those
injections. *Why:* a weight makes an injection less likely, but never
impossible.

**The expansion streams into the reservoir.** As in [LIN26]_, ``draw``
offers each leaf to the reservoir as the expansion produces it, after the
filter. Only the reservoir is stored. ``draws([(k, seed), ...])`` fills one
reservoir per case from a single pass. *Why:* memory stays bounded by the
reservoirs, not the universe, and a suite costs one pass rather than one
per case.

**Composition waits for the pass to finish.** [LIN26]_ lets composition
start before the traversal finishes. Shifty completes the pass first, bounded
by ``ExpansionLimits``. *Why:* a case then depends on the whole universe and
not on how far the traversal got, so it reproduces.

**Listing caches the expansion.** ``len``, iteration, and indexing expand the
universe once and keep it, and later draws read the cache. A streamed draw
and a cached draw return the same case, because the reservoir keeps the
smallest keys whatever order injections arrive in. Views from ``where`` and
``weighted`` share the cache and the verification results.

**Draws reproduce across processes.** The same inputs and ``(k, seed)`` give
the same case on every run. *Why:* a benchmark must be regenerable. The
hashes are fixed functions, not Python's or Rust's randomly seeded ones.
Sibling evidence is ordered by a digest of shape structure, because the
engine's internal shape ids vary between processes. Anonymous blank nodes
are relabeled canonically, because parsers label them afresh on every parse.

Other differences
-----------------

- The expansion is a fold over Shifty's satisfaction evidence on the
  normalized shape algebra (``∧ ∨ ¬``, counts, universals, tests, closed
  shapes), not a rule per SHACL component.
- Under ``¬``, ``VIO`` switches to its dual, ``SAT(x, φ)``, which makes a
  failing shape hold.
- The engine checks each proposed replacement value against its qualifier
  before use.
- Pairs that already fail in ``G_ref`` are never broken and never counted as
  failures.
- With ``materialize``, the SHACL-AF closure of the data is ``G_ref``, so
  edits may touch inferred triples.

The rewrite rules, caps, and determinism measures are in the `design notes
<https://github.com/gtfierro/shifty/blob/main/development_docs/violation-generation.md>`_.

.. [LIN25] T.-W. Lin, G. Fierro, H. Li, T. Hong, P. Nuzzo, and
   A. Sangiovanni-Vincentelli. *Systematic Evaluation of Knowledge Graph
   Repair with Large Language Models.* arXiv:2507.22419, 2025.
   https://arxiv.org/abs/2507.22419

.. [LIN26] T.-W. Lin, G. Fierro, H. Li, T. Hong, P. Nuzzo, and
   A. Sangiovanni-Vincentelli. *Selective Retrieval Routing for
   Multi-Violation Knowledge Graph Repair.* arXiv:2611.07342, 2026.
   https://arxiv.org/abs/2611.07342
