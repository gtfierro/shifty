Generating violations
=====================

.. note::

   Violation generation is experimental. Its API and output format may
   change.

This page explains how ``shifty.experimental.vio`` builds test cases for
repair tools, and why it works the way it does. For usage, see
:doc:`../how-to/generate-violations`.

The problem
-----------

A repair tool takes a graph that fails validation and edits it until it
passes. Passing is easy to check and says little. Deleting every failing
node passes too. What matters is whether the tool recovered the graph the
author meant, and a validator cannot know that.

So the generator starts from the answer. It takes a graph that conforms,
the *reference graph*, and breaks it with a recorded set of edits. The
broken graph is the test input. The recorded edits, reversed, are a repair
known to be correct.

The approach is from Lin et al. [LIN25]_, extended to several violations
per case in [LIN26]_. Real graphs rarely have one problem at a time, and
violations interact: fixing one can expose or hide another. A useful
benchmark has to contain such cases.

Finding ways to break a graph
-----------------------------

Take a shape that says every sensor has exactly one label, and a sensor
that has one. Why does the sensor pass? Because the label triple exists,
and because there is no second one. Each reason is a way to break it:
remove the label, or add another.

That is the whole idea. When Shifty validates, it can produce *evidence*:
a tree that records why each node satisfies each shape. The generator walks
that tree and turns each reason into the edits that defeat it:

- A value that had to exist can be removed.
- A value that had to pass a test can be swapped for one that fails it.
- A count that had to stay under a limit can be pushed over it.
- Under ``sh:not`` the direction flips: the inner shape had to fail, so the
  edits make it hold.

Shapes nest, so the walk recurses. To break "has a point that has a
description", remove the point, or break the point's description.

Each complete set of edits is an *injection*: it makes one node fail one
shape. All the injections together are the *universe*. The walk runs over
Shifty's internal shape algebra, a handful of operators that every SHACL
constraint lowers to, so there is one rule per operator, not one per SHACL
component.

The search is bounded. A large graph has an enormous number of ways to
swap one value for another, so each step tries a capped number of
alternatives. The universe is therefore a large sample of the ways to break
the graph, not all of them.

Picking injections
------------------

A case needs a few injections out of tens of thousands, chosen at random
but reproducibly. The generator gives each injection a score, computed from
a hash of the injection and the seed, and keeps the lowest-scoring few
hundred in a *reservoir*. This is the weighted sampling of [LIN26]_
(Efraimidis and Spirakis): a weight divides the score, so a heavier
injection is more likely to land in the reservoir.

Scoring by hash has three useful consequences:

- The sample depends only on the injections and the seed, not on the order
  they are found in. The same seed gives the same case on any machine.
- Injections can be scored as the walk produces them. A draw never stores
  the universe, only its reservoir.
- Several cases can share one walk, each with its own reservoir. A suite
  costs one pass, not one per case.

By default all weights are 1 and every injection is equally likely.
[LIN26]_ weights one class of predicates to test a hypothesis about it.
That choice belongs to the experiment, so weights are supplied by the
caller, per predicate or per operation kind. Filters are separate: a weight
makes an injection unlikely, and only a filter makes it impossible.

Combining injections
--------------------

The generator takes injections from the reservoir in score order and adds
each one to the case unless it conflicts with one already chosen. Two
injections conflict when they cannot both take effect:

- One deletes a triple that the other adds.
- Both replace the same value, with different values.
- One needs a node to fail a constraint that the other needs it to satisfy.

The last rule is the papers' ([LIN26]_ §6.3). The first two are added here.
They matter because an edit that gets cancelled leaves the recorded repair
wrong.

Everything else is allowed. Two injections may share an edit, break the
same property, or break the same node. Overlap is not a defect: tangled
failures are what a multi-violation benchmark is for.

Checking the result
-------------------

The papers stop at compatibility. Shifty adds two checks, because the
search is approximate and compatible injections can still interfere.

**Each injection is tested alone.** Before an injection joins a case, its
edits are applied to the reference graph and its node is re-evaluated. It
must fail the property the injection aimed at. An injection can miss: the
search caps its alternatives, follows one route along a property path, and
does not see values that a rule would infer back.

**The finished case is validated.** One injection can undo another's
effect, for example by adding a value that restores support the other
removed. So the combined edits are applied and the whole graph is
validated. An injection whose property no longer fails is dropped, and the
next one from the reservoir takes its place.

After the second check, every failure listed in a case was reported by the
validator on the broken graph. Nothing in a case is a prediction.

The check has a limit. When two injections break the same property, it
confirms that the property fails, not that both injections are needed. One
may be redundant. Detecting that would take one more validation per
injection, and the generator does not do it.

What the numbers mean
---------------------

The budget ``k`` counts injections, and it is an upper bound. A small
universe, or a tight filter, can yield fewer.

Failures are counted separately and need not equal ``k``. One edit can
break several shapes, so a case can have more failures than injections.
Several injections can break one property, so it can have fewer. A failure
that no injection aimed at is a side effect, and is listed with no
injections.

A node that already failed in the reference graph is never broken, and
never listed as a failure.

Reproducing a case
------------------

A benchmark must be regenerable, so a draw is a pure function of the data,
the shapes, the limits, and ``(k, seed)``. Three things could break that,
and each is handled:

- Hash functions that are seeded per process. The generator uses fixed
  ones.
- Internal shape ids, which vary between processes. Ordering uses a digest
  of each shape's structure.
- Blank-node names, which a parser picks afresh on every parse. The
  reference graph's blank nodes are renamed canonically.

For the same reason, composition waits until the walk has finished.
[LIN26]_ lets it start earlier. Waiting means a case depends on the whole
universe, not on how far the walk had got.

Differences from the papers
---------------------------

- Injections come from Shifty's evidence over the shape algebra, not from
  a rule per SHACL component.
- Each injection is tested alone, and each case is validated.
- Edits that cancel are conflicts, in addition to the papers' rule.
- A case holds up to ``k`` injections. [LIN26]_ says exactly ``K`` in §6.1
  and "up to ``K``" in §6.3; Shifty follows §6.3.
- Weights default to 1 and are given by the caller. Operation kinds can be
  weighted as well as predicates.
- Filters exist alongside weights.
- Composition starts after the walk ends.
- With ``materialize``, the reference graph is the data plus everything
  its SHACL-AF rules infer, so edits may touch inferred triples.

The rewrite rules, limits, and implementation are in the `design notes
<https://github.com/gtfierro/shifty/blob/main/development_docs/violation-generation.md>`_.

.. [LIN25] T.-W. Lin, G. Fierro, H. Li, T. Hong, P. Nuzzo, and
   A. Sangiovanni-Vincentelli. *Systematic Evaluation of Knowledge Graph
   Repair with Large Language Models.* arXiv:2507.22419, 2025.
   https://arxiv.org/abs/2507.22419

.. [LIN26] T.-W. Lin, G. Fierro, H. Li, T. Hong, P. Nuzzo, and
   A. Sangiovanni-Vincentelli. *Selective Retrieval Routing for
   Multi-Violation Knowledge Graph Repair.* arXiv:2611.07342, 2026.
   https://arxiv.org/abs/2611.07342
