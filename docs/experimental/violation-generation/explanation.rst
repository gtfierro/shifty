How violation generation works
==============================

.. include:: experimental.inc

``vio`` breaks a conforming graph with recorded edits, so that undoing the
edits is a repair known to be correct. The approach is from Lin et al.
[LIN25]_. [LIN26]_ extends it to several violations per case, because real
graphs rarely have one problem at a time.

Building a case takes four steps: find ways to break the graph, pick a few
at random, combine them, and check the result.

Finding ways to break a graph
-----------------------------

Take a shape that says a sensor has exactly one label, and a sensor with
one label. It passes for two reasons: the label exists, and there is no
second one. Each reason is a way to break it. Remove the label, or add
another.

Shifty can record why each node passes each shape. This record is called
*evidence*. ``vio`` walks the evidence and turns each reason into the edits
that defeat it:

- A value that had to exist is removed.
- A value that had to pass a test is swapped for one that fails.
- A count that had to stay under a limit is pushed over it.
- Under ``sh:not``, the edits make the inner shape hold instead.

Shapes nest, so the walk recurses. To break "has a point with a
description", remove the point, or break its description.

One complete set of edits is an *injection*. It makes one node fail one
shape. All the injections together are the *universe*.

The walk works on Shifty's shape algebra, which every SHACL constraint
compiles to. A handful of operators covers all of SHACL, so ``vio`` needs
one rule per operator rather than one per constraint type.

There are often far too many ways to swap a value, so the walk tries only a
few at each step (the :ref:`expansion limits <vio-limits>`). The universe
is a large sample of the ways to break the graph, not every one.

Picking injections
------------------

A case needs a few injections out of tens of thousands. They must be
random, and the same seed must give the same pick.

``vio`` scores each injection by hashing it with the seed. It keeps the
lowest scores in a small *reservoir*. A weight divides the score, so heavier
injections are more likely to land there. This is the weighted sampling
used in [LIN26]_.

Hashing has three benefits:

- The pick doesn't depend on the order injections are found in, so any
  machine picks the same ones.
- Injections are scored as they are found. A draw never stores the whole
  universe.
- Many cases can share one walk, each with its own reservoir. A suite costs
  one walk.

All weights default to 1. Choosing weights is part of designing an
experiment, so they come from the caller. Filters are separate: a weight
makes an injection unlikely, and only a filter rules it out.

Combining injections
--------------------

``vio`` takes injections from the reservoir in score order. It adds each to
the case unless it conflicts with one already chosen. Two injections
conflict when they can't both take effect:

- One deletes a triple the other adds.
- Both replace the same value, with different values.
- One needs a node to fail a constraint that the other needs it to pass.

The last rule is from [LIN26]_ (§6.3). ``vio`` adds the first two because
an edit that gets cancelled out would leave the recorded repair wrong.

Anything else is allowed. Two injections can share an edit, or break the
same node. Tangled failures are what a multi-violation benchmark is for.

Checking the result
-------------------

The papers stop once injections are compatible. ``vio`` adds two checks,
because its search is approximate and compatible injections can still
interfere.

**Each injection is tested alone.** Its edits are applied to the original
graph, and its node is validated again. It has to fail the property it
aimed at. Rules run during this check, so an injection that a rule would
undo fails here.

**Each finished case is validated.** One injection can undo another, for
example by adding back a value the other removed. So the combined edits are
applied and the whole graph is validated. An injection that no longer
causes a failure is dropped, and the next one in the reservoir takes its
place.

After these checks, every failure a case lists was reported by the
validator. None of them are predictions.

One gap remains. When two injections break the same property, the check
confirms that the property fails. It doesn't confirm that both injections
are needed.

What the numbers mean
---------------------

``k`` is the most injections a case holds. A small universe or a tight
filter can give fewer.

The number of failures can differ from ``k``:

- One edit can break several shapes, giving more failures.
- Several injections can break the same property, giving fewer.

A failure that no injection aimed at is a side effect. It is listed with no
injections.

Reproducing a case
------------------

A benchmark has to be rebuildable. A draw depends only on the data, the
shapes, the limits, ``k``, and the seed. Three things could break that, and
each is handled:

- **Hash functions** that change per process. ``vio`` uses fixed ones.
- **Internal shape ids**, which change between runs. ``vio`` orders shapes
  by a digest of their structure.
- **Blank-node names**, which a parser picks afresh each time. ``vio``
  renames them the same way every time.

Combining waits until the walk is done, so a case depends on the whole
universe and not on how far the walk got. [LIN26]_ lets combining start
earlier.

Differences from the papers
---------------------------

- Injections come from evidence over the shape algebra, not from one rule
  per SHACL constraint type.
- Each injection is tested alone, and each case is validated.
- Edits that cancel out count as conflicts.
- A case holds *up to* ``k`` injections. [LIN26]_ says "exactly" in §6.1
  and "up to" in §6.3. ``vio`` follows §6.3.
- Weights default to 1 and are chosen by the caller, per predicate or per
  operation kind.
- Filters exist alongside weights.
- Combining starts after the walk ends.
- Rules run on every check, so no case relies on an edit the rules would
  undo.

The `design notes
<https://github.com/gtfierro/shifty/blob/main/development_docs/violation-generation.md>`_
cover the rewrite rules and the implementation.

.. [LIN25] T.-W. Lin, G. Fierro, H. Li, T. Hong, P. Nuzzo, and
   A. Sangiovanni-Vincentelli. *Systematic Evaluation of Knowledge Graph
   Repair with Large Language Models.* arXiv:2507.22419, 2025.
   https://arxiv.org/abs/2507.22419

.. [LIN26] T.-W. Lin, G. Fierro, H. Li, T. Hong, P. Nuzzo, and
   A. Sangiovanni-Vincentelli. *Selective Retrieval Routing for
   Multi-Violation Knowledge Graph Repair.* arXiv:2611.07342, 2026.
   https://arxiv.org/abs/2611.07342
