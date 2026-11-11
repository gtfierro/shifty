How violation generation works
==============================

.. include:: experimental.inc

``vio`` starts with a conforming graph and records edits that make it fail.
Undoing those edits gives a known repair. The approach follows Lin et al.
[LIN25]_ and its extension to several violations per case [LIN26]_.

From a passing node to an injection
-----------------------------------

A sensor with exactly one label passes because a label exists and no second
label exists. Either reason suggests an edit that breaks the constraint.
Remove the label, or add another.

``vio`` uses validation evidence to find such edits. It can remove a required
value, replace a value with one that fails a test, or exceed a count limit.
Under ``sh:not``, it makes the inner shape pass. For nested shapes, it can
edit the link to a value or a property of that value.

A complete set of edits is an *injection*. The *universe* contains the
injections the search found. Expansion limits bound how many alternatives
it explores, so the universe covers a subset of possible edits. Some
constraints have no generation rule. See the :doc:`reference` for supported
operations and limits.

Filters and sampling
--------------------

``where`` restricts the injections eligible for a case. ``weighted`` changes
their sampling weights, which default to 1. Use filters to rule out edits and
weights to favor some edits over others.

A draw scores injections using the seed and keeps a small sample of
candidates. It can process candidates as they are generated. Listing the
universe caches the expansion, while ``draws`` can fill several samples in
one pass. The :doc:`benchmark guide <how-to>` shows filters, weights, and
suite generation.

Combining and checking edits
----------------------------

Candidates are combined unless their edits conflict. Conflicts include
adding a triple another injection deletes, replacing one value with two
different values, or requiring opposite outcomes for the same constraint.
Compatible injections can share edits or affect the same node.

Each candidate is applied alone and checked against its target. Then the
combined graph is validated. An injection whose intended constraint no
longer fails is dropped and another candidate is tried. SHACL-AF rules run
during these checks by default, so an edit the rules undo cannot be drawn.

Every entry in ``case.failures`` comes from validation of the final graph.
The case also includes failures caused as side effects, with no injection
assigned to them.

Interpreting a case
-------------------

``k`` limits the number of injections. The case may contain fewer if the
filtered universe or candidate sample runs out. Its number of failures can
differ from ``k``. One edit can break several shapes, and several injections
can break the same property.

When two injections affect one property, validation confirms that the
property fails. It does not establish that both injections were necessary.

The inverse delta restores the reference graph. When scoring a repair tool,
check both conformance and agreement with that reference. Several different
graphs may satisfy the same shapes.

Reproducing a case
------------------

Keep the Shifty version, input graphs, expansion limits, filters, weights,
``k``, and seed fixed. The generator uses stable hashing and canonical
blank-node labels by default. Changing the number of worker processes leaves
the generated suite equivalent.

The `design notes
<https://github.com/gtfierro/shifty/blob/main/development_docs/violation-generation.md>`_
cover the rewrite rules, sampling algorithm, and differences from the papers.

.. [LIN25] T.-W. Lin, G. Fierro, H. Li, T. Hong, P. Nuzzo, and
   A. Sangiovanni-Vincentelli. *Systematic Evaluation of Knowledge Graph
   Repair with Large Language Models.* arXiv:2507.22419, 2025.
   https://arxiv.org/abs/2507.22419

.. [LIN26] T.-W. Lin, G. Fierro, H. Li, T. Hong, P. Nuzzo, and
   A. Sangiovanni-Vincentelli. *Selective Retrieval Routing for
   Multi-Violation Knowledge Graph Repair.* arXiv:2611.07342, 2026.
   https://arxiv.org/abs/2611.07342
