Generate repair benchmarks
==========================

.. warning::

   **Experimental.** ``shifty.multivio`` is new and its output format may
   change. It generates test cases. It does not repair graphs.

``shifty.multivio`` injects SHACL violations with known ground-truth repairs
into a reference graph that conforms to its shapes. Each test case contains an
invalid graph, the edits that were injected, and the inverse edits that
restore the reference graph. A case can hold several interacting violations.
Repair systems can then be scored on whether they recover the intended graph,
not just on whether their output validates.

The method follows Lin et al., `Systematic Evaluation of Knowledge Graph
Repair with Large Language Models <https://arxiv.org/abs/2507.22419>`_, and
its multi-violation extension in `Selective Retrieval Routing for
Multi-Violation Knowledge Graph Repair <https://arxiv.org/abs/2611.07342>`_.
Shifty's satisfaction evidence supplies the expansion. See the
`design notes <https://github.com/gtfierro/shifty/blob/main/development_docs/violation-generation.md>`_
for the rewrite rules.

Generate a suite from the command line
--------------------------------------

.. code-block:: bash

   python -m shifty.multivio --shapes shapes.ttl --data reference.ttl \
       --out suite/ -k 2 -k 5 -k 10 -n 10 --seed 0

This writes ten cases for each budget ``K``:

.. code-block:: text

   suite/
     reference.nt          the reference graph, normalized to N-Triples
     suite.json            universe size, operation mix, case list
     k05/case-003/
       invalid.nt          G_test = G_ref ⊕ Δ
       repair.ru           Δ⁻¹ as a SPARQL 1.1 Update
       case.json           Δ, Δ⁻¹, each injected expression, and every
                           failing (focus, statement) pair in G_test

Pass ``--materialize`` when the model relies on SHACL-AF rules, as ASHRAE 223P
models do. The rules then run once, and the closure becomes the reference
graph. ``--weights`` and ``--kind-weights`` take JSON files of sampling
weights, described below.

Generate cases from Python
--------------------------

.. code-block:: python

   from shifty.multivio import ViolationGenerator

   gen = ViolationGenerator(shapes, reference, seed=7)
   print(len(gen.expressions()), "candidate violations")

   for case in gen.generate(budget=5, cases=10):
       case.save(f"suite/case-{case.index:03d}")
       for expr in case.expressions:
           print(expr.shape, expr.focus, [op.detail for op in expr.operations])

``gen.expressions()`` traverses the reference graph once. Each
``ViolationExpression`` is one way to make a single
``(focus, statement)`` pair fail. Its ``operations`` are atomic edits:

``remove``
   Cut an edge that supports a value, such as the only ``ex:hasPoint`` that
   satisfies a ``sh:qualifiedMinCount``.
``substitute``
   Replace a value with one that fails, or satisfies, its qualifier, such as
   ``"RAT_01"`` → ``"RA"`` under ``sh:minLength 3``. Proposed terms are
   checked by the engine before use.
``add_value``
   Add a value along a path, such as a second ``qudt:hasQuantityKind`` under
   ``sh:maxCount 1``. New values follow the other universals on the same path
   when they can, so a cardinality violation stays one.
``add_forbidden``
   Add a predicate that a ``sh:closed`` shape disallows.

Under ``sh:not``, the generator makes the negated shape *hold*. For example,
``sh:not [ sh:path ex:deprecated ; sh:hasValue true ]`` is broken by adding
``ex:deprecated true``.

``generate`` draws every case from that one traversal. Each case scans a
weighted sample of the expressions. It admits an expression only if the
expression's own edits make its target fail, and only if it does not overlap
or contradict the expressions already chosen. It then revalidates the
combined edits and replaces any expression another one masked. Every
violation recorded in ``case.violations`` is one the engine reports for
``invalid.nt``. Because one edit can fail several statements, there can be
more failing pairs than the budget.

Steer the mix
-------------

Sampling uses weighted reservoir keys ``-ln h(v) / w(v)``. ``h`` is a stable
hash of the expression and a per-case salt, and ``w`` is the geometric mean of
the weights of the predicates the expression edits:

.. code-block:: python

   S223 = "http://data.ashrae.org/standard223#"
   gen.reweight({S223 + "observes": 50, S223 + "hasPhysicalLocation": 50})
   evidential = gen.generate(budget=10, cases=10)

``reweight`` keeps the traversal, so several suites can share one universe.
``inverse_frequency_weights(gen.expressions())`` spreads cases across
predicates instead of following their natural frequency. ``kind_weights``
weights operation kinds. On 223P models, ``add_value`` dominates because
nearly every property carries both ``sh:class`` and ``sh:maxCount``.

``python/examples/multivio_223p.py`` builds the three predicate-mixture suites
from the paper for a 223P model.

Bound the expansion
-------------------

``ExpansionLimits`` caps each choice point: values per
count, candidate terms per value, joint choices, nesting depth, and
expressions per target. The defaults produce about 77,000 expressions for
NIST-IBAL (9,625 triples after materialization) in roughly 15 seconds.

Reproducibility
---------------

For a fixed shapes graph, reference graph, seed, and limits, the universe and
the cases are identical across runs and processes. Expressions are identified
by their net edit set, not by engine-internal constraint ids. Those ids can
differ between processes.

Limitations
-----------

- Relational constraints (``sh:equals``, ``sh:disjoint``, ``sh:lessThan``),
  SPARQL constraints, and ``sh:xone`` are not broken yet.
- New values come from the reference graph, from facet-derived literals, or
  from fresh IRIs named after the focus node's namespace. The generator does
  not invent realistic entity names.
- Paths are materialized along their shortest predicate word. Alternatives
  take the first branch, and stars take zero steps.
- Focus nodes without outgoing triples in the reference graph, such as
  vocabulary terms described by the shapes graph, are skipped. Pass
  ``include_external_foci=True`` to include them.
