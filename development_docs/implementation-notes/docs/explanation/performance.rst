Evidence performance
====================

Evidence records a derivation for each selected ``(statement, focus)`` pair.
That work costs more than deciding conformance alone, especially when many
nodes pass and only a few failures need explanation.

Choose the smallest result you need
-----------------------------------

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Need
     - Entry point on a prepared evidence session
   * - Only conformance counts
     - ``validate_conformance()``
   * - Which selected pairs failed
     - ``find_failures()``
   * - Derivation for one selected pair
     - ``explain(pair)``
   * - Passing and failing derivations for every pair
     - ``validate()``

Use ``find_failures()`` followed by ``explain()`` when you need failure
explanations. This avoids building evidence for passing pairs. The savings
depend on how many pairs fail and the complexity of their derivations.

What adds cost
--------------

The evidence pass retains paths, values, and nested judgments that conformance
can discard. Terms and path-support certificates recur in serialized runs.
``to_compact_json()`` stores repeated terms and subtrees once. The size reduction
depends on how much repetition the run contains. It does not reduce the work
needed to construct the evidence.

Reuse a prepared schema across data snapshots to avoid paying compilation
cost for every graph. For large runs, find failing pairs first and explain
only those the application will show. See :doc:`../how-to/explain-failures` for
the calls and :doc:`../reference/evidence` for their return values.

Validation benchmarks
---------------------

:doc:`../benchmarks` measures the end-to-end validation pipeline, including
fixed setup cost.
