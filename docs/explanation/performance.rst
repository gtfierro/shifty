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

On measured Brick models, ``find_failures()`` followed by ``explain()`` for
every failure cost 3–34% more than conformance alone. Materializing evidence
for *every* selected pair cost 2.5–5.4 times as much. Those ratios reflect the
measured corpus and its share of failing pairs; use them to choose an interface,
not to predict a new dataset's runtime.

What adds cost
--------------

The evidence pass retains paths, values, and nested judgments that conformance
can discard. Terms and path-support certificates recur in serialized runs.
``to_compact_json()`` stores repeated terms and subtrees once; it reduced the
measured serialized size by 76% on Brick and 50% on ASHRAE 223P. It does not
reduce the work needed to construct the evidence.

Reuse a prepared schema across data snapshots to avoid paying compilation
cost for every graph. For large runs, find failing pairs first and explain
only those the application will show. See :doc:`../how-to/explain-failures` for
the calls and :doc:`../reference/evidence` for their return values.

Measurements and reproduction
-----------------------------

The :doc:`evidence-performance-study` records the models, timings, memory and
serialization measurements, rejected optimization hypotheses, and commands
used to reproduce them. :doc:`../benchmarks` measures the separate end-to-end
validation pipeline, including fixed setup cost.
