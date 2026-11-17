Benchmarks
==========

How long the Shifty CLI takes on real building models: 45 Brick and 19
ASHRAE 223P. Lower is faster.

Release history
---------------

Each bar is the average of the per-model medians. ``infer`` and ``validate``
are timed separately. Both include loading inputs, preparing shapes, and
writing output. Hover over a bar for details.

.. raw:: html

   <script src="https://cdn.plot.ly/plotly-2.32.0.min.js" charset="utf-8"></script>
   <div id="shifty-bench-chart" style="width:100%;height:640px;"></div>

Per-model results
-----------------

Pick two versions to compare model by model. Negative changes are faster.

.. raw:: html

   <div class="bench-controls">
     <label>Dataset <select id="bench-dataset"></select></label>
     <label>Version <select id="bench-version"></select></label>
     <label>Compare with <select id="bench-baseline"></select></label>
   </div>
   <div id="bench-tables"></div>

How it's measured
-----------------

- Each model runs three times, each in a fresh process. The median is
  recorded.
- Every version runs on the same models and shapes from the current checkout.
  Build time is not counted.
- ``validate`` includes inference, so it is always at least as slow as
  ``infer``.
- A model that fails validation still counts as a completed run. A crash
  stops the benchmark.

Only compare results from the same machine and settings.

These numbers include setup on every call. An application that reuses a
``PreparedValidator`` pays that cost once. See :ref:`evidence-cost` for
how evidence settings affect cost.

.. _run_history:

Run the benchmarks
------------------

From the repository root:

.. code-block:: bash

   python3 benchmark/run.py                   # current checkout
   python3 benchmark/run.py v0.6.0-alpha.4    # a release
   python3 benchmark/run.py --history --list  # preview every release

Useful options:

- ``--suite brick`` or ``--suite s223`` runs one corpus.
- ``--iterations 5`` takes more samples.
- ``--force`` re-measures a release that is already recorded.
- ``--history`` measures every release since v0.1.1. This can take hours.

``make html`` in ``docs/`` redraws the chart from the recorded results. It
does not run any benchmarks.

The `benchmark README
<https://github.com/gtfierro/shifty/blob/main/benchmark/README.md>`_ covers
the result files and other measurements.
