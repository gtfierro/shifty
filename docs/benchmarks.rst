Benchmarks
==========

These results track Shifty's CLI on 45 Brick and 19 ASHRAE 223P building
models. Lower times mean a faster complete run. Hover over a bar for its
model count and recorded revision.

Release history
---------------

Each bar averages the per-model medians. Inference and inference-plus-validation
are timed as separate commands. Both include loading the inputs, preparing the
shapes, and formatting output.

.. raw:: html

   <script src="https://cdn.plot.ly/plotly-2.32.0.min.js" charset="utf-8"></script>
   <div id="shifty-bench-chart" style="width:100%;height:640px;"></div>

Per-model results
-----------------

Choose two recorded versions to inspect individual models. Negative changes
mean faster runs. The summary compares the geometric mean of ratios for models
present in both recordings.

.. raw:: html

   <div class="bench-controls">
     <label>Dataset <select id="bench-dataset"></select></label>
     <label>Version <select id="bench-version"></select></label>
     <label>Compare with <select id="bench-baseline"></select></label>
   </div>
   <div id="bench-tables"></div>

Methodology
-----------

- Each model is measured three times by default, in a fresh process each time.
  The recorded value is the median. The spread is the largest distance from
  that median to a sample.
- Shapes use the checked-in ontology closures. Every version runs against the
  same model and shapes files from the current checkout. Build time is excluded.
- ``infer`` measures rule inference. ``validate`` runs inference and validation
  together. Their difference can include noise, so the chart shows both
  measured totals independently.
- New recordings include the revision, input and binary hashes, machine,
  toolchain, sample count, and raw timings. Earlier text-only recordings have
  their release revisions resolved from Git tags. ``HEAD`` includes tracked
  working-tree changes.
- Validation uses best-effort unsupported-feature handling to match older releases.
  A nonconforming validation counts as a completed run. An execution error
  stops collection.

Reading the results
-------------------

Compare recordings made on the same machine with the same fixtures and
settings. Changes to validation semantics can also change the amount of work.

These are CLI costs, including setup on each call. Applications can reuse a
``PreparedValidator`` across data graphs. See :ref:`evidence-cost` for choosing
how much evidence to build.

.. _run_history:

Record and reproduce
--------------------

From the repository root, measure the current checkout or named releases:

.. code-block:: bash

   python3 benchmark/run.py
   python3 benchmark/run.py v0.6.0-alpha.4
   python3 benchmark/run.py --history --list

Tagged recordings are reused when complete. ``--force`` replaces them.
``--suite brick`` selects one corpus, and ``--iterations 5`` takes more samples.
``--history`` measures all release tags from v0.1.1 onward, including alphas.
It can take hours.

A normal documentation build regenerates the chart from recorded results:

.. code-block:: bash

   cd docs
   make html

The `benchmark README
<https://github.com/gtfierro/shifty/blob/main/benchmark/README.md>`_
describes the scripts, result files, and separate evidence measurements.
