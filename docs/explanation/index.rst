Explanation
===========

Why Shifty works the way it does. These pages are for reading rather than
following: they discuss design decisions, the reasoning behind them, and the
trade-offs each one bought — including the ones that turned out badly.

.. list-table::
   :widths: 30 70

   * - :doc:`architecture`
     - Shapes are compiled, not interpreted. What the algebra is, what each
       layer of the pipeline does, and why one IR drives validation,
       inference, evidence, and repair alike.
   * - :doc:`shapes-and-data`
     - The distinction between the shapes graph and the data graph, and the
       separate question of which triples are visible during evaluation. The
       most common source of a validation that passes for the wrong reason.
   * - :doc:`recursion`
     - Cyclic shape references have no answer in the SHACL spec. What Shifty
       chose, why validation and inference use opposite fixed points, and why
       some schemas are refused.
   * - :doc:`evidence-design`
     - Why the validator keeps its derivation, what "canonical" evidence means
       and why it deliberately omits things, and what it honestly cannot
       explain.
   * - :doc:`repair-design`
     - Repair as the abductive dual of validation, and why the library
       computes the space of fixes but refuses to choose one.
   * - :doc:`performance`
     - What evidence costs, measured. Includes two optimizations that began as
       hypotheses the measurements contradicted.
   * - :doc:`benchmarks`
     - Validation performance across real building models, tracked per
       release.

.. toctree::
   :maxdepth: 1
   :hidden:

   architecture
   shapes-and-data
   recursion
   evidence-design
   repair-design
   performance
   benchmarks
