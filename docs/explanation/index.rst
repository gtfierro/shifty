Explanation
===========

Design and semantics of Shifty's compiler, evaluator, evidence model, and
experimental repair layer.

.. list-table::
   :widths: 30 70

   * - :doc:`architecture`
     - Shapes are compiled, not interpreted. What the algebra is, what each
       layer of the pipeline does, and why one IR drives validation,
       inference, and the richer result formats alike.
   * - :doc:`validation-interfaces`
     - The native algebraic result model and W3C ``sh:ValidationReport``
       compatibility interface, including their reporting tradeoffs.
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
   * - :doc:`performance`
     - What evidence costs, measured. Includes two optimizations that began as
       hypotheses the measurements contradicted.
   * - :doc:`benchmarks`
     - Validation performance across real building models, tracked per
       release.
   * - :doc:`repair-design`
     - **Experimental.** Repair as the abductive dual of validation, and why
       the library computes the space of fixes but refuses to choose one.

.. toctree::
   :maxdepth: 1
   :hidden:

   architecture
   validation-interfaces
   shapes-and-data
   recursion
   evidence-design
   performance
   benchmarks
   repair-design
