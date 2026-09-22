Explanation
===========

Design and semantics of Shifty's compiler, evaluator, evidence model, and
experimental repair layer.

.. list-table::
   :widths: 30 70

   * - :doc:`architecture`
     - How shapes are compiled, how rules feed inference, and how algebraic
       findings differ from the W3C report path.
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
     - Choose an evidence entry point using measured runtime and size costs.
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
   evidence-performance-study
   repair-design
