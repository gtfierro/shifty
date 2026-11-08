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
       triples visible during evaluation, including how ``graph_mode`` affects
       target selection and constraints.
   * - :doc:`recursion`
     - How Shifty handles cyclic shape references, why validation and inference
       use different fixed points, and which schemas are rejected.
   * - :doc:`evidence-design`
     - What derivations contain, how canonical evidence selects relevant
       constraints, and where structural explanations are unavailable.
   * - :doc:`performance`
     - Choose an evidence entry point and avoid unnecessary work.
   * - :doc:`repair-design`
     - **Experimental.** Repair as the abductive dual of validation, and why
       drivers supply domain knowledge to choose among candidate edits.

.. toctree::
   :maxdepth: 1
   :hidden:

   architecture
   validation-interfaces
   shapes-and-data
   recursion
   evidence-design
   performance
   repair-design
