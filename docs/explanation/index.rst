Explanation
===========

How to interpret validation results, graph visibility, recursive shapes,
evidence, and experimental repair candidates.

.. list-table::
   :widths: 30 70

   * - :doc:`architecture`
     - Choose structured application findings or an RDF validation report.
   * - :doc:`shapes-and-data`
     - The distinction between the shapes graph and the data graph, and the
       triples visible during evaluation, including how ``graph_mode`` affects
       target selection and constraints.
   * - :doc:`recursion`
     - How Shifty handles cyclic shape references, why validation and inference
       use different fixed points, and which schemas are rejected.
   * - :doc:`evidence-design`
     - Read passing and failing evaluations and understand their limits.
   * - :doc:`performance`
     - Choose an evidence entry point and avoid unnecessary work.
   * - :doc:`../experimental/repair/explanation`
     - **Experimental.** Fill repair templates and validate candidate edits.

.. toctree::
   :maxdepth: 1
   :hidden:

   architecture
   validation-interfaces
   shapes-and-data
   recursion
   evidence-design
   performance
