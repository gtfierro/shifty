Violation generation
====================

.. warning::

   **Experimental.** ``shifty.experimental.vio`` may change its API and
   output format in any release, without a deprecation period. Pin the
   Shifty version if you depend on it.

Violation generation builds test cases for repair tools. It takes a graph
that conforms to its shapes, breaks it with a few recorded edits, and keeps
the inverse edits as the known-correct repair. A validator can say that a
repaired graph conforms, but not that the repair was the right one; these
cases can.

It is available from Python as ``shifty.experimental.vio`` and
``python -m shifty.experimental.vio``, and from Rust as
``shifty_experimental::vio``.

.. list-table::
   :widths: 30 70

   * - :doc:`how-to`
     - Break a graph, choose what gets broken, write a suite to disk, and
       use it from Rust.
   * - :doc:`explanation`
     - How the edits are found and combined, and why the generator works
       backwards from a conforming graph.

.. toctree::
   :hidden:

   how-to
   explanation
