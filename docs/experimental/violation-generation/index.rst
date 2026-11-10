Violation generation
====================

.. include:: experimental.inc

Violation generation builds test cases for repair tools. A validator can
say that a repaired graph conforms, but not that the repair was the right
one: deleting the failing node conforms too. To score a repair you need the
intended answer.

``shifty.experimental.vio`` gets one by working backwards. It takes a graph
that conforms, breaks it with a few recorded edits, and keeps the inverse of
those edits as a repair known to be correct:

.. code-block:: python

   from shifty.experimental import vio

   universe = vio.Universe("model.ttl", "shapes.ttl")  # every way to break it
   case = universe.draw(3, seed=0)                     # up to 3 of them, together

   case.graph()    # the broken graph: the test input
   case.repair     # the edits that undo the breakage: the answer

A case can hold several violations that interact, so fixing one can expose
or hide another, as in real graphs. Every failure a case lists was reported
by the validator on the broken graph, and the same ``(k, seed)`` gives the
same case on every machine.

There are three ways to use it:

- **From Python**, with ``shifty.experimental.vio``: build a ``Universe``,
  narrow and weight it, and draw cases or write a whole suite.
- **From the command line**, with ``python -m shifty.experimental.vio``,
  which writes a suite to disk.
- **From Rust**, with ``shifty_experimental::vio`` in the
  ``shifty-experimental`` crate. The Python module wraps it, and the names
  match. The ``shifty-vio`` crate behind it is its implementation, not an
  interface.

.. list-table::
   :widths: 30 70

   * - :doc:`tutorial`
     - Break a small graph, watch the validator catch it, and repair it with
       the edits that broke it.
   * - :doc:`how-to`
     - Build a benchmark: choose what gets broken, write a suite, handle
       models that need rules, and score a repair tool.
   * - :doc:`explanation`
     - How injections are found, sampled, combined, and checked, and how
       this differs from the papers it follows.
   * - :doc:`reference`
     - The Python classes, the output files, the command-line options, the
       Rust names, and the limitations.

``python/examples/`` in the repository has three scripts: ``vio_validate.py``
(the tutorial's script), ``vio_suite.py`` (narrow, weight, and draw a
suite), and ``vio_223p.py`` (a suite for an ASHRAE 223P model).

.. toctree::
   :hidden:

   tutorial
   how-to
   explanation
   reference
