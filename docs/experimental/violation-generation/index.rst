Violation generation
====================

.. include:: experimental.inc

A repair tool takes a graph that fails validation and edits it until it
passes. Passing is easy to check, and it says little: deleting the failing
node passes too. To score a repair, you need to know what the graph was
meant to be.

``shifty.experimental.vio`` gets that answer by working backwards. Start
from a graph that conforms, break it with a few recorded edits, and keep
the edits. Undoing them is a repair known to be correct.

.. code-block:: python

   from shifty.experimental import vio

   universe = vio.Universe("model.ttl", "shapes.ttl")
   case = universe.draw(3, seed=0)

   case.graph()   # the broken graph: give this to the repair tool
   case.repair    # the edits that undo the breakage: the answer

A case can hold several violations at once, and they can interact: fixing
one can expose or hide another. Every failure a case lists was confirmed by
validating the broken graph. The same seed always gives the same case.

It works from Python, from the command line
(``python -m shifty.experimental.vio``), and from Rust
(``shifty_experimental::vio``).

.. list-table::
   :widths: 30 70

   * - :doc:`tutorial`
     - Break a small graph, catch it with the validator, and repair it.
   * - :doc:`how-to`
     - Build a benchmark suite from your own model, and score a tool on it.
   * - :doc:`explanation`
     - How the edits are found, picked, and checked.
   * - :doc:`reference`
     - Classes, options, output files, and limitations.

.. toctree::
   :hidden:

   tutorial
   how-to
   explanation
   reference
