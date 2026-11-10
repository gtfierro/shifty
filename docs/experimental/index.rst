Experimental
============

.. admonition:: Experimental: this can change at any time
   :class: experimental-banner

   Everything here can change in any release, without a deprecation period:
   the languages, the commands, the ``shifty.experimental`` Python modules,
   the ``shifty-experimental`` Rust crate, and the Rust repair API. Pin the
   Shifty version if you depend on them.

These features are working and tested, but their interfaces are still
settling.

.. list-table::
   :widths: 30 70

   * - :doc:`Symbolic repair <repair/index>`
     - Compute the edits that would make a failing node conform, and check
       them against the whole graph.
   * - :doc:`Shape definition DSL <shapeshift/index>`
     - Write SHACL shapes as short lists of requirements, in ``.shm`` files
       or from Python.
   * - :doc:`Violation generation <violation-generation/index>`
     - Break a valid graph on purpose, to test repair tools against a known
       answer.

.. toctree::
   :hidden:

   repair/index
   shapeshift/index
   violation-generation/index
