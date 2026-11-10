Experimental
============

.. admonition:: Experimental: this can change at any time
   :class: experimental-banner

   Everything here can change in any release, without a deprecation period:
   the languages, the commands, the ``shifty.experimental`` Python modules,
   and the ``shifty-experimental`` Rust crate. Pin the Shifty version if you
   depend on them.

These features are working and tested, but their interfaces are still
settling.

.. list-table::
   :widths: 30 70

   * - :doc:`Shape definition DSL <shapeshift/index>`
     - Write SHACL shapes as short lists of requirements, in ``.shm`` files
       or from Python.
   * - :doc:`Violation generation <violation-generation/index>`
     - Break a valid graph on purpose, to test repair tools against a known
       answer.

.. toctree::
   :hidden:

   shapeshift/index
   violation-generation/index
