Experimental
============

.. admonition:: Experimental: this can change at any time
   :class: experimental-banner

   Everything in this section is experimental. Its languages, commands,
   Python modules (under ``shifty.experimental``), and Rust interfaces (the
   ``shifty-experimental`` crate) can change in any release, without a
   deprecation period. Pin the Shifty version if you depend on them.

.. list-table::
   :widths: 30 70

   * - :doc:`Shape definition DSL <shapeshift/index>`
     - Shapeshift: a language for writing SHACL shapes as a list of
       requirements, from text or from Python. A tutorial, a language guide,
       a Python guide, and a reference.
   * - :doc:`Violation generation <violation-generation/index>`
     - Break a conforming graph with recorded edits to build test cases for
       repair tools. A tutorial, a how-to, an explanation, and a reference.

.. toctree::
   :hidden:

   shapeshift/index
   violation-generation/index
