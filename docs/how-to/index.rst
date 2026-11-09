How-to guides
=============

Guides for validation, inference, evidence, and graph queries. See
:doc:`../reference/index` for arguments and defaults, and
:doc:`../explanation/index` for result semantics.

If you are new to Shifty, the :doc:`tutorials <../tutorials/index>` are a better
starting point.

.. toctree::
   :maxdepth: 1

   validate
   infer
   explain-failures
   shape-maps
   inspect-pipeline
   conformance
   browser
   upgrading-0.6

Experimental
------------

.. toctree::
   :maxdepth: 1

   repair
   generate-violations

The :doc:`repair` guide covers candidate edits for supported constraint kinds.
The API may change. Applications choose values and validate each candidate
before applying it. :doc:`generate-violations` runs in the other direction. It
injects violations with known repairs into a conforming graph to build repair
benchmarks.

:doc:`../shapeshift/index` is a language for writing SHACL shapes, with a
Python module that writes it. Its pages are experimental too.
