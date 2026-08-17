How-to guides
=============

Recipes for a particular job, assuming you already know what you want. Each
page is a sequence of steps with just enough explanation to make the steps make
sense; the reasoning behind them lives in :doc:`../explanation/index`, and the
exhaustive parameter lists in :doc:`../reference/index`.

If you are new to Shifty, the :doc:`tutorials <../tutorials/index>` are a better
starting point.

.. toctree::
   :maxdepth: 1

   install
   validate
   infer
   explain-failures
   shape-maps
   inspect-pipeline
   browser

Experimental
------------

.. toctree::
   :maxdepth: 1

   repair

:doc:`repair` covers the symbolic repair layer, which computes the space of
edits that would make a failing node conform. It works, but it is early: the
API is expected to change, several constraint kinds are not yet invertible, and
it edits data graphs only. Treat it as a preview rather than a stable
interface.
