Tutorials
=========

These tutorials introduce validation, structured results, and failure
explanations using small examples.

:doc:`first-validation` installs Shifty, writes a shapes file and a data file
by hand, and produces a validation report. It also separates the roles of the
shapes graph and the data graph.

:doc:`reading-results` moves from reading a report to writing code that
consumes one: walking violations and their reasons, branching on the constraint
that failed rather than on its message, and grouping findings the way your
consumer needs them.

:doc:`explaining-a-failure` uses the evidence interface to inspect which nodes
passed or failed and the derivation behind each result.

If you already know SHACL and just want a specific job done, the
:doc:`how-to guides <../how-to/index>` are shorter and assume more.

.. toctree::
   :maxdepth: 1

   first-validation
   reading-results
   explaining-a-failure
