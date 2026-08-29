Tutorials
=========

Three lessons, meant to be worked through in order at a terminal. They are
learning material rather than reference: every step is spelled out, and the
files are small enough that you can hold the whole example in your head.

:doc:`first-validation` installs Shifty, writes a shapes file and a data file
by hand, and gets a validation report out. By the end you will have seen the
difference between the shapes graph and the data graph, which is the single
distinction that causes the most confusion later.

:doc:`reading-results` moves from reading a report to writing code that
consumes one: walking violations and their reasons, branching on the constraint
that failed rather than on its message, and grouping findings the way your
consumer needs them.

:doc:`explaining-a-failure` asks the harder question. A validation result is a
list of failures, so it cannot tell you which nodes passed, or why. The
evidence interface keeps the validator's derivation and answers both.

If you already know SHACL and just want a specific job done, the
:doc:`how-to guides <../how-to/index>` are shorter and assume more.

.. toctree::
   :maxdepth: 1

   first-validation
   reading-results
   explaining-a-failure
