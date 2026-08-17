Tutorials
=========

Two lessons, meant to be worked through in order at a terminal. They are
learning material rather than reference: every step is spelled out, and the
files are small enough that you can hold the whole example in your head.

:doc:`first-validation` installs Shifty, writes a shapes file and a data file
by hand, and gets a validation report out. By the end you will have seen the
difference between the shapes graph and the data graph, which is the single
distinction that causes the most confusion later.

:doc:`explaining-a-failure` picks up the graph you just failed and asks the
engine a harder question: not *is this wrong*, but *why*, and *what would fix
it*. It ends with the repair loop patching the graph and the validator
confirming it conforms.

If you already know SHACL and just want a specific job done, the
:doc:`how-to guides <../how-to/index>` are shorter and assume more.

.. toctree::
   :maxdepth: 1

   first-validation
   explaining-a-failure
