Inspect a shapes graph
======================

Use ``shifty inspect`` to check how a shapes graph was parsed or to inspect a
recursion diagnostic. It takes the shapes file as a positional argument:

.. code-block:: bash

   shifty inspect --stage rdf shapes.ttl
   shifty inspect --stage strata shapes.ttl

The command reads shapes only. For runtime measurements on a data graph,
use validation's ``--profile`` option. The :doc:`../reference/cli` lists
inspection stages, output formats, and profiling options.

Detailed examples of the compiler stages are preserved in the
`development documentation
<https://github.com/gtfierro/shifty/blob/main/development_docs/implementation-notes/docs/how-to/inspect-pipeline.rst>`_.
