.. _architecture-result-paths:

Validation interfaces
=====================

Use ``validate()`` for a W3C ``sh:ValidationReport``. It returns
``(conforms, report_graph, results_text)`` and requires ``pyshifty[rdflib]``.
The CLI produces the same report format with ``shifty validate --report``.

Use ``validate_algebra()`` for Python objects describing failures and their
nested reasons. This is also the CLI's default result format. It works with
:doc:`evidence <../reference/evidence>` and
:doc:`shape maps <../reference/shape-maps>`.

The two formats describe constraints differently. For example, a datatype
failure can appear as a count of invalid values in an algebraic finding.
The RDF report identifies ``sh:DatatypeConstraintComponent``.
See :doc:`../tutorials/reading-results` for examples and
:doc:`../reference/python` for fields and options.

To validate many data graphs against the same shapes, use
``PreparedValidator`` to compile the shapes once. See :doc:`../how-to/validate`.

Compiler details are in the `development documentation
<https://github.com/gtfierro/shifty/tree/main/development_docs>`_.
