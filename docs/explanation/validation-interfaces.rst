Validation interfaces
=====================

Shifty provides a native algebraic result model and a W3C-compatible report
model. They apply the same shapes and produce the same conformance decision,
but retain different information about the evaluation.

Algebraic results
-----------------

``validate_algebra()`` returns an ``AlgebraResult``. The command-line
validator uses the same result model for its default text and JSON output.

.. code-block:: python

   result = shifty.validate_algebra(data, shapes)

   if not result.conforms:
       for violation in result.violations:
           for reason in violation.reasons:
               print(reason.constraint_kind, reason.path, reason.value)

Each reason identifies the nested algebra operator that failed, its property
path, and the relevant value. Stable statement and constraint identifiers join
the result to evidence and repair records. The exact fields are listed in the
:doc:`Python API reference <../reference/python>`.

This interface does not construct a W3C ``sh:ValidationReport`` graph. It
avoids the cost of constructing and serializing report RDF and preserves
details that do not map directly to the W3C result vocabulary. The compiled
evaluator also normalizes shared expressions, seeds targets from indexes, and
orders conjunctions by estimated cost. :doc:`architecture` describes those
optimizations, and :doc:`benchmarks` records end-to-end measurements.

The algebraic representation supports several additional views:

- :doc:`Evidence <../reference/evidence>` records a satisfaction trace or
  failure witness for every selected ``(statement, focus)`` pair, including
  nodes that pass.
- :ref:`Property witnesses <python-property-witnesses>` extract values from
  property shapes attached to passing focus nodes.
- :doc:`Shape maps <../reference/shape-maps>` expose selected focus nodes and
  values as typed bindings, including partial bindings for failures.

W3C reports
-----------

``validate()`` returns ``(conforms, report_graph, results_text)``. The report
graph is an ``rdflib.Graph`` containing a W3C ``sh:ValidationReport``.

.. code-block:: python

   conforms, report_graph, results_text = shifty.validate(data, shapes)

The corresponding CLI option is:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl --report

Use this interface when the result must be consumed by software that expects
the standard SHACL report vocabulary. The report contains validation failures;
it does not represent passing focus nodes or the complete derivation behind a
result. Evidence and shape maps provide those views independently of the W3C
report.

Basis and rationale
-------------------

The path and shape algebra is adapted from the SHACL formalization in:

   Shqiponja Ahmetaj et al. `Common Foundations for SHACL, ShEx, and
   PG-Schema <https://doi.org/10.1145/3696410.3714694>`_. *Proceedings of The
   Web Conference 2025*, pp. 8–21. ACM, 2025.

`Introducing the Shifty SHACL Engine
<https://gtf.fyi/posts/shacl/shifty/>`_ describes the reporting and performance
requirements that motivated the implementation.
