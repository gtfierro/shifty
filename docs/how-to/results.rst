Handle validation results in code
=================================

Use ``validate_algebra()`` to handle failures as Python objects. For a W3C
report graph, use ``validate()`` as shown in :doc:`../tutorials/first-validation`.

Read individual failures
------------------------

Using the shapes and failing data from the validation tutorial:

.. literalinclude:: ../examples/structured-results.py
   :language: python

.. program-output:: python examples/structured-results.py
   :cwd: ..

A violation is one failing focus node under one statement. Its reasons
describe the individual failures. Bob has one violation with two reasons.

Branch on ``reason.constraint_kind`` when handling a failure in code.
``reason.message`` is display text and may change:

.. code-block:: python

   import shifty

   result = shifty.validate_algebra("data.ttl", "shapes.ttl")
   for violation in result.violations:
       for reason in violation.reasons:
           if reason.constraint_kind == shifty.ConstraintKind.Cardinality:
               print("missing or excess values:", reason.path)
           elif reason.constraint_kind == shifty.ConstraintKind.ValueType:
               print("wrong type:", reason.value, "at", reason.path)

``reason.value`` identifies an offending value when available; for a missing
value, it falls back to the focus node. Prefer
``reason.author_message or reason.message`` to display an authored
``sh:message`` when present.

Apply a severity threshold
--------------------------

To retain warnings in the results while only violations make the graph fail:

.. code-block:: python

   result = shifty.validate_algebra(
       "data.ttl", "shapes.ttl", minimum_severity="violation"
   )
   print(result.conforms)
   for violation in result.violations:
       for reason in violation.reasons:
           print(reason.severity, reason.message)

``conforms`` applies the threshold. ``violations`` still includes findings
below it. Severity belongs to each reason; ``violation.severity`` is the most
severe reason for that focus node and statement.

.. _trace-result-constraint:

Inspect the compiled constraint
-------------------------------

Each reason identifies the compiled constraint that produced it:

.. code-block:: python

   for violation in result.violations:
       for reason in violation.reasons:
           print(reason.constraint_id, reason.constraint.render)
           print(reason.constraint.definition)

``violation.constraint_id`` identifies the top-level constraint;
``reason.constraint_id`` identifies the nested constraint that failed.
Compare these with :ref:`inspect-validation` to trace a result through the
compiled schema.

See :doc:`../reference/python` for exact result fields and
:doc:`explain-failures` for supporting values and evidence trees.
