Validate a graph
================

Run validation from the CLI or Python, choose the report format, and
configure which shapes and triples are checked.

Basic run
---------

.. literalinclude:: ../examples/quick-start/validate.sh
   :language: bash

.. program-output:: bash validate.sh
   :cwd: ../examples/quick-start

.. code-block:: python

   conforms, report_graph, results_text = shifty.validate(data, shapes)

Both ``--shapes`` and ``--data`` accept local paths or ``http(s)`` URLs, and
both are repeatable — several files are merged into one graph before anything
else happens.

Omitting the second Python argument uses shapes embedded in the data graph.
Omitting CLI ``--data`` makes the ``--shapes`` graph serve both roles. An
explicitly supplied shapes graph with zero triples is rejected. See
:doc:`../explanation/shapes-and-data` for graph-input rules.

Get a machine-readable report
-----------------------------

The default CLI output is a summary for a human. For a W3C
``sh:ValidationReport`` graph, serialized as Turtle:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl --report

For JSON:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl --format json

From Python, ``validate()`` already returns the report as an ``rdflib.Graph``,
so serialize it however you like:

.. code-block:: python

   conforms, report_graph, _ = shifty.validate(data, shapes)
   print(report_graph.serialize(format="turtle"))

``validate_algebra()`` returns violations as Python objects with the failing
focus node, the property path, the offending value, and a stable constraint
kind to branch on:

.. code-block:: python

   result = shifty.validate_algebra(data, shapes)
   for violation in result.violations:
       for reason in violation.reasons:
           if reason.constraint_kind == shifty.ConstraintKind.Cardinality:
               print(violation.focus_node, reason.path, reason.message)

Branch on ``constraint_kind``, never on the text of ``message``.

Skip inference
--------------

By default a validation run first executes any SHACL-AF ``sh:rule`` entries in
the shapes graph to a fixed point, and validates the extended graph. If your
shapes contain no rules this costs almost nothing; if they do, and you want to
validate only what is asserted:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl --no-infer

.. code-block:: python

   conforms, report, text = shifty.validate(data, shapes, infer=False)

To retain the derived triples in a Python graph, use ``in_place=True`` as
shown in :ref:`inference-during-validation`.

Validate against only some shapes
---------------------------------

A large shapes graph often contains many independent profiles, and you want one
of them. ``--shape-name`` (repeatable; ``--entry-shape`` is an alias) restricts
which shapes act as *entry points*:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl \
     --shape-name http://example.org/PersonShape

.. code-block:: python

   conforms, report, text = shifty.validate(
       data, shapes,
       shape_names=["http://example.org/PersonShape"],
   )

The named shapes select the focus nodes. Any helper shapes they reference
through ``sh:node``, ``sh:property``, qualified value shapes, or boolean
combinations are also evaluated. IRIs may be bare or in angle brackets.

Change which triples are visible
--------------------------------

``graph_mode`` controls the graph that property paths, class hierarchies, and
SPARQL see during evaluation. It is independent of where shape *definitions*
come from.

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl --graph-mode union-all

.. code-block:: python

   conforms, report, text = shifty.validate(data, shapes, graph_mode="union-all")

- ``data`` — focus nodes and evaluation use the data graph alone.
- ``union`` (default) — focus nodes from data; evaluation sees data ∪ shapes.
- ``union-all`` — both focus selection and evaluation see data ∪ shapes.

Use ``data`` for a self-contained data graph and ``union-all`` when the shapes
graph also contains instances to validate. The
:doc:`graph-input explanation <../explanation/shapes-and-data>` covers
ontology visibility and shape definitions.

Choose what counts as failure
-----------------------------

SHACL constraints carry a ``sh:severity``. By default every severity — info,
warning, and violation — makes the run non-conforming. To ignore the milder
ones:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl --minimum-severity violation

.. code-block:: python

   conforms, report, text = shifty.validate(
       data, shapes, minimum_severity="violation",
   )

Results below the threshold remain in the W3C report and Python result objects;
the threshold controls their ``conforms`` value. The CLI's default text and JSON
summaries omit below-threshold findings.

Validate many graphs against one schema
---------------------------------------

``PreparedValidator`` compiles the shapes once and reuses them for each
data graph:

.. code-block:: python

   validator = shifty.PreparedValidator(shapes)

   for path in data_files:
       conforms, report, text = validator.validate(path)

This avoids repeating setup when the shapes are large relative to the data.
See :doc:`../benchmarks` for measurements.

Handle unsupported constructs
-----------------------------

A few SHACL features are supported partially (see
:doc:`../reference/feature-support`). ``on_unsupported`` decides what happens
when the engine meets one:

.. code-block:: python

   conforms, report, text = shifty.validate(data, shapes, on_unsupported="error")

``"error"`` is the default: unsupported features reject the run. Pass
``"ignore"`` to request best effort explicitly; that result may be incomplete.
An execution failure raises ``shifty.ValidationFailure`` and supplies no
conformance verdict. A completed validation with constraint violations still
returns a report with ``conforms=False``.

Invalid shapes are separate from ``on_unsupported``. A malformed SHACL
constraint or rule — including malformed SPARQL or an unresolved query prefix
— raises an error before validation starts. It is never treated as an omitted
unsupported feature.

.. _inspect-validation:

Investigate unexpected results
------------------------------

Use ``shifty inspect`` to examine a shapes graph without loading data. It
takes the shapes file as a positional argument:

.. code-block:: bash

   shifty inspect --stage rdf shapes.ttl
   shifty inspect --stage algebra shapes.ttl
   shifty inspect --stage strata shapes.ttl

The ``rdf`` stage shows parsed shapes, ``algebra`` shows compiled constraints,
and ``strata`` helps diagnose rejected recursion. The
:doc:`CLI reference <../reference/cli>` lists every stage and format.

For a validation run, ``--profile`` reports input sizes and timings.
``--dump-data`` and ``--dump-shapes`` write the graphs used by that run,
including inferred data. See :ref:`cli-profile` and :ref:`cli-dump` for output
details, or :doc:`explain-failures` to inspect an individual result.

See also
--------

- :doc:`../reference/cli` and :doc:`../reference/python` — every flag and
  argument.
- :doc:`explain-failures` — when the report is not enough.
