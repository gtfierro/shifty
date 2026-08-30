Validate a graph
================

The common variations on running a validation, and what each one changes.

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

An explicitly supplied shapes graph with zero triples is rejected. Omitting the
second Python argument uses shapes embedded in the data graph; omitting CLI
``--data`` makes the ``--shapes`` graph serve both roles.

Omitting ``--data`` (or the second Python argument) makes the single graph play
both roles. If that is not what you expect, read
:doc:`../explanation/shapes-and-data` before going further; it is the most
common source of a validation that passes for the wrong reason.

Get a machine-readable report
-----------------------------

The default CLI output is a summary for a human. For a W3C
``sh:ValidationReport`` graph, serialized as N-Triples:

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

If you would rather have structured objects than an RDF graph to query,
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

Only target-bearing statements owned by the named shapes select focus nodes.
Helper shapes those entries reach through ``sh:node``, ``sh:property``,
qualified value shapes, or boolean combinations are still evaluated as normal —
this narrows what is checked, not what the checks mean. IRIs may be bare or in
angle brackets.

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

The default exists because class hierarchies and ontology axioms are usually
authored alongside the shapes, and ``sh:class`` needs to traverse
``rdfs:subClassOf`` to work. Reach for ``data`` when you want a strict check
that the data stands on its own; reach for ``union-all`` when the shapes graph
also contains instances you intend to validate.

The :doc:`shapes and data graph explanation
<../explanation/shapes-and-data>` tabulates these modes and covers the separate
question of where shape definitions come from.

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

Results below the threshold are still computed and still appear in the report;
the flag only decides which ones flip ``conforms`` to false.

Validate many graphs against one schema
---------------------------------------

Compiling a shapes graph — parsing, lowering, normalizing, planning — is a
fixed cost paid before any data is examined, and for a large ontology it
dominates a small validation. ``PreparedValidator`` pays it once:

.. code-block:: python

   validator = shifty.PreparedValidator(shapes)

   for path in data_files:
       conforms, report, text = validator.validate(path)

This matters more than it sounds like it should: on the Brick corpus, whose
models are small against a 229k-triple shapes closure, most of a per-process
run's wall clock is this setup. See :doc:`../explanation/benchmarks`.

Handle unsupported constructs
-----------------------------

A few SHACL features are supported partially (see
:doc:`../reference/feature-support`). ``on_unsupported`` decides what happens
when the engine meets one:

.. code-block:: python

   conforms, report, text = shifty.validate(data, shapes, on_unsupported="error")

``"ignore"`` (the default) makes a best effort and may return an unreliable
answer; ``"error"`` refuses, so the problem surfaces instead of being silently
absorbed. If you are validating anything you will act on, ``"error"`` is the
safer default and the one to start with.

See also
--------

- :doc:`../reference/cli` and :doc:`../reference/python` — every flag and
  argument.
- :doc:`explain-failures` — when the report is not enough.
