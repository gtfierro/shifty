SQLite result database
======================

The experimental ``ResultDatabase`` projects RDF data into SQLite. The shapes
graph defines the tables, and each refresh fills them with full and partial
results from the :doc:`shape map <../reference/shape-maps>`.

.. code-block:: python

   from shifty.experimental.result_database import ResultDatabase

   with ResultDatabase("sqlite:///building.sqlite") as db:
       db.add_shape_tables("hvac-shapes.ttl")
       db.add_shape_tables("meter-shapes.ttl")
       db.populate_shape_tables("building.ttl")

       vavs = db.connection.execute("SELECT * FROM [ex:VavShape]").fetchall()

.. warning::

   Every successful ``populate_shape_tables()`` call replaces **all rows** in
   the Shifty-managed result tables. This discards manual SQL inserts,
   updates, and deletes to those tables. SQL edits affect SQLite only. Edit
   the source RDF and refresh again to make a lasting change. An error during
   refresh preserves the previous snapshot.

Use ``:memory:`` for a temporary database, a filename for a local file, or a
``sqlite:///`` URL. For an absolute path use four slashes, as in
``sqlite:////tmp/building.sqlite``. A file-backed database retains the shapes
graphs and their selections, so it can be reopened and refreshed later.

Run the self-contained VAV demo from the repository's ``python/`` directory.
It creates a SQLite file and queries the VAVs and their airflow sensors.
The first VAV has ``NULL`` for airflow and still has its supply-air temperature
sensor. The second VAV appears twice, once per sensor. Pass a new output
filename to run the demo again.

.. code-block:: console

   uv run python examples/result_database.py result-flat.db

Choose the tables
-----------------

By default, ``add_shape_tables()`` creates an empty table for every named node
shape, including shapes with no targets. Use ``shape_names`` for an explicit
list of full shape IRIs and ``shape_query`` for a SPARQL ``SELECT`` that binds
``?shape`` to a node shape IRI in the shapes graph. When supplied together,
both selectors contribute shapes. An explicit empty selection creates zero
tables. Shifty's bundled SPARQL evaluator runs the selector once at
registration. Referenced helper shapes remain available during evaluation.

.. code-block:: python

   db.add_shape_tables(
       "hvac-shapes.ttl",
       shape_names=["https://example.org/VavShape"],
       shape_query="""
           PREFIX site: <https://example.org/>
           SELECT ?shape WHERE { ?shape site:publish true }
       """,
   )

Table names use the shape's IRI compacted with the shapes graph's prefix
declarations, using the most specific matching namespace. ``ex:VavShape``
becomes ``[ex:VavShape]`` in SQL. The local name keeps its original spelling,
so ``ex:vav_shape`` becomes ``[ex:vav_shape]``. When no declaration applies,
use the full IRI as the table name, for example
``[https://example.org/VavShape]``. SQLite double quotes also work for these identifiers.

Each direct ``sh:property`` defines a nullable column on its shape's table.
Field names default to ``sh:name``, the property shape's local name, or the path
predicate's local name, in that order. Complex paths without a name use
``field_N``. To choose exact SQL names or resolve name collisions, set
``sql:tableName`` on the node shape or ``sql:columnName`` on the property
shape, using the vocabulary ``https://shifty.gtf.fyi/ns/sql#``.

Read full and partial results
-----------------------------

Shifty creates one application table per selected shape. A row holds
``focus`` and one RDF term per property column. A property with no
recovered qualifying values shows ``NULL``. Focus and value terms use
N-Triples spelling.

A focus gets one row per combination of its qualifying values. Two airflow
sensors and three dampers produce six rows for that focus. The same rule
preserves recovered values when a property exceeds its declared maximum.
Use ``COUNT(DISTINCT focus)`` when counting VAVs.

.. code-block:: sql

   SELECT *
   FROM [ex:VavShape]
   ORDER BY focus, airflow;

Validation results
------------------

The same refresh records validation in two shared tables. Use the generated
SQL table name and ``focus`` to find the corresponding validation results.

- ``_shifty_result_validation`` has one row per shape and focus, with ``conforms``.
- ``_shifty_result_binding_status`` has one row per shape, focus, and field,
  with ``status``, ``value_count``, and ``missing``. ``value_count`` is zero
  for a known empty set and ``NULL`` when values are unavailable.

.. code-block:: sql

   SELECT focus
   FROM _shifty_result_validation
   WHERE table_name = 'ex:VavShape' AND conforms = 0;

Find missing required values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``_shifty_result_missing_required_values`` view shows each field with a
positive missing count in the current snapshot. In the VAV demo it lists
``vav1`` with one missing ``airflow`` value. Refreshing after adding the
required value removes that entry from the view.

.. code-block:: sql

   SELECT table_name, focus, field_name, missing
   FROM _shifty_result_missing_required_values
   ORDER BY table_name, focus, field_name;
