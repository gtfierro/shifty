SQLite result database
======================

The experimental ``ResultDatabase`` projects RDF data into SQLite. Register
shapes to populate tables with full and partial results from a
:doc:`shape map <../reference/shape-maps>`. A SPARQL ``SELECT`` can define
additional tables in the same database.

See :doc:`result-database-transformations` for the steps Shifty uses to
derive the tables and fill them on refresh.

.. toctree::
   :hidden:

   result-database-transformations

Create and refresh a database
-----------------------------

.. code-block:: python

   from shifty.experimental.result_database import ResultDatabase

   with ResultDatabase("sqlite:///building.sqlite") as db:
       db.add_shape_tables("hvac-shapes.ttl")
       db.add_shape_tables("meter-shapes.ttl")
       db.refresh("building.ttl")

       vavs = db.connection.execute("SELECT * FROM [ex:VavShape]").fetchall()

.. warning::

   Every successful ``refresh()`` call replaces **all rows** in the
   Shifty-managed result tables. ``populate_shape_tables()`` also refreshes
   the database. Refresh discards manual SQL inserts, updates, and deletes
   to those tables. SQL edits affect SQLite only. Edit
   the source RDF and refresh again to make a lasting change. An error during
   refresh preserves the previous snapshot.

Use ``:memory:`` for a temporary database, a filename for a local file, or a
``sqlite:///`` URL. For an absolute path use four slashes, as in
``sqlite:////tmp/building.sqlite``. A file-backed database retains the shapes
graphs, shape selections, and SPARQL queries for future refreshes.

Try the VAV demo
----------------

Run the self-contained VAV demo from the repository's ``python/`` directory.
It creates a SQLite file with the shape table ``[ex:VavShape]`` and a SPARQL
table named ``vav_locations``. Pass a new output filename to run the demo
again.

.. code-block:: bash

   uv run python examples/result_database.py result-with-queries.db
   sqlite3 -readonly result-with-queries.db

Join the tables on ``focus`` to see airflow sensors and room assignments
alongside the profile labels from the shapes graph.

.. code-block:: sql

   SELECT vav.focus, vav.airflow, locations.room, locations.profile_label
   FROM [ex:VavShape] AS vav
   JOIN vav_locations AS locations USING (focus)
   ORDER BY vav.focus, vav.airflow;

The demo returns one row for ``vav1`` with ``NULL`` for airflow and room,
and two rows for ``vav2`` in room 101, one per airflow sensor.

Choose the tables
-----------------

By default, ``add_shape_tables()`` creates an empty table for every named node
shape, including shapes with no targets. Use ``shape_names`` for an explicit
list of full shape IRIs and ``shape_query`` for a SPARQL ``SELECT`` that binds
``?shape`` to a node shape IRI in the shapes graph. When supplied together,
both selectors contribute shapes. An explicit empty selection creates zero
tables. Shifty runs the selector once at registration, with the shapes
graph's prefix declarations available to the query. Referenced helper
shapes remain available during evaluation.

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
Shifty uses the full IRI, as in ``[https://example.org/VavShape]``. SQLite
double quotes also work for these identifiers.

Each direct ``sh:property`` defines a nullable column on its shape's table.
Field names default to ``sh:name``, the property shape's local name, or the path
predicate's local name, in that order. Shifty converts the chosen label to
snake case for columns, as in ``supply_air_temperature``, and uses
``field_N`` when there is no usable label. To choose exact SQL names or
resolve name collisions, set ``sql:tableName`` on the node shape or
``sql:columnName`` on the property shape, using the vocabulary
``https://shifty.gtf.fyi/ns/sql#``.

Define a table with SPARQL
--------------------------

``add_query_table()`` creates an empty table with the columns projected by
the query. Each solution produces a row. Bound terms use N-Triples spelling,
which also lets you join against a shape table. An unbound variable becomes
SQL ``NULL``. Declare any prefixes in the SPARQL query.

.. code-block:: python

   db.add_query_table("vav_locations", """
       PREFIX ex: <https://example.org/>
       PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       PREFIX sh: <http://www.w3.org/ns/shacl#>
       SELECT ?focus ?profile_label ?room WHERE {
           ?focus a ?class .
           ?profile sh:targetClass ?class ; rdfs:label ?profile_label .
           OPTIONAL { ?focus ex:locatedIn ?room }
       }
   """)
   db.refresh("building.ttl")

The query table's ``graph_mode`` is set at registration. The default
``"union"`` queries the supplied data together with all registered shapes
graphs. ``"data"`` uses only the supplied data. ``"shapes"`` uses only the
union of the registered shapes graphs. A graph registered with
``add_shape_tables(..., shape_names=[])`` contributes to the shapes union
while creating zero shape tables.

``refresh()`` updates both kinds of tables in one transaction. The ``infer``
and ``graph_mode`` arguments to ``refresh()`` control SHACL evaluation. Query
tables read the supplied data and shapes graphs according to their own mode.

Read full and partial results
-----------------------------

Shifty creates one application table per selected shape. A row holds
``focus`` and one RDF term per property column. A property with no
recovered qualifying values shows ``NULL``. Focus and value terms use
N-Triples spelling.

A focus gets one row per combination of its qualifying values. Two airflow
sensors and three dampers produce six rows for that focus. The same rule
preserves recovered values when a property exceeds its declared maximum.
Because ``focus`` can repeat, use ``COUNT(DISTINCT focus)`` when counting
VAVs.

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

``refresh(minimum_severity=...)`` controls ``conforms``. For example,
``minimum_severity="violation"`` allows a focus with only warnings to conform.
The binding table still records those warnings as failed bindings.

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
