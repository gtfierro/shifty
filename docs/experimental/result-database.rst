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

       incomplete = db.connection.execute(
           "SELECT focus FROM [ex:VavShape] WHERE conforms = 0"
       ).fetchall()

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

Each direct ``sh:property`` defines a field. Field names default to
``sh:name``, the property shape's local name, or the path predicate's local
name, in that order. Complex paths without a name use ``field_N``. To choose
exact SQL names or resolve name collisions, set ``sql:tableName`` on the node
shape or ``sql:columnName`` on the property shape, using the vocabulary
``https://shifty.gtf.fyi/ns/sql#``.

Read full and partial results
-----------------------------

Each shape table has one row per targeted ``focus`` and its ``conforms``
flag. A field with a maximum of one in its extraction domain becomes a
nullable column. Repeatable fields use a ``<table>_<field>`` child table with
``focus`` and ``value`` columns. Focus and value terms are stored in N-Triples
spelling. A failing focus still contributes its qualifying values.

Query ``<table>_slot_status`` by ``focus`` and ``field`` to see the binding
``status``, qualifying ``value_count``, and ``missing`` count. An empty set
has ``value_count = 0``. An unavailable set has ``value_count IS NULL``. When
a scalar field has several qualifying values, its column contains ``NULL``
and ``<table>_scalar_values`` records the values by ``focus`` and ``field``.
The ``_shifty_result_tables`` and ``_shifty_result_fields`` catalogs identify
the source shapes and each field's storage table.
