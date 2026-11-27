Advanced result database features
=================================

.. include:: experimental.inc

The :doc:`basic workflow <index>` turns every shape into a table. The
features on this page add context from SPARQL, control which tables exist,
rename tables and columns, and change what counts as conforming.

Add tables from SPARQL
----------------------

Some useful facts are not part of any shape. In the VAV example, the shape
says nothing about which room a VAV serves. ``add_query_table()`` turns a
SPARQL ``SELECT`` into a table. Each variable becomes a column and each
solution becomes a row:

.. code-block:: python

   db.add_query_table("vav_rooms", """
       PREFIX brick: <https://brickschema.org/schema/Brick#>
       SELECT ?focus ?room WHERE {
           ?focus a brick:VAV .
           OPTIONAL { ?focus brick:hasLocation ?room }
       }
   """)
   db.refresh("building.ttl")

A variable with no value becomes ``NULL``. Declare the prefixes in the query
itself.

Query tables write RDF terms the same way shape tables do, so you can join
the two kinds of table on ``focus``:

.. code-block:: sql

   SELECT vav.focus, vav.airflow, rooms.room
   FROM [ex:VavShape] AS vav
   JOIN vav_rooms AS rooms USING (focus);

Each refresh updates both kinds of table together.

By default, a query sees your data and all registered shapes graphs at once.
This lets a query read details from the shapes, such as a shape's label. To
narrow it, pass ``graph_mode`` to ``add_query_table()``:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - ``graph_mode``
     - The query sees
   * - ``"union"``
     - The data and every registered shapes graph. This is the default.
   * - ``"data"``
     - Only the data passed to ``refresh()``.
   * - ``"shapes"``
     - Only the registered shapes graphs.

Choose which shapes become tables
---------------------------------

By default, every named node shape gets a table, including shapes with no
targets. To create fewer tables, pick shapes by name, by query, or both:

.. code-block:: python

   db.add_shape_tables(
       "hvac-shapes.ttl",
       shape_names=["https://example.org/VavShape"],
   )

``shape_query`` takes a SPARQL ``SELECT`` that binds ``?shape``. It runs
against the shapes graph, which is useful when the shapes mark themselves
for publishing:

.. code-block:: python

   db.add_shape_tables(
       "hvac-shapes.ttl",
       shape_query="""
           PREFIX ex: <https://example.org/>
           SELECT ?shape WHERE { ?shape ex:publish true }
       """,
   )

If you pass both, Shifty creates a table for every shape either one selects.

A helper shape that a selected shape refers to, for example through
``sh:node``, is still checked. It just doesn't get its own table.

``shape_names=[]`` creates no tables. The shapes graph is still registered,
so SPARQL tables can read from it.

You can call ``add_shape_tables()`` more than once to register several
shapes graphs in one database. Each shape can have only one table, so
registering a shape that already has one raises an error.

Name tables and columns
-----------------------

A table is named after its shape, using the prefixes declared in the shapes
file. ``ex:VavShape`` becomes the table ``[ex:VavShape]``. Without a matching
prefix, the table uses the full IRI. Square brackets or double quotes both
work in SQL.

A column is named after the first one of these that exists:

1. the property shape's ``sql:columnName``, used exactly as written
2. the first value reached by the name paths, tried in order
3. the local name of the property shape's IRI
4. the local name of the property's path

Shifty converts every name except ``sql:columnName`` to snake case, so
``rdfs:label "supply air temperature"`` becomes ``supply_air_temperature``.

The name paths default to ``sh:name``, then ``rdfs:label``. A property shape
with both uses its ``sh:name``. To read names from other predicates, pass
``name_path`` to ``add_shape_tables()``. It takes one SPARQL property path or
an ordered list, and replaces the defaults:

.. code-block:: python

   db.add_shape_tables("shapes.ttl", name_path=["skos:prefLabel", "rdfs:label"])

To add a path to the defaults instead of replacing them, extend
``DEFAULT_NAME_PATHS``:

.. code-block:: python

   from shifty.shapemap import DEFAULT_NAME_PATHS

   db.add_shape_tables("shapes.ttl", name_path=[*DEFAULT_NAME_PATHS, "skos:prefLabel"])

``sh:`` and ``rdfs:`` work without a prefix declaration. Declare any other
prefix, such as ``skos:``, in the shapes file. The database stores the paths
with the shapes, so later refreshes keep the same columns. Shape maps use the
same ``name_path`` setting and defaults. See :doc:`../../how-to/shape-maps`.

With ``name_path=None``, both VAV properties fall back to their shared path,
``brick:hasPoint``, and Shifty rejects the two matching ``has_point``
columns.

To set one name directly, annotate the shape:

.. code-block:: turtle

   @prefix sql: <https://shifty.gtf.fyi/ns/sql#> .

   ex:VavShape sql:tableName "vavs" ;
       sh:property [
           sh:path brick:hasPoint ;
           sh:qualifiedValueShape [ sh:class brick:Supply_Air_Temperature_Sensor ] ;
           sql:columnName "sat_sensor"
       ] .

Decide what counts as conforming
--------------------------------

By default, any SHACL result marks a focus as not conforming, even an
``sh:Info`` result. To ignore lower-severity results, pass
``minimum_severity`` to ``refresh()``:

.. code-block:: python

   db.refresh("building.ttl", minimum_severity="violation")

With this setting, a VAV with only warnings counts as conforming in
``_shifty_result_validation``. The per-field table still marks the field
that raised the warning as failed.

``refresh()`` also accepts the ``infer`` and ``graph_mode`` options of
ordinary validation. See :doc:`../../how-to/validate`.

Choose where the database lives
-------------------------------

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Pass
     - To get
   * - ``":memory:"``
     - A temporary database that disappears on close. This is the default.
   * - ``"building.sqlite"``
     - A file, relative to the current directory.
   * - ``"sqlite:///building.sqlite"``
     - The same file, as a URL.
   * - ``"sqlite:////tmp/building.sqlite"``
     - An absolute path. Note the four slashes.

A file-backed database stores its shapes graphs, shape selections, and
SPARQL queries. That is why a reopened database can refresh without
registering them again.
