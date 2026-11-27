SQLite result database
======================

.. include:: experimental.inc

Shifty can store validation results in a SQLite database. It creates one
table per node shape and one column per property shape, then adds a row for
each focus node holding the value nodes that matched each property. Another
way to think of it is putting the :doc:`shape map <../../reference/shape-maps>`
results into SQL tables. Because the rows come from the shape map, a focus
node that fails validation still gets a row, with ``NULL`` wherever a value
is missing.

Here is a shape, written against the `Brick <https://brickschema.org>`_
ontology, that requires every VAV to have exactly one supply-air temperature
sensor and at least one airflow sensor:

.. code-block:: turtle

   ex:VavShape a sh:NodeShape ;
       sh:targetClass brick:VAV ;
       sh:property [
           rdfs:label "supply air temperature" ;
           sh:path brick:hasPoint ;
           sh:qualifiedValueShape [ sh:class brick:Supply_Air_Temperature_Sensor ] ;
           sh:qualifiedMinCount 1 ; sh:qualifiedMaxCount 1
       ] , [
           rdfs:label "airflow" ;
           sh:path brick:hasPoint ;
           sh:qualifiedValueShape [ sh:class brick:Air_Flow_Sensor ] ;
           sh:qualifiedMinCount 1
       ] .

And here is data for two VAVs, where ``vav1`` has no airflow sensor and
``vav2`` has two:

.. code-block:: turtle

   ex:vav1 a brick:VAV ; brick:hasPoint ex:sat1 .
   ex:vav2 a brick:VAV ; brick:hasPoint ex:sat2, ex:flow2, ex:flow3 .

   ex:sat1 a brick:Supply_Air_Temperature_Sensor .
   ex:sat2 a brick:Supply_Air_Temperature_Sensor .
   ex:flow2 a brick:Air_Flow_Sensor .
   ex:flow3 a brick:Air_Flow_Sensor .

Validating that data produces this ``[ex:VavShape]`` table:

.. list-table::
   :header-rows: 1

   * - focus
     - airflow
     - supply_air_temperature
   * - ``<https://example.org/vav1>``
     - ``NULL``
     - ``<https://example.org/sat1>``
   * - ``<https://example.org/vav2>``
     - ``<https://example.org/flow2>``
     - ``<https://example.org/sat2>``
   * - ``<https://example.org/vav2>``
     - ``<https://example.org/flow3>``
     - ``<https://example.org/sat2>``

``vav1`` fails the shape because it has no airflow sensor, but its
temperature sensor still matched, so it gets a row with ``airflow`` left
``NULL``. ``vav2`` has two airflow sensors and gets a row for each, so use
``COUNT(DISTINCT focus)`` when you count VAVs.

Both properties follow ``brick:hasPoint``, so each column holds only the
points that match its qualified value shape. Column names come from each
property shape's ``rdfs:label`` in snake case, and values are RDF terms
written in N-Triples form. See :doc:`advanced` to name columns from other
predicates.

Every shape table also has a ``result_id`` column, left out above. It links
each row to the validation result for its focus node, as described in
`Running queries`_.

Creating the database
---------------------

Register the shapes with ``add_shape_tables()``, which creates an empty table
for each node shape, then call ``refresh()`` to validate the data and fill
the tables:

.. code-block:: python

   from shifty.experimental.result_database import ResultDatabase

   with ResultDatabase("building.sqlite") as db:
       db.add_shape_tables("shapes.ttl")
       db.refresh("building.ttl")

The repository includes this example as ``python/examples/result_database.py``.
Run it from ``python/``, giving it a new filename each time:

.. code-block:: bash

   uv run python examples/result_database.py demo.sqlite

Running queries
---------------

You can open the database with any SQLite tool. Alongside the shape tables,
every refresh records how each focus did against its shape in
``_shifty_result_validation``, one row per focus and shape. Each of those
rows has a ``result_id``, and every row of the shape table carries the
``result_id`` of its focus as a foreign key. Both of ``vav2``'s rows share one
``result_id``.

To find the VAVs that fail, join the shape table to the validation results:

.. code-block:: sql

   SELECT vav.*
   FROM [ex:VavShape] AS vav
   JOIN _shifty_result_validation USING (result_id)
   WHERE conforms = 0;

This returns ``vav1``'s row, with its temperature sensor and a ``NULL``
airflow. To see what each failing focus is missing, query the
``_shifty_result_missing_required_values`` view:

.. code-block:: sql

   SELECT focus, field_name, missing
   FROM _shifty_result_missing_required_values;

This returns a single row saying ``vav1`` is missing one ``airflow`` value.
For the full picture, ``_shifty_result_binding_status`` records every field
of every result: whether it passed, how many values matched, and how many are
missing. It has ``table_name`` and ``focus`` as well as ``result_id``, so you
can filter it directly or join it on ``result_id``.

A ``result_id`` is a 16-character hex string, such as ``9f1f032621117e5b``,
hashed from the shape IRI and focus node. The same VAV keeps the same
``result_id`` across refreshes, even if you rename the table. You can store it
outside the database and look the VAV up again later. Blank-node foci are the
exception: their labels change between refreshes, and so do their IDs.

Keeping tables up to date
-------------------------

Call ``refresh()`` again whenever the RDF changes. The database file stores
its shapes, so you can reopen it later and refresh with just the new data:

.. code-block:: python

   with ResultDatabase("building.sqlite") as db:
       db.refresh("building-v2.ttl")

.. warning::

   Each refresh replaces every row in the tables Shifty manages, so any
   changes you make to them with SQL are lost on the next refresh. To make a
   lasting change, edit the RDF and refresh.

If a refresh fails, the database keeps the results from the previous one.

Next steps
----------

.. list-table::
   :widths: 30 70

   * - :doc:`advanced`
     - Add tables from SPARQL queries, choose which shapes become tables,
       rename tables and columns, and decide what counts as conforming.
   * - :doc:`how-it-works`
     - How Shifty turns shapes and data into tables and rows.

.. toctree::
   :hidden:

   advanced
   how-it-works
