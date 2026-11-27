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

.. literalinclude:: ../../examples/result-database/shapes.ttl
   :language: turtle

And here is data for two VAVs, where ``vav1`` has no airflow sensor and
``vav2`` has two:

.. literalinclude:: ../../examples/result-database/data.ttl
   :language: turtle

Creating the database
---------------------

This script registers the shapes with ``add_shape_tables()``, which creates an
empty table for each node shape. It then calls ``refresh()``, which validates
the data and fills the tables, and prints the ``[ex:VavShape]`` table:

.. literalinclude:: ../../examples/result-database/vav-table.py
   :language: python

.. program-output:: python vav-table.py
   :cwd: ../../examples/result-database

The columns are ``result_id``, ``focus``, ``supply_air_temperature``, and
``airflow``. Python prints SQL ``NULL`` as ``None``.

``vav1`` fails the shape because it has no airflow sensor, but its
temperature sensor still matched, so it gets a row with ``airflow`` left
``NULL``. ``vav2`` has two airflow sensors and gets a row for each, so use
``COUNT(DISTINCT focus)`` when you count VAVs.

Both properties follow ``brick:hasPoint``, so each column holds only the
points that match its qualified value shape. Column names come from each
property shape's ``rdfs:label`` in snake case, and values are RDF terms
written in N-Triples form. See :doc:`advanced` to name columns from other
predicates. The ``result_id`` column links each row to the validation result
for its focus node, as described in `Running queries`_.

``ResultDatabase()`` keeps the database in memory. To save it to a file you
can open with any SQLite tool, pass a filename, such as
``ResultDatabase("building.sqlite")``.

Running queries
---------------

You can open the database with any SQLite tool. Alongside the shape tables,
every refresh records how each focus did against its shape in
``_shifty_result_validation``, one row per focus and shape. Each of those
rows has a ``result_id``, and every row of the shape table carries the
``result_id`` of its focus as a foreign key. Both of ``vav2``'s rows share one
``result_id``.

Each query below comes from a complete script in
``docs/examples/result-database``, which builds the database the same way as
``vav-table.py``.

To find the VAVs that fail, join the shape table to the validation results:

.. literalinclude:: ../../examples/result-database/failing.py
   :language: sql
   :start-after: query = """
   :end-before: """

.. program-output:: python failing.py
   :cwd: ../../examples/result-database

To see what each failing focus is missing, query the
``_shifty_result_missing_required_values`` view:

.. literalinclude:: ../../examples/result-database/missing.py
   :language: sql
   :start-after: query = """
   :end-before: """

.. program-output:: python missing.py
   :cwd: ../../examples/result-database

For the full picture, ``_shifty_result_binding_status`` records every field
of every result: whether it passed, how many values matched, and how many are
missing. It has ``table_name`` and ``focus`` as well as ``result_id``, so you
can filter it directly or join it on ``result_id``:

.. literalinclude:: ../../examples/result-database/binding-status.py
   :language: sql
   :start-after: query = """
   :end-before: """

.. program-output:: python binding-status.py
   :cwd: ../../examples/result-database

A ``result_id`` is a 16-character hex string hashed from the shape IRI and
focus node. The same VAV keeps the same ``result_id`` across refreshes, even
if you rename the table. You can store it outside the database and look the
VAV up again later. Blank-node foci are the exception: their labels change
between refreshes, and so do their IDs.

Keeping tables up to date
-------------------------

Call ``refresh()`` again whenever the RDF changes. A database file stores its
shapes, so you can reopen it later and refresh with just the new data:

.. code-block:: python

   db = ResultDatabase("building.sqlite")
   db.refresh("data-v2.ttl")

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
