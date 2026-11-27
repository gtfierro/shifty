How the result database works
==============================

.. include:: experimental.inc

Register shapes or SPARQL queries to create empty tables. Call
``refresh()`` to fill them. The figures below use shortened IRIs from
the :doc:`VAV example <index>`.

Shapes define the columns
-------------------------

Each selected node shape gets a table. Each direct ``sh:property`` gets
its own column, even when two property shapes share a path.

.. code-block:: text

   SHACL                                          SQLite
   ex:VavShape                                 →  [ex:VavShape]
       focus node                              →     focus
       brick:hasPoint "supply air temperature" →     supply_air_temperature
       brick:hasPoint "airflow"                →     airflow

Shape maps fill the rows
------------------------

The shape map supplies the values Shifty found for each VAV, including
passing properties on a failing VAV. See :doc:`../../reference/shape-maps`.

.. code-block:: text

   vav1 ── temperature ── sat1
        └─ airflow ────── (empty)

   vav2 ── temperature ── sat2
        └─ airflow ────── flow2, flow3

.. list-table:: Rows in ``[ex:VavShape]``
   :header-rows: 1

   * - focus
     - supply_air_temperature
     - airflow
   * - ``vav1``
     - ``sat1``
     - ``NULL``
   * - ``vav2``
     - ``sat2``
     - ``flow2``
   * - ``vav2``
     - ``sat2``
     - ``flow3``

Shifty removes duplicate values within each field, then writes every
combination across fields. An empty field contributes one ``NULL``, keeping
the focus visible. Two airflow sensors and three dampers yield six rows.
Use ``COUNT(DISTINCT focus)`` to count VAVs.

SPARQL adds another table
-------------------------

The :doc:`advanced` example query combines room assignments from the data
graph with profile labels from the shapes graph.

.. code-block:: text

   Data graph   ── rooms ──┐
                           ├─→ SELECT ?focus ?profile_label ?room
   Shapes graph ── labels ─┘                 │
                                             ▼
                   vav_locations (focus, profile_label, room)

Each selected variable becomes a column. Each solution becomes a row,
with ``NULL`` for unbound variables. Queries read the input graphs before
SHACL inference. See :doc:`advanced` for graph options.

Both kinds of table store RDF terms as N-Triples text, so an IRI or blank
node can join across tables. Shifty gives distinct blank nodes distinct
labels within a refresh. Use IRIs to track a node across refreshes.

Validation counts each focus once
---------------------------------

``vav2`` has two shape rows and one validation row. Shifty gives each
validation row a ``result_id``, a 64-bit hash of the shape IRI and focus node
written as 16 hex characters, and writes that ID into every shape row for the
focus. Binding status is recorded once per ``result_id`` and field. Both
tables reference the validation row through a foreign key.

.. literalinclude:: ../../examples/result-database/validation.sql
   :language: sql

.. program-output:: python ../../_ext/result_database_table.py validation.sql
   :cwd: ../../examples/result-database

Both of ``vav2``'s shape rows carry ``vav2``'s ``result_id``. Because the ID
depends only on the shape and focus, a refresh gives the same focus the same
ID, and renaming the table with ``sql:tableName`` doesn't change it. This is
also why each shape can feed only one table.

``minimum_severity`` controls the conformance flag. Shifty replaces the
result rows in one transaction. A refresh error preserves the previous
snapshot.

Source code
-----------

- :download:`Python row projection and SQLite writes <../../../python/shifty/experimental/result_database.py>`
- :download:`Rust table layouts, blank nodes, and SPARQL evaluation <../../../python/src/repair/result_database.rs>`
