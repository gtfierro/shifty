How the result database transforms RDF
======================================

The :doc:`result-database` defines tables at registration and fills them on
refresh. Each refresh uses one RDF snapshot to build the shape tables,
SPARQL tables, and validation results.

Derive the table and column layouts
-----------------------------------

When you register a shapes graph, Shifty reads the authored node and
property shapes. It selects all named node shapes by default. If you pass
shape IRIs or a selector query, Shifty takes the union of those selections
and checks that each IRI names a node shape in the graph.

Each selected shape declares a table. Shifty adds a ``focus`` column, then
one column for every direct ``sh:property``, even if a refresh finds zero
values for that property. The layout keeps the identity of each authored
property shape so it can match evidence to the right column. Two property
shapes on the same path can measure different qualifying values and need
separate columns.

For a SPARQL table, Shifty reads the projected variables from the parsed
``SELECT`` in projection order. Modifiers such as ``DISTINCT`` and
``LIMIT`` can change the rows, so Shifty applies them when it runs the
query on refresh. The columns are known even when the query returns
zero rows.

Shifty stores the registration in SQLite so the database can be reopened
and refreshed. The saved shapes retain their declared prefixes and base IRI,
which the evaluator can use for authored SPARQL constraints. Shifty quotes
SQL identifiers and checks for name collisions using SQLite's ASCII
case-insensitive rules.

Give both projections the same RDF terms
----------------------------------------

Shifty takes a snapshot of the incoming data and scopes the blank nodes
before either projection reads it. Blank-node labels are local to their
source graphs, so two files can use the same label for different nodes.
Shifty keeps the data graph's labels and scans registered shapes graphs in
registration order. When a label is already in use, Shifty assigns that
shapes node a fresh label and rewrites all of its occurrences. Both the
shape-map projection and the query evaluator read those same scoped graphs.
This lets SQL joins match a blank node across result tables from one refresh.
Use IRIs for identifiers that need to survive across snapshots.

Both projections store RDF terms as SQLite text using the same N-Triples
spelling. An IRI appears between angle brackets. A literal keeps its
lexical value, with a language tag or explicit datatype when needed. A
form feed and the two literal characters backslash and ``f``, for example,
produce different SQL values because the serializer escapes them
differently.

Expand shape-map bindings into rows
-----------------------------------

Shifty validates the selected shapes with one evidence session per
registration. It builds a shape map from each validation run and the
session that produced it. With the session, bindings can recover values
from passing properties on a failing focus, as well as qualifying values
from a failing property. See :doc:`../reference/evidence` and
:doc:`../reference/shape-maps`.

For each shape, Shifty processes each focus once, even when several
targets select it. It matches the bindings to the columns by the authored
property shape identity. For each column it deduplicates and sorts the
qualifying RDF terms. An empty or unavailable set contributes one
``NULL``, keeping the focus and its other bindings visible.

Shifty takes the Cartesian product of these column values. For example,
a focus with two airflow sensors, one temperature sensor, and an empty
optional damper field produces two rows. Each has a different airflow
sensor, the same temperature sensor, and ``NULL`` for the damper. If the
damper field has three values, Shifty produces six rows, one for each
airflow and damper pair. The number of rows multiplies with the number
of values in each field. Use SQL ``ORDER BY`` when you need a particular
row order.

Load SPARQL solutions
---------------------

Shifty's bundled SPARQL evaluator runs each saved query against its
registered graph mode. It reuses the data and shapes datasets across
tables and builds their merged dataset if a query needs ``"union"``.
These datasets come from the supplied graphs before SHACL inference.

Each solution becomes one SQL row. Shifty reads its bindings in column
order, serializes bound terms the same way as shape-map values, and
inserts ``NULL`` for unbound variables. Repeated solutions produce repeated
rows unless the query removes them.

Record validation and publish the refresh
-----------------------------------------

For each shape and focus, Shifty records conformance using the
``minimum_severity`` threshold. It records the underlying binding status,
qualifying value count, and missing count once per field, before expanding
the rows. The missing-required-values view selects binding rows whose
missing count is positive. Its results reflect the current snapshot as
soon as refresh commits.

Shifty replaces all managed rows in a single SQLite transaction. Ordinary
SHACL failures still yield full or partial results. If any step raises
an error, including a SPARQL query or SQL insert, Shifty rolls back and
preserves the prior snapshot.
