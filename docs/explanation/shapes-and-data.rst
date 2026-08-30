Shapes graphs and data graphs
=============================

.. _shapes-and-data-graphs:

There are two questions that sound like one, and conflating them is the most
common way to get a validation run that passes for the wrong reason:

1. Where do **shape definitions** come from?
2. Which triples are **visible during evaluation**?

They are controlled by different things and have different answers.

Where shapes come from
----------------------

The rule is the same in every frontend.

**One graph in → it is both shapes and data.** Supply a single graph and Shifty
reads shape definitions *and* the data to validate from it. This is the common
combined-file case, where ``sh:NodeShape`` definitions sit alongside instance
data.

**Two graphs in → shapes come only from the shapes graph.** Supply a separate
shapes graph and data graph and the schema is compiled *only* from the shapes
graph. SHACL vocabulary that happens to live in the data graph is **ignored** —
a stray ``sh:property`` or ``sh:NodeShape`` triple in your data will never
quietly become a constraint.

.. list-table::
   :widths: 45 25 30
   :header-rows: 1

   * - Invocation
     - Shapes read from
     - Data read from
   * - ``shifty.validate(combined)`` / ``shifty.validate(combined, None)``
     - the one graph
     - the one graph
   * - ``shifty.validate(data, shapes)``
     - ``shapes`` only
     - ``data`` only
   * - ``shifty validate --shapes combined.ttl``
     - the one graph
     - the one graph
   * - ``shifty validate --shapes shapes.ttl --data data.ttl``
     - ``--shapes`` only
     - ``--data`` only

Why the asymmetry
~~~~~~~~~~~~~~~~~

It would be more convenient, in the moment, to read shapes from wherever they
are found. It is a bad idea for the same reason that letting a payload rewrite
its own validation rules is a bad idea: the data is usually the untrusted side.

Data graphs are generated, merged from several sources, and edited by people
who are not thinking about your shapes. If shapes could be sourced from data,
then anyone who can write data can weaken the schema, and — worse — can do it
by accident. Someone copies an example file that happens to include a
``sh:NodeShape``, or an upstream export includes its own SHACL profile, and now
your validation is checking something other than what you wrote. Nothing fails.
The run stays green.

Keeping the schema fixed makes validation predictable and matches the SHACL
specification's own separation of the two graphs.

Shapes embedded in data
~~~~~~~~~~~~~~~~~~~~~~~

Say so explicitly. Both ``--shapes`` and the Python ``shapes`` argument accept
multiple sources and union them, so add the data file as an additional shapes
source:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --shapes data.ttl --data data.ttl

.. code-block:: python

   conforms, report, text = shifty.validate(data, [shapes, data])

To validate a combined graph in Python, omit the second argument or pass
``None``. An explicitly supplied shapes graph that contains no triples raises
``ValueError``, preventing an accidental vacuous validation from being reported
as successful.

.. code-block:: python

   shifty.validate(combined)              # combined is both
   shifty.validate(combined, None)        # same
   shifty.validate(combined, rdflib.Graph())   # ValueError: empty shapes graph

This guard applies only when the supplied shapes graph has zero triples. A
nonempty schema whose targets select no focus nodes remains a valid conforming
run.

Which triples are visible
-------------------------

The second question is entirely separate, and applies *after* the schema is
fixed. It is controlled by ``graph_mode`` (``--graph-mode`` on the CLI), and it
governs both where focus nodes are selected and what path traversal,
class-hierarchy lookup, and SPARQL can see.

.. list-table::
   :widths: 20 35 45
   :header-rows: 1

   * - Mode
     - Focus selection
     - Evaluation graph
   * - ``data``
     - Data
     - Data
   * - ``union`` *(default)*
     - Data
     - Data ∪ shapes
   * - ``union-all``
     - Data ∪ shapes
     - Data ∪ shapes

The default is ``union`` because of class hierarchies. ``sh:class ex:Sensor``
has to hold for an ``ex:TemperatureSensor`` when the ontology says
``ex:TemperatureSensor rdfs:subClassOf ex:Sensor`` — and that axiom is almost
always authored with the shapes, not with the instance data. Under ``data``
mode the validator cannot see it, and every subclass instance fails a
constraint it satisfies.

So the modes trade off like this. ``data`` is the strict reading: the data
graph must stand entirely on its own, ontology included. Use it when you want
to know whether a graph is self-contained. ``union`` is the practical default:
the data is validated, the shapes side supplies vocabulary. ``union-all`` also
selects focus nodes from the shapes graph, which is useful when the shapes file
contains instances you intend to validate too. It can also select ontology
resources in the shapes graph as validation targets.

``infer()`` takes no ``graph_mode``. Graph modes describe what validation can
see; inference always reads and extends the data graph.

Note that expanding the evaluation graph can flip a result in either direction.
It usually makes a constraint easier to satisfy — more triples to traverse —
but it also gives ``sh:closed`` more predicates to object to, and gives
``sh:maxCount`` more values to count. Widening the graph is not monotonically
more permissive.
