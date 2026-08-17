Repair a graph
==============

.. warning::

   **Experimental.** The repair layer is the newest and least settled part of
   Shifty. Expect the API to change, and expect gaps: several constraint kinds
   have no invertible form yet, ``sh:equals`` reconciliation is coarse, and
   repairs edit data graphs only. Validation and inference are stable; this is
   not. Gate anything it produces before applying it — the API makes that
   step mandatory for a reason.

Shifty can compute the set of edits that would make a failing node conform. It
will not choose among them: which term fills a hole, how many values to add,
which alternative to take, whether to accept the result — all of those are
yours. The library computes; you decide. :doc:`../explanation/repair-design`
argues why that line is where it is.

The input is the failure evidence from
:doc:`../tutorials/explaining-a-failure`; the recipes below assume you have
read that.

Look at the repair space without writing code
---------------------------------------------

.. code-block:: bash

   shifty repair --shapes shapes.ttl --data data.ttl

.. code-block:: text

   <http://example.org/bob>  [target: class(<http://example.org/Person>)]
     All — do all:
       Edits:
         del <http://example.org/bob> <http://example.org/name> "123"^^<http://www.w3.org/2001/XMLSchema#integer>
         add <http://example.org/bob> <http://example.org/name> ?0
         ?0 : typed value
       Repeat [1..∞]:
         Edits:
           add <http://example.org/bob> <http://example.org/email> ?1
           ?1 : any node

``--stage`` picks which structure to print:

- ``witness`` — why each focus node violates (the failure evidence).
- ``tree`` (default) — the synthesized repair template.
- ``solve`` — a concrete delta found by the built-in enumeration driver.

``--format json`` on any of them, and ``--no-infer`` to skip SHACL-AF rules
before witnessing.

Apply a repair from the command line
------------------------------------

.. code-block:: bash

   shifty repair --shapes shapes.ttl --data data.ttl --apply

This runs the fixpoint driver and writes the repaired data graph as N-Triples
to stdout, overriding ``--stage``. The driver fills holes by enumerating terms
already present in the graph, which is fine for inspection and rarely the
policy you want in production — it will happily satisfy ``ex:email`` with any
node that fits. Use the Python API when the choices matter.

Enumerate the failures
----------------------

.. code-block:: python

   session = shifty.RepairSession(shapes, data)

   for failure in session.witnesses():
       print(failure.focus, failure.shape_name, failure.selector)
       print(failure.explain())

``witnesses()`` is the violation horizon: one ``Failure`` per failing
``(focus, statement)``. An empty list means the graph conforms. To scope it to a
single shape, ``witnesses_for(shape_iri)``; for the passing side,
``satisfactions_for(shape_iri)`` gives one ``Satisfaction`` per conforming focus
node, each recording the values matched along every checked path.

Fill in a repair template
-------------------------

.. code-block:: python

   tree = failure.repair_tree()
   print(tree.explain())

   plan = shifty.RepairPlan()
   instance = tree.instantiate(plan)
   while not instance.is_complete:
       for node_id in instance.open_choices:
           plan.count(node_id, 1)
       for hole in instance.open_holes:
           plan.bind(hole.id, choose_a_term(hole))
       instance = tree.instantiate(plan)

The loop is not decoration. Setting a ``Repeat`` count stamps out that many
copies of its body, and each copy gets **fresh holes with new ids**. Bind the
holes you saw before fixing the count and you will bind a template hole that no
longer corresponds to anything. Resolve choices first, re-instantiate, then bind
whatever appears.

A ``RepairPlan`` is just data — ``choose(node_id, branch)`` at an alternative,
``count(node_id, n)`` at a repeat, ``bind(hole_id, term)`` at a hole, and
``clear(id)`` to undo one. Partial plans are legal; ``instantiate`` reports
what is still open and never validates or decides anything.

Terms passed to ``bind`` are N-Triples spellings: ``'"Bob"'`` for a string
literal, ``'<http://example.org/bob>'`` for an IRI.

Choose what goes in a hole
--------------------------

Each hole states what a legal value looks like:

.. code-block:: python

   for hole in tree.holes():
       print(hole.id, hole.constraint)
       print(hole.candidates(limit=8))

``candidates()`` enumerates existing terms in the graph that satisfy the
constraint. It is a convenience for drivers that prefer reusing a node over
minting one; nothing requires a binding to come from it, and for a hole that
wants a fresh node it is the wrong tool.

When a hole demands conformance to a sub-shape rather than a simple type, it
says so:

.. code-block:: python

   if hole.conforms_to is not None:
       print(session.describe_shape(hole.conforms_to))
       subtree = session.repair_node_against(candidate, hole.conforms_to)

``describe_shape`` prints the sub-shape fully expanded, with every child inlined
and no ``@id`` indirection, which is what you need to understand what the hole
is actually asking for. ``repair_node_against`` synthesizes a tree that would
make a chosen node conform to it, returning ``None`` if it already does — that
is how you recurse into a nested obligation.

Propose an edit you wrote yourself
----------------------------------

You are not restricted to filling templates. A driver often knows the right
answer as a subgraph — a new node with its type and properties — rather than as
a single term:

.. code-block:: python

   delta = shifty.delta_from_graph(add=my_turtle_patch)
   outcome = session.gate(delta)

It gates and applies exactly like a synthesized delta. Application order is
deletes first, then adds, so a triple appearing on both sides is a net add.

Check before you commit
-----------------------

.. code-block:: python

   outcome = session.gate(instance.delta)
   outcome.fixed         # violations this delta removes
   outcome.introduced    # NEW violations it would cause
   outcome.remaining     # pre-existing, still unfixed
   outcome.is_progress   # sound, and fixed something

The gate is whole-graph, not focus-local, because a repair that fixes one node
by breaking another is not a repair. It re-validates ``G ⊕ ΔG`` and returns the
difference. It applies nothing and decides nothing — ``is_progress`` is a
summary offered for convenience, not a verdict acted on.

Accept and iterate
------------------

.. code-block:: python

   session = session.advance(instance.delta)

``advance`` returns a *new* session over the patched graph, reusing the compiled
schema and skipping re-inference. The fixpoint driver is this loop:

.. code-block:: python

   while True:
       failures = session.witnesses()
       if not failures:
           break                              # conforms
       tree = failures[0].repair_tree()       # your focus-ordering policy
       ...                                    # your hole policy
       outcome = session.gate(instance.delta)
       if not outcome.is_progress:
           break                              # give up, or choose differently
       session = session.advance(instance.delta)

Get the result out with ``session.to_graph()`` for an ``rdflib.Graph`` of the
current state, or ``session.apply(delta)`` to materialize ``G ⊕ ΔG`` without
advancing.

When there is nothing to offer
------------------------------

Some branches admit no data repair, and the tree says so rather than staying
silent:

.. code-block:: python

   if tree.is_blocked:
       print(tree.explain())

A ``sh:sparql`` constraint is opaque — an arbitrary query is not algebraically
invertible. An identity test on the focus node itself cannot be fixed by editing
data. A support reached only through a recursive back-edge has no finite set of
facts to delete. A conjunction with any blocked child is blocked; an alternative
drops its blocked branches, and is blocked only when all of them are. So a
subtree you are handed never contains a dead branch inside a live one, and a
blocked root means no data repair exists in scope for that focus.

The scope limit is deliberate: repairs edit the data graph, never the schema.
Widening a ``closed`` list or lowering a ``minCount`` would often be the right
fix in practice, and Shifty will not propose it.

See also
--------

- :doc:`../reference/repair` — the complete object model.
- :doc:`../explanation/repair-design` — repair as abduction, and the driver
  boundary.
- ``python/examples/repair.py`` and ``repair_interactive.py`` — worked drivers.
