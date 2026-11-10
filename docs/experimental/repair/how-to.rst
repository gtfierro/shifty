Repair a graph
==============

.. include:: experimental.inc

This guide walks the repair loop: find what fails, fill in a repair, check
it, and apply it. Repair starts from failure evidence, which
:doc:`../../tutorials/explaining-a-failure` introduces.

Look at the repair space
------------------------

Before you write a driver, see what Shifty would offer. The CLI prints it:

.. code-block:: bash

   shifty repair --shapes shapes.ttl --data data.ttl

.. program-output:: shifty repair --shapes shapes.ttl --data data.ttl
   :cwd: ../../examples/quick-start

``--stage`` picks what to print:

- ``witness``: why each focus node fails.
- ``tree`` (the default): the repair template.
- ``solve``: one concrete edit, found by the built-in driver.

Add ``--format json`` for machine-readable output, and ``--no-infer`` to skip
SHACL-AF rules.

To write the repaired graph, add ``--apply``. It prints N-Triples to stdout.
The built-in driver fills each hole with any term in the graph that fits, so
it will happily give a person someone else's email address. Use it to
inspect, and write your own driver when the values matter.

Find what fails
---------------

A session holds the shapes and the data. ``witnesses()`` lists every failure:

.. code-block:: python

   from shifty.experimental import repair

   session = repair.RepairSession(shapes, data)

   for failure in session.witnesses():
       print(failure.focus, failure.shape_name)
       print(failure.explain())

There is one ``Failure`` per failing focus node and statement. An empty list
means the graph conforms. ``witnesses_for(shape_iri)`` limits it to one
shape.

Fill in a template
------------------

A failure's repair tree is a template. It has **choices**, where there is
more than one way to fix it, and **holes**, where a value is needed.

.. code-block:: python

   tree = failure.repair_tree()
   print(tree.explain())

You fill it with a ``RepairPlan``, then instantiate the tree to see what is
still open:

.. code-block:: python

   plan = repair.RepairPlan()
   choices = {choice.node_id: choice for choice in tree.choices()}

   instance = tree.instantiate(plan)
   while not instance.is_complete:
       for node_id in instance.open_choices:
           choice = choices[node_id]
           if choice.kind == repair.ChoiceKind.Repeat:
               plan.count(node_id, choice.min or 0)
           else:
               plan.choose(node_id, choose_branch(choice))
       instance = tree.instantiate(plan)
       for hole in instance.open_holes:
           plan.bind(hole.id, choose_a_term(hole))
       instance = tree.instantiate(plan)

``choose_branch`` and ``choose_a_term`` are your policy. Resolve the choices
before binding holes: a ``Repeat`` count copies its body, and each copy gets
new hole ids.

Terms are N-Triples: ``'"Bob"'`` for a string, ``'<http://example.org/bob>'``
for an IRI.

Pick a value for a hole
-----------------------

Each hole says what a legal value looks like. ``candidates()`` lists terms
already in the graph that fit:

.. code-block:: python

   for hole in tree.holes():
       print(hole.id, hole.constraint)
       print(hole.candidates(limit=8))

Use it when reusing a node is right. Your binding doesn't have to come from
it, and for a hole that wants a new node, it is the wrong tool.

Some holes need a value that conforms to another shape:

.. code-block:: python

   if hole.conforms_to is not None:
       print(session.describe_shape(hole.conforms_to))
       subtree = session.repair_node_against(candidate, hole.conforms_to)

``describe_shape`` shows the constraints the value must meet.
``repair_node_against`` returns the edits that would make ``candidate``
conform, or ``None`` if it already does.

Propose your own edit
---------------------

Sometimes your driver knows the answer as a subgraph, such as a new node
with its type and properties. Build the edit from Turtle or an
``rdflib.Graph``:

.. code-block:: python

   import shifty

   delta = shifty.delta_from_graph(add=my_turtle_patch)

It is checked and applied like any other edit. Deletes apply before adds, so
a triple in both is added.

Check it
--------

An edit that fixes one node can break another. ``gate`` validates the whole
patched graph without applying anything:

.. code-block:: python

   outcome = session.gate(instance.delta)
   outcome.fixed         # failures it removes
   outcome.introduced    # new failures it causes
   outcome.remaining     # old failures it leaves
   outcome.is_progress   # fixed something and broke nothing

Apply it
--------

``advance`` returns a new session over the patched graph. It reuses the
compiled shapes and doesn't re-run inference.

.. code-block:: python

   session = session.advance(instance.delta)

The whole driver is a loop:

.. code-block:: python

   while True:
       failures = session.witnesses()
       if not failures:
           break                              # conforms
       tree = failures[0].repair_tree()       # which failure first: your call
       ...                                    # fill the template
       outcome = session.gate(instance.delta)
       if not outcome.is_progress:
           break                              # give up, or choose again
       session = session.advance(instance.delta)

``session.to_graph()`` returns the current graph as an ``rdflib.Graph``.
``session.apply(delta)`` returns a patched graph without advancing.

When there is nothing to offer
------------------------------

Some failures have no data repair. The tree says so:

.. code-block:: python

   if tree.is_blocked:
       print(tree.explain())

:doc:`explanation` lists the causes. A blocked branch never hides inside a
live one: an ``All`` with a blocked child is blocked, and an ``Any`` drops
its blocked branches.

See also
--------

- :doc:`reference`: the full object model.
- ``python/examples/repair.py`` and ``repair_interactive.py``: complete
  drivers.
