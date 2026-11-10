Break a graph and repair it
===========================

.. include:: experimental.inc

In this tutorial you break a small, valid graph on purpose, watch the
validator catch it, and then fix it with the edits that broke it. You need
``pip install "pyshifty[rdflib]"``.

The graph and the shapes
------------------------

Two sensors, each in a room. The shapes say every sensor needs one string
label, a unit of ``"degC"`` or ``"degF"``, and a room that is an
``ex:Room``.

.. literalinclude:: ../../examples/violation-generation/break_a_graph.py
   :language: python
   :lines: 23-42

The script
----------

Save the shapes and data above together with this code as
``break_a_graph.py``:

.. literalinclude:: ../../examples/violation-generation/break_a_graph.py
   :language: python
   :lines: 15-21,45-

Run it:

.. program-output:: python break_a_graph.py
   :cwd: ../../examples/violation-generation

The steps below walk through the output.

1. Check that the graph conforms
--------------------------------

``vio`` only breaks graphs that conform. If a node already fails a shape,
``vio`` leaves it alone.

2. Draw a case
--------------

.. code-block:: python

   case = vio.Universe(DATA, SHAPES).draw(k, seed=seed)

A ``Universe`` holds every way ``vio`` found to break the graph. Each way
is an *injection*: a few edits that make one node fail one shape.

``draw(2, seed=0)`` picks up to two injections that can be applied
together. Here, ``ex:r1`` loses its type, so ``ex:s1`` now points at
something that is not a room. ``ex:s2`` gets a unit that is not allowed.

The same seed gives the same case every time. Try another seed to see a
different case.

3. Read the edits
-----------------

``case.delta`` holds the edits: triples deleted (``-``) and added
(``+``). ``case.graph()`` returns the broken graph.

4. Validate the broken graph
----------------------------

The validator reports two violations, and ``case.failures`` expects the
same two nodes to fail. The case does what it says.

5. Repair it
------------

``case.repair`` undoes ``case.delta``. Apply it, and the graph conforms
again.

To score a repair tool, apply the tool's edits instead, and compare:

.. code-block:: python

   candidate = case.apply(tool_delta)
   intended = case.apply(case.repair)  # the original graph

Next steps
----------

- :doc:`how-to`: build a suite from your own model.
- :doc:`explanation`: how injections are found and combined.
- :doc:`reference`: every class and option.
