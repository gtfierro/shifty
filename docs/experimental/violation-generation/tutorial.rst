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
``ex:Room``:

.. literalinclude:: ../../examples/violation-generation/graph.py
   :language: python
   :start-after: """The graph and shapes for the violation-generation tutorial."""

Each property shape has an IRI (``ex:label``, ``ex:unit``, ``ex:room``), so
the validation reports below can name them.

1. Check that the graph conforms
--------------------------------

``vio`` only breaks graphs that conform. If a node already fails a shape,
``vio`` leaves it alone. So check first:

.. literalinclude:: ../../examples/violation-generation/step1_validate.py
   :language: python
   :start-after: # ---

.. program-output:: python step1_validate.py
   :cwd: ../../examples/violation-generation

2. Draw a case
--------------

A ``Universe`` holds every way ``vio`` found to break the graph. Each way
is an *injection*: a few edits that make one node fail one shape.

``draw(2, seed=0)`` picks up to two injections that can be applied
together:

.. literalinclude:: ../../examples/violation-generation/step2_draw.py
   :language: python
   :start-after: # ---

.. program-output:: python step2_draw.py
   :cwd: ../../examples/violation-generation

``ex:r1`` loses its type, so ``ex:s1`` now points at something that is not
a room. ``ex:s2`` gets a unit that is not allowed.

The same seed gives the same case every time. Try another seed to see a
different case.

3. Read the edits
-----------------

``case.delta`` holds the edits, as triples to delete and to add:

.. literalinclude:: ../../examples/violation-generation/step3_edits.py
   :language: python
   :start-after: # ---

.. program-output:: python step3_edits.py
   :cwd: ../../examples/violation-generation

4. Validate the broken graph
----------------------------

``case.graph()`` is the original graph with those edits applied. Validate
it:

.. literalinclude:: ../../examples/violation-generation/step4_broken.py
   :language: python
   :start-after: # ---

.. program-output:: python step4_broken.py
   :cwd: ../../examples/violation-generation

The case already knows which nodes should fail. Compare
``case.failures``:

.. literalinclude:: ../../examples/violation-generation/step4_failures.py
   :language: python
   :start-after: # ---

.. program-output:: python step4_failures.py
   :cwd: ../../examples/violation-generation

The two agree. Every failure a case lists has been confirmed this way.

5. Repair it
------------

``case.repair`` undoes ``case.delta``. ``case.apply`` applies a set of
edits to the broken graph:

.. literalinclude:: ../../examples/violation-generation/step5_repair.py
   :language: python
   :start-after: # ---

.. program-output:: python step5_repair.py
   :cwd: ../../examples/violation-generation

The graph conforms again.

To score a repair tool, apply the tool's edits instead of ``case.repair``,
and compare the result with the original:

.. code-block:: python

   candidate = case.apply(tool_delta)
   intended = case.apply(case.repair)

Next steps
----------

- :doc:`how-to`: build a suite from your own model.
- :doc:`explanation`: how injections are found and combined.
- :doc:`reference`: every class and option.
