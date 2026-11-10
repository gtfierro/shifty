Break a graph and repair it
===========================

.. warning::

   **Experimental.** ``shifty.experimental.vio`` may change in any release.

This tutorial breaks a small graph on purpose, watches the validator catch
it, and repairs it with the edits that broke it. It takes a few minutes and
needs only ``pip install "pyshifty[rdflib]"``.

The idea is to start from a graph that already conforms. Break it with a few
recorded edits, and the inverse of those edits is a repair known to be
correct. That is what a repair tool is scored against.

The graph and the shapes
------------------------

The script holds both in strings. Every ``ex:Sensor`` must have one string
label, a unit that is ``"degC"`` or ``"degF"``, and at least one room that
really is an ``ex:Room``.

.. literalinclude:: ../../examples/violation-generation/break_a_graph.py
   :language: python
   :lines: 23-42

The script
----------

Save it as ``break_a_graph.py``, together with the shapes and data above, and
run it. The output follows; each numbered block of output is a step below.

.. literalinclude:: ../../examples/violation-generation/break_a_graph.py
   :language: python
   :lines: 15-21,45-

.. program-output:: python break_a_graph.py
   :cwd: ../../examples/violation-generation

1. Validate the original data
-----------------------------

``conforms: True``. This matters: ``vio`` needs a graph that conforms. A
node that already fails a shape is left alone and never counted as an
injected failure.

2. Draw a case
--------------

.. code-block:: python

   case = vio.Universe(DATA, SHAPES).draw(k, seed=seed)

A ``Universe`` is every way ``vio`` found to break the graph, each one an
``Injection``: a few edits that make one node fail one of its shapes.
``draw(2, seed=0)`` picks up to two that work together, which is a ``Case``.
Here it removed the type of ``ex:r1``, so ``ex:s1`` points at a room that is
no longer a room, and gave ``ex:s2`` a unit that is not ``"degC"`` or
``"degF"``.

The same ``(k, seed)`` gives the same case every time, in every process, so
a benchmark built from it can be rebuilt. Pass another seed to see other
cases.

3. Read the edits
-----------------

``case.delta`` holds the edits that broke the graph: triples deleted
(``-``) and added (``+``). ``case.graph()`` is the broken graph as an
``rdflib.Graph``.

4. Validate the broken graph
----------------------------

The validator now reports both injected violations, and ``case.failures``
lists the focus nodes the case expects to fail. The two lists agree, which
is the check that the case does what it says.

5. Apply the repair
-------------------

``case.repair`` is the inverse of ``case.delta``. ``case.apply(case.repair)``
applies it to the broken graph, and the result conforms again. To score a
repair tool, apply its edits instead and compare:

.. code-block:: python

   candidate = case.apply(tool_delta)
   intended = case.apply(case.repair)  # the original graph

Next
----

- :doc:`how-to` covers choosing what gets broken, writing a suite of cases
  to disk, and running in parallel.
- :doc:`explanation` describes how the edits are found and combined.
- ``python/examples/`` in the repository has ``vio_suite.py`` and
  ``vio_223p.py``, which generate whole suites.
