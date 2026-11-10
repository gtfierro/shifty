Build a repair benchmark
========================

.. include:: experimental.inc

This page shows how to turn a conforming model into a suite of repair test
cases. It assumes the :doc:`tutorial`: what a ``Universe``, an injection, and
a ``Case`` are. Every parameter is listed in the :doc:`reference`.

Load the model and the shapes
-----------------------------

.. code-block:: python

   from shifty.experimental import vio

   universe = vio.Universe("model.ttl", "shapes.ttl")  # data first, like validate()

The data and the shapes take anything ``shifty.validate`` takes: paths,
URLs, Turtle strings or bytes, ``rdflib.Graph`` objects, or lists of these,
which are merged.

The data should conform. A node that already fails a shape is left alone
and never reported as a failure; ``universe.baseline_failures`` lists those
pairs, and the command line warns about them. If the list is long, fix the
model or the shapes first, or most of the model is out of reach.

Models that rely on rules
~~~~~~~~~~~~~~~~~~~~~~~~~

Some models conform only after their SHACL-AF rules have run. ASHRAE 223P
models are an example. Nothing extra is needed: like ``shifty.validate``,
``vio`` runs the shapes' rules on every validation by default.

Edits touch only the asserted triples, and every check re-runs the rules.
An injection that the rules would undo, by inferring back what it removed,
fails its check and never reaches a case. The broken graph is the asserted
model with the edits applied, which is what a repair tool would be given.

``infer=False`` (``--no-infer``) ignores the rules, for benchmarks whose
graphs are validated without them.

Rules make drawing slower, since every check runs them: about four times
slower on NIST-IBAL.

Choose what gets broken
-----------------------

``where`` keeps only some injections, and ``weighted`` changes how likely
each one is. Both return a new view and leave the universe unchanged, so
they are cheap and they chain.

.. code-block:: python

   S223 = "http://data.ashrae.org/standard223#"

   zones = universe.where(shape=S223 + "Zone")      # break only this shape
   cnx = universe.where(predicate=S223 + "cnx")     # edit only this predicate
   mine = universe.where(focus=my_nodes)            # break only these nodes
   single = universe.where(lambda e: len(e.operations) == 1)

   zones.weighted(kinds={"add_value": 0.2}).draw(10, seed=3)

Use a filter to rule injections out, and a weight to make them rarer or
more common. A weight never excludes anything. Weights are relative, and
anything without one counts as 1: a weight of 50 on one predicate makes its
injections 50 times as likely as the rest.

To see what you are choosing from, list the universe:

.. code-block:: python

   from collections import Counter

   print(len(zones))
   print(Counter(kind for e in zones for kind in e.kinds))
   for e in zones[:5]:
       print(e.focus, e.operations[0].detail)

Listing stores every injection, which a draw alone does not, so on a large
model it costs memory.

On 223P models, weight the operation kinds. Most properties there have both
``sh:class`` and ``sh:maxCount``, so ``add_value`` makes up most of the
universe, and an unweighted suite is mostly extra values.

Write a suite
-------------

``generate`` draws many cases and writes them to disk:

.. code-block:: python

   suite = vio.generate(
       "model.ttl", "shapes.ttl", "suite/",
       budgets=(2, 5, 10), cases=10, seed=0, jobs=4,
   )

This draws ten cases at each budget ``k``, with seeds 0 to 9, and writes
the original graph as ``suite/reference.nt``, one directory per case under
``suite/k05/case-003/`` and so on, and a summary as ``suite/suite.json``.
The :ref:`file formats <vio-output-files>` are in the reference.

``generate`` takes the filters of ``where`` (``shape``, ``predicate``,
``focus``) and the weights of ``weighted`` (``predicates``, ``kinds``) as
keyword arguments. The command line does the same:

.. code-block:: bash

   python -m shifty.experimental.vio --data model.ttl --shapes shapes.ttl \
       --out suite/ -k 2 -k 5 -k 10 -n 10 --seed 0 --jobs 4

To keep the cases in memory instead, draw them in one pass from a view you
have built:

.. code-block:: python

   cases = zones.draws([(k, s) for k in (2, 5, 10) for s in range(10)])
   for case in cases:
       case.save(f"suite/k{case.k:02}/case-{case.seed:03}")

Run in parallel
~~~~~~~~~~~~~~~

``jobs=N`` (``--jobs N``) splits the cases across ``N`` worker processes.
The suite is the same for any ``N``, since cases do not depend on each
other. Each worker builds its own universe, so memory grows with ``N``.

For NIST-IBAL, a 223P model of 6,016 asserted triples (9,625 after rules),
with a universe of about 75,000 injections:

.. list-table::
   :header-rows: 1

   * - Step
     - Time
   * - One case at ``k = 10``
     - 4 s
   * - Thirty cases at ``k`` = 2, 5, 10
     - 41 s
   * - The same with ``jobs=8``
     - 11 s

A worker uses about 500 MB for this model, and about 760 MB if it lists the
universe. Most of the time goes to checking candidates and validating each
finished case, with the rules run each time, which grows with the graph. If the universe is too large, lower the
:ref:`expansion limits <vio-limits>`.

Score a repair tool
-------------------

Give the tool the broken graph, ``graph.nt`` or ``case.graph()``. Apply the
edits it proposes, as a ``shifty.RepairDelta``, and compare the result with
the original:

.. code-block:: python

   candidate = case.apply(tool_delta)  # the broken graph with the tool's edits
   intended = case.apply(case.repair)  # the original graph

``case.failures`` says which nodes and shapes the tool should have fixed,
and which injections caused each failure, so a score can count failures
fixed as well as triples matched. A tool that writes SPARQL can be compared
against ``repair.ru``, the same repair as a SPARQL update.

A repair other than the recorded one can also conform. Check the candidate
with ``shifty.validate`` and treat conformance and agreement with the
intended graph as separate scores.
