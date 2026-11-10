Build a repair benchmark
========================

.. include:: experimental.inc

This guide takes you from your own model to a suite of test cases, and
then to a score for a repair tool. It assumes you have done the
:doc:`tutorial`.

1. Start from a conforming model
--------------------------------

.. code-block:: python

   from shifty.experimental import vio

   universe = vio.Universe("model.ttl", "shapes.ttl")

The inputs can be paths, URLs, Turtle strings, ``rdflib.Graph`` objects, or
lists of these.

The model should conform, because ``vio`` only breaks nodes that pass. Check
what is already failing:

.. code-block:: python

   len(universe.baseline_failures)

Those nodes are out of reach. If there are many, fix the model or the
shapes first.

Models that use rules
~~~~~~~~~~~~~~~~~~~~~

Some models conform only after their SHACL-AF rules run, ASHRAE 223P models
among them. You don't need to do anything for these. ``vio`` runs the rules
on every check, as ``shifty.validate`` does.

It also only edits the triples you wrote, never inferred ones. If an edit
would be undone by a rule, ``vio`` drops it. The broken graph is your model
with the edits applied.

To ignore the rules, pass ``infer=False`` (``--no-infer``).

2. Choose what gets broken
--------------------------

A benchmark usually tests something specific: one kind of equipment, one
property, one kind of mistake. Two tools narrow the universe to that:

- ``where`` filters. Injections it drops can never be drawn.
- ``weighted`` changes how often injections are drawn, without dropping
  any.

.. code-block:: python

   S223 = "http://data.ashrae.org/standard223#"

   zones = universe.where(shape=S223 + "Zone")      # only break zones
   cnx = universe.where(predicate=S223 + "cnx")     # only edit cnx
   mine = universe.where(focus=my_nodes)            # only break these nodes
   simple = universe.where(lambda e: len(e.operations) == 1)

   zones.weighted(kinds={"add_value": 0.2})         # fewer extra values

Both return a new view. They are cheap and they chain.

Weights are relative, and anything unweighted counts as 1. A weight of 50
on a predicate makes its injections 50 times as likely as the rest.

To see what you are choosing from, list the universe:

.. code-block:: python

   from collections import Counter

   len(zones)
   Counter(kind for e in zones for kind in e.kinds)

On 223P models, most injections add an extra value, because most
properties have a ``sh:maxCount``. Weight ``add_value`` down to get a mix.

Listing keeps every injection in memory, so on a large model it costs
memory. Drawing doesn't.

3. Write a suite
----------------

``generate`` draws many cases and writes them to disk:

.. code-block:: python

   vio.generate(
       "model.ttl", "shapes.ttl", "suite/",
       budgets=(2, 5, 10), cases=10, jobs=4,
   )

This writes ten cases with up to 2 injections, ten with up to 5, and ten
with up to 10:

.. code-block:: text

   suite/
     reference.nt          the original graph
     suite.json            a summary of every case
     k05/case-003/
       graph.nt            the broken graph
       repair.ru           the repair, as a SPARQL update
       case.json           the edits, injections, and failures

``generate`` takes the same filters and weights as ``where`` and
``weighted``. The command line does the same:

.. code-block:: bash

   python -m shifty.experimental.vio --data model.ttl --shapes shapes.ttl \
       --out suite/ -k 2 -k 5 -k 10 -n 10 --jobs 4

To build a suite from a view, use ``draws``. It returns the cases instead
of writing them:

.. code-block:: python

   cases = zones.draws([(k, seed) for k in (2, 5, 10) for seed in range(10)])

How long it takes
~~~~~~~~~~~~~~~~~

``jobs`` splits the cases across worker processes. The suite comes out the
same for any number of jobs.

On NIST-IBAL, a 223P model of about 6,000 triples:

.. list-table::
   :header-rows: 1

   * - Work
     - Time
   * - One case, up to 10 injections
     - 4 s
   * - 30 cases
     - 41 s
   * - 30 cases, ``jobs=8``
     - 11 s

Each worker uses about 500 MB. Most of the time goes to running the rules
on each check. If the universe is too large to handle, lower the
:ref:`expansion limits <vio-limits>`.

4. Score a repair tool
----------------------

Give the tool ``graph.nt``. Apply the edits it proposes, and compare the
result with the original:

.. code-block:: python

   candidate = case.apply(tool_delta)
   intended = case.apply(case.repair)

Score two things separately:

- **Conformance.** Validate ``candidate``. A repair other than the intended
  one can still conform.
- **Agreement.** Compare ``candidate`` with ``intended``.
  ``case.failures`` lists each failure and the injections behind it, so
  you can also count failures fixed.

A tool that writes SPARQL can be compared against ``repair.ru``.
