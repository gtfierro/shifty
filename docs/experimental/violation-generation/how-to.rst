Generate repair test cases
==========================

.. warning::

   **Experimental.** ``shifty.experimental.vio`` may change its API and
   output format in any release. It generates test cases. It does not repair
   graphs.

A validator can tell you that a repaired graph conforms. It cannot tell you
that the repair was the right one: deleting the offending node also
conforms. To score a repair tool you need to know the intended answer.

``shifty.experimental.vio`` gets one by working backwards. It takes a graph
that conforms, breaks it with a few chosen edits, and records them. Undoing
those edits is a repair that is known to be correct.

:doc:`explanation` explains how the edits are found
and combined. This page shows how to use the module.

Break a graph
-------------

.. code-block:: python

   from shifty.experimental import vio

   universe = vio.Universe("model.ttl", "shapes.ttl")  # data first, like validate()
   case = universe.draw(3, seed=0)

A ``Universe`` is every way ``vio`` found to break the graph. Each way is an
``Injection``: a few edits that make one node fail one of its shapes.
``draw(k, seed)`` picks up to ``k`` injections that work together and
returns a ``Case``:

.. code-block:: python

   case.injections      # what was injected
   case.graph()         # the broken graph, as an rdflib.Graph
   case.graph_ntriples  # the same, as N-Triples
   case.delta           # the edits that broke it, a shifty.RepairDelta
   case.repair          # the edits that undo them
   case.failures        # the failing (node, shape) pairs, each with its injections
   case.save("case-000")  # graph.nt, repair.ru, case.json

To score a repair tool, apply its edits to the broken graph and compare the
result with the original:

.. code-block:: python

   candidate = case.apply(tool_delta)   # the broken graph with a delta applied
   intended = case.apply(case.repair)   # the original graph

The same ``(k, seed)`` always returns the same case, in every process.

The data and shapes can be paths, URLs, Turtle strings or bytes,
``rdflib.Graph`` objects, or lists of these, which are merged. The data must
conform. A node that already fails a shape is left alone and never reported
as a failure.

What an injection does
----------------------

An injection has ``operations``, each one edit of one kind:

``remove``
   Delete a triple that a constraint needs, such as a sensor's only label
   under ``sh:minCount 1``.
``substitute``
   Swap a value for one that fails, such as ``"degC"`` for ``"degC_x"``
   under ``sh:in``.
``add_value``
   Add a value, such as a second label under ``sh:maxCount 1``.
``add_forbidden``
   Add a predicate that a ``sh:closed`` shape does not allow.

Under ``sh:not`` the direction flips: the injection makes the negated shape
hold. ``sh:not [ sh:path ex:deprecated ; sh:hasValue true ]`` is broken by
adding ``ex:deprecated true``.

An injection also has the ``focus`` node it breaks, the ``shape``, the
``predicates`` it edits, and its ``deletes`` and ``adds``. Its ``key`` is
those edits as sorted text, and identifies it across runs.

A universe behaves like a list, so you can browse it:

.. code-block:: python

   len(universe)
   for injection in universe[:5]:
       print(injection.focus, injection.operations[0].detail)

A draw alone does not build this list. It samples the injections as they
are found and keeps only its sample. Listing the universe stores all of it.

Choose what gets broken
-----------------------

``where`` narrows a universe, and ``weighted`` tilts the odds. Both return a
new view and leave the universe unchanged, so they are cheap and they chain.

.. code-block:: python

   S223 = "http://data.ashrae.org/standard223#"

   zones = universe.where(shape=S223 + "Zone")       # break only this shape
   cnx = universe.where(predicate=S223 + "cnx")      # edit only this predicate
   mine = universe.where(focus=my_nodes)             # break only these nodes
   single = universe.where(lambda e: len(e.operations) == 1)

   zones.weighted(kinds={"add_value": 0.2}).draw(10, seed=3)

A filter excludes: a draw from ``zones`` can only pick injections that break
a zone. A weight never excludes. It makes an injection more or less likely
to be picked, relative to the others.

Without weights, every injection is equally likely. ``predicates`` weights
injections by the predicates they edit, and ``kinds`` by their operation
kinds. Anything without a weight counts as 1, so only ratios matter: a
weight of 50 on one predicate makes its injections 50 times as likely as
the rest.

Weighting kinds is useful on ASHRAE 223P models. Most properties there have
both ``sh:class`` and ``sh:maxCount``, so ``add_value`` makes up most of
the universe.

Generate a suite
----------------

``generate`` draws many cases and writes them to disk:

.. code-block:: python

   suite = vio.generate(
       "model.ttl", "shapes.ttl", "suite/",
       budgets=(2, 5, 10), cases=10, seed=0, jobs=4,
   )

This draws ten cases at each budget, with seeds 0 to 9:

.. code-block:: text

   suite/
     reference.nt        the original graph
     suite.json          the list of cases and the mix of operation kinds
     k05/case-003/
       graph.nt          the broken graph
       repair.ru         the repair, as a SPARQL update
       case.json         the edits, the injections, and the failures

It returns the contents of ``suite.json``. It takes the same inputs as
``Universe``, the filters of ``where`` (``shape``, ``predicate``,
``focus``), and the weights of ``weighted`` (``predicates``, ``kinds``).

``jobs=N`` runs ``N`` worker processes. Cases do not depend on each other,
so the suite is the same for any ``N``. Each worker builds its own
universe, so memory grows with ``N``.

The command line does the same:

.. code-block:: bash

   python -m shifty.experimental.vio --data model.ttl --shapes shapes.ttl \
       --out suite/ -k 2 -k 5 -k 10 -n 10 --seed 0 --jobs 4

``--shape`` and ``--predicate`` filter, and both repeat. ``--weights`` and
``--kind-weights`` take JSON files that map predicates or kinds to weights.

To draw a suite without writing it, use ``universe.draws([(k, seed), ...])``.
It returns the cases that the matching ``draw`` calls would, from one pass
over the universe.

Models that rely on rules
-------------------------

Some models conform only after SHACL-AF rules have run. ASHRAE 223P models
are an example. Pass ``materialize=True`` (``--materialize``): the rules run
once, and the inferred graph becomes the graph that gets broken. Edits may
then touch inferred triples.

What it costs
-------------

For NIST-IBAL, a 223P model of 9,625 triples after rules, the universe has
about 77,000 injections:

.. list-table::
   :header-rows: 1

   * - Step
     - Time
   * - One pass over the universe
     - 1 s
   * - One case at ``k = 10``, including the pass
     - 1.5 s
   * - Thirty cases at ``k`` = 2, 5, 10
     - 12 s
   * - The same with ``jobs=8``
     - 5 s

Most of the time goes to checking. Each candidate injection is tested on
its own node, which is cheap. Each finished case is validated in full,
which takes about 0.15 s here and grows with the graph.

A worker uses about 550 MB for this model. Listing the universe stores
every injection and raises that to about 800 MB.

``ExpansionLimits`` bounds the search, so a large graph does not produce an
unbounded universe. It caps how many alternatives, values, and replacement
terms are tried at each step, and how many injections one node and shape
can yield.

Reproducibility
---------------

The same data, shapes, limits, and ``(k, seed)`` give the same case on
every run and every machine. Two details make this hold:

- An injection is identified by its edits, not by internal ids.
- Parsers name anonymous blank nodes differently on every parse, so the
  universe renames them in a canonical way. The output files use those
  names. ``canonicalize_blank_nodes=False`` keeps the input's names, and
  gives up reproducibility for graphs with anonymous blank nodes.

Examples
--------

In ``python/examples/``:

- ``vio_validate.py`` breaks a small graph, validates it, and repairs it.
- ``vio_suite.py`` narrows and weights a universe, then draws a suite.
- ``vio_223p.py`` generates a suite for a 223P model.

Use it from Rust
----------------

The generator is written in Rust, as ``shifty_experimental::vio``. The
Python module wraps it, and the names match.

.. code-block:: rust

   use shifty_experimental::vio::{Options, Universe};

   let shapes = shifty_parse::load_turtle(shapes_ttl, None)?;
   let data = shifty_parse::load_turtle(data_ttl, None)?.graph;
   let universe = Universe::new(shapes, data, Options::default())?;
   let zones = universe.filter(|e| e.shape.as_deref() == Some(ZONE));
   let requests: Vec<(usize, u64)> = (0..10).map(|seed| (5, seed)).collect();
   for case in zones.draws(&requests)? {
       let graph = case.graph_ntriples();
       let repair = case.repair(); // a shifty_repair::GraphDelta
   }

Limitations
-----------

- ``sh:equals``, ``sh:disjoint``, ``sh:lessThan``, SPARQL constraints, and
  ``sh:xone`` are not broken yet.
- New values are taken from the graph, derived from the constraint (a
  string one character too short), or minted as fresh IRIs. They are not
  realistic names.
- A property path is broken along its shortest route only.
- Nodes with no outgoing triples, such as vocabulary terms that the shapes
  describe, are skipped. ``include_external_foci=True`` includes them.
