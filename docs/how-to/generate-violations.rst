Generate repair benchmarks
==========================

.. warning::

   **Experimental.** ``shifty.experimental.vio`` is new and its API and
   output format may change in any release. It generates test cases. It does
   not repair graphs.

``shifty.experimental.vio`` injects SHACL violations with known ground-truth repairs
into a reference graph that conforms to its shapes. Each case contains a
broken graph, the edits that were injected, and the inverse edits that
restore the reference graph. A case can hold several interacting violations.
Repair systems can then be scored on whether they recover the intended graph,
not just on whether their output validates.

The method follows Lin et al., `Systematic Evaluation of Knowledge Graph
Repair with Large Language Models <https://arxiv.org/abs/2507.22419>`_, and
its multi-violation extension in `Selective Retrieval Routing for
Multi-Violation Knowledge Graph Repair <https://arxiv.org/abs/2611.07342>`_.
Shifty's satisfaction evidence supplies the expansion.
:doc:`../explanation/violation-generation` explains the framework and what
Shifty adds to it. The
`design notes <https://github.com/gtfierro/shifty/blob/main/development_docs/violation-generation.md>`_
cover the rewrite rules.

Three nouns
-----------

``Universe``
   Every way to break the reference graph, as an immutable sequence of
   injections. It is built from a data graph and a shapes graph. Building
   it does not run the expansion: draws stream it, and listing the universe
   stores it.
``Injection``
   One member: atomic edits that together make one ``(focus, statement)``
   pair fail. Its ``key``, the sorted net delta, identifies it.
``Case``
   One draw from a universe: up to ``k`` compatible injections, the broken
   graph, the injected delta ``Δ``, and the repair ``Δ⁻¹``.

.. code-block:: python

   from shifty.experimental import vio

   universe = vio.Universe(reference, shapes)  # data first, like validate()

   case = universe.draw(5, seed=0)
   case.graph_ntriples   # G_test = G_ref ⊕ Δ, as N-Triples
   case.graph()          # G_test, as an rdflib.Graph
   case.apply(delta)     # G_test ⊕ delta, as an rdflib.Graph
   case.delta            # Δ, a shifty.RepairDelta
   case.repair           # Δ⁻¹, a shifty.RepairDelta
   case.failures         # every failing pair in G_test, with its injections
   case.save("suite/case-000")

   # A suite: one pass over the expansion fills a reservoir per case.
   suite = universe.draws([(k, s) for k in (2, 5, 10) for s in range(10)])

   print(len(universe), "injections")  # expands once and caches the universe

The same ``(k, seed)`` draws the same case from the same universe, in every
process, whether the universe was streamed or cached. ``case.delta`` and
``case.repair`` are the deltas a
:class:`shifty.RepairSession` gates, so a repair system's output can be
compared with the ground truth directly.

Injections
----------

Each injection's ``operations`` are atomic edits:

``remove``
   Cut an edge that supports a value, such as the only ``ex:hasPoint`` that
   satisfies a ``sh:qualifiedMinCount``.
``substitute``
   Replace a value with one that fails, or satisfies, its qualifier, such as
   ``"RAT_01"`` → ``"RA"`` under ``sh:minLength 3``. Proposed terms are
   checked by the engine before use.
``add_value``
   Add a value along a path, such as a second ``qudt:hasQuantityKind`` under
   ``sh:maxCount 1``. New values follow the other universals on the same path
   when they can, so a cardinality violation stays one.
``add_forbidden``
   Add a predicate that a ``sh:closed`` shape disallows.

Under ``sh:not``, the generator makes the negated shape *hold*. For example,
``sh:not [ sh:path ex:deprecated ; sh:hasValue true ]`` is broken by adding
``ex:deprecated true``. Such an operation has ``polarity == "satisfy"``.

An injection also carries its ``focus``, its ``shape``, the ``predicates``
it edits, its ``deletes`` and ``adds``, and its edits as a ``delta``.

How a draw composes a case
--------------------------

``draw(k, seed)`` streams the expansion into a weighted reservoir of
``max(8k, 64)`` injections, then scans it in key order. It admits an
injection only if the injection's own edits make its target fail, and its
edits can be applied with those of the injections already chosen. Two
injections cannot be applied together when one deletes a triple the other
adds, when both replace one value differently, or when they need one node to
both fail and satisfy one constraint. Shared edits are fine, and so are
several injections on one property of one focus node. This is the paper's
composition rule. Beyond the paper, the draw then revalidates the combined
edits and replaces any injection whose targeted property no longer fails. Every
entry of ``case.failures`` is one the engine reports for ``G_test``. Because
one edit can fail several statements, there can be more failures than
injections, and several injections can share one failure. A failure with no
injections is a side effect.

Views: filters and weights
--------------------------

A universe never changes. ``where`` and ``weighted`` return views that share
its engine session and caches, so they are cheap, and they compose:

.. code-block:: python

   S223 = "http://data.ashrae.org/standard223#"
   zones = universe.where(shape=S223 + "Zone")
   cnx = universe.where(predicate=S223 + "cnx")
   mine = universe.where(lambda e: len(e.operations) == 1, focus=my_nodes)

   zones.weighted(kinds={"add_value": 0.2}).draw(10, seed=3)

``where`` is a hard filter. ``shape`` keeps the injections that break that
shape, ``predicate`` keeps those that edit it, and ``focus`` keeps those
aimed at that node. Each takes one IRI or several. A callable filters on
anything else. Filters run as the expansion streams, so a callable is called
once per injection on each pass.

``weighted`` changes how a draw samples the view. The reservoir keeps the
injections with the smallest keys ``-ln h(v) / w(v)``, where ``h`` is a
stable hash of the injection and the seed. With no weights ``w`` is 1, so a
draw is uniform and every predicate is treated alike. ``predicates`` adds
the geometric mean of the weights of the predicates an injection edits.
``kinds`` adds the geometric mean of the weights of its operation kinds.
When both are set, ``w`` is their product. A predicate or kind without an
entry weighs 1, so only the ratios matter. Weights favour injections but
never exclude them. ``weighted`` replaces the view's weights, and ``where``
keeps them.

On 223P models ``add_value`` dominates, because nearly every property carries
both ``sh:class`` and ``sh:maxCount``.

Generate a suite from the command line
--------------------------------------

.. code-block:: bash

   python -m shifty.experimental.vio --data reference.ttl --shapes shapes.ttl \
       --out suite/ -k 2 -k 5 -k 10 -n 10 --seed 0

This writes ten cases for each ``k``, drawn with seeds ``0`` to ``9``:

.. code-block:: text

   suite/
     reference.nt          the reference graph, normalized to N-Triples
     suite.json            operation mix of the injections, case list
     k05/case-003/
       graph.nt            G_test = G_ref ⊕ Δ
       repair.ru           Δ⁻¹ as a SPARQL 1.1 Update
       case.json           Δ, Δ⁻¹, each injection, and every failing
                           (focus, statement) pair in G_test

Pass ``--materialize`` when the model relies on SHACL-AF rules, as ASHRAE 223P
models do. The rules then run once, and the closure becomes the reference
graph. ``--shape`` and ``--predicate`` filter like ``where``, and both
repeat. ``--weights`` and ``--kind-weights`` take JSON files of predicate and
operation-kind weights.

``-j``/``--jobs N`` splits the cases across ``N`` worker processes. Each
worker builds its own universe and draws every ``N``-th case. A case does
not depend on the others, so the suite is the same for any ``N``. Each
worker holds a full universe in memory, about 550 MB for NIST-IBAL.

Three examples in ``python/examples/`` run each step on their own:

- ``vio_validate.py`` breaks a small graph, validates it, and repairs it.
- ``vio_suite.py`` narrows and weights the universe, then draws a suite.
- ``vio_223p.py`` draws a suite for a 223P model.

Bound the expansion
-------------------

``ExpansionLimits`` caps each choice point: values per count, candidate terms
per value, joint choices, nesting depth, and injections per target. The
defaults produce about 77,000 injections for NIST-IBAL (9,625 triples after
materialization). One pass over them takes about 1 second. A draw at
``k = 10`` takes about 1.5 seconds, including that pass, and ``draws`` of ten
cases at ``k = 5`` about 4 seconds. Thirty cases at ``k`` = 2, 5, and 10 take
about 12 seconds from the CLI, and about 5 seconds with ``--jobs 8``.

Composition dominates. Checking a candidate re-selects and evaluates its
target pair only. Checking a composed case finds the failing pairs with a
conformance pass over the whole graph, about 0.15 seconds for NIST-IBAL,
and builds evidence only for the new failures.

A streamed draw holds only its reservoirs. Listing the universe stores every
injection: for NIST-IBAL, peak memory rises from about 550 MB to about
800 MB.

Reproducibility
---------------

For a fixed data graph, shapes graph, and limits, the universe is identical
across runs and processes, and so is every draw. Injections are identified
by their net edit set, not by engine-internal ids, which can differ between
processes.

Parsers label anonymous blank nodes afresh on every parse. The universe
therefore relabels the reference graph's blank nodes canonically, and the
emitted files use those labels. Pass ``canonicalize_blank_nodes=False`` to keep
the input's labels, at the cost of reproducibility for graphs with anonymous
blank nodes.

Use it from Rust
----------------

The generator is implemented in Rust as ``shifty_experimental::vio``, with
the same three nouns. The Python module wraps it.

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
       // ...
   }

Limitations
-----------

- Relational constraints (``sh:equals``, ``sh:disjoint``, ``sh:lessThan``),
  SPARQL constraints, and ``sh:xone`` are not broken yet.
- New values come from the reference graph, from facet-derived literals, or
  from fresh IRIs named after the focus node's namespace. The generator does
  not invent realistic entity names.
- Paths are materialized along their shortest predicate word. Alternatives
  take the first branch, and stars take zero steps.
- Focus nodes without outgoing triples in the reference graph, such as
  vocabulary terms described by the shapes graph, are skipped. Pass
  ``include_external_foci=True`` to include them.
