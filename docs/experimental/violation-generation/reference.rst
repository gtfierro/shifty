Violation generation reference
==============================

.. include:: experimental.inc

Everything below is in ``shifty.experimental.vio``.

- Nodes and triples are N-Triples terms, such as ``<http://ex/s1>`` and
  ``"degC"``.
- Arguments that take IRIs accept them bare or in angle brackets.

``Universe``
------------

.. code-block:: python

   vio.Universe(data_graph, shacl_graph, *, infer=True,
                include_external_foci=False, canonicalize_blank_nodes=True,
                graph_mode="union", on_unsupported="error", base=None,
                limits=None)

Every way to break a conforming graph, as a sequence of ``Injection``
objects. Creating one compiles the shapes and validates the graph. Injections
are found later, when you draw or list.

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Parameter
     - Meaning
   * - ``data_graph``, ``shacl_graph``
     - The reference graph and the shapes, in any form ``shifty.validate``
       accepts. Lists are merged.
   * - ``infer``
     - Run the shapes' SHACL-AF rules on every check. Only asserted
       triples are edited, and an edit the rules would undo is dropped.
       ``False`` ignores the rules.
   * - ``include_external_foci``
     - Also break nodes with no outgoing triples, such as vocabulary terms
       the shapes describe.
   * - ``canonicalize_blank_nodes``
     - Rename blank nodes the same way every run, so draws reproduce. Turn
       off to keep the input's names.
   * - ``graph_mode``, ``on_unsupported``, ``base``
     - As in ``shifty.validate``; see :doc:`/reference/python`.
   * - ``limits``
     - An ``ExpansionLimits``.

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``len(u)``, ``u[i]``, ``u[a:b]``, ``for e in u``, ``e in u``
     - Find every injection and keep them in memory. Later draws reuse
       them.
   * - ``where(fn=None, /, *, shape=None, predicate=None, focus=None)``
     - The injections that break ``shape``, edit ``predicate``, break
       ``focus``, and pass ``fn(injection)``. Each takes one IRI or several.
       Filters compose; weights carry over.
   * - ``weighted(predicates=None, kinds=None)``
     - The same injections, drawn with these weights. Keys are predicate
       IRIs and operation kinds; missing keys weigh 1. An injection weighs
       the geometric mean of its predicates' weights times the geometric
       mean of its kinds' weights.
   * - ``draw(k, *, seed=0)``
     - A ``Case`` of up to ``k`` injections. The same ``k`` and seed
       always give the same case.
   * - ``draws(requests)``
     - One ``Case`` per ``(k, seed)`` in ``requests``, from one pass.
   * - ``baseline_failures``
     - The ``(focus, statement)`` pairs that already fail. They are never
       broken.
   * - ``reference_ntriples``
     - The original graph as sorted N-Triples. It holds only asserted
       triples.
   * - ``predicate_weights``, ``kind_weights``
     - The weights of this view.

``Injection`` and ``Operation``
-------------------------------

An ``Injection`` is one way to break the graph: operations that together
make one node fail one shape.

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Attribute
     - Meaning
   * - ``focus``
     - The node it breaks.
   * - ``statement``, ``shape``
     - The shape statement it breaks, and its IRI if it has one.
   * - ``operations``
     - Its ``Operation`` objects.
   * - ``deletes``, ``adds``, ``delta``
     - Its edits, as sets of triples and as a ``shifty.RepairDelta``.
   * - ``predicates``, ``kinds``
     - The predicates it edits, and its operations' kinds.
   * - ``key``
     - Its edits as sorted ``- s p o .`` and ``+ s p o .`` lines. It
       identifies the injection across runs.

An ``Operation`` has ``kind``, ``node``, ``polarity``, ``deletes``,
``adds``, and ``detail``, a description for people. ``polarity`` is
``"violate"``, or ``"satisfy"`` under ``sh:not``, where the edit makes the
negated shape hold.

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Kind
     - Edit
   * - ``remove``
     - Delete a triple a constraint needs, such as a sensor's only label
       under ``sh:minCount 1``.
   * - ``substitute``
     - Replace a value with one that fails, such as ``"degC"`` with a value
       outside ``sh:in``.
   * - ``add_value``
     - Add a value, such as a second label under ``sh:maxCount 1``.
   * - ``add_forbidden``
     - Add a predicate a ``sh:closed`` shape does not allow.

``Case``
--------

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``k``, ``seed``
     - The draw that made it.
   * - ``injections``
     - The injections, in the order they were admitted. At most ``k``.
   * - ``failures``
     - Every failing ``(focus, statement)`` pair of the broken graph, as
       ``CaseFailure`` objects with ``focus``, ``statement``, ``shape``, and
       ``injections``. Empty ``injections`` marks a side effect.
   * - ``delta``, ``repair``
     - The edits that broke the graph, and their inverse, as
       ``shifty.RepairDelta``.
   * - ``graph()``, ``graph_ntriples``
     - The broken graph, as an ``rdflib.Graph`` and as sorted N-Triples.
   * - ``apply(delta)``
     - The broken graph with ``delta`` applied, deletes first, as an
       ``rdflib.Graph``. Blank-node labels in ``delta`` name the nodes of
       ``graph_ntriples``.
   * - ``repair_sparql``
     - ``repair`` as a SPARQL 1.1 Update. It uses ``DELETE WHERE`` when the
       repair deletes a blank-node triple.
   * - ``to_dict()``
     - The ``case.json`` manifest.
   * - ``save(directory)``
     - Write ``graph.nt``, ``repair.ru``, and ``case.json``.

.. _vio-limits:

``ExpansionLimits``
-------------------

Bounds on the search for injections. Each caps one way it branches, so a
large graph does not give an unbounded universe.

.. list-table::
   :widths: 30 15 55
   :header-rows: 1

   * - Field
     - Default
     - Caps
   * - ``max_alternatives``
     - 24
     - Alternatives kept at any one choice.
   * - ``max_values``
     - 6
     - Values sampled per count or universal constraint.
   * - ``max_candidates``
     - 3
     - Replacement or new terms tried per value.
   * - ``max_combinations``
     - 12
     - Joint choices when several values must change together.
   * - ``max_depth``
     - 12
     - Nesting through ``sh:node`` and ``sh:qualifiedValueShape``.
   * - ``max_per_target``
     - 64
     - Injections per node and shape.
   * - ``max_injections``
     - 200,000
     - Injections in total.

``generate``
------------

.. code-block:: python

   vio.generate(data_graph, shacl_graph, out, *, budgets=(5,), cases=10,
                seed=0, jobs=1, shape=None, predicate=None, focus=None,
                predicates=None, kinds=None, infer=True)

Draws ``cases`` cases for each ``k`` in ``budgets``, writes them under
``out``, and returns the contents of ``suite.json``.

- Seeds run ``seed``, ``seed + 1``, and so on.
- ``shape``, ``predicate``, and ``focus`` filter, as in ``where``.
- ``predicates`` and ``kinds`` weight, as in ``weighted``.
- ``infer`` is as in ``Universe``.
- ``jobs`` splits the cases across worker processes. The suite is the same
  for any value.

.. _vio-output-files:

Output files
------------

.. code-block:: text

   out/
     reference.nt        the reference graph
     suite.json          the summary
     k05/case-003/       k = 5, the fourth case (seed + 3)
       graph.nt          the broken graph
       repair.ru         the repair, as a SPARQL update
       case.json         the manifest

``suite.json`` holds:

- ``baseline_failures``: how many pairs already failed.
- ``operation_kinds``: how many operations of each kind the suite injected.
- ``cases``: one row per case, with ``path``, ``k``, ``seed``,
  ``injected``, and ``failures``.

``case.json`` holds:

- ``k`` and ``seed``.
- ``delta`` and ``repair``.
- ``injections``, each with ``focus``, ``statement``, ``shape``,
  ``predicates``, ``operations``, and ``key``.
- ``failures``, each with ``focus``, ``statement``, ``shape``, and the
  indices of its ``injections``.

Command line
------------

.. code-block:: bash

   python -m shifty.experimental.vio --data DATA --shapes SHAPES --out OUT [options]

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Option
     - Meaning
   * - ``--data``, ``--shapes``
     - The reference graph and the shapes, as paths or URLs. Required.
   * - ``--out``
     - The output directory. Required.
   * - ``-k K``
     - Injections per case. Repeat for several budgets. Default 5.
   * - ``-n``, ``--cases``
     - Cases per budget. Default 10.
   * - ``--seed``
     - The first case's seed; later cases count up. Default 0.
   * - ``--shape IRI``, ``--predicate IRI``
     - Break only this shape, edit only this predicate. Both repeat.
   * - ``--weights FILE``
     - A JSON file mapping predicate IRIs to weights.
   * - ``--kind-weights FILE``
     - A JSON file mapping operation kinds to weights.
   * - ``--infer``, ``--no-infer``
     - Run the shapes' rules on every validation, or not. On by default.
   * - ``-j``, ``--jobs``
     - Worker processes. Default 1.

It exits with status 1 on an error. It warns when the original graph
already has failures.

Rust
----

``shifty_experimental::vio`` exports ``Universe``, ``Options``,
``Weights``, ``ExpansionLimits``, ``Injection``, ``Operation``,
``OperationKind``, ``Polarity``, ``Case``, ``CaseFailure``, and
``Triple``. The Python module wraps them, and the names match, with two
exceptions:

- ``filter`` takes a closure, in place of ``where``.
- ``weighted`` takes a ``Weights``.

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
- A value that only an inferred triple supports cannot be cut. That would
  take editing the rule's premises, which the search does not trace.
- When two injections break the same property, the check confirms that the
  property fails, not that both injections are needed.
