CLI reference
=============

.. list-table::
   :widths: 20 80

   * - Binary
     - ``shifty`` (crate ``shifty-cli``)
   * - Stability
     - Stable
   * - Related
     - :doc:`../install`, :doc:`../how-to/validate`,
       :doc:`feature-support`

.. code-block:: text

   shifty <COMMAND>

   version   Print the shifty CLI version
   inspect   Show a layer's view of a shapes graph
   validate  Validate a data graph against a shapes graph
   infer     Run SHACL-AF rule inference (forward chaining to a fixpoint)
   repair    Show symbolic-repair structures for a data graph's violations

Common conventions
------------------

``--shapes`` and ``--data`` accept local paths or ``http(s)`` URLs. Both are
repeatable, and multiple sources are merged into one graph before anything
else.

``--data`` defaults to ``--shapes`` when omitted, which makes the single graph
serve as both. When both are given, the schema is compiled **only** from
``--shapes``; SHACL vocabulary in the data graph is ignored. See
:doc:`../explanation/shapes-and-data`.

``--base`` sets the base IRI used while parsing relative IRIs.

Invalid shapes diagnostics, such as malformed SPARQL or an unresolved query
prefix, make ``validate``, ``infer``, and ``repair`` exit with an error. They
are not treated as unsupported features and cannot silently remove a constraint
or rule. ``inspect`` remains diagnostic-oriented and shows lowering output.

``shifty validate``
-------------------

.. code-block:: bash

   shifty validate --shapes <SHAPES> [--data <DATA>] [OPTIONS]

.. list-table::
   :widths: 34 66
   :header-rows: 1

   * - Flag
     - Meaning
   * - ``--shapes <SHAPES>``
     - Shapes file(s) or URL(s). Repeatable. Required.
   * - ``--data <DATA>``
     - Data file(s) or URL(s). Repeatable. Defaults to ``--shapes``.
   * - ``--base <BASE>``
     - Override the default base IRI. Local files default to their own absolute
       file URI; URLs default to the retrieval URL. A document's base declaration
       can override that default.
   * - ``--format <FORMAT>``
     - ``text`` (default) or ``json``.
   * - ``--report``
     - Emit a W3C ``sh:ValidationReport`` graph as Turtle instead of the
       summary.
   * - ``--no-infer``
     - Skip SHACL-AF rule inference before validating.
   * - ``--graph-mode <MODE>``
     - ``data``, ``union`` (default), or ``union-all``. Alias:
       ``--graph-scope``.
   * - ``--shape-name <IRI>``
     - Use only this named shape as a validation entry point. Repeatable.
       Alias: ``--entry-shape``.
   * - ``--minimum-severity <LEVEL>``
     - ``info`` (default), ``warning``, or ``violation``. The lowest severity
       that makes the run non-conforming. Lower-severity findings are omitted
       from the default text and JSON output, but retained in ``--report``.
   * - ``--on-unsupported <POLICY>``
     - ``error`` (default) or ``ignore``. Use ``ignore`` to permit partially
       supported features; see :doc:`feature-support`.
   * - ``--dump-data <PATH>``
     - Write the data graph validation actually read, as Turtle, to ``PATH``
       (``-`` for stdout). See :ref:`cli-dump`.
   * - ``--dump-shapes <PATH>``
     - Write the merged shapes graph, as Turtle, to ``PATH`` (``-`` for
       stdout). See :ref:`cli-dump`.
   * - ``--profile``
     - Print input, shape, cache, and SPARQL execution telemetry afterwards.
       See :ref:`cli-profile`.

Default output for the :doc:`first-validation example
<../tutorials/first-validation>`:

.. program-output:: bash validate.sh
   :cwd: ../examples/quick-start

The summary groups by finding. ``affects`` lists the focus nodes with that
failure; ``value node`` identifies an offending value when one exists. One
focus node can appear under several findings.

Graph modes
~~~~~~~~~~~

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Mode
     - Behaviour
   * - ``data``
     - Focus nodes and evaluation use the data graph only.
   * - ``union`` *(default)*
     - Focus nodes from data; path traversal, class hierarchy, and SPARQL see
       data ∪ shapes.
   * - ``union-all``
     - Focus nodes and evaluation both see data ∪ shapes.

Named entry points
~~~~~~~~~~~~~~~~~~

``--shape-name`` restricts which target-bearing statements select focus nodes.
Helper shapes reached through ``sh:node``, ``sh:property``, qualified value
shapes, and boolean shape expressions are still evaluated normally. IRIs may be
bare or in angle brackets.

``shifty infer``
----------------

.. code-block:: bash

   shifty infer --shapes <SHAPES> [--data <DATA>] [OPTIONS]

.. list-table::
   :widths: 34 66
   :header-rows: 1

   * - Flag
     - Meaning
   * - ``--shapes <SHAPES>``
     - Shapes/rules file(s) or URL(s). Repeatable. Required.
   * - ``--data <DATA>``
     - Data file(s) or URL(s). Repeatable. Defaults to ``--shapes``.
   * - ``--base <BASE>``
     - Base IRI for parsing.
   * - ``--format <FORMAT>``
     - ``text`` (default) or ``json``.
   * - ``--profile``
     - Print input, shape, cache, and SPARQL execution telemetry afterwards.
       See :ref:`cli-profile`.

.. program-output:: bash infer.sh
   :cwd: ../examples/inference

Only the derived triples are printed. The CLI does not write a merged graph;
use the Python API's ``InferResult.graph()`` for that (:doc:`../how-to/infer`).

``shifty repair``
-----------------

.. code-block:: bash

   shifty repair --shapes <SHAPES> [--data <DATA>] [OPTIONS]

.. list-table::
   :widths: 34 66
   :header-rows: 1

   * - Flag
     - Meaning
   * - ``--shapes <SHAPES>``
     - Shapes file(s) or URL(s). Repeatable. Required.
   * - ``--data <DATA>``
     - Data file(s) or URL(s). Repeatable. Defaults to ``--shapes``.
   * - ``--base <BASE>``
     - Base IRI for parsing.
   * - ``--stage <STAGE>``
     - ``witness``, ``tree`` (default), or ``solve``.
   * - ``--format <FORMAT>``
     - ``text`` (default) or ``json``.
   * - ``--no-infer``
     - Skip SHACL-AF rule inference before witnessing.
   * - ``--apply``
     - Run the fixpoint driver and emit the repaired data graph as N-Triples.
       Overrides ``--stage``.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Stage
     - What it prints
   * - ``witness``
     - The witness tree per failing focus node — why each violates.
   * - ``tree``
     - The synthesized repair template per failing focus node — how it could
       be fixed, with holes and decision points left open.
   * - ``solve``
     - A concrete delta found by the built-in enumeration driver.

The enumeration driver binds holes from terms already present in the graph. It
is meant for inspection; :doc:`../experimental/repair/how-to` covers driving the loop
yourself.

``shifty inspect``
------------------

.. code-block:: bash

   shifty inspect [--stage <STAGE>] [--format <FORMAT>] [--base <BASE>] <FILE>

The shapes file is a **positional argument**, not ``--shapes``, and no data
graph is read.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Stage
     - What it prints
   * - ``rdf``
     - The raw parsed RDF triples, before lowering.
   * - ``algebra`` *(default)*
     - The lowered formalism IR.
   * - ``normalized``
     - The IR after common-subexpression elimination and simplification.
   * - ``strata``
     - The recursion and stratification analysis.
   * - ``plan``
     - The physical plan: focus sources and cost-ordered shape checks.
   * - ``capability``
     - Which SPARQL constraint queries lower to the native executor and which
       fall back to Spareval.
   * - ``access``
     - Static default/shapes graph reads by statement, rule, and function;
       query/path identities, function calls, possible rule writes, and
       conservative coverage. This stage does not read a data graph or report
       runtime index choices.

``--format text`` (default) works for every stage. ``--format json`` works
except for ``capability``;
``--format dot`` emits Graphviz for the ``algebra`` and ``normalized`` stages
and is rejected for the others.

See :ref:`inspect-validation` for schema inspection and troubleshooting.

``shifty version``
------------------

.. code-block:: bash

   shifty version

Prints the installed CLI version.

.. _cli-profile:

``--profile``
-------------

``validate`` and ``infer`` accept ``--profile``, which appends a telemetry
block to stdout after the normal output — after the ``sh:ValidationReport``
document under ``--report``, so it never interrupts it.

The block opens with the inputs: for each of ``--shapes`` and ``--data``, the
format that parsed each source and the number of triples it contributed.

.. code-block:: text

   conforms: true
   profile: shapes: 28 triples from shapes.ttl [turtle]
   profile: data: 633 triples from ontology.ttl [turtle]
   profile: stage: shapes load: 1.234 ms
   profile: stage: compile: 2.345 ms
   profile: stage: data load: 3.456 ms
   profile: stage: session and inference: 4.567 ms
   profile: inference: 0 triples added before validation
   profile: stage: first validation: 5.678 ms
   profile: stage: export: 0.009 ms
   profile: 2 distinct shape(s)/rule(s)
     rule[0]: 1 call(s), 24µs total, 24µs avg
   ...

The profile reports the format that successfully parsed each input. For
multiple sources, it also gives the merged graph size, each source's
contribution, and the number of duplicates:

.. code-block:: text

   profile: data: 2 triples from 2 sources (1 triple dropped as duplicate)
     first.ttl: 1 triple [turtle]
     second.ttl: 2 triples [turtle]

``validate`` reports the number of inferred triples, or ``skipped
(--no-infer)``. The remaining profile fields describe time, memory, and graph
access during this run.

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Field group
     - Contents
   * - Stage timings
     - Input loading, compilation, session construction and inference, and
       output formatting. Validation also times its first evaluation.
   * - Storage
     - Store and encoded-dataset builds, source and local rows, committed
       inference rows, optional index bytes, and indexes declined by budget.
   * - Compilation and encoding
     - Source and access-catalog compilation, source and session encoding,
       and inference commit batches and time.
   * - Primary pair buffers
     - Allocated pair capacity for shared source and session storage.
       Dictionary strings and map nodes are excluded.
   * - Graph materialization
     - Full union graph copies and lazy compatibility projections. These
       should be zero unless a caller requests a public graph getter.
   * - Optional indexes
     - Scope, direction, predicate, build reason, estimated and allocated
       bytes, budget, build time, and admission decision.
   * - Scans
     - Calls and candidate rows by graph scope and bound positions
       (``S``, ``P``, ``O``), useful for finding repeated broad scans.
   * - Reach cache
     - Hits, misses, insertions, and result ids declined by its admission cap.
   * - Evaluation
     - Per-shape, per-rule, and per-query SPARQL times, plus shape-cache hit
       rate and peak size.

See :doc:`../benchmarks` for measurements across repeated runs. The
`storage design notes
<https://github.com/gtfierro/shifty/blob/main/development_docs/sessions-and-storage.md>`_
explain the indexes and caches.

.. _cli-dump:

``--dump-data`` and ``--dump-shapes``
-------------------------------------

``validate`` can write out the two graphs it evaluated, as Turtle, to a path or
to stdout with ``-``:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data ontology.ttl \
       --dump-data used-data.ttl --dump-shapes used-shapes.ttl

Neither graph is the file on disk, which is the reason the flags exist:

- **The data graph is post-inference.** Unless ``--no-infer`` is given, SHACL-AF
  rules run before validation and their conclusions are part of what the
  constraints see. A shape that fails on a triple appearing in no input
  document is failing on an inferred one, and the dump is where to find it.
- **The shapes graph is every** ``--shapes`` **source merged**, so a constraint
  that only exists after two files are combined shows up here as it was
  evaluated.
- Under the default ``--graph-mode union`` the evaluator reads the two
  together; they are dumped separately, as they are held.

Blank node labels are the parser's own, so they are stable within a run but
need not match the source document's.

Both dumps are written before the validation result, so a run that goes on to
fail still leaves them behind. With ``-`` and ``--report`` on the same command
line, the dumped graph comes first and the report follows.

Seeing what inference contributed:

.. code-block:: text

   $ shifty validate --shapes shapes.ttl --data alias.ttl --dump-data - --profile
   @prefix bro: <https://ontology.brickschema.org/2.0/#bro:> .
   @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
   bro:AHU rdfs:subClassOf bro:AirHandlingUnit ;
       a rdfs:Class ;
       bro:aliasClassOf bro:AirHandlingUnit .
   bro:AirHandlingUnit rdfs:subClassOf bro:AHU ;
       a rdfs:Class .
   conforms: true
   profile: data: 3 triples from alias.ttl [turtle]
   profile: inference: 2 triples added before validation

The two ``rdfs:subClassOf`` statements are in no input file; a rule derived
them.
