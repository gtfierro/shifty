CLI reference
=============

.. list-table::
   :widths: 20 80

   * - Binary
     - ``shifty`` (crate ``shifty-cli``)
   * - Stability
     - Stable
   * - Related
     - :doc:`../how-to/install`, :doc:`../how-to/validate`,
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
     - Base IRI for parsing.
   * - ``--format <FORMAT>``
     - ``text`` (default) or ``json``.
   * - ``--report``
     - Emit a W3C ``sh:ValidationReport`` graph as N-Triples instead of the
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
       that makes the run non-conforming.
   * - ``--dump-data <PATH>``
     - Write the data graph validation actually read, as Turtle, to ``PATH``
       (``-`` for stdout). See :ref:`cli-dump`.
   * - ``--dump-shapes <PATH>``
     - Write the merged shapes graph, as Turtle, to ``PATH`` (``-`` for
       stdout). See :ref:`cli-dump`.
   * - ``--profile``
     - Print input, shape, cache, and SPARQL execution telemetry afterwards.
       See :ref:`cli-profile`.

Default output:

.. code-block:: text

   conforms: false
   violations: 1
     <http://example.org/bob>  [severity: Violation; target: class(<http://example.org/Person>)]
         - [Violation] (<http://example.org/email>) <http://example.org/bob> → at least 1 value(s) required along <http://example.org/email>, found 0
         - [Violation] (<http://example.org/name>) "123"^^<http://www.w3.org/2001/XMLSchema#integer> → test(datatype(xsd:string)) not satisfied

Results are grouped by focus node, with one line per reason. The parenthesised
term is the property path; then the offending value, or the focus node itself
when the failure is an absence; then the constraint that was not satisfied, in
the engine's algebraic notation.

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

.. code-block:: text

   inferred 1 triple(s):
     <http://example.org/r1> <http://example.org/area> "3"^^<http://www.w3.org/2001/XMLSchema#integer>

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
is meant for inspection; :doc:`../how-to/repair` covers driving the loop
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

``--format text`` (default) and ``--format json`` work for every stage;
``--format dot`` emits Graphviz for the ``algebra`` and ``normalized`` stages
and is rejected for the others.

See :doc:`../how-to/inspect-pipeline` for how to read each stage.

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
   profile: data: 633 triples from ontology.ttl.md [turtle]
   profile: inference: 0 triples added before validation
   profile: 2 distinct shape(s)/rule(s)
     rule[0]: 1 call(s), 24µs total, 24µs avg
   ...

The format is the one that *succeeded*, not the one the extension suggests. A
document is identified by content type, then by extension, then by sniffing its
first non-comment token, and finally by trying each supported format in turn;
``ontology.ttl.md`` above is literate Turtle — markdown prose on ``#`` comment
lines, statements indented — and is reported as ``turtle`` because Turtle is
what read it.

This is how to tell an empty result from an unread input: a shape whose target
predicate never appears in the data conforms vacuously, and ``conforms: true``
alone cannot distinguish that from a document that failed to contribute
anything. With several sources the header gives the merged graph's size and
each source's own contribution:

.. code-block:: text

   profile: data: 2 triples from 2 sources (1 triple dropped as duplicate)
     first.ttl: 1 triple [turtle]
     second.ttl: 2 triples [turtle]

``validate`` also reports what rule inference added before validation, or
``skipped (--no-infer)``. Then come the engine's own counters: per-shape and
per-rule wall-clock time, shape-cache hit rate and peak size, and per-query
SPARQL execution time.

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
