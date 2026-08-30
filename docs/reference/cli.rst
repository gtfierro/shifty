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
   * - ``--profile``
     - Print shape, cache, and SPARQL execution telemetry afterwards.

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
     - Print shape, cache, and SPARQL execution telemetry afterwards.

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
