CLI Reference
=============

.. raw:: html

   <div class="sh-section-intro">
     The <strong>shifty</strong> binary provides commands for validation and inference,
     plus <code>inspect</code> and <code>version</code> commands for pipeline introspection
     and build identification.
     Shapes files and data files can be local paths or HTTP/HTTPS URLs; both
     <code>--shapes</code> and <code>--data</code> are repeatable to merge multiple files.
   </div>

Install
-------

.. code-block:: bash

   # Build and install from the workspace:
   cargo install --path crates/shifty-cli

   # Or build without installing:
   cargo build --release -p shifty-cli
   ./target/release/shifty --help

Print the installed CLI version:

.. code-block:: bash

   shifty version

validate
--------

Validate a data graph against a SHACL shapes graph:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl

.. note::

   ``--data`` defaults to ``--shapes`` when omitted. So ``shifty validate
   --shapes combined.ttl`` reads shapes *and* data from the one file, while
   ``--shapes shapes.ttl --data data.ttl`` compiles the schema **only** from
   ``--shapes`` — SHACL vocabulary in the ``--data`` graph is ignored, never
   turned into constraints. Both ``--shapes`` and ``--data`` are repeatable and
   unioned, so to validate against shapes embedded in your data, add that file
   as an extra ``--shapes`` source. See :ref:`shapes-and-data-graphs`.

Default output is a human-readable summary:

.. code-block:: text

   conforms: false
   violations: 1
     <http://example.org/bob>  [target: ∃ rdf:type .⊤]
         - (ex:name) 123 → expected datatype xsd:string

Emit a W3C ``sh:ValidationReport`` in Turtle:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl --report

JSON output:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl --format json

Skip SHACL-AF rule inference (validation only, no ``sh:rule`` expansion):

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl --no-infer

Graph mode
~~~~~~~~~~

The ``--graph-mode`` flag controls which triples are visible to path traversal
and SPARQL evaluation during validation.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Mode
     - Description
   * - ``data``
     - Focus nodes come from the data graph; path traversal and SPARQL use the data graph only
   * - ``union`` *(default)*
     - Focus nodes from data; paths and SPARQL use data ∪ shapes
   * - ``union-all``
     - Focus nodes and evaluation both use data ∪ shapes

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl --graph-mode union
   shifty validate --shapes shapes.ttl --data data.ttl --graph-mode union-all

Named Shape Entry Points
~~~~~~~~~~~~~~~~~~~~~~~~

Use ``--shape-name`` to validate only selected named shapes as top-level
entry points. The flag is repeatable, and ``--entry-shape`` is an alias.

.. code-block:: bash

   shifty validate \
     --shapes shapes.ttl \
     --data data.ttl \
     --shape-name http://example.org/PersonShape

Only target-bearing statements owned by the selected named shapes are used as
entry points. Referenced helper shapes are still evaluated normally, so
dependencies reached through ``sh:node``, ``sh:property``, qualified value
shapes, and boolean shape expressions keep their usual semantics. Shape names
may be passed as bare IRIs or wrapped in angle brackets.

infer
-----

Run SHACL-AF ``sh:rule`` entries to a fixed point, then print the derived triples:

.. code-block:: bash

   shifty infer --shapes rules.ttl --data data.ttl

Output:

.. code-block:: text

   inferred 3 triple(s):
     <http://example.org/r1> <http://example.org/area> "6"^^<http://www.w3.org/2001/XMLSchema#integer>
     ...

If the rules are embedded in the data graph, omit ``--data`` and pass a single file:

.. code-block:: bash

   shifty infer --shapes combined.ttl

To write the full graph (original + inferred) to a Turtle file:

.. code-block:: bash

   shifty infer --shapes rules.ttl --data data.ttl --output result.ttl

inspect
-------

``inspect`` reveals the internal pipeline stages so you can see exactly how shifty
interprets a shapes graph.

.. code-block:: bash

   # Raw triples after parsing
   shifty inspect --stage rdf shapes.ttl

   # Lowered algebraic IR (φ/π notation)
   shifty inspect --stage algebra shapes.ttl

   # After normalization and common-subexpression elimination
   shifty inspect --stage normalized shapes.ttl

   # Stratification analysis (recursion detection)
   shifty inspect --stage strata shapes.ttl

   # Physical plan: focus sources + cost-ordered shape checks
   shifty inspect --stage plan shapes.ttl

   # SPARQL constraint capability: native vs. Spareval fallback
   shifty inspect --stage capability shapes.ttl

All stages support ``--format text`` (default) and ``--format json``.
The ``algebra`` and ``normalized`` stages also accept ``--format dot`` for
Graphviz output.

For the full Rust API, see `docs.rs/shifty-engine <https://docs.rs/shifty-engine>`_.
