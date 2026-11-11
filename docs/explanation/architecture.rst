.. _architecture-result-paths:

Validation results and evidence
===============================

Shifty can return a standard SHACL report, structured failures, or evidence
for both passing and failing nodes. Choose according to what your application
needs to read.

Choose a result format
----------------------

.. list-table::
   :header-rows: 1
   :widths: 28 35 37

   * - Need
     - Python entry point
     - Result
   * - Exchange reports with SHACL tools
     - ``validate()``
     - ``(conforms, report_graph, results_text)``. The graph uses the W3C
       ``sh:ValidationReport`` vocabulary and requires ``pyshifty[rdflib]``.
   * - Handle failures in application code
     - ``validate_algebra()``
     - Python objects containing violations and their nested reasons.
   * - Inspect passing nodes and supporting values
     - ``EvidenceSession.validate()``
     - Evidence grouped by statement and selected focus node.
   * - Extract property values
     - ``shape_map()``
     - Bindings for each selected shape and focus node.

The CLI renders native findings by default. ``shifty validate --report``
produces the W3C report.

The formats describe constraints differently. A datatype failure may appear
as a count of invalid values in a structured finding, while the RDF report
identifies ``sh:DatatypeConstraintComponent``. The
:doc:`result-handling guide <../how-to/results>` shows how to
handle findings in code. Exact fields and arguments are in
:doc:`../reference/python`.

.. _understanding-evidence:

What evidence records
---------------------

An evidence run keeps every included statement, even when its target selects
no nodes. Each selected node has a ``pass`` or ``fail`` status. This lets you
distinguish a passing node from one the shape never checked.

Evidence also records values and triples supporting a result. For example,
it can identify the sensor that satisfied a room's temperature-sensor
requirement. A :doc:`shape map <../how-to/shape-maps>` presents those bindings
as a table.

A failure explanation contains the children needed to establish the failure.
For a conjunction, passing children may be omitted. Use
``FocusEvaluation.progress`` for the status of each immediate authored child,
then ``session.evidence_for(focus, constraint_id)`` for a child's full
explanation. The :doc:`evidence guide <../how-to/explain-failures>` shows both.

.. _evidence-limits:

Limits of an explanation
------------------------

SPARQL and expression constraints may return ``opaque`` explanations. The
validator has a result, but cannot describe its derivation structurally.
Recursive successes may end in ``coinductive`` leaves, as described under
:ref:`recursive-shapes`.

Path support records one successful route through the graph. Another route
may still satisfy the constraint after those triples are deleted. Validate
candidate edits with the :doc:`repair gate <../experimental/repair/how-to>`.

.. _evidence-cost:

Choose how much evidence to build
---------------------------------

Recording paths, values, and child explanations takes more time and memory
than deciding conformance alone. Prepared evidence sessions offer several
levels of detail.

.. list-table::
   :header-rows: 1
   :widths: 48 52

   * - Need
     - Entry point
   * - Conformance counts
     - ``validate_conformance()``
   * - The selected pairs that failed
     - ``find_failures()``
   * - An explanation for one selected pair
     - ``explain(pair)``
   * - Passing and failing explanations for every pair
     - ``validate()``

For a screen that shows failures, use ``find_failures()`` followed by
``explain()`` for the pairs you display. Reuse a prepared validator across
data graphs, or an evidence session for repeated queries over one snapshot,
to avoid compiling the shapes again.

``to_compact_json()`` stores repeated terms and subtrees once. It reduces
serialized size without reducing the work of building evidence. See the
:doc:`evidence reference <../reference/evidence>` for serialization options
and :doc:`../benchmarks` for validation timings.

Compiler and execution algorithms are documented in the `development notes
<https://github.com/gtfierro/shifty/tree/main/development_docs>`_.
