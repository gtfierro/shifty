Python API reference
====================

The ``pyshifty`` package exposes the engine through `PyO3 <https://pyo3.rs>`_
bindings. Install with ``pip install pyshifty``; import as ``shifty``.

.. list-table::
   :widths: 20 80

   * - Distribution
     - ``pyshifty``; import name ``shifty``
   * - Stability
     - Stable, except interfaces explicitly marked experimental
   * - Related
     - :doc:`evidence`, :doc:`shape-maps`, :doc:`feature-support`

This page covers validation and inference. The evidence and shape-map
interfaces have their own pages: :doc:`evidence` and :doc:`shape-maps`. The
experimental repair API is in :doc:`repair`.

Graph inputs
------------

Every entry point accepts the same input type, written ``GraphInput`` below:

- ``str`` — Turtle text, a local file path, or an HTTP(S) URL
- ``bytes`` — Turtle bytes
- ``pathlib.Path`` — a file, parsed in Rust from its extension
- ``rdflib.Graph``

A ``list`` or ``tuple`` of these is merged at the triple level first.

``pathlib.Path`` is the fastest file form, because the file never crosses the
Python/Rust boundary as text. Existing string paths use the same behavior: a
directory raises ``IsADirectoryError``, and a missing RDF-looking filename
such as ``shapes.ttl`` raises ``FileNotFoundError``. Long or multiline strings
are Turtle and are never probed as paths. This policy applies to every list or
tuple member. HTTP(S) URLs are fetched once and passed as bytes. ``rdflib.Graph``
inputs are serialized as Turtle so namespace bindings required by SHACL-SPARQL
queries and rules survive. URL formats are inferred from the response content
type or the final URL suffix; Turtle is the fallback.

.. note::

   **Which graph shapes come from.** Passing a single graph (omitting
   ``shacl_graph``, or passing ``None``) makes that graph both the shapes and
   the data. Passing a separate shapes graph compiles the schema **only** from
   it — SHACL vocabulary sitting in the data graph is ignored, never turned
   into constraints. An explicitly empty shapes graph raises ``ValueError`` by
   default; it is not the same as ``None``. See
   :doc:`../explanation/shapes-and-data`.

``validate``
------------

.. code-block:: python

   shifty.validate(
       data_graph,
       shacl_graph=None,
       *,
       graph_mode="union",
       shape_names=None,
       infer=True,
       in_place=False,
       minimum_severity="info",
       sort_results=True,
       on_unsupported="ignore",
       base=None,
   ) -> tuple[bool, rdflib.Graph, str]

The report model and three-value return form used by ``pyshacl.validate``.
Compare keyword arguments when migrating existing code. Returns ``(conforms,
report_graph, results_text)``: the boolean, a W3C ``sh:ValidationReport`` as an
``rdflib.Graph``, and that report rendered for a human. Shifty additionally
rejects an explicitly empty shapes graph by default.

Requires ``rdflib`` at call time, since it constructs the report graph.

.. list-table::
   :widths: 24 76
   :header-rows: 1

   * - Argument
     - Meaning
   * - ``data_graph``
     - The RDF data to validate.
   * - ``shacl_graph``
     - The shapes graph. ``None`` means shapes live in ``data_graph``.
   * - ``graph_mode``
     - ``"union"`` (default), ``"data"``, or ``"union-all"`` — which triples
       can select focus nodes and which are visible to constraint evaluation.
   * - ``shape_names``
     - Named shape IRIs to use as top-level entry points. Referenced helper
       shapes are still evaluated normally. Bare or angle-bracketed.
   * - ``infer``
     - Run SHACL-AF ``sh:rule`` entries to a fixed point before validating.
       Default ``True``.
   * - ``in_place``
     - Add inferred triples to a caller-owned ``rdflib.Graph``. Requires
       ``infer=True`` and a single ``rdflib.Graph`` data input. Default ``False``.
   * - ``minimum_severity``
     - ``"info"`` (default), ``"warning"``, or ``"violation"`` — the lowest
       severity that makes ``conforms`` false. Findings below it are still
       reported.
   * - ``sort_results``
     - Deterministic ordering of results. Default ``True``.
   * - ``on_unsupported``
     - ``"ignore"`` (default) or ``"error"``. See
       :doc:`feature-support`.
   * - ``base``
     - Base IRI for resolving relative IRIs while parsing.

.. code-block:: python

   conforms, report_graph, results_text = shifty.validate(data, shapes)

   conforms, report, text = shifty.validate(data, shapes, infer=False)
   conforms, report, text = shifty.validate(
       data, shapes, shape_names=["http://example.org/PersonShape"],
   )

``validate_algebra``
--------------------

.. code-block:: python

   shifty.validate_algebra(data_graph, shacl_graph=None, **same_keywords)
       -> AlgebraResult

The algebraic evaluator returns structured objects instead of an RDF report.
Its constraint traversal differs from ``validate()``; see
:ref:`architecture-result-paths`. It does not require ``rdflib`` and is useful
when a program consumes the findings directly.

.. code-block:: python

   result = shifty.validate_algebra(data, shapes)

   for violation in result.violations:
       for reason in violation.reasons:
           print(violation.focus_node, reason.constraint_kind, reason.path)

``AlgebraResult``
~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 24 76
   :header-rows: 1

   * - Field
     - Meaning
   * - ``conforms``
     - Whether the run conformed, at the configured ``minimum_severity``.
       ``bool(result)`` is equivalent.
   * - ``violations``
     - One ``Violation`` per failing ``(focus node, statement)``. Findings
       below ``minimum_severity`` still appear here — the threshold changes
       only ``conforms``.
   * - ``results_text``
     - The findings rendered for a human.

``Violation``
~~~~~~~~~~~~~

.. list-table::
   :widths: 24 76
   :header-rows: 1

   * - Field
     - Meaning
   * - ``focus_node``
     - The node that failed.
   * - ``reasons``
     - One ``Reason`` per thing that went wrong at this node. A node with two
       broken obligations is one violation with two reasons.
   * - ``severity``
     - The most severe severity among its reasons.
   * - ``shape_name``
     - The IRI of the shape that targeted this node, when the shape is a named
       RDF node.
   * - ``statement_id``
     - Stable statement identity.
   * - ``constraint_id``
     - The algebra id of the statement's **top-level** shape.

``Reason``
~~~~~~~~~~

.. list-table::
   :widths: 24 76
   :header-rows: 1

   * - Field
     - Meaning
   * - ``constraint_kind``
     - A stable enum naming the algebra operator that failed. Branch on this
       rather than parsing ``message``.
   * - ``path``
     - The property path checked, where one applies.
   * - ``value``
     - The offending value node. Falls back to the focus node when the failure
       is an absence and there is no offending value.
   * - ``message``
     - Engine-generated description. For display, not for matching on.
   * - ``author_message``
     - The shape's own ``sh:message``, or ``None``. Prefer it when present:
       ``reason.author_message or reason.message``.
   * - ``severity``
     - This reason's effective SHACL severity.
   * - ``constraint``
     - The ``Constraint`` for the algebra node that produced this cause, with
       ``id``, ``kind``, ``render``, ``definition``, ``definition_pretty``, and
       ``json``. ``definition_pretty`` is the same text broken and indented by
       nesting depth, and is byte-identical to ``definition`` when the one-line
       form already fits, so it can be used unconditionally.
   * - ``constraint_id``
     - The specific **nested** algebra node responsible. Differs from
       ``Violation.constraint_id`` whenever the shape is a conjunction,
       disjunction, or other composite — which is nearly always.
   * - ``observed_count``
     - For a cardinality failure, how many values along the path satisfied the
       qualifier; ``None`` for every other kind. The bound is already in
       ``constraint``, so this is the one number needed to state a shortfall
       without parsing ``message``.
   * - ``statement_id``
     - The statement this reason belongs to.
   * - ``sparql_diagnostic``
     - Query diagnostic, for a ``ConstraintKind.Sparql`` failure.

``ConstraintKind``
~~~~~~~~~~~~~~~~~~

``Cardinality``, ``ValueType``, ``ClassMembership``, ``NodeKind``,
``Constant``, ``Closed``, ``Conjunction``, ``Disjunction``, ``Negation``,
``Equals``, ``Disjoint``, ``LessThan``, ``LessThanOrEquals``, ``UniqueLang``,
``Expression``, ``Sparql``, ``Top``, ``Unknown``.

These name algebra operators, not SHACL keywords. ``sh:minCount``,
``sh:maxCount``, and ``sh:qualifiedMinCount`` all surface as ``Cardinality``,
because the compiler lowers them to one counting operator — see
:doc:`../explanation/architecture`. Use ``reason.constraint`` when you need to
distinguish them.

:doc:`../tutorials/reading-results` works through consuming these.

``infer``
---------

.. code-block:: python

   shifty.infer(data_graph, shapes_graph=None, *,
                in_place=False, on_unsupported="ignore", base=None) -> InferResult

Runs SHACL-AF ``sh:rule`` entries to a fixed point. Note it takes no
``graph_mode``.

.. code-block:: python

   result = shifty.infer(data, rules)

   result.inferred_count      # number of newly derived triples
   result.diagnostics         # non-fatal lowering warnings / unsupported features
   result.inferred_ntriples   # just the derived delta, as N-Triples text
   result.graph_ntriples      # original + inferred, as N-Triples text
   result.graph()             # the same, as an rdflib.Graph

``in_place``
~~~~~~~~~~~~

``infer()``, ``validate()``, ``validate_algebra()``, and the two
``PreparedValidator`` methods accept ``in_place=True``, which writes the triples
SHACL-AF inference derived straight into a caller-owned ``rdflib.Graph`` passed
as the data graph, rather than returning a separate copy. Only the derived delta
crosses back from Rust, and a triple derived about a blank node lands on the
blank node the caller's graph already holds. It requires an ``rdflib.Graph``
input and is off by default, so existing calls are unaffected. See
:doc:`../how-to/infer` and :doc:`../how-to/validate`.

``PreparedValidator``
---------------------

.. code-block:: python

   shifty.PreparedValidator(shacl_graph, *, base=None)

Parses, lowers, normalizes, and plans a shapes graph once, for reuse across
many data graphs. This is the right tool whenever the schema is fixed and the
data changes, which is most batch and service workloads.

An explicitly empty shapes graph raises ``ValueError``.

.. code-block:: python

   validator = shifty.PreparedValidator(shapes)
   validator.diagnostics                       # non-fatal lowering diagnostics

   conforms, report, text = validator.validate(data)
   result = validator.validate_algebra(data, infer=False)

``validate`` and ``validate_algebra`` take the data graph positionally and
accept ``graph_mode``, ``shape_names``, ``infer``, ``in_place``, ``minimum_severity``,
``sort_results``, and ``on_unsupported`` as keywords, with the same meanings as
the module-level functions.

.. _python-property-witnesses:

``PreparedValidator.witnesses``
-------------------------------

.. code-block:: python

   validator.witnesses(data_graph, *, key_path=None, graph_mode="union",
                       infer=True, on_unsupported="ignore")
       -> list[PropertyWitness]

The inverse of validation. For every focus node that *conforms* to a
target-bearing node shape, it returns the values each ``sh:property`` shape's
``sh:path`` resolved to — so a SHACL profile can double as an extraction
schema.

.. code-block:: python

   shapes = """
   @prefix sh:  <http://www.w3.org/ns/shacl#> .
   @prefix zea: <http://example.org/zea#> .
   @prefix ex:  <http://example.org/> .

   ex:VavProfile a sh:NodeShape ;
       sh:targetClass ex:Vav ;
       sh:property [
           zea:role ex:OutsideAirTempRole ;
           sh:path ex:hasPoint ;
           sh:qualifiedValueShape [ sh:hasValue ex:oat ] ;
           sh:qualifiedMinCount 1 ;
           sh:qualifiedMaxCount 1 ;
       ] .
   ex:OutsideAirTempRole zea:roleName "outsideAirTemp" .
   """

   validator = shifty.PreparedValidator(shapes)
   for w in validator.witnesses(data, key_path="zea:role/zea:roleName"):
       print(w.focus, w.key, w.values)
   # <http://example.org/vav1> outsideAirTemp ['<http://example.org/oat>']

``key_path`` is a SPARQL 1.1 property path — sequence ``/``, alternation ``|``,
inverse ``^``, and the Kleene forms ``*``, ``+``, ``?`` — evaluated from each
``sh:property`` shape's own node over the shapes graph. The example key is not
a direct annotation on the property shape; it is one hop further, through a
role-descriptor node, which a bare predicate lookup could not reach. A direct
annotation would be ``key_path="zea:roleName"``, and a descriptor pointing *at*
the property shape would be ``key_path="^zea:describes/zea:roleName"``.
Prefixes resolve against the shapes document's ``@prefix`` declarations.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - ``PropertyWitness``
     - Meaning
   * - ``focus``
     - The focus node that conformed.
   * - ``shape``
     - The node shape it conformed to.
   * - ``key``
     - The lexical value reached by ``key_path``, or the property shape's own
       IRI/blank-node id when the path resolves to nothing or is omitted.
   * - ``values``
     - Deduped ``sh:path`` bindings, rendered in full (``<iri>``, ``"lit"``,
       ``"lit"@lang``, ``"lit"^^<datatype>``) so IRIs and literals stay
       distinguishable — narrowed to the ``sh:qualifiedValueShape`` matches
       when the property shape declares one.

For a richer version of the same idea, with typed keys, typed terms, and
partial bindings for non-conforming nodes, see :doc:`shape-maps`.

Diagnostics
-----------

``PreparedValidator``, ``EvidenceSession``, ``RepairSession``,
``InferResult``, and — since 0.5 — the ``AlgebraResult`` returned by
``validate_algebra()`` all expose ``.diagnostics`` for **non-fatal** lowering
warnings and unsupported features.

``validate()`` and ``PreparedValidator.validate()`` return the
``(conforms, report_graph, results_text)`` tuple, which has
nowhere to put a diagnostic. They instead emit one
``shifty.ShaclDiagnosticWarning`` per diagnostic, so a rule that could not run
does not read as a clean pass:

.. code-block:: python

   import warnings

   with warnings.catch_warnings(record=True) as raised:
       warnings.simplefilter("always")
       conforms, report, text = shifty.validate(data, shapes)

   # Or silence them:
   warnings.filterwarnings("ignore", category=shifty.ShaclDiagnosticWarning)

Invalid shapes diagnostics are different: a malformed SPARQL query or an
unresolved query prefix raises ``ValueError`` while the operation is prepared.
They never leave an API call running with that constraint or rule omitted.
