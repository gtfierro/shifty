Python API reference
====================

The ``pyshifty`` package exposes the engine through `PyO3 <https://pyo3.rs>`_
bindings. Install with ``pip install pyshifty``; import as ``shifty``.

This page covers validation and inference. The evidence, shape-map, and repair
interfaces have their own pages: :doc:`evidence`, :doc:`shape-maps`,
:doc:`repair`.

Graph inputs
------------

Every entry point accepts the same input type, written ``GraphInput`` below:

- ``str`` — Turtle text
- ``bytes`` — Turtle bytes
- ``pathlib.Path`` — a file, parsed in Rust from its extension
- ``rdflib.Graph``

A ``list`` or ``tuple`` of these is merged at the triple level first.

``pathlib.Path`` is the fastest form, because the file never crosses the
Python/Rust boundary as text. ``rdflib.Graph`` inputs are transferred as
N-Triples, avoiding rdflib's slower Turtle serializer.

.. note::

   **Which graph shapes come from.** Passing a single graph (omitting
   ``shacl_graph``, or passing ``None``) makes that graph both the shapes and
   the data. Passing a separate shapes graph compiles the schema **only** from
   it — SHACL vocabulary sitting in the data graph is ignored, never turned
   into constraints. Passing an empty ``rdflib.Graph()`` means "an explicitly
   empty shapes graph", which is not the same as ``None``. See
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
       minimum_severity="info",
       sort_results=True,
       on_unsupported="ignore",
       base=None,
   ) -> tuple[bool, rdflib.Graph, str]

The ``pyshacl``-compatible entry point. Returns ``(conforms, report_graph,
results_text)``: the boolean, a W3C ``sh:ValidationReport`` as an
``rdflib.Graph``, and that report rendered for a human.

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
       path traversal, class hierarchy, and SPARQL can see.
   * - ``shape_names``
     - Named shape IRIs to use as top-level entry points. Referenced helper
       shapes are still evaluated normally. Bare or angle-bracketed.
   * - ``infer``
     - Run SHACL-AF ``sh:rule`` entries to a fixed point before validating.
       Default ``True``.
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

The same validation, returning structured objects instead of an RDF report. It
does not require ``rdflib``.

.. code-block:: python

   result = shifty.validate_algebra(data, shapes)

   print(result.conforms)
   for violation in result.violations:
       print(violation.focus_node)      # failing focus node
       print(violation.statement_id)    # stable statement id
       print(violation.constraint_id)   # statement-level algebra id
       print(violation.shape_name)      # the shape that targeted it, if named
       for reason in violation.reasons:
           print(reason.message)          # human-readable description
           print(reason.path)             # property path, if applicable
           print(reason.value)            # the offending value node
           print(reason.constraint_kind)  # ConstraintKind.Cardinality, ...
           print(reason.constraint.render)

``bool(result)`` is equivalent to ``result.conforms``.

Algebraic provenance
~~~~~~~~~~~~~~~~~~~~

``Reason.constraint``
   A ``Constraint`` for the algebra node that produced this cause, with ``id``,
   ``kind``, ``render``, ``definition``, and ``json`` fields.

``Reason.constraint_kind``
   A stable enum: ``ConstraintKind.Cardinality``,
   ``ConstraintKind.ClassMembership``, ``ConstraintKind.ValueType``,
   ``ConstraintKind.NodeKind``, ``ConstraintKind.Conjunction``,
   ``ConstraintKind.Disjunction``, ``ConstraintKind.Sparql``, and others.
   Branch on this rather than parsing ``message`` or matching Rust type names.

``Reason.constraint_id``
   The specific nested algebra node responsible. This differs from
   ``Violation.constraint_id`` when the violated shape is a conjunction,
   disjunction, or other composite.

``Violation.statement_id`` / ``Violation.constraint_id``
   The top-level statement identity, and the join key shared with
   ``RepairSession.witnesses()``:

.. code-block:: python

   result = shifty.validate_algebra(data, shapes, infer=False)
   session = shifty.RepairSession(shapes, data, infer=False)

   witnesses = {
       (w.focus, w.statement_id, w.constraint_id): w
       for w in session.witnesses()
   }

   for v in result.violations:
       witness = witnesses.get((v.focus_node, v.statement_id, v.constraint_id))
       if witness is not None:
           print(witness.repair_tree().explain())

``infer``
---------

.. code-block:: python

   shifty.infer(data_graph, shapes_graph=None, *,
                on_unsupported="ignore", base=None) -> InferResult

Runs SHACL-AF ``sh:rule`` entries to a fixed point. Note it takes no
``graph_mode``.

.. code-block:: python

   result = shifty.infer(data, rules)

   result.inferred_count      # number of newly derived triples
   result.diagnostics         # warnings raised while lowering
   result.graph_ntriples      # original + inferred, as N-Triples text
   result.graph()             # the same, as an rdflib.Graph

``PreparedValidator``
---------------------

.. code-block:: python

   shifty.PreparedValidator(shacl_graph, *, base=None)

Parses, lowers, normalizes, and plans a shapes graph once, for reuse across
many data graphs. This is the right tool whenever the schema is fixed and the
data changes, which is most batch and service workloads.

.. code-block:: python

   validator = shifty.PreparedValidator(shapes)
   validator.diagnostics                       # lowering warnings

   conforms, report, text = validator.validate(data)
   result = validator.validate_algebra(data, infer=False)

``validate`` and ``validate_algebra`` take the data graph positionally and
accept ``graph_mode``, ``shape_names``, ``infer``, ``minimum_severity``,
``sort_results``, and ``on_unsupported`` as keywords, with the same meanings as
the module-level functions.

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

``PreparedValidator``, ``EvidenceSession``, ``RepairSession``, and
``InferResult`` all expose ``.diagnostics``: warnings raised while lowering the
shapes graph. Constructs that were not understood show up here, and it is worth
checking once when a constraint seems to be doing nothing.
