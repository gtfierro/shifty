Python API Reference
====================

.. raw:: html

   <div class="sh-section-intro">
     The <strong>pyshifty</strong> package exposes the full Shifty engine through
     <a href="https://pyo3.rs">PyO3</a> bindings with native
     <a href="https://rdflib.readthedocs.io">rdflib</a> interop.
     Pre-built wheels are on PyPI — no Rust toolchain required.
   </div>

Install
-------

.. code-block:: bash

   pip install pyshifty   # Python 3.9+

.. code-block:: python

   import shifty   # the package imports as `shifty`

Graph inputs throughout the API can be a ``str`` (Turtle text), ``bytes``,
``pathlib.Path``, or an ``rdflib.Graph``.

validate
--------

The primary validation entry point is compatible with the ``pyshacl`` interface:

.. code-block:: python

   conforms, report_graph, results_text = shifty.validate(data, shapes)

- ``data`` — the data graph to validate
- ``shapes`` — the SHACL shapes graph (optional; if omitted, shapes are read from ``data``)
- Returns: ``(bool, rdflib.Graph, str)`` — conforms flag, W3C ``sh:ValidationReport`` graph, human-readable summary

.. code-block:: python

   shapes = """
   @prefix sh:  <http://www.w3.org/ns/shacl#> .
   @prefix ex:  <http://example.org/> .
   @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

   ex:PersonShape a sh:NodeShape ;
       sh:targetClass ex:Person ;
       sh:property [
           sh:path ex:name ;
           sh:minCount 1 ;
           sh:datatype xsd:string ;
       ] ;
       sh:property [
           sh:path ex:age ;
           sh:maxCount 1 ;
           sh:datatype xsd:integer ;
       ] .
   """

   data = """
   @prefix ex: <http://example.org/> .

   ex:Alice a ex:Person ; ex:name "Alice" ; ex:age 30 .
   ex:Bob   a ex:Person .
   """

   conforms, report_graph, results_text = shifty.validate(data, shapes)
   # conforms → False
   # report_graph → rdflib.Graph with sh:ValidationReport
   # results_text → human-readable summary

Keyword arguments
~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Argument
     - Description
   * - ``graph_mode``
     - ``"union"`` (default), ``"data"``, or ``"union-all"`` — controls which triples are visible to path traversal and SPARQL evaluation
   * - ``shape_names``
     - Optional list of named shape IRIs to use as top-level validation entry points; referenced helper shapes are still evaluated normally
   * - ``infer``
     - ``True`` (default) — run SHACL-AF ``sh:rule`` entries to a fixed point before validating; set ``False`` to skip inference

.. code-block:: python

   # Skip inference, validate data only
   conforms, report, text = shifty.validate(data, shapes, infer=False)

   # Use data ∪ shapes for path/SPARQL evaluation
   conforms, report, text = shifty.validate(data, shapes, graph_mode="union")

   # Validate only selected named shapes as entry points
   conforms, report, text = shifty.validate(
       data,
       shapes,
       shape_names=["http://example.org/PersonShape"],
   )

Embedded shapes
~~~~~~~~~~~~~~~

If shapes are embedded in the data graph, omit the second argument or pass ``None``:

.. code-block:: python

   conforms, report, text = shifty.validate("combined.ttl")
   conforms, report, text = shifty.validate(combined_graph, None)

Do **not** pass an empty ``rdflib.Graph()`` for this case — an empty graph is treated
as an explicit empty shapes graph and will produce no violations.

validate_algebra
~~~~~~~~~~~~~~~~

Returns structured ``Violation`` objects instead of an RDF report graph:

.. code-block:: python

   result = shifty.validate_algebra(data, shapes)

   print(result.conforms)           # False
   for v in result.violations:
       print(v.focus_node)          # IRI of the failing focus node
       print(v.shape_name)          # shape that targeted this node (if any)
       for r in v.reasons:
           print(r.message)         # human-readable failure description
           print(r.path)            # property path checked, if applicable
           print(r.value)           # the offending value node

Accepts the same ``infer=`` and ``graph_mode=`` keyword arguments as ``validate()``.

PreparedValidator
~~~~~~~~~~~~~~~~~

For repeated validation against the same shapes graph, compile once:

.. code-block:: python

   validator = shifty.PreparedValidator(shapes)

   # Validate many data graphs against the compiled shapes
   for data_file in data_files:
       conforms, report, text = validator.validate(data_file)

   # Or use the structured result form
   result = validator.validate_algebra(data, infer=False)

File inputs
~~~~~~~~~~~

.. code-block:: python

   import pathlib

   conforms, report, text = shifty.validate(
       pathlib.Path("data.ttl"),
       pathlib.Path("shapes.ttl"),
   )

``pathlib.Path`` inputs are parsed directly by Rust. ``rdflib.Graph`` inputs
use N-Triples for the Python-to-Rust transfer to avoid rdflib's slower Turtle
serializer.

Property witnesses
-------------------

``validate`` / ``validate_algebra`` report violations. ``PreparedValidator.witnesses()``
is their inverse: for every focus node that *conforms* to a target/profile node
shape, it returns the values each ``sh:property`` shape's ``sh:path`` resolved to.
Useful when a SHACL profile doubles as an extraction schema — e.g. disambiguating
several same-typed sensors on a piece of equipment via ``sh:qualifiedValueShape``.

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
       ] ;
       sh:property [
           zea:role ex:ReturnAirTempRole ;
           sh:path ex:hasPoint ;
           sh:qualifiedValueShape [ sh:hasValue ex:rat ] ;
           sh:qualifiedMinCount 1 ;
           sh:qualifiedMaxCount 1 ;
       ] .
   ex:OutsideAirTempRole zea:roleName "outsideAirTemp" .
   ex:ReturnAirTempRole zea:roleName "returnAirTemp" .
   """
   data = """
   @prefix ex: <http://example.org/> .
   ex:vav1 a ex:Vav ; ex:hasPoint ex:oat, ex:rat, ex:sat, ex:mat .
   """

   validator = shifty.PreparedValidator(shapes)
   for w in validator.witnesses(data, key_path="zea:role/zea:roleName"):
       print(w.focus, w.key, w.values)
   # <http://example.org/vav1> outsideAirTemp ['<http://example.org/oat>']
   # <http://example.org/vav1> returnAirTemp  ['<http://example.org/rat>']

``key_path`` is a SPARQL 1.1 property path expression — sequence ``/``, alternation
``|``, inverse ``^``, and the Kleene forms ``*``/``+``/``?`` are all supported —
evaluated from each ``sh:property`` shape's own node, over the shapes graph, to
produce a stable key. The key above isn't a direct annotation on the property
shape — it lives one hop further away, through an intermediate role-descriptor
node — which a bare predicate lookup couldn't reach but a path can. A direct
annotation (``zea:roleName "outsideAirTemp"`` right on the ``sh:property`` shape)
would just be ``key_path="zea:roleName"``; a descriptor that points *at* the
property shape instead of the other way around would use an inverse hop,
``key_path="^zea:describes/zea:roleName"``. Prefixes resolve against the shapes
document's declared ``@prefix``\ es.

Each ``PropertyWitness`` has:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Attribute
     - Description
   * - ``focus``
     - The focus node that conformed
   * - ``shape``
     - The node shape (application profile) it conformed to
   * - ``key``
     - The lexical value reached by ``key_path``, or the property shape's own
       IRI/blank-node id when the path resolves to no value (or is omitted)
   * - ``values``
     - The deduped ``sh:path`` bindings, rendered in full (``<iri>``, ``"lit"``,
       ``"lit"@lang``, ``"lit"^^<datatype>``) so IRI and literal bindings stay
       distinguishable — narrowed to the ``sh:qualifiedValueShape`` matches when
       the property shape declares one

infer
-----

Run SHACL-AF ``sh:rule`` entries to a fixed point:

.. code-block:: python

   result = shifty.infer(data, rules)

   print(result.inferred_count)    # number of newly derived triples
   g = result.graph()              # rdflib.Graph with original + inferred data

.. code-block:: python

   rules = """
   @prefix sh: <http://www.w3.org/ns/shacl#> .
   @prefix ex: <http://example.org/> .

   ex:RectangleShape a sh:NodeShape ;
       sh:targetClass ex:Rectangle ;
       sh:rule [
           a sh:TripleRule ;
           sh:subject sh:this ;
           sh:predicate ex:area ;
           sh:object [ sh:path ex:width ] ;
       ] .
   """

   data = """
   @prefix ex: <http://example.org/> .
   ex:r1 a ex:Rectangle ; ex:width 3 ; ex:height 2 .
   """

   result = shifty.infer(data, rules)
   print(result.inferred_count)    # 1
   g = result.graph()              # graph contains ex:r1 ex:area 3 .

If rules are embedded in the data graph, omit the second argument or pass ``None``:

.. code-block:: python

   result = shifty.infer(combined_data_and_rules)
   result = shifty.infer(combined_data_and_rules, None)

Passing ``rdflib.Graph()`` as the second argument means "run with an explicit
empty rules graph" — no embedded rules will be parsed.

graph_mode
----------

Both ``validate()`` and ``validate_algebra()`` accept a ``graph_mode`` keyword argument
that controls which triples are visible to path traversal and SPARQL evaluation:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Mode
     - Behaviour
   * - ``"data"``
     - Focus nodes come from the data graph; path traversal and SPARQL use data only
   * - ``"union"`` *(default)*
     - Focus nodes from data; paths and SPARQL use data ∪ shapes
   * - ``"union-all"``
     - Focus nodes and evaluation both use data ∪ shapes

``infer()`` does not accept ``graph_mode``.

shape_names
-----------

Both ``validate()`` and ``validate_algebra()`` accept ``shape_names=[...]`` to
validate only selected named shapes as top-level entry points:

.. code-block:: python

   result = shifty.validate_algebra(
       data,
       shapes,
       shape_names=["http://example.org/PersonShape"],
   )

Only target-bearing statements owned by the selected named shapes are used as
entry points. Dependencies referenced from those entries are still evaluated
normally, including helper shapes reached through ``sh:node``, ``sh:property``,
qualified value shapes, and boolean shape expressions. Shape names may be
passed as bare IRIs or wrapped in angle brackets.

For full API documentation including the ``RepairSession`` interface, see
`docs.rs/shifty-engine <https://docs.rs/shifty-engine>`_.
