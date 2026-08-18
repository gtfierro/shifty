C++ API reference
=================

The C++ SDK is a C++17 static library embedding the same Rust engine the CLI,
Python, and WebAssembly frontends wrap. Parsing, SPARQL, SHACL-AF inference,
validation, and shape-map extraction all run in Rust; the C++ side is a thin
RAII layer over a stable C ABI. The public API is a single header,
``shifty/shifty.hpp``.

:doc:`../how-to/install` covers building with CMake and linking. This page
describes the API surface; the shape-map vocabulary (typed keys, bindings, and
terms) has its own section below and mirrors the Python
:doc:`shape-maps` reference.

``Dataset``
-----------

An in-memory RDF graph owned by the engine. Move-only; read-only operations may
run concurrently only with external synchronization against load operations.
Multiple sources union at the triple level: call ``load``/``load_file``
repeatedly, or pass several documents where a single graph is accepted.

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``Dataset()``
     - An empty graph.
   * - ``load(data, format=Auto, base_iri={})``
     - Parse RDF from memory (``std::string_view``) and add it.
   * - ``load_file(path, format=Auto, base_iri={})``
     - Parse an RDF file and add it.
   * - ``size()``
     - Unique triples in the dataset.
   * - ``ntriples()``
     - The whole graph as N-Triples.
   * - ``query(sparql)``
     - A ``QueryResult`` for a SELECT / ASK / CONSTRUCT / DESCRIBE query.

``QueryResult`` carries the result form (``QueryResultKind``), ``data()``
(SPARQL Results JSON for SELECT/ASK, N-Triples for CONSTRUCT/DESCRIBE),
``media_type()``, and ``boolean_value()`` for ASK.

``PreparedValidator``
---------------------

Parses and normalizes shapes once and validates any dataset against them.

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``PreparedValidator(shapes, format=Auto, base_iri={})``
     - Prepare shapes from memory.
   * - ``from_file(path, ...)`` / ``from_files(paths, ...)``
     - Prepare shapes from one or several files, unioned at the triple level.
   * - ``from_memory(documents, ...)``
     - Prepare shapes from several in-memory documents, unioned the same way.
   * - ``diagnostics_json()``
     - Parser/lowering diagnostics as a JSON array.
   * - ``validate(dataset, options={})``
     - W3C ``sh:ValidationReport`` path: returns a ``ValidationResult`` with
       ``conforms()``, ``report_turtle()``, and ``results_text()``.
   * - ``validate_algebra(dataset, options={})``
     - Structured path: returns an ``AlgebraResult`` with a typed
       ``violations()`` tree instead of an RDF report graph.
   * - ``shape_map(dataset, options={})``
     - Returns configuration-oriented typed key/value bindings.

``ValidationOptions`` carries ``graph_mode`` (``Data``/``Union``/``UnionAll``),
``run_inference``, ``minimum_severity`` (``Severity::Info``/``Warning``/
``Violation`` — findings below the threshold stay reported but stop failing
``conforms()``), ``shape_names`` (limit validation to named entry shapes).

Shape maps
----------

The shape-map view is the C++ port of Python ``shifty.shape_map()``. For every
selected ``(shape, focus)`` pair it
produces a ``Mapping`` of the shape's property obligations: bound keys carry the
values the data supplied as typed ``Term``\ s (exact even on
partially-conforming foci), unbound keys carry the shortfall count and
near-misses.

.. code-block:: cpp

   shifty::ShapeMapOptions opts;
   opts.name_path = "sh:name";            // author's name per slot, shapes graph
   opts.value_paths = {{"ts", "demo:hasTimeseriesId"}};  // annotate each value

   const auto smap = validator.shape_map(dataset, opts);

   for (const auto &name : smap.shape_names()) {
       for (const auto &mapping : smap.mappings(name)) {
           for (const auto *binding : mapping.successful()) {
               std::cout << binding->key().str() << ":";
               for (const auto &value : binding->values()) {
                   std::cout << " " << value.n3();
               }
               std::cout << "\n";
           }
       }
   }

``ShapeMapOptions``
~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``graph_mode`` / ``run_inference`` / ``minimum_severity``
     - Validation behavior used while extracting bindings.
   * - ``shape_names``
     - Optional named entry shapes; empty selects every target-bearing shape.
   * - ``name_path``
     - Property path naming the *slot*, evaluated from the authored
       property-shape node over the **shapes** graph. Constant per property
       shape. Defaults to ``"sh:name"``; set empty to skip slot naming.
   * - ``value_paths``
     - ``std::vector<std::pair<std::string, std::string>>`` — ``label -> path``
       pairs annotating each *bound value*, evaluated from the value node over
       the graph validation read. Varies per row. Resolved eagerly, one batched
       call per label.

Unlike Python, the C++ shape map is materialized eagerly at build time, so a
``ShapeMap`` is a plain value that never needs its session to outlive it.

``ShapeMap``
~~~~~~~~~~~~

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``conforms()``
     - Whether the whole run conformed.
   * - ``shape_names()``
     - Every shape identity with at least one authored statement — named shape
       IRIs, or ``_:statement-N`` placeholders for anonymous shapes.
   * - ``mappings(shape_name)``
     - The ``Mapping``\ s of one shape (by name; throws ``std::out_of_range``
       for an unknown shape), or by index with ``mappings(index)``.
   * - ``all()``
     - Every ``Mapping`` across shapes, as pointers.
   * - ``conforming(shape_name)`` / ``nonconforming(shape_name)``
     - Those mappings split by conformance.
   * - ``for_focus(focus)``
     - Every mapping for one node, across profiles. Accepts a ``Term`` or an
       N-Triples string; a bare IRI is wrapped in angle brackets first.
   * - ``mapping_count(index)`` / ``shape_count()`` / ``total_mappings()``
     - Sizes.
   * - ``to_json()``
     - A JSON-compatible summary keyed by shape, focus, and rendered key, with
       terms in N-Triples spelling — the analogue of Python's ``to_dict()``.

``Mapping``
~~~~~~~~~~~

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``focus()``
     - The focus node, rendered in full N-Triples form.
   * - ``shape_name()``
     - The shape IRI, or empty for an anonymous shape.
   * - ``target()``
     - The selector that chose this focus, rendered.
   * - ``conforms()``
     - Whether this pair conformed.
   * - ``bindings()``
     - Every ``Binding``, in authored order.
   * - ``successful()`` / ``unsuccessful()``
     - Pointers to the bound / unbound bindings, in authored order.
   * - ``size()`` / ``empty()``
     - The number of bindings.
   * - ``by_name(name)``
     - The first binding whose ``name()`` matches; names need not be unique.
       Throws ``std::out_of_range`` when none matches.
   * - ``find(key)``
     - The binding with the given typed ``Key`` or rendered string, or
       ``nullptr``.
   * - ``value_map()`` / ``value_map_by_name()``
     - Successful bindings only, as ``std::map<Key, std::vector<Term>>``
       (respectively keyed by ``binding.name()`` falling back to ``str(key)``)
       for application configuration.

``Binding``
~~~~~~~~~~~

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``key()``
     - The typed ``Key`` (``path()``/``qualifier()`` accessors).
   * - ``ok()`` / ``status()``
     - Whether the key is usable; ``BindingStatus::Bound`` or ``Unbound``.
   * - ``name()`` / ``names()``
     - The resolved slot name (first value of ``names()``, or ``nullptr``), and
       every value ``name_path`` reached.
   * - ``values()``
     - The bound values as ``Term`` objects. For a failing key these are the
       qualifying near-matches (same as ``partial_values()``).
   * - ``partial_values()``
     - Values that qualified so far, on an unsatisfied obligation.
   * - ``rejected_values()``
     - Candidates the qualifier rejected.
   * - ``missing()``
     - How many more values are required.
   * - ``min()`` / ``max()``
     - Declared cardinality bounds (``std::optional<std::size_t>``).
   * - ``observed()``
     - The observed qualifying-value count, where available.
   * - ``expects_single()``
     - True exactly for a ``1..1`` obligation.
   * - ``annotated_values()``
     - ``BoundValue`` objects pairing each term with its ``value_paths``
       annotations.
   * - ``annotations()``
     - ``label -> value -> reached``, pivoted from ``annotated_values()``.
A partially-conforming focus yields both sides: its failing keys report
``missing()``/``rejected_values()``, while its passing keys are materialized so
a configuration consumer sees every value the focus can already supply.

``Key``
~~~~~~~

A typed, hashable key: the property shape's path plus its qualifier class when
one is declared, disambiguated by ``ordinal()`` when several authored
obligations share a path and qualifier.

.. list-table::
   :widths: 22 78
   :header-rows: 1

   * - Member
     - Values
   * - ``path()``
     - ``std::optional<Path>`` — ``PathKind::Id``/``Pred``/``Inverse``/``Seq``/
       ``Alt``/``Star`` with ``iri()`` and ``children()``; ``std::nullopt`` for
       a pathless key (nodeKind, …).
   * - ``qualifier()``
     - ``std::optional<Qualifier>`` — ``QualifierKind::Cls``/``Const``/
       ``Datatype``/``ShapeRef`` with ``iri()`` (and ``term()`` for ``Const``),
       when the qualification is recoverable from the shape.
   * - ``ordinal()``
     - Distinguishes two authored obligations sharing a path and qualifier.
       Part of equality and ordering.
   * - ``kind()``
     - Typed ``KeyKind`` fallback for pathless keys.
   * - ``str()``
     - Renders as e.g. ``hasPoint→SupplyAirTemperatureSensor``. Compacts IRIs
       to local names, so it is not globally unique — logs and headings, not
       program logic.

``Path::parse_json()`` decodes the serde spelling of the algebra ``Path`` into
a typed ``Path`` for pattern matching; ``Term``\ s are ``TermKind::Iri`` /
``Literal`` / ``BNode`` with ``value()``, ``datatype()``, ``language()``, and
``n3()`` (N-Triples rendering matching ``terms.py`` — ``xsd:string`` datatypes
are omitted, lexical escapes applied). ``Term`` supports ``operator==`` and
``operator<`` so it works as a ``std::map``/``std::set`` member.

See also
--------

- :doc:`shape-maps` — the Python object model this page mirrors.
- :doc:`../how-to/shape-maps` — the shape map how-to (Python, but the
  semantics carry over).
- ``cpp/README.md`` — build instructions and the same API in prose.
