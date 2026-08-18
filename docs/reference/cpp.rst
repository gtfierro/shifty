C++ API reference
=================

The C++ SDK is a C++17 static library embedding the same Rust engine the CLI,
Python, and WebAssembly frontends wrap. Parsing, SPARQL, SHACL-AF inference,
validation, and evidence-carrying validation all run in Rust; the C++ side is a
thin RAII layer over a stable C ABI. The public API is a single header,
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
   * - ``witnesses(dataset, options={})``
     - The inverse of validation: observed ``sh:property`` bindings at
       *conforming* focus nodes, as ``PropertyWitness`` rows. ``options
       .key_path`` (a SPARQL property path over the shapes graph) produces a
       stable key per property shape.

``ValidationOptions`` carries ``graph_mode`` (``Data``/``Union``/``UnionAll``),
``run_inference``, ``minimum_severity`` (``Severity::Info``/``Warning``/
``Violation`` — findings below the threshold stay reported but stop failing
``conforms()``), ``shape_names`` (limit validation to named entry shapes), and
``key_path`` (for ``witnesses()``).

``EvidenceSession``
-------------------

Evidence-carrying validation over one immutable snapshot. Inference,
normalization, stratification, indexing, and SPARQL preparation happen once in
the constructor and are reused by every call; ``graph_mode`` and
``run_inference`` are read there and fixed, while ``minimum_severity`` and
``shape_names`` stay per call.

.. list-table::
   :widths: 34 66
   :header-rows: 1

   * - Member
     - Meaning
   * - ``EvidenceSession(validator, dataset, options={})``
     - Prepare the snapshot. The validator and dataset need not outlive it.
   * - ``constraints_json()``
     - The source/normalized constraint catalogs this snapshot's evidence
       refers to by id, as JSON. Fixed per snapshot.
   * - ``validate(options={})``
     - The complete coverage horizon as an ``EvidenceRun``: every authored
       statement, every selected focus, one evidence polarity each.
   * - ``validate_conformance(options={})``
     - The same pairs decided with one short-circuiting test: a
       ``ConformanceRun`` of counts, no evidence materialized. Does not honor
       ``minimum_severity``.
   * - ``find_failures(options={})``
     - The conformance pass retaining the failing ``SelectedPair``s, for
       scan-then-explain.
   * - ``explain(failures, index)``
     - Materialize evidence for one pair of a failure list without re-running
       target selection.
   * - ``explain(statement, focus_node)``
     - Explain an arbitrary ``(normalized statement, focus)`` pair by naming
       the focus's N-Triples spelling.
   * - ``shape_map(run, options={})``
     - The shape-map view of a run: typed ``Key`` -> ``Binding`` per selected
       pair. See `Shape maps`_ below.
   * - ``binding_names(name_path="sh:name")``
     - Raw source constraint id -> the values ``name_path`` reaches from that
       constraint's originating shapes-graph node (over the shapes graph).
   * - ``shape_name_of(constraint_id)``
     - The named shape IRI a source constraint was lowered from, when it has
       one.
   * - ``resolve_path(nodes, path)``
     - Batch-evaluate a SPARQL 1.1 property path from each N-Triples node over
       the session's evaluation graph (the data graph in ``Data`` mode, the
       union otherwise), in input order.

An ``EvidenceRun`` carries ``statements()``, each a ``StatementEvidence`` with
``selected_foci`` of ``FocusEvidence`` — ``focus_node``, ``status`` /
``passed()``, ``evidence_json``, and ``explanation``. ``EvidenceRun`` also
offers ``json()`` and ``compact_json(include_catalog)`` with the free function
``expand_evidence(compact, catalog={})`` round-tripping losslessly. A run from
``explain()`` carries an empty constraint catalog; supply the snapshot's
``constraints_json()`` when expanding a catalog-less encoding.

Shape maps
----------

One level above the evidence trees is the shape-map view — the C++ port of the
Python ``shifty.shape_map()``. For every selected ``(shape, focus)`` pair it
produces a ``Mapping`` of the shape's property obligations: bound keys carry the
values the data supplied as typed ``Term``\ s (exact even on
partially-conforming foci), unbound keys carry the shortfall count and
near-misses.

.. code-block:: cpp

   shifty::ShapeMapOptions opts;
   opts.name_path = "sh:name";            // author's name per slot, shapes graph
   opts.value_paths = {{"ts", "demo:hasTimeseriesId"}};  // annotate each value

   const shifty::EvidenceSession session(validator, dataset);
   const auto smap = session.shape_map(session.validate(), opts);

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
     - The focus node, rendered in full — the same spelling
       ``FocusEvidence::focus_node`` carries.
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
   * - ``evaluation()``
     - The underlying ``FocusEvidence`` — the way back to the full evidence
       tree.

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
     - Whether the obligation was satisfied; ``"pass"`` or ``"fail"``.
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
     - The count the evidence saw, where available.
   * - ``expects_single()``
     - True exactly for a ``1..1`` obligation.
   * - ``severity()``
     - Effective SHACL severity, lowercase.
   * - ``annotated_values()``
     - ``BoundValue`` objects pairing each term with its ``value_paths``
       annotations.
   * - ``annotations()``
     - ``label -> value -> reached``, pivoted from ``annotated_values()``.
   * - ``evidence_json()``
     - This key's evidence subtree as JSON. Empty when the evidence was not
       materialized.
   * - ``explain()``
     - Human-readable rendering.

A partially-conforming focus yields both sides: its failing keys report
``missing()``/``rejected_values()`` and the witness subtree, while its passing
keys are materialized eagerly (the raw failure witness elides them) so a repair
driver sees every value the focus can already supply.

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
     - The constraint tag fallback for pathless keys (e.g. ``count``).
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

Session helpers
~~~~~~~~~~~~~~~

The three helpers the features build on, mirroring the Python
``EvidenceSession``:

.. list-table::
   :widths: 34 66
   :header-rows: 1

   * - Member
     - Meaning
   * - ``binding_names(name_path="sh:name")``
     - ``std::map<uint32_t, std::vector<std::string>>``: raw (source)
       constraint id -> the values ``name_path`` reaches from that constraint's
       originating shapes-graph node, evaluated over the shapes graph.
       ``name_path = None`` means ``sh:name``; constraints with no provenance
       or no matches are omitted.
   * - ``shape_name_of(constraint_id)``
     - ``std::optional<std::string>`` — the raw schema's shape name (the IRI of
       the named RDF node) for a constraint id, when it has one.
   * - ``resolve_path(nodes, path)``
     - ``std::vector<std::pair<std::string, std::vector<std::string>>>`` —
       batch-evaluate ``path`` (a SPARQL 1.1 property path, same grammar as
       ``name_path``) from each N-Triples node over the session's evaluation
       graph, in input order.

See also
--------

- :doc:`shape-maps` — the Python object model this page mirrors.
- :doc:`evidence` — the evidence data model the shape map sits on.
- :doc:`../how-to/shape-maps` — the shape map how-to (Python, but the
  semantics carry over).
- ``cpp/README.md`` — build instructions and the same API in prose.
