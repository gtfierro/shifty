Shape map reference
===================

A shape map is a binding table over an evidence run: one ``Mapping`` per
selected ``(shape, focus)`` pair, each a dictionary from a typed ``Key`` — the
property obligation — to a ``Binding`` — the values that satisfied it, or the
information needed to see why it did not.

It is similar in spirit to a ShEx shape map, but deliberately keeps the
property-level bindings, so a profile can serve as an extraction schema and as
a contract that needs repair. :doc:`../how-to/shape-maps` shows it in use.

``shape_map``
-------------

.. code-block:: python

   shifty.shape_map(
       shacl_graph,
       data_graph=None,
       *,
       name_path="sh:name",
       value_paths=None,
       shape_names=None,
       minimum_severity="info",
       infer=True,
       graph_mode="union",
       base=None,
   ) -> ShapeMap

A convenience over ``EvidenceSession(...).validate()`` followed by
``ShapeMap.from_run``. Note the argument order: **shapes first**, unlike
``validate()``.

.. list-table::
   :widths: 22 78
   :header-rows: 1

   * - Argument
     - Meaning
   * - ``name_path``
     - Property path naming the *slot*, evaluated from the authored
       property-shape node over the **shapes** graph. Constant per property
       shape. ``None`` skips slot naming.
   * - ``value_paths``
     - ``{label: path}`` annotating each *bound value*, evaluated from the
       value node over the graph validation read. Varies per row. Resolved
       lazily.
   * - ``shape_names``, ``minimum_severity``, ``infer``, ``graph_mode``, ``base``
     - As for ``EvidenceSession`` — see :doc:`evidence`.

``ShapeMap.from_run``
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   session = shifty.EvidenceSession(shapes, data, infer=False)
   run = session.validate()
   smap = shifty.ShapeMap.from_run(run, session, name_path="sh:name",
                                   value_paths=None)

Pass the session. Canonical failure evidence omits the passing siblings of a
failed conjunction; with the session the map materializes them on demand, and
without it such a binding has ``values is None`` — explicitly unknown rather
than guessed.

``ShapeMap``
------------

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``conforms``
     - Whether the whole run conformed.
   * - ``shape_names``
     - The shape IRIs present in the map.
   * - ``smap[shape_name]``
     - The list of ``Mapping`` for that shape.
   * - ``iter(smap)``
     - Every ``Mapping``, across shapes.
   * - ``conforming(shape_name)`` / ``nonconforming(shape_name)``
     - Those mappings split by conformance.
   * - ``for_focus(focus)``
     - Every mapping for one node, across profiles. Accepts a ``Term``, an
       N-Triples term, or a bare IRI. Indexes on first use.
   * - ``to_dict()``
     - A JSON-compatible summary keyed by shape, focus, and rendered key, with
       terms in N-Triples spelling.

``Mapping``
-----------

Implements ``collections.abc.Mapping``, so ``items()``, ``keys()``, ``len()``,
and ``mapping[key]`` behave normally. ``__getitem__`` also accepts a rendered
key string, for interactive use.

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``focus``
     - The focus node, as a ``Term``.
   * - ``shape_name``
     - The shape IRI.
   * - ``target``
     - The selector that chose this focus.
   * - ``conforms``
     - Whether this pair conformed.
   * - ``successful`` / ``unsuccessful``
     - Ordered ``(Key, Binding)`` lists for each side.
   * - ``value_map(*, by="key", python=False)``
     - Successful bindings only. ``by="name"`` keys by resolved name instead of
       ``Key``, falling back to ``str(key)`` for an unnamed binding;
       ``python=True`` converts terms to plain Python values.
   * - ``by_name(name)``
     - The first binding with that name, in authored order. Names need not be
       unique; use ``Key`` when uniqueness matters.
   * - ``evaluation``
     - The underlying ``FocusEvaluation`` — the way back to the full evidence
       tree or a repair.

``Binding``
-----------

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``ok``
     - Whether the obligation was satisfied.
   * - ``name`` / ``names``
     - The resolved slot name, and every value ``name_path`` reached.
   * - ``values``
     - The bound values as ``Term`` objects. ``None`` means *unknown* — a
       passing sibling elided from canonical evidence with no session to
       recover it.
   * - ``partial_values``
     - Values that qualified so far, on an unsatisfied obligation.
   * - ``rejected_values``
     - Candidates the qualifier rejected.
   * - ``missing``
     - How many more values are required.
   * - ``min`` / ``max``
     - Declared cardinality bounds.
   * - ``observed``
     - The count the evidence saw, where available.
   * - ``expects_single``
     - True exactly for a ``1..1`` obligation.
   * - ``severity``
     - Effective SHACL severity, lowercase.
   * - ``annotated_values``
     - ``BoundValue`` objects pairing each term with its ``value_paths``
       annotations.
   * - ``annotations``
     - The annotation map directly.
   * - ``evidence``
     - The evidence subtree this binding was derived from.
   * - ``explain()``
     - Human-readable rendering.

``Key``
-------

Immutable and hashable, so it works as a dictionary key or set member.

.. list-table::
   :widths: 22 78
   :header-rows: 1

   * - Field
     - Values
   * - ``path``
     - ``Id``, ``Pred(iri)``, ``Inv(path)``, ``Seq(paths)``, ``Alt(paths)``, or
       ``Star(path)``.
   * - ``qualifier``
     - ``Cls(iri)``, ``Const(term)``, ``Datatype(iri)``, or ``ShapeRef(id)``,
       when the qualification is recoverable from the shape.
   * - ``ordinal``
     - Distinguishes two authored obligations sharing a path and qualifier.
       Part of equality and hashing.

``str(key)`` renders as e.g. ``hasPoint→SupplyAirTemperatureSensor``. That form
compacts IRIs to local names, so it is not globally unique and is intended for
logs and table headings, not program logic.

.. code-block:: python

   from shifty import Cls, Key, Pred

   match key:
       case Key(Pred("http://example.org/hasPoint"), Cls(sensor_class)):
           ...

Terms
-----

``Iri(value)``, ``Literal(value, datatype=None, language=None)``, and
``BNode(id)``, sharing the base class ``Term``. Focus nodes and bound values are
these rather than rendered strings, so an IRI, a blank node, and a literal with
similar text cannot be confused.

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Method
     - Meaning
   * - ``Term.parse(text)``
     - Parse an N-Triples spelling.
   * - ``.n3()``
     - Render as N-Triples.
   * - ``Literal.to_python()``
     - Convert common numeric and boolean datatypes; unknown datatypes keep
       their lexical form.
   * - ``.to_rdflib()``
     - Convert to the rdflib equivalent. Requires ``rdflib``.
