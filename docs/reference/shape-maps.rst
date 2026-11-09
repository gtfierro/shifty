Shape map reference
===================

A shape map is a binding table over an evidence run: one ``Mapping`` per
selected ``(shape, focus)`` pair, each a dictionary from a typed ``Key`` — the
property obligation — to a ``Binding`` — the values that satisfied it, or the
information needed to see why it did not.

Like a ShEx shape map, it associates shapes with focus nodes. It also keeps
property bindings, allowing a profile to serve as an extraction schema and
identify constraints that need repair. :doc:`../how-to/shape-maps` shows it in use.

``shape_map``
-------------

.. code-block:: python

   shifty.shape_map(
       data_graph,
       shacl_graph=None,
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
``ShapeMap.from_run``. Its graph arguments follow ``validate()`` and
``validate_algebra()``: data first, then shapes. If ``shacl_graph`` is omitted,
``data_graph`` supplies both shapes and data.

.. list-table::
   :widths: 22 78
   :header-rows: 1

   * - Argument
     - Meaning
   * - ``name_path``
     - Property path naming the *slot*, evaluated from the authored
       property-shape node over the **shapes** graph. Constant per property
       shape. ``None`` skips slot naming. The standard ``sh:`` prefix remains
       available for the default ``"sh:name"`` even when the shapes input has
       no prefix declarations.
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

Failure witnesses for universal value checks (such as ``sh:pattern`` or
``sh:datatype``) can also omit valid values. With the session, the map lazily
recovers accepted and rejected candidates from the authored property. Without
it, ``values`` and ``partial_values`` are ``None`` when the complete qualifying
set is unavailable; ``rejected_values`` still exposes witnessed failures.
Recovery requires the session's original data snapshot. An unrelated session,
or the original session passed alongside a ``revalidate()`` result, is treated
as unavailable for graph-based resolution so old values cannot replace the
new run's evidence.

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
       terms in N-Triples spelling. Binding summaries include ``position``,
       ``order`` (a decimal string or null), and ``constraint_components``
       (an array of IRI strings or null), plus ``kind`` and ``algebra_kind``.

``Mapping``
-----------

Implements ``collections.abc.Mapping``, so ``items()``, ``keys()``, ``len()``,
and ``mapping[key]`` behave normally. ``__getitem__`` also accepts a rendered
key string, for interactive use.

Bindings are ordered by ascending ``sh:order`` (including decimal values),
followed by bindings with no order. Ties use source IRIs or canonical anonymous
source identities, independently of RDF iteration and Turtle statement order.
Anonymous tie-breaking is stable for the same shapes graph and library version;
it is not a persistent identifier across schema edits or library upgrades.
Duplicate-key ordinals are assigned in this order, consistently for every focus.

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
     - Successful bindings with known values, including ``[]`` for empty slots.
       Failed bindings and unavailable values (``None``) are omitted.
       ``by="name"`` keys by resolved name instead of
       ``Key``, falling back to ``str(key)`` for an unnamed binding;
       ``python=True`` converts terms to plain Python values.
   * - ``by_name(name)``
     - The first binding with that name, in binding order. Names need not be
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
   * - ``position``
     - One-based position among all obligations for this mapping, including
       failing obligations. Separate from key equality and hashing.
   * - ``order``
     - Authored ``sh:order`` as a ``decimal.Decimal``, or ``None``.
   * - ``constraint_components``
     - Sorted tuple of full authored constraint-component IRI strings, including
       activated custom components. Describes declarations, not just failures,
       and excludes components inside nested qualifier shapes. ``None`` means
       the binding has no single authored RDF source; ``()`` means its source
       declares no component (for example an unbounded qualified value shape).
   * - ``algebra_kind``
     - Lowered algebra tag, such as ``"count"``. Intended for compiler and
       evidence inspection; ``key.kind`` describes the authored SHACL family.
   * - ``name`` / ``names``
     - The resolved slot name, and every value ``name_path`` reached.
   * - ``values``
     - The bound values as ``Term`` objects. ``None`` means *unknown* — a
       passing sibling elided from canonical evidence with no session to
       recover it. An unbounded ``sh:qualifiedValueShape`` still yields its
       qualifying values, despite normalizing to a vacuous validation
       constraint.
       Without a session, bindings combining distinct count domains report
       ``None`` rather than projecting matches from an unrelated domain.
       Source provenance distinguishes universal value checks from authored
       counts even when their evidence has the same structure. A successful
       universal whose trace contains only the zero counterexample count has
       unavailable values, rather than a known empty set.
   * - ``partial_values``
     - The same qualifying values as ``values`` on an unsatisfied obligation,
       or ``None`` when that set is unknown. Empty on a successful obligation.
   * - ``rejected_values``
     - Candidates the qualifier rejected.
   * - ``missing``
     - How many more values are required.
   * - ``min`` / ``max``
     - Declared cardinality bounds. When a binding combines count domains,
       these may apply to different value sets and do not form one interval.
   * - ``observed``
     - Exact count in the authored cardinality domain: all path values for an
       ordinary count, qualifying values for a qualified count, or all checked
       path values for a universal value check. Invalid values still contribute
       to ordinary cardinality. ``None`` means the count cannot be recovered,
       or the binding combines multiple distinct count domains (for example
       ``sh:minCount`` and ``sh:qualifiedMinCount``).
   * - ``expects_single``
     - True when a single count domain declares a ``1..1`` obligation.
       False for mixed domains, whose lower and upper bounds may apply to
       different value sets.
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
     - SHACL-oriented summary of the declared constraints, status, counts,
       qualifying values, and rejected candidates. Unavailable observations
       remain explicitly unavailable. For compound bindings the listed
       declarations do not imply that every component failed.
       Bounds applying to different value sets (for example, ordinary and
       qualified counts) are not rendered as one required-count interval.
   * - ``explain(algebra=True)``
     - The underlying evidence tree, retaining tags such as ``count_high``.
       Also the default when authored component metadata is unavailable.

``Key``
-------

Immutable and hashable, so it works as a dictionary key or set member.
``kind`` describes the authored SHACL constraint family. For example, a
``sh:pattern`` binding has ``kind="pattern"``, even though its implementation
uses a count. ``binding.algebra_kind`` exposes that implementation tag.

Ordinary minimum/maximum cardinalities share ``"count"``; qualified
cardinalities share ``"qualified_count"``. Other core families use snake-case
names such as ``"datatype"``, ``"class"``, ``"node_kind"``, ``"has_value"``,
and ``"less_than_or_equals"``. Several families on one binding yield
``"compound"``. A single custom component yields ``"custom"``; several
distinct custom components also yield ``"compound"``. A source declaring no
component yields ``"unconstrained"``. If authored metadata is unavailable,
``kind`` falls back to the algebra tag.

Use ``binding.constraint_components`` for the full component identities,
including every family in a compound binding. ``kind`` is part of key equality
and hashing; include it when constructing a key to look up a binding.

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
       When several qualifiers are available, a representative is selected by
       qualifier type name and full RDF identity, independently of graph order.
       This representative does not describe every constraint on the binding.
   * - ``ordinal``
     - Distinguishes two authored obligations sharing a path and qualifier.
       Part of equality and hashing.
   * - ``kind``
     - Authored constraint family, also used to render pathless keys.

``str(key)`` renders as e.g. ``hasPoint→SupplyAirTemperatureSensor``. That form
compacts IRIs to local names, so it is not globally unique and is intended for
logs and table headings, not program logic.

The pattern-matching example requires Python 3.10 or newer. The package also
supports Python 3.9.

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
