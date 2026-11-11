Extract bindings with a shape map
=================================

A shape map extracts the values that satisfied a shape's property constraints.
For example, a shape requiring a VAV to have a supply-air temperature sensor
can also identify that sensor for an application.

The map contains one entry per selected ``(shape, focus)`` pair, with bindings
for each property obligation. It uses the validation evidence, so you can
retrieve these values without writing a separate query for the shape's paths
and qualifiers.

Get a map
---------

.. literalinclude:: ../examples/shape-map.py
   :language: python

.. program-output:: python examples/shape-map.py
   :cwd: ..

Both VAVs appear. ``vav-1`` is missing its airflow sensor and therefore does not
conform — but it still bound the temperature sensor correctly, and that binding
is in the map.

A ``Mapping`` behaves like a read-only ``dict``, so ``items()``, ``keys()`` and
``mapping[key]`` work. ``successful`` and ``unsuccessful`` are ordered lists of
``(Key, Binding)`` pairs when you want one side.

Bindings follow ascending ``sh:order``, with unordered properties last and
deterministic source identities breaking ties. ``binding.order`` exposes the
authored decimal order; ``binding.position`` is its one-based position among
all bindings, including unsuccessful ones. Turtle statement order is not used.

Use bindings in an application
------------------------------

Build a configuration dictionary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   mapping.value_map(by="name", python=True)
   # {'airflow': ['http://example.org/flow-2'],
   #  'supply air temperature': ['http://example.org/sat-2']}

``value_map()`` keeps successful bindings with known values, including ``[]``
for empty slots. Failed bindings and unknown values (``None``) are omitted.
``by="name"`` uses the authored ``sh:name`` as the key instead of the structured
``Key``, falling back to ``str(key)`` when no name was authored. ``python=True``
converts terms to plain Python — IRIs and blank nodes become strings, literals
convert where the datatype allows.

Names are not required to be unique. ``mapping.by_name(name)`` returns the first
match in binding order; when uniqueness matters, key by ``Key``.

Match on a key in application code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``binding.constraint_components`` when branching on authored SHACL checks:

.. code-block:: python

   pattern_component = "http://www.w3.org/ns/shacl#PatternConstraintComponent"
   for key, binding in mapping.items():
       if pattern_component in (binding.constraint_components or ()):
           print(key, binding.values, binding.rejected_values)

A property may declare multiple components. ``key.kind`` describes the authored family:
``"pattern"`` for a pattern check, ``"datatype"`` for a datatype check,
``"count"`` for ordinary cardinalities, and ``"qualified_count"`` for qualified
cardinalities. Several families yield ``"compound"``; inspect
``binding.constraint_components`` for their full identities. The compiler tag
is available separately as ``binding.algebra_kind``.

``binding.explain()`` describes those authored constraints and the binding's
values. For example, a rejected pattern value produces a summary like:

.. code-block:: text

   alpha: pattern constraint failed
     observed count: 1
     qualifying values: none
     rejected values: "a-value"

For a binding with multiple components, the summary lists its declarations
without claiming that every component failed. Counts and qualifying sets that
cannot be recovered remain explicitly unavailable. To inspect the underlying
evidence tree, use ``binding.explain(algebra=True)``.

Keys render as strings for logs and table headings:

.. code-block:: python

   key, binding = mapping.successful[0]
   print(key)      # hasPoint→SupplyAirTemperatureSensor

Do not use that string as a program interface — it compacts IRIs to local names,
so it is not globally unique. The key is structured data. This pattern-matching
example requires Python 3.10 or newer; the package also supports Python 3.9:

.. code-block:: python

   from shifty import Cls, Key, Pred

   match key:
       case Key(Pred("http://example.org/hasPoint"), Cls(sensor_class)):
           print(sensor_class)

Keys are immutable and hashable. Their ``ordinal`` distinguishes obligations
with the same path and qualifier. See :doc:`../reference/shape-maps` for
the path and qualifier types. ``mapping[...]`` also accepts a rendered string
for interactive lookup.

Work with the values
~~~~~~~~~~~~~~~~~~~~

Bound values are typed ``Term`` objects — ``Iri``, ``Literal``, ``BNode`` — not
rendered strings, so an IRI and a literal with the same text cannot be confused:

.. code-block:: python

   from shifty import Iri, Literal

   assert mapping.focus == Iri("http://example.org/vav-1")

   value = mapping.successful[0][1].values[0]
   value.n3()                         # <http://example.org/sat-1>
   Literal("12", "http://www.w3.org/2001/XMLSchema#integer").to_python()   # 12

``Term.parse()`` reads N-Triples spelling, ``.n3()`` writes it,
``Literal.to_python()`` converts common numeric and boolean datatypes and leaves
anything unrecognised in its lexical form, and ``Term.to_rdflib()`` is available
when ``rdflib`` is installed.

Find a node across profiles
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   for mapping in smap.for_focus("http://example.org/vav-1"):
       print(mapping.shape_name, mapping.conforms)

The argument may be a ``Term``, an N-Triples term, or a bare IRI. The index is
built on first use.

``mapping.evaluation`` provides the underlying evidence for further inspection
or repair.

Annotate slots and values
-------------------------

Slot names and value annotations use separate paths:

``name_path`` names the **slot**. It starts at the authored property-shape node
and runs over the *shapes* graph, so its result is the same for every focus
node. The default is ``sh:name``.

``value_paths`` annotates each **bound value**. It starts at the value node and
runs over the graph validation read, so its result differs row by row.

.. code-block:: python

   smap = shifty.shape_map(
       data, shapes, infer=False,
       name_path="sh:name",
       value_paths={"timeseries": "ex:hasReference/ex:hasId"},
   )

   binding = smap["http://example.org/VavShape"][0].successful[0][1]
   binding.name       # "supply air temperature"
   binding.names      # every value name_path reached

   for bound in binding.annotated_values:
       print(bound.term, bound.annotations["timeseries"])

Value paths resolve lazily: the first access to ``annotated_values`` or
``annotations`` batches every bound value in the whole map, once per label.
Supplying no ``value_paths`` costs nothing, and ``name_path=None`` skips slot
naming entirely.

Slot names are also retained for a node shape with a single property shape.

For an optional extraction slot, an unbounded ``sh:qualifiedValueShape``
exposes matching values through ``Binding.values`` without imposing a minimum
count.

The default ``name_path="sh:name"`` works for Turtle, paths, bytes, and
``rdflib.Graph`` inputs. The standard ``sh:`` prefix is available for this
default even when a graph serialization has no prefix declarations.

Use the partial bindings of a failing node
------------------------------------------

A failing focus node can still have successful bindings. Use these to inspect
which property obligations it satisfied:

.. code-block:: python

   for key, binding in mapping.unsuccessful:
       print(key, "needs", binding.missing, "more")
       print("  qualifying so far:", binding.partial_values)
       print("  rejected by the qualifier:", binding.rejected_values)
       print(binding.explain())

``binding.min`` and ``binding.max`` are the declared bounds, ``binding.observed``
the count of path values (qualifying values for qualified counts),
``binding.expects_single`` true exactly for a
``1..1`` obligation, and ``binding.severity`` the effective SHACL severity in
lowercase.

Invalid values still count toward ordinary cardinality. A pattern failure
can therefore have ``observed == 2``, one qualifying ``partial_values`` entry,
and one ``rejected_values`` entry. Without its session, a pruned qualifying
set is unknown (``values`` and ``partial_values`` are ``None``). ``observed``
is also ``None`` when unavailable or when the property combines different
count domains, such as ordinary and qualified cardinality.

Reuse an evidence session
~~~~~~~~~~~~~~~~~~~~~~~~~

``shape_map()`` is a convenience over ``EvidenceSession`` plus
``ShapeMap.from_run``. If you build the map yourself, pass the session too:

.. code-block:: python

   session = shifty.EvidenceSession(shapes, data, infer=False)
   run = session.validate()
   smap = shifty.ShapeMap.from_run(run, session)

The session lets the map recover values omitted from failure evidence.
Without it, a binding whose values cannot be recovered has
``binding.values is None``. Reuse the same session and run to build several
maps over one snapshot.

Serialize
---------

.. code-block:: python

   smap.to_dict()

A compact JSON-compatible summary organised by shape, focus, and rendered key,
with terms in N-Triples spelling so IRIs, literals, languages, datatypes and
blank nodes stay unambiguous. It carries each binding's status, values, missing
count and resolved name. The string keys are a presentation format; keep using
``Key`` in program logic.

See also
--------

- :doc:`../reference/shape-maps` — the full object model.
- ``python/examples/shape_map_point_list.py`` — a worked example over a
  building point list.
