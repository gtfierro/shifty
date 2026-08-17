Extract bindings with a shape map
=================================

Use this when a shape is really an extraction schema — when you do not want to
know *whether* the VAV has a supply-air temperature sensor, you want the sensor.

A shape map turns an evidence run into a flat table: one entry per selected
``(shape, focus)`` pair, mapping each property obligation to the values that
satisfied it. Without it you would validate, learn that the node conforms, and
then write a second query that re-implements the shape's property paths and
qualified-value filtering just to recover the values the validator already
found.

Get a map
---------

.. code-block:: python

   import shifty

   shapes = """
   @prefix ex: <http://example.org/> .
   @prefix sh: <http://www.w3.org/ns/shacl#> .

   ex:VavShape a sh:NodeShape ;
       sh:targetClass ex:Vav ;
       sh:property [
           sh:path ex:hasPoint ;
           sh:name "supply air temperature" ;
           sh:qualifiedValueShape [ sh:class ex:SupplyAirTemperatureSensor ] ;
           sh:qualifiedMinCount 1 ] ;
       sh:property [
           sh:path ex:hasPoint ;
           sh:name "airflow" ;
           sh:qualifiedValueShape [ sh:class ex:AirFlowSensor ] ;
           sh:qualifiedMinCount 1 ] .
   """

   data = """
   @prefix ex: <http://example.org/> .
   ex:vav-1 a ex:Vav ; ex:hasPoint ex:sat-1 .
   ex:sat-1 a ex:SupplyAirTemperatureSensor .
   ex:vav-2 a ex:Vav ; ex:hasPoint ex:sat-2, ex:flow-2 .
   ex:sat-2 a ex:SupplyAirTemperatureSensor .
   ex:flow-2 a ex:AirFlowSensor .
   """

   smap = shifty.shape_map(shapes, data, infer=False)

   for mapping in smap["http://example.org/VavShape"]:
       print(mapping.focus, "conforms:", mapping.conforms)
       for key, binding in mapping.successful:
           print("   ", binding.name, binding.values)

.. code-block:: text

   <http://example.org/vav-1> conforms: False
       supply air temperature [Iri(value='http://example.org/sat-1')]
   <http://example.org/vav-2> conforms: True
       airflow [Iri(value='http://example.org/flow-2')]
       supply air temperature [Iri(value='http://example.org/sat-2')]

Both VAVs appear. ``vav-1`` is missing its airflow sensor and therefore does not
conform — but it still bound the temperature sensor correctly, and that binding
is in the map.

A ``Mapping`` behaves like a read-only ``dict``, so ``items()``, ``keys()`` and
``mapping[key]`` work. ``successful`` and ``unsuccessful`` are ordered lists of
``(Key, Binding)`` pairs when you want one side.

Produce a configuration dictionary
----------------------------------

For output that feeds an application rather than more analysis:

.. code-block:: python

   mapping.value_map(by="name", python=True)
   # {'airflow': ['http://example.org/flow-2'],
   #  'supply air temperature': ['http://example.org/sat-2']}

``value_map()`` keeps only successful bindings. ``by="name"`` uses the authored
``sh:name`` as the key instead of the structured ``Key``, and ``python=True``
converts terms to plain Python — IRIs and blank nodes become strings, literals
convert where the datatype allows.

Names are not required to be unique. ``mapping.by_name(name)`` returns the first
match in authored order; when uniqueness matters, key by ``Key``.

Match on a key in application code
----------------------------------

Keys render as strings for logs and table headings:

.. code-block:: python

   key, binding = mapping.successful[0]
   print(key)      # hasPoint→SupplyAirTemperatureSensor

Do not use that string as a program interface — it compacts IRIs to local names,
so it is not globally unique, and it depends on rendering choices. The key is
structured data, and pattern matching over it is the intended access:

.. code-block:: python

   from shifty import Cls, Key, Pred

   match key:
       case Key(Pred("http://example.org/hasPoint"), Cls(sensor_class)):
           print(sensor_class)

``Key.path`` is one of ``Id``, ``Pred``, ``Inv``, ``Seq``, ``Alt``, ``Star``.
``Key.qualifier`` is a ``Cls``, ``Const``, ``Datatype``, or ``ShapeRef`` when
the qualification can be recovered from the shape. Both are immutable and
hashable, so a ``Key`` works as a dict key or set member. ``ordinal``
distinguishes two authored obligations with the same path and qualifier, and
counts towards equality.

``mapping[...]`` does accept the rendered string, for interactive use.

Work with the values
--------------------

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

Annotate slots and values
-------------------------

Two independent annotation mechanisms, easy to confuse because both take a path:

``name_path`` names the **slot**. It starts at the authored property-shape node
and runs over the *shapes* graph, so its result is the same for every focus
node. The default is ``sh:name``.

``value_paths`` annotates each **bound value**. It starts at the value node and
runs over the graph validation read, so its result differs row by row.

.. code-block:: python

   smap = shifty.shape_map(
       shapes, data, infer=False,
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

Use the partial bindings of a failing node
------------------------------------------

An invalid focus node is not an empty result, and for onboarding or repair
workflows the bindings it *did* satisfy are often the most useful context:

.. code-block:: python

   for key, binding in mapping.unsuccessful:
       print(key, "needs", binding.missing, "more")
       print("  qualifying so far:", binding.partial_values)
       print("  rejected by the qualifier:", binding.rejected_values)
       print(binding.explain())

``binding.min`` and ``binding.max`` are the declared bounds, ``binding.observed``
the count the evidence saw, ``binding.expects_single`` true exactly for a
``1..1`` obligation, and ``binding.severity`` the effective SHACL severity in
lowercase.

Keep the session when building a map by hand
--------------------------------------------

``shape_map()`` is a convenience over ``EvidenceSession`` plus
``ShapeMap.from_run``. If you build the map yourself, pass the session too:

.. code-block:: python

   session = shifty.EvidenceSession(shapes, data, infer=False)
   run = session.validate()
   smap = shifty.ShapeMap.from_run(run, session)

The session is what keeps the map exact for partially conforming nodes.
Canonical failure evidence omits the passing siblings of a failed conjunction;
with the session, the map materializes those on demand. Without it, an omitted
passing sibling has ``binding.values is None`` — explicitly unknown, rather than
guessed.

Keeping the session is also how you reuse one prepared snapshot across several
maps instead of re-parsing and re-validating.

Find every profile that selected a node
---------------------------------------

.. code-block:: python

   for mapping in smap.for_focus("http://example.org/vav-1"):
       print(mapping.shape_name, mapping.conforms)

The argument may be a ``Term``, an N-Triples term, or a bare IRI. The index is
built on first use.

Each mapping also carries ``mapping.evaluation``, the underlying evidence — the
way back to the full tree or a repair when the flat view has told you there is a
problem but not enough about it.

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
