Shapeshift in Python
====================

.. include:: experimental.inc

Writing ``.shm`` by hand works until the shapes come from somewhere else:
a spreadsheet of points, a specification, another model. Then it is easier
to generate them.

``shifty.experimental.shapeshift`` builds Shapeshift modules from Python
values. It writes them as ``.shm`` text or as SHACL Turtle.

.. literalinclude:: ../../examples/shapeshift/vav.py
   :language: python

.. program-output:: python examples/shapeshift/vav.py
   :cwd: ../..

The output is formatted the same way as ``shifty shape fmt``.

Names
-----

Assigning to a module defines a name, and reading one refers to it.

- ``m.zone_temperature = ...`` defines ``zone-temperature``. Underscores
  become ``-``.
- ``m["zone_temp"] = ...`` keeps the name as written. Use it for names from
  data, or names that clash with ``Module`` methods (``check``, ``source``,
  ``write``).
- ``m.SAT_AVG`` looks up the name as written first, then its ``-`` form.
- A name you read before defining it is a reference, resolved when the
  module is written. So you can use a shape before its definition. An
  unknown name fails then, with a suggestion.
- Assigned values become shapes, except paths: ``Path(...)`` or an
  expression like ``BRICK.feeds.plus``.
- A bare term could be either, so assigning one is an error. Write
  ``Path(BRICK.hasPoint)`` or ``Shape(BRICK.Point)``.
- Keywords (``a``, ``is``, ``value``, ``length``, ...) can't name
  definitions or parameters. They can be labels: ``Shape(value=...)`` is
  ``value: ...``.

Shapes
------

``Shape(*lines, **labelled)`` holds when all its lines hold. Keyword
arguments are named lines (``temp: ...``), exported as named property
shapes.

``&``, ``|``, and ``~`` mean *and*, *or*, and *not*. On terms, the meaning
depends on where the result goes:

- Where a path goes, they combine paths. ``BRICK.feeds | BRICK.serves`` is
  ``brick:feeds or brick:serves``.
- Where a line or test goes, they combine classes.
  ``points.each(BRICK.A | BRICK.B)`` accepts either class.
- A template argument is read as a path. For either class, pass
  ``any_of(BRICK.A, BRICK.B)``.

.. list-table::
   :header-rows: 1

   * - Python
     - ``.shm``
   * - ``BRICK.VAV`` alone on a line
     - ``is a brick:VAV``
   * - a shape or template call alone on a line
     - composes it: ``damper``, ``with-point(...)``
   * - ``a & b``
     - both: the same as ``Shape(a, b)``
   * - ``a | b``, ``BRICK.A | BRICK.B``
     - ``any of { a } { b }``, ``is a brick:A or brick:B``
   * - ``~a``, ``~BRICK.A``
     - ``none of { a }``, ``none of { is a brick:A }``
   * - ``any_of(a, b, k=c)``, ``exactly_one_of``, ``none_of``,
       ``at_least_of(n, ...)``, ``at_most_of(n, ...)``
     - frames; a keyword argument is a branch of one named line
   * - ``when(cond).then(body)``
     - ``when { cond } then { body }``
   * - ``is_(T)``
     - ``is T``, a test of the node: ``is_(ss.iri)`` is ``is iri``, and
       ``is_("x")`` is ``is one of "x"``

A ``Shape`` that no module names is written inline, as ``{ ... }``.

Only a named shape takes ``for_every=``, ``targets=``, ``doc=``,
``comment=``, ``severity=``, ``message=``, or ``closed=True``
(``only these properties``).

Paths and fields
----------------

Where a path goes, a term is a property. Paths combine with:

- ``/`` for a sequence, ``|`` for ``or``, and ``~`` for ``^``.
- ``.plus``, ``.star``, and ``.maybe`` for repetition.

``SELF`` is the empty path. A path with steps, like
``BRICK.feeds.plus / BRICK.hasPart``, is only ever a path.

A path's methods make fields:

.. list-table::
   :header-rows: 1

   * - Python
     - ``.shm``
   * - ``p.required()``, ``p.optional()``
     - ``p required``, ``p optional``
   * - ``p.exactly(n)``, ``p.at_least(n)``, ``p.at_most(n)``, ``p.between(a, b)``
     - the count
   * - ``p.at_least(n, T)``
     - ``p at least n is T``, or ``p at least n matches S`` for a shape
   * - ``p.each(T, U)``
     - ``p each T, each U``
   * - ``p.same_as(q)``, ``p.different_from(q)``, ``p.below(q)``,
       ``p.up_to(q)``, ``p.unique_language()``
     - comparisons

Field methods chain: ``p.required().each(T)``. Annotations return a copy:
``.doc()``, ``.severity()``, ``.message()``, ``.comment()``.

Pass rows to a count method to make a group:

.. code-block:: python

   points.required(sat=BRICK.Supply_Air_Temperature_Sensor, flow=BRICK.Air_Flow_Sensor)
   points.has(rat=ss.row(BRICK.Return_Air_Temperature_Sensor, "optional", doc="if any"))
   parts.required(room=BRICK.Room & points.required(BRICK.Open_Close_Status))

- ``has`` uses the default count: exactly one per row.
- ``row(item, count, doc=, comment=)`` gives a row its own count:
  ``"required"``, ``"optional"``, ``n``, or ``(min, max)``, with ``None`` for
  an open end.
- ``doc`` goes on the line before the row, and ``comment`` at the end of it.
- ``C & lines`` as a row is ``C { lines }``.

Counts come out in the language's spelling: ``p.at_least(1)`` is
``p required``, and ``p.between(0, 1)`` is ``p optional``.

Tests
-----

Where a test goes (``each``, ``at_least(n, ...)``), Python values map to
tests by type:

- A term is a class: ``a C``. ``BRICK.A | BRICK.B`` is
  ``a brick:A or brick:B``.
- A shape or template call is a shape: ``matches S``.
- A ``str``, ``int``, ``float``, ``bool``, ``datetime.date``,
  ``datetime.datetime``, or ``datetime.time`` is that value: ``one of v``.
  ``lit(s, lang)`` is a tagged string.

The other tests are functions:

- ``one_of(...)``
- ``datatype("string")`` or ``datatype(term)``
- ``node_kind("iri")``
- ``length(n)`` or ``length(min=, max=)``
- ``value_range(lo, hi)``, with ``above=`` and ``below=`` for excluded
  bounds. ``value_range(date(2024, 1, 1), below=date(2025, 1, 1))`` is
  ``from 2024-01-01 below 2025-01-01``.
- ``pattern(regex, flags)``
- ``language(*tags)``

Templates
---------

A Python function is a natural template: its parameters are the template's.
``@m.template`` turns one into a template. The function runs once, when the
module is written, with a placeholder for each parameter. It returns the
template's lines.

- The template takes the function's name, with ``-`` for ``_``, unless you
  pass ``name=``. The docstring becomes its ``doc``.
- Python defaults become the template's defaults. ``P.cls`` refers to an
  earlier parameter: ``room_cls: Class = P.cls``.
- A parameter annotated ``Class`` is a class, and alone on a line it is
  ``is cls``. An unannotated parameter is a shape.
- Calls are checked against the signature, at the call or, if the template
  isn't defined yet, when the module is written.

``@m.path_template`` does the same for paths:
``fed(p=BRICK.hasPoint): return BRICK.feeds.plus / p``.

Modules
-------

``Module(name, base=, comment=)`` writes to ``<name>.shm``.

- Prefixes and imports are declared for you, from what the module uses.
- ``m.uses(other, alias="o")`` imports ``other`` under an alias, so its
  names are written ``o.name``. ``uses`` also sets the import order.
- ``m.uses("https://brickschema.org/schema/1.4/Brick")`` imports RDF.
  ``check`` then verifies the classes you use against it.
- Modules in different Python files share definitions through Python
  imports.

Output
------

.. list-table::
   :header-rows: 1

   * - Call
     - Result
   * - ``m.source()``, ``str(m)``
     - ``.shm`` text, formatted
   * - ``m.write(dir)``, ``ss.write(dir, *modules)``
     - the ``.shm`` files, with everything they import
   * - ``m.check(catalog=None)``
     - diagnostics, as ``shifty shape check`` reports them
   * - ``m.export(core=False, catalog=None)``
     - SHACL Turtle, as a string
   * - ``m.write_shacl(path, core=False)``
     - the Turtle, written to ``path``, or to ``<name>.ttl`` if ``path`` is a
       directory
   * - ``m.validate(data, targets=None, **options)``
     - ``shifty.validate``'s ``(conforms, report_graph, report_text)``

- ``check`` returns ``Diagnostic`` objects. ``str(d)`` prints one as the
  CLI does.
- ``source``, ``export``, and ``write_shacl`` raise ``ShapeshiftError`` with
  the diagnostics when the module doesn't build.
- ``core=True`` keeps to SHACL Core.

A library module often has no targets, so validating with it would check
nothing and always pass. ``validate`` raises ``ValueError`` instead. Pass
``targets`` to say which nodes to check:

.. code-block:: python

   conforms, report, text = vav.validate(
       model, targets={vav.vav_cooling_only: BRICK.VAV}
   )

A target is a class, a target such as ``ss.subjects_of(p)``, or a list of
these. To keep targets for later, write them into a module:
``check.every_vav = Shape(vav.vav_cooling_only, for_every=BRICK.VAV)``.

Larger examples
---------------

``examples/shapeshift/g36/`` in the repository has ASHRAE Guideline 36,
written in Python. ``generate.py`` writes the ``.shm`` files:

- ``terminal_units.py``: sections 4.1 to 4.5.
- ``air_handlers.py``: sections 4.6 and 4.8.
- ``afdd.py``: the fault conditions of section 5, one shape each.
- ``brick.py``, ``bacnet.py``, and ``components.py``: the modules the others
  import, including the rule that every point states its unit and its
  BACnet object.
