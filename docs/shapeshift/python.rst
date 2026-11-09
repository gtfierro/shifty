Shapeshift in Python
====================

.. include:: experimental.inc

``shifty.experimental.shapeshift`` builds Shapeshift (``.shm``) modules from Python values
and writes them as ``.shm`` text or SHACL Turtle. Use it when shapes are
generated from data, or to reuse Python functions as templates.

.. literalinclude:: ../examples/shapeshift/vav.py
   :language: python

.. program-output:: python examples/shapeshift/vav.py
   :cwd: ..

The text goes through the same formatter as ``shifty shape fmt``.

Names
-----

Assigning to a module defines a name. Reading one refers to it.

- ``m.zone_temperature = ...`` defines ``zone-temperature``: underscores in an
  attribute name become ``-``.
- ``m["zone_temp"] = ...`` uses the name as written. Use it for names from
  data, and for names that clash with ``Module`` methods (``check``,
  ``source``, ``write``).
- Reading ``m.SAT_AVG`` finds the name as written first, then its ``-`` form,
  so names that are not kebab case read as attributes too.
- Reading a name that is not defined yet returns a reference. It is resolved
  when the module is written, so shapes and template calls can come before
  their definitions. An unknown name fails at that point, with a suggestion.
- Any assigned value becomes a shape, except ``Path(...)``. A path expression
  such as ``BRICK.feeds.plus`` becomes a path. Assigning a bare term, or
  ``A | B`` of terms, is an error, because it could mean either: write
  ``Path(BRICK.hasPoint)`` or ``Shape(BRICK.Point)``.
- Shapeshift keywords (``a``, ``is``, ``value``, ``length``, ...) cannot name
  definitions or template parameters, which are written bare. They can be
  labels: ``Shape(value=...)`` is ``value: ...``.

Shapes
------

``Shape(*lines, **labelled)`` is a conjunction of lines. Keyword arguments are
named lines (``temp: ...``), which become named property shapes on export.

``&``, ``|``, and ``~`` mean *and*, *or*, and *not* everywhere. On shapes they
are shape logic. On terms they depend on where the result goes, as a term
does: a path where a path goes (``BRICK.feeds | BRICK.serves`` is
``brick:feeds or brick:serves``), class logic where a line or test goes
(``points.each(BRICK.A | BRICK.B)`` is either class). A template argument is
read as a path; pass ``any_of(BRICK.A, BRICK.B)`` for either class.

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

A ``Shape`` that no module names is written inline, as ``{ ... }``. Only a
named shape takes ``for_every=``, ``targets=``, ``doc=``, ``comment=``,
``severity=``, ``message=``, or ``closed=True`` (``only these properties``).

Paths and fields
----------------

A term is a property where a path goes. Paths combine with ``/`` (sequence),
``|`` (``or``), ``~`` (``^``), ``.plus``, ``.star``, and ``.maybe``. ``SELF`` is
the empty path. Only ``|`` and ``~`` of bare terms double as class logic; a
path with steps, like ``BRICK.feeds.plus / BRICK.hasPart``, is only a path.

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

A field's methods chain: ``p.required().each(T)``. Its line annotations
return a copy: ``.doc()``, ``.severity()``, ``.message()``, ``.comment()``.

Given rows, a count method makes a group: named fields along one path.

.. code-block:: python

   points.required(sat=BRICK.Supply_Air_Temperature_Sensor, flow=BRICK.Air_Flow_Sensor)
   points.has(rat=ss.row(BRICK.Return_Air_Temperature_Sensor, "optional", doc="if any"))
   parts.required(room=BRICK.Room & points.required(BRICK.Open_Close_Status))

``has`` uses the default count (exactly one per row). ``row(item, count, doc=,
comment=)`` gives a row its own count: ``"required"``, ``"optional"``, ``n``,
or ``(min, max)`` with ``None`` for an open end. The ``doc`` is written on the
line before the row, and ``comment`` after it on its line (``-- comment``).
``C & lines`` as a row is ``C { lines }``.

Counts are written the way the language spells them: ``p.at_least(1)`` is
``p required``, and ``p.between(0, 1)`` is ``p optional``.

Tests
-----

Where a test goes (``each``, ``at_least(n, ...)``):

- a term is a class: ``a C``, and ``BRICK.A | BRICK.B`` is
  ``a brick:A or brick:B``;
- a shape or call is a shape: ``matches S``;
- a Python ``str``, ``int``, ``float``, or ``bool`` is that value, as are
  ``datetime.date``, ``datetime.datetime``, and ``datetime.time``
  (``2024-01-31``, ``2024-01-31T08:00:00Z``, ``08:00:00``), and
  ``lit(s, lang)`` is a tagged string: ``one of v``.

The other tests are functions: ``one_of(...)``, ``datatype("string")`` or
``datatype(term)``, ``node_kind("iri")``, ``length(n)`` or
``length(min=, max=)``, ``value_range(lo, hi)`` with ``above=`` and ``below=``
for bounds that are excluded, ``pattern(regex, flags)``, and
``language(*tags)``. A range's ends are numbers, or dates and times:
``value_range(date(2024, 1, 1), below=date(2025, 1, 1))`` is
``from 2024-01-01 below 2025-01-01``.

Templates
---------

``@m.template`` makes a template from a function. The function runs once, when
the module is written. Each parameter is passed as a placeholder, and the
function returns the template's lines.

- The template is named after the function, with ``-`` for ``_``, unless you
  pass ``name=``. The docstring becomes its ``doc``.
- Python defaults become the template's defaults. ``P.cls`` refers to an
  earlier parameter: ``room_cls: Class = P.cls``.
- A parameter annotated ``Class`` is a class. Alone on a line, it is
  ``is cls``. Unannotated, it is a composed shape.
- Calls are checked against the signature: at the call, or, for a call made
  before the template is defined, when the module is written.

``@m.path_template`` does the same for paths:
``fed(p=BRICK.hasPoint): return BRICK.feeds.plus / p``.

Modules
-------

``Module(name, base=, comment=)`` writes to ``<name>.shm``.

- A module declares the prefixes of the terms it uses, and imports the
  modules whose definitions it uses.
- ``m.uses(other, alias="o")`` imports ``other`` under an alias, so its names
  are written ``o.name``. ``uses`` also sets the order of the imports.
  ``m.uses("https://brickschema.org/schema/1.4/Brick")`` imports RDF, whose
  classes ``check`` then verifies.
- Modules in different Python files share definitions through Python imports.

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

``check`` returns a list of ``Diagnostic`` objects; ``str(d)`` is the CLI's
rendering. ``source``, ``export``, and ``write_shacl`` raise
``ShapeshiftError``, carrying the diagnostics, when the module does not parse
or build. ``core=True`` keeps to SHACL Core.

A shape without a target checks no nodes, so a module of such shapes, like a
library, validates anything. ``validate`` refuses to: it raises
``ValueError`` unless some shape has a target. Pass ``targets`` to give
shapes their nodes for this validation:

.. code-block:: python

   conforms, report, text = vav.validate(
       model, targets={vav.vav_cooling_only: BRICK.VAV}
   )

A target is a class, a target such as ``ss.subjects_of(p)``, or a list of
either. To keep the targets, write them into a module:
``check.every_vav = Shape(vav.vav_cooling_only, for_every=BRICK.VAV)``.

Larger examples
---------------

The ``.shm`` files in ``examples/shapeshift/g36/`` in the repository are
generated from Python by ``generate.py``. ``terminal_units.py`` writes
sections 4.1 to 4.5, ``air_handlers.py`` 4.6 and 4.8, and ``afdd.py`` the
fault conditions of section 5, one shape for each. ``brick.py``, ``bacnet.py``, and ``components.py`` write
the modules they import, including the rule that every point states its unit
and its BACnet object. The modules import one another as Python modules.
