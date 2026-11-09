Write Shapeshift modules in Python
==================================

``shifty.shapeshift`` builds Shapeshift (``.shm``) modules from Python values
and writes them as ``.shm`` text or SHACL Turtle. Use it when shapes are
generated from data, or to reuse Python functions as templates.

.. literalinclude:: ../examples/shapeshift.py
   :language: python

.. program-output:: python examples/shapeshift.py
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
- Reading a name that is not defined yet returns a reference. It is resolved
  when the module is written, so shapes and template calls can come before
  their definitions. An unknown name fails at that point, with a suggestion.
- Any assigned value becomes a shape, except ``Path(...)``. A path expression
  such as ``BRICK.feeds.plus`` becomes a path. Assigning a bare term is an
  error, because it could mean either: write ``Path(BRICK.hasPoint)`` or
  ``Shape(BRICK.Point)``.

Shapes
------

``Shape(*lines, **labelled)`` is a conjunction of lines. Keyword arguments are
named lines (``temp: ...``), which become named property shapes on export.

.. list-table::
   :header-rows: 1

   * - Python
     - ``.shm``
   * - ``BRICK.VAV`` alone on a line
     - ``is brick:VAV``
   * - a shape or template call alone on a line
     - composes it: ``damper``, ``with-point(...)``
   * - ``a & b``
     - both: the same as ``Shape(a, b)``
   * - ``a | b``
     - ``any of { a } { b }``
   * - ``~a``
     - ``none of { a }``
   * - ``any_of(a, b, k=c)``, ``all_of``, ``exactly_one_of``, ``none_of``,
       ``at_least_of(n, ...)``
     - frames; a keyword argument is a branch of one named line
   * - ``when(cond).then(body)``
     - ``when { cond } then { body }``
   * - ``is_(x)``
     - ``is x``, for a value or an unannotated parameter

A ``Shape`` that no module names is written inline, as ``{ ... }``. Only a
named shape takes ``for_every=``, ``targets=``, ``doc=``, ``comment=``,
``severity=``, ``message=``, or ``closed=True`` (``only these properties``).

Paths and fields
----------------

A term is a property where a path goes. Paths combine with ``/`` (sequence),
``|`` (``or``), ``~`` (``^``), ``.plus``, ``.star``, and ``.maybe``. ``SELF`` is
the empty path.

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
     - ``p at least n is T``
   * - ``p.each(T, U)``
     - ``p each T, each U``
   * - ``p.same_as(q)``, ``p.different_from(q)``, ``p.less_than(q)``,
       ``p.unique_language()``
     - comparisons

A field's methods chain: ``p.required().each(T)``. Its line annotations
return a copy: ``.doc()``, ``.severity()``, ``.message()``, ``.comment()``.

Given rows, a count method makes a group: named fields along one path.

.. code-block:: python

   points.required(sat=BRICK.Supply_Air_Temperature_Sensor, flow=BRICK.Air_Flow_Sensor)
   points.has(rat=ss.row(BRICK.Return_Air_Temperature_Sensor, "optional", doc="if any"))
   parts.required(room=BRICK.Room & points.required(BRICK.Open_Close_Status))

``has`` uses the default count (exactly one per row). ``row(item, count, doc=)``
gives a row its own count: ``"required"``, ``"optional"``, ``n``, or
``(min, max)`` with ``None`` for an open end. ``C & lines`` as a row is
``C { lines }``.

Tests
-----

Where a test goes (``each``, ``at_least(n, ...)``):

- a term is a class;
- a shape or call is a shape;
- a Python ``str``, ``int``, ``float``, or ``bool`` is that value, and
  ``lit(s, lang)`` is a tagged string.

The other tests are functions: ``one_of(...)``, ``datatype("string")`` or
``datatype(term)``, ``node_kind("iri")``, ``length(n)`` or
``length(min=, max=)``, ``value_range(lo, hi)``, ``pattern(regex, flags)``, and
``language(*tags)``.

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

``check`` returns a list of ``Diagnostic`` objects; ``str(d)`` is the CLI's
rendering. ``source``, ``export``, and ``write_shacl`` raise
``ShapeshiftError``, carrying the diagnostics, when the module does not parse
or build. ``core=True`` keeps to SHACL Core.

Pitfalls
--------

- ``BRICK.A | BRICK.B`` is a path alternative. For either class, write
  ``any_of(BRICK.A, BRICK.B)``.
- Shapeshift keywords (``a``, ``is``, ``shape``, ``doc``, ...) cannot be names,
  labels, or parameter names.
- Shapes without targets validate nothing. Give one ``for_every=``, or wrap
  it: ``check.every_vav = Shape(vav.vav_cooling_only, for_every=BRICK.VAV)``.

Larger examples
---------------

``examples/shapeshift/g36/`` in the repository writes Guideline 36 sections 4.1
(``vav_cooling_only.py``) and 4.8 (``sz_vav_ahu.py``), with the modules they
import (``brick.py``, ``components.py``). Each regenerates its ``.shm`` file
exactly.
