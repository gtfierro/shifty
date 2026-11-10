The Shapeshift language
=======================

.. include:: experimental.inc

A Shapeshift file (``.shm``) declares shapes. This page goes through the
language from the file down to a single test. :doc:`reference` says what each
construct is in SHACL.

Files
-----

A file starts with its header, then declares shapes and paths in any order:

.. code-block:: shapeshift

   -- Shapes for VAV boxes.
   base <urn:example/vav/>
   prefix brick: <https://brickschema.org/schema/Brick#>

   doc "A VAV box."
   shape vav for every brick:VAV {
     brick:isFedBy exactly 1
   }

- ``base <iri>`` is the IRI the file's shapes are named under: ``vav`` is
  ``<urn:example/vav/vav>``. Without a base, the build warns and names the
  shapes under a placeholder ``urn:shapeshift:`` IRI made from the file name.
- ``prefix name: <iri>`` declares a prefix, as in Turtle. ``rdf``, ``rdfs``,
  ``xsd``, ``owl``, and ``sh`` are declared already.
- ``import`` brings in another file or an RDF graph. See `Imports`_.
- ``--`` starts a comment, which runs to the end of the line. The formatter
  keeps comments; the compiler ignores them.
- ``doc "..."`` documents the shape or line after it. Unlike a comment, a doc
  is part of the shape: it is exported (as ``rdfs:comment`` or
  ``sh:description``) and quoted in reports. A string in triple quotes
  (``"""..."""``) may span lines.

Inside braces, a line ends at a newline or a ``;``. Names are made of
letters, digits, ``_``, and ``-``: ``zone-temperature``, ``SAT_AVG``.

Shapes and targets
------------------

``shape name { lines }`` defines a shape. A node conforms when every line
holds. ``for`` clauses, between the name and the braces, say which nodes the
shape checks:

.. code-block:: shapeshift

   shape vav
     for every brick:VAV                      -- instances of a class
     for every node with brick:hasDamper      -- subjects of a property
     for every value of brick:feeds           -- objects of a property
     for node ex:vav-7                        -- one node
   {
     brick:hasPoint required
   }

``for every node matching s`` checks the nodes that conform to shape ``s``,
and ``for sparql "SELECT ?this WHERE { ... }"`` the nodes a query returns.

A shape with no ``for`` checks nothing on its own. It is a definition, for
other shapes to use. This differs from SHACL, where a shape is often a target
by accident; in Shapeshift what a shape checks is always on its first line.

``for every C`` and the ``a C`` test include instances of subclasses of
``C``, as in SHACL, when the ``rdfs:subClassOf`` triples are in the graph
being validated.

Fields
------

Most lines are fields: a path, then how many values it has, then tests on
the values.

.. code-block:: shapeshift

   brick:isFedBy exactly 1, each a brick:AHU
   brick:hasPoint at least 1 is a brick:Air_Flow_Sensor
   rdfs:label required, each literal, each language "en"

A count limits the number of values:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Count
     - Values
   * - ``required``
     - at least one
   * - ``exactly n``
     - exactly ``n``
   * - ``optional``
     - at most one
   * - ``at least n``, ``at most n``
     - at least, or at most, ``n``
   * - ``between a and b``
     - from ``a`` to ``b``, inclusive

A field with no count may have any number of values. Each count has one
spelling: ``at least 1`` is written ``required``, ``at most 1`` and
``between 0 and 1`` are written ``optional``, and the build says so.

After the count come qualifiers, separated by commas:

- ``each T``: every value passes the test ``T``. A node with no values passes,
  so ``required, each a brick:Point`` is the usual "one or more, all points".
- ``at least n is T``, ``at most n is T``, ``exactly n is T``: count only
  the values that pass ``T``. The node may have other values too. For a
  shape, the word is ``matches``: ``at least 1 matches damper``.

The two are easy to confuse, so they are different words:

.. code-block:: shapeshift

   -- every point is a brick:Point
   brick:hasPoint each a brick:Point
   -- one point, at least, is an airflow sensor; the others can be anything
   brick:hasPoint at least 1 is a brick:Air_Flow_Sensor

A value test always follows ``each`` or a qualified count: ``ex:p string`` is
an error, and ``ex:p each string`` the field.

Fields also compare two properties of the node, or check its strings'
languages:

.. code-block:: shapeshift

   ex:start below ex:end              -- each value of one below each of the other
   ex:low up to ex:high               -- ... or equal to it
   rdfs:label same as skos:prefLabel  -- the same set of values
   ex:parent different from ex:child  -- no value in common
   rdfs:label unique language         -- at most one label per language

Tests
-----

A test is what ``each`` and ``at least n is`` apply to each value. Every
test starts with the word that says what kind of test it is:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Test
     - The value
   * - ``a brick:Sensor``
     - is an instance of the class, or of a subclass
   * - ``a brick:AHU or brick:RTU``
     - is an instance of one of the classes
   * - ``matches damper``
     - conforms to the shape ``damper``
   * - ``{ lines }``
     - conforms to these lines: a shape written in place
   * - ``one of ex:on ex:off "auto"``
     - is one of these nodes or literals
   * - ``string``, ``integer``, ``decimal``, ``boolean``, ``dateTime``, ...,
       ``datatype xsd:gYear``
     - is a literal of the datatype
   * - ``iri``, ``literal``, ``blank``
     - is that kind of node
   * - ``length 8``, ``length from 1 to 8``, ``length from 2``,
       ``length up to 8``
     - has a string of this length
   * - ``from 0 to 100``, ``up to 1.5``, ``from 2024-01-01``
     - is in the range, bounds included
   * - ``above 0``, ``below 1.5``, ``above 0 below 1``, ``from 0 below 1``
     - is in the range, bounds excluded
   * - ``pattern "^AHU-[0-9]+$"``, with ``flags "i"``
     - matches the regular expression
   * - ``language "en" "de"``
     - is a string in one of these languages

``a`` and ``matches`` keep a class and a shape apart wherever they meet:
``each brick:AHU`` is an error that asks for ``each a brick:AHU``, and
``each damper`` one that asks for ``each matches damper``. Builtin datatypes
are written by their word: ``datatype xsd:string`` is ``string``.

``or`` after ``a`` lists classes, as it lists properties in a path: a value
passes when it is an instance of any of them. For alternatives that are not
all classes, use ``any of`` (see `Frames`_).

.. code-block:: shapeshift

   brick:isFedBy exactly 1, each a brick:AHU or brick:RTU
   brick:hasPoint at least 1 is a brick:Air_Flow_Sensor or brick:Air_Flow_Setpoint

A range's ends are numbers, or dates and times. Dates and times are written
bare, in their XSD form: ``2024-01-31`` is an ``xsd:date``,
``2024-01-31T08:00:00Z`` an ``xsd:dateTime``, and ``08:00:00`` an
``xsd:time``, each with an optional timezone (``Z``, ``+02:00``). They are
values like any other, so ``one of`` takes them too:

.. code-block:: shapeshift

   ex:commissioned optional, each date, each from 2000-01-01
   ex:reading each above 2024-01-01T00:00:00Z below 2025-01-01T00:00:00Z
   ex:opens each from 06:00:00 to 10:00:00
   ex:holiday each one of 2024-12-25 2024-12-26

Both ends of a range are the same kind of value, and a value is in the range
only if it is that kind too: a date range holds for no number, and for no
``xsd:dateTime``. A date that does not exist, like ``2024-02-30``, is an
error.

Paths
-----

A path is a property, or properties combined:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Path
     - Follows
   * - ``brick:hasPart/brick:hasPoint``
     - one property, then the other
   * - ``brick:feeds or brick:serves``
     - either property
   * - ``^brick:hasPoint``
     - the property backwards
   * - ``brick:feeds+``, ``brick:hasPart*``, ``brick:hasPart?``
     - one or more steps, zero or more, zero or one
   * - ``(brick:feeds or brick:serves)/brick:hasPoint``
     - parentheses group
   * - ``self``
     - no step: the node itself

``path`` names a path for the file to reuse. A path with parameters is a
template, with defaults:

.. code-block:: shapeshift

   prefix brick: <https://brickschema.org/schema/Brick#>

   path points = brick:hasPoint
   path downstream(p = brick:hasPart?) = brick:feeds+/p

   shape box {
     points required
     downstream(brick:hasPoint) at least 1 is a brick:Zone_Air_Temperature_Sensor
   }

Names and composition
---------------------

``name:`` before a line names it. The name changes nothing about what the
line requires. It gives the line an identity: the line exports as a named
property shape (``<urn:example/vav/vav.fed>``), reports and shape maps use
it, and other shapes can reuse it.

A field is written on one line. Its ``doc``, ``message``, and ``severity``
go on the lines before it:

.. code-block:: shapeshift

   shape vav for every brick:VAV {
     flow: brick:hasPoint at least 1 is a brick:Air_Flow_Sensor
     doc "Dual-duct boxes are modelled as two VAVs."
     message "A VAV must be fed by exactly one AHU."
     fed: brick:isFedBy exactly 1, each a brick:AHU
     unit: brick:hasUnit optional, each iri
   }

A shape's name alone on a line composes it: all of its lines hold here too,
but not its targets. ``shape.name`` composes one named line:

.. code-block:: shapeshift

   shape vav for every brick:VAV {
     fed: brick:isFedBy exactly 1, each a brick:AHU
     flow: brick:hasPoint at least 1 is a brick:Air_Flow_Sensor
   }

   shape reheat-vav for every brick:RVAV {
     vav.fed                                    -- only the AHU rule
     reheat: brick:hasPoint at least 1 is a brick:Reheat_Valve_Command
   }

``is`` on a line tests the node itself: ``is a brick:Damper`` says the node
is a damper, and ``is iri`` that it is an IRI. A shape is not written after
``is``: alone on the line, it already applies to the node.

Groups
------

A group lists named points (or parts, or any values) along one path. Each row
names a value and says what it is:

.. code-block:: shapeshift

   shape ahu {
     is a brick:AHU
     brick:hasPoint {
       sat: brick:Supply_Air_Temperature_Sensor
       rat: brick:Return_Air_Temperature_Sensor optional
       doc "outdoor air"
       oat: brick:Outside_Air_Temperature_Sensor
     }
     brick:hasPoint required { brick:Damper_Position_Command; brick:Frequency_Command }
     brick:hasPart {
       fan: brick:Supply_Fan { brick:hasPoint required { brick:Fan_Status } }
     }
   }

A row counts the values that are instances of its class (or that conform to
its shape): a row is the one place a class or a shape is written without
``a`` or ``matches``, since a row is always one or the other. By default it
requires exactly one. A count after the path sets the default for every row,
and a count on a row sets its own, with the same meaning as on a field. A row
``C { lines }`` counts the values that are ``C`` and satisfy the lines. A
row's ``doc`` goes on the line before it, and its name is optional. For two
values of the same class, write one row with ``exactly 2``, not the row
twice: the build warns about repeated rows, which require only one value.

Groups read like the point lists in a specification. Declaring
``path points = brick:hasPoint`` lets the group read ``points { ... }``.

Frames
------

Alternatives and prohibitions are blocks behind a phrase that says how they
combine. Each block is a shape written in place:

.. code-block:: shapeshift

   any of { brick:hasPoint at least 1 is a brick:Damper_Position_Command }
          { brick:hasPart at least 1 is a brick:Damper }
   exactly one of { brick:hasTag required } { rdfs:label required }
   none of { brick:hasLocation required }
   at least 2 of { brick:hasPart required } { brick:isPartOf required } { brick:feeds required }
   at most 1 of { brick:hasTag required } { brick:hasLocation required } { brick:feeds required }
   when { brick:hasPoint at least 1 is a brick:Economizer_Status }
   then { brick:hasPoint at least 1 is a brick:Outside_Air_Damper }

Every line of a block must hold, so there is no ``all of``. ``at least 1 of``
is ``any of``, and ``at most 0 of`` is ``none of``.
``when C then B`` is "if ``C`` holds, ``B`` must": the common rule "if it has
X, it must have Y". There is no ``or`` between lines and no ``not``: a reader
can tell what a block means from its first word.

To choose among values rather than lines, use a test:
``each one of ex:on ex:off``.

Templates
---------

A shape with parameters is a template. A parameter may have a default, which
may refer to an earlier parameter:

.. code-block:: shapeshift

   prefix brick: <https://brickschema.org/schema/Brick#>

   doc "At least `n` points of class `cls`."
   shape has-point(cls, n = 1) { brick:hasPoint at least n is a cls }

   doc "Between `min` and `max` points of class `cls`: exactly `min` by default."
   shape has-points(cls, min = 1, max = min) {
     brick:hasPoint at least min is a cls
     brick:hasPoint at most max is a cls
   }

   shape vav for every brick:VAV {
     has-point(brick:Air_Flow_Sensor)
     temp: has-point(brick:Zone_Air_Temperature_Sensor, n = 2)
     has-points(brick:Damper_Position_Command)
     has-points(brick:Sensor, 2, max = 6)
   }

Arguments are positional, then named. A call is checked against the
template: a missing argument, or a name where a number belongs, is an error
that quotes the template's parameters. Calls with the same arguments share
one exported shape, which records the call it expands. A labelled call
(``temp: has-point(...)``) is exported under its label instead: ``vav.temp``.

Annotations
-----------

``doc``, ``message``, and ``severity`` each go on the line before what they
annotate:

.. code-block:: shapeshift

   shape zone for every brick:HVAC_Zone {
     severity warning
     message "A zone should report its temperature."
     brick:hasPoint at least 1 is a brick:Zone_Air_Temperature_Sensor
     brick:hasPart required
   }

``severity`` (``info``, ``warning``, or ``violation``) and ``message`` apply
to the next line only: here, the temperature line is a warning, and a
missing part is still a violation. Before a ``shape``, they apply to the
whole shape. ``message`` is what a report tells the
data's author; ``doc`` is what the line is for, for the next person reading
the shapes.

``only these properties`` closes a shape: a node may have no properties the
shape does not name, other than ``rdf:type``:

.. code-block:: shapeshift

   shape space for every brick:Space {
     brick:hasPart each matches space
     only these properties
   }

Imports
-------

``import`` reads another Shapeshift file, or an RDF graph:

.. code-block:: shapeshift

   import "./sensors.shm" as sensors
   import <https://brickschema.org/schema/1.4/Brick>

A ``.shm`` path is relative to the importing file. With ``as sensors``, its
names are written ``sensors.temp-sensor``; without an alias they are written
bare. Shapes keep the IRIs of the file that defines them.

An RDF import is an ontology, a SHACL shapes graph, or both. An ontology's
classes and properties are checked: ``shifty shape check`` reports a class
the ontology does not define, with the nearest one it does. A shapes graph's
shapes can be referenced with ``matches``.

Imports are not fetched unless you allow it. ``--map IRI=PATH`` reads an IRI
from a local file, and ``--fetch`` downloads the imports that are not mapped.

Recursion
---------

A shape may refer to itself, directly or through other shapes, as long as no
loop passes through ``none of``, ``at most n of``, an ``at most n is`` (or
``at most n matches``) qualifier, or a ``when`` condition an odd number of
times. ``each matches`` is always safe:

.. code-block:: shapeshift

   shape space for every brick:Space {
     brick:hasPart each matches space
   }

A loop that breaks the rule has no consistent meaning, and the build says
which loop it is:

.. literalinclude:: ../../examples/shapeshift/language/odd.shm
   :language: shapeshift

.. program-output:: shifty shape check odd.shm
   :cwd: ../../examples/shapeshift/language
   :returncode: 1

Recursive shapes export as legal SHACL, but the SHACL specification does not
say what they mean. Shifty validates them with greatest-fixpoint semantics,
and ``export`` warns that other engines may differ.
