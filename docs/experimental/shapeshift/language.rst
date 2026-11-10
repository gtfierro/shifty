The Shapeshift language
=======================

.. include:: experimental.inc

This page covers the whole language, from a file down to a single test.
For the SHACL each construct exports to, see :doc:`reference`.

Files
-----

A file starts with a header, then declares shapes and paths in any order:

.. code-block:: shapeshift

   -- Shapes for VAV boxes.
   base <urn:example/vav/>
   prefix brick: <https://brickschema.org/schema/Brick#>

   doc "A VAV box."
   shape vav for every brick:VAV {
     brick:isFedBy exactly 1
   }

- ``base <iri>`` names the file's shapes: ``vav`` is
  ``<urn:example/vav/vav>``. Without a base, the build warns and uses a
  placeholder ``urn:shapeshift:`` IRI made from the file name.
- ``prefix name: <iri>`` works as in Turtle. ``rdf``, ``rdfs``, ``xsd``,
  ``owl``, and ``sh`` are already declared.
- ``import`` brings in another file or an RDF graph. See `Imports`_.
- ``--`` starts a comment, which runs to the end of the line.
- ``doc "..."`` documents the next shape or line. Unlike a comment, a doc is
  exported and quoted in reports. Triple quotes (``"""..."""``) allow
  several lines.

Inside braces, a line ends at a newline or a ``;``. Names use letters,
digits, ``_``, and ``-``: ``zone-temperature``, ``SAT_AVG``.

Shapes and targets
------------------

``shape name { lines }`` defines a shape. A node conforms when every line
holds. ``for`` clauses say which nodes the shape checks:

.. code-block:: shapeshift

   shape vav
     for every brick:VAV                      -- instances of a class
     for every node with brick:hasDamper      -- subjects of a property
     for every value of brick:feeds           -- objects of a property
     for node ex:vav-7                        -- one node
   {
     brick:hasPoint required
   }

Two more targets:

- ``for every node matching s`` checks the nodes that conform to shape
  ``s``.
- ``for sparql "SELECT ?this WHERE { ... }"`` checks the nodes a query
  returns.

A shape with no ``for`` checks nothing by itself. It is a definition for
other shapes to use. In SHACL it is easy to target nodes by accident. In
Shapeshift, what a shape checks is always on its first line.

``for every C`` and the test ``a C`` include instances of subclasses of
``C``, as in SHACL, as long as the ``rdfs:subClassOf`` triples are in the
validated graph.

Fields
------

Most lines are fields. A field gives a path, how many values it has, and
tests on the values.

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

A field with no count can have any number of values.

Each count has one spelling. Write ``required``, not ``at least 1``, and
``optional``, not ``at most 1``. The build points out the other spellings.

After the count come qualifiers, separated by commas:

- ``each T``: every value passes the test ``T``. A node with no values
  passes too, so "one or more, all points" is
  ``required, each a brick:Point``.
- ``at least n is T`` (and ``at most``, ``exactly``): count only the values
  that pass ``T``. Other values are allowed. For a shape, write
  ``matches``: ``at least 1 matches damper``.

The two are easy to mix up, so they use different words:

.. code-block:: shapeshift

   -- every point is a brick:Point
   brick:hasPoint each a brick:Point
   -- one point, at least, is an airflow sensor; the others can be anything
   brick:hasPoint at least 1 is a brick:Air_Flow_Sensor

A value test always follows ``each`` or a qualified count. ``ex:p string``
is an error; write ``ex:p each string``.

Fields can also compare two properties, or check languages:

.. code-block:: shapeshift

   ex:start below ex:end              -- each value of one below each of the other
   ex:low up to ex:high               -- ... or equal to it
   rdfs:label same as skos:prefLabel  -- the same set of values
   ex:parent different from ex:child  -- no value in common
   rdfs:label unique language         -- at most one label per language

Tests
-----

A test is what ``each`` and ``at least n is`` check on each value. Every
test starts with a word that says what kind of test it is:

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

``a`` and ``matches`` keep classes and shapes apart. ``each brick:AHU`` is
an error that suggests ``each a brick:AHU``. ``each damper`` suggests
``each matches damper``.

Built-in datatypes are written as words: ``string``, not
``datatype xsd:string``.

``or`` after ``a`` lists classes. A value passes when it is an instance of
any of them. For alternatives that aren't all classes, use ``any of`` (see
`Frames`_).

.. code-block:: shapeshift

   brick:isFedBy exactly 1, each a brick:AHU or brick:RTU
   brick:hasPoint at least 1 is a brick:Air_Flow_Sensor or brick:Air_Flow_Setpoint

Ranges work on numbers, dates, and times. Dates and times are written bare,
in XSD form, with an optional timezone (``Z``, ``+02:00``):

- ``2024-01-31`` is an ``xsd:date``.
- ``2024-01-31T08:00:00Z`` is an ``xsd:dateTime``.
- ``08:00:00`` is an ``xsd:time``.

``one of`` takes them too:

.. code-block:: shapeshift

   ex:commissioned optional, each date, each from 2000-01-01
   ex:reading each above 2024-01-01T00:00:00Z below 2025-01-01T00:00:00Z
   ex:opens each from 06:00:00 to 10:00:00
   ex:holiday each one of 2024-12-25 2024-12-26

Both ends of a range must be the same kind, and only values of that kind
fall in it. No number is in a date range, and no ``xsd:dateTime`` either.
A date that doesn't exist, like ``2024-02-30``, is an error.

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

Long paths repeat, so ``path`` names one for reuse. A path with parameters
is a template:

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

``name:`` before a line names it. The name doesn't change what the line
requires. It gives the line an identity:

- It exports as a named property shape (``<urn:example/vav/vav.fed>``).
- Reports and shape maps refer to it by name.
- Other shapes can reuse it.

A field fits on one line. Its ``doc``, ``message``, and ``severity`` go on
the lines before it:

.. code-block:: shapeshift

   shape vav for every brick:VAV {
     flow: brick:hasPoint at least 1 is a brick:Air_Flow_Sensor
     doc "Dual-duct boxes are modelled as two VAVs."
     message "A VAV must be fed by exactly one AHU."
     fed: brick:isFedBy exactly 1, each a brick:AHU
     unit: brick:hasUnit optional, each iri
   }

To reuse a shape, write its name alone on a line. Its lines then hold here
too, but its targets don't come along. ``shape.name`` reuses one named
line:

.. code-block:: shapeshift

   shape vav for every brick:VAV {
     fed: brick:isFedBy exactly 1, each a brick:AHU
     flow: brick:hasPoint at least 1 is a brick:Air_Flow_Sensor
   }

   shape reheat-vav for every brick:RVAV {
     vav.fed                                    -- only the AHU rule
     reheat: brick:hasPoint at least 1 is a brick:Reheat_Valve_Command
   }

``is`` on a line tests the node itself. ``is a brick:Damper`` says the node
is a damper, and ``is iri`` says it is an IRI. A shape doesn't need ``is``:
alone on a line, it already applies to the node.

Groups
------

Specifications list equipment as points: a supply-air sensor, a return-air
sensor, and so on. A group writes such a list along one path. Each row
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

A row counts the values of its class, or that match its shape. Rows are
always a class or a shape, so they skip ``a`` and ``matches``.

- A row requires exactly one value by default.
- A count after the path sets the default for every row. A count on a row
  sets its own.
- ``C { lines }`` counts values that are ``C`` and satisfy the lines.
- A row's name is optional, and its ``doc`` goes on the line before it.

For two values of one class, write one row with ``exactly 2``. Repeating
the row requires only one value, and the build warns about it.

With ``path points = brick:hasPoint``, a group reads ``points { ... }``.

Frames
------

Some rules are choices ("a damper command or a damper part") or bans.
Frames write these as blocks after a phrase that says how they combine:

.. code-block:: shapeshift

   any of { brick:hasPoint at least 1 is a brick:Damper_Position_Command }
          { brick:hasPart at least 1 is a brick:Damper }
   exactly one of { brick:hasTag required } { rdfs:label required }
   none of { brick:hasLocation required }
   at least 2 of { brick:hasPart required } { brick:isPartOf required } { brick:feeds required }
   at most 1 of { brick:hasTag required } { brick:hasLocation required } { brick:feeds required }
   when { brick:hasPoint at least 1 is a brick:Economizer_Status }
   then { brick:hasPoint at least 1 is a brick:Outside_Air_Damper }

- Every line of a block must hold, so there is no ``all of``.
- ``at least 1 of`` is written ``any of``, and ``at most 0 of`` is
  ``none of``.
- ``when C then B`` means "if ``C`` holds, ``B`` must": "if it has X, it
  must have Y".

There is no ``or`` between lines and no ``not``. The first word of a frame
tells you what it means.

To choose among values instead of lines, use a test:
``each one of ex:on ex:off``.

Templates
---------

Many rules differ only in a class or a number. A shape with parameters is a
template for them. A parameter can have a default, which can use an earlier
parameter:

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

Arguments are positional, then named. Each call is checked against the
template. A missing argument, or a name where a number belongs, is an error
that shows the template's parameters.

On export, calls with the same arguments share one shape, which records the
call it came from. A labelled call (``temp: has-point(...)``) is exported
under its label instead: ``vav.temp``.

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

``severity`` (``info``, ``warning``, or ``violation``) and ``message``
apply to the next line only. Here the temperature line is a warning, and a
missing part is still a violation. Before a ``shape``, they apply to the
whole shape.

``message`` speaks to whoever wrote the data. ``doc`` explains the line to
whoever reads the shapes next.

``only these properties`` closes a shape. A node can then have no
properties the shape doesn't name, other than ``rdf:type``:

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

A ``.shm`` path is relative to the importing file.

- With ``as sensors``, its names are written ``sensors.temp-sensor``.
- Without an alias, they are written bare.
- Shapes keep the IRIs of the file that defines them.

An RDF import can be an ontology, a SHACL shapes graph, or both. Importing
an ontology catches typos: ``shifty shape check`` reports a class the
ontology doesn't define, and suggests the nearest one that it does.
Imported shapes can be used with ``matches``.

Imports are never fetched unless you allow it. ``--map IRI=PATH`` reads an
IRI from a local file, and ``--fetch`` downloads the rest.

Recursion
---------

Buildings nest: a space has parts that are spaces. A shape can refer to
itself, directly or through other shapes. ``each matches`` is always
safe:

.. code-block:: shapeshift

   shape space for every brick:Space {
     brick:hasPart each matches space
   }

A loop must not pass an odd number of times through ``none of``,
``at most n of``, an ``at most n is`` or ``at most n matches`` qualifier, or
a ``when`` condition. Such a loop has no consistent meaning, and the build
names it:

.. literalinclude:: ../../examples/shapeshift/language/odd.shm
   :language: shapeshift

.. program-output:: shifty shape check odd.shm
   :cwd: ../../examples/shapeshift/language
   :returncode: 1

Recursive shapes export as legal SHACL, but the SHACL specification
doesn't say what they mean. Shifty uses greatest-fixpoint semantics, and
``export`` warns that other engines may differ.
