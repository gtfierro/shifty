Writing shapes
==============

.. include:: experimental.inc

In this tutorial you write shapes for the VAV boxes in a building, validate
a small building against them, and fix the one failure. You need the
``shifty`` command (see :doc:`../../install`).

Write the shapes
----------------

Save this as ``vav.shm``:

.. literalinclude:: ../../examples/shapeshift/tutorial/vav.shm
   :language: shapeshift

Line by line:

- ``base`` sets the IRI the shapes are named under. ``vav`` becomes
  ``<urn:example/tutorial/vav>``.
- ``prefix`` works as in Turtle. ``--`` starts a comment.
- ``doc "..."`` documents the next shape. Unlike a comment, it is kept: it
  is exported as ``rdfs:comment`` and shown in reports.
- ``shape vav for every brick:VAV`` defines a shape and says which nodes it
  checks. ``damper`` has no ``for``, so it checks nothing by itself. ``vav``
  uses it.
- Each line in braces is one requirement, and all must hold.
  ``brick:isFedBy exactly 1, each a brick:AHU`` means one ``brick:isFedBy``
  value, and it is a ``brick:AHU``.
- ``at least 1 is a brick:Air_Flow_Sensor`` counts only the points that are
  air-flow sensors. The box can have other points too.
- ``a`` tests a class and ``matches`` tests a shape, so
  ``at least 1 matches damper`` counts parts that match ``damper``.
- ``fed:``, ``flow:``, and ``damper:`` name their lines. Names are optional.
  Reports and the exported SHACL use them.
- ``is a brick:Damper`` is a requirement on the node itself.

Check them
----------

``shifty shape check`` builds the shapes without any data, so you catch
mistakes before validating anything:

.. program-output:: shifty shape check vav.shm
   :cwd: ../../examples/shapeshift/tutorial

Errors point at their line. ``typo.shm`` is ``vav.shm`` with ``exactly``
misspelled:

.. program-output:: shifty shape check typo.shm
   :cwd: ../../examples/shapeshift/tutorial
   :returncode: 1

Validate a building
-------------------

This building has two VAV boxes. The damper of ``vav-2`` has no position
command:

.. literalinclude:: ../../examples/shapeshift/tutorial/building.ttl
   :language: turtle

.. program-output:: shifty shape validate vav.shm --data building.ttl
   :cwd: ../../examples/shapeshift/tutorial
   :returncode: 1

The report names the node, the shape, and the line that failed, and points
into ``vav.shm``. It quotes the shape's ``doc``, and says what the line
needed and what it found. ``vav-1`` conforms, so it isn't listed.

The command exits with status 1, so a CI job fails on it.

To fix the building, give the damper its command:

.. code-block:: turtle

   ex:vav-2-damper a brick:Damper ;
       brick:hasPoint ex:vav-2-damper-cmd .
   ex:vav-2-damper-cmd a brick:Damper_Position_Command .

Export SHACL
------------

Other SHACL engines don't read ``.shm``. ``shifty shape export`` writes the
shapes as SHACL Turtle:

.. program-output:: shifty shape export vav.shm
   :cwd: ../../examples/shapeshift/tutorial

Named lines become named property shapes (``:vav.fed``), so other shapes
and reports can refer to them. ``each a brick:AHU`` became ``sh:class``, and
``at least 1 is ...`` became a qualified value shape.

Format
------

``shifty shape fmt`` prints a file in the standard layout, keeping comments
and docs.

- ``fmt -w`` rewrites the file.
- ``fmt --check`` exits with status 1 if the file isn't formatted. Use it
  in CI.

The same shapes in Python
-------------------------

When shapes come from data, it is easier to generate them.
``shifty.experimental.shapeshift`` builds the same module from Python:

.. literalinclude:: ../../examples/shapeshift/tutorial/vav.py
   :language: python

.. program-output:: python vav.py
   :cwd: ../../examples/shapeshift/tutorial

The output matches ``vav.shm`` line for line.

Next steps
----------

- :doc:`language`: groups of named points, templates, ``any of`` and
  ``when``, and imports.
- :doc:`python`: the Python module.
