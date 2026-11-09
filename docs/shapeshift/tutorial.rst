Write your first shapes
=======================

.. include:: experimental.inc

This tutorial writes two shapes for the VAV boxes of a building, checks them,
validates a small building against them, and reads the one failure. It needs
the ``shifty`` command (see :doc:`../install`).

Write the shapes
----------------

Save this as ``vav.shm``:

.. literalinclude:: ../examples/shapeshift/tutorial/vav.shm
   :language: shapeshift

Line by line:

- ``base`` is the IRI the shapes are named under: ``vav`` becomes
  ``<urn:example/tutorial/vav>``. ``prefix`` declares a prefix, as in Turtle.
  ``--`` starts a comment.
- ``doc "..."`` documents the shape after it. Unlike a comment, it is part of
  the shape: it is exported as ``rdfs:comment`` and shown in reports.
- ``shape vav for every brick:VAV`` defines a shape named ``vav`` and says
  which nodes it checks: every ``brick:VAV``. A shape with no ``for`` checks
  nothing on its own. ``damper`` has none; ``vav`` uses it.
- Each line in braces is one requirement, and all of them must hold.
  ``brick:isFedBy exactly 1, each a brick:AHU`` reads as written: exactly one
  value of ``brick:isFedBy``, and each value is a ``brick:AHU``.
- ``at least 1 is a brick:Air_Flow_Sensor`` counts only the values that are
  air-flow sensors, so the box may have other points too. ``at least 1
  matches damper`` counts the parts that match the ``damper`` shape: ``a``
  tests a class, and ``matches`` a shape.
- ``fed:``, ``flow:``, and ``damper:`` name their lines. A name is optional,
  but reports and the exported SHACL use it.
- ``is a brick:Damper`` is a requirement on the node itself: it is a
  ``brick:Damper``.

Check them
----------

``shifty shape check`` parses the file, resolves its imports, and builds the
shapes, without any data:

.. program-output:: shifty shape check vav.shm
   :cwd: ../examples/shapeshift/tutorial

A mistake is reported against its line. Here ``typo.shm`` is ``vav.shm`` with
``exactly`` misspelled:

.. program-output:: shifty shape check typo.shm
   :cwd: ../examples/shapeshift/tutorial
   :returncode: 1

Validate a building
-------------------

Here is a building with two VAV boxes. The damper of ``vav-2`` has no position
command:

.. literalinclude:: ../examples/shapeshift/tutorial/building.ttl
   :language: turtle

.. program-output:: shifty shape validate vav.shm --data building.ttl
   :cwd: ../examples/shapeshift/tutorial
   :returncode: 1

The report names the node, the shape, and the line that failed, with their
places in ``vav.shm``. It quotes the shape's ``doc``, and says what the line
needed and what it found. ``vav-1`` conforms, so it is not listed. The command
exits with status 1, so a CI job fails on it.

To fix the building, give the damper its command:

.. code-block:: turtle

   ex:vav-2-damper a brick:Damper ;
       brick:hasPoint ex:vav-2-damper-cmd .
   ex:vav-2-damper-cmd a brick:Damper_Position_Command .

Export SHACL
------------

``shifty shape export`` writes the shapes as SHACL Turtle, for any SHACL
engine:

.. program-output:: shifty shape export vav.shm
   :cwd: ../examples/shapeshift/tutorial

Named lines become named property shapes (``:vav.fed``), so other shapes and
reports can refer to them. ``each a brick:AHU`` became ``sh:class``, and
``at least 1 is ...`` a qualified value shape.

Format
------

``shifty shape fmt`` prints a file in the standard layout, keeping comments
and docs. ``fmt -w`` rewrites the file, and ``fmt --check`` exits with
status 1 if it is not formatted, for CI.

The same shapes in Python
-------------------------

``shifty.experimental.shapeshift`` builds the same module from Python:

.. literalinclude:: ../examples/shapeshift/tutorial/vav.py
   :language: python

.. program-output:: python vav.py
   :cwd: ../examples/shapeshift/tutorial

The output is ``vav.shm``, line for line.

Next
----

- :doc:`language` covers the rest of the language: groups of named points,
  templates, ``any of`` and ``when``, and imports.
- :doc:`python` covers the Python module.
