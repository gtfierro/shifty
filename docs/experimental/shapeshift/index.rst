Shapeshift
==========

.. include:: experimental.inc

SHACL in Turtle is hard to read and harder to write. One requirement
becomes a nest of blank nodes, and a failure report points at a blank node
instead of the rule you meant.

Shapeshift is a short language for the same shapes. A ``.shm`` file is a
list of requirements, one per line:

.. code-block:: shapeshift

   base <urn:example/building/>
   prefix brick: <https://brickschema.org/schema/Brick#>

   doc "A VAV box: fed by one AHU, with an airflow sensor."
   shape vav for every brick:VAV {
     fed: brick:isFedBy exactly 1, each a brick:AHU
     flow: brick:hasPoint at least 1 is a brick:Air_Flow_Sensor
   }

It compiles to the same shape algebra as SHACL, so it validates exactly
like the equivalent SHACL. Failures are reported against the line you
wrote. ``shifty shape export`` writes readable SHACL Turtle for other
tools.

You can write Shapeshift two ways:

- **As text**, in ``.shm`` files, with the ``shifty shape`` commands.
- **From Python**, with ``shifty.experimental.shapeshift``. Use this when
  shapes come from data, or to share pieces between files with Python
  imports.

From Rust, use ``shifty_experimental::shapeshift`` in the
``shifty-experimental`` crate. The other crates under
``crates/experimental/`` are its implementation, not an interface.

.. list-table::
   :widths: 30 70

   * - :doc:`tutorial`
     - Write a shape, validate a building against it, and read the failure.
   * - :doc:`language`
     - The whole language, from files down to single tests.
   * - :doc:`python`
     - Write Shapeshift modules from Python.
   * - :doc:`reference`
     - The SHACL for each construct, reserved words, and commands.

For a larger example, see ``examples/shapeshift/g36/`` in the repository:
the ASHRAE Guideline 36 sequences, written in Python and generated as
``.shm`` files.

.. toctree::
   :hidden:

   tutorial
   language
   python
   reference
