Shapeshift
==========

.. include:: experimental.inc

Shapeshift is a language for writing SHACL shapes. A ``.shm`` file reads as a
list of requirements, one per line, in the words of the domain:

.. code-block:: shapeshift

   base <urn:example/building/>
   prefix brick: <https://brickschema.org/schema/Brick#>

   doc "A VAV box: fed by one AHU, with an airflow sensor."
   shape vav for every brick:VAV {
     fed: brick:isFedBy exactly 1, each a brick:AHU
     flow: brick:hasPoint at least 1 is a brick:Air_Flow_Sensor
   }

Shifty compiles the file to its shape algebra, so it validates exactly as the
equivalent SHACL does, and raises it to readable SHACL Turtle for other
tools. Validation reports a failure against the line you wrote, not against a
blank-node property shape.

There are two ways to write Shapeshift:

- **As text**, in ``.shm`` files, with the ``shifty shape`` commands to check,
  format, export, and validate them.
- **From Python**, with ``shifty.experimental.shapeshift``, which builds the
  same modules from Python values and writes them as ``.shm`` text or SHACL.
  Use it when shapes are generated from data, or to share pieces between
  files with ordinary Python imports.

From Rust, the ``shifty-experimental`` crate is the entry point:
``shifty_experimental::shapeshift`` parses, formats, builds, and exports
``.shm`` files, and runs the sessions behind ``shifty shape``. It is kept
apart from Shifty's stable crates, and the crates it is built from
(``shapeshift``, ``shapeshift-parse``, and the others under
``crates/experimental/``) are its implementation, not an interface.

.. list-table::
   :widths: 30 70

   * - :doc:`tutorial`
     - Write a shape, check it, validate a building against it, and read the
       failure.
   * - :doc:`language`
     - The language: shapes, targets, fields, groups, frames, templates, and
       imports.
   * - :doc:`python`
     - Write Shapeshift modules from Python.
   * - :doc:`reference`
     - What each construct means in SHACL, the reserved words, and the
       ``shifty shape`` commands.

The Guideline 36 sequences in ``examples/shapeshift/g36/`` in the repository
are a larger example, written in Python and generated as ``.shm`` files.

.. toctree::
   :hidden:

   tutorial
   language
   python
   reference
