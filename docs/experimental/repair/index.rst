Symbolic repair
===============

.. include:: experimental.inc

A validation report says what is wrong. It does not say what to change.
Symbolic repair works out the edits that would make a failing node conform.

It stops short of choosing them. Shifty returns a template with holes and
choices in it, and your code fills them in.

.. code-block:: python

   from shifty.experimental import repair

   session = repair.RepairSession(shapes, data)
   failure = session.witnesses()[0]
   tree = failure.repair_tree()    # the edits that would fix it, with holes
   print(tree.explain())

A fix for one node can break another, so ``session.gate(delta)`` validates
the whole patched graph before you accept an edit.

It works from Python, from the command line (``shifty repair``), and from
Rust.

.. list-table::
   :widths: 30 70

   * - :doc:`how-to`
     - Inspect the repair space, fill a template, check it, and apply it.
   * - :doc:`explanation`
     - What Shifty computes, what you decide, and what it can't repair.
   * - :doc:`reference`
     - Sessions, templates, holes, plans, deltas, and the gate.

To test a repair tool against known answers, see
:doc:`../violation-generation/index`.

.. toctree::
   :hidden:

   how-to
   explanation
   reference
