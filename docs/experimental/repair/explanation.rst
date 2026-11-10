How repair decides
==================

.. include:: experimental.inc

Shifty computes, you decide
---------------------------

A shape describes what valid data looks like. It rarely pins down one valid
graph. If a person needs an email address, the shape knows the value must be
a string, but not which string. Picking one is a fact about the world, and
the shapes graph doesn't know it.

So Shifty stops there. It computes the space of repairs as a template: what
to add or remove, with **holes** for values it can't know and **choices**
where there is more than one way to fix it. Your code fills the holes and
makes the choices.

Check against the whole graph
-----------------------------

An edit that fixes one node can break another. Adding a manager to one node
might push a second node over its ``sh:maxCount``. ``gate()`` validates the
whole patched graph and reports what the edit fixed, what it broke, and what
it left alone. Call it on every candidate.

What it can't repair
--------------------

Some constraints have no edit that undoes them. These come back
**blocked**:

- ``sh:sparql``: an arbitrary query can't be run backwards.
- An identity test on the focus node: editing data can't change which node
  it is.
- Support reached only through a recursive back-edge: there is no finite set
  of facts to delete.

Repairs edit the data graph only. If the shape is what's wrong, edit the
shape.

:doc:`how-to` shows the full loop. The algorithms are in the `development
documentation
<https://github.com/gtfierro/shifty/tree/main/development_docs>`_.
