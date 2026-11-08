Choosing repair candidates
==========================

.. note::

   Symbolic repair is experimental. Its API may change.

Shifty describes candidate graph edits for supported constraints. Your
application chooses values and alternatives. A shape can require an email
address, for example, but cannot determine which address belongs to a person.

Fill a repair template, then check the candidate with ``gate()`` against the
full graph. Fixing one failure can affect other constraints. Some cases are
blocked, including arbitrary SPARQL constraints and some recursive shapes.

See :doc:`../how-to/repair` for an example and :doc:`../reference/repair`
for operations and limits. Algorithms and design notes are in the
`development documentation
<https://github.com/gtfierro/shifty/tree/main/development_docs>`_.
