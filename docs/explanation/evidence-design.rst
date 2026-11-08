Understanding evidence
======================

A validation report lists failures. Evidence also shows which nodes were
checked, which passed, and which values or triples support an explanation.
For example, it can show which sensor satisfied a room's temperature-sensor
requirement. Use :doc:`../reference/shape-maps` to extract those values.

An evidence run groups results by the original SHACL statement. Each selected
node has a ``pass`` or ``fail`` status. An empty ``selected_foci`` list means
that the statement selected no nodes.

Failure explanations show the constraints that caused the failure. For other
views, use ``FocusEvaluation.progress`` for child statuses or
``session.evidence_for(focus, constraint_id)`` for one constraint's explanation.
See :doc:`../how-to/explain-failures` for examples.

Limits
------

SPARQL and expression constraints can have ``opaque`` explanations: a result
is available, but a structural explanation is not. A recursive success can
have a ``coinductive`` leaf; see :doc:`recursion`.

Path support shows one successful route. Deleting its triples may leave
another route, so repair candidates still need validation.

Evidence takes extra time and memory. Request only the explanations you need;
see :doc:`performance` and :doc:`../reference/evidence`. Implementation notes
are in the `development documentation
<https://github.com/gtfierro/shifty/tree/main/development_docs>`_.
