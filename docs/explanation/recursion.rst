Recursive shapes
================

Shape references can form cycles. SHACL 1.0 leaves their meaning undefined;
Shifty uses the following policy.

During validation, a cycle can conform if no reachable constraint violation
rules it out. For example, a shape may require each person and everyone they
know to have a name. Two named people who know each other conform; removing
either name causes the relevant checks to fail. This is called greatest
fixed-point validation.

Shifty rejects cycles through negation, such as a shape that requires a node
to fail that same shape. Qualified cardinality can also introduce a negative
dependency. Inspect a rejected schema with:

.. code-block:: bash

   shifty inspect --stage strata shapes.ttl

Inference derives new facts only from asserted or previously derived facts
(a least fixed point). When enabled, it finishes before validation.

Recursive successes can have ``coinductive`` evidence leaves. These do not
provide a finite set of triples to delete, so deletion-based repair may be
blocked.

The `recursion design notes
<https://github.com/gtfierro/shifty/blob/main/development_docs/evaluation.md>`_
explain the algorithms.
