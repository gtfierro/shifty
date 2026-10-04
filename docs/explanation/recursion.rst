Recursion and stratification
============================

SHACL shapes can reference each other through ``sh:node``, ``sh:property``, and
``sh:qualifiedValueShape``, and nothing stops those references from forming a
cycle. The W3C specification leaves the meaning of such a schema **undefined**.
Some cyclic schemas have no consistent two-valued answer.

Shifty accepts stratifiable recursion and rejects cycles through negation.
The normalizer's rewrites preserve conformance under these semantics.

The paradox
-----------

Consider the smallest problematic schema:

.. code-block:: text

   S := ¬ ∃p. S        "v conforms iff no p-successor conforms"

and a graph where a node has a ``p``-edge to itself. If the node conforms, then
it has a conforming ``p``-successor — itself — so it does not conform. If it
does not conform, then it has no conforming ``p``-successor, so it conforms.
There is no two-valued assignment that satisfies the definition.

A validator could introduce a third truth value, *undefined*, and propagate it
through evaluation, optimization, and reporting. Shifty instead rejects the
schema with a diagnostic naming the cycle, so the shape author can correct it.
Returning a boolean in this case would give an inconsistent result.

Stratification
--------------

The paradox needs a cycle *through a negation*. Purely positive recursion —
"every node I point at also conforms" — has clean fixed points and is common
and useful.

So the test is stratifiability: build the shape dependency graph with edges
labelled by polarity, condense it into strongly connected components, and check
whether any component contains a negative internal edge. If none does, the
schema splits into strata that can be evaluated bottom-up, each fully decided
before the next. If one does, the schema is refused.

The same machinery serves SHACL-AF rule inference, which is Datalog with
stratified negation. Recursive validation and recursive inference run on one
engine rather than two.

Dependency polarity
~~~~~~~~~~~~~~~~~~~

Dependency polarity follows monotonicity rather than the surface syntax of the
normalized expression.

Shifty encodes ``∀π.φ`` as ``∃≤0 π.¬φ`` (see :doc:`architecture`). So a
thoroughly positive SHACL constraint — ``sh:node S`` inside a property shape —
looks *syntactically negative* in the IR: there is a ``¬`` right there. But it
sits under an upper-bound count, which is itself anti-monotone, and two
anti-monotone operators compose to a monotone one. The constraint is positive.

The dependency analysis therefore has to track monotonicity rather than surface
negation signs:

.. list-table::
   :widths: 60 40
   :header-rows: 1

   * - Construct
     - Polarity of the referenced shape
   * - ``sh:node``, ``sh:property``, ``minCount``, a qualifier under a lower
       bound
     - **positive** (monotone)
   * - ``sh:not``
     - **negative**
   * - ``maxCount``, a qualifier under an upper bound
     - **negative** (anti-monotone)
   * - ``closed``, ``disjoint``
     - **negative**

Because a fused ``Count`` node carries one qualifier constrained by both
bounds, the analysis un-fuses it: the ``min`` side contributes a positive edge
and the ``max`` side a negative one. A qualifier governed by both — a genuine
``sh:qualifiedValueShape`` with a min and a max — is non-monotone, so it
contributes a negative edge.

Two fixed points
----------------

Within a stratum, validation uses a greatest fixed point and inference uses a
least fixed point.

Least and greatest fixed points differ only on cyclic data; on a DAG they
coincide. Take the constraint "*v* conforms iff *v* is a Person and every
``knows``-neighbour conforms", over two people who know each other:

- The **least** fixed point builds up from grounded base cases. The cycle never
  bottoms out, so neither node is ever established as conforming, and both
  **fail**. This is the inductive reading: conformance must be finitely
  justified.
- The **greatest** fixed point starts by assuming everything conforms and
  removes anything with a concrete violation. Neither node has one, so both
  **conform**. This is the coinductive reading: no reachable counterexample.

**Validation uses the greatest fixed point.** A universal constraint such as
"everyone I transitively follow is verified" can hold on cyclic data. The
coinductive interpretation accepts those cycles when no reachable node
violates the constraint. For acyclic data, such as Brick's part-of and feeds
hierarchies, the two fixed points give the same result.

**Inference uses the least fixed point.** A rule fires when its body is
satisfied by asserted or previously derived triples. This prevents facts from
being derived solely because they justify themselves, and matches standard
semi-naive rule evaluation.

The two never conflict because they run in separate phases: inference to a
fixed point first, then validation over the result.

The cost of the choice
~~~~~~~~~~~~~~~~~~~~~~

Under the greatest fixed point, an *inductive* constraint — "this structure
must be acyclic" or "this chain must be finite" — is not expressible by default.
It would need an explicit acyclicity check.

Stratification could support either fixed point per positive stratum. Shifty
currently uses the greatest fixed point for validation.

In practice
-----------

.. code-block:: bash

   shifty inspect --stage strata shapes.ttl

.. code-block:: text

   strata: stratifiable = true; 13 shape(s) in 13 stratum(strata); 0 recursive component(s)

A schema with no cycles reports zero recursive components, and the stratum
count is just the topological layering. When a schema is refused, this stage
names the offending cycle.

One consequence shows up in evidence: a recursive success reached through a
back-edge is recorded as a ``coinductive`` satisfaction leaf. The node conforms
under greatest-fixed-point semantics because there is no reachable
counterexample, without a finite set of supporting triples. Deletion-direction
repair cannot construct a deletion from this evidence and marks the branch as
blocked. It is therefore incomplete through positive recursion.
