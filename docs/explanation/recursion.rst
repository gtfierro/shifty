Recursion and stratification
============================

SHACL shapes can reference each other through ``sh:node``, ``sh:property``, and
``sh:qualifiedValueShape``, and nothing stops those references from forming a
cycle. The W3C specification leaves the meaning of such a schema **undefined**.
That is not an oversight to route around; for some cyclic schemas there is
genuinely no consistent answer, and a validator has to decide what it does
about that.

This page describes what Shifty decided and why. It is load-bearing: every
optimization in the normalizer is only sound relative to this choice.

The paradox
-----------

Consider the smallest problematic schema:

.. code-block:: text

   S := ¬ ∃p. S        "v conforms iff no p-successor conforms"

and a graph where a node has a ``p``-edge to itself. If the node conforms, then
it has a conforming ``p``-successor — itself — so it does not conform. If it
does not conform, then it has no conforming ``p``-successor, so it conforms.
There is no two-valued assignment that satisfies the definition. Not "hard to
compute": there is no answer.

A validator can respond in three ways. It can adopt a three-valued semantics,
where the node's status is *undefined*, which is principled and drags a third
truth value through every operator, every optimization, and every report. It
can pick an answer, which is fast and wrong. Or it can detect the situation and
refuse.

Shifty refuses, with a diagnostic naming the cycle. The reasoning is that a
schema like this is nearly always a mistake in the schema, and the useful thing
to do with a mistake is name it. Silently returning ``conforms: true`` for a
question with no answer is the one outcome with no recovery path — you cannot
tell it apart from a real pass.

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

Polarity is semantic, not syntactic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There is a trap here worth spelling out, because getting it wrong would reject
schemas that are perfectly fine.

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

Within a stratum, validation and inference use *opposite* fixed points. This
sounds inconsistent and is not: they are answering different questions.

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

**Validation uses the greatest fixed point.** For a universal constraint, the
coinductive reading is what people usually mean — "everyone I transitively
follow is verified" is a safety property, not a claim that the follow graph
terminates. And the alternative flags legitimate cyclic data as invalid, which
matters for social graphs and any other genuinely cyclic domain. For acyclic
data, such as Brick's part-of and feeds hierarchies, the choice makes no
difference at all.

**Inference uses the least fixed point.** It has to. A rule fires when its body
is actually satisfied by asserted or derived triples; a fact cannot be
materialized on the grounds that it justifies itself. The least fixed point is
also the standard semi-naive rule evaluation.

The two never conflict because they run in separate phases: inference to a
fixed point first, then validation over the result.

The cost of the choice
~~~~~~~~~~~~~~~~~~~~~~

Under the greatest fixed point, an *inductive* constraint — "this structure
must be acyclic" or "this chain must be finite" — is not expressible by default.
It would need an explicit acyclicity check. That is a real loss, and it is the
price of not flagging cyclic data.

It is not a one-way door. Stratification supports either direction per positive
stratum, so the greatest fixed point is the documented default rather than a
structural commitment.

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
back-edge is recorded as a ``coinductive`` satisfaction leaf. That is an honest
label. The node conforms under the greatest-fixed-point semantics, but there is
no finite set of supporting triples to point at — the justification is the
absence of a counterexample, not the presence of a witness. Repair inherits the
same limit: there is nothing finite to delete, so deletion-direction repair is
incomplete through positive recursion, and says so rather than guessing.
