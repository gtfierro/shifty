Why repair computes but does not choose
=======================================

.. note::

   The repair layer is **experimental**. This page describes the design as it
   currently stands. The API may change.

Validation asks: does ``G, v ⊨ φ``? Repair asks the inverse question:

.. code-block:: text

   repair(φ, v)  =  { ΔG : (G ⊕ ΔG), v ⊨ φ }

The expression describes the ideal repair set. The implementation synthesizes
candidates for constraint kinds it can invert, then checks a chosen candidate
against the whole graph. This is abduction: inferring premises that would
produce the desired conclusion.

Shifty's repair layer describes candidate edits for the constraint kinds it
can invert. The driver chooses candidates and checks them with ``gate()``.

Where the driver chooses
------------------------

Every repair involves choices that the data and the schema do not determine:

- *Which focus node to fix first*, when several fail.
- *Which term fills a hole.* A missing ``ex:email`` needs an email address.
  Nothing in the graph or the shape says which one.
- *Which alternative to take*, at a disjunction. Both branches satisfy the
  shape; they are not equally good in your domain.
- *How many values to add*, when a lower bound leaves the count open.
- *Whether to accept a candidate*, given what it fixes and what it might break.
- *When to stop.*

The graph and shapes do not provide enough information to make these choices.
For example, several email addresses may satisfy a datatype constraint, but
only one belongs to the person being described. The driver needs domain
knowledge or an external data source to choose a value.

The repair API exposes templates and a validation gate. A **driver** supplies
data, choices, and control flow; the CLI includes an optional enumeration
driver for inspection.

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - The library provides
     - The driver provides
   * - the violation horizon — what is wrong
     - which focus to fix, in what order
   * - the repair template — the inspectable space of fixes
     - how to fill holes, pick branches, set counts
   * - candidate enumeration (optional)
     - its own data sources: a database, a person, a model
   * - instantiation — choices to concrete edits
     - the plan of choices
   * - the gate — what a delta fixes and what it would break
     - whether to accept, apply, re-witness, loop, or stop

The reference drivers that ship with Shifty — enumeration, monomorphism, and
the fixpoint loop — are worked examples over this API, not privileged
components. The CLI's ``--apply`` uses the enumeration driver, which fills holes
from terms already in the graph. Applications can supply a driver with their
own selection policy.

Repair templates
----------------

The central object is a ``RepairTree``: a parametric, inspectable description
of supported candidate edits for one violation. Its four constructs follow
the structure of φ:

- ``All`` — satisfy every child (from a conjunction).
- ``Any`` — satisfy any one child (from a disjunction).
- ``Repeat [min..max]`` — instantiate the body that many times (from a
  cardinality gap).
- ``Edits`` — concrete add and delete patterns, whose slots may be **holes**.

A hole is a typed placeholder carrying what a legal value must satisfy: any
node, a freshly minted node, equality with a constant, a value type, a node
kind, membership in a finite set, or conformance to a sub-shape. The hole is
the point where the driver supplies domain knowledge. A driver can fill it using
an ASP solver, a database lookup, a UI form, or a language model.

You can render a template, show it to a person, serialize the choices, or
partially fill it before supplying the remaining values. ``instantiate``
folds a plan over a template; it validates nothing and chooses nothing.

Algebraic repair synthesis
--------------------------

Repair recurses over the shape arena. A W3C validation report identifies
failures but does not contain the nested constraint structure needed for
synthesis.

The report walker deliberately treats ``sh:and``, ``sh:or``, ``sh:not``, and
``sh:node`` as opaque units — it does not drill into sub-failures, because the
report format has no place to put them. Repair *must* drill in: to describe how
to repair ``φ₁ ∧ φ₂`` you need the repair spaces of both conjuncts. The report
is used only to seed which statements failed at which focus nodes; everything
structural comes from the algebra.

Failure evidence retains the witness needed by repair synthesis. See
:doc:`evidence-design`.

Repair synthesis folds
----------------------

Synthesis is three mutually recursive folds:

- ``repair`` — additive: make a *failing existing node* hold.
- ``break`` — deletive: falsify a *holding existing node*.
- ``build`` — additive but *hypothetical*: constrain a *not-yet-existing* node
  to satisfy a shape.

The first two are the polarity duality again: crossing a ``¬`` flips add into
delete. They both walk an already-pruned witness or trace, so they are finite.

``build`` exists because a cardinality gap says "add *n* new values satisfying
this qualifier" — values that do not exist yet, so there is nothing to witness
against. It walks the *shape* instead, since everything must be constructed. And
because a recursive shape can be built forever, ``build`` is the one that
carries a fuel limit. When that limit is reached, a recursive obligation
becomes a ``conforms to`` hole for the driver to fill.

Repair scope
------------

A template adds and deletes data triples while keeping the schema fixed.
Some failures may instead require a schema change, such as widening a
``closed`` list, lowering a ``minCount``, or deleting an incorrect statement.
Shifty does not propose schema edits; the shape author must review those
changes separately.

Blocked branches are visible
----------------------------

Some constraints admit no data repair. A ``sh:sparql`` constraint is not
algebraically invertible. An identity test on the focus node itself cannot be
satisfied by editing data. A support reached only through a greatest-fixed-point
back-edge (:doc:`recursion`) has no finite set of facts to delete.

Rather than omit these silently, the tree marks them as blocked with a reason,
and the reasons propagate the way the logic requires: an ``All`` with any blocked
child is blocked, since the conjunction is unsatisfiable in scope; an ``Any``
drops blocked children and is blocked only when all of them are. A driver
therefore never has to reason around a dead branch inside a live one, and a
blocked root means the synthesizer has no supported data repair for that focus
in its current scope.

The gate is whole-graph
-----------------------

An edit that fixes one node can introduce a violation elsewhere. The gate
re-validates the entire graph and reports which violations the delta fixes,
introduces, or leaves unresolved.

A delta is **sound** exactly when it introduces nothing. Soundness plus a
non-empty fixed set is **progress**. The gate returns this verdict and acts on
none of it. Requiring soundness, tolerating a regression in exchange for a
larger fix, or stopping after the first failed attempt are all policies, and
policies belong to the driver.

The verdict is exactly the set difference of ``violations(G ⊕ ΔG, S)`` against
``violations(G, S)``, computed by re-running the same validator. That is more
work than strictly necessary — a cheaper affected-set re-validation, restricted
to the nodes the delta can touch, would have identical semantics. Being defined
as a delta of violations rather than a local check is what makes that
substitution possible later without changing what the gate means.

Known limitations
-----------------

- **Set equality is coarse.** ``sh:disjoint``, ``sh:lessThan``,
  ``sh:lessThanOrEquals`` and ``sh:uniqueLang`` have sound per-kind repair
  strategies. ``sh:equals`` reconciliation — aligning two value sets — is
  offered only as a blunt add-one-side-or-delete-the-difference alternative, or
  blocked when neither side is safely editable. A finer set-diff plan is future
  work.
- **Edit cost is a flat default.** Each edit carries a cost for driver-side
  minimality ranking, but synthesis only assigns a default. Weighting reuse
  against minting a fresh node is left to the driver, and a principled cost
  model is open.
- **Deletion is incomplete through positive recursion**, for the coinductive
  reason above.
