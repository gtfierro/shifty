Why repair computes but does not choose
=======================================

Validation asks: does ``G, v ⊨ φ``? Repair asks the inverse question:

.. code-block:: text

   repair(φ, v)  =  { ΔG : (G ⊕ ΔG), v ⊨ φ }

Given a graph that fails, what set of edits would make it pass? This is
abduction — inferring the premises that would produce a desired conclusion —
and it is computed by the same structural recursion over φ that decides
validation in the first place.

Shifty's repair layer computes that set and describes it. It does not pick a
member. This page is about why that line is drawn there, because it is the
decision that shapes the whole API.

The library decides nothing
---------------------------

Every repair involves choices that the data and the schema do not determine:

- *Which focus node to fix first*, when several fail.
- *Which term fills a hole.* A missing ``ex:email`` needs an email address.
  Nothing in the graph or the shape says which one.
- *Which alternative to take*, at a disjunction. Both branches satisfy the
  shape; they are not equally good in your domain.
- *How many values to add*, when a lower bound leaves the count open.
- *Whether to accept a candidate*, given what it fixes and what it might break.
- *When to stop.*

An engine that answered these would be making domain decisions from inside a
constraint solver, using no information about the domain. It would produce
plausible, wrong data — the worst possible output for a data-quality tool,
because it is expensive to detect later.

So the split is: the library is a set of pure functions that decide nothing,
and a **driver** supplies data, choices, and control flow.

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
from terms already in the graph; it is a demonstration, and its policy is
almost certainly not yours.

The template is the interesting artifact
----------------------------------------

The central object is a ``RepairTree``: a parametric, *inspectable* description
of the entire repair space for one violation. Four constructs, mirroring φ on
purpose, because a repair tree is the skeleton of a satisfaction proof:

- ``All`` — satisfy every child (from a conjunction).
- ``Any`` — satisfy any one child (from a disjunction).
- ``Repeat [min..max]`` — instantiate the body that many times (from a
  cardinality gap).
- ``Edits`` — concrete add and delete patterns, whose slots may be **holes**.

A hole is a typed placeholder carrying what a legal value must satisfy: any
node, a freshly minted node, equality with a constant, a value type, a node
kind, membership in a finite set, or conformance to a sub-shape. The hole is
precisely the seam where domain knowledge enters, and making it a first-class
object is what lets a driver be an ASP solver, a database lookup, a form in a
UI, or a language model, without the library knowing which.

Being a description rather than an action is what makes this inspectable. You
can render a template, show it to a person, serialize the choices as data, fill
it partially, and come back to it. ``instantiate`` is a pure fold of a plan over
a template; it validates nothing and chooses nothing.

Computed from the algebra, not from the report
----------------------------------------------

Repair recurses over the shape arena rather than over the W3C validation
report, and this is not an implementation detail.

The report walker deliberately treats ``sh:and``, ``sh:or``, ``sh:not``, and
``sh:node`` as opaque units — it does not drill into sub-failures, because the
report format has no place to put them. Repair *must* drill in: to describe how
to repair ``φ₁ ∧ φ₂`` you need the repair spaces of both conjuncts. The report
is used only to seed which statements failed at which focus nodes; everything
structural comes from the algebra.

This is the same argument that makes evidence and repair the same machinery.
The witness that failure evidence produces is exactly the lossless input
synthesis needs — see :doc:`evidence-design`.

Three folds, and why there are three
------------------------------------

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
carries fuel. At fuel exhaustion a recursive obligation becomes a
``conforms to`` hole and is handed to the driver, which is a better failure mode
than either diverging or silently truncating.

Data only, and why
------------------

A template adds and deletes *data* triples. The schema is ground truth.

This is a scope decision, not a claim that it is always the right fix. Often the
correct repair is to the schema: widen a ``closed`` list, lower a ``minCount``,
delete a statement that was never right. Shifty will not propose those, because
proposing schema edits from a data failure is how a validator talks itself out
of enforcing anything. The IR is general enough to express them if that changes.

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
blocked root is an unambiguous statement that no data repair exists in scope.

The gate is whole-graph
-----------------------

A repair that fixes one node by breaking another is not a repair, so the gate
re-validates the entire graph and returns the difference against the original:
what this delta fixes, what it would introduce, and what remains.

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
