Why evidence
============

A SHACL validation report is a list of what went wrong. That is the right
output for a gate — a build step that should fail on bad data needs a boolean
and a diagnostic — and it is a lossy summary of what the validator computed.

The validator decided conformance by structural recursion over the constraint.
At every step it knew which sub-constraint held, on which values, supported by
which triples. That derivation existed, was used to produce the boolean, and
was discarded. Evidence is the decision to keep it.

Three things a report cannot tell you
-------------------------------------

**Why a node passed.** A report has no row for a conforming node, so there is
no way to ask what satisfied the constraint. If your shape says a VAV has a
supply-air temperature sensor, and it does, the report will not tell you which
sensor — you have to write a second query that re-implements the shape's
property paths and qualified-value filtering. That query is a duplicate of
logic the validator already executed, and it can drift out of sync with the
shape. :doc:`../reference/shape-maps` exists to eliminate it.

**Whether a node was checked at all.** In a report, a node that passed and a
node no target selected look identical: absent. For a coverage question —
"which of my assets did this profile actually apply to?" — that is exactly the
distinction you need.

**Why the failure is a failure.** A report gives a message and a constraint
component. It does not give the shape of the derivation: which branch of a
disjunction was tried, which values were counted, which triples supported the
path that reached the offending value. Anything downstream that wants to act on
a failure has to reconstruct it.

The interface is statement-oriented
-----------------------------------

Evidence is organised around *authored statements* rather than findings. Every
statement that was included in the run appears, and each selected
``(statement, focus)`` pair gets exactly one row of one polarity:

.. code-block:: text

   EvidenceRun
   └── StatementEvaluation                 one per included authored statement
       ├── selected_foci = []               target selected nothing
       └── FocusEvaluation                  one per selected focus node
           ├── status = "pass" → Satisfaction
           └── status = "fail" → Failure

This makes the three states observably different: no row means unselected, a
``pass`` row means checked and held, a ``fail`` row means checked and did not.
Retaining statements whose target selected nothing is the part that requires
deliberate effort — the easy implementation drops them, and with them the
answer to "did this shape apply to anything?"

Two polarities, one machine
---------------------------

``Satisfaction`` and ``Failure`` are not two report formats that happen to
resemble each other. They are logical complements, computed by mutually
recursive folds over the same arena with the same conformance oracle, and they
share their traversal and projection code.

The mutual recursion is forced by negation. To explain why ``¬φ`` *failed*, you
have to explain why ``φ`` *held* — so the failure fold calls the satisfaction
fold, and vice versa. Every ``¬`` flips the direction. Counting is the other
flip point, and it is self-dual: a lower bound is broken by removing matches, an
upper bound by adding them.

This is also why satisfaction evidence is not a nice-to-have. Repair needs it:
when a repair crosses a negation, the only way to fix the failure is to falsify
something that currently holds, and the satisfaction trace is the record of
what to falsify. The two polarities are one machine because the problem is one
problem.

Canonical evidence is a proof, not a log
----------------------------------------

A failed conjunction retains the children that establish the failure and drops
the ones that passed. This is deliberate, and it is the design decision people
question most, so it is worth stating the reasoning.

Canonical evidence answers *why did this result hold?* The answer to that
question, for a conjunction, is the failing conjunct. A passing sibling is not
part of the explanation — including it would inflate every failure tree with
irrelevant material, and the trees are already large enough to be a performance
concern (see :doc:`performance`). It also happens to be exactly the shape repair
needs, since a repair must address the failing conjunct and has no business
touching the passing one.

But a UI often *does* want the siblings: "three of these four obligations are
met" is useful to a person, and it is not what a proof contains. So the
authored children are available separately, through
``FocusEvaluation.progress``, which reports the immediate authored children and
their statuses without materializing why each held.

The division of labour:

- **canonical evidence** answers *why did this result hold?*
- **progress** answers *what happened to the immediate authored children while
  evaluating it?*
- **session.evidence_for(focus, constraint_id)** materializes the full evidence
  for one of those elided children, on demand.

Two identities, because normalization is not identity-preserving
----------------------------------------------------------------

Evidence carries both a source and a normalized identity for every statement
and constraint. This is a direct consequence of compiling shapes
(:doc:`architecture`): the normalizer deduplicates structurally identical nodes,
folds contradictions, and rewrites boolean structure, so the executed algebra
does not correspond one-to-one with what you wrote.

The source identity is what you correlate with author intent — this constraint
came from that line of that shapes file. The normalized identity is what
actually ran, and several source statements may share one after
common-subexpression elimination. Discarding either one loses something: with
only source ids you cannot explain the execution, and with only normalized ids
you cannot point at the SHACL the user wrote.

What evidence honestly cannot explain
-------------------------------------

The validation *status* is exact everywhere. The explanation is not always
available, and the cases where it is missing are marked in the tree rather than
silently degraded.

A ``sh:sparql`` constraint is **opaque**. An arbitrary SPARQL query is not
something the algebra can fold over, so a failing one carries its query
diagnostic and nothing structural, and a passing one is **blocked** for repair
purposes — a query cannot generally be falsified by a sound deletion. SHACL-AF
expression failures are opaque for the same reason. Passing closed and
relational constraints are blocked only in the deletive direction, which does
not affect their validation result.

Coinductive satisfaction is the subtlest case. Under greatest-fixed-point
semantics (:doc:`recursion`), a node can conform because no counterexample is
reachable, and there is then no finite set of supporting triples to point at.
Evidence records a ``coinductive`` leaf. That is a real limit, not a placeholder
for missing work.

One more limit worth stating plainly: a ``PathSupport`` is *one* concrete
successful route, not an enumeration of all of them. For an alternative path,
Shifty keeps the first successful syntactic alternative. So a path support is a
positive reachability certificate and is **not** a deletion cut — anything
derived from it is a candidate that still has to pass the repair gate.

The costs
---------

Keeping the derivation is not free, and the numbers are not small. Materializing
evidence for every selected pair costs 2.5–5.4x deciding conformance, rising
with model size, and a mid-size model's serialized run can reach tens of
megabytes before compaction.

That is why the interface is a set of graded entry points rather than one
function: decide conformance, find which pairs failed, explain one pair, or
explain everything. Most callers want to know why something *failed*, and
failures are a small minority of pairs — so finding the failures and explaining
each one costs a few percent over plain conformance rather than several times
it. :doc:`performance` has the measurements, the attribution, and an account of
which optimizations worked.
