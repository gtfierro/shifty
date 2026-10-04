Why evidence
============

A SHACL validation report lists failures. This is useful for a build step
that should fail on invalid data, but it leaves out information computed
during validation.

The validator decided conformance by structural recursion over the constraint.
At every step it knew which sub-constraint held, on which values, supported by
which triples. The evidence interface retains that derivation after computing
the boolean.

Information absent from validation reports
------------------------------------------

**Why a node passed.** A report has no row for a conforming node, so there is
no way to ask what satisfied the constraint. If your shape says a VAV has a
supply-air temperature sensor, and it does, the report will not tell you which
sensor — you have to write a second query that re-implements the shape's
property paths and qualified-value filtering. That query is a duplicate of
logic the validator already executed, and it can drift out of sync with the
shape. :doc:`../reference/shape-maps` exists to eliminate it.

**Whether a node was checked.** Neither passing nodes nor unselected nodes
appear in a report. To check which assets a profile applied to, you need to
distinguish these cases.

**How a constraint failed.** A report gives a message and a constraint
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

A focus node without a row was not selected. A ``pass`` row means the
constraint held, and a ``fail`` row means it did not. Statements with no
selected focus nodes are retained with an empty ``selected_foci`` list, so you
can check whether a shape applied to anything.

Satisfaction and failure evidence
---------------------------------

``Satisfaction`` and ``Failure`` are logical complements. They are computed by
mutually recursive folds over the same shape arena, using the same conformance
oracle, traversal, and projection code.

The mutual recursion is forced by negation. To explain why ``¬φ`` *failed*, you
have to explain why ``φ`` *held* — so the failure fold calls the satisfaction
fold, and vice versa. Every ``¬`` flips the direction. Counting is the other
flip point, and it is self-dual: a lower bound is broken by removing matches, an
upper bound by adding them.

Repair uses satisfaction evidence when fixing a failure under negation. It
must falsify a constraint that currently holds, and the satisfaction trace
records the values and triples that support that constraint.

Canonical evidence
------------------

A failed conjunction retains the children that establish the failure and drops
the ones that passed.

The failing children explain why the conjunction failed and identify what
repair needs to address. Omitting passing children also reduces the size of
failure trees (see :doc:`performance`).

For a UI that shows progress, such as "three of these four obligations are
met", use ``FocusEvaluation.progress``. It reports the immediate authored
children and their statuses without building a derivation for each.

Canonical evidence, progress, and on-demand evidence provide different views:

- **canonical evidence** answers *why did this result hold?*
- **progress** answers *what happened to the immediate authored children while
  evaluating it?*
- **session.evidence_for(focus, constraint_id)** materializes the full evidence
  for one of those elided children, on demand.

Source and normalized identities
--------------------------------

Evidence carries both a source and a normalized identity for every statement
and constraint. This is a direct consequence of compiling shapes
(:doc:`architecture`): the normalizer deduplicates structurally identical nodes,
folds contradictions, and rewrites boolean structure, so the executed algebra
does not correspond one-to-one with what you wrote.

The source identity links a constraint to the shapes file. The normalized
identity identifies the executed constraint; several source statements may
share it after common-subexpression elimination. Keeping both lets you relate
the evaluation to the original SHACL.

Limits of evidence
------------------

Validation status is exact, but a structural explanation is not always
available. The evidence tree marks these cases explicitly.

A ``sh:sparql`` constraint is **opaque**. An arbitrary SPARQL query is not
something the algebra can fold over, so a failing one carries its query
diagnostic and nothing structural, and a passing one is **blocked** for repair
purposes — a query cannot generally be falsified by a sound deletion. SHACL-AF
expression failures are opaque for the same reason. Passing closed and
relational constraints are blocked only in the deletive direction, which does
not affect their validation result.

Under greatest-fixed-point semantics (:doc:`recursion`), a node can conform
because no counterexample is reachable, without a finite set of supporting
triples. Evidence records this as a ``coinductive`` leaf.

``PathSupport`` records one concrete successful route rather than enumerating
all of them. For an alternative path,
Shifty keeps the first successful syntactic alternative. So a path support is a
positive reachability certificate and is **not** a deletion cut — anything
derived from it is a candidate that still has to pass the repair gate.

The costs
---------

Generating evidence retains derivations that conformance checking can discard.
Runtime and output size depend on the number of selected pairs and the
structure of the constraints and paths they traverse.

The interface supports conformance counts, failure discovery, single-pair
explanations, and full evidence. If you only need failure explanations, finding
failures first avoids building evidence for passing pairs. See :doc:`performance`
for entry-point guidance.
