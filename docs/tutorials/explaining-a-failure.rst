Asking why a node passed or failed
==================================

:doc:`reading-results` ended on two questions a validation result cannot
answer, because a result is a list of failures: *which nodes passed?* and *why,
exactly, did this one fail?*

This tutorial answers both with the evidence interface, which keeps the
derivation the validator built instead of discarding it once the boolean has
been extracted. By the end you will be able to tell a passing node from an
unchecked one, and read a failure as a tree rather than a message.

Set up
------

Use the failing version of ``data.ttl`` from the first tutorial, so there is
something to explain:

.. code-block:: turtle

   @prefix ex: <http://example.org/> .

   ex:alice a ex:Person ; ex:name "Alice" ; ex:email "alice@example.org" .
   ex:bob   a ex:Person ; ex:name 123 .

``shapes.ttl`` is unchanged.

Evidence: the derivation, kept
------------------------------

An ordinary validation report is lossy on purpose: it tells you what to fix and
discards everything else. The evidence interface keeps the derivation instead.
Open a session over the two graphs and validate:

.. code-block:: python

   import pathlib
   import shifty

   shapes = pathlib.Path("shapes.ttl").read_text()
   data = pathlib.Path("data.ttl").read_text()

   session = shifty.EvidenceSession(shapes, data, infer=False)
   run = session.validate()

   print("conforms:", run.conforms)
   for statement in run.statements:
       print(statement.selector, "selected", len(statement.selected_foci))
       for focus in statement.selected_foci:
           print("  ", focus.status, focus.focus)

.. code-block:: text

   conforms: False
   class(<http://example.org/Person>) selected 2
      pass <http://example.org/alice>
      fail <http://example.org/bob>

Alice is here. That is the first thing the evidence interface buys you: the
report from the previous tutorial had no row for Alice, and no way to tell
"passed" apart from "never selected". Here there are three distinct states — a
statement whose target selected nothing has an empty ``selected_foci`` list, a
selected node that passed has ``status == "pass"``, and a selected node that
failed has ``status == "fail"``.

Now ask why Bob failed:

.. code-block:: python

   for statement in run.statements:
       for focus in statement.selected_foci:
           if focus.status == "fail":
               print(focus.evidence.explain())

.. code-block:: text

   All — fix every:
     CountLow along <http://example.org/email>: have 0, need 1
     All — fix every:
       CountHigh along <http://example.org/name>: 1 match(es), max 0
         value "123"^^<http://www.w3.org/2001/XMLSchema#integer>:
           Atom at "123"^^<http://www.w3.org/2001/XMLSchema#integer> via <http://example.org/name> [cuttable]

This is a tree, not a list, and its shape is the shape of the constraint. The
outer ``All`` is the conjunction of Bob's two property obligations: both
branches failed, and both must be fixed. The ``CountLow`` branch is the missing
email, stated as an arithmetic gap — zero values found, one required — rather
than as a message.

The ``CountHigh`` branch is more surprising, because nothing in ``shapes.ttl``
mentions a maximum. It is there because ``sh:datatype`` is a constraint on
*every* value of ``ex:name``, and "every value satisfies φ" is compiled as "at
most zero values satisfy ¬φ". So a universal constraint appears as a count with
``max 0``, and the "match" it is complaining about is the one value that
violates it. This is the algebra showing through; :doc:`../explanation/architecture`
explains the encoding.

``[cuttable]`` is the engine noting that this leaf rests on a concrete triple —
one that could be pointed at, or removed, to change the outcome. Leaves that
have no such finite support say so instead; :doc:`../explanation/recursion`
covers the case where that happens.

Two things are worth knowing before you build on this. ``explain()`` produces
text for humans — parse ``walk()``, ``constraint_kind``, and the structured
projections instead, all described in :doc:`../reference/evidence`. And this
evidence is *canonical*: a failed conjunction keeps the children that establish
the failure and drops passing siblings, so the tree is a proof rather than a
log. When you want the siblings too, ``focus.progress`` has them.

Ask what satisfied the passing node
-----------------------------------

Alice conforms. The interesting question is *with what* — and this is the one a
validation report cannot answer at all, because Alice does not appear in it.

Rather than walking the satisfaction tree by hand, use the projections. They
work identically on both polarities:

.. code-block:: python

   for statement in run.statements:
       for focus in statement.selected_foci:
           evidence = focus.evidence
           print(focus.status, focus.focus)
           print("   matched: ", evidence.matched_values())
           print("   support: ", evidence.supporting_triples())

.. code-block:: text

   pass <http://example.org/alice>
      matched:  ['"alice@example.org"', '"Alice"']
      support:  ['<http://example.org/alice> <http://example.org/email> "alice@example.org"',
                 '<http://example.org/alice> <http://example.org/name> "Alice"']
   fail <http://example.org/bob>
      matched:  ['"123"^^<http://www.w3.org/2001/XMLSchema#integer>']
      support:  ['<http://example.org/bob> <http://example.org/name> "123"^^<http://www.w3.org/2001/XMLSchema#integer>']

``matched_values()`` on Alice returns the two values that actually satisfied her
obligations. That is the answer you would otherwise get by writing a second
query that re-implements the shape's property paths — and which could drift out
of sync with the shape. ``supporting_triples()`` gives the triples underneath
them, in N-Triples form.

Note that Bob has matched values too. On a failing node they mean "these are the
values the constraint counted", which for his ``max 0`` datatype check is
precisely the value that offended. Which brings us to the failure-side
projections:

.. code-block:: python

   for statement in run.statements:
       for focus in statement.selected_foci:
           if focus.status != "fail":
               continue
           print("offending:", focus.evidence.offending_values())
           for gap in focus.evidence.missing_obligations():
               print(f"need {gap.missing} more: "
                     f"observed {gap.observed_count}, required {gap.required_count}")

.. code-block:: text

   offending: ['"123"^^<http://www.w3.org/2001/XMLSchema#integer>']
   need 1 more: observed 0, required 1

These are structured, not prose: ``gap.missing`` is an integer you can act on.
A repair tool, a data-entry form, or a coverage dashboard all want this rather
than the sentence "at least 1 value(s) required".

See the siblings a proof leaves out
-----------------------------------

Canonical evidence is decisive: Bob's tree contains what makes him fail and
nothing else. Sometimes you want the fuller picture — "two of these three
obligations are met" is useful to a person, and is not what a proof contains.

``focus.progress`` reports the immediate authored children and their statuses:

.. code-block:: python

   for statement in run.statements:
       for focus in statement.selected_foci:
           if focus.progress is None:
               continue
           print(focus.focus)
           for child in focus.progress.evaluated_children:
               print("   ", child.source_constraint_ref,
                     child.constraint_kind, child.status)

.. code-block:: text

   <http://example.org/alice>
       1 ConstraintKind.Cardinality pass
       4 ConstraintKind.Conjunction pass
   <http://example.org/bob>
       1 ConstraintKind.Cardinality fail
       4 ConstraintKind.Conjunction fail

Progress reports *that* each child passed or failed without materializing
*why* — that is what makes it cheap. When you need the full evidence for one of
them, ask the session directly:

.. code-block:: python

   detail = session.evidence_for(focus.focus, child.normalized_constraint_ref)
   # {"status": "pass", "evidence": {...}}

The division of labour is worth remembering: canonical evidence answers *why
did this result hold?*, progress answers *what happened to the authored
children along the way?*, and ``evidence_for`` fills in any one of them on
demand.

What you have seen
------------------

The evidence interface is the validator's own derivation, kept rather than
discarded. That is why it cannot disagree with ``validate()`` about whether a
graph conforms — there is no second implementation of SHACL involved, only a
richer return value from the same fold.

It also has a cost. Materializing evidence for every selected pair runs
2.5–5.4x the time of deciding conformance, and grows with model size. If you
only care about failures — which is most callers — there is a much cheaper
path; :doc:`../explanation/performance` has the measurements and the entry
points.

Where to go next:

- :doc:`../how-to/shape-maps` — the same bindings as a flat table, for when a
  shape is really an extraction schema.
- :doc:`../reference/evidence` — the exact data model, for building on.
- :doc:`../explanation/evidence-design` — why evidence is shaped this way, and
  what it honestly cannot explain.
- :doc:`../how-to/repair` — **experimental**: failure evidence is also the
  input to a symbolic repair layer that computes which edits would make a node
  conform. It is early and its API is expected to change.
