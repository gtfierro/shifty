Explaining, and repairing, a failure
====================================

:doc:`first-validation` ended with a report saying Bob was wrong. This tutorial
asks the engine three progressively harder questions about the same graph:

1. Why did Bob fail — not which constraint, but the whole derivation?
2. What is the complete set of edits that would make him pass?
3. Can the engine apply one and confirm it worked?

By the end you will have watched a repair loop take a failing graph, propose a
patch, check that the patch does not break anything else, and re-validate to a
conforming graph.

Set up
------

Restore the failing version of ``data.ttl`` from the previous tutorial, so
there is something to explain:

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

``[cuttable]`` is the engine noting that this leaf is supported by a concrete
triple it could delete. That is what makes the next section possible.

Two things are worth knowing before you build on this. ``explain()`` produces
text for humans — parse ``walk()``, ``constraint_kind``, and the structured
projections instead, all described in :doc:`../reference/evidence`. And this
evidence is *canonical*: a failed conjunction keeps the children that establish
the failure and drops passing siblings, so the tree is a proof rather than a
log. When you want the siblings too, ``focus.progress`` has them.

From evidence to a repair
-------------------------

The evidence tree says which facts are responsible for the failure. Inverting
it — asking which edits would remove the failure — is a mechanical
transformation over the same structure, and that is what the repair layer does.

The CLI shows the result directly:

.. code-block:: bash

   shifty repair --shapes shapes.ttl --data data.ttl

.. code-block:: text

   <http://example.org/bob>  [target: class(<http://example.org/Person>)]
     All — do all:
       Edits:
         del <http://example.org/bob> <http://example.org/name> "123"^^<http://www.w3.org/2001/XMLSchema#integer>
         add <http://example.org/bob> <http://example.org/name> ?0
         ?0 : typed value
       Repeat [1..∞]:
         Edits:
           add <http://example.org/bob> <http://example.org/email> ?1
           ?1 : any node

This is a *repair template*, and it mirrors the evidence tree branch for
branch. Two features of it matter.

``?0`` and ``?1`` are **holes** — typed placeholders. The engine will not
invent Bob's email address for you, because it has no way to know it; the hole
records what a legal value would have to look like (``?0`` must be a value of
the right datatype, ``?1`` may be any node) and leaves the choice to you.

``Repeat [1..∞]`` is a **variadic block**. The email constraint has a lower
bound and no upper bound, so the template says "one or more instances of this
block", with the count left open. A ``minCount 3`` would produce ``[3..∞]``.

Nothing here is a decision. The template describes the whole space of repairs;
picking a point in it is the caller's job. That separation is deliberate and is
argued in :doc:`../explanation/repair-design`.

Driving the loop
----------------

Now do it from Python, making the choices yourself. The instantiation loop
below is the standard shape, and it is iterative for a reason worth
understanding.

.. code-block:: python

   session = shifty.RepairSession(shapes, data, infer=False)
   failure = session.witnesses()[0]
   tree = failure.repair_tree()

   # Your policy for filling a hole. A real driver would consult a
   # database, a user, or a model; this one hardcodes two answers.
   VALUES = {
       "any node": '"bob@example.org"',
       "datatype(xsd:string)": '"Bob"',
   }

   plan = shifty.RepairPlan()
   instance = tree.instantiate(plan)
   while not instance.is_complete:
       for node_id in instance.open_choices:
           plan.count(node_id, 1)          # one instance of each Repeat
       for hole in instance.open_holes:
           plan.bind(hole.id, VALUES[hole.constraint])
       instance = tree.instantiate(plan)

   print("add:   ", instance.delta.add)
   print("delete:", instance.delta.delete)

.. code-block:: text

   add:    [('<http://example.org/bob>', '<http://example.org/email>', '"bob@example.org"'),
            ('<http://example.org/bob>', '<http://example.org/name>', '"Bob"')]
   delete: [('<http://example.org/bob>', '<http://example.org/name>', '"123"^^<http://www.w3.org/2001/XMLSchema#integer>')]

The loop repeats because choosing a count *creates holes*. Before you say how
many emails to add, the ``Repeat`` body is a template with one hole in it;
after you say "one", that body is stamped out once and its hole becomes a
concrete hole with its own id, which ``instantiate`` reports as newly open. Fill
counts first, then bind what appears, and re-instantiate until
``is_complete``. Binding a hole you saw before the count was fixed silently
binds the wrong thing.

``instantiate`` is a pure fold of your plan over the template: it validates
nothing and applies nothing.

Checking the patch before applying it
-------------------------------------

A repair that fixes one node by breaking another is not a repair. So the delta
goes through a gate, which re-validates the whole graph and diffs the
violations:

.. code-block:: python

   outcome = session.gate(instance.delta)
   print("fixed:", len(outcome.fixed))
   print("introduced:", len(outcome.introduced))
   print("remaining:", len(outcome.remaining))
   print("is_progress:", outcome.is_progress)

.. code-block:: text

   fixed: 1
   introduced: 0
   remaining: 0
   is_progress: True

``introduced`` is the one that matters: a delta is **sound** exactly when it is
empty. Progress is soundness plus having fixed something. The gate returns this
verdict and does nothing with it — accepting a repair is another decision the
library leaves to you.

Accept it, and re-witness from the patched graph:

.. code-block:: python

   session = session.advance(instance.delta)
   print("remaining failures:", len(session.witnesses()))

   repaired = session.to_graph()      # an rdflib.Graph
   print(repaired.serialize(format="turtle"))

.. code-block:: text

   remaining failures: 0

``advance`` returns a *new* session over ``G ⊕ ΔG`` with the same compiled
schema, so the shapes are not re-parsed and inference is not re-run. Wrapping
the whole thing in ``while session.witnesses():`` is the fixpoint driver — and
because each iteration is gated, it either converges or tells you which failure
it could not make progress on.

The CLI has that driver built in, if you want the result rather than the
control:

.. code-block:: bash

   shifty repair --shapes shapes.ttl --data data.ttl --apply

It writes the repaired graph as N-Triples on stdout. It picks holes by
enumeration over terms already in the graph, which is fine for a demonstration
and is rarely what you want in production — that is precisely the choice the
Python API hands back to you.

What you have seen
------------------

Three views of one computation. The evidence tree is the derivation the
validator built; the repair template is that derivation inverted; the gate is
the validator run again over a proposed edit. None of them re-implements SHACL,
which is why they cannot disagree with each other about whether a graph
conforms.

Where to go next:

- :doc:`../how-to/shape-maps` — when the shape is really an extraction schema
  and you want the *values* a conforming node bound, not a pass/fail.
- :doc:`../reference/evidence` and :doc:`../reference/repair` — the exact data
  model, for building on rather than reading.
- :doc:`../explanation/evidence-design` — why evidence is shaped this way, and
  what it is honestly not able to explain.
- :doc:`../explanation/performance` — what keeping the derivation costs, in
  measured numbers, and which entry point to use when you only care about
  failures.
