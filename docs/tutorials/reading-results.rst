Reading validation results in code
==================================

This tutorial uses the structured result interface to group failures, route
them by severity, and render application-specific explanations.

It takes about fifteen minutes. Use the ``shapes.ttl`` and the *failing*
``data.ttl`` from the first tutorial:

.. code-block:: turtle

   @prefix ex: <http://example.org/> .

   ex:alice a ex:Person ; ex:name "Alice" ; ex:email "alice@example.org" .
   ex:bob   a ex:Person ; ex:name 123 .

Result formats
--------------

Shifty can hand you a validation result in three shapes.

``validate()`` returns a **W3C report graph** — an ``rdflib.Graph`` containing
``sh:ValidationResult`` nodes. Use it when something downstream expects
standard SHACL output, or when you want to store the result as RDF alongside
the data. The cost is that reading a finding means querying a graph.

``validate()`` also returns **rendered text**, which is for a human reading a
terminal and nothing else.

``validate_algebra()`` returns **structured objects**. Use it when your program
is the consumer. There is no graph to query and no text to parse, and — the
part that matters most — the constraint that failed is available as a stable
enum rather than as English.

This tutorial uses the third.

Walk the violations
-------------------

.. literalinclude:: ../examples/structured-results.py
   :language: python

.. program-output:: python examples/structured-results.py
   :cwd: ..

The result has two levels.

A **violation** is one failing focus node under one statement. A **reason** is
one thing that went wrong there. Bob is a single violation with two reasons.
If you write ``len(result.violations)`` and call it "number of problems", you
will under-count; if you flatten reasons and call it "number of bad nodes", you
will over-count. Which one you want depends on whether you are counting things
to fix or nodes to fix them on.

``reason.value`` is the *offending value* when there is one —
the integer ``123`` for the datatype failure. For the missing email there is no
offending value, because the problem is an absence, so the field falls back to
the focus node.

Branch on constraint kind
-------------------------

``reason.message`` is generated prose. It is good for display and a bad thing
to make decisions from: it is not a stable interface, and matching on
substrings of it breaks silently the next time the wording improves.

``reason.constraint_kind`` is the stable form:

.. code-block:: python

   for violation in result.violations:
       for reason in violation.reasons:
           if reason.constraint_kind == shifty.ConstraintKind.Cardinality:
               print("missing or excess values:", reason.path)
           elif reason.constraint_kind == shifty.ConstraintKind.ValueType:
               print("wrong type:", reason.value, "at", reason.path)
           elif reason.constraint_kind == shifty.ConstraintKind.Sparql:
               print("SPARQL constraint:", reason.sparql_diagnostic)

.. code-block:: text

   missing or excess values: <http://example.org/email>
   wrong type: "123"^^<http://www.w3.org/2001/XMLSchema#integer> at <http://example.org/name>

The full set is ``Cardinality``, ``ValueType``, ``ClassMembership``,
``NodeKind``, ``Constant``, ``Closed``, ``Conjunction``, ``Disjunction``,
``Negation``, ``Equals``, ``Disjoint``, ``LessThan``, ``LessThanOrEquals``,
``UniqueLang``, ``Expression``, ``Sparql``, ``Top``, and ``Unknown``.

Notice these are algebra operators, not SHACL vocabulary terms. ``sh:minCount``,
``sh:maxCount``, and ``sh:qualifiedMinCount`` all arrive as ``Cardinality``,
because the compiler lowers all of them to the same counting operator. That is
usually what you want — one branch handles "the number of values is wrong" —
but it does mean ``constraint_kind`` will not tell you which SHACL keyword the
author wrote. If you need that, read on.

Use the author's own message
----------------------------

If the shape author wrote a ``sh:message``, it is almost certainly better than
anything the engine generates, because it can say what the constraint means in
the domain. Add one to ``shapes.ttl``:

.. code-block:: turtle

   ex:PersonShape a sh:NodeShape ;
       sh:targetClass ex:Person ;
       sh:property [
           sh:path ex:name ;
           sh:minCount 1 ;
           sh:datatype xsd:string ;
           sh:message "every person needs a string name" ;
       ] ;
       sh:property [
           sh:path ex:email ;
           sh:minCount 1 ;
           sh:severity sh:Warning ;
           sh:message "contact email is recommended" ;
       ] .

Then prefer it, falling back to the engine's:

.. code-block:: python

   for violation in result.violations:
       for reason in violation.reasons:
           print(reason.severity, "|", reason.author_message or reason.message)

.. code-block:: text

   Warning | contact email is recommended
   Violation | every person needs a string name

``author_message`` is ``None`` when the shape declares no ``sh:message``, which
is why the fallback matters. This two-line pattern is most of what a good
error-reporting layer needs.

Reason severity
---------------

The email obligation above is a ``sh:Warning`` while the name obligation is a
``sh:Violation``, and Bob has both. So severity lives on the reason.
``violation.severity`` reports the most severe reason under that node — useful
for sorting, misleading if you assume every reason shares it.

Severity also decides what counts as failure. Give Bob a valid name so the
only remaining problem is the warning-level missing email:

.. code-block:: python

   data_name_ok = """
   @prefix ex: <http://example.org/> .
   ex:alice a ex:Person ; ex:name "Alice" ; ex:email "alice@example.org" .
   ex:bob   a ex:Person ; ex:name "Bob" .
   """

   for level in ("info", "violation"):
       result = shifty.validate_algebra(data_name_ok, shapes, minimum_severity=level)
       print(level, "→ conforms:", result.conforms,
             "| findings:", len(result.violations))

.. code-block:: text

   info → conforms: False | findings: 1
   violation → conforms: True | findings: 1

The finding does not disappear at the higher threshold. ``minimum_severity``
changes *only* whether ``conforms`` flips to false. That separation is
deliberate — a warning you have decided not to fail the build on is still
something you want to log — but it means ``conforms`` and "the violations list
is empty" are different questions, and code that treats them as one will
quietly ignore warnings.

Group findings the way your consumer needs
------------------------------------------

The result is organised by focus node. Reporting is often better organised by
constraint — "these 40 assets are all missing an email" reads far better than
40 near-identical rows:

.. code-block:: python

   import collections

   by_problem = collections.defaultdict(list)

   result = shifty.validate_algebra(data, shapes)
   for violation in result.violations:
       for reason in violation.reasons:
           key = (reason.constraint_kind, reason.path)
           by_problem[key].append(violation.focus_node)

   for (kind, path), nodes in sorted(by_problem.items(), key=lambda kv: -len(kv[1])):
       print(f"{len(nodes):4d}  {kind} on {path}")
       for node in nodes[:3]:
           print("        ", node)

.. code-block:: text

      1  ConstraintKind.Cardinality on <http://example.org/email>
            <http://example.org/bob>
      1  ConstraintKind.ValueType on <http://example.org/name>
            <http://example.org/bob>

On a two-node example this is pointless. On a corpus of thousands it is the
difference between a report someone reads and a report someone closes.

Trace a finding back to the compiled constraint
-----------------------------------------------

Every reason carries the algebra node that produced it, which is how you find
out what the engine actually checked:

.. code-block:: python

   for violation in result.violations:
       print("statement", violation.statement_id,
             "top-level constraint", violation.constraint_id)
       for reason in violation.reasons:
           print("   nested constraint", reason.constraint_id)
           print("   render:    ", reason.constraint.render)
           print("   definition:", reason.constraint.definition)

.. code-block:: text

   statement 0 top-level constraint 11
      nested constraint 2
      render:     ∃[1..] <http://example.org/email> . ⊤
      definition: ∃[1..] <http://example.org/email> . any node
      nested constraint 4
      render:     test(datatype(xsd:string))
      definition: test(datatype(xsd:string))

Two ids, and they are different on purpose. ``violation.constraint_id`` (11) is
the top-level shape the statement targets. ``reason.constraint_id`` (2 and 4)
is the specific nested node that failed inside it — they differ whenever the
shape is a conjunction, disjunction, or other composite, which is nearly
always.

``render`` is the constraint in the engine's notation and ``definition`` is the
same thing with sub-shapes expanded. Both are what ``shifty inspect --stage
algebra`` prints (see :doc:`../how-to/inspect-pipeline`), so a finding can be
traced to a specific node of the compiled schema.

What this does not tell you
---------------------------

You now have every failure, in a form you can branch on and group. Two
questions remain unanswerable from here, and both come from the same
structural fact — a validation result is a list of failures:

- **Which nodes passed?** Alice is nowhere in this output. A conforming node
  and a node the shape never selected are equally absent.
- **Why exactly did this fail?** You have the failing constraint, but not the
  derivation: which branch of a disjunction was tried, which values were
  counted, which triples supported the path to the offending value.

:doc:`explaining-a-failure` answers both.
