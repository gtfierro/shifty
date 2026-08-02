Validation evidence
===================

Ordinary SHACL validation answers whether a graph conforms and reports its
violations. Shifty's evidence interface also records *why* every selected focus
node passed or failed. The result is machine-readable, linked to both the
authored SHACL statement and its normalized algebra, and suitable for
explanation, extraction, debugging, and repair workflows.

The interface is deliberately statement-oriented. It preserves statements
whose targets select nothing, then partitions every selected
``(statement, focus)`` pair into exactly one evidence polarity:

.. code-block:: text

   EvidenceRun
   └── StatementEvaluation                 one per included authored statement
       ├── selected_foci = []               target selected nothing
       └── FocusEvaluation                  one per selected focus node
           ├── status = "pass"
           │   └── Satisfaction             why the constraint holds
           └── status = "fail"
               └── Failure                  why the constraint does not hold

This makes three states observably different:

* **unselected** — there is no focus row for that node;
* **selected and passing** — the row contains satisfaction evidence;
* **selected and failing** — the row contains failure evidence.

Python quick start
------------------

Construct an ``EvidenceSession`` once for an immutable shapes/data
snapshot, then call ``validate()``. Parsing, optional inference, normalization,
indexing, and SPARQL preparation are retained by the session.

.. code-block:: python

   import shifty

   shapes = """
   @prefix sh: <http://www.w3.org/ns/shacl#> .
   @prefix ex: <http://example.org/> .

   ex:PersonShape a sh:NodeShape ;
       sh:targetClass ex:Person ;
       sh:property [
           sh:path ex:name ;
           sh:minCount 1
       ] .

   # This statement remains visible with selected_foci=[].
   ex:UnusedShape a sh:NodeShape ;
       sh:targetClass ex:NeverPresent ;
       sh:nodeKind sh:IRI .
   """

   data = """
   @prefix ex: <http://example.org/> .
   ex:Alice a ex:Person ; ex:name "Alice" .
   ex:Bob a ex:Person .
   ex:Untargeted ex:name "Not a selected person" .
   """

   session = shifty.EvidenceSession(shapes, data, infer=False)
   run = session.validate()

   print(run.conforms)  # False
   for statement in run.statements:
       print(statement.source_statement_id, statement.selector)
       if not statement.selected_foci:
           print("  target selected nothing")
       for focus in statement.selected_foci:
           print(" ", focus.status, focus.focus)
           print(focus.evidence.explain())

``ex:Alice`` has a ``pass`` row, ``ex:Bob`` has a ``fail`` row, and
``ex:Untargeted`` has no row because the selector never chose it. The unused
statement is still present with an empty ``selected_foci`` list.

Inputs and options
~~~~~~~~~~~~~~~~~~

``EvidenceSession`` accepts the same graph input forms as the rest of the
Python API: Turtle text, ``bytes``, ``pathlib.Path``, an ``rdflib.Graph``, or a
sequence of those inputs.

.. code-block:: python

   session = shifty.EvidenceSession(
       shacl_graph="shapes.ttl",
       data_graph=["ontology.ttl", "model.ttl"],
       infer=True,
       graph_mode="union",
   )

   run = session.validate(
       shape_names=["http://example.org/PersonShape"],
       minimum_severity="warning",
       sort_results=True,
   )

Constructor options:

``infer``
   Run SHACL-AF rules to a fixed point before constructing evidence. It defaults
   to ``True``. The evidence describes the inferred graph snapshot.

``graph_mode``
   Controls the graph visible to paths, class hierarchy, and SPARQL. The values
   are ``"union"`` (default), ``"data"``, and ``"union-all"``; they have the
   same meaning as in :doc:`python-api/index`.

``base``
   Optional base IRI used while parsing relative IRIs.

Validation options:

``shape_names``
   Restrict top-level validation to named shapes. Statements excluded by this
   filter do not appear in the run; this differs from an included statement
   whose selector produces ``selected_foci=[]``.

``minimum_severity``
   Controls which failures make ``run.conforms`` false. Evidence is still
   materialized for selected pairs below the threshold, so a run may conform
   while containing a ``fail`` row for an ignored lower-severity constraint.

``sort_results``
   When true (the default), source statements are ordered by source statement
   id and focus nodes by lexical N-Triples form.

Inspecting a run
----------------

The main Python objects expose the following fields:

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Object
     - Important fields
   * - ``EvidenceRun``
     - ``conforms`` and ``statements``; ``bool(run)`` is equivalent to
       ``run.conforms``
   * - ``StatementEvaluation``
     - ``source_statement_id``, ``normalized_statement_id``,
       ``source_constraint_id``, ``normalized_constraint_id``,
       ``constraint_kind``, ``constraint``, ``selector``, ``target``, and
       ``selected_foci``
   * - ``FocusEvaluation``
     - ``focus``, ``status``, ``evidence``, ``satisfaction``, ``failure``, and
       optional ``progress``
   * - ``Satisfaction`` / ``Failure``
     - The typed evidence tree, its projections, serialization helpers, and
       human-readable ``explain()`` output

The polarity-specific properties make type-directed code straightforward:

.. code-block:: python

   for statement in run.statements:
       for focus in statement.selected_foci:
           if focus.status == "pass":
               assert focus.satisfaction is focus.evidence
               assert focus.failure is None
           else:
               assert focus.failure is focus.evidence
               assert focus.satisfaction is None

Source and normalized identity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Shifty retains two related identities because normalization can simplify,
compact, or deduplicate the authored algebra:

``source_statement_id`` / ``source_constraint_id``
   Identify the statement and constraint in the schema produced directly from
   the authored SHACL graph. Use these to correlate evidence with source-level
   progress and author intent.

``normalized_statement_id`` / ``normalized_constraint_id``
   Identify the executable normalized form. Multiple source statements may map
   to the same normalized identity after common-subexpression elimination.

``constraint_kind`` / ``constraint``
   Give the stable semantic category and structured normalized operator. Prefer
   ``constraint_kind`` for program logic; do not parse ``explain()`` text.

The serialized run also contains source and normalized constraint catalogs, so
child constraint ids remain resolvable without access to the live session.

Canonical evidence and evaluation progress
------------------------------------------

Canonical evidence is intentionally decisive. For example, a failed
conjunction retains the children that establish failure; it does not inflate
the failure tree with successful siblings. This is the same structured failure
witness consumed by repair synthesis. Satisfaction is its logical complement
and records the successful branches and values that establish a pass.

Sometimes a UI also needs to show work that was evaluated but was not necessary
to the final proof. ``FocusEvaluation.progress`` provides that source-oriented
view for immediate authored children:

.. code-block:: python

   for statement in run.statements:
       for focus in statement.selected_foci:
           print(focus.status, focus.focus)
           for node in focus.evidence.walk():
               print("canonical", node.status, node.kind, node.constraint_id)

           if focus.progress is not None:
               for child in focus.progress.evaluated_children:
                   print(
                       "authored child",
                       child.source_constraint_ref,
                       child.normalized_constraint_ref,
                       child.constraint_kind,
                       child.status,
                   )

Use canonical evidence to answer *why did this result hold?* Use progress to
answer *what happened to the immediate authored children while evaluating it?*

Evidence projections
--------------------

Both ``Satisfaction`` and ``Failure`` offer the same projection methods. They
preserve deterministic traversal order and deduplicate by first occurrence.

``walk()``
   Typed, deterministic pre-order traversal. Each node has ``status``, ``kind``,
   ``constraint_id``, and JSON-compatible detail.

``supporting_triples()``
   RDF triples used by positive path certificates, rendered in N-Triples form.

``path_supports()``
   Structured path certificates. A certificate is one concrete successful
   route; it is not an enumeration of every route and is not necessarily a
   deletion cut.

``matched_values()``
   Values that qualified or were checked successfully along count and
   all-values constraints.

``missing_obligations()``
   Cardinality deficits with ``constraint_id``, ``observed_count``,
   ``required_count``, and ``missing``.

``offending_values()``
   Values implicated in atomic, closed, relational, and excessive-count
   failures.

``source_constraints()``
   Source constraint ids associated with the evidence object.

For example:

.. code-block:: python

   for statement in run.statements:
       for focus in statement.selected_foci:
           evidence = focus.evidence
           print("matched", evidence.matched_values())
           print("offending", evidence.offending_values())
           for obligation in evidence.missing_obligations():
               print(
                   f"missing {obligation.missing}: "
                   f"observed {obligation.observed_count}, "
                   f"required {obligation.required_count}"
               )
           for triple in evidence.supporting_triples():
               print("support", triple)

Serialization
-------------

The complete run and each polarity-specific evidence object can be serialized
without losing tree structure or constraint identity:

.. code-block:: python

   import json

   payload = run.to_dict()
   encoded = run.to_json()
   assert payload == json.loads(encoded)

   one_evidence = run.statements[0].selected_foci[0].evidence
   evidence_payload = one_evidence.to_dict()
   evidence_json = one_evidence.to_json()

The JSON representation uses explicit status and variant tags. Treat those
tags, ids, and structured fields as the interchange format; ``explain()`` is a
human-readable rendering rather than a parsing API.

Evidence and repair
-------------------

Failure evidence is the same lossless witness used by the repair layer. A
failing focus can therefore move directly from explanation to symbolic repair:

.. code-block:: python

   for statement in run.statements:
       for focus in statement.selected_foci:
           if focus.failure is None:
               continue
           print(focus.failure.explain())
           repair_tree = focus.failure.repair_tree()
           print(repair_tree.explain())

The complement is equally important: satisfaction evidence explains which
facts and nested decisions currently make the shape hold. When repair crosses
a negation, this satisfaction trace supplies the deletive side of synthesis.
Both polarities therefore share traversal and projection infrastructure rather
than being unrelated report formats.

Rust API
--------

At the Rust layer, parse the source schema and prepare a reusable validator.
The prepared object owns normalization, stratification, indexing, and SPARQL
setup for the snapshot:

.. code-block:: rust

   use shifty_engine::{
       Evidence, PreparedEvidenceValidator, ValidationGraphMode,
       ValidationOptions,
   };

   let shapes = shifty_parse::load_turtle(shapes_bytes, None)?;
   let parsed = shifty_parse::parse_loaded(&shapes);
   let data = shifty_parse::load_turtle(data_bytes, None)?;

   let prepared = PreparedEvidenceValidator::with_graphs(
       &data.graph,
       &shapes.graph,
       &parsed.schema,
       ValidationGraphMode::Union,
   )?;
   let run = prepared.validate(&ValidationOptions::default());

   for statement in &run.statements {
       for focus in &statement.selected_foci {
           match &focus.evidence {
               Evidence::Satisfaction(trace) => {
                   println!("{} passes: {trace:?}", focus.focus);
               }
               Evidence::Failure(failure) => {
                   println!("{} fails: {failure:?}", focus.focus);
               }
           }
       }
   }

For one-shot calls, use ``validate_with_evidence``,
``validate_with_context_and_evidence``, or
``validate_graphs_with_evidence``. Variants accepting graph mode and
``ValidationOptions`` are available when the defaults are insufficient.

Semantics and guarantees
------------------------

* Evidence uses the logical validation evaluator as its oracle. It reuses
  normalized schemas, indexed datasets, and SPARQL preparation, but it is not
  yet fused with every physical-plan operator.
* Positive recursion follows Shifty's greatest-fixed-point validation
  semantics. A recursive success back-edge becomes a finite ``coinductive``
  satisfaction leaf.
* Canonical failure evidence is pruned to decisive failures. Immediate authored
  siblings remain available through ``progress``.
* Statement ordering, focus ordering, evidence traversal, and projections are
  deterministic under the default sorting option.
* A ``PathSupport`` is a positive reachability certificate. For an alternative
  path, Shifty retains the first successful syntactic alternative.

Opaque and blocked evidence
---------------------------

Validation status remains exact even when Shifty cannot construct actionable
repair provenance for an operator. These cases are explicit in the tree:

* a failing SPARQL constraint is ``opaque`` and retains its query diagnostic;
* a passing SPARQL constraint is ``blocked`` with ``opaque_sparql`` because an
  arbitrary query cannot generally be falsified by a sound data deletion;
* SHACL-AF expression failures are currently opaque, while passing expressions
  are blocked where expression-level repair provenance is unavailable;
* passing closed and relational constraints are blocked only in the deletive
  repair direction—their validation result is still valid;
* coinductive satisfaction records a greatest-fixed-point back-edge, not a
  finite set of supporting triples.

Complete Brick point-list example
---------------------------------

The repository includes a larger demonstration adapted from BuildingMOTIF's
ZonePAC point-list shapes and Shifty's Brick benchmark model:

.. code-block:: bash

   cd python
   uv run maturin develop
   uv run python examples/evidence_point_list.py

See ``python/examples/evidence_point_list.py`` for an end-to-end example with
passing, failing, empty-selection, and unselected cases; progress inspection;
evidence projections; and JSON round-tripping.
