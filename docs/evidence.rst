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

Walking the statements is the complete view, but a driver usually asks about
one node or one shape. The run is indexed for both, and a proposed edit can be
checked against the same session:

.. code-block:: python

   run.failures_for("http://example.org/Bob")        # by focus node
   run.failures_for_shape("http://example.org/PersonShape")
   run.covered_shapes()                              # what the run addresses

   after = session.revalidate(delta)                 # the run G ⊕ ΔG would give

These are covered in `Looking up one focus`_, `Looking up one shape`_, and
`Validating a proposed edit`_.

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
       ``run.conforms``; per-focus and per-shape projections (see below)
   * - ``ConformanceRun``
     - ``conforms``, ``selected_pairs``, ``passed``, and ``failed`` — counts
       only, from the evidence-free entry points
   * - ``SelectedPair``
     - ``focus``, ``normalized_statement``, and ``source_statements``: the
       handle ``find_failures`` returns and ``explain`` takes
   * - ``StatementEvaluation``
     - ``source_statement_id``, ``normalized_statement_id``,
       ``source_constraint_id``, ``normalized_constraint_id``,
       ``constraint_kind``, ``constraint``, ``selector``, ``target``,
       ``shape_iri``, and ``selected_foci``
   * - ``FocusEvaluation``
     - ``focus``, ``status``, ``evidence``, ``satisfaction``, ``failure``, and
       optional ``progress``
   * - ``Satisfaction`` / ``Failure``
     - ``shape_iri`` plus the typed evidence tree, its projections,
       serialization helpers, and human-readable ``explain()`` output

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

Looking up one focus
~~~~~~~~~~~~~~~~~~~~

A run is grouped by statement, but a driver usually asks about a node. The run
carries a focus index, so these projections cost one lookup rather than a scan
over every statement. A focus is named by its IRI with or without angle
brackets, or by the rendered form of a blank node or literal.

``results_for(focus)``
   Every evaluation of ``focus``, one per statement that selected it, in
   statement order. A focus no statement selected returns ``[]``.

``failures_for(focus)`` / ``satisfactions_for(focus)``
   The same list restricted to one polarity, yielding ``Failure`` and
   ``Satisfaction`` objects directly.

``failure_for(focus, statement=None)`` / ``satisfaction_for(focus, statement=None)``
   The single evidence object for ``focus``, or the one under authored
   statement ``statement``. These are strict: a miss raises ``ValueError``, and
   so does an ambiguous match, naming the statements that could have been
   meant. Nothing is resolved silently.

.. code-block:: python

   for failure in run.failures_for("http://example.org/boiler-1"):
       print(failure.statement, failure.explain())

   # One statement per focus is a common shape; say so and let it fail loudly.
   failure = run.failure_for("http://example.org/boiler-1", statement=3)

Looking up one shape
~~~~~~~~~~~~~~~~~~~~

The same projections exist for the authored shape a statement heads. They read
``shape_iri``, which is the shape's IRI, or ``None`` when the shape was written
as a blank node — an anonymous shape has no name to project by, and its
evaluations are reachable only through ``statements``.

``covered_shapes()``
   The named shapes this run has statements for, in statement order without
   duplicates. A statement whose selector chose nothing still covers its shape.

``results_for_shape(iri)``, ``failures_for_shape(iri)``, ``satisfactions_for_shape(iri)``
   Every evaluation made under ``iri``, optionally restricted to one polarity,
   in statement order. Angle brackets are optional.

These distinguish two cases a single empty list would conflate. An IRI that
names *no shape in the schema* raises ``ValueError`` — a typo should not read as
a clean bill of health. An IRI that names a real shape this run has no
statements for returns ``[]``, and ``covered_shapes()`` is how you tell in
advance:

.. code-block:: python

   for shape in run.covered_shapes():
       failing = run.failures_for_shape(shape)
       print(shape, len(failing), "failing focus nodes")

   run.failures_for_shape("http://example.org/NotAShape")   # ValueError

Scoping this way projects a run you already have. To spend no work at all on
the other shapes, scope the validation itself with
``validate(shape_names=[...])``; the two agree on the shapes they share.

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

``values_for_path(path)``
   The subset of ``matched_values()`` counted along one path, read from the
   structured match records — no parsing of ``explain()`` text or re-derivation
   from supporting triples. ``path`` is the rendered form (``ex:p``,
   ``^ex:p``, ``<http://ex/a>/<http://ex/b>``) or, for a single predicate step,
   its IRI with or without angle brackets. A path the evidence never counted
   along returns ``[]``.

``missing_obligations()``
   Cardinality deficits, each describing the edge that would close it:
   ``node`` has ``observed_count`` values along ``path`` satisfying
   ``qualifier``, and needs ``required_count``. Also carries ``constraint_id``
   and ``missing``.

   ``node`` is not always the focus — a count nested inside a rejected
   candidate reports its own node — so compare it against ``focus`` when you
   mean deficits on the focus itself. ``path`` is rendered in the spelling
   ``values_for_path`` accepts, and ``qualifier`` is a structured
   ``Constraint``, so "what is missing" and "what is already there" are both
   reachable without reading ``explain()``:

   .. code-block:: python

      for obligation in failure.missing_obligations():
          if obligation.node != failure.focus:
              continue                      # a nested deficit, not the focus's
          print(
              f"add {obligation.missing} more {obligation.path} "
              f"satisfying {obligation.qualifier.definition}; "
              f"have {failure.values_for_path(obligation.path)}"
          )

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

Cheaper entry points
--------------------

``validate()`` materializes evidence for every selected pair. Three other entry
points share the same prepared snapshot and cost less, which matters when you
only need to know *why something failed* and failures are a small minority:

``validate_conformance()``
   Counts only, no evidence. ``minimum_severity`` does not apply — with no
   failure evidence there is no per-constraint severity to weigh, so any failing
   pair makes ``conforms`` false.

``find_failures()``
   The same pass, plus a :class:`SelectedPair` handle per failing pair.

``explain(pair)`` / ``explain_canonical(pair)``
   Evidence for one pair, returned as a run holding just that pair, so every
   projection works on it.

.. code-block:: python

   session = shifty.EvidenceSession(shapes, data)
   counts, failures = session.find_failures()
   print(counts.passed, counts.failed)

   for pair in failures:
       one = session.explain(pair)
       for failure in one.failures_for(pair.focus):
           print(failure.explain())

Target selection is not re-run by ``explain``: the pair is taken as already
selected, which is the point — re-deriving the selection costs what the whole
pass costs. Pairs should come from ``find_failures`` or an earlier run over the
same snapshot.

Normalized and authored counts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ConformanceRun`` counts *normalized* pairs, because deciding a merged
statement once is what makes the pass cheap. A run instead reports one focus row
per *authored* statement. When common-subexpression elimination merges
statements that state the same constraint, the run has more rows than the
conformance pass had decisions, and ``selected_pairs`` equals the distinct
``(normalized_statement_id, focus)`` pairs the run contains.

``SelectedPair`` keeps the two apart deliberately. Its ``normalized_statement``
is the statement evidence is materialized against; ``source_statements`` lists
the authored statements that normalize to it, which is why ``explain`` returns
one evaluation per authored statement rather than one per pair. Everywhere else
in this API a bare statement id is an authored one, so the field is not called
``statement``.

The catalog travels separately
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A run from ``explain`` carries no constraint catalog: it is fixed per snapshot
rather than per pair, and on a small 223P model it is 57% of a whole run's
serialized bytes. ``constraints()`` serves it once. That only affects
serialization — the ``constraint`` objects on statements and evidence are
present either way — and it is what makes an out-of-band catalog work:

.. code-block:: python

   catalog = session.constraints()          # once per snapshot
   wire = run.to_compact_json(include_catalog=False)
   assert shifty.expand_evidence(wire, catalog) == run.to_dict()

Validating a proposed edit
--------------------------

``EvidenceSession.revalidate(delta)`` returns the run ``validate()`` would
produce over ``G ⊕ ΔG`` — this session's graph with ``delta`` applied. It is
pure: the session keeps its own snapshot, so a run taken before the edit stays
valid and comparable, and the two are diffed with the ordinary projections.

.. code-block:: python

   session = shifty.EvidenceSession(shapes, data)
   before = session.validate()

   failure = before.failure_for("http://example.org/ahu-1")
   obligation = next(
       o for o in failure.missing_obligations() if o.node == failure.focus
   )
   # obligation.path and obligation.qualifier say what edge to author.
   delta = shifty.RepairDelta.from_ntriples(add=my_triples)

   after = session.revalidate(delta)
   fixed = set(before.failures_for(focus)) and not after.failures_for(focus)

Unlike ``validate()``, this cannot reuse the prepared snapshot: a patched graph
needs its own normalization, indexing, and SPARQL preparation. It still skips
file I/O, parsing, and schema lowering, so it is cheaper than building a new
session but not comparable to a repeated ``validate()``.

``infer`` re-runs SHACL-AF rules over the patched graph. It defaults to
whatever the session was built with, which keeps the before and after runs on
the same baseline — a session that never ran the rules does not start now.
Passing ``infer=False`` patches the already-inferred graph and leaves the rules
alone. That is cheaper, and sound only if the edit fires none of them:
inference only ever *adds*, so deleting a triple that supported a derived one
leaves the derivation stranded, and the edit can look like it conforms when it
does not. When ``infer`` is on, the rules re-run over the graph as it stood
before they last ran, so a deletion correctly takes its derivations with it.

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
           for origin in repair_tree.origins():
               print(origin.statement_id, origin.path, origin.kind)

``RepairTree.root_id`` is the stable address of its root operator, and
``origins(node_id)`` returns the evidence occurrences that justify any repair
node. An origin records the normalized statement id, its child-index path in
the evidence tree, constraint id, judgment node, status, and evidence kind.
Most nodes have one origin; a joint synthetic node can have several. This makes
the route from validation evidence to a repair choice inspectable rather than
depending on two walks happening to use the same positions.

The complement is equally important: satisfaction evidence explains which
facts and nested decisions currently make the shape hold. When repair crosses
a negation, this satisfaction trace supplies the deletive side of synthesis.
Both polarities therefore share traversal and projection infrastructure rather
than being unrelated report formats.

Every item returned by ``evidence.walk()`` has an exact typed
``EvidenceKind`` in ``item.evidence_kind``. Its ``status`` property is
``"pass"`` or ``"fail"`` and its string form is the existing snake-case kind:

.. code-block:: python

   for item in focus.evidence.walk():
       assert item.status == item.evidence_kind.status
       assert item.kind == str(item.evidence_kind)  # compatibility spelling

``WitnessKind`` and ``SatKind`` remain the coarser kinds of flattened summary
rows. Summary rows also expose ``evidence_kind`` so, for example, a
``SatKind.Match`` says whether it came from ``EvidenceKind.CountHeld`` or
``EvidenceKind.AllValuesHeld``.

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

``Evidence`` carries the same projections in Rust. ``matched_values_by_path``
groups the structured match records by the path each value was counted along,
and ``values_for_path`` takes the single-path slice:

.. code-block:: rust

   for (path, values) in focus.evidence.matched_values_by_path() {
       println!("{path:?}: {values:?}");
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
* Projections address an existing run and never re-evaluate. Looking a focus or
  shape up returns the same objects the statements hold, so identity and order
  are stable across calls.
* ``revalidate`` is the one evidence call that evaluates a different graph, and
  it leaves its session untouched. With ``infer`` on it re-runs the rules over
  the pre-inference graph, so derivations track deletions; with ``infer`` off
  the already-derived triples are kept as they stand.

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
