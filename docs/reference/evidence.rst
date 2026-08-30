Evidence reference
==================

The evidence interface records why every selected focus node passed or failed,
linked to both the authored SHACL statement and its normalized algebra.

.. list-table::
   :widths: 20 80

   * - Frontends
     - Python ``EvidenceSession``, Rust ``PreparedEvidenceValidator``, and C++
       ``EvidenceSession``
   * - Stability
     - Stable
   * - Related
     - :doc:`../how-to/explain-failures`,
       :doc:`../explanation/evidence-design`, :doc:`shape-maps`

:doc:`../how-to/explain-failures` shows how to use it;
:doc:`../explanation/evidence-design` explains why it is shaped this way.

Structure of a run
------------------

.. code-block:: text

   EvidenceRun
   └── StatementEvaluation                 one per included authored statement
       ├── selected_foci = []               target selected nothing
       └── FocusEvaluation                  one per selected focus node
           ├── status = "pass"
           │   └── Satisfaction             why the constraint holds
           └── status = "fail"
               └── Failure                  why the constraint does not hold

The interface is statement-oriented. It preserves statements whose targets
select nothing, then partitions every selected ``(statement, focus)`` pair into
exactly one polarity. Three states are therefore observably different:

- **unselected** — no focus row exists for that node;
- **selected and passing** — the row carries satisfaction evidence;
- **selected and failing** — the row carries failure evidence.

``EvidenceSession``
-------------------

.. code-block:: python

   shifty.EvidenceSession(shacl_graph, data_graph=None, *,
                          infer=True, graph_mode="union", base=None)

Parsing, lowering, optional inference, dataset indexing, and SPARQL preparation
happen in the constructor and are retained, so reuse the session rather than
rebuilding it.

.. list-table::
   :widths: 22 78
   :header-rows: 1

   * - Argument
     - Meaning
   * - ``infer``
     - Run SHACL-AF rules to a fixed point first. Default ``True``. The
       evidence then describes the inferred snapshot.
   * - ``graph_mode``
     - ``"union"`` (default), ``"data"``, or ``"union-all"`` — the graph
       visible to paths, class hierarchy, and SPARQL.
   * - ``base``
     - Base IRI used while parsing relative IRIs.

Methods
~~~~~~~

.. code-block:: python

   session.validate(*, shape_names=None, minimum_severity="info",
                    sort_results=True) -> EvidenceRun

``shape_names``
   Restrict top-level validation to named shapes. Statements excluded by this
   filter do not appear in the run at all — which is different from an included
   statement whose selector produced ``selected_foci=[]``.

``minimum_severity``
   Which failures make ``run.conforms`` false. Evidence is still materialized
   for pairs below the threshold, so a run can conform while containing a
   ``fail`` row.

``sort_results``
   When true (the default), statements are ordered by source statement id and
   foci by lexical N-Triples form.

.. code-block:: python

   session.evidence_for(focus, constraint_id) -> EvidenceNode

Evidence for one focus against one **normalized** constraint id — any
constraint in the run's catalog, not only a statement's top shape. The returned
node exposes ``status``, ``evidence_kind``, ``kind``, ``constraint_id``,
``to_dict()``, and ``to_json()``. No target selection is involved, so a focus
that no statement selects still yields well-defined evidence. This is the
drill-down for children a canonical failure elided.

Objects and fields
------------------

.. list-table::
   :widths: 26 74
   :header-rows: 1

   * - Object
     - Fields
   * - ``EvidenceRun``
     - ``conforms``, ``statements``; ``bool(run)`` equals ``run.conforms``;
       ``to_dict()``, ``to_json()``, ``to_compact_json(include_catalog)``,
       ``to_compact_dict(include_catalog)``
   * - ``StatementEvaluation``
     - ``source_statement_id``, ``normalized_statement_id``,
       ``source_constraint_id``, ``normalized_constraint_id``,
       ``constraint_kind``, ``constraint``, ``selector``, ``shape_name``,
       ``target``, ``selected_foci``
   * - ``FocusEvaluation``
     - ``focus``, ``status``, ``evidence``, ``satisfaction``, ``failure``,
       ``progress``
   * - ``Satisfaction`` / ``Failure``
     - the evidence tree, its projections, serialization, and ``explain()``

The polarity-specific properties make type-directed code straightforward:

.. code-block:: python

   if focus.status == "pass":
       assert focus.satisfaction is focus.evidence
       assert focus.failure is None
   else:
       assert focus.failure is focus.evidence
       assert focus.satisfaction is None

Source and normalized identity
------------------------------

Normalization simplifies, compacts, and deduplicates the authored algebra, so
two identities are kept.

``source_statement_id`` / ``source_constraint_id``
   Identify the statement and constraint in the schema produced directly from
   the authored SHACL graph. Use these to correlate evidence with author intent
   and with source-level progress.

``normalized_statement_id`` / ``normalized_constraint_id``
   Identify the executable normalized form. Several source statements may share
   one normalized identity after common-subexpression elimination.

``constraint_kind`` / ``constraint``
   The stable semantic category and the structured normalized operator. Branch
   on ``constraint_kind``; do not parse ``explain()``.

A serialized run carries both source and normalized constraint catalogs, so
child constraint ids stay resolvable without the live session.

Canonical evidence and progress
-------------------------------

Canonical evidence is decisive: a failed conjunction retains the children that
establish the failure and does not carry successful siblings. It is a proof, and
it is the same structured witness the repair layer consumes. Satisfaction is its
logical complement, recording the branches and values that establish a pass.

``FocusEvaluation.progress`` is the source-oriented view for the *immediate*
authored children, including ones the canonical tree elided:

.. code-block:: python

   for node in focus.evidence.walk():
       print("canonical", node.status, node.kind, node.constraint_id)

   if focus.progress is not None:
       for child in focus.progress.evaluated_children:
           print("authored child",
                 child.source_constraint_ref,
                 child.normalized_constraint_ref,
                 child.constraint_kind,
                 child.status)

Canonical evidence answers *why did this result hold?*; progress answers *what
happened to the immediate authored children along the way?*

Projections
-----------

``Satisfaction`` and ``Failure`` share these. They traverse deterministically
and deduplicate by first occurrence.

``walk()``
   Typed pre-order traversal. Each node has ``status``, ``kind``,
   ``constraint_id``, and JSON-compatible detail.

``supporting_triples()``
   RDF triples used by positive path certificates, in N-Triples form.

``path_supports()``
   Structured path certificates. A certificate is *one* concrete successful
   route — not an enumeration of every route, and not necessarily a deletion
   cut.

``matched_values()``
   Values that qualified or were checked successfully along count and
   all-values constraints.

``missing_obligations()``
   Cardinality deficits, each with ``constraint_id``, ``observed_count``,
   ``required_count``, and ``missing``.

``offending_values()``
   Values implicated in atomic, closed, relational, and excessive-count
   failures.

``source_constraints()``
   Source constraint ids associated with this evidence object.

``summary()``
   A flat list of ``WitnessAtom`` (on ``Failure``) or ``SatAtom`` (on
   ``Satisfaction``) — the leaf facts, without the tree.

``explain()``
   Human-readable rendering. Not a parsing API.

Serialization
-------------

.. code-block:: python

   payload = run.to_dict()
   encoded = run.to_json()
   assert payload == json.loads(encoded)

   evidence = run.statements[0].selected_foci[0].evidence
   evidence.to_dict()
   evidence.to_json()

The JSON uses explicit status and variant tags. Treat the tags, ids, and
structured fields as the interchange format.

Compact encoding
~~~~~~~~~~~~~~~~

.. code-block:: python

   wire = run.to_compact_json(include_catalog=False)
   catalog = run.to_dict()["constraints"]
   restored = shifty.expand_evidence(wire, catalog)
   assert restored == run.to_dict()

Each distinct evidence node and RDF term is stored once and referenced by index.
Lossless, and ``shifty.expand_evidence(compact, catalog=None, *, as_dict=True)``
restores the run exactly. The catalog is fixed per snapshot regardless of how
many findings there are, so omitting it matters most on small runs — see
:doc:`../explanation/performance` for measured sizes.

Rust API
--------

.. code-block:: rust

   use shifty_engine::{
       Evidence, PreparedEvidenceValidator, ValidationGraphMode, ValidationOptions,
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
               Evidence::Satisfaction(trace) => println!("{} passes", focus.focus),
               Evidence::Failure(failure) => println!("{} fails", focus.focus),
           }
       }
   }

``PreparedEvidenceValidator`` also has partial entry points. Conformance-only
scans take ``ConformanceOptions`` rather than the broader ``ValidationOptions``
because severity filtering requires failure evidence:

.. list-table::
   :widths: 34 66
   :header-rows: 1

   * - Method
     - Cost
   * - ``validate_conformance(&scan_options)``
     - Counts only, no evidence. The baseline.
   * - ``find_failures(&scan_options)``
     - Counts plus which pairs failed.
   * - ``explain(&pair)``
     - Evidence for one already-selected pair.
   * - ``explain_constraint(focus, constraint)``
     - Evidence for one focus against one constraint id.
   * - ``constraints()``
     - The constraint catalog, fixed per snapshot.
   * - ``validate(&options)``
     - Evidence for every selected pair.

For one-shot calls there are ``validate_with_evidence``,
``validate_with_context_and_evidence``, and ``validate_graphs_with_evidence``,
with variants taking a graph mode and ``ValidationOptions``.

Guarantees and limits
---------------------

- Evidence uses the logical validation evaluator as its oracle. It reuses
  normalized schemas, indexed datasets, and SPARQL preparation, but is not yet
  fused with every physical-plan operator.
- Positive recursion follows the greatest-fixed-point validation semantics. A
  recursive success back-edge becomes a finite ``coinductive`` satisfaction
  leaf.
- Canonical failure evidence is pruned to decisive failures; immediate authored
  siblings remain available through ``progress``.
- Statement ordering, focus ordering, traversal, and projections are
  deterministic under the default sorting option.
- A ``PathSupport`` is a positive reachability certificate. For an alternative
  path, Shifty retains the first successful syntactic alternative.

Opaque and blocked evidence
---------------------------

Validation status stays exact even where Shifty cannot construct actionable
provenance. These cases are explicit in the tree rather than silent:

- a failing SPARQL constraint is ``opaque``, carrying its query diagnostic;
- a passing SPARQL constraint is ``blocked`` with ``opaque_sparql``, because an
  arbitrary query cannot generally be falsified by a sound data deletion;
- SHACL-AF expression failures are opaque, and passing expressions are blocked
  where expression-level provenance is unavailable;
- passing closed and relational constraints are blocked only in the deletive
  repair direction — their validation result is still valid;
- coinductive satisfaction records a greatest-fixed-point back-edge, not a
  finite set of supporting triples.
