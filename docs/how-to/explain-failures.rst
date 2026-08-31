Explain why a node passed or failed
===================================

A validation report tells you what to fix. The evidence interface tells you why
the engine reached that conclusion, for failures *and* passes, in a structure
you can walk rather than a string you have to parse.

:doc:`../tutorials/explaining-a-failure` introduces this from scratch. This page
is the recipe collection; :doc:`../reference/evidence` is the data model.

Get evidence for a snapshot
---------------------------

Build a session once, then validate. The session owns the parsed graphs, the
inference result, the normalized schema, the dataset index, and the prepared
SPARQL — all the fixed setup cost — so reusing it across calls is much cheaper
than rebuilding it.

.. code-block:: python

   session = shifty.EvidenceSession(shapes, data, infer=False)
   run = session.validate()

   for statement in run.statements:
       for focus in statement.selected_foci:
           print(focus.status, focus.focus)

``EvidenceSession`` accepts the same graph inputs as everything else — Turtle
text, ``bytes``, a ``pathlib.Path``, an ``rdflib.Graph``, or a list of those to
be merged:

``rdflib.Graph`` inputs retain their namespace bindings when converted to a
shapes graph, so ``sh:sparql`` constraints and SHACL-AF SPARQL rules may use
the prefixes declared on the graph. An unresolved prefix is an invalid shapes
graph error; it never silently removes a constraint.

For a string input, an existing path is read from disk; a directory raises
``IsADirectoryError`` and a missing filename ending in a recognized RDF suffix
such as ``.ttl`` or ``.nt`` raises ``FileNotFoundError``. Long or multiline
strings are treated directly as Turtle and are never probed as filesystem
paths. This policy applies to every member of a list or tuple as well.

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
   )

Distinguish passing and unselected nodes
----------------------------------------

Validation reports do not distinguish a passing node from an unselected node.
Evidence runs retain statements whose targets select nothing:

.. literalinclude:: ../examples/evidence-selection.py
   :language: python
   :start-after: # [example-start]
   :end-before: # [example-end]

For a shapes graph with ``Person`` and ``Equipment`` target statements, where
the data contains Alice and Bob but no equipment, the output is:

.. program-output:: python examples/evidence-selection.py
   :cwd: ..

Evidence distinguishes three states. No focus row means the node was not
selected. A row with ``status == "pass"`` means it was checked and held. A row
with ``status == "fail"`` means it was checked and did not.

The ``Equipment`` statement is present with an empty ``selected_foci`` list. If
``shape_names`` excluded that statement, its selector block would not appear in
the output at all.

Extract values and supporting triples
--------------------------------------

Rather than walking the tree yourself, use the projections — they are the same
on ``Satisfaction`` and ``Failure``, traverse deterministically, and deduplicate
by first occurrence:

.. code-block:: python

   for statement in run.statements:
       for focus in statement.selected_foci:
           evidence = focus.evidence

           evidence.matched_values()        # values that qualified
           evidence.offending_values()      # values implicated in a failure
           evidence.supporting_triples()    # N-Triples backing a positive path
           evidence.missing_obligations()   # cardinality gaps

           for gap in evidence.missing_obligations():
               print(f"need {gap.missing} more: have {gap.observed_count}, "
                     f"want {gap.required_count}")

For anything more structured, ``walk()`` is a typed pre-order traversal:

.. code-block:: python

   for node in evidence.walk():
       print(node.status, node.kind, node.constraint_id)

Branch on ``kind`` and ``constraint_kind``. ``explain()`` renders the tree for a
human to read and its wording is not a stable interface.

See the passing siblings of a failure
-------------------------------------

Canonical failure evidence is a proof, not a log: a failed conjunction keeps the
children that establish the failure and drops the ones that held. That is what
you want for repair and usually not what you want for a UI, so the authored
children are available separately:

.. code-block:: python

   if focus.progress is not None:
       for child in focus.progress.evaluated_children:
           print(child.source_constraint_ref, child.constraint_kind, child.status)

``progress`` covers the *immediate* authored children only, and reports their
status without materializing why. To get the full evidence for one of those
elided passes, ask the session for it directly:

.. code-block:: python

   detail = session.evidence_for(focus.focus, child.normalized_constraint_ref)
   print(detail.status, detail.evidence_kind)

``evidence_for`` takes the pair as given — no target selection happens — so it
also works for a focus node no statement selects.

Explain failures on demand
--------------------------

Materializing evidence for every selected pair is the expensive case, and
failures are usually a small minority. If you only care about failures, do not
call the full ``validate``: find the failing pairs first and explain each one.

That split is available today on the Rust ``PreparedEvidenceValidator``:

.. code-block:: rust

   let scan = ConformanceOptions::default();
   let conformance = prepared.validate_conformance(&scan);      // counts only
   let (_, failures) = prepared.find_failures(&scan);           // which pairs failed
   for pair in &failures {
       let evidence = prepared.explain(pair);                   // one pair
   }
   let catalog = prepared.constraints();                        // once per snapshot

On the Brick corpus this costs 3–34% over deciding conformance, against
2.5–5.4x for explaining everything — see :doc:`../explanation/performance` for
the measurements and the reasoning. ``explain`` returns exactly what ``validate``
would have produced for that pair. It does not re-run target selection, so
pairs must come from ``find_failures`` or an earlier run.

The Python ``EvidenceSession`` exposes the same workflow with
``find_failures(shape_names=...)`` and ``explain(pair)``.

Serialize a run
---------------

.. code-block:: python

   payload = run.to_dict()
   encoded = run.to_json()

The JSON uses explicit status and variant tags, and includes catalogs of both
source and normalized constraints so child ids stay resolvable without the live
session.

Runs get large — a single mid-size Brick model can reach tens of megabytes —
because the same RDF terms and evidence subtrees recur constantly. The compact
encoding stores each distinct term and node once and refers to it by index:

.. code-block:: python

   wire = run.to_compact_json(include_catalog=False)
   catalog = run.to_dict()["constraints"]

   restored = shifty.expand_evidence(wire, catalog)
   assert restored == run.to_dict()

It is lossless and round-trips exactly. Drop the catalog when the receiver
already has the schema — it is fixed per snapshot regardless of how many
findings there are, and on a small run it can be the majority of the bytes.

See also
--------

- :doc:`../reference/evidence` — objects, fields, and projections.
- :doc:`../explanation/evidence-design` — why the interface is
  statement-oriented, and which constructs it cannot explain.
- ``python/examples/evidence_point_list.py`` — a larger worked example over
  BuildingMOTIF point-list shapes, with passing, failing, empty-selection and
  unselected cases.
- :doc:`repair` — **experimental**: failure evidence is also the witness the
  symbolic repair layer consumes, so ``focus.failure.repair_tree()`` is one
  call away. Early, and expected to change.
