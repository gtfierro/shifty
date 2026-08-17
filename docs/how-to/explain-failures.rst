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

Tell "passed" apart from "never checked"
----------------------------------------

This is the question a validation report cannot answer, and the reason the
evidence run keeps statements whose target selected nothing:

.. code-block:: python

   for statement in run.statements:
       if not statement.selected_foci:
           print("selected nothing:", statement.selector)
           continue
       for focus in statement.selected_foci:
           print(focus.status, focus.focus)

Three states, all observable: no focus row at all means the node was not
selected; a row with ``status == "pass"`` means it was checked and held; a row
with ``status == "fail"`` means it was checked and did not.

Note the difference between a statement that selected nothing and one excluded
by ``shape_names``. The first appears in the run with an empty
``selected_foci``; the second does not appear at all.

Pull the facts out of an evidence tree
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
   # {"status": "pass", "evidence": {...}}

``evidence_for`` takes the pair as given — no target selection happens — so it
also works for a focus node no statement selects.

Explain only the failures, cheaply
----------------------------------

Materializing evidence for every selected pair is the expensive case, and
failures are usually a small minority. If you only care about failures, do not
call the full ``validate``: find the failing pairs first and explain each one.

That split is available today on the Rust ``PreparedEvidenceValidator``:

.. code-block:: rust

   let conformance = prepared.validate_conformance(&options);   // counts only
   let failures = prepared.find_failures(&options);             // which pairs failed
   for pair in &failures.pairs {
       let evidence = prepared.explain(pair);                   // one pair
   }
   let catalog = prepared.constraints();                        // once per snapshot

On the Brick corpus this costs 3–34% over deciding conformance, against
2.5–5.4x for explaining everything — see :doc:`../explanation/performance` for
the measurements and the reasoning. ``explain`` returns exactly what ``validate``
would have produced for that pair. It does not re-run target selection, so
pairs must come from ``find_failures`` or an earlier run.

The Python bindings currently expose the whole-run ``validate`` only; use
``find_failures``/``explain`` from Rust when the cost matters.

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
