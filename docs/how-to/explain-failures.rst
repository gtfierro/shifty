Explain why a node passed or failed
===================================

The evidence interface records derivations for passing and failing nodes.
You can traverse them to inspect the constraints, values, and supporting
triples behind a result.

Use the shapes and failing data from :doc:`../tutorials/first-validation`
to try the examples. :doc:`../reference/evidence` describes the data model.

Get evidence for a snapshot
---------------------------

Build a session once, then validate. The session retains the prepared shapes
and data snapshot for reuse across queries.

.. code-block:: python

   import shifty

   shapes = "shapes.ttl"
   data = "data.ttl"
   session = shifty.EvidenceSession(shapes, data, infer=False)
   run = session.validate()

   for statement in run.statements:
       for focus in statement.selected_foci:
           print(focus.status, focus.focus)

Alice has status ``pass`` and Bob has status ``fail``. To render Bob's
explanation, add:

.. code-block:: python

   for statement in run.statements:
       for focus in statement.selected_foci:
           if focus.status == "fail":
               print(focus.evidence.explain())

The tree groups the constraints that establish the failure. ``CountLow``
records too few matching values. A datatype constraint can appear as
``CountHigh`` with a maximum of zero: it counts values that violate the
datatype. Use :doc:`results` to handle individual findings in code, or the
structured projections below to inspect evidence.

``EvidenceSession`` accepts the graph input forms in :doc:`../reference/python`.
Pass lists when a schema or data snapshot spans several files:

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

Use these projections on either ``Satisfaction`` or ``Failure`` to extract
values, supporting triples, and missing counts:

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

A failed conjunction's evidence can omit passing children. To display every
immediate authored child's status, use ``progress``:

.. code-block:: python

   if focus.progress is not None:
       for child in focus.progress.evaluated_children:
           print(child.source_constraint_ref, child.constraint_kind, child.status)

To get a child's full explanation, ask the session for it:

.. code-block:: python

   detail = session.evidence_for(focus.focus, child.normalized_constraint_ref)
   print(detail.status, detail.evidence_kind)

``evidence_for`` takes the pair as given — no target selection happens — so it
also works for a focus node no statement selects.

Explain failures on demand
--------------------------

To display failure explanations, find failing pairs first and explain only
the ones you need:

.. code-block:: python

   counts, pairs = session.find_failures()
   for pair in pairs:
       detail = session.explain(pair)

``explain`` returns the same evidence as ``validate`` for that pair. Use a
pair from this session's ``find_failures()`` result. The Rust
``PreparedEvidenceValidator`` offers the same workflow. See
:ref:`evidence-cost` for the available levels of detail.

Serialize a run
---------------

.. code-block:: python

   payload = run.to_dict()
   encoded = run.to_json()

The JSON uses explicit status and variant tags, and includes catalogs of both
source and normalized constraints so child ids stay resolvable without the live
session.

For large runs, the compact encoding stores repeated terms and subtrees once:

.. code-block:: python

   wire = run.to_compact_json(include_catalog=False)
   catalog = run.to_dict()["constraints"]

   restored = shifty.expand_evidence(wire, catalog)
   assert restored == run.to_dict()

Drop the catalog when the receiver already has it for this schema. Expanding
the compact payload with that catalog reconstructs the original run.

See also
--------

- :doc:`../reference/evidence` — objects, fields, and projections.
- :ref:`understanding-evidence` and :ref:`evidence-limits` — interpreting
  selected nodes and explanations.
- ``python/examples/evidence_point_list.py`` — a larger worked example over
  BuildingMOTIF point-list shapes, with passing, failing, empty-selection and
  unselected cases.
- :doc:`../experimental/repair/index` — experimental repair templates built
  from failure evidence.
