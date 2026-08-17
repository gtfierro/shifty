Repair reference
================

The repair API computes the space of edits that would make a failing node
conform, and applies whichever ones you choose. It decides nothing itself:
which focus to fix, which term fills a hole, which alternative to take, how
many values to add, whether to accept a candidate, and when to stop are all
yours. :doc:`../explanation/repair-design` explains that boundary;
:doc:`../how-to/repair` shows the loop.

Repairs edit the **data graph only**. The schema is treated as ground truth.

``RepairSession``
-----------------

.. code-block:: python

   shifty.RepairSession(shacl_graph, data_graph=None, *, infer=True, base=None)

Binds a shapes graph and a data graph, running SHACL-AF inference first by
default. ``data_graph=None`` means the shapes graph embeds the data.

.. list-table::
   :widths: 34 66
   :header-rows: 1

   * - Method
     - Meaning
   * - ``witnesses()``
     - The violation horizon: one ``Failure`` per failing ``(focus,
       statement)``. Empty exactly when the graph conforms.
   * - ``witnesses_for(shape_iri)``
     - The same, restricted to statements targeting one shape. Raises
       ``ValueError`` if no shape is named that.
   * - ``satisfactions_for(shape_iri)``
     - The dual: one ``Satisfaction`` per *passing* focus for that shape, each
       recording the values matched along every checked path.
   * - ``gate(delta)``
     - Re-validate ``G ⊕ ΔG`` and diff against ``G``. Returns a
       ``RepairOutcome``. Applies nothing, decides nothing.
   * - ``advance(delta)``
     - A **new** session over ``G ⊕ ΔG``, same schema, no re-inference.
   * - ``apply(delta)``
     - Materialize ``G ⊕ ΔG`` as a fresh ``rdflib.Graph`` without advancing.
   * - ``to_graph()``
     - The session's current graph, with every accepted delta applied.
   * - ``repair_node_against(node, shape_id)``
     - A tree making ``node`` conform to sub-shape ``shape_id``. ``None`` if it
       already does. The building block for a ``conforms to`` hole.
   * - ``describe_shape(shape_id)``
     - A fully expanded, human-readable definition of that shape, every child
       inlined and no ``@id`` indirection.
   * - ``diagnostics``
     - Warnings from lowering the shapes graph.

``G ⊕ ΔG`` applies **deletes first, then adds**, so a triple present on both
sides is a net add.

``Failure`` and ``Satisfaction``
--------------------------------

The same objects the :doc:`evidence` interface returns. A ``Failure`` is the
lossless witness of why a focus violates; a ``Satisfaction`` is why it holds.

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``focus``
     - The focus node.
   * - ``shape_name``
     - The IRI of the statement's shape, when it is a named RDF node.
   * - ``selector``
     - The ``Target`` that selected this focus.
   * - ``statement_id`` / ``constraint_id``
     - The join key shared with ``validate_algebra()``'s violations.
   * - ``summary()``
     - Leaf facts as ``WitnessAtom`` / ``SatAtom``, without the tree.
   * - ``explain()``
     - Human-readable rendering.
   * - ``walk()``, ``supporting_triples()``, ``path_supports()``,
       ``matched_values()``, ``missing_obligations()``, ``offending_values()``,
       ``source_constraints()``
     - The evidence projections; see :doc:`evidence`.
   * - ``to_json()`` / ``to_dict()``
     - Serialization.
   * - ``repair_tree()``
     - Synthesize the repair template for this failure. ``Failure`` only.

``RepairTree``
--------------

The template: a parametric, inspectable description of the whole repair space
for one violation. Its structure mirrors the constraint — conjunction becomes
"do all", disjunction becomes "do any one", a cardinality gap becomes a repeat
block.

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``is_blocked``
     - True when no data repair exists in scope for this focus.
   * - ``explain()``
     - Human-readable rendering of the template.
   * - ``holes()``
     - Every ``Hole`` currently in the tree.
   * - ``choices()``
     - Every open decision point, as ``Choice`` objects.
   * - ``instantiate(plan)``
     - Fold a ``RepairPlan`` over the tree. Returns ``Instantiated``. Pure:
       validates nothing, chooses nothing.

Node kinds, as they appear in ``explain()`` output:

.. list-table::
   :widths: 24 76
   :header-rows: 1

   * - Kind
     - Meaning
   * - ``Noop``
     - Already satisfied; the empty repair.
   * - ``Blocked``
     - Unrepairable in scope, with a reason.
   * - ``Edits``
     - A set of add/delete triple patterns, with the holes they mention.
   * - ``All``
     - Satisfy every child.
   * - ``Any``
     - Satisfy any one child.
   * - ``Repeat [min..max]``
     - Instantiate the body between ``min`` and ``max`` times.

Blocked branches are normalized away rather than left for you to trip over: an
``All`` with any blocked child is itself blocked; an ``Any`` drops its blocked
children and is blocked only if all of them were. So a live branch never
contains a dead one.

Reasons a branch can be blocked:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Reason
     - Meaning
   * - opaque SPARQL
     - ``sh:sparql`` is not algebraically invertible.
   * - cannot mutate identity
     - A node-kind or identity test on the focus itself; editing data cannot
       change it.
   * - coinductive
     - Support reached through a greatest-fixed-point back-edge, with no finite
       set of facts to delete.
   * - unsupported
     - A construct this version does not synthesize repairs for.

``Hole``
--------

A typed placeholder. The engine will not invent a value; the hole says what a
legal one looks like.

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``id``
     - The identifier to pass to ``RepairPlan.bind``.
   * - ``constraint``
     - What a legal value must satisfy, rendered — e.g. ``any node``,
       ``datatype(xsd:string)``, ``typed value``.
   * - ``candidates(limit)``
     - Existing terms in the graph that satisfy the constraint. A convenience
       for reuse-oriented drivers; bindings need not come from it.
   * - ``conforms_to``
     - The sub-shape id a value must conform to, if any.
   * - ``conforms_to_shapes``
     - Every such shape id, when there is more than one.
   * - ``sub_shapes()``
     - ``(id, description)`` pairs for those shapes.

Hole constraints correspond to: any node; a freshly minted node; equality with
a constant; a value type (datatype, numeric range, length, pattern); a node
kind; membership in a finite set; or conformance to a sub-shape.

``RepairPlan``
--------------

Your choices, as serializable data keyed by node and hole id. Partial plans are
legal.

.. code-block:: python

   plan = shifty.RepairPlan()
   plan.choose(node_id, branch_index)   # pick a child at an Any
   plan.count(node_id, n)               # pick a count at a Repeat
   plan.bind(hole_id, term)             # bind a hole
   plan.clear(id)                       # undo one entry

``term`` is an N-Triples spelling: ``'"Bob"'``, ``'"12"^^<...#integer>'``,
``'<http://example.org/bob>'``.

``Instantiated``
----------------

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``delta``
     - The ``RepairDelta`` resolved so far.
   * - ``open_holes``
     - Holes still needing a term.
   * - ``open_choices``
     - Node ids of ``Any``/``Repeat`` nodes still needing a decision.
   * - ``is_complete``
     - True when nothing is open.

.. important::

   Resolving a ``Repeat`` count stamps out that many copies of its body, and
   **each copy gets fresh holes with new ids**. Fill choices first,
   re-instantiate, then bind the holes that appear. Binding a hole id observed
   before the count was fixed binds a template hole that no longer corresponds
   to anything in the output.

``RepairDelta``
---------------

.. list-table::
   :widths: 34 66
   :header-rows: 1

   * - Member
     - Meaning
   * - ``add`` / ``delete``
     - Lists of ``(subject, predicate, object)`` N-Triples strings.
   * - ``is_empty``
     - Whether the delta does nothing.
   * - ``RepairDelta.from_ntriples(add, delete)``
     - Build one from N-Triples text.

.. code-block:: python

   shifty.delta_from_graph(add=None, delete=None) -> RepairDelta

Build a delta from hand-authored subgraphs — an ``rdflib.Graph``, Turtle text,
or a list of either, unioned. This is how a driver proposes a whole subgraph (a
new node with its type and properties) rather than one term. It gates and
applies exactly like a synthesized delta.

Because deletes are applied before adds, a triple you intend to remove must not
also be re-asserted by an add source. The replace pattern is: put it in
``delete``, and the replacement in ``add``.

``RepairOutcome``
-----------------

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Member
     - Meaning
   * - ``fixed``
     - Violations this delta removes.
   * - ``introduced``
     - **New** violations it would cause.
   * - ``remaining``
     - Pre-existing violations still unfixed.
   * - ``is_sound``
     - ``introduced`` is empty.
   * - ``is_progress``
     - Sound, and ``fixed`` is non-empty.

The gate is whole-graph rather than focus-local, because a delta that fixes one
node by breaking another is not a repair. The verdict is exactly the set
difference of ``violations(G ⊕ ΔG, S)`` against ``violations(G, S)``, computed
by re-running the same validator.

Rust
----

``shifty-repair`` exposes the same model: ``witness_violations``,
``witness_shape``, ``satisfy_shape``, ``witness_node``, ``synthesize``,
``synthesize_focus``, ``instantiate``, ``candidates``, ``gate``, and ``render``
(which projects a template to a parameterized RDF graph plus a self-hosted
SHACL sidecar over its holes, for graph-matching drivers). See
`docs.rs <https://docs.rs/shifty-engine>`_.
