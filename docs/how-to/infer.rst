Run SHACL-AF inference
======================

SHACL-AF rules (``sh:rule``) derive new triples from existing ones. Shifty runs
them by forward chaining to a fixed point: every rule whose body is satisfied
fires, the derived triples become available to other rules, and this repeats
until nothing new appears.

For a complete example from rule to validation, see
:doc:`../tutorials/applying-inference`.

Rules and data
--------------

A rule lives on a shape and fires for each node that shape targets. This
example derives an ``ex:area`` value from a rectangle's width:

.. literalinclude:: ../examples/inference/rules.ttl
   :language: turtle

.. literalinclude:: ../examples/inference/data.ttl
   :language: turtle

Print the derived triples
-------------------------

.. literalinclude:: ../examples/inference/infer.sh
   :language: bash

.. program-output:: bash infer.sh
   :cwd: ../examples/inference

Only the *new* triples are listed, not the input. ``--format json`` gives the
same thing structured.

If the rules are embedded in the data graph, pass one file:

.. code-block:: bash

   shifty infer --shapes combined.ttl

Get the extended graph
----------------------

The CLI prints derived triples; it does not write a merged file. To get the
original graph plus everything derived, use Python:

.. code-block:: python

   result = shifty.infer(data, rules)

   print(result.inferred_count)        # 1
   graph = result.graph()              # rdflib.Graph: original + inferred
   graph.serialize("out.ttl", format="turtle")

``result.graph_ntriples`` gives the same content as an N-Triples string without
constructing an rdflib graph, which is faster if you are writing it straight to
a file or passing it to another process.

To add the derived triples to an existing ``rdflib.Graph``, pass
``in_place=True``:

.. code-block:: python

   data = rdflib.Graph()
   data.parse("data.ttl")

   result = shifty.infer(data, rules, in_place=True)
   print(result.inferred_count)   # 1
   result.graph() is data         # True — same object, now extended

``in_place=True`` requires a single ``rdflib.Graph``. Other input types raise
``TypeError``. Existing blank-node identities are preserved when inferred
triples are added.

Rules embedded in the data graph work the same way — omit the second argument:

.. code-block:: python

   result = shifty.infer(combined)

Passing an empty ``rdflib.Graph()`` as the second argument is *not* the same
thing: it means "run with an explicitly empty rules graph", so no rules are
found and nothing is derived.

Rules that create blank nodes
----------------------------

SPARQL rules can create fresh blank nodes in CONSTRUCT templates or through
``BNODE()``. Use a guard such as ``FILTER NOT EXISTS`` when a rule should add a
default value only once per focus node. Each solution creates its own blank
nodes, and triples describing the same template node stay connected.

Rules that continually create new terms may never reach a fixed point.
Inference stops with an execution failure after 1000 passes by default.
``infer()`` accepts a positive ``max_iterations`` to change this limit:

.. code-block:: python

   result = shifty.infer(data, rules, max_iterations=2000)

The limit includes the final pass that checks convergence. Exceeding it raises
``shifty.ValidationFailure``, naming the active rules and their shapes. This
also applies to ``on_unsupported="ignore"``. Failed inference leaves a graph
passed with ``in_place=True`` untouched.

Constraints and CONSTRUCT rules allow ``VALUES`` on unrelated variables, such
as ``VALUES ?prop { brick:hasQuantity brick:hasSubstance }``. A clause mentioning
a potentially pre-bound variable, such as ``this`` or a custom constraint
parameter (including an optional parameter), is rejected before execution.
ASK validators also pre-bind ``value``; SELECT validators may use it as an
ordinary result variable. Both ``?this`` and ``$this`` denote the same variable.
This follows the narrower restriction in
`SHACL 1.2 Appendix A <https://www.w3.org/TR/2026/WD-shacl12-sparql-20261022/#pre-binding>`_
instead of SHACL 1.0's blanket ban. ``on_unsupported`` does not override these
collisions. See also
`SHACL-AF SPARQL rules <https://www.w3.org/TR/shacl-af/#sparql-rules>`_.

.. _inference-during-validation:

Inference during validation
---------------------------

``validate()`` runs inference first by default and validates the extended
graph, which is usually what you want — a rule that derives ``ex:area`` should
be able to satisfy a shape that requires ``ex:area``. Turn it off with
``infer=False`` (Python) or ``--no-infer`` (CLI) to validate only asserted
triples.

``validate()`` normally discards that inferred closure once the report is
built. Pass ``in_place=True`` (with ``infer=True``, the default) to add it
into ``data_graph`` instead, the same way ``infer(..., in_place=True)`` does:

.. code-block:: python

   data = rdflib.Graph()
   data.parse("data.ttl")

   conforms, report, _ = shifty.validate(data, shapes, in_place=True)
   # data now includes whatever the rules derived; report is still a
   # fresh rdflib.Graph either way.

``validate_algebra()`` and ``PreparedValidator.validate()`` /
``.validate_algebra()`` accept the same ``in_place`` option.

The two phases do not interleave. Inference runs to a fixed point, then
validation runs over the result. They also use opposite fixed points —
inference takes the least, validation the greatest — for reasons explained in
:ref:`recursive-shapes`.

When rules refer to each other
------------------------------

Rules may depend on rules, including cyclically. Shifty analyses the dependency
graph and evaluates in strata. A schema whose recursion passes through
negation is rejected with a diagnostic. To see the
analysis for a shapes file:

.. code-block:: bash

   shifty inspect --stage strata rules.ttl

Likewise, malformed shapes (including malformed SPARQL or an unresolved query
prefix) raise an error before inference starts. A malformed rule is never
silently skipped; non-fatal unsupported features remain available through the
result's ``diagnostics``.

``sh:order`` and ``sh:condition`` on a rule are honoured within this scheme.

``infer()`` takes no ``graph_mode``. With separate inputs, rules can read
data and shapes while deriving additions to the data graph. See
:doc:`../explanation/shapes-and-data` for the evaluation rules.

See also
--------

- :doc:`../reference/feature-support` — which rule and node-expression forms
  are supported.
- :ref:`recursive-shapes` — stratification, and why inference uses the
  least fixed point.
