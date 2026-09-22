Inspect how shapes were compiled
===================================

Shifty does not interpret your SHACL graph directly. It lowers it to an
algebra, normalizes that, analyses recursion, and builds a physical plan.
``shifty inspect`` prints any of those stages, which is how you find out what
the engine thinks your shapes mean.

Use it when a constraint is not firing when you expect it to, when validation is
slower than it should be, or when you want to know whether a SPARQL constraint
is running natively.

.. code-block:: bash

   shifty inspect --stage <stage> shapes.ttl

Every stage except ``capability`` supports ``--format text`` (default) and
``--format json``; ``capability`` is text-only. The ``algebra`` and
``normalized`` stages also accept ``--format dot`` for Graphviz. Note that
``inspect`` takes the shapes file as a positional argument, not ``--shapes``,
and reads no data graph — it is entirely about the schema.

The examples below use the shapes file from
:doc:`../tutorials/first-validation`.

What was parsed
---------------

.. code-block:: bash

   shifty inspect --stage rdf shapes.ttl

The raw triples, after parsing and before any interpretation. Reach for this
when you suspect a prefix or a syntax problem rather than a semantic one.

What the shapes compiled to
---------------------------

.. code-block:: bash

   shifty inspect --stage algebra shapes.ttl

.. program-output:: shifty inspect --stage algebra shapes.ttl
   :cwd: ../examples/quick-start

Shapes are numbered nodes in an arena, referring to each other by id. The
``ex:email`` lower count comes directly from ``sh:minCount 1``. The datatype
constraint appears as an upper count of zero values that fail the datatype
test: that is how the algebra expresses "every value has this datatype".

If a constraint you wrote is missing here, it was not understood. That is the
fastest way to catch a misspelled SHACL predicate, which is otherwise silent —
an unrecognised triple is not an error, it simply constrains nothing.

What the optimizer did to it
----------------------------

.. code-block:: bash

   shifty inspect --stage normalized shapes.ttl

.. program-output:: shifty inspect --stage normalized shapes.ttl
   :cwd: ../examples/quick-start

The identical ``⊤`` nodes in this example are hash-consed into one. On a larger
shapes graph this stage may collapse more, and
also flattens boolean nesting, folds contradictory facets to ⊥, tightens
overlapping ranges, and pushes negation to the leaves.

The ``statements`` line shows the compiled target: ``sh:targetClass ex:Person``
became a path expression that walks ``rdf:type`` and then any number of
``rdfs:subClassOf`` steps. This is the stage to check when a target is
selecting more or fewer nodes than you expected.

Normalization preserves meaning but not identity, so constraint ids differ
between the source and normalized schemas. Evidence carries both — see
:doc:`../reference/evidence`.

Whether recursion is well-founded
---------------------------------

.. code-block:: bash

   shifty inspect --stage strata shapes.ttl

.. program-output:: shifty inspect --stage strata shapes.ttl
   :cwd: ../examples/quick-start

Shapes may reference each other cyclically. Shifty evaluates such a schema in
strata and refuses one whose recursion runs through a negation, because that has
no consistent two-valued answer. If a schema is rejected, this stage names the
cycle. See :doc:`../explanation/recursion`.

What will actually be executed
------------------------------

.. code-block:: bash

   shifty inspect --stage plan shapes.ttl

.. program-output:: shifty inspect --stage plan shapes.ttl
   :cwd: ../examples/quick-start

Two things are decided here. The ``seed`` line is how focus nodes are found —
an index lookup rather than a scan over the graph. And the conjunctions are
reordered by estimated cost: the email lower count runs before the name
constraints, so a node missing its email can short-circuit.

The plan exposes two common sources of runtime cost: a conjunction whose cheap
branch is not first, and target selection that seeds from a scan rather than an
index.

Whether SPARQL runs natively
----------------------------

.. code-block:: bash

   shifty inspect --stage capability shapes.ttl

Shifty executes a subset of ``sh:sparql`` constraints and CONSTRUCT rules
directly against its own indexes, and falls back to a general SPARQL engine for
the rest. This stage classifies each query. A fallback may cost more on a given
dataset; use ``--profile`` to measure its actual impact.

Which accesses compilation expects
-----------------------------------

.. code-block:: bash

   shifty inspect --stage access shapes.ttl

This stage lists each statement, rule, and function's possible reads from the
default evaluation graph and named shapes graph. It identifies fixed or any
predicates, forward/reverse/membership/open probes, node-domain reads, and
whether analysis is complete or conservative. Rule entries also show possible
predicate writes. Query and path IDs link repeated uses of the same compiled
body. Use ``--format json`` to consume the catalog programmatically.

These are data-independent requirements, not indexes already built. Runtime
``--profile`` output shows which source or session indexes were admitted, their
estimated and allocated bytes, the byte budget, and scan work. A conservative
or unknown requirement keeps a correct scan fallback; it does not restrict
which triples a query can read.

See also
--------

- :doc:`../explanation/architecture` — what each layer is for.
- :doc:`../reference/cli` — the full flag list.
