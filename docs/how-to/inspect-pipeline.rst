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

Every stage supports ``--format text`` (default) and ``--format json``. The
``algebra`` and ``normalized`` stages also accept ``--format dot`` for
Graphviz. Note that
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

.. code-block:: text

   schema: 1 statement(s), 0 rule(s), 13/13 shape(s)
   shapes:
     @0 = severity(Violation, @11)  # <http://example.org/PersonShape>
     @1 = severity(Violation, @3)
     @2 = ⊤
     @3 = ∃[1..] <http://example.org/email> . ⊤
     @4 = severity(Violation, @10)
     @5 = test(datatype(xsd:string))
     @6 = ¬@5
     @7 = ∃[..0] <http://example.org/name> . @6
     @8 = ⊤
     @9 = ∃[1..] <http://example.org/name> . ⊤
     @10 = @7 ∧ @9
     @11 = @1 ∧ @4

Shapes are numbered nodes in an arena, referring to each other by id. Reading
it back: ``@3`` is "at least one value along ``ex:email``", the direct
translation of ``sh:minCount 1``. ``@9`` is the same for ``ex:name``, and
``@7`` is the datatype constraint — expressed, as every universal is, as "at
most zero values along ``ex:name`` fail the datatype test". ``@10`` conjoins
those two, ``@11`` conjoins both property shapes, and ``@0`` is the named shape.

If a constraint you wrote is missing here, it was not understood. That is the
fastest way to catch a misspelled SHACL predicate, which is otherwise silent —
an unrecognised triple is not an error, it simply constrains nothing.

What the optimizer did to it
----------------------------

.. code-block:: bash

   shifty inspect --stage normalized shapes.ttl

.. code-block:: text

   schema: 1 statement(s), 0 rule(s), 12/12 shape(s)
   shapes:
     @0 = test(<http://example.org/Person>)
     ...
     @11 = severity(Violation, @10)  # <http://example.org/PersonShape>
   statements:
     ∃≥1 rdf:type/rdfs:subClassOf* . φ  ⇒  @11

Thirteen shapes became twelve: the two structurally identical ``⊤`` nodes were
hash-consed into one. On a real shapes graph this stage collapses far more, and
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

.. code-block:: text

   strata: stratifiable = true; 13 shape(s) in 13 stratum(strata); 0 recursive component(s)

Shapes may reference each other cyclically. Shifty evaluates such a schema in
strata and refuses one whose recursion runs through a negation, because that has
no consistent two-valued answer. If a schema is rejected, this stage names the
cycle. See :doc:`../explanation/recursion`.

What will actually be executed
------------------------------

.. code-block:: bash

   shifty inspect --stage plan shapes.ttl

.. code-block:: text

   plan: 1 statement(s)
     [0] seed <http://example.org/Person> ⟵ rdf:type/rdfs:subClassOf*  ⇒  @11
   shapes (cost-ordered):
     @1 [cost 1] = test(datatype(xsd:string))
     @2 [cost 1] = ¬@1
     @3 [cost 2] = ∃[..0] <http://example.org/name> . @2
     @4 [cost 0] = ⊤
     @5 [cost 1] = ∃[1..] <http://example.org/name> . ⊤
     @6 [cost 3] = @5 ∧ @3
     @7 [cost 3] = severity(Violation, @6)
     @8 [cost 1] = ∃[1..] <http://example.org/email> . ⊤
     @9 [cost 1] = severity(Violation, @8)
     @10 [cost 4] = @9 ∧ @7
     @11 [cost 4] = severity(Violation, @10)

Two things are decided here. The ``seed`` line is how focus nodes are found —
an index lookup rather than a scan over the graph. And the conjunctions are
reordered by estimated cost: ``@10`` checks ``@9`` (cost 1, the email
minCount) before ``@7`` (cost 3, the name constraints), so a node missing its
email short-circuits without touching the more expensive branch.

This is the stage that explains a surprising runtime. A conjunction whose cheap
branch is not first, or a target seeding from a scan rather than an index, will
show up here.

Whether SPARQL runs natively
----------------------------

.. code-block:: bash

   shifty inspect --stage capability shapes.ttl

Shifty executes a subset of ``sh:sparql`` constraints and CONSTRUCT rules
directly against its own indexes, and falls back to a general SPARQL engine for
the rest. This stage classifies each query. A constraint that fell back is
usually the slow one, and this tells you before you spend time measuring.

See also
--------

- :doc:`../explanation/architecture` — what each layer is for.
- :doc:`../reference/cli` — the full flag list.
