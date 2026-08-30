Run SHACL-AF inference
======================

SHACL-AF rules (``sh:rule``) derive new triples from existing ones. Shifty runs
them by forward chaining to a fixed point: every rule whose body is satisfied
fires, the derived triples become available to other rules, and this repeats
until nothing new appears.

Rules and data
--------------

A rule lives on a shape and fires for each node that shape targets. This one
copies a rectangle's width to an ``ex:area`` property — not a useful
calculation, but a small complete example:

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

Rules embedded in the data graph work the same way — omit the second argument:

.. code-block:: python

   result = shifty.infer(combined)

Passing an empty ``rdflib.Graph()`` as the second argument is *not* the same
thing: it means "run with an explicitly empty rules graph", so no rules are
found and nothing is derived.

Inference during validation
---------------------------

``validate()`` runs inference first by default and validates the extended
graph, which is usually what you want — a rule that derives ``ex:area`` should
be able to satisfy a shape that requires ``ex:area``. Turn it off with
``infer=False`` (Python) or ``--no-infer`` (CLI) to validate only asserted
triples.

The two phases do not interleave. Inference runs to a fixed point, then
validation runs over the result. They also use opposite fixed points —
inference takes the least, validation the greatest — for reasons explained in
:doc:`../explanation/recursion`.

When rules refer to each other
------------------------------

Rules may depend on rules, including cyclically. Shifty analyses the dependency
graph and evaluates in strata, and a schema whose recursion passes through a
negation is refused with a diagnostic rather than guessed at. To see the
analysis for a shapes file:

.. code-block:: bash

   shifty inspect --stage strata rules.ttl

``sh:order`` and ``sh:condition`` on a rule are honoured within this scheme.

Note that ``infer()`` takes no ``graph_mode``. Graph modes describe what
validation can see; inference always reads and extends the data graph.

See also
--------

- :doc:`../reference/feature-support` — which rule and node-expression forms
  are supported.
- :doc:`../explanation/recursion` — stratification, and why inference uses the
  least fixed point.
