Applying inference to a model
=============================

Use a SHACL-AF rule to copy each person's name into a display-name property,
then validate the extended model. Install Shifty as described in
:doc:`../install`.

Create a rule and data file
---------------------------

Save this as ``rules.ttl``:

.. literalinclude:: ../examples/applying-inference/rules.ttl
   :language: turtle

The rule selects each ``ex:Person`` and adds an ``ex:displayName`` triple
using their ``ex:name`` value. The property constraint requires a display name.

Save this as ``data.ttl``:

.. literalinclude:: ../examples/applying-inference/data.ttl
   :language: turtle

Run inference
-------------

Run this beside the two files:

.. code-block:: bash

   shifty infer --shapes rules.ttl --data data.ttl

.. program-output:: shifty infer --shapes rules.ttl --data data.ttl
   :cwd: ../examples/applying-inference

The output lists the new triple. Rules run repeatedly until they produce no
further additions.

In Python, retrieve the extended graph:

.. literalinclude:: ../examples/applying-inference/infer.py
   :language: python

.. program-output:: python infer.py
   :cwd: ../examples/applying-inference

``result.graph()`` contains both the original data and the derived triples.
To save it, call ``graph.serialize("extended.ttl", format="turtle")``.

Validate with the rule
----------------------

Validation runs inference by default, so the derived display name satisfies
the property constraint:

.. code-block:: bash

   shifty validate --shapes rules.ttl --data data.ttl

.. program-output:: shifty validate --shapes rules.ttl --data data.ttl
   :cwd: ../examples/applying-inference

Add ``--no-infer`` and validation fails because the original data has no
``ex:displayName``. Python has the same default:

.. literalinclude:: ../examples/applying-inference/validate.py
   :language: python

.. program-output:: python validate.py
   :cwd: ../examples/applying-inference

Continue with :doc:`../how-to/infer` for in-place RDFLib updates and other
inference options, or :doc:`../reference/feature-support` for supported rules.
