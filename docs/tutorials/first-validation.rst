Validating a model
==================

Validate an RDF data file against SHACL shapes, read the failures, and fix the
data. The examples use Turtle, RDF's text format.

Install Shifty with ``pip install "pyshifty[rdflib]"`` for Python. For the CLI,
follow :doc:`../install`.

Create the shapes and data
--------------------------

Save this as ``shapes.ttl``. Every person must have a string name and an email.

.. literalinclude:: ../examples/quick-start/shapes.ttl
   :language: turtle

Save this as ``data.ttl``. Alice satisfies the shape. Bob has an integer name
and no email.

.. literalinclude:: ../examples/quick-start/data.ttl
   :language: turtle

Validate from the command line
------------------------------

Run this in the directory containing the two files:

.. literalinclude:: ../examples/quick-start/validate.sh
   :language: bash

.. program-output:: bash validate.sh
   :cwd: ../examples/quick-start

Both findings concern Bob: one for his name's datatype and one for the missing
email. Alice does not appear because the report lists failures.

Validation runs SHACL-AF inference before checking the data by default. These
shapes contain no rules, so the data stays as written. Use ``--no-infer`` on
the CLI or ``infer=False`` in Python to check only asserted triples. The
:doc:`applying-inference` tutorial shows a model with rules.

Validate from Python
--------------------

Pass the data file first and the shapes file second:

.. literalinclude:: ../examples/quick-start/report.py
   :language: python

.. program-output:: python report.py
   :cwd: ../examples/quick-start

``conforms`` is the validation decision. ``report_graph`` is an
``rdflib.Graph`` containing a standard W3C ``sh:ValidationReport``;
``results_text`` renders that report for display. Print ``results_text`` to
see the individual failures.

You can also pass RDFLib graphs directly:

.. code-block:: python

   from rdflib import Graph

   data = Graph().parse("data.ttl")
   shapes = Graph().parse("shapes.ttl")
   conforms, report_graph, results_text = shifty.validate(data, shapes)

See the :doc:`Python reference <../reference/python>` for other input forms
and options. If shapes and data share one file, ``shifty.validate("model.ttl")``
uses it for both roles.

Fix the data
------------

Replace Bob's entry in ``data.ttl`` with a string name and an email:

.. code-block:: turtle

   ex:bob a ex:Person ; ex:name "Bob" ; ex:email "bob@example.org" .

Run either validation again. The CLI now prints:

.. code-block:: text

   conforms: true

In Python, ``conforms`` is now ``True``. In Turtle, ``123`` is an integer and
``"123"`` is a string; ``sh:datatype`` distinguishes them.

Next steps
----------

- :doc:`applying-inference` — derive triples with rules and validate the result.
- :doc:`shape-maps` — retrieve the values that satisfied a shape.
- :doc:`../how-to/results` — handle individual failures in application code.
- :doc:`../how-to/validate` — configure reports, graph visibility, and severity.
