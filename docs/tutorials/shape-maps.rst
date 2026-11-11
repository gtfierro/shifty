Extracting values with a shape map
==================================

A shape map returns the values that satisfied a shape's property constraints.
Use one to find sensors, build a configuration, or feed validated values into
an application. Install Shifty as described in :doc:`../install`.

Describe the values to find
---------------------------

In this example, each VAV should have a temperature sensor and an airflow
sensor. The shapes name those two slots. Save and run this Python script:

.. literalinclude:: ../examples/shape-map.py
   :language: python

.. program-output:: python examples/shape-map.py
   :cwd: ..

``vav-2`` satisfies both requirements. ``vav-1`` lacks an airflow sensor, but
its temperature sensor is still available as a successful binding.

Use the bindings
----------------

Add this after the loop to build a dictionary for the conforming VAV:

.. code-block:: python

   mapping = next(
       row for row in smap["http://example.org/VavShape"] if row.conforms
   )
   for name, values in sorted(mapping.value_map(by="name", python=True).items()):
       print(name, values)

.. code-block:: text

   airflow ['http://example.org/flow-2']
   supply air temperature ['http://example.org/sat-2']

The keys come from ``sh:name``. Each value is a list because a property may
match several nodes. ``python=True`` converts RDF terms to Python values.
Failed bindings are omitted from this dictionary.

Continue with :doc:`../how-to/shape-maps` to inspect missing values, annotate
bindings, and serialize a map. The :doc:`shape-map reference
<../reference/shape-maps>` lists the available fields and methods.
