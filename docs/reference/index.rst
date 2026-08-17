Reference
=========

Exact behaviour, organised by interface. These pages describe what each thing
does and what its arguments mean; they do not explain why you would want it.
For that, see the :doc:`how-to guides <../how-to/index>` and
:doc:`../explanation/index`.

.. list-table::
   :widths: 30 70

   * - :doc:`cli`
     - Every command and flag of the ``shifty`` binary.
   * - :doc:`python`
     - The ``pyshifty`` validation, inference, and prepared-validator API.
   * - :doc:`evidence`
     - The evidence data model: sessions, runs, polarity, projections, and
       serialization.
   * - :doc:`shape-maps`
     - Typed keys, bindings, and terms.
   * - :doc:`repair`
     - Sessions, templates, holes, plans, deltas, and the gate.
   * - :doc:`feature-support`
     - Which parts of SHACL Core and SHACL-AF are implemented, and what
       happens at the edges.

The Rust API is documented separately on
`docs.rs/shifty-engine <https://docs.rs/shifty-engine>`_.

.. toctree::
   :maxdepth: 1
   :hidden:

   cli
   python
   evidence
   shape-maps
   repair
   feature-support
