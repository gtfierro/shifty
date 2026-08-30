Reference
=========

Exact behaviour, organised by interface. These pages describe what each thing
does and what its arguments mean; they do not explain why you would want it.
For that, see the :doc:`how-to guides <../how-to/index>` and
:doc:`../explanation/index`.

.. list-table::
   :widths: 24 18 58
   :header-rows: 1

   * - Interface
     - Stability
     - Contents
   * - :doc:`cli`
     - Stable
     - Every command and flag of the ``shifty`` binary.
   * - :doc:`python`
     - Stable
     - The ``pyshifty`` validation, inference, and prepared-validator API.
   * - :doc:`cpp`
     - Stable
     - The C++17 static library: dataset, prepared validator, evidence
       sessions, and the shape-map vocabulary.
   * - :doc:`evidence`
     - Stable
     - The evidence data model: sessions, runs, polarity, projections, and
       serialization.
   * - :doc:`shape-maps`
     - Stable
     - Typed keys, bindings, and terms.
   * - :doc:`feature-support`
     - Stable
     - Which parts of SHACL Core and SHACL-AF are implemented, and what
       happens at the edges.
   * - :doc:`repair`
     - Experimental
     - Sessions, templates, holes, plans, deltas, and the validation gate.

``Stable`` means compatibility is intended across minor releases.
``Experimental`` APIs may change as their semantics and use cases settle.

The Rust API is documented separately on
`docs.rs/shifty-engine <https://docs.rs/shifty-engine>`_.

.. toctree::
   :maxdepth: 1
   :hidden:

   cli
   python
   cpp
   evidence
   shape-maps
   feature-support
   repair
