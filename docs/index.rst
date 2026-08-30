Shifty
======

.. raw:: html

   <div class="sh-hero">
     <div class="sh-badges">
       <a href="https://pypi.org/project/pyshifty/"><img src="https://img.shields.io/pypi/v/pyshifty.svg" alt="PyPI"></a>
       <a href="https://crates.io/crates/shifty-cli"><img src="https://img.shields.io/crates/v/shifty-cli.svg" alt="Crates.io"></a>
       <a href="https://docs.rs/shifty-engine"><img src="https://docs.rs/shifty-engine/badge.svg" alt="docs.rs"></a>
       <a href="https://github.com/gtfierro/shifty"><img src="https://img.shields.io/badge/GitHub-shifty-181717?logo=github" alt="GitHub"></a>
       <a href="https://github.com/gtfierro/shifty/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-BSD--3--Clause-blue.svg" alt="BSD-3-Clause"></a>
     </div>
     <div class="sh-installs">
       <code class="sh-install-cmd">pip install pyshifty</code>
       <code class="sh-install-cmd">cargo install --path crates/shifty-cli</code>
     </div>
   </div>

Shifty is a SHACL/SHACL-AF inference and validation engine for RDF graphs. It
is available from Python, the command line, C++17, Rust, and WebAssembly.

Quick start
-----------

Validate a data graph against a shapes graph:

.. literalinclude:: examples/quick-start/validate.sh
   :language: bash

.. program-output:: bash validate.sh
   :cwd: examples/quick-start

Run SHACL-AF rules to a fixed point:

.. code-block:: bash

   shifty infer --shapes rules.ttl --data data.ttl

The structured Python interface returns the same validation decision:

.. literalinclude:: examples/quick-start/validate.py
   :language: python

.. program-output:: python validate.py
   :cwd: examples/quick-start

The `playground <https://shifty.gtf.fyi/playground/>`_ runs the WebAssembly
build locally in the browser, so graphs entered there do not leave the machine.

Validation interfaces
---------------------

Shifty compiles SHACL into the path and shape algebra developed in Ahmetaj et
al., `Common Foundations for SHACL, ShEx, and PG-Schema
<https://doi.org/10.1145/3696410.3714694>`_ (The Web Conference 2025). The
compiled representation supports normalization, shared subexpressions, indexed
target selection, and cost-based planning before evaluation begins.

Two result interfaces are available:

.. list-table::
   :widths: 24 76
   :header-rows: 1

   * - Interface
     - Result
   * - Native algebraic
     - ``validate_algebra()`` returns structured violations and their nested
       algebraic reasons; the CLI renders the same result model by default.
       This is the lower-overhead reporting path and does not construct a W3C
       ``sh:ValidationReport``.
   * - W3C-compatible
     - ``validate()`` and ``shifty validate --report`` return a W3C
       ``sh:ValidationReport`` for interoperability with SHACL tooling.

The native representation also supports :doc:`evidence
<reference/evidence>`, passing-node :ref:`property witnesses
<python-property-witnesses>`, and :doc:`shape-map bindings
<reference/shape-maps>`. See :doc:`explanation/validation-interfaces` for the
tradeoffs between the two result models and `Introducing the Shifty SHACL
Engine <https://gtf.fyi/posts/shacl/shifty/>`_ for the original rationale.

Interface support
-----------------

.. list-table::
   :widths: 22 78

   * - :doc:`Python <reference/python>`
     - ``pip install pyshifty``. Validate, infer, explain, and reuse prepared
       schemas from ``pyshifty``.
   * - :doc:`Command line <reference/cli>`
     - Install the ``shifty`` binary and use it in scripts or CI.
   * - :doc:`C++ <reference/cpp>`
     - Link the C++17 static library and use its prepared-validator API.
   * - `Rust <https://docs.rs/shifty-engine>`_
     - Use the engine crates directly; API details live on docs.rs.
   * - :doc:`Browser / WebAssembly <how-to/browser>`
     - Run Shifty locally in the browser.

Common tasks
------------

.. list-table::
   :widths: 32 68

   * - :doc:`Run a first validation <tutorials/first-validation>`
     - Install Shifty, validate a small graph, and fix one failure.
   * - :doc:`Configure validation <how-to/validate>`
     - Choose graph visibility, report format, severity threshold, and named
       entry shapes.
   * - :doc:`Run inference <how-to/infer>`
     - Execute SHACL-AF rules and retrieve the inferred triples.
   * - :doc:`Explain a failure <how-to/explain-failures>`
     - Trace a finding through its compiled constraint and supporting triples.
   * - :doc:`Extract shape-map bindings <how-to/shape-maps>`
     - Query which focus nodes and values matched selected shapes.
   * - :doc:`Inspect the compiler pipeline <how-to/inspect-pipeline>`
     - Inspect the lowered algebra, normalization, recursion strata, and plan.
   * - :doc:`Compute symbolic repairs <how-to/repair>`
     - Explore candidate graph edits with the experimental repair API.

.. note::

   Symbolic repair is **experimental**. Its API may change; see
   :doc:`how-to/repair` for its current scope.

Documentation
-------------

.. list-table::
   :widths: 25 75

   * - :doc:`Tutorials <tutorials/index>`
     - Guided introductions to validation, results, and evidence.
   * - :doc:`How-to guides <how-to/index>`
     - Procedures for specific validation and inference tasks.
   * - :doc:`Reference <reference/index>`
     - Interfaces, options, fields, and feature-support boundaries.
   * - :doc:`Explanation <explanation/index>`
     - Design, semantics, and performance tradeoffs.

The :doc:`documentation contribution guide <contributing>` describes the page
conventions and preview workflow. Documentation issues can be filed on
`GitHub <https://github.com/gtfierro/shifty/issues/new>`_.

.. toctree::
   :maxdepth: 2
   :hidden:

   tutorials/index
   how-to/index
   reference/index
   explanation/index
   contributing
   Rust API (docs.rs) <https://docs.rs/shifty-engine>
