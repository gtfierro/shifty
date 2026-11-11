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

The structured Python interface returns a validation decision and reasons:

.. literalinclude:: examples/quick-start/validate.py
   :language: python

.. program-output:: python validate.py
   :cwd: examples/quick-start

The :doc:`playground <playground>` runs the WebAssembly build locally in the
browser, so graphs entered there do not leave the machine.

For a W3C ``sh:ValidationReport``, use ``validate()`` in Python or
``shifty validate --report`` on the CLI. See :ref:`architecture-result-paths`
to choose between reports, structured findings, and evidence.

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
   * - :doc:`Browser / WebAssembly <playground>`
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
   * - :doc:`Compute symbolic repairs <experimental/repair/how-to>`
     - Explore candidate graph edits with the experimental repair API.
   * - :doc:`Write shapes in Shapeshift <experimental/shapeshift/tutorial>`
     - Write SHACL shapes in a short language of their own, or from Python.
   * - :doc:`Build a repair benchmark <experimental/violation-generation/how-to>`
     - Generate broken graphs with known repairs to evaluate a repair tool.

.. note::

   Symbolic repair, Shapeshift, and violation generation are
   **experimental**. Their APIs, and the Shapeshift language, may change in
   any release; see :doc:`experimental/index`.

Use the :doc:`tutorials <tutorials/index>` to learn the interfaces,
:doc:`how-to guides <how-to/index>` for specific tasks, and the
:doc:`reference <reference/index>` for exact options and fields.
:doc:`Explanation <explanation/index>` covers graph evaluation and results.

See :doc:`contributing` for development checks, the W3C conformance suite,
and documentation conventions. :doc:`benchmarks` reports validation timings.

.. toctree::
   :maxdepth: 2
   :hidden:

   install
   tutorials/index
   how-to/index
   reference/index
   explanation/index
   experimental/index
   playground
   benchmarks
   changelog
   contributing
   Rust API (docs.rs) <https://docs.rs/shifty-engine>
