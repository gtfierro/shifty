Shifty
======

.. raw:: html

   <div class="sh-hero">
     <p class="sh-tagline">
       A SHACL validation and SHACL-AF inference engine that compiles shapes to
       an algebra — and can tell you <em>why</em> a node passed or failed.
     </p>
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

A SHACL validator answers a yes/no question: does this graph conform to these
shapes? That answer is enough to gate a pipeline, and not much else. If a node
failed, you usually want to know which constraint failed and on which triples.
If it passed, you may want to know *what* satisfied the constraint — the sensor
that matched, the value that qualified — and today you get that by writing a
second query that duplicates the shape's logic.

Shifty is built around the observation that the validator already knows all of
this. It computes the answer by structural recursion over the constraint; the
derivation exists in memory and is then thrown away. Shifty keeps it. That
single decision is what the :doc:`evidence <reference/evidence>` interface, the
:doc:`shape map <reference/shape-maps>` view, and the
:doc:`symbolic repair <reference/repair>` layer are all built on: they are
projections of the same derivation, not separate re-implementations of SHACL.

Making that affordable is why shapes are compiled rather than interpreted.
Shifty lowers SHACL to a path algebra (π) and a shape grammar (φ) taken from
`Common Foundations for SHACL, ShEx, and PG-Schema <https://arxiv.org/abs/2502.01295>`_,
normalizes it, and plans it. The same intermediate representation drives
validation, inference, evidence, and repair — so a constraint has one meaning
in the system, not four.

Shifty runs as a command-line tool, a Python library (``pyshifty``), a C++17
static library, and a WebAssembly module that runs in the browser.

Where to start
--------------

.. list-table::
   :widths: 25 75

   * - :doc:`Tutorials <tutorials/index>`
     - Start here if you are new. Two worked lessons that take you from an
       empty directory to a validated graph, then to a graph the engine
       repaired for you.
   * - :doc:`How-to guides <how-to/index>`
     - Recipes for a specific job: run inference, extract bindings, drive a
       repair loop, inspect the compiled plan.
   * - :doc:`Reference <reference/index>`
     - Exact behaviour of the CLI flags, the Python API, the evidence data
       model, and the supported SHACL feature set.
   * - :doc:`Explanation <explanation/index>`
     - Why the engine is built this way — the algebra, the recursion
       semantics, what evidence costs, and what it is measured to cost.

Quick start
-----------

Validate a data graph against a shapes graph:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl

.. code-block:: text

   conforms: false
   violations: 1
     <http://example.org/bob>  [severity: Violation; target: class(<http://example.org/Person>)]
         - [Violation] (<http://example.org/email>) <http://example.org/bob> → at least 1 value(s) required along <http://example.org/email>, found 0
         - [Violation] (<http://example.org/name>) "123"^^<http://www.w3.org/2001/XMLSchema#integer> → test(datatype(xsd:string)) not satisfied

The same thing from Python, with a ``pyshacl``-compatible signature:

.. code-block:: python

   import shifty

   conforms, report_graph, results_text = shifty.validate(data, shapes)

And in the browser: the `playground <https://shifty.gtf.fyi/playground/>`_ runs
the whole engine as WebAssembly, so nothing you paste into it leaves your
machine.

.. toctree::
   :maxdepth: 2
   :hidden:

   tutorials/index
   how-to/index
   reference/index
   explanation/index
   Rust API (docs.rs) <https://docs.rs/shifty-engine>
