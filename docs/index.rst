Shifty
======

.. raw:: html

   <div class="sh-hero">
     <p class="sh-tagline">
       Formalism-first SHACL validation and SHACL-AF inference —
       available as a CLI, Python library, and browser playground.
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

Shifty implements both **SHACL Core validation** and **SHACL-AF forward-chaining
inference**, grounded in the algebraic treatment of
`Common Foundations for SHACL, ShEx, and PG-Schema <https://arxiv.org/abs/2502.01295>`_.
Shapes are compiled to a path algebra (π) and shape grammar (φ) before
evaluation — the same IR drives both validation and inference.

- **Full SHACL Core validation** — node and property shapes, all standard constraint components
- **SHACL-AF inference** — ``sh:rule`` evaluation (Triple Rules and SPARQL Construct Rules) to a fixed point
- **Algebraic IR** — normalization, CSE, and cost-ordered physical planning before any execution
- **Multiple frontends** — CLI, Python bindings (``pyshifty``), and a browser-native WebAssembly module

Quick start
-----------

**CLI** — validate a data graph against SHACL shapes:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl

Run SHACL-AF rules to a fixed point and print the inferred triples:

.. code-block:: bash

   shifty infer --shapes rules.ttl --data data.ttl

**Python** — validate with a pyshacl-compatible interface:

.. code-block:: python

   import shifty

   conforms, report_graph, results_text = shifty.validate(data, shapes)

Run inference and retrieve the extended graph:

.. code-block:: python

   result = shifty.infer(data, rules)
   g = result.graph()           # rdflib.Graph with original + inferred triples

**Browser** — `open the live playground <https://shifty.gtf.fyi/playground/>`_ to run
validation and inference entirely in your browser with no installation required.

Contents
--------

.. toctree::
   :maxdepth: 2

   getting-started
   cli/index
   python-api/index
   playground
   Rust API (docs.rs) <https://docs.rs/shifty-engine>
