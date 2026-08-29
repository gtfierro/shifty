Shifty
======

.. raw:: html

   <div class="sh-hero">
     <p class="sh-tagline">
       A SHACL validation and SHACL-AF inference engine, built on a compiled
       algebra rather than an interpreter over the shapes graph.
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

Shifty does two things: it **validates** RDF graphs against SHACL shapes, and it
runs **SHACL-AF inference** — ``sh:rule`` entries forward-chained to a fixed
point. Both are driven by the same compiled representation, so a constraint has
one meaning in the system rather than one per feature.

Rather than interpret the shapes graph at validation time, Shifty lowers SHACL
to a path algebra (π) and a shape grammar (φ) taken from `Common Foundations for
SHACL, ShEx, and PG-Schema <https://arxiv.org/abs/2502.01295>`_, normalizes it,
and plans it. SHACL's vocabulary is much larger than its semantics; reducing
dozens of constraint components to a handful of operators is what makes the
optimizer, the inference engine, and the result formats below tractable to write
once each.

- **Full SHACL Core validation** — node and property shapes, all standard
  constraint components, the full property-path language.
- **SHACL-AF inference** — triple rules and SPARQL CONSTRUCT rules to a fixed
  point, with stratification analysis for recursive rulesets.
- **Recursion with a defined semantics** — cyclic shape references are
  evaluated in strata; a schema whose recursion runs through a negation is
  diagnosed and refused rather than guessed at.
- **Multiple frontends** — CLI, Python (``pyshifty``), a C++17 static library,
  and a WebAssembly module that runs in the browser.

Results you can act on
----------------------

Validation returns a W3C ``sh:ValidationReport`` when you want interoperability.
When you want to *do* something with the result, ``validate_algebra`` returns
the same findings as structured objects — no RDF graph to query, and a stable
constraint kind to branch on:

.. code-block:: python

   result = shifty.validate_algebra(data, shapes)

   for violation in result.violations:
       print(violation.focus_node, violation.severity)
       for reason in violation.reasons:
           print(" ", reason.constraint_kind)   # ConstraintKind.Cardinality
           print(" ", reason.path)              # <http://example.org/email>
           print(" ", reason.value)             # the offending value node
           print(" ", reason.message)           # engine-generated description
           print(" ", reason.author_message)    # your sh:message, if any

Each reason also carries the algebra node that produced it, so you can see the
compiled constraint behind a finding rather than reverse-engineering it from
prose. :doc:`tutorials/reading-results` walks through this.

Going further, the :doc:`evidence interface <reference/evidence>` keeps the
whole derivation instead of discarding it — which nodes a shape actually
selected, why each one passed, and which triples supported it. That answers
questions a validation report structurally cannot, such as "did this profile
apply to anything?" and "which sensor satisfied this obligation?"

Where to start
--------------

.. list-table::
   :widths: 25 75

   * - :doc:`Tutorials <tutorials/index>`
     - Start here if you are new. Three lessons: get a validation running,
       walk its results in code, then ask the engine why each node passed or
       failed.
   * - :doc:`How-to guides <how-to/index>`
     - Recipes for a specific job: run inference, extract bindings, inspect
       the compiled plan.
   * - :doc:`Reference <reference/index>`
     - Exact behaviour of the CLI flags, the Python API, the evidence data
       model, and the supported SHACL feature set.
   * - :doc:`Explanation <explanation/index>`
     - Why the engine is built this way — the algebra, the recursion
       semantics, and what the measurements say things cost.

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

Run rules to a fixed point:

.. code-block:: bash

   shifty infer --shapes rules.ttl --data data.ttl

From Python, with a ``pyshacl``-compatible signature:

.. code-block:: python

   import shifty

   conforms, report_graph, results_text = shifty.validate(data, shapes)

And in the browser: the `playground <https://shifty.gtf.fyi/playground/>`_ runs
the whole engine as WebAssembly, so nothing you paste into it leaves your
machine.

.. note::

   Shifty also has an **experimental** symbolic repair layer, which computes the
   space of edits that would make a failing node conform. It is early and its
   API is expected to change; see :doc:`how-to/repair` if you want to try it.

.. toctree::
   :maxdepth: 2
   :hidden:

   tutorials/index
   how-to/index
   reference/index
   explanation/index
   Rust API (docs.rs) <https://docs.rs/shifty-engine>
