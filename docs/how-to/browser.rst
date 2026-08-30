Run Shifty in the browser
=========================

The whole engine compiles to WebAssembly, so validation and inference run
inside the browser tab. There is no server component, which also means nothing
you paste in is uploaded anywhere.

.. raw:: html

   <div class="sh-playground-launch">
     <p>
       Paste Turtle into the editor, upload a file, or load the built-in sample.
     </p>
     <a href="https://shifty.gtf.fyi/playground/" class="sh-launch-btn">Open Playground ↗</a>
   </div>

Use the hosted playground
-------------------------

**Validate** — supply a shapes graph and a data graph and press *Validate*.
Results are grouped by severity, with expandable rows showing the focus node,
the failing property path, and the offending value.

**Infer** — run ``sh:rule`` entries to a fixed point. You get a count of derived
triples and can download a Turtle file containing the original graph plus
everything derived.

**Advanced options** — the same knobs as the other frontends:

- *Run inference* — apply rules before validating (on by default).
- *Graph mode* — ``data``, ``union``, or ``union-all``; see
  :doc:`../explanation/shapes-and-data`.
- *Minimum severity* — which severities make the run non-conforming.
- *Sort results* — deterministic ordering by severity, focus node, and
  constraint.

**Files** — uploads are kept in the browser's IndexedDB, so you can switch
between graphs without re-uploading. The *Files* button in the toolbar manages
the cache.

The playground is the fastest way to check a hypothesis about SHACL semantics
without touching your local environment, and a reasonable way to hand someone a
reproducible example: the input is small enough to paste.

Build it locally
----------------

The playground needs the compiled WebAssembly module:

.. code-block:: bash

   # requires wasm-pack: https://rustwasm.github.io/wasm-pack/
   ./crates/shifty-wasm/build.sh

   python3 -m http.server -d crates/shifty-wasm
   # open http://localhost:8000/example/

Embed it in your own page
-------------------------

``crates/shifty-wasm/README.md`` documents the JavaScript API and the embedding
details.

The module contains the full engine, which increases the initial download. It
also runs in the tab's memory, so a shapes closure of a few hundred thousand
triples against a large data graph reaches browser memory limits before the
native engine. The browser build is intended for interactive-sized inputs.
