Browser Playground
==================

.. raw:: html

   <div class="sh-section-intro">
     The Shifty playground runs the full inference and validation pipeline entirely
     in your browser via WebAssembly — no server, no installation, no round-trips.
     Your data stays on your device.
   </div>

.. raw:: html

   <div class="sh-playground-launch">
     <p>
       Paste Turtle directly into the editor, upload a file, or load the built-in
       sample to try it out immediately.
     </p>
     <a href="https://shifty.gtf.fyi/playground/" class="sh-launch-btn">Open Playground ↗</a>
   </div>

What the playground supports
-----------------------------

**Validate** — paste or upload a shapes graph and a data graph, then click
*Validate*. Results are grouped by severity (violation / warning / info) with
expandable detail rows showing the focus node, failing property path, and
offending value.

**Infer** — run SHACL-AF ``sh:rule`` entries to a fixed point. The inferred
triples are shown as a count and can be downloaded as a Turtle file containing
the original graph plus all derived triples.

**Options** — the *Advanced options* panel exposes:

- **Run inference** — apply ``sh:rule`` entries before validating (on by default)
- **Graph mode** — ``data``, ``union``, or ``union-all`` (controls which triples are visible to path traversal)
- **Minimum severity** — filter which severity levels cause non-conformance
- **Sort results** — deterministic ordering by severity, focus node, and constraint

**File cache** — uploaded files are stored in your browser's IndexedDB so you
can switch between them without re-uploading. The file cache manager is
accessible via the *Files* button in the toolbar.

Building the playground locally
--------------------------------

The playground requires the compiled WebAssembly module. After cloning:

.. code-block:: bash

   # Requires wasm-pack (https://rustwasm.github.io/wasm-pack/)
   ./crates/shifty-wasm/build.sh

   # Serve the example:
   python3 -m http.server -d crates/shifty-wasm
   # Open http://localhost:8000/example/

See ``crates/shifty-wasm/README.md`` for the full JS API and embedding details.
