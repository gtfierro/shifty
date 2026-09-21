Build and embed the WebAssembly module
======================================

The whole engine compiles to WebAssembly, so validation and inference run
inside the browser tab, with no server component. :doc:`../playground` covers
the hosted build and what it can do; this page is about running that same
module yourself.

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
details. In outline:

.. list-table::
   :widths: 34 66
   :header-rows: 1

   * - Function
     - Returns
   * - ``validate(shapesRdf, dataRdf, options)``
     - ``{ conforms, violations, resultsText, diagnostics }`` — the structured
       algebra findings.
   * - ``validateW3c(shapesRdf, dataRdf, options)``
     - ``{ conforms, reportTurtle, resultsText, diagnostics }`` — a W3C
       ``sh:ValidationReport``.
   * - ``infer(shapesRdf, dataRdf)``
     - ``{ inferredCount, totalCount, graphNtriples, inferredNtriples,
       diagnostics }``.
   * - ``ntriplesToTurtle(ntriples)``
     - Re-serializes a graph held only as N-Triples, without re-running the
       engine.

``diagnostics`` is a string array of non-fatal unsupported-feature and
rule-execution diagnostics. All three evaluation functions return it; before
0.5 only ``infer()`` did, so a run whose rules silently did nothing was
indistinguishable from one whose rules had nothing to do. An invalid shapes
graph — including a malformed or unresolved SPARQL prefix — rejects the call
instead, and is never treated as though the affected constraint or rule were
absent.

Limits
------

An embedded module carries the same download size and memory ceiling as the
hosted build; :doc:`../playground` describes both.
