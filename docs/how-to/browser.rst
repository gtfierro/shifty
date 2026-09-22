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

   # requires the wasm32 target and wasm-bindgen-cli version in Cargo.lock
   ./crates/shifty-wasm/build.sh

   python3 -m http.server -d crates/shifty-wasm
   # open http://localhost:8000/example/

Embed it in your own page
-------------------------

Serve the generated ``pkg/`` directory beside a page with a module script:

.. code-block:: html

   <script type="module">
     import init, { validate } from "./pkg/shifty_wasm.js";

     await init();
     const shapes = `@prefix sh: <http://www.w3.org/ns/shacl#> .
       @prefix ex: <http://example.org/> .
       ex:PersonShape a sh:NodeShape ; sh:targetClass ex:Person ;
         sh:property [ sh:path ex:name ; sh:minCount 1 ] .`;
     const data = `@prefix ex: <http://example.org/> .
       ex:bob a ex:Person .`;
     const result = validate(shapes, data, { graphMode: "data" });
     console.log(result.conforms, result.violations);
   </script>

``graphMode`` defaults to ``"data"`` in JavaScript. Pass ``"union"`` when
validation should also see ontology triples in the shapes graph. The
`crate README <https://github.com/gtfierro/shifty/blob/main/crates/shifty-wasm/README.md>`_
covers the remaining options and build prerequisites. The other exports are:

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

``diagnostics`` contains non-fatal unsupported-feature and rule-execution
messages. Invalid shapes, including malformed or unresolved SPARQL prefixes,
reject the call.

Limits
------

An embedded module carries the same download size and memory ceiling as the
hosted build; :doc:`../playground` describes both.
