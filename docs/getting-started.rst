Getting Started
===============

.. raw:: html

   <div class="sh-section-intro">
     Install Shifty via pip or cargo, then run your first validation or inference
     in under a minute — using the CLI, the Python library, or the browser playground.
   </div>

Installation
------------

Python (pip)
~~~~~~~~~~~~

Pre-built wheels are published to PyPI — no Rust toolchain required.

.. code-block:: bash

   pip install pyshifty

The package is published as ``pyshifty`` and imported as ``shifty``:

.. code-block:: python

   import shifty

To build from source (requires Rust and `maturin <https://github.com/PyO3/maturin>`_):

.. code-block:: bash

   git clone https://github.com/gtfierro/shifty
   cd shifty/python
   pip install maturin
   maturin develop --release

CLI (Cargo)
~~~~~~~~~~~

Build and install from source (requires a Rust toolchain):

.. code-block:: bash

   git clone https://github.com/gtfierro/shifty
   cd shifty
   cargo install --path crates/shifty-cli

Or build without installing:

.. code-block:: bash

   cargo build --release -p shifty-cli
   # binary at target/release/shifty

Browser / WebAssembly
~~~~~~~~~~~~~~~~~~~~~

The full inference and validation engine runs in WebAssembly — no server,
no round trips. Open the `live playground </playground/>`_ to use it directly
in your browser.

To build the WASM module from source:

.. code-block:: bash

   # requires wasm-pack and a Rust toolchain
   ./crates/shifty-wasm/build.sh
   python3 -m http.server -d crates/shifty-wasm   # open http://localhost:8000/example/

First steps
-----------

Validate
~~~~~~~~

Given a shapes file and a data file in Turtle format, run SHACL validation:

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl

You should see output like:

.. code-block:: text

   conforms: false
   violations: 1
     <http://example.org/bob>  [target: ∃ rdf:type .⊤]
         - (ex:name) 123 → expected datatype xsd:string

Infer
~~~~~

Run SHACL-AF rules to a fixed point and print the derived triples:

.. code-block:: bash

   shifty infer --shapes rules.ttl --data data.ttl

Output:

.. code-block:: text

   inferred 3 triple(s):
     <http://example.org/r1> <http://example.org/area> "6"^^<http://www.w3.org/2001/XMLSchema#integer>
     ...

Python quick check
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import shifty

   shapes = """
   @prefix sh:  <http://www.w3.org/ns/shacl#> .
   @prefix ex:  <http://example.org/> .
   @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

   ex:PersonShape a sh:NodeShape ;
       sh:targetClass ex:Person ;
       sh:property [
           sh:path ex:name ;
           sh:minCount 1 ;
           sh:datatype xsd:string ;
       ] .
   """

   data = """
   @prefix ex: <http://example.org/> .
   ex:Alice a ex:Person ; ex:name "Alice" .
   ex:Bob   a ex:Person .
   """

   conforms, report_graph, results_text = shifty.validate(data, shapes)
   print(results_text)
   # conforms: false
   # violations: 1
   #   ex:Bob — sh:minCount 1 on ex:name
