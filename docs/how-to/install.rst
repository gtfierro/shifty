Install Shifty
==============

Shifty ships as four frontends over one engine. Pick by how you intend to call
it.

Python
------

.. code-block:: bash

   pip install pyshifty

Wheels on PyPI carry a pre-compiled engine, so no Rust toolchain is needed.
Python 3.9 or newer. The distribution is ``pyshifty``; the module is
``shifty``.

``rdflib`` is an optional dependency. The core validation and evidence paths do
not need it, but anything returning an ``rdflib.Graph`` — ``validate()``,
``InferResult.graph()``, ``RepairSession.apply()`` — imports it on demand and
will raise ``ModuleNotFoundError`` if it is absent.

To build from a checkout instead, which you need if you are changing the Rust:

.. code-block:: bash

   git clone https://github.com/gtfierro/shifty
   cd shifty/python
   pip install maturin
   maturin develop --release

Leave off ``--release`` for a faster compile and a much slower engine. The
difference is large enough that a debug build is not worth benchmarking.

Command line
------------

There are no pre-built binaries yet, so the CLI needs a Rust toolchain:

.. code-block:: bash

   git clone https://github.com/gtfierro/shifty
   cd shifty
   cargo install --path crates/shifty-cli

Or build without installing:

.. code-block:: bash

   cargo build --release -p shifty-cli
   ./target/release/shifty --help

Check what you have with ``shifty version``.

Browser / WebAssembly
---------------------

The hosted `playground <https://shifty.gtf.fyi/playground/>`_ needs no
installation at all — see :doc:`browser`.

To build the module yourself, for embedding in your own page:

.. code-block:: bash

   # requires wasm-pack: https://rustwasm.github.io/wasm-pack/
   ./crates/shifty-wasm/build.sh

   python3 -m http.server -d crates/shifty-wasm
   # then open http://localhost:8000/example/

``crates/shifty-wasm/README.md`` documents the JavaScript API.

C++
---

A C++17 static library lives in ``cpp/``, built with CMake, exposing the full
SDK — dataset and SPARQL, validation, evidence-carrying validation, and the
shape-map vocabulary. See :doc:`../reference/cpp` for the API and
``cpp/README.md`` for build instructions.

Rust
----

.. code-block:: toml

   [dependencies]
   shifty-engine = "0.3"

The engine crate is the same one every frontend wraps; its API documentation is
on `docs.rs/shifty-engine <https://docs.rs/shifty-engine>`_. The workspace also
publishes ``shifty-algebra`` (the IR), ``shifty-parse``, ``shifty-opt`` (the
normalizer and planner), and ``shifty-repair``.
