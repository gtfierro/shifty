Install Shifty
==============

Shifty ships through five interfaces over one engine. Pick by how you intend to call
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

Install the extra when using the W3C report interface or graph-returning APIs:

.. code-block:: bash

   pip install "pyshifty[rdflib]"

To build from a checkout instead, which you need if you are changing the Rust:

.. code-block:: bash

   git clone https://github.com/gtfierro/shifty
   cd shifty/python
   uv sync --dev --frozen --reinstall-package pyshifty

The locked development environment builds the editable extension. Re-run this
command after changing Rust sources; a plain ``uv sync`` leaves the previously
compiled extension in place. See :doc:`contributing` for the quality checks.

Command line
------------

Download a CLI archive for Linux x86-64, Windows x86-64, or macOS arm64 from
`GitHub Releases <https://github.com/gtfierro/shifty/releases>`_. Extract the
``shifty`` executable (``shifty.exe`` on Windows) and put it on your ``PATH``.
For other platforms, or to build from a checkout, use Rust:

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

The hosted playground needs no installation at all — see
:doc:`playground`.

To build the module yourself, for embedding in your own page — see
:doc:`how-to/browser` for the JavaScript API:

.. code-block:: bash

   # requires wasm32-unknown-unknown and wasm-bindgen-cli (see the crate README)
   ./crates/shifty-wasm/build.sh

   python3 -m http.server -d crates/shifty-wasm
   # then open http://localhost:8000/example/

``crates/shifty-wasm/README.md`` documents the JavaScript API.

C++
---

A C++17 static library lives in ``cpp/``, built with CMake, exposing the full
SDK — dataset and SPARQL, validation, evidence-carrying validation, and the
shape-map vocabulary. See :doc:`reference/cpp` for the API and
``cpp/README.md`` for build instructions.

Rust
----

.. code-block:: toml

   [dependencies]
   shifty-engine = "0.5.0-alpha.3"

The engine crate is the same one every frontend wraps; its API documentation is
on `docs.rs/shifty-engine <https://docs.rs/shifty-engine>`_. The workspace also
publishes ``shifty-algebra`` (the IR), ``shifty-parse``, ``shifty-opt`` (the
normalizer and planner), and ``shifty-repair``.
