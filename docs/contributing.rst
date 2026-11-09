Contributing
============

Code contributions must pass the checks below. Documentation contributions
follow the page conventions and include a Sphinx build.

Develop Shifty
--------------

Rust changes run the same gates CI does, from the repository root:

.. code-block:: bash

   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets --all-features -- -D warnings
   cargo test --workspace

Python changes work from ``python/`` against the locked development
environment:

.. code-block:: bash

   uv sync --dev --frozen --reinstall-package pyshifty
   uv run ruff check .
   uv run ruff format --check .
   uv run ty check shifty
   uv run pytest -q

Use ``--reinstall-package pyshifty`` whenever Rust sources have changed.
The extension is an editable build, and a plain ``uv sync`` does not rebuild
it. Without reinstalling, ``pytest`` tests the previously compiled ``_shifty``
shared object, so its results may not reflect the current source. CI builds
from a fresh checkout.

Fix diagnostics rather than weakening or skipping a check, unless a documented
compatibility constraint requires a narrow exception.

Run the pinned W3C suite with the procedure in :doc:`how-to/conformance`.
Preserve upstream fixtures unchanged. Compiler and algorithm descriptions
belong in the repository's ``development_docs/`` directory; public pages
should explain behavior, usage, results, and support boundaries.

The C++ SDK and the WebAssembly module have their own gates:

.. code-block:: bash

   cmake -S cpp -B build/cpp -DSHIFTY_CPP_BUILD_TESTS=ON
   cmake --build build/cpp
   ctest --test-dir build/cpp --output-on-failure

Prepare a Python source archive
-------------------------------

From ``python/``, use the locked Python 3.12 development environment:

.. code-block:: bash

   uv run python tools/build_sdist.py --out ../target/sdist --verify

The helper builds with Maturin, then prunes the staged Cargo lockfile to match
the archive's smaller workspace. Retained package versions and checksums must
match the original lockfile. The checkout's lockfile is unchanged.
``--verify`` rebuilds a wheel from a fresh extraction offline with ``--locked``.
The Python CI and release jobs use this command. Calling ``maturin sdist``
directly skips lockfile preparation.

Prepare a release
-----------------

Use the Cargo prerelease spelling for the workspace version, internal crate
dependencies, changelog heading, and Git tag. For the second 0.6 alpha these are
``0.6.0-alpha.2`` and ``v0.6.0-alpha.2``. Maturin writes the Python distribution
version as ``0.6.0a2``; the Python runtime's ``shifty.version()`` continues to
report the Rust version.

Run the Rust, Python, SDK, documentation, and pinned conformance checks above,
then build and verify the Python source archive. Confirm that a wheel installed
outside the checkout reports the expected version and can validate and infer.
Keep generated release artifacts under ``target/``.

Commit the release sources before tagging. Pushing a ``v*.*.*`` tag starts the
release workflow: it checks that the tag matches Cargo and the changelog, builds
CLI binaries and Python distributions, and smoke-tests wheels on Python 3.9 and
3.14. After those checks pass it creates the GitHub release and publishes to
PyPI. Tags containing a hyphen produce a GitHub prerelease and skip crates.io
publication. Publishing uses the repository's ``publish`` and ``deploy``
environments. Treat pushing the tag as the publication step; prepare and review
the release commit first.

Contribute to the documentation
-------------------------------

Shifty's documentation follows Diátaxis. Choose the page type based on what
the reader needs to do:

Choose the page type
~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 20 35 45
   :header-rows: 1

   * - Type
     - Reader's need
     - Writing rule
   * - Tutorial
     - Learn through a successful first experience.
     - Lead the reader through a complete sequence. Explain only what the next
       step requires, and include checkpoints with expected output.
   * - How-to guide
     - Accomplish a known, concrete task.
     - State the outcome first, list prerequisites, and give the shortest
       reliable procedure. Link elsewhere for theory.
   * - Reference
     - Look up exact behavior while working.
     - Document signatures or syntax, stability, arguments and defaults,
       returns or output, errors, a minimal example, and related operations.
   * - Explanation
     - Understand why the system behaves or is designed this way.
     - Develop the reasoning, alternatives, and tradeoffs. Do not turn the
       page into a procedure or an API inventory.

Page conventions
~~~~~~~~~~~~~~~~

- Give each page one primary reader goal and use that goal in its opening.
- State behavior directly and explain it with a concrete example when useful.
  Avoid slogans, invented anecdotes, and asides about what the reader should
  find surprising or important. Use ``docs.brickschema.org`` and
  ``gtf.fyi/posts`` as style references.
- Put exact defaults, accepted values, failure behavior, and stability in the
  reference page—not only in examples or source comments.
- Link to related capabilities and useful next steps. When a tutorial or
  how-to guide introduces an interface, include a forward pointer to its
  reference page for exact fields, arguments, and behavior. Prefer links over
  repeating an explanation in several places.
- Mark experimental interfaces wherever a reader first encounters them.
- Use a figure only when it makes a relationship, sequence, or state change
  clearer than a short paragraph or table. Keep figures as scalable SVGs in
  ``docs/_static``, add SVG ``title`` and ``desc`` elements, and supply useful
  alternative text in the Sphinx ``figure`` directive.

Executable examples
~~~~~~~~~~~~~~~~~~~

Key examples live in ``docs/examples`` as runnable files. Include their source
with ``literalinclude`` and their output with ``program-output`` so the page
cannot show code from one program and output from another. Keep these examples
offline, deterministic, and fast. Sort values before printing if an API does
not guarantee their order.

Use ordinary ``code-block`` directives for fragments, invalid-input examples,
installation commands, network access, and operations that are too expensive
or environment-dependent to run during every documentation build.

Preview your changes
~~~~~~~~~~~~~~~~~~~~

From the repository root:

.. code-block:: bash

   cd docs
   make html SPHINXOPTS="-W --keep-going"

The normal build does not rewrite benchmark source data. To regenerate the
benchmark chart deliberately before building, set
``SHIFTY_REGENERATE_BENCHMARKS=1``.

Every rendered page has an **Edit this page** link to its source on GitHub. For
a change that needs discussion first, `open an issue
<https://github.com/gtfierro/shifty/issues/new>`_.
