Run the W3C SHACL suite
=======================

From a checkout with Rust and uv installed:

.. code-block:: bash

   cd python
   uv sync --dev --frozen --reinstall-package pyshifty
   uv run python -m conformance --include-unlisted

This runs 120 approved SHACL 1.0 Core/SPARQL tests and one proposed test.
The proposed test is reported separately. SHACL-AF, SHACL-JS, and SHACL 1.2
are outside this suite.

The runner loads the data and shapes specified by each manifest and compares
Shifty's report with the expected report using the `W3C test process
<https://w3c.github.io/data-shapes/data-shapes-test-suite/>`_. Inference is
disabled and unsupported features are errors. Passing these tests covers the
cases in this suite; it does not prove complete specification conformance.

Run a section or one test
-------------------------

.. code-block:: bash

   uv run python -m conformance --section core
   uv run python -m conformance --section sparql
   uv run python -m conformance --test urn:x-shacl-test:/core/misc/message-001

Read the results
----------------

Output goes to ``target/conformance/`` at the repository root:

* ``results.json`` — per-test results, diagnostics, versions, and run settings.
* ``shifty-earl.ttl`` — the EARL implementation report. Use ``--assertor`` to
  set the IRI of the person or organization making the report.
* ``cases/`` — expected and actual reports, with differences, for mismatches.

Use ``--output PATH`` to keep separate runs. Old diagnostic files are retained.
RDFLib may warn about deliberately invalid literals in the test data.

``passed`` means the report matched or Shifty diagnosed an expected
SHACL-SPARQL failure. ``partial`` means only the conformance boolean matched.
``failed`` means conformance differed or an expected failure was missing.
Loading errors and unexpected exceptions are ``error``; rejected tests are
``untested``.

The exit code is 0 for passes, 1 for failed or partial results, and 2 for
runner errors. ``--report-only`` allows failed and partial results.

Run the CI check
----------------

.. code-block:: bash

   uv run python -m conformance --include-unlisted \
     --baseline ../testdata/w3c-shacl/baseline.json

Use the baseline with the full suite. It checks the test inventory, upstream
revision, and report and algebra results for regressions. Fixture sources and
update instructions are in `testdata/README.md
<https://github.com/gtfierro/shifty/blob/main/testdata/README.md>`_.
