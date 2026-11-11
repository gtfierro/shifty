Run the W3C SHACL suite
=======================

See :doc:`../conformance` for the recorded results and downloadable reports.

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

The exit code is 0 for passes, 1 for failed or partial results or an algebra
mismatch, and 2 for runner or algebra exceptions. ``--report-only`` allows
failed and partial results and algebra mismatches; exceptions still exit 2.

Run the CI check
----------------

.. code-block:: bash

   uv run python -m conformance --include-unlisted \
     --baseline ../testdata/w3c-shacl/baseline.json

Use the baseline with the full suite. It checks the test inventory, upstream
revision, and report and algebra results for regressions. Fixture sources and
update instructions are in `testdata/README.md
<https://github.com/gtfierro/shifty/blob/main/testdata/README.md>`_.

Compare CLI and Python reports
------------------------------

Build the CLI from the repository root, then run the parity check:

.. code-block:: bash

   cargo build --locked -p shifty-cli
   cd python
   uv run python -m conformance.parity --cli ../target/debug/shifty

This checks all 121 cases through native file inputs on both interfaces.
Reports are compared to the expected RDF by graph isomorphism; blank-node
labels are local to each process. Both runs disable inference, use the data
graph, and reject unsupported features. Results go to
``target/interface-parity/parity.json``. Use ``--test`` for one case or
``--output`` for another results directory. Timeouts and crashes fail the run.

Verified report snapshots are kept in `conformance-results/
<https://github.com/gtfierro/shifty/tree/main/conformance-results>`_.
