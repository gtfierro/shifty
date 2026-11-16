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

The current engine deliberately follows SHACL 1.2's narrower ``VALUES``
restriction. The 1.0 case ``sparql/pre-binding/unsupported-sparql-002`` therefore
reports ``failed``: it expects a blanket rejection of ``VALUES ?any``. The CI
baseline records this difference without changing the upstream fixture or
counting it as a SHACL 1.0 pass.

Run the SHACL-AF regressions
-----------------------------

From the repository root:

.. code-block:: bash

   cargo test -p shifty-engine --test w3c_advanced -- --nocapture

The locally adapted AF/DASH inventory covers 7 inference cases, 97 validation
cases, and 2 function cases. Four additional project fixtures cover function
expressions, value-node scope, optional arguments, path inputs, and fresh blank
nodes. These regressions are separate from the pinned SHACL 1.0 implementation
report.

The harness enforces its fixture inventory and category counts. Unexpected
diagnostics and execution failures fail the test, rather than reducing the
number of cases executed. Inference compares the complete derived delta,
including unexpected triples and blank-node structure. Validation checks
severity, expected authored messages and source constraints as well as the
usual result fields. SHACL-JS remains outside this profile.

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
