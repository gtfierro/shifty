Run the W3C SHACL suite
=======================

Check that a change hasn't broken conformance. Recorded results are on
:doc:`../conformance`.

Run the suite
-------------

You need Rust and uv. From the repository root:

.. code-block:: bash

   cd python
   uv sync --dev --frozen --reinstall-package pyshifty
   uv run python -m conformance --include-unlisted

This runs the 120 approved SHACL 1.0 tests. ``--include-unlisted`` adds the
one proposed test, which is reported separately.

RDFLib prints warnings about invalid literals. Some tests contain them on
purpose.

To run part of the suite:

.. code-block:: bash

   uv run python -m conformance --section core
   uv run python -m conformance --section sparql
   uv run python -m conformance --test urn:x-shacl-test:/core/misc/message-001

Read the results
----------------

Output goes to ``target/conformance/``. Use ``--output DIR`` to keep runs
apart.

- ``results.json``: the result of each test, plus versions and settings.
- ``shifty-earl.ttl``: the EARL report. ``--assertor`` sets who is making
  the report.
- ``cases/``: expected and actual reports for each mismatch.

Each test gets one outcome:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Outcome
     - Meaning
   * - ``passed``
     - The report matched, or Shifty rejected a query the test expects to be
       rejected.
   * - ``partial``
     - Only the ``sh:conforms`` value matched.
   * - ``failed``
     - The verdict was wrong, or an expected rejection didn't happen.
   * - ``error``
     - Loading or running the test crashed.
   * - ``untested``
     - The W3C rejected this test, so it isn't run.

Each test also reruns through ``shifty.validate_algebra``, the structured
results API. Its verdict is shown as ``[algebra: passed]``, ``failed``, or
``not-run``.

The exit code is:

- 0 if everything passed.
- 1 for any failed or partial result, or an algebra mismatch.
- 2 if anything crashed.

``--report-only`` ignores failures but still exits 2 on a crash.

Check against the baseline
--------------------------

CI uses this command:

.. code-block:: bash

   uv run python -m conformance --include-unlisted --baseline

``--baseline`` compares the run with ``testdata/w3c-shacl/baseline.json``,
the checked-in record of expected results. You can pass a different file as
``--baseline PATH``. The run fails if any test got worse or the set of tests
changed.

The console marks known failures as ``EXPECTED-FAILED``, with the reason
from the baseline. If a test now passes but the baseline says it fails, you
see ``UNEXPECTED-PASS``. Update the baseline if that change was intended.

These labels only appear in the console. ``results.json`` and the EARL report
always record outcomes against the official suite.

Compare the CLI and Python
--------------------------

The CLI and the Python package should produce the same reports. To check:

.. code-block:: bash

   cargo build --locked -p shifty-cli
   cd python
   uv run python -m conformance.parity --cli ../target/debug/shifty

This runs all 121 tests through both. Results go to
``target/interface-parity/parity.json``. ``--test`` and ``--output`` work as
above. A timeout or crash fails the run.

Run the SHACL-AF tests
----------------------

SHACL-AF (rules, functions, advanced targets) isn't part of the W3C suite. A
separate Rust test covers it, using locally adapted fixtures. From the
repository root:

.. code-block:: bash

   cargo test -p shifty-engine --test w3c_advanced -- --nocapture

It runs 7 inference, 97 validation, and 2 function cases, plus 8 Shifty
fixtures. It fails if the expected set of cases changes. Inference results
must match exactly, including any extra triples.

Update the fixtures
-------------------

Fixture sources and update steps are in `testdata/README.md
<https://github.com/gtfierro/shifty/blob/main/testdata/README.md>`_.
