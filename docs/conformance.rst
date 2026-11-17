SHACL conformance
=================

Shifty is tested against the official W3C SHACL 1.0 test suite.

Recorded results
----------------

Version **0.6.0-alpha.4**, commit
`aaaea85 <https://github.com/gtfierro/shifty/commit/aaaea85bd406021f1fd9056a53d5dca7c3b9a53c>`_,
run on **2026-11-11**:

.. list-table::
   :header-rows: 1
   :widths: 55 15 15 15

   * - Suite section
     - Passed
     - Failed
     - Errors
   * - SHACL Core (approved)
     - 98
     - 0
     - 0
   * - SHACL-SPARQL (approved)
     - 22
     - 0
     - 0
   * - SHACL-SPARQL (proposed)
     - 1
     - 0
     - 0

The CLI and Python reports also matched on all 121 cases.

- :download:`EARL implementation report <../conformance-results/aaaea85/shifty-earl.ttl>`
- :download:`Per-test results and run metadata (JSON) <../conformance-results/aaaea85/results.json>`
- :download:`CLI/Python parity results (JSON) <../conformance-results/aaaea85/parity.json>`
- :download:`Report checksums <../conformance-results/aaaea85/SHA256SUMS>`

These files come from a `CI run
<https://github.com/gtfierro/shifty/actions/runs/43076972974>`_ on a clean
checkout. `Earlier snapshots
<https://github.com/gtfierro/shifty/tree/main/conformance-results>`_ are kept
too.

One deliberate failure
----------------------

Since this run, one SHACL 1.0 test fails on purpose:
``sparql/pre-binding/unsupported-sparql-002``.

SHACL 1.0 rejects every ``VALUES`` clause in a SHACL-SPARQL query. SHACL 1.2
rejects it only when it binds a variable that could be pre-bound, such as
``$this``. Shifty follows 1.2, so it accepts this test's ``VALUES ?any``.

CI records this as an expected failure. The upstream test is left unchanged.

What this covers
----------------

- Each test runs with its own shapes and data, with inference off.
  Unsupported features count as errors.
- Reports are compared using the suite's own rules.
- The suite is pinned to W3C revision
  ``40231dfb97c5b606d22ae733947c0d14b4f45c07``.

It does not test SHACL-AF, SHACL-JS, or SHACL 1.2. See
:doc:`reference/feature-support` for those. These are Shifty's own results,
not a W3C endorsement.

To run the suite yourself, see :doc:`how-to/conformance`. The `conformance
workflow <https://github.com/gtfierro/shifty/actions/workflows/conformance.yml>`_
runs it on every pull request and every change to ``main``.
