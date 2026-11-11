SHACL conformance
=================

Shifty passes all 120 approved cases in the pinned W3C SHACL 1.0 Core/SPARQL
suite. One proposed case also passes and is counted separately.

Recorded results
----------------

This report was produced on **2026-11-11** using **0.6.0-alpha.4** at commit
`aaaea85 <https://github.com/gtfierro/shifty/commit/aaaea85bd406021f1fd9056a53d5dca7c3b9a53c>`_.

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

Both the W3C reports and Shifty's structured validation results passed.
The CLI and Python file-input reports also matched across all 121 cases,
including seven tests that expect a validation failure diagnostic.

- :download:`EARL implementation report <../conformance-results/aaaea85/shifty-earl.ttl>`
- :download:`Per-test results and run metadata (JSON) <../conformance-results/aaaea85/results.json>`
- :download:`CLI/Python parity results (JSON) <../conformance-results/aaaea85/parity.json>`
- :download:`Report checksums <../conformance-results/aaaea85/SHA256SUMS>`

The `CI run <https://github.com/gtfierro/shifty/actions/runs/43076972974>`_
produced these files from a clean checkout. `Earlier report snapshots
<https://github.com/gtfierro/shifty/tree/main/conformance-results>`_ are also
retained.

What this checks
----------------

The runner uses W3C suite revision
``40231dfb97c5b606d22ae733947c0d14b4f45c07``. It loads each case's declared
shapes and data, disables inference, and treats unsupported features as
errors. Actual and expected reports are compared using the suite's report
comparison rules; process-local blank-node labels are compared by graph
isomorphism.

Passing covers the cases in this suite, rather than every possible use of
SHACL. The suite does not test SHACL-AF, SHACL-JS, or SHACL 1.2. See
:doc:`reference/feature-support` for support and limitations beyond these
tests. These are implementation results, not a W3C endorsement.

To reproduce the report, follow :doc:`how-to/conformance`. The
`conformance workflow <https://github.com/gtfierro/shifty/actions/workflows/conformance.yml>`_
runs on changes to ``main`` and on pull requests.
