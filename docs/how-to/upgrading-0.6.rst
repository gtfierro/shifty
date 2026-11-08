Upgrading to 0.6
================

Unsupported features fail by default
------------------------------------

Validation and inference now reject partially supported features. This includes
calls to graph-reading SHACL functions from SPARQL queries. To retain best effort
explicitly, use the policy for your interface:

.. list-table::
   :header-rows: 1

   * - Interface
     - Compatibility option
   * - Python
     - ``on_unsupported="ignore"``
   * - CLI validation
     - ``--on-unsupported ignore``
   * - JavaScript
     - ``{ onUnsupported: "ignore" }``
   * - Rust
     - ``EngineOptions { unsupported: UnsupportedPolicy::Ignore }``
   * - C++ validation
     - ``options.on_unsupported = shifty::UnsupportedPolicy::Ignore``

Best effort may omit constraints or evaluate a function over an empty dataset.
It does not suppress malformed shapes or failures to execute a query.

Execution failures have no validation verdict
---------------------------------------------

A completed run can conform or report constraint violations. A query that could
not execute supplies neither verdict. In Python, catch that case separately:

.. code-block:: python

   try:
       conforms, report, text = shifty.validate(data, shapes)
   except shifty.ValidationFailure as error:
       # No validation report is available.
       raise RuntimeError("Validation could not complete") from error

Malformed inputs and shapes still raise ``ValueError``. Session admission and
inference errors also reject the call. Failed inference targets, rule conditions,
and node filters raise ``ValidationFailure`` before an inferred snapshot is
returned or written back. Repair gates and witness operations raise the same
exception for query execution failures. JavaScript throws an ``Error``; C returns
``SHIFTY_STATUS_VALIDATION_ERROR`` and C++ throws ``shifty::Error`` with that
status. The C ABI version is now 7; rebuild C++ applications against the new
headers and library.

Rust session methods ``validate``, ``report``, ``evidence``, ``conformance``,
``find_failures``, and ``property_witnesses`` now return ``Result``. Prepared
evidence evaluation and standalone report functions are also fallible. Propagate
errors with ``?``. Match ``EvaluationError::Sparql`` to inspect the operation and
diagnostic. Standalone algebra validation returns ``ValidationError``, whose
variants separate ``NonStratifiable`` from ``Evaluation`` failures. Standalone
witness and repair-gate functions propagate the same error type.

Construct Rust reports through the API
--------------------------------------

``ValidationResult`` and ``ValidationReport`` are non-exhaustive. Replace struct
literals with their constructors:

.. code-block:: rust

   let mut result = ValidationResult::new(focus, component, source_shape);
   result.value = Some(value);
   let report = ValidationReport::new(false, vec![result]);

The report's conformance flag is explicit because severity policy can permit
findings in a conforming report. Use accessors such as ``results()`` and
``source_constraint()`` to read output. Existing public result fields remain
available. Complex-path descriptions are private serialization context;
``path_triples()`` borrows them and ``with_path_triples(graph)`` supplies them for
a report constructed by your application. Destructuring must include ``..``.

Browser validation defaults match Python and CLI
------------------------------------------------

The default graph mode is now ``"union"``: ontology triples in the shapes graph
are visible during evaluation, while focus discovery uses the data graph.
Pass ``{ graphMode: "data" }`` to retain the previous graph visibility.

Omitted options, ``undefined``, ``null``, and ``{}`` all enable inference and
sorting. Pass ``infer: false`` or ``sortResults: false`` explicitly to disable
either. Standalone ``infer`` now accepts an optional third options argument for
``onUnsupported``.
