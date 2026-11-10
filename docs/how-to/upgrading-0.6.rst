Upgrading to 0.6
================

Try the latest alpha with ``python -m pip install "pyshifty[rdflib]==0.6.0a2"``.
The corresponding Rust and CLI version is ``0.6.0-alpha.2`` and the release tag
is ``v0.6.0-alpha.2``. The changes below apply when upgrading from 0.5.x.

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

Repair moved to ``shifty.experimental``
---------------------------------------

Symbolic repair was always experimental. Its Python API now says so in the
import:

.. code-block:: python

   # 0.5
   session = shifty.RepairSession(shapes, data)

   # 0.6
   from shifty.experimental import repair

   session = repair.RepairSession(shapes, data)

This covers ``RepairSession``, ``RepairPlan``, ``RepairTree``,
``RepairOutcome``, ``Failure``, ``Satisfaction``, and the types they return.
``RepairDelta`` and ``delta_from_graph`` stay in ``shifty``. The old names
raise ``AttributeError`` with the new import.

The deprecated ``FocusWitness`` and ``FocusSatisfaction`` aliases are gone.
Use ``Failure`` and ``Satisfaction``.

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
``PreparedEvidenceValidator::raw_constraint_holds`` returns
``Result<Option<bool>, EvaluationError>``. Lazy shape-map value extraction can
raise an execution error even after validation succeeds, when it evaluates an
authored qualifier that validation normalized away.

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

Shape-map kinds describe authored constraints
---------------------------------------------

Starting with alpha.2, ``Key.kind`` identifies the authored SHACL family. A
pattern binding has ``kind="pattern"`` and a datatype binding has
``kind="datatype"``. Ordinary
cardinalities remain ``"count"``; qualified cardinalities use
``"qualified_count"``. A binding combining families has ``kind="compound"``;
inspect ``binding.constraint_components`` for its full component IRIs.

Code inspecting compiler tags should use ``binding.algebra_kind``. Code
constructing typed keys must supply the new family, because ``kind`` is part
of equality and hashing:

.. code-block:: python

   key = shifty.Key(shifty.Pred("http://example.org/label"), None, kind="pattern")
   binding = mapping[key]

Prefer the keys returned by ``mapping.items()`` when possible. String lookup
continues to use the path display form; duplicate paths and qualifiers receive
distinct ordinals even when their authored families differ.

``binding.explain()`` now summarizes the authored SHACL constraints, status,
counts, and values. A pattern rejection is described as a pattern failure
rather than a count overflow. Use ``binding.explain(algebra=True)`` for the
previous evidence-tree rendering. Compound summaries list declarations;
they do not assert that every declared component failed.
