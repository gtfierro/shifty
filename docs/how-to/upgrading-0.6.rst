Upgrading to 0.6
================

The changes below apply when upgrading from 0.5.x.

Rust API changes in 0.6.1
--------------------------

Version 0.6.1 includes source and serialized-IR compatibility changes for Rust
consumers that construct or persist public engine types:

* ``Rule.order`` is now ``Option<shifty_algebra::Order>`` instead of
  ``Option<i64>``. Use ``Some(1_i64.into())`` for integer orders or
  ``Order::try_from("0.5".to_owned())`` for decimal orders. Serialized orders
  are decimal strings, so stored numeric orders must be converted to strings.
* ``Rule`` adds ``owner`` and ``shape_order``. Use ``None`` when constructing
  rules without an authored owning shape or shape order.
* ``SparqlConstraint`` adds ``annotations``; ``SparqlTarget`` adds
  ``extra_bindings``. Use empty vectors when these features are unused.
* ``Reason`` and ``ValidationResult`` add ``annotations``. Evidence variants
  ``Witness::Opaque`` and ``Failure::Opaque`` add ``result_annotations``;
  their ``Not`` variants add ``annotations``. Update struct literals and
  exhaustive variant patterns accordingly. New annotation fields default to
  empty when deserializing older results.
* ``SparqlOperation`` adds ``Function`` for function invocation failures.
  Update exhaustive matches on this enum.

Shapes execute by shape order, then by rule order within each shape. Equal
orders share a snapshot. Guarded rules can therefore produce different
inferred graphs than under the earlier global rule ordering. Function
parameters also support decimal orders. Graph-reading functions now execute
under either unsupported-feature policy; their failures remain execution errors.

Unsupported features fail by default
------------------------------------

Validation and inference now reject partially supported features. To retain
best effort explicitly, use the policy for your interface:

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
     - ``EngineOptions { unsupported: UnsupportedPolicy::Ignore, ..Default::default() }``
   * - C++ validation
     - ``options.on_unsupported = shifty::UnsupportedPolicy::Ignore``

Best effort may omit unsupported constraints. It does not suppress malformed
shapes or failures to execute a query. Graph-reading SHACL functions, including
calls between function bodies, now use the current data/shapes snapshot under
either policy.

Repair moved to ``shifty.experimental``
---------------------------------------

Starting with alpha.3, the repair API lives in ``shifty.experimental``.
Symbolic repair was always experimental. Its Python API now says so in the
import:

.. code-block:: python

   # 0.5
   session = shifty.RepairSession(shapes, data)

   # 0.6
   from shifty.experimental import repair

   session = repair.RepairSession(shapes, data)

This covers ``RepairSession``, ``RepairPlan``, ``RepairTree``,
``RepairOrigin``, ``RepairOutcome``, ``Hole``, ``Choice``, ``ChoiceKind``,
and ``Instantiated``. The old names raise ``AttributeError`` with the new
import.

The evidence types stay where they were: ``Failure``, ``Satisfaction``,
``Target``, and their atoms and kinds. So do ``RepairDelta`` and
``delta_from_graph``.

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

Malformed inputs and shapes raise ``ValueError``. Execution failures in
inference targets, rule conditions, node filters, repair gates, and witness
operations raise ``ValidationFailure``. Failed inference stops before a
snapshot is returned or written back.

Lazy shape-map value extraction can also raise an execution error after
validation succeeds, when it evaluates a qualifier that validation simplified
away.

Rust callers
~~~~~~~~~~~~

Session methods ``validate``, ``report``, ``evidence``, ``conformance``,
``find_failures``, and ``property_witnesses`` now return ``Result``. Prepared
evidence evaluation and standalone report functions are also fallible.
Propagate errors with ``?``.

- ``EvaluationError::Sparql`` carries the operation and diagnostic.
- Standalone algebra validation, witness, and repair-gate functions return
  ``ValidationError``, distinguishing ``NonStratifiable`` and ``Evaluation``
  failures.
- ``PreparedEvidenceValidator::raw_constraint_holds`` returns
  ``Result<Option<bool>, EvaluationError>``.

JavaScript and C++ callers
~~~~~~~~~~~~~~~~~~~~~~~~~~

JavaScript throws an ``Error``. The C API returns
``SHIFTY_STATUS_VALIDATION_ERROR``, and C++ throws ``shifty::Error`` with that
status. The C ABI version is 7. Rebuild C++ applications against the new headers
and library.

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
