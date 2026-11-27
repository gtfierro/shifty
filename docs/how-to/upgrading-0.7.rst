Upgrading to 0.7
================

Shifty 0.7 changes public Rust types and some SHACL-AF execution behavior.
Python users can keep existing call signatures, but should review the inference
and validation changes below.

Rust source and serialized IR
-----------------------------

* ``Rule.order`` changes from ``Option<i64>`` to ``Option<Order>``. Use
  ``Some(1_i64.into())`` for integer orders or
  ``Order::try_from("0.5".to_owned())`` for decimals. Serialized orders are now
  decimal strings: convert stored numeric values before deserializing them.
* ``Rule`` literals need ``source``, ``owner`` and ``shape_order``. Use ``None``
  when those authored details are unavailable. ``Order`` is no longer ``Copy``.
* ``EngineOptions`` adds ``max_inference_iterations``. Use
  ``..Default::default()`` when constructing options to retain the default.
* ``SparqlConstraint`` adds ``annotations``, ``SparqlTarget`` adds
  ``extra_bindings``, and ``FunctionDef`` adds ``optional_params``. Supply
  empty vectors when constructing definitions that do not use these features.
* ``Reason`` adds ``annotations``. The ``Failure::Not`` and ``Failure::Opaque``
  evidence variants add ``annotations`` and ``result_annotations`` respectively;
  update constructors and exhaustive field patterns. The new annotation fields
  default to empty when deserializing older results. ``ValidationResult`` is
  non-exhaustive and exposes annotations through a field and accessor.
* ``NodeExpr`` adds ``PathFrom``. Add a match arm when matching this enum
  exhaustively. ``EvaluationError`` and ``SparqlOperation`` gain variants but
  were already non-exhaustive.

Inference and validation behavior
---------------------------------

Shapes execute in shape order, followed by rule order within each shape. Tied
shapes and tied rules read isolated snapshots. This can change inferred graphs
when guards depend on earlier additions. Function parameters support decimal
orders, and graph-reading functions run under both unsupported-feature policies.

CONSTRUCT rules can create fresh blank nodes. Inference now fails after 1,000
passes by default if it has not converged. Rust callers can configure
``EngineOptions.max_inference_iterations``; Python's ``infer`` accepts the
optional keyword ``max_iterations``. An iteration-limit failure does not write
partial results back to the caller's graph.

``sh:expression`` requires exactly the singleton ``{ true }`` for every value
node. Reports identify the source expression and the failing value node. Rule
results, conformance verdicts, and report details can therefore differ from 0.6.

C++ and other bindings
----------------------

The C API adds annotation accessors. The C++ ``AlgebraReason`` type gains an
``annotations`` member; rebuild C++ consumers and update aggregate initializers
if used. Python reasons and WASM result objects expose annotation values.

The C ABI is now version 8. Rebuild C and C++ consumers with matching headers
and library. ``shifty_prepared_validator_shape_map()`` takes an array of
``ShiftyStringView`` and a count of paths. In C++, replace
``options.name_path = "sh:name"`` with ``options.name_paths = {"sh:name"}``
to keep using that path alone.
