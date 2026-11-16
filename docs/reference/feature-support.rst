Feature support
===============

Legend: ✅ supported · ⚠️ partial or gated · ❌ unsupported.

The :doc:`conformance report <../conformance>` lists the pinned SHACL 1.0
suite results and downloads. Test results cover the cases in that suite;
the feature descriptions below also identify extension-specific limits.

SHACL Core
----------

.. list-table::
   :header-rows: 1
   :widths: 50 12 38

   * - Feature
     - Status
     - Notes
   * - Node & property shapes
     - ✅
     -
   * - Targets — ``targetNode``, ``targetClass``, ``targetSubjectsOf``, ``targetObjectsOf``, implicit class
     - ✅
     -
   * - Cardinality — ``minCount``, ``maxCount``
     - ✅
     -
   * - Value type — ``datatype``, ``nodeKind``, ``class``
     - ✅
     -
   * - Range — ``min/maxInclusive``, ``min/maxExclusive``
     - ✅
     - numeric, date/time, and duration ordering
   * - String — ``minLength``, ``maxLength``, ``pattern`` (+ ``flags``), ``languageIn``, ``uniqueLang``
     - ✅
     -
   * - Logical — ``and``, ``or``, ``not``, ``xone``
     - ✅
     -
   * - Shape-based — ``node``, ``property``, ``qualifiedValueShape`` (+ counts, ``qualifiedValueShapesDisjoint``)
     - ✅
     -
   * - Property pairs — ``equals``, ``disjoint``, ``lessThan``, ``lessThanOrEquals``
     - ✅
     - on node and property shapes
   * - Other — ``closed`` (+ ``ignoredProperties``), ``hasValue``, ``in``
     - ✅
     -
   * - Paths — predicate, inverse, sequence, alternative, ``zeroOrMore``, ``oneOrMore``, ``zeroOrOne``
     - ✅
     -
   * - ``severity``, ``deactivated``, ``message``
     - ✅
     -

SHACL-SPARQL
------------

.. list-table::
   :header-rows: 1
   :widths: 50 12 38

   * - Feature
     - Status
     - Notes
   * - SPARQL constraints — ``sh:sparql`` (``sh:select``)
     - ✅
     - native execution with Spareval fallback; ``VALUES`` uses SHACL 1.2's
       collision check against potentially pre-bound variables
   * - Custom constraint components — ``sh:parameter`` + ``sh:validator`` / ``sh:nodeValidator`` / ``sh:propertyValidator``
     - ✅
     - optional params, simple and complex ``$PATH``, report path

SHACL-AF (Advanced Features)
----------------------------

.. list-table::
   :header-rows: 1
   :widths: 50 12 38

   * - Feature
     - Status
     - Notes
   * - Rules — ``sh:TripleRule``, ``sh:SPARQLRule`` (CONSTRUCT)
     - ✅
     - forward-chained to a fixed point; decimal ``sh:order`` sorts shapes first,
       then rules within each shape. Tied shapes, rules, and focus nodes read
       isolated snapshots; ``sh:condition`` is supported;
       CONSTRUCT supports ``VALUES`` and fresh blank nodes; inference is bounded
       by an iteration limit (1000 passes by default)
   * - Node expressions — ``sh:this``, constants, ``sh:path``, ``sh:filterShape``, ``sh:intersection``, ``sh:union``, function application
     - ✅
     - path expressions honour ``sh:nodes``; function arguments expand over
       their result sets, including empty optional arguments
   * - SPARQL targets — ``sh:target`` + ``sh:select``
     - ✅
     -
   * - Reusable SPARQL target types — ``sh:SPARQLTargetType``
     - ❌
     - inline SELECT targets are supported; parameterized target types remain
       outside the current implementation
   * - Expression constraints — ``sh:expression``
     - ✅
     - evaluated at every value node; require exactly ``{ true }``; support
       nested function calls and graph-reading functions in node expressions
   * - SHACL functions — ``sh:SPARQLFunction`` in node expressions
     - ✅
     - full data-graph access
   * - SHACL functions — ``sh:SPARQLFunction`` called from SPARQL (``sh:sparql``, CONSTRUCT, ``dash:expression``)
     - ✅
     - shared function environment supports calls inside function bodies and
       reads the current data/shapes snapshot; recursive invocation is bounded
       to 32 levels and execution failures propagate under either policy
   * - JavaScript — ``sh:js*``, ``sh:JSFunction``
     - ❌
     - no JS engine

Recursion and semantics
-----------------------

.. list-table::
   :header-rows: 1
   :widths: 50 12 38

   * - Feature
     - Status
     - Notes
   * - Stratified recursive shapes
     - ✅
     - greatest fixed point for validation, least for inference, per stratum
   * - Non-stratifiable schemas (a cycle through negation)
     - ❌
     - **diagnosed and refused**, never guessed

See :ref:`recursive-shapes` for the recursion policy.

Partial support
---------------

A ⚠️ feature has partial support and may produce incorrect results.
``on_unsupported`` controls how these features are handled. It is set through
``EngineOptions`` in Rust, and a keyword on
``validate``, ``validate_algebra``, ``infer``, and ``PreparedValidator`` in
Python:

``"ignore"``
   Best effort for partially supported features. It does not suppress query
   execution failures or the function recursion limit.

``"error"`` (default)
   Refuse. The unsupported construct surfaces as a failure instead of a silent
   wrong answer.

.. code-block:: python

   conforms, report, text = shifty.validate(data, shapes, on_unsupported="error")

Invalid shapes diagnostics are separate from this policy. A malformed SHACL
constraint or rule, including malformed SPARQL or an unresolved query prefix,
is always rejected before validation or inference starts; it is never lowered
as an absent feature.

Use ``"error"`` when an unsupported feature should stop validation.

Evidence and repair coverage
----------------------------

Structural explanations are not always available: a ``sh:sparql`` constraint is opaque to evidence and
blocked for repair, because an arbitrary query cannot be inverted. The complete
list of these cases is in :doc:`evidence` under "Opaque and blocked evidence",
and repair's blocking reasons are in
:doc:`../experimental/repair/reference`.
