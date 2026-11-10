Feature support
===============

Legend: ✅ supported · ⚠️ partial or gated · ❌ unsupported.

The pinned SHACL 1.0 suite and reporting procedure are described in
:doc:`../how-to/conformance`. Test results cover the cases in that suite;
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
     - native execution with Spareval fallback
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
     - forward-chained to a fixed point, honouring ``sh:order`` / ``sh:condition``
   * - Node expressions — ``sh:this``, constants, ``sh:path``, ``sh:filterShape``, ``sh:intersection``, ``sh:union``, function application
     - ✅
     -
   * - SPARQL targets — ``sh:target`` + ``sh:select``
     - ✅
     -
   * - Expression constraints — ``sh:expression``
     - ✅
     -
   * - SHACL functions — ``sh:SPARQLFunction`` in node expressions
     - ✅
     - full data-graph access
   * - SHACL functions — ``sh:SPARQLFunction`` called from SPARQL (``sh:sparql``, CONSTRUCT, ``dash:expression``)
     - ⚠️
     - evaluated as **pure** functions of their arguments; a body that reads
       the data graph is gated — see `Partial support`_
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

See :doc:`../explanation/recursion` for the recursion policy.

Partial support
---------------

A ⚠️ feature has partial support and may produce incorrect results.
``on_unsupported`` controls how these features are handled. It is set through
``EngineOptions`` in Rust, and a keyword on
``validate``, ``validate_algebra``, ``infer``, and ``PreparedValidator`` in
Python:

``"ignore"``
   Best effort. A graph-reading function called from a SPARQL context is
   evaluated over an empty dataset, so the result may be wrong.

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
