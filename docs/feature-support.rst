Feature support
===============

Legend: ✅ supported · ⚠️ partial / gated · ❌ unsupported.

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
     - forward-chained to a fixed point with ``sh:order`` / ``sh:condition``
   * - Node expressions — ``sh:this``, constants, ``sh:path``, ``sh:filterShape``, ``sh:intersection``, ``sh:union``, function application
     - ✅
     -
   * - SPARQL targets — ``sh:target`` + ``sh:select``
     - ✅
     -
   * - SPARQL constraints — ``sh:sparql`` (``sh:select`` / ``sh:ask``)
     - ✅
     - native execution with Spareval fallback
   * - Custom constraint components — ``sh:parameter`` + ``sh:validator`` / ``sh:nodeValidator`` / ``sh:propertyValidator``
     - ✅
     - optional params, simple & complex ``$PATH``; report path
   * - Expression constraints — ``sh:expression``
     - ✅
     -
   * - SHACL functions — ``sh:SPARQLFunction`` in node expressions
     - ✅
     - full data-graph access
   * - SHACL functions — ``sh:SPARQLFunction`` called from SPARQL (``sh:sparql``, CONSTRUCT, ``dash:expression``)
     - ⚠️
     - evaluated as **pure** functions of their arguments; a body that reads the data graph is gated (see below)
   * - JavaScript — ``sh:js*``, ``sh:JSFunction``
     - ❌
     - no JS engine

Recursion & semantics
---------------------

.. list-table::
   :header-rows: 1
   :widths: 50 12 38

   * - Feature
     - Status
     - Notes
   * - Stratified recursive shapes
     - ✅
     - gfp validation / lfp inference per stratum
   * - Non-stratifiable schemas (cycle through negation)
     - ❌
     - **diagnosed and refused**, never guessed

Feature-handling policy
-----------------------

Partially supported features are handled per an ``on_unsupported`` setting
(``EngineOptions`` in Rust; the ``on_unsupported=`` keyword on
``validate`` / ``validate_algebra`` / ``infer`` and ``PreparedValidator`` in
Python):

- ``"ignore"`` (default) — best-effort: e.g. a graph-reading function called from
  a SPARQL context is evaluated over an empty dataset (result may be unreliable).
- ``"error"`` — fail loudly: the unsupported construct is refused so the failure
  surfaces (e.g. as a constraint error) instead of a silent wrong answer.

.. code-block:: python

   conforms, report, text = shifty.validate(data, shapes, on_unsupported="error")
