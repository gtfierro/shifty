Shapeshift reference
====================

.. include:: experimental.inc

What each construct is in SHACL, the reserved words, and the commands. The
SHACL is what ``shifty shape export`` writes; :doc:`language` explains the
constructs.

Constructs in SHACL
-------------------

Targets
~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - Shapeshift
     - SHACL
   * - ``for every C``
     - ``sh:targetClass C``
   * - ``for every node with p``
     - ``sh:targetSubjectsOf p``
   * - ``for every value of p``
     - ``sh:targetObjectsOf p``
   * - ``for node n``
     - ``sh:targetNode n``
   * - ``for sparql "..."``
     - ``sh:target [ a sh:SPARQLTarget ; sh:select "..." ]``
   * - ``for every node matching S``
     - a SPARQL target over every node, with ``sh:or ( [ sh:not S ] shape )``.
       Not SHACL Core: ``export --core`` refuses it.

Fields
~~~~~~

A field is a property shape on its path. A shape with a single field is
exported as that property shape.

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - Shapeshift
     - SHACL
   * - ``p required``, ``p at least n``
     - ``sh:minCount 1``, ``sh:minCount n``
   * - ``p at most n``
     - ``sh:maxCount n``
   * - ``p exactly n``, ``p between a and b``
     - ``sh:minCount`` and ``sh:maxCount``
   * - ``p optional``
     - ``sh:maxCount 1``
   * - ``p each T``
     - the test ``T`` on the property shape
   * - ``p at least n is T``, ``p at least n matches S``
     - ``sh:qualifiedValueShape T ; sh:qualifiedMinCount n``
   * - ``p at most n is T``
     - ``sh:qualifiedValueShape T ; sh:qualifiedMaxCount n``
   * - ``p exactly n is T``
     - ``sh:qualifiedMinCount n ; sh:qualifiedMaxCount n``
   * - ``p below q``, ``p up to q``
     - ``sh:lessThan q``, ``sh:lessThanOrEquals q``
   * - ``p same as q``, ``p different from q``
     - ``sh:equals q``, ``sh:disjoint q``
   * - ``p unique language``
     - ``sh:uniqueLang true``

Tests
~~~~~

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - Shapeshift
     - SHACL
   * - ``a C``
     - ``sh:class C``
   * - ``a C or D``
     - ``sh:or ( [ sh:class C ] [ sh:class D ] )``
   * - ``matches S``
     - ``sh:node S`` (``sh:property S`` when ``S`` is a single field)
   * - ``{ lines }``
     - a blank-node shape
   * - ``one of x y``
     - ``sh:in ( x y )``
   * - ``string``, ``integer``, ..., ``datatype D``
     - ``sh:datatype xsd:string``, ..., ``sh:datatype D``
   * - ``iri``, ``literal``, ``blank``
     - ``sh:nodeKind sh:IRI``, ``sh:Literal``, ``sh:BlankNode``
   * - ``length n``
     - ``sh:minLength n ; sh:maxLength n``
   * - ``length from a to b``, ``length from a``, ``length up to b``
     - ``sh:minLength a ; sh:maxLength b``, ``sh:minLength a``,
       ``sh:maxLength b``
   * - ``from a``, ``up to b``
     - ``sh:minInclusive a``, ``sh:maxInclusive b``
   * - ``above a``, ``below b``
     - ``sh:minExclusive a``, ``sh:maxExclusive b``
   * - ``from a to b``, ``above a below b``, ...
     - both bounds
   * - ``pattern "re" flags "i"``
     - ``sh:pattern "re" ; sh:flags "i"``
   * - ``language "en" "de"``
     - ``sh:languageIn ( "en" "de" )``

On a line, ``is T`` puts the test on the node itself: ``is a C`` is
``sh:class C`` on the shape, and ``is one of ex:x`` requires that exact node.

Literals
~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - Shapeshift
     - RDF
   * - ``"text"``, ``"text"@en``
     - ``"text"``, ``"text"@en``
   * - ``3``, ``1.5``, ``1e3``
     - ``xsd:integer``, ``xsd:decimal``, ``xsd:double``
   * - ``true``, ``false``
     - ``xsd:boolean``
   * - ``2024-01-31``
     - ``"2024-01-31"^^xsd:date``
   * - ``2024-01-31T08:00:00Z``
     - ``"2024-01-31T08:00:00Z"^^xsd:dateTime``
   * - ``08:00:00``
     - ``"08:00:00"^^xsd:time``

Paths
~~~~~

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - Shapeshift
     - SHACL
   * - ``p/q``
     - ``( p q )``
   * - ``p or q``
     - ``[ sh:alternativePath ( p q ) ]``
   * - ``^p``
     - ``[ sh:inversePath p ]``
   * - ``p*``, ``p?``
     - ``[ sh:zeroOrMorePath p ]``, ``[ sh:zeroOrOnePath p ]``
   * - ``p+``
     - ``( p [ sh:zeroOrMorePath p ] )``
   * - ``path name = ...``, ``name(args)``
     - the path, substituted where it is used

Names, groups, and frames
~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - Shapeshift
     - SHACL
   * - ``shape s { ... }`` with ``base <b>``
     - ``<b s>``: a ``sh:NodeShape``, or a ``sh:PropertyShape`` when it has
       one field
   * - ``name: line`` in shape ``s``
     - a property shape ``<b s.name>``
   * - ``s`` alone on a line
     - ``sh:node s``
   * - ``is T`` on a line
     - the test ``T`` on the shape
   * - ``s.name`` on a line
     - ``sh:property <b s.name>``
   * - ``p { name: C }``
     - ``sh:qualifiedValueShape [ sh:class C ] ; sh:qualifiedMinCount 1 ;
       sh:qualifiedMaxCount 1``
   * - row ``C required``, row ``C optional``
     - only ``sh:qualifiedMinCount 1``, only ``sh:qualifiedMaxCount 1``
   * - ``any of``, ``exactly one of``, ``none of``
     - ``sh:or``, ``sh:xone``, ``sh:not``
   * - ``at least n of``
     - ``sh:or`` over the combinations
   * - ``at most n of``
     - ``sh:not`` of ``at least n+1 of``
   * - ``when C then B``
     - ``sh:or ( [ sh:not C ] B )``
   * - ``template(args)``
     - a shape for each distinct argument list, with
       ``shapeshift:expands "template(args)"``

Annotations
~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - Shapeshift
     - SHACL
   * - ``doc "..."``
     - ``rdfs:comment`` on a shape, ``sh:description`` on a line
   * - ``message "..."``
     - ``sh:message``
   * - ``severity info``, ``warning``, ``violation``
     - ``sh:severity sh:Info``, ``sh:Warning``, ``sh:Violation``
   * - ``only these properties``
     - ``sh:closed true ; sh:ignoredProperties ( rdf:type )``

An export marks what other SHACL engines need to know on the file's
``owl:Ontology`` node: ``shapeshift:recursive true`` when a shape refers to
itself, and ``shapeshift:shapeTargets`` or ``shapeshift:sparqlTargets`` when
it uses those targets.

Reserved words
--------------

These words are keywords and cannot name a shape, a path, or a parameter:

.. code-block:: text

   a above and any as at base below between blank datatype different doc each
   every exactly false flags for from import iri is language least length
   literal matches matching message most node none of one only optional or
   path pattern prefix properties required same self severity shape sparql
   then these to true unique up value when with

Datatype words, such as ``string`` and ``integer``, are not reserved, but
where a test is expected they always mean the ``xsd`` datatype: a shape named
``string`` is reached only with ``matches string``.

Commands
--------

``shifty shape`` works on ``.shm`` files. Run ``shifty shape <command>
--help`` for every option.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Command
     - Does
   * - ``check FILE``
     - Parse, resolve imports, and build. Prints diagnostics; exits 1 on an
       error. ``--format json`` for editors and CI.
   * - ``build FILE``
     - Build and print a summary, or the compiled schema as JSON.
   * - ``fmt FILE``
     - Print the file in the standard layout, keeping comments and docs.
       ``-w`` rewrites it; ``--check`` exits 1 if it is not formatted.
   * - ``export FILE``
     - Write the shapes as SHACL Turtle. ``--core`` refuses what SHACL Core
       cannot express; ``-o`` writes to a file.
   * - ``validate FILE -d DATA``
     - Validate data and report failures against the lines of the file.
       ``-d`` repeats; exits 1 if the data does not conform.
   * - ``imports FILE``
     - List the import closure.
   * - ``explain FILE SHAPE``
     - Show one shape's source, algebra, and targets.
   * - ``repl``
     - An interactive session: define shapes, load data, validate, save.

Every command that reads imports (all but ``fmt`` and ``repl``) takes
``--map IRI=PATH``, which reads an RDF import from a local file, and
``--fetch``, which downloads the imports that are not mapped.

Editors
-------

``crates/tree-sitter-shapeshift`` is a tree-sitter grammar for ``.shm``, with
highlighting queries, for editors that use tree-sitter. These pages are
highlighted by a Pygments lexer, ``docs/_ext/shapeshift_lexer.py``, which
registers the ``shapeshift`` and ``shm`` languages for Sphinx and other
Pygments users.
