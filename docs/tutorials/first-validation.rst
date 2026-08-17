Your first validation
=====================

In this tutorial you write a SHACL shapes file and a data file, validate one
against the other, read the report, and fix the data. It takes about ten
minutes and assumes no prior SHACL knowledge — only that you can read Turtle,
RDF's text format, well enough to recognise a triple when you see one.

Install
-------

Wheels for ``pyshifty`` are published to PyPI and contain a pre-compiled
engine, so installing it needs no Rust toolchain:

.. code-block:: bash

   pip install pyshifty

The distribution is named ``pyshifty`` but the module is ``shifty``:

.. code-block:: python

   import shifty

For the command-line tool you do need Rust, because there are no pre-built
binaries yet:

.. code-block:: bash

   git clone https://github.com/gtfierro/shifty
   cd shifty
   cargo install --path crates/shifty-cli

This tutorial uses the CLI for the first half and Python for the second. If
you would rather not install the Rust toolchain, skip to `Doing the same thing
from Python`_ — nothing in the CLI half is a prerequisite.

Two files, two roles
--------------------

Create a working directory with two files in it. The first describes what a
valid person looks like. Call it ``shapes.ttl``:

.. code-block:: turtle

   @prefix sh:  <http://www.w3.org/ns/shacl#> .
   @prefix ex:  <http://example.org/> .
   @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

   ex:PersonShape a sh:NodeShape ;
       sh:targetClass ex:Person ;
       sh:property [
           sh:path ex:name ;
           sh:minCount 1 ;
           sh:datatype xsd:string ;
       ] ;
       sh:property [
           sh:path ex:email ;
           sh:minCount 1 ;
       ] .

Read that as three separate claims. ``sh:targetClass ex:Person`` says *which*
nodes this shape applies to: every node typed ``ex:Person``. The first
``sh:property`` block says each of those nodes must have at least one
``ex:name``, and that every value found there must be a string. The second says
each must have at least one ``ex:email``, with no constraint on the value.

The second file is the data to check. Call it ``data.ttl``:

.. code-block:: turtle

   @prefix ex: <http://example.org/> .

   ex:alice a ex:Person ; ex:name "Alice" ; ex:email "alice@example.org" .
   ex:bob   a ex:Person ; ex:name 123 .

Alice satisfies both obligations. Bob breaks both, in two different ways: his
``ex:name`` is present but is an integer rather than a string, and he has no
``ex:email`` at all. Two failure modes in one node is deliberate — they produce
visibly different output.

Run the validator
-----------------

.. code-block:: bash

   shifty validate --shapes shapes.ttl --data data.ttl

.. code-block:: text

   conforms: false
   violations: 1
     <http://example.org/bob>  [severity: Violation; target: class(<http://example.org/Person>)]
         - [Violation] (<http://example.org/email>) <http://example.org/bob> → at least 1 value(s) required along <http://example.org/email>, found 0
         - [Violation] (<http://example.org/name>) "123"^^<http://www.w3.org/2001/XMLSchema#integer> → test(datatype(xsd:string)) not satisfied

Alice is absent from the output. This is worth noticing now, because it is the
first thing people find surprising: a validation report lists what went wrong,
so a node that passed leaves no trace, and a node that passed is
indistinguishable in the report from a node the shape never looked at.
:doc:`explaining-a-failure` is about getting that information back.

The report groups by *focus node* — the node being checked — rather than by
constraint. Bob is one violation with two reasons under it. Each reason names
the property path in parentheses, then the value that offended, then what was
expected. The second line quotes ``"123"^^xsd:integer`` back at you, showing
the datatype that made it fail; the first has no offending value to quote,
because the problem is an absence, so it names the focus node instead.

``target: class(<http://example.org/Person>)`` is the selector that pulled Bob
in. That is Shifty's compiled form of ``sh:targetClass``, and you will see this
algebraic vocabulary in several places — see :doc:`../explanation/architecture`
if you want to know what it is.

Fix the data
------------

Edit ``data.ttl`` so Bob's name is a string and he has an email:

.. code-block:: turtle

   @prefix ex: <http://example.org/> .

   ex:alice a ex:Person ; ex:name "Alice" ; ex:email "alice@example.org" .
   ex:bob   a ex:Person ; ex:name "Bob"   ; ex:email "bob@example.org" .

Run the same command again:

.. code-block:: text

   conforms: true

The quotes around ``"Bob"`` are doing real work here. In Turtle, ``123`` is an
``xsd:integer`` and ``"123"`` is an ``xsd:string``; they are different RDF
terms, and ``sh:datatype`` distinguishes them. A surprising share of SHACL
violations in practice are this typo.

Doing the same thing from Python
--------------------------------

``shifty.validate`` takes the data graph first and the shapes graph second —
the argument order of ``pyshacl.validate``, so existing code can switch by
changing the import:

.. code-block:: python

   import pathlib
   import shifty

   conforms, report_graph, results_text = shifty.validate(
       pathlib.Path("data.ttl"),
       pathlib.Path("shapes.ttl"),
   )

   print(conforms)
   print(results_text)

You get three things back. ``conforms`` is the boolean. ``report_graph`` is an
``rdflib.Graph`` holding a W3C ``sh:ValidationReport``, which is the
interoperable form to hand to another tool. ``results_text`` is that report
rendered for a human:

.. code-block:: text

   Validation Report
   Conforms: False
   Results (2):
   Constraint Violation in MinCountConstraintComponent
     Severity: sh:Violation
     Source Shape: _:d321972bccd14812550776ed4b7e38e7
     Focus Node: <http://example.org/bob>
     Result Path: <http://example.org/email>
     Message: Fewer than 1 values on path <http://example.org/email>

   Constraint Violation in DatatypeConstraintComponent
     Severity: sh:Violation
     Source Shape: _:fb1fe0806f4b62bba984f3fac3413f0c
     Focus Node: <http://example.org/bob>
     Result Path: <http://example.org/name>
     Value: "123"^^<http://www.w3.org/2001/XMLSchema#integer>
     Message: Value 123 does not have datatype <http://www.w3.org/2001/XMLSchema#string>

This is the standard SHACL report vocabulary rather than the CLI's compact
summary, so the two commands print different text for the same result. The
``Source Shape`` is a blank-node identifier because the property shapes in
``shapes.ttl`` were written inline with ``[ ... ]`` and so have no IRI of their
own.

Any of ``str`` (Turtle text), ``bytes``, ``pathlib.Path``, or an
``rdflib.Graph`` works as an argument, and a list of them is merged first.
Passing paths is the fastest option, because the file is parsed in Rust without
a round-trip through rdflib.

The rule about which graph is which
-----------------------------------

You passed two files, and Shifty treated them asymmetrically: shapes were read
**only** from ``shapes.ttl``. If ``data.ttl`` had contained a stray
``sh:NodeShape`` — copied in from somewhere, or generated by an upstream tool —
it would have been ignored rather than quietly becoming a constraint.

If you pass just one graph, it plays both roles:

.. code-block:: bash

   shifty validate --shapes combined.ttl

.. code-block:: python

   conforms, report, text = shifty.validate("combined.ttl")

This is the common case where shape definitions and instance data live in the
same file. The full rule, including how to deliberately validate against shapes
embedded in your data, is in :doc:`../explanation/shapes-and-data` — it is
worth reading once, because getting it wrong produces a validation run that
passes for the wrong reason.

Where to go next
----------------

:doc:`explaining-a-failure` continues with this same graph and asks the engine
why Bob failed, and what would fix him.

For a specific job — running SHACL-AF rules, extracting bindings from a
conforming node, or looking at how the shapes were compiled — the
:doc:`how-to guides <../how-to/index>` are the shorter path.
