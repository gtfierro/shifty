Contribute to the documentation
===============================

Shifty's documentation uses Diátaxis to keep pages focused, then applies the
same lookup-friendly conventions throughout its reference material. Before
writing, decide what the reader is doing.

Choose the page type
--------------------

.. list-table::
   :widths: 20 35 45
   :header-rows: 1

   * - Type
     - Reader's need
     - Writing rule
   * - Tutorial
     - Learn through a successful first experience.
     - Lead the reader through a complete sequence. Explain only what the next
       step requires, and include checkpoints with expected output.
   * - How-to guide
     - Accomplish a known, concrete task.
     - State the outcome first, list prerequisites, and give the shortest
       reliable procedure. Link elsewhere for theory.
   * - Reference
     - Look up exact behavior while working.
     - Document signatures or syntax, stability, arguments and defaults,
       returns or output, errors, a minimal example, and related operations.
   * - Explanation
     - Understand why the system behaves or is designed this way.
     - Develop the reasoning, alternatives, and tradeoffs. Do not turn the
       page into a procedure or an API inventory.

Page conventions
----------------

- Give each page one primary reader goal and use that goal in its opening.
- Write at the level of the subject. State the technical fact directly; avoid
  narrating the teaching strategy or telling readers what they will find
  surprising, confusing, or important.
- Put exact defaults, accepted values, failure behavior, and stability in the
  reference page—not only in examples or source comments.
- Link to related capabilities and useful next steps. When a tutorial or
  how-to guide introduces an interface, include a forward pointer to its
  reference page for exact fields, arguments, and behavior. Prefer links over
  repeating an explanation in several places.
- Mark experimental interfaces wherever a reader first encounters them.
- Use a figure only when it makes a relationship, sequence, or state change
  clearer than a short paragraph or table. Keep figures as scalable SVGs in
  ``docs/_static``, add SVG ``title`` and ``desc`` elements, and supply useful
  alternative text in the Sphinx ``figure`` directive.

Executable examples
-------------------

Key examples live in ``docs/examples`` as runnable files. Include their source
with ``literalinclude`` and their output with ``program-output`` so the page
cannot show code from one program and output from another. Keep these examples
offline, deterministic, and fast. Sort values before printing if an API does
not guarantee their order.

Use ordinary ``code-block`` directives for fragments, invalid-input examples,
installation commands, network access, and operations that are too expensive
or environment-dependent to run during every documentation build.

Preview your changes
--------------------

From the repository root:

.. code-block:: bash

   cd docs
   make html SPHINXOPTS="-W --keep-going"

The normal build does not rewrite benchmark source data. To regenerate the
benchmark chart deliberately before building, set
``SHIFTY_REGENERATE_BENCHMARKS=1``.

Every rendered page has an **Edit this page** link to its source on GitHub. For
a change that needs discussion first, `open an issue
<https://github.com/gtfierro/shifty/issues/new>`_.
