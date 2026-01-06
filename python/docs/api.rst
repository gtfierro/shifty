API Reference
=============

.. py:module:: shifty

Functions
---------

.. py:function:: validate(data_graph, shapes_graph, **options)

   Validate data against SHACL shapes.

   Returns ``(conforms, report_graph, report_text)`` by default. When any
   diagnostics are requested, returns ``(conforms, report_graph, report_text,
   diagnostics)``.

   Common options:

   - ``run_inference``: enable rule-based inference before validation
   - ``inference``: dict of inference options (aliases for the explicit kwargs)
   - ``min_iterations`` / ``max_iterations`` / ``run_until_converged``
   - ``error_on_blank_nodes`` / ``debug``
   - ``skip_invalid_rules`` / ``warnings_are_errors`` / ``do_imports``
   - ``graphviz`` / ``heatmap`` / ``heatmap_all`` / ``trace_events``
   - ``trace_file`` / ``trace_jsonl`` / ``return_inference_outcome``
   - ``union``: include original data alongside inferred triples (inference output)

.. py:function:: infer(data_graph, shapes_graph, **options)

   Run SHACL rule inference and return the inferred graph. When diagnostics are
   requested, returns ``(graph, diagnostics)``. Set ``union=True`` to include
   the original data triples in the returned graph.

   Options mirror ``validate`` but omit validation-only toggles.

.. py:function:: generate_ir(shapes_graph, **options)

   Compile and cache the ShapeIR for a shapes graph. Returns a
   :class:`CompiledShapeGraph` instance.

Classes
-------

.. py:class:: CompiledShapeGraph

   Cached ShapeIR for repeated inference or validation against new datasets.

   .. py:method:: infer(data_graph, **options)

      Run inference with the cached shapes. Returns ``graph`` or
      ``(graph, diagnostics)``. Set ``union=True`` to include the original
      data triples in the returned graph.

   .. py:method:: validate(data_graph, **options)

      Validate data with the cached shapes. Returns ``(conforms, report_graph,
      report_text)`` or ``(conforms, report_graph, report_text, diagnostics)``.
