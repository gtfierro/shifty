Usage
=====

Basic validation::

   import shifty

   conforms, results_graph, report_text = shifty.validate(
       data_graph,
       shapes_graph,
       run_inference=True,
       inference={"min_iterations": 1, "max_iterations": 8},
       graphviz=True,
       heatmap=True,
       trace_events=True,
       return_inference_outcome=True,
   )

Inference-only::

   inferred_graph, diagnostics = shifty.infer(
       data_graph,
       shapes_graph,
       min_iterations=1,
       max_iterations=4,
       graphviz=True,
       union=True,
       return_inference_outcome=True,
   )

Reuse compiled shapes across datasets::

   compiled = shifty.generate_ir(
       shapes_graph,
       skip_invalid_rules=True,
       warnings_are_errors=False,
       do_imports=True,
   )
   conforms, _, _, diagnostics = compiled.validate(data_graph, run_inference=True)

Diagnostics are returned only when explicitly requested. See
``python/README.md`` for the full list of options and examples.
