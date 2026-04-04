# /// script
# dependencies = [
#     "ontoenv==0.5.0a8",
#     "pyshifty",
#     "rdflib",
# ]
#
# [tool.uv.sources]
# pyshifty = { path = "python" }
# ///

from rdflib import Graph, URIRef, Literal, Namespace
from ontoenv import OntoEnv
import shifty

# https://ontoenv.gtf.fyi/ for ontology dependencies
# ontoenv>=0.5 removed `no_search`; pass an empty search list to disable scanning.
env = OntoEnv(temporary=True, search_directories=[])
# use nightly Brick release (1.4.nightly); mymodel.ttl imports Brick 1.4
env.add("https://github.com/BrickSchema/Brick/releases/download/nightly/Brick.ttl")
model_graph = Graph().parse("mymodel.ttl", format="turtle")
print("Model triples:", len(model_graph))

# download all dependencies for the model
shape_graph, imported = env.get_dependencies_graph(
    model_graph, fetch_missing=True, recursion_depth=1
)
print(f"Imported {imported} dependencies.")
print(imported)
print("Shape triples:", len(shape_graph))

# skip imports during inference to avoid re-fetching dependencies
# compiled = shifty.infer(model_graph, shape_graph, do_imports=False, skip_invalid_rules=True)
# print("Inferred triples:", len(compiled))

# runs validation. Can perform inference as part of this (run_inference=True)
# We pre-computed the shape graph (get_dependencies_graph) so we use do_imports=False to avoid re-doing that work.
# In practice, no reason to do this. Just call shifty.validate(model_graph, shape_graph) and it should handle everything!
validates, results_graph, results_text = shifty.validate(
    model_graph,
    shape_graph,
    run_inference=True,
    do_imports=False,
    skip_invalid_rules=True,
)
print(results_text)
print("Conforms?", validates)
