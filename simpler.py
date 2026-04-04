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
model_graph = Graph().parse("mymodel.ttl", format="turtle")
print("Model triples:", len(model_graph))

validates, results_graph, results_text = shifty.validate(
    model_graph, run_inference=True, skip_invalid_rules=True
)
print(results_text)
print("Conforms?", validates)
