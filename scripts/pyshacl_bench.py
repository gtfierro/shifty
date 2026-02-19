# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rdflib",
#   "pyshacl",
#   "pyontoenv>=0.5.0a5",
# ]
# ///
from rdflib import Graph
import pyshacl
from ontoenv import OntoEnv
import sys

env = OntoEnv(temporary=True, no_search=True)
model_graph = Graph().parse(sys.argv[1], format="turtle")

# add a local copy of 223p.ttl to the environment
ont_id = env.add(sys.argv[2])
# returns an rdflib Graph of the ontology with all its dependencies, e.g. QUDT
ont_with_dependencies = env.get_graph(ont_id)
env.import_dependencies(ont_with_dependencies)
model_graph += ont_with_dependencies
print(len(ont_with_dependencies))

for i in range(5):
    pyshacl.validate(
        data_graph=model_graph,
        shacl_graph=ont_with_dependencies,
        ont_graph=ont_with_dependencies,
        advanced=True,
        inplace=True,  # this will add inferred triples to 'model_graph'
        js=True,
        allow_warnings=True,
    )
# validate your model against the SHACL shapes
valid, report_graph, report_human = pyshacl.validate(
    data_graph=model_graph,
    shacl_graph=ont_with_dependencies,
    ont_graph=ont_with_dependencies,
    advanced=True,
    inplace=True,  # this will add inferred triples to 'model_graph'
    js=True,
    allow_warnings=True,
)

report_graph.serialize("pyshacl.ttl")
print(report_human)
print(f"Is the data graph valid? {valid}")
