from rdflib import Graph
from ontoenv import OntoEnv
import shacl_rs
import sys

env = OntoEnv()

model = sys.argv[1]
model_name = env.add(model)
print(f"Fetching dependencies for model: {model_name}")
model_graph = env.get_graph(model_name)
shape_graph, imported = env.get_closure(model_name)
print(f"Imported ontologies for SHACL shape graph: {imported}")

print("Running SHACL inference...")
inferred = shacl_rs.infer(model_graph, shape_graph)
print(inferred.serialize(format="turtle"))

valid, results_graph, report_string = shacl_rs.validate(
    model_graph,
    shape_graph,
    inference={"debug": True},
)
print("Validation Report:")
print(report_string)
print(f"Model is valid: {valid}")
