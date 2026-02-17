# /// script
# requires-python = "==3.12"
# dependencies = [
#   "brick-tq-shacl>=0.4.1",
#   "pyontoenv>=0.5.0a5",
# ]
# ///
import rdflib
from brick_tq_shacl import infer, validate
from ontoenv import OntoEnv
import sys

model = rdflib.Graph().parse(sys.argv[1])

env = OntoEnv(temporary=True, no_search=True)
shapes_id = env.add(sys.argv[2])
shapes_with_deps = env.get_graph(shapes_id)
env.import_dependencies(shapes_with_deps)
model += shapes_with_deps

inferred = infer(model, shapes_with_deps)
valid, _, report_str = validate(inferred, shapes_with_deps)
print(report_str)
print(f"Is valid: {valid}")
