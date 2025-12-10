# /// script
# requires-python = "==3.12"
# dependencies = [
#   "brick-tq-shacl>=0.4.1",
# ]
# ///
import rdflib
from brick_tq_shacl import infer, validate
import sys

model = rdflib.Graph().parse(sys.argv[1])
shapes = rdflib.Graph().parse(sys.argv[2])

inferred = infer(model, shapes)
valid, _, report_str = validate(inferred, shapes)
print(report_str)
print(f"Is valid: {valid}")
