# /// script
# requires-python = "==3.12"
# dependencies = [
#   "buildingmotif[topquadrant] @ https://github.com/NREL/BuildingMOTIF.git#develop",
# ]
# ///
from buildingmotif import BuildingMOTIF
from buildingmotif.dataclasses import Model
from buildingmotif.dataclasses import Library
import sys

bm = BuildingMOTIF("sqlite://", shacl_engine="topquadrant")  # in-memory


# load ontologies
s223 = Library.load(ontology_graph=sys.argv[2])

# create model
bldg = Model.from_file(sys.argv[1])
compiled = bldg.compile([s223.get_shape_collection()])

# validate the model (includes running SHACL inference)
context = compiled.validate()

print(context.report_string)
print(f"Is valid: {context.valid}")
