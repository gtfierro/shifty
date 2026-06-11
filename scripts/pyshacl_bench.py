# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rdflib",
#   "pyshacl",
# ]
# ///
"""Run pySHACL on a single data graph + pre-closure shapes file.

Usage: uv run scripts/pyshacl_bench.py <data.ttl> <shapes-closure.ttl>

Expects shapes files that already include all transitive OWL imports
(i.e., the *-closure.ttl files under brick/ and s223/).  For SHACL-AF
inference, pyshacl's own advanced/inplace mode is used.
"""

import sys
from rdflib import Graph
import pyshacl

data_path, shapes_path = sys.argv[1], sys.argv[2]

shapes_graph = Graph().parse(shapes_path)
data_graph = Graph().parse(data_path)
# Merge shapes into data so class hierarchies and OWL axioms are visible
# during validation (standard SHACL 'union graph' mode).
data_graph += shapes_graph

conforms, report_graph, report_text = pyshacl.validate(
    data_graph=data_graph,
    shacl_graph=shapes_graph,
    advanced=True,   # enables sh:rule / SHACL-AF
    inplace=True,    # inferred triples added to data_graph in place
    allow_warnings=True,
)
print(report_text)
print(f"conforms: {conforms}")
