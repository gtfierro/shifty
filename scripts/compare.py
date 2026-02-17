# /// script
# dependencies = [
# 'rdflib',
# ]
# ///
import rdflib
from rdflib.compare import graph_diff, isomorphic, to_isomorphic, to_canonical_graph
import sys

g1 = to_isomorphic(rdflib.Graph().parse(sys.argv[1]))
g2 = to_isomorphic(rdflib.Graph().parse(sys.argv[2]))
print(g1, g2)

both, first, second = graph_diff(g1, g2)

print('both')
print(both.serialize())
both.serialize("both.ttl", format="turtle")
print('first')
print(first.serialize())
first.serialize("first.ttl", format="turtle")
print('second')
print(second.serialize())
second.serialize("second.ttl", format="turtle")

print(isomorphic(g1, g2))
