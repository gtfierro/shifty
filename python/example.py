from rdflib import Graph, Namespace, RDF, RDFS, Literal
import shacl_rs  # built via `uvx maturin develop` inside python/

EX = Namespace("http://example.com/ns#")

data = Graph()
data.bind("ex", EX)
data.add((EX.Person1, RDF.type, EX.Person))
data.add((EX.Person1, RDFS.label, Literal("Alice")))
data.add((EX.Person2, RDF.type, EX.Person))

shapes = Graph()
shapes.parse(
  data="""
      PREFIX sh: <http://www.w3.org/ns/shacl#>
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX ex: <http://example.com/ns#>

      ex:PersonShape
          a sh:NodeShape ;
          sh:targetClass ex:Person ;
          sh:property [
              sh:path rdfs:label ;
              sh:minCount 1 ;
              sh:maxCount 1 ;
          ] .
  """,
  format="turtle",
)

# Run validation (no inference)
conforms, results_graph, results_text = shacl_rs.validate(data, shapes)
print(conforms)           # False
print(results_text)       # Turtle SHACL report
print(len(results_graph)) # Number of results triples

# Run SHACL rules beforehand, returning only inferred triples
inferred = shacl_rs.infer(data, shapes)
print(len(inferred))      # New triples inferred by rules
