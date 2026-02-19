# /// script
# dependencies = [
#     'rdflib',
# ]
# ///
import glob
from rdflib import Graph, URIRef
import re


def has_protocol(uri):
    return uri.index(":") > 0


def replace_invalid_iris(graph, prefix="urn:test-case:"):
    for s, p, o in graph.triples((None, None, None)):
        if isinstance(s, URIRef) and not has_protocol(str(s)):
            new_subject = URIRef(prefix + re.sub(r"[^a-zA-Z0-9]", "_", str(s)))
            graph.remove((s, p, o))
            graph.add((new_subject, p, o))
            s = new_subject  # Update s to new_subject for object handling
        if isinstance(p, URIRef) and not has_protocol(str(p)):
            new_predicate = URIRef(prefix + re.sub(r"[^a-zA-Z0-9]", "_", str(p)))
            graph.remove((s, p, o))
            graph.add((s, new_predicate, o))
            p = new_predicate  # Update p to new_predicate for next handling
        if isinstance(o, URIRef) and not has_protocol(str(o)):
            new_object = URIRef(prefix + re.sub(r"[^a-zA-Z0-9]", "_", str(o)))
            graph.remove((s, p, o))
            graph.add((s, p, new_object))


def process_ttl_files(directory):
    ttl_files = glob.glob(f"{directory}/**/*.ttl", recursive=True)
    for input_file in ttl_files:
        graph = Graph()
        print(f"Processing {input_file}")

        graph.parse(input_file, format="ttl")

        replace_invalid_iris(graph, prefix="urn:test-case:")

        # Overwrite the original file with the rewritten graph
        graph.serialize(destination=input_file, format="ttl")


if __name__ == "__main__":
    process_ttl_files("test-suite")
