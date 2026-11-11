import shifty

result = shifty.infer("data.ttl", "rules.ttl")
print("new triples:", result.inferred_count)
graph = result.graph()
