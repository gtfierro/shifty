import shifty

conforms, report_graph, results_text = shifty.validate("data.ttl", "shapes.ttl")
print("conforms:", conforms)
