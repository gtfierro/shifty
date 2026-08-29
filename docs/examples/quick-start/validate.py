import shifty


result = shifty.validate_algebra("data.ttl", "shapes.ttl")
print("conforms:", result.conforms)
for violation in result.violations:
    print(violation.focus_node)
