import shifty

for infer in (True, False):
    conforms, _, _ = shifty.validate("data.ttl", "rules.ttl", infer=infer)
    print(f"infer={infer}: conforms={conforms}")
