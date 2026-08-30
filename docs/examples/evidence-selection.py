import shifty


shapes = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.org/> .

ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:email ; sh:minCount 1 ] .

ex:EquipmentShape a sh:NodeShape ;
    sh:targetClass ex:Equipment ;
    sh:nodeKind sh:IRI .
"""

data = """
@prefix ex: <http://example.org/> .

ex:alice a ex:Person ; ex:email "alice@example.org" .
ex:bob a ex:Person .
"""

# [example-start]
run = shifty.EvidenceSession(shapes, data, infer=False).validate()

for statement in run.statements:
    print(statement.selector)
    if not statement.selected_foci:
        print("  selected nothing")
        continue
    for focus in statement.selected_foci:
        print(" ", focus.status, focus.focus)
# [example-end]
