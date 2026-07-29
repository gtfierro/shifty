#!/usr/bin/env python3
"""Show how validation reasons connect to algebra and repair witnesses."""

import shifty


SHAPES = """
@prefix sh:  <http://www.w3.org/ns/shacl#> .
@prefix ex:  <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [
        sh:path ex:name ;
        sh:minCount 1 ;
        sh:datatype xsd:string ;
    ] .
"""

DATA = """
@prefix ex: <http://example.org/> .

ex:bob a ex:Person .
"""


def main() -> None:
    result = shifty.validate_algebra(DATA, SHAPES, infer=False)
    session = shifty.RepairSession(SHAPES, DATA, infer=False)

    witnesses = {
        (w.focus, w.statement_id, w.constraint_id): w
        for w in session.witnesses()
    }

    for violation in result.violations:
        key = (
            violation.focus_node,
            violation.statement_id,
            violation.constraint_id,
        )
        witness = witnesses.get(key)

        print("violation")
        print(f"  focus:       {violation.focus_node}")
        print(f"  statement:   {violation.statement_id}")
        print(f"  constraint:  {violation.constraint_id}")
        print(f"  repair tree: {'available' if witness else 'none'}")

        for reason in violation.reasons:
            print("  reason")
            print(f"    kind:       {reason.constraint_kind}")
            print(f"    id:         {reason.constraint_id}")
            print(f"    operator:   {reason.constraint.render}")
            print(f"    definition: {reason.constraint.definition}")
            print(f"    path:       {reason.path}")
            print(f"    message:    {reason.author_message or reason.message}")

        if witness is not None:
            print("  repair atoms")
            for atom in witness.summary():
                print(
                    f"    {atom.kind} from {atom.constraint_kind} "
                    f"#{atom.constraint_id}: {atom.detail}"
                )


if __name__ == "__main__":
    main()
