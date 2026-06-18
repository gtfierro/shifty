#!/usr/bin/env python3
"""Drive symbolic repair of a data graph from Python.

This is a *reference driver* — the library computes the repair space and gates
candidates, but every decision (which hole binding, which branch, accept/reject,
focus order, loop control) is made here, in Python. It is intentionally simple:
for each violation it binds every hole to its first reuse candidate and accepts
the repair if the gate says it makes sound progress.

Usage:
    python repair.py <shapes> <data> [--no-infer] [--apply out.nt]

Examples:
    python repair.py ../examples/person.ttl ../examples/person-data.ttl
    python repair.py shapes.ttl data.ttl --apply repaired.nt
"""

import argparse
import sys

import shifty


def solve_one(session, fw):
    """Try to find a sound delta for one violation. Returns the delta or None.

    Policy: take the minimum count at every Repeat, then bind each open hole to
    its first candidate. A real driver would rank candidates, branch, or ask a
    user / LLM here.
    """
    tree = fw.repair_tree()
    if tree.is_blocked:
        return None

    plan = shifty.RepairPlan()
    for choice in tree.choices():
        if choice.kind == shifty.ChoiceKind.Repeat:
            plan.count(choice.node_id, choice.min)
        elif choice.kind == shifty.ChoiceKind.Any:
            plan.choose(choice.node_id, 0)  # first branch

    # Discover per-instance holes (Repeat unrolls), then bind them.
    for hole in tree.instantiate(plan).open_holes:
        cands = hole.candidates(limit=16)
        if not cands:
            return None  # nothing to reuse; a builder driver would mint here
        plan.bind(hole.id, cands[0])

    inst = tree.instantiate(plan)
    if not inst.is_complete:
        return None

    outcome = session.gate(inst.delta)
    return inst.delta if outcome.is_progress else None


def main():
    parser = argparse.ArgumentParser(description="Drive symbolic repair of RDF data")
    parser.add_argument("shapes", help="Shapes graph (Turtle file)")
    parser.add_argument("data", help="Data graph to repair (Turtle file)")
    parser.add_argument("--no-infer", action="store_true", help="Skip SHACL-AF inference")
    parser.add_argument("--apply", metavar="OUT", help="Write the repaired graph (N-Triples) here")
    args = parser.parse_args()

    session = shifty.RepairSession(args.shapes, args.data, infer=not args.no_infer)

    applied = 0
    iterations = 0
    while True:
        witnesses = session.witnesses()
        if not witnesses:
            break
        iterations += 1

        fw = witnesses[0]  # your focus-ordering policy
        print(f"[{iterations}] {fw.focus}  ({fw.target})")
        for atom in fw.summary():
            print(f"      - [{atom.kind}] {atom.detail}")

        delta = solve_one(session, fw)
        if delta is None:
            print("      ! no sound repair found for this focus; stopping")
            break

        for s, p, o in delta.delete:
            print(f"      del {s} {p} {o}")
        for s, p, o in delta.add:
            print(f"      add {s} {p} {o}")

        session = session.advance(delta)  # accept and re-witness
        applied += 1

    remaining = len(session.witnesses())
    print(f"\napplied {applied} repair(s) over {iterations} iteration(s); {remaining} remain")

    if args.apply:
        session.to_graph().serialize(destination=args.apply, format="nt", encoding="utf-8")
        print(f"wrote repaired graph to {args.apply}")

    sys.exit(0 if remaining == 0 else 1)


if __name__ == "__main__":
    main()
