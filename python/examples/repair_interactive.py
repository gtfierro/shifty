#!/usr/bin/env python3
"""Interactive symbolic-repair driver.

Loops over the violations in a data graph; for each one it enumerates the
concrete repair options (gated, so each is sound), prints them as a numbered
menu, and lets you pick one. The chosen edit is applied, the graph re-validated,
and the loop continues until the graph conforms (or you quit).

This is a *driver* built entirely on the public ``shifty.RepairSession`` API —
the library computes the repair space and gates candidates; every decision here
is made by you at the prompt.

Usage:
    python repair_interactive.py <shapes> <data> [--no-infer] [--apply OUT]

Commands at the prompt:
    <number>   apply that repair option
    s          skip this violation (leave it for now)
    q          quit (optionally writing the graph so far)
"""

import argparse
import itertools
import re
import sys

import shifty

# bounds so enumeration stays interactive on large repair spaces.
MAX_OPTIONS = 25
HOLE_CANDIDATES = 12

XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"

# fresh-node counter, for minting existential witnesses (blank nodes / IRIs).
_FRESH = itertools.count(1)


def mint_candidates(constraint: str) -> list[str]:
    """Fresh nodes to *bake an existential into the graph* for a node-typed hole.

    For a hole that wants a node (a bare ``sh:minCount``, ``sh:nodeKind sh:IRI``,
    or ``sh:node`` sub-shape) we can satisfy the existential by minting a brand
    new node rather than reusing one — a blank node literally *is* an existential
    variable. We propose both a blank node and a fresh IRI and let the gate keep
    whichever actually conforms; for a value-typed hole (``sh:datatype`` etc.)
    neither will pass, so we mint nothing and fall back to reuse / asking.

    TODO(suggestions): when reuse/mint don't fit, suggest values drawn from nodes
    that *almost* satisfy the predicate (near-misses on type/range/pattern).
    """
    c = constraint.lower()
    # NB: not "conforms to" — a minted bare node never satisfies a sub-shape hole
    # (that needs structural build-out, not yet automated), so don't propose it.
    if c.startswith(("fresh", "any node")):
        n = next(_FRESH)
        return [f"_:fresh{n}", f"<urn:shifty:fresh:{n}>"]
    if c.startswith("nodekind"):
        n = next(_FRESH)
        return [f"<urn:shifty:fresh:{n}>", f"_:fresh{n}"]
    return []


def coerce_value(raw: str) -> str:
    """Turn user input into an N-Triples term. Blank → mint fresh; a bare integer
    → ``xsd:integer`` literal; an explicit ``<iri>``/``_:b``/``"lit"`` is used
    as-is; anything else becomes a plain string literal."""
    raw = raw.strip()
    if not raw:
        return f"_:fresh{next(_FRESH)}"
    if raw[0] in "<_\"":
        return raw
    if re.fullmatch(r"[+-]?\d+", raw):
        return f'"{raw}"^^<{XSD_INTEGER}>'
    esc = raw.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{esc}"'


def enumerate_options(session, fw):
    """Return a list of ``(delta, outcome)`` sound repair options for ``fw``.

    Walks the repair tree depth-first, resolving one open choice/hole at a time:
    ``Any`` branches and hole values each fan out into options; ``Repeat`` counts
    default to their minimum. A hole's values are the reuse candidates from the
    data graph **plus** freshly-minted existentials (see :func:`mint_candidates`).
    Each fully-bound plan is gated; only sound options (introduce nothing) are
    kept, ranked by how much they fix.
    """
    tree = fw.repair_tree()
    if tree.is_blocked:
        return []

    choice_by_id = {c.node_id: c for c in tree.choices()}
    plans: list[list[tuple]] = []

    def rec(ops):
        if len(plans) >= MAX_OPTIONS:
            return
        inst = tree.instantiate(_build_plan(ops))
        if inst.open_choices:
            nid = inst.open_choices[0]
            ch = choice_by_id[nid]
            if ch.kind == "any":
                for b in range(ch.branches or 0):
                    rec(ops + [("choose", nid, b)])
            else:  # repeat → default to the minimum count
                rec(ops + [("count", nid, ch.min)])
            return
        if inst.open_holes:
            hole = inst.open_holes[0]
            cands = list(hole.candidates(limit=HOLE_CANDIDATES))
            cands += mint_candidates(hole.constraint)  # existential witnesses
            if not cands:
                return  # nothing to reuse and nothing to mint; user must supply
            for c in cands:
                rec(ops + [("bind", hole.id, c)])
            return
        plans.append(ops)  # complete

    rec([])

    # Gate each plan; keep sound ones, dedup identical deltas, rank by fixes.
    seen = set()
    options = []
    for ops in plans:
        delta = tree.instantiate(_build_plan(ops)).delta
        sig = (
            frozenset(delta.add),
            frozenset(delta.delete),
        )
        if sig in seen:
            continue
        seen.add(sig)
        outcome = session.gate(delta)
        if outcome.is_sound and outcome.is_progress:
            options.append((delta, outcome))

    options.sort(key=lambda o: (-len(o[1].fixed), len(o[1].introduced)))
    return options


def _build_plan(ops):
    plan = shifty.RepairPlan()
    for kind, a, b in ops:
        if kind == "choose":
            plan.choose(a, b)
        elif kind == "count":
            plan.count(a, b)
        elif kind == "bind":
            plan.bind(a, b)
    return plan


def _short(term: str) -> str:
    """Abbreviate a couple of common namespaces for readable menus."""
    return (
        term.replace("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:")
        .replace("http://www.w3.org/2001/XMLSchema#", "xsd:")
        .replace("http://example.org/", "ex:")
    )


def render_delta(delta) -> list[str]:
    lines = []
    for s, p, o in delta.delete:
        lines.append(f"del {_short(s)} {_short(p)} {_short(o)}")
    for s, p, o in delta.add:
        lines.append(f"add {_short(s)} {_short(p)} {_short(o)}")
    return lines


def fill_by_hand(fw):
    """Prompt the user for each open hole, then return a complete delta (or None).

    Sets every ``Repeat`` to its minimum and every ``Any`` to its first branch,
    then asks for a value per resulting hole (blank input mints a fresh node).
    """
    tree = fw.repair_tree()
    if tree.is_blocked:
        print("  this violation is blocked — no data repair is possible.")
        return None

    plan = shifty.RepairPlan()
    for ch in tree.choices():
        if ch.kind == "repeat":
            plan.count(ch.node_id, ch.min)
        elif ch.kind == "any":
            plan.choose(ch.node_id, 0)

    inst = tree.instantiate(plan)
    if inst.open_choices:
        print("  this repair has nested choices the manual path can't drive; skip it.")
        return None
    if not inst.open_holes:
        print("  nothing to fill here.")
        return None

    for hole in inst.open_holes:
        if hole.constraint.startswith("conforms to"):
            # a sub-shape hole: the value must ITSELF conform. A bare/new node
            # won't — only an existing conforming node will satisfy the gate.
            print(
                "  ⚠ this value must ALREADY conform to the sub-shape below;\n"
                "    a bare or freshly-minted node won't (build-out isn't automated yet)."
            )
            hintstr = ""
        else:
            hint = hole.candidates(limit=5)
            hintstr = f"  reuse e.g. {', '.join(hint)}" if hint else ""
        try:
            raw = input(
                f"  value for ?{hole.id} [{hole.constraint}]{hintstr}\n"
                f'    type 0 | "Alice" | <http://ex/a> | _:b ;  blank = mint fresh > '
            )
        except EOFError:
            return None
        plan.bind(hole.id, coerce_value(raw))

    inst = tree.instantiate(plan)
    return inst.delta if inst.is_complete else None


def gate_and_report(session, delta, what):
    """Gate ``delta``; on rejection print why and return None, else the outcome."""
    outcome = session.gate(delta)
    if not outcome.is_sound:
        foci = ", ".join(_short(v.focus_node) for v in outcome.introduced)
        print(f"  ✗ that {what} introduces new violation(s) at: {foci}. Not applied.")
        return None
    if not outcome.is_progress:
        print(
            f"  ✗ no progress: the {what} is valid but the violation persists.\n"
            "    The added value(s) must form proper, conforming node(s) (right rdf:type +\n"
            "    required properties) — a bare/new node leaves a qualified count or sub-shape\n"
            "    unmet. Author a subgraph that includes the node's type + properties, or bind\n"
            "    an existing conforming node."
        )
        return None
    return outcome


BUILD_FUEL = 6  # max recursion depth when building nested conforming nodes


def _delta_from_tuples(adds, dels):
    def nt(tuples):
        return "\n".join(f"{s} {p} {o} ." for s, p, o in tuples)

    return shifty.RepairDelta.from_ntriples(nt(adds), nt(dels))


def build_tree(session, tree, fuel, indent=""):
    """Realize a RepairTree into (adds, deletes) triple-tuple lists.

    Recurses into ``conforms to @N`` holes by minting a fresh node and building it
    out against the sub-shape (the structural build); prompts the user for plain
    value holes. Returns None on abort / depth-exhaustion.
    """
    if tree.is_blocked:
        print(
            indent + "✗ blocked — synthesis can't build this sub-shape "
            "(e.g. it constrains values reached by an inverse/complex path).\n"
            + indent + "  use 'g' to author the node's subgraph by hand instead."
        )
        return None
    plan = shifty.RepairPlan()
    for ch in tree.choices():
        if ch.kind == "repeat":
            plan.count(ch.node_id, ch.min)
        elif ch.kind == "any":
            plan.choose(ch.node_id, 0)
    inst = tree.instantiate(plan)
    if inst.open_choices:
        print(indent + "✗ nested choices aren't supported in auto-build.")
        return None

    sub_builds = []  # (fresh_node, sub_shape_id) to recurse into
    for hole in inst.open_holes:
        sid = hole.conforms_to
        if sid is not None:
            f = f"<urn:shifty:build:{next(_FRESH)}>"
            plan.bind(hole.id, f)
            sub_builds.append((f, sid))
        else:
            hint = hole.candidates(limit=5)
            hintstr = f"  reuse e.g. {', '.join(hint)}" if hint else ""
            try:
                raw = input(
                    f"{indent}value for ?{hole.id} [{hole.constraint}]{hintstr}\n"
                    f'{indent}  type 0 | "Alice" | <http://ex/a> ;  blank = mint > '
                )
            except EOFError:
                return None
            plan.bind(hole.id, coerce_value(raw))

    inst = tree.instantiate(plan)
    if not inst.is_complete:
        print(indent + "✗ could not complete this node.")
        return None

    adds, dels = list(inst.delta.add), list(inst.delta.delete)
    if sub_builds and fuel <= 0:
        print(indent + "✗ build depth limit reached.")
        return None
    for f, sid in sub_builds:
        sub = session.repair_node_against(f, sid)
        if sub is None:  # the fresh node already conforms (no positive reqs)
            continue
        print(f"{indent}↳ building {_short(f)} to conform to @{sid}")
        res = build_tree(session, sub, fuel - 1, indent + "  ")
        if res is None:
            return None
        adds += res[0]
        dels += res[1]
    return adds, dels


def read_subgraph() -> str:
    """Read a pasted Turtle subgraph from stdin until a line `END` (or EOF)."""
    print("  paste Turtle for triples to ADD (include @prefix lines or full <IRIs>).")
    print("  finish with a line containing only  END  (or Ctrl-D):")
    lines = []
    while True:
        try:
            ln = input()
        except EOFError:
            break
        if ln.strip() == "END":
            break
        lines.append(ln)
    return "\n".join(lines)


def prompt_choice(n: int) -> str:
    sel = f"select [1-{n}], " if n else ""
    while True:
        try:
            raw = input(f"{sel}(v)alue, (g)raph, (b)uild, (s)kip, (q)uit > ").strip().lower()
        except EOFError:
            return "q"
        if raw in ("v", "g", "b", "s", "q"):
            return raw
        if n and raw.isdigit() and 1 <= int(raw) <= n:
            return raw
        print("  ? type a number from the menu, or 'v', 'g', 'b', 's', 'q'")


def main():
    parser = argparse.ArgumentParser(description="Interactive SHACL repair driver")
    parser.add_argument("shapes", help="Shapes graph (Turtle file)")
    parser.add_argument("data", help="Data graph to repair (Turtle file)")
    parser.add_argument("--no-infer", action="store_true", help="Skip SHACL-AF inference")
    parser.add_argument("--apply", metavar="OUT", help="Write the repaired graph (N-Triples) on exit")
    args = parser.parse_args()

    session = shifty.RepairSession(args.shapes, args.data, infer=not args.no_infer)
    skipped: set[tuple[str, int]] = set()
    applied = 0

    while True:
        witnesses = [
            fw for fw in session.witnesses() if (fw.focus, fw.statement) not in skipped
        ]
        if not witnesses:
            break

        fw = witnesses[0]
        remaining = len(witnesses)
        print(f"\n── violation ({remaining} left) ────────────────────────────────")
        print(f"focus:  {_short(fw.focus)}")
        print(f"target: {fw.target}")
        for atom in fw.summary():
            where = f" on {_short(atom.path)}" if atom.path else ""
            print(f"  · [{atom.kind}]{where}: {atom.detail}")

        options = enumerate_options(session, fw)
        if options:
            print("\noptions:")
            for i, (delta, outcome) in enumerate(options, start=1):
                print(f"  [{i}] (fixes {len(outcome.fixed)})")
                for line in render_delta(delta):
                    print(f"        {line}")
        else:
            print("\n  no automatic option (no reusable/mintable value).")
        # how to author a repair by hand:
        print(
            "\n  'v' = supply a value yourself"
            '   (e.g. v ↵ then  0 | "Alice" | <http://ex/a> ;  blank = mint fresh)\n'
            "  'g' = paste a subgraph patch in Turtle (a new node WITH its rdf:type +\n"
            "        properties) — applied only if the gate says it fixes the violation.\n"
            "  'b' = auto-build: mint nodes for 'conforms to' holes and recursively\n"
            "        build them out (prompting for any plain values), then gate."
        )

        choice = prompt_choice(len(options))
        if choice == "q":
            break
        if choice == "s":
            skipped.add((fw.focus, fw.statement))
            print("  skipped.")
            continue

        if choice == "v":
            delta = fill_by_hand(fw)
            if delta is None:
                continue
            outcome = gate_and_report(session, delta, "value")
            if outcome is None:
                continue
        elif choice == "g":
            ttl = read_subgraph()
            if not ttl.strip():
                print("  (empty) nothing to apply.")
                continue
            try:
                delta = shifty.delta_from_graph(ttl)
            except Exception as e:  # noqa: BLE001 — surface any parse error to the user
                print(f"  ✗ could not parse that subgraph: {e}")
                continue
            outcome = gate_and_report(session, delta, "subgraph")
            if outcome is None:
                continue
        elif choice == "b":
            res = build_tree(session, fw.repair_tree(), BUILD_FUEL)
            if res is None:
                continue
            delta = _delta_from_tuples(*res)
            print("  proposed build:")
            for line in render_delta(delta):
                print(f"        {line}")
            outcome = gate_and_report(session, delta, "build")
            if outcome is None:
                continue
        else:
            delta, outcome = options[int(choice) - 1]

        session = session.advance(delta)  # apply + re-validate next loop
        applied += 1
        print(f"  applied. ({len(outcome.fixed)} violation(s) fixed)")

    remaining = len(session.witnesses())
    print(
        f"\ndone: applied {applied} repair(s); "
        f"{remaining} violation(s) remain"
        + (f", {len(skipped)} skipped" if skipped else "")
        + "."
    )

    if args.apply:
        session.to_graph().serialize(destination=args.apply, format="nt", encoding="utf-8")
        print(f"wrote graph to {args.apply}")

    sys.exit(0 if remaining == 0 else 1)


if __name__ == "__main__":
    main()
