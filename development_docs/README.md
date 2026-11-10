# Development documentation

Current designs, algorithms, and implementation invariants. Public API and
usage documentation lives in [docs/](../docs/index.rst).

| Reference | Contents |
| --- | --- |
| [Compiler pipeline](compiler-pipeline.md) | Algebra, lowering, normalization, target seeding, planning, inspection |
| [Evaluation](evaluation.md) | Validation, signed recursion admission, ordered rule inference, witnessing |
| [SPARQL execution](sparql-execution.md) | Native query admission, prebindings, execution, fallback, caching |
| [Evidence](evidence.md) | Polarity, traversal, source identity, compact encoding, invariants |
| [Repair](repair.md) | Experimental synthesis, templates, plans, enumeration, whole-schema gating |
| [Sessions and storage](sessions-and-storage.md) | Ownership, graph roles, snapshots, term dictionaries, index policy |
| [Shape maps](shape-maps.md) | Authored bindings, lazy evidence, typed keys, names and annotations |
| [Violation generation](violation-generation.md) | VIO/SAT expansion over evidence, sampling, conflict-free composition, determinism |

Keep these references aligned with their linked implementations. Plans, task
logs, release handoffs, and archived copies do not belong here.
