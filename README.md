# shifty — formalism-first SHACL + SHACL-AF

A clean-room SHACL **validation + SHACL-AF inference** engine in Rust, grounded
in the SHACL fragment of *Common Foundations for SHACL, ShEx, and PG-Schema*
(arXiv:2502.01295), specialized to RDF and built to be heavily optimized
(static analysis, algebraic rewriting, compilation/JIT).

> Branch `shacl-formalism`. The earlier spec-shaped implementation lives on the
> `bluesky-shacl-core` branch and is kept only as reference.

## Design docs

Start with [`docs/`](docs/):

1. [`docs/00-formalism.md`](docs/00-formalism.md) — the core IR (path algebra
   `π`, shape grammar `φ`, selectors, schema, reference semantics).
2. [`docs/01-gap-analysis.md`](docs/01-gap-analysis.md) — W3C SHACL/SHACL-AF vs.
   the formalism: every hole and its fix.
3. [`docs/02-roadmap.md`](docs/02-roadmap.md) — the layered build (Layer 0 → 7).

## Workspace

| crate | role |
|-------|------|
| `shacl-algebra` | core formalism IR + reference semantics |
| `shacl-parse` | RDF shapes graph → IR lowering |
| `shacl-opt` | static analysis, normalization, planning |
| `shacl-engine` | validation + AF inference execution |
| `shacl-cli` | `shacl` binary: inspect / validate / infer |

```sh
cargo build --workspace
cargo test --workspace
```
