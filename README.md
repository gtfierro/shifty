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

# Inspect how a shapes graph evolves through the layers:
cargo run -p shacl-cli -- inspect --stage rdf     examples/person.ttl   # raw triples
cargo run -p shacl-cli -- inspect --stage algebra examples/person.ttl   # lowered IR (φ/π notation)
cargo run -p shacl-cli -- inspect --stage algebra --format json examples/person.ttl

# Validate a data graph against shapes (reference evaluator):
cargo run -p shacl-cli -- validate --shapes examples/person.ttl --data examples/person-data.ttl

# Validation graph scope defaults to `union`: focus nodes from data, with
# paths/class hierarchy/SPARQL evaluated over data + shapes.
cargo run -p shacl-cli -- validate --graph-mode data --shapes shapes.ttl --data data.ttl
cargo run -p shacl-cli -- validate --graph-mode union-all --shapes shapes.ttl --data data.ttl
```

The Rust API mirrors these modes: `validate_graphs` defaults to union
evaluation, while `validate_graphs_with_mode` accepts `ValidationGraphMode`.

## Profiling

Linux profiling uses
[`cargo-flamegraph`](https://github.com/flamegraph-rs/flamegraph) and `perf`.
Install the Rust tool and the `perf` package for the running kernel, then run:

```sh
cargo install flamegraph

# Profiles inference followed by validation on the 223P/NIST workload.
scripts/flamegraph.sh

# Profile any other shacl CLI invocation.
FLAMEGRAPH_OUTPUT=person.svg scripts/flamegraph.sh \
  validate --shapes examples/person.ttl --data examples/person-data.ttl
```

Open the generated SVG in a browser to use its interactive search and zoom.
The script uses the release profile with debug symbols and disables expensive
inline-frame expansion during flamegraph generation. It samples the software
`cpu-clock` event so Intel hybrid-core systems produce one complete profile.
Use `FLAMEGRAPH_FREQUENCY` and `FLAMEGRAPH_STACK_SIZE` to override the
disk-conscious defaults.

The `algebra` text view is a cycle-safe flat dump of the shape graph: each arena
slot prints as `@i = <φ>` with child shapes referenced as `@j`, so recursion and
sharing are visible at a glance.
