# shifty

Clean-room SHACL and SHACL-AF implementation work in Rust.

This branch keeps only the new stack:

- `crates/shacl-core`
  - parsing, syntax preservation, normalized algebra, static analysis, rewriting, logical planning, reports
- `crates/shacl-core-inmemory`
  - in-memory validation and inference backend
- `crates/shacl-core-cli`
  - inspection, planning, rewrite, backend-view, physical-plan, and validate CLI
- `testdata/`
  - shared W3C SHACL manifests and local fixtures used by the new stack

## Build

```bash
cargo build --workspace
```

## Test

```bash
cargo test --workspace
```

## CLI

The CLI binary is `shacl-core-inspect`.

Examples:

```bash
cargo run -p shifty-shacl-core-cli -- dump --stage program testdata/fixtures/af_default_shapes.ttl
cargo run -p shifty-shacl-core-cli -- graphviz testdata/fixtures/af_target_shapes.ttl
cargo run -p shifty-shacl-core-cli -- static-analyze testdata/fixtures/af_default_shapes.ttl
cargo run -p shifty-shacl-core-cli -- backend-view --kind both testdata/fixtures/af_default_shapes.ttl
cargo run -p shifty-shacl-core-cli -- plan --kind validation testdata/fixtures/af_default_shapes.ttl
cargo run -p shifty-shacl-core-cli -- physical-plan --data models/bldg1.ttl Brick.ttl
cargo run -p shifty-shacl-core-cli -- validate --data models/bldg1.ttl Brick.ttl
```

Benchmark timings can be written with:

```bash
cargo run -r -p shifty-shacl-core-cli -- validate \
  --data models/bldg1.ttl \
  --benchmark-json /tmp/bench.json \
  Brick.ttl
```

Inference rule profiles can be exported with:

```bash
cargo run -p shifty-shacl-core-cli -- validate \
  --data models/bldg1.ttl \
  --inference-profile-json /tmp/inference-profile.json \
  Brick.ttl
```

## Test Data

The W3C SHACL manifests and local AF fixtures used by the new implementation live under `testdata/`:

- `testdata/test-suite`
- `testdata/fixtures`
- `testdata/data-shapes`

## Notes

- OntoEnv is used for graph loading and import resolution.
- `docs/static-analysis-plan.md` tracks the core analysis, rewrite, planning, and execution work completed on this branch.
