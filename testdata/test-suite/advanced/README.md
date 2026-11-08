# Local SHACL-AF/DASH regression fixtures

This directory is populated. Run its Rust harness from the repository root:

```sh
cargo test -p shifty-engine --test w3c_advanced -- --nocapture
```

The harness runs inference, validation, and function cases; no environment
toggle is required. It resolves imported helper ontologies locally. Some
document IRIs have been rewritten, so these fixtures are retained as local
regressions rather than included in the SHACL 1.0 implementation report.

The pinned Core/SPARQL fixtures and their supported manifest runner are
documented in [testdata/README.md](../../README.md).
