# Repository Guidelines

## Project Structure & Module Organization
This workspace hosts two Rust crates: `lib/` (the SHACL validation engine under `lib/src/`) and `cli/` (the end-user binary in `cli/src/main.rs`). Shared fixtures live under `lib/tests/test-suite/` and mirror the W3C test manifests; auxiliary Turtle samples sit in the repository root for quick experiments. Cargo generates build artefacts in `target/`; keep large generated files and benchmarking artefacts out of version control.

## Build, Test, and Development Commands
Use `cargo build --workspace` to compile every crate, and `cargo fmt --all` before sending changes to ensure consistent formatting. Run `cargo clippy --workspace --all-targets --all-features` to surface lints the CI expects. Execute `cargo test --workspace` (or `./run_test.sh`) to run unit and integration suites; tests will emit `report.ttl` and `expected.ttl` diffs in the repo root for debugging. To exercise the CLI locally, try `cargo run -p cli -- validate --shapes-file examples/shapes.ttl --data-file examples/data.ttl` and adjust to match your fixture paths.

Unless specifically directed, run the tests after making changes to ensure the tests still pass. If the tests break, continue to make changes until the tests pass

## Coding Style & Naming Conventions
Target Rust 2021 idioms with four-space indentation and `rustfmt` defaults. Modules and files stay `snake_case` (e.g., `named_nodes.rs`), types use `UpperCamelCase`, and functions or locals prefer `snake_case`. Keep public APIs documented with Rustdoc comments and favor expressive enum variants over boolean flags. Log output goes through `env_logger`; prefer structured log messages over `println!` in library code.

## Testing Guidelines
Most automated coverage runs through `lib/tests/manifest_test.rs`, which walks the SHACL test manifests; add new fixtures beneath `lib/tests/test-suite/core/` and register them via the `generate_test_cases!` macro. Name new tests after the manifest file they cover to aid traceability. When debugging validation deltas, compare the generated `report.ttl` with the corresponding expected fixture and clean up the temporary files before committing.

## Commit & Pull Request Guidelines
Follow the existing Conventional Commit style (`fix:`, `refactor:`, `feat:`) with concise, imperative summaries. Each PR should describe the problem, outline the solution, and call out regressions mitigated by new tests. Link related issues, note any manual steps (e.g., regenerating graphs), and confirm the `fmt`, `clippy`, and `test` commands above have been run. Screenshots or sample reports help reviewers evaluate CLI-facing changes.
