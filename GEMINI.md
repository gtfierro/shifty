# GEMINI.md

## Project Overview
`shifty` appears to be a SHACL (Shapes Constraint Language) processor or compiler, likely implemented in Rust with Python bindings, given the directory structure (`crates/`, `python/`, `lib/`, `cli/`).

## Development Standards
- **Language:** Rust for core logic, Python for scripting and bindings.
- **Testing:** Use `cargo test` for Rust and `pytest` (if applicable) for Python.
- **Style:** Follow standard Rust (`cargo fmt`) and Python (`black`/`ruff`) conventions.

## Tooling Preferences
- Prefer `cargo` for Rust-related tasks.
- Prefer `uv` for Python-related tasks if `uv.lock` or `pyproject.toml` is present.
