Data Shapes Test Harness — Design Notes

Goals
- Run the official W3C SHACL test suite located at `lib/tests/data-shapes/data-shapes-test-suite/`.
- Reuse the existing validator API (`Validator`) and report comparison utilities (canonicalization + deskolemization).
- Provide selective execution, clear skips for unsupported features, and easy diffs (`report.ttl` vs `expected.ttl`).

Test Suite Layout
- Root manifest: `lib/tests/data-shapes/data-shapes-test-suite/tests/manifest.ttl`.
- Uses `mf:include` to nest manifests (e.g., `core/manifest.ttl`, `sparql/manifest.ttl`).
- Individual test files are either self-contained (data + shapes in the same Turtle file; `sht:dataGraph <>` and `sht:shapesGraph <>`) or reference external files (e.g., `qualified-001-data.ttl`, `qualified-001-shapes.ttl`).

Scope (Initial)
- Target the classic data-shapes test suite under `data-shapes-test-suite/tests`. Defer the SHACL 1.2/“shacl12-*” trees for later.
- Run all “core” tests first. Skip SPARQL-based tests initially (`tests/sparql/**`).
- Treat negative parser/ill-formed tests explicitly if/when we add a parser-validation mode.

Harness Architecture
- Manifest loading
  - Start at the root manifest and recursively follow `mf:include`.
  - For each `mf:Manifest`, iterate `mf:entries` (RDF list).
  - Identify `sht:Validate` entries only (ignore other classes for now).
  - Extract `mf:action` node and resolve:
    - `sht:dataGraph` and `sht:shapesGraph`: resolve against manifest file directory.
    - `<>` means “this file” (the manifest/test file itself contains graphs).
  - Extract `mf:result` node:
    - Read expected `sh:conforms` boolean.
    - Build expected report graph: include `sh:ValidationReport`, each `sh:result`, and recursively include `sh:resultPath` blank-node subgraphs so comparisons are structural, not lexical.

- Execution
  - For each test case:
    - Build the validator via `Validator::from_files(shapes_path, data_path)`.
    - Run `validate()` to obtain the runtime report.
    - Deskolemize the runtime report graph with both data and shapes base IRIs:
      - `file://.../.well-known/skolem/` base is inferred from each graph’s canonical file URL (mirrors how we skolemize in `lib`).
    - Convert both graphs to Turtle for human-friendly diffs and write `report.ttl` and `expected.ttl` to the repo root (as today).
    - Assertions:
      - Conformance boolean matches.
      - Graphs are isomorphic (after deskolemization), using `are_isomorphic`.

- Skips / Filters
  - Allow filtering via env vars:
    - `SHACL_W3C_INCLUDE=<regex>`: only run tests whose name or path matches.
    - `SHACL_W3C_EXCLUDE=<regex>`: skip matching tests (applied after include).
- SPARQL tests now execute unconditionally; SHACL-AF tests run by default (advanced manifests are currently skipped pending support).
  - Optional static skip list (reasons documented inline) for specific edge cases.

Planned Code Changes
- `lib/src/test_utils.rs`
  - Extend `load_manifest`:
    - Follow `mf:include` recursively (avoid cycles via a `HashSet<PathBuf>`).
    - Parse `mf:action` blank node and set `data_graph_path` and `shapes_graph_path` correctly:
      - `<>` → set to current test file path.
      - `<relative.ttl>` → resolve with existing `resolve_path` helper.
    - Keep `extract_report_graph` logic for expected graph; already handles `sh:resultPath` recursion.
  - Consider splitting types:
    - `Manifest { path, test_cases }` remains.
    - `TestCase { name, conforms, data_graph_path, shapes_graph_path, expected_report }` stays sufficient.

- New test runner: `lib/tests/data_shapes_harness.rs`
  - Single test that:
    - Loads the root manifest path.
    - Expands all includes and collects `TestCase`s.
    - Applies env-based include/exclude filters and SPARQL skip.
    - Iterates cases, running the same execution and comparison routine used in `manifest_test.rs`.
  - Name each subtest deterministically (sanitize `rdfs:label` or derive from relative path) so `cargo test <pattern>` works well.

- Reuse from `manifest_test.rs`
  - Keep `graph_to_turtle`, deskolemization, isomorphism, and file emission behavior identical for consistency.
  - Consider factoring shared helpers into a small internal module to avoid duplication between the two test files.

Edge Cases & Nuances
- Base IRIs
  - Ensure the Turtle parser for manifests uses a base IRI derived from the canonical file URL so that `<>` and relative IRIs resolve correctly.
  - When deskolemizing, apply both the data and shapes base IRIs, as today.

- Embedded vs external graphs
  - Embedded case: both shapes and data are `<>` (the test file). Our current loader already works by passing the same path for both; we just need to preserve this behavior when parsing `mf:action`.
  - External files: resolve via `resolve_path(manifest_path, relative)`.

- Unsupported features
  - SPARQL-based constraints and pre-binding tests.
  - SHACL 1.2-only constructs (in `shacl12-*` trees) — defer.
  - Negative syntax tests (if any) may require separate parser-validation mode; otherwise skip.

Developer UX
- Run all W3C tests (core only):
  - `cargo test --package lib -- data_shapes_` (assuming we prefix generated test fns with `data_shapes_`).
- Run a slice:
  - `SHACL_W3C_INCLUDE=qualified-001 cargo test -- data_shapes_`
  - Pre-binding compliance tests run by default now that the engine enforces SHACL-AF pre-binding semantics.

Incremental Plan
1) Extend `load_manifest` to parse `mf:action` and return proper data/shapes paths; keep current behavior for `<>`.
2) Add recursive include handling (depth-first, de-duplicate by canonical path).
3) Add `lib/tests/data_shapes_harness.rs` that:
   - Loads `lib/tests/data-shapes/data-shapes-test-suite/tests/manifest.ttl`.
   - Applies filters/skips; prints concise skip reasons.
   - Executes tests and writes `report.ttl`/`expected.ttl` on mismatch for debugging.
4) Wire env flags for filtering and optional SHACL-AF gating.
5) Land with `cargo fmt`, `clippy`, and CI runtime bounded to “core” by default.
6) Iterate on skipped sets as features land; enable SPARQL group when supported.

Open Questions
- Should we emit an EARL summary (as RDF) like many SHACL engines? Nice-to-have; defer.
- Do we need a feature flag to exclude these tests from default `cargo test` runs due to duration? If so, gate with `#[ignore]` and add an opt-in alias in CI.
- Some expected reports contain `sh:resultPath` structures that require stable blank nodes; our recursive inclusion should already cover structure. If we find mismatches, augment path extraction accordingly.

Acceptance Criteria for “Phase 1”
- Running the harness against `core` produces a pass/fail list with clear diffs.
- Embedded and external test files both resolve and run.
- SPARQL tests execute by default with clear reporting when they fail.
