# AOT / SrcGen TODO

This file captures remaining work to complete the srcgen-first AOT compiler rollout.

## Current Snapshot

- `srcgen` is wired and usable via `shifty compile --compiler srcgen --backend specialized`.
- Many core constraints are specialized.
- 223p run still reports `specialization_ready: false` and falls back to runtime validation.
  - In recent `srcgen.ir.json`, fallback annotations were all `Sparql` (76 components).
- Inference path is still runtime-driven for rules.
- Specialized report building does not yet match full runtime report fidelity.

## Priority 1: Replace Global Fallback With Hybrid Per-Component Dispatch

### Problem

Current control flow falls back for the entire validation run when `!SPECIALIZATION_READY`.
See `crates/shacl-srcgen-compiler/src/codegen/modules/run.rs`.

### Goal

Run specialized validators for supported components/shapes even when unsupported components exist; only dispatch unsupported parts to runtime fallback.

### Tasks

1. Add IR metadata for dispatch planning:
   - Track supported/unsupported components per node/property shape.
   - Keep fallback reasons attached to component IDs.
2. Change generated run path:
   - Always execute specialized validation over supported components.
   - Execute runtime fallback only for unsupported components/shapes.
   - Merge violation sets deterministically.
3. Keep metrics:
   - `fast_path_hits` should increase when any specialized path executes.
   - `fallback_dispatches` should reflect only unsupported dispatches, not whole-run fallback.

### Acceptance Criteria

- `specialized` backend no longer acts as all-or-nothing.
- In mixed workloads, specialized + fallback both execute in one run.
- Output is parity-equivalent to runtime-only validation.

## Priority 2: Specialize SPARQL Constraint Components (Main Blocker for 223)

### Problem

`ComponentDescriptor::Sparql` is currently marked unsupported in lowering.
See `crates/shacl-srcgen-compiler/src/lower/mod.rs`.

### Goal

Add specialized SPARQL component execution for a safe subset first, then broaden.

### Tasks

1. Implement SPARQL subset support:
   - Handle pre-bound `$this` focus-node semantics.
   - Support common `SELECT`/`ASK` validation patterns used by 223 shapes.
2. Add guarded fallback:
   - If query uses unsupported features, fallback for that component only.
3. Add deterministic query handling:
   - Stable binding order and stable result ordering for report determinism.
4. Add fixtures:
   - Dedicated regression cases for closed-world and domain-specific SPARQL constraints.

### Acceptance Criteria

- 223 `srcgen.ir.json` fallback annotations for `Sparql` trend to zero (or explicitly documented residuals).
- `specialization_ready` can become `true` on representative 223 profiles.

## Priority 3: Generated Inference Instead of Runtime Inference Delegation

### Problem

Generated inference currently calls runtime inference through validator APIs.
See `crates/shacl-srcgen-compiler/src/codegen/modules/inference.rs`.

### Goal

Compile inference rules to generated execution for supported rule kinds, with fallback for unsupported ones.

### Tasks

1. Split rule coverage:
   - `TripleRule`: generate direct insertion logic.
   - `SPARQLRule`: support common construct forms first.
2. Remove blanket readiness gate:
   - Current readiness requires `shape_ir.rules.is_empty()`.
   - Change to per-rule support + fallback strategy.
3. Maintain convergence controls:
   - Iteration limits, convergence checks, and metrics parity with runtime path.

### Acceptance Criteria

- `specialization_ready` is not automatically false merely because rules exist.
- Supported rules execute in generated code.
- Runtime fallback only for unsupported rules.

## Priority 4: Report Fidelity Parity

### Problem

Specialized report generation currently hardcodes severity and omits some runtime-rich fields.
See `crates/shacl-srcgen-compiler/src/codegen/modules/report.rs`.

### Goal

Generated report output should be semantically equivalent to runtime report for the same violations.

### Tasks

1. Preserve per-constraint severity (Violation/Warning/Info).
2. Include parity fields where available:
   - `sh:resultMessage`
   - `sh:sourceConstraint`
   - `sh:resultPath` fidelity for complex paths
3. Ensure blank-node/follow-bnode behavior matches runtime options.
4. Validate deterministic serialization and ordering.

### Acceptance Criteria

- Manifest parity tests pass for report semantics, not just conforms boolean.
- No known report-field regressions versus runtime path.

## Priority 5: Parity + Performance Gates for Default Switch

### Goal

Establish objective gates before making srcgen specialized the default execution path in practice.

### Tasks

1. Expand compiled parity suite:
   - Run W3C SHACL manifests via compiled path and runtime path, compare outputs.
2. Add representative domain regressions:
   - 223 medium profile should fail on known invalid triples (e.g., `nrel00000000` case).
3. Add performance baselines:
   - Compile time, runtime latency, memory.
   - Compare `legacy`, `srcgen tables`, `srcgen specialized`.
4. Stabilize CI:
   - Shared build dirs/caches to avoid artifact ballooning.
   - Deterministic ontology/import behavior in test scripts.

### Acceptance Criteria

- Agreed parity threshold met (ideally zero unexplained deltas).
- Performance story is clearly documented and acceptable.
- CI reliably green for compiled backend tests.

## Suggested Execution Order

1. Hybrid per-component dispatch (Priority 1)
2. SPARQL specialization subset (Priority 2)
3. Generated inference (Priority 3)
4. Report fidelity (Priority 4)
5. Gate hardening + default switch prep (Priority 5)

## Quick Verification Commands

```bash
# Build all
cargo build --workspace

# Format/lint
cargo fmt --all
cargo clippy --workspace --all-targets --all-features

# Core library tests
cargo test -p shifty-shacl --lib

# Compiled manifest tests (new backend path)
cargo test -p shifty-shacl --features compiled-tests --test compiled_manifest_test -- --nocapture

# 223 compiled scenario (deterministic script)
DATA_FILE=ttl/medium-223p.ttl bash 223test.sh

# Inspect fallback reasons quickly
jq -r '.fallback_annotations[].reason' /tmp/compiled-shacl-223p/srcgen.ir.json | sort | uniq -c | sort -nr
```

## Notes for Next Context Window

- Start by inspecting current fallback composition in `srcgen.ir.json`.
- If fallback reasons are still mostly `Sparql`, prioritize SPARQL component specialization before inference/report polish.
- Keep commits small and scoped (one workstream per commit) for easier debugging and bisecting.
