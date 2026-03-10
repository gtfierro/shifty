# AOT Tables + Shared Kernel Design

## Status
- Proposed
- Scope: `lib` + `crates/shacl-srcgen-compiler`

## Problem Statement
The current compiler path generates substantial executable logic (helpers, caches, report assembly, target resolution, and constraint evaluation glue) directly into output Rust modules. This creates three maintainability issues:

1. Core behavior is duplicated between the library runtime and generated runtime.
2. Feature work requires edits in two places (core runtime and compiler templates/codegen).
3. Optimization work is tightly coupled to code generation shape, which slows iteration.

## Goals
1. Make generated artifacts mostly data, not logic.
2. Centralize execution semantics in one shared kernel in `lib`.
3. Preserve or improve current performance characteristics.
4. Make future optimizations pluggable, especially those driven by:
- shape graph analysis (static)
- data graph analysis (runtime)
5. Keep compatibility/versioning explicit and testable.

## Non-Goals
1. Replacing SHACL semantics or changing conformance behavior.
2. Introducing a full bytecode VM in phase 1.
3. Removing existing validator path immediately.

## Design Summary
Move to an **AOT program + shared kernel** architecture:

1. Compiler emits a compact `CompiledProgram` (tables + hints + metadata).
2. `shifty::compiled_runtime` executes the program against a data graph.
3. Optimizations are implemented as analyzers/rewriters inside the kernel, not in generated source templates.

Generated Rust remains minimal bootstrapping:
- load embedded program tables
- invoke `compiled_runtime::run(...)`
- expose CLI/service entry points

## Proposed Module Layout

### In `lib`
- `lib/src/compiled_runtime/mod.rs`
- `lib/src/compiled_runtime/program.rs`
- `lib/src/compiled_runtime/kernel.rs`
- `lib/src/compiled_runtime/analysis/mod.rs`
- `lib/src/compiled_runtime/analysis/shape.rs`
- `lib/src/compiled_runtime/analysis/data.rs`
- `lib/src/compiled_runtime/opt/mod.rs`
- `lib/src/compiled_runtime/report.rs`
- `lib/src/compiled_runtime/path.rs`
- `lib/src/compiled_runtime/constraint_kernels.rs`

### In `crates/shacl-srcgen-compiler`
- `crates/shacl-srcgen-compiler/src/emit/program.rs` (new table emitter)
- `crates/shacl-srcgen-compiler/src/codegen/bootstrap.rs` (small generated entrypoints)

## Program Schema (AOT Tables)

```rust
pub struct CompiledProgram {
    pub header: ProgramHeader,
    pub terms: TermPool,
    pub shapes: Vec<ShapeRow>,
    pub components: Vec<ComponentRow>,
    pub paths: Vec<PathRow>,
    pub targets: Vec<TargetRow>,
    pub rules: Vec<RuleRow>,
    pub shape_graph_triples: Vec<TripleRow>,
    pub static_hints: StaticHints,
    pub ext: ExtensionMap,
}

pub struct ProgramHeader {
    pub schema_version: u32,
    pub min_kernel_version: u32,
    pub program_hash: [u8; 32],
    pub feature_bits: u64,
}
```

### Schema Principles
1. **Stable IDs**: IDs are immutable indices into tables.
2. **No behavior fields**: tables describe structure, not execution code.
3. **Forward-compatible extensions**: unknown `ext` keys are ignored by older kernels.
4. **Deterministic serialization**: required for cache keys and diffability.

## Shared Kernel API

```rust
pub struct KernelOptions {
    pub enable_inference: bool,
    pub follow_bnodes: bool,
    pub analysis_budget: AnalysisBudget,
    pub optimization_level: u8,
}

pub fn run(
    store: &Store,
    data_graph: Option<GraphNameRef<'_>>,
    program: &CompiledProgram,
    opts: &KernelOptions,
) -> Result<KernelReport, String>;
```

The kernel owns:
1. Target resolution
2. Path evaluation
3. Constraint kernels (datatype, class, cardinality, logical, SPARQL, etc.)
4. Inference loop (if enabled)
5. Report materialization
6. Shared caches and memoization

## Analysis and Optimization Framework

### Two-Phase Analysis
1. **Static shape analysis** (once per program):
- shape dependency graph + SCCs
- target signatures and required predicates/classes
- path complexity and normalization metadata
- constraint capability map (fast-path eligibility)
- message/severity lookup pre-indexing

2. **Runtime data analysis** (per data graph, budgeted):
- class instance index and subclass closure materialization
- predicate presence bitsets
- sampled cardinality distributions
- datatype/language profile statistics
- advanced target selectivity estimates

### Planner Boundary
Analysis outputs feed a planner that creates an `ExecutionPlan`:

```rust
pub struct ExecutionPlan {
    pub active_node_shapes: Vec<ShapeId>,
    pub active_property_shapes: Vec<ShapeId>,
    pub component_strategies: Vec<ComponentStrategy>,
    pub partitions: Vec<PlanPartition>,
}
```

This allows optimization work without changing table schema or compiler output format.

## Extensibility Model

### Analyzer Trait
```rust
pub trait Analyzer {
    fn id(&self) -> &'static str;
    fn run(&self, ctx: &AnalysisContext, state: &mut AnalysisState) -> Result<(), String>;
}
```

### Rewriter Trait
```rust
pub trait PlanRewriter {
    fn id(&self) -> &'static str;
    fn rewrite(&self, plan: &mut ExecutionPlan, state: &AnalysisState) -> Result<(), String>;
}
```

### Why this helps maintainability
1. New optimizations are isolated units, not scattered codegen edits.
2. Optimizations can be toggled/benchmarked independently.
3. Kernel behavior remains debuggable with stable instrumentation points.

## Optimization Hooks (Shape + Data Graph Driven)

### Shape-Graph-Driven Hooks
1. Constraint ordering by static cost/selectivity.
2. Transitive dependency pruning for unreachable/deactivated shapes.
3. Precomputed path normalization and canonicalization hints.
4. Rule scheduling hints (topological and SCC-based).

### Data-Graph-Driven Hooks
1. Data-dependent shape pruning using predicate/class presence.
2. Strategy selection:
- index-backed class checks vs SPARQL fallback
- batch vs per-focus evaluation for closed-world checks
3. Adaptive parallel partition sizing based on target counts.
4. Query planning hints for SPARQL constraints (prebind strategy, fast reject).

## Caching Strategy

### Cache Layers
1. **Program cache key**: `program_hash` from `CompiledProgram`.
2. **Data analysis cache key**: `(program_hash, graph_fingerprint, analysis_version)`.
3. **Plan cache key**: `(analysis_cache_key, optimization_level)`.

### Invalidation
- Any schema version bump invalidates program caches.
- Any analyzer version bump invalidates analysis/plan caches.

## Versioning and Compatibility
1. `schema_version` controls table format compatibility.
2. `min_kernel_version` prevents running new programs on old kernels.
3. Kernel supports an explicit compatibility matrix in tests.
4. Fallback behavior:
- If incompatible: clear error with required versions.
- Optional fallback to the older compiled path during the migration window.

## Migration Plan

### Phase 0: Shared Runtime Extraction
1. Add `compiled_runtime` module in `lib`.
2. Move duplicated helper logic from compiler templates into kernel utilities.
3. Keep existing generated validators working.

### Phase 1: Table Emitter
1. Introduce `CompiledProgram` schema and deterministic serializer.
2. Update compiler to emit tables + minimal bootstrap code.
3. Keep parity tests against current path.

### Phase 2: Constraint Kernel Consolidation
1. Replace generated per-component logic with kernel-dispatched evaluation.
2. Delete large helper template once feature parity is reached.

### Phase 3: Analysis/Planner Plugins
1. Add analyzer and rewriter registries.
2. Port existing optimizations into plugins.
3. Add budget controls and profiling output.

### Phase 4: Default Switch
1. Make AOT + shared kernel default for compiled output.
2. Keep temporary migration compatibility controls until parity is proven.

## Testing Strategy
1. **Semantic parity**:
- compare compiled kernel output vs existing `Validator` output across current manifests.
2. **Compatibility tests**:
- old program/new kernel and new program/old kernel failure behavior.
3. **Performance tests**:
- benchmark suites for compile time, startup latency, validation throughput.
4. **Analyzer correctness tests**:
- deterministic fixtures for each analyzer and plan rewrite.
5. **Regression snapshots**:
- serialized program snapshots for schema stability checks.

## Observability
1. Standardized stage timing:
- `load_program`, `shape_analysis`, `data_analysis`, `plan_build`, `validate`, `report`.
2. Per-optimizer counters:
- shapes pruned, strategies selected, cache hit rates.
3. Structured trace IDs tied to `program_hash` for reproducibility.

## Risks and Mitigations
1. **Risk**: Performance regressions from over-generalization.
- Mitigation: retain specialized kernels and add opt-level gating.
2. **Risk**: Schema churn.
- Mitigation: strict versioning policy + snapshot tests.
3. **Risk**: Migration complexity.
- Mitigation: phased rollout with temporary fallback until parity is proven.

## Decision
Adopt **AOT tables + shared kernel** as the target architecture, with phased migration and explicit analyzer/rewriter extension points to support future shape-graph and data-graph optimizations without codegen duplication.
