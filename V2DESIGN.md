# SHACL-RS v2 Design

This document proposes a ground‑up “v2” of shacl-rs that cleanly separates parsing from evaluation, enables both compiled and interpreted validators, and keeps today’s mostly-working behaviour as a safety net while we refactor.

## Goals
- Clean separation: parsing produces a pure shapes/data IR with no evaluation concerns; evaluation consumes that IR through a stable API.
- Pluggable evaluation backends: interpreted (current style), compiled (preplanned queries / generated code), future inference-friendly engines.
- Deterministic, reusable artifacts: parsed shape graphs can be cached, versioned, and reused across runs and backends.
- Simpler reasoning about data access: a narrow graph abstraction sits between evaluators and Oxigraph/RDF libraries.
- First-class diagnostics: traces, heatmaps, and validation reports flow from a common event stream regardless of backend.
- Maintain SHACL Core + AF parity with v1 while opening space for experiments.

Non-goals for v2: rewrite the CLI UX, change the public CLI flags, or remove the existing integration tests; these remain the regression harness during the rebuild.

## Key Findings From v1
- Parser + runtime coupling: `ParsingContext` owns the store, ID tables, and runtime-facing structures; `ShapesModel` is a frozen `ParsingContext` plus store/env. This makes it hard to swap evaluators or reuse parsed shapes independently.
- Execution interleaves traversal and SPARQL string building (`PropertyShape::validate` synthesizes queries per run), making compilation/preplanning difficult.
- Data access assumes Oxigraph everywhere; evaluators pull terms directly from the store instead of via a small trait.
- Components are “structural” but their runtime counterparts are eagerly built and cached inside `ValidationContext`; descriptor/evaluator boundaries are blurred.
- Inference/rules sit beside validation but share state through the same context, limiting scheduling options (e.g., alternating rule/materialization with validation).
- Diagnostic streams (traces, graphviz) are tied to the interpreter’s control flow rather than a backend-agnostic event model.

These frictions explain why experimenting with “compiled” validators or alternate backends is hard today.

## Design Principles
1) Single-responsibility stages (load → parse → normalize → plan → execute) with serializable artifacts between them.  
2) Replace global Oxigraph dependency with a `GraphBackend` trait that covers the operations evaluators need (pattern lookups, path expansion, prepared SELECT/ASK).  
3) IR-first: all lowering/validation happens on immutable IR structs, not against the live store.  
4) Pluggable planners/executors: the IR is turned into an execution plan; interpreters and compilers implement the same `Executor` trait.  
5) Deterministic IDs and provenance: every IR element keeps stable identifiers and source locations for trace/report generation.  
6) Backward compatibility through adapters: v1-style validation remains available via an “interpreter” backend so tests keep passing during migration.  
7) Observability built-in: traces are emitted as structured events decoupled from backend control flow.

## Target Architecture (Layers & Crates)
```
sources → loader → parser → IR → planner → executor (interpreted / compiled) → report
```

- **crates/shacl-loader**: Reads shapes/data from files, URLs, or OntoEnv; outputs immutable `LoadedGraph` handles plus base IRI info. Thin wrapper over Oxigraph today, replaceable later.
- **crates/shacl-syntax**: Parses the shapes graph into a syntax-level AST (NodeShapeDef, PropertyShapeDef, ComponentUse, RuleDef, TemplateDef) with raw RDF terms and source spans. No store access after parsing.
- **crates/shacl-ir**: Semantic analysis + normalization. Resolves prefixes, paths, targets, templates; deduplicates components; assigns stable IDs; outputs `ShapeIR` (node/property definitions + component descriptors + rule set + template registry). Completely store-free and `Serialize`.
- **crates/shacl-plan**: Turns `ShapeIR` into an executable `ValidationPlan`. Responsibilities: target strategies, path programs, component programs, rule scheduling. Produces backend-neutral instructions (e.g., `FindTargets`, `EvalConstraint`, `RunRule`).
- **crates/shacl-backend-api**: Defines the minimal traits the plan needs:
  - `GraphBackend`: triple pattern search, path evaluation, prepared SELECT/ASK with variable binding, bulk insert for rules.
  - `ValueCodec`: canonicalizes value nodes (skolemization, original literal recovery).
  - `TraceSink`: receives structured events (shape entered, component passed/failed, query executed).
- **crates/shacl-exec-interpreter**: Reference executor that walks a `ValidationPlan` and uses `GraphBackend` to answer queries. This keeps v1 behaviour but moves it behind the API.
- **crates/shacl-exec-compiled**: Experimental executor that preplans constraints:
  - Prebuilds prepared queries per constraint/path/target with fixed result projections.
  - Optionally lowers plans to a small bytecode or generated Rust for hot paths.
  - Shares compiled artifacts across runs (cache keyed by `ShapeIR` hash + backend type).
- **crates/shacl-inference**: Rule engine consuming `ValidationPlan` rule stages; can run pre-, post-, or interleaved with validation based on plan metadata.
- **crates/shacl-report**: Converts trace events + failures into SHACL reports, heatmaps, and graphviz; backend-agnostic.
- **crates/cli** & **python/**: Thin frontends that pick an executor (`--engine interpreted|compiled`) and wire up loaders/planners.

## Data & Control Flow
1) Loader builds `LoadedGraph` for shapes and data.  
2) Parser reads shapes graph → Syntax AST (no store mutation).  
3) IR pass resolves/normalizes → `ShapeIR` + `RuleIR`; stores serialized to disk/cache keyed by shape graph hash.  
4) Planner builds `ValidationPlan` (DAG or linear program) with explicit stages:
   - target discovery program per shape
   - value-node program per property path (precompiled SPARQL or path bytecode)
   - constraint programs per component
   - rule stages (construct/ask/select) with dependency ordering
5) Executor executes the plan using a `GraphBackend`, emitting events to `TraceSink`. Different executors can be swapped without touching earlier stages.
6) Reporter consumes events + plan metadata → report graph, CLI summaries, heatmaps.

## Compiled vs Interpreted Validators
- **Interpreted**: walk the plan, emit dynamic SPARQL strings only where necessary; mirrors v1 for correctness.
- **Compiled**:
  - Prebind `$this`, `$PATH`, parameter values at plan-build time where possible.
  - For common constraints, emit specialized evaluators that avoid SPARQL (e.g., numeric bounds over cached literals).
  - Property paths lowered to a tiny path bytecode executed over the backend (supports future non-SPARQL backends).
  - Cache prepared queries and compiled evaluators keyed by `ShapeIR` fingerprint to reuse across runs and data sets.

## Inference & Rules
- Rules live in the IR and plan as first-class stages.  
- Planner chooses scheduling policy (pre, post, alternating to fixpoint).  
- Backends expose bulk insert and optional transaction hooks so rule-produced triples can be isolated or committed atomically.  
- Trace events mark inferred triples with provenance (rule ID, iteration) for later reporting.

## Diagnostics & Observability
- Standardized `TraceEvent` enum (entered shape, exited shape, component pass/fail, query start/finish, rule applied, inferred triples).  
- `TraceSink` implementations: in-memory buffer for CLI heatmaps, streaming logger for debugging, optional JSONL for long runs.  
- Reporter uses plan metadata + traces to label results without touching backend internals.

## Migration Strategy (keep tests green)
1) **Backend trait + interpreter shim**: Wrap current Oxigraph access behind `GraphBackend`; route existing validator through the shim. Tests keep passing.  
2) **Parsing/IR split**: Extract syntax parser that outputs serializable `ShapeIR`; make the interpreter consume IR instead of `ParsingContext`.  
3) **Planner introduction**: Build minimal `ValidationPlan` that mirrors current control flow; interpreter executes the plan.  
4) **Diagnostics refactor**: Emit trace events from interpreter; reimplement report/graphviz on top of events.  
5) **Inference isolation**: Move rule execution to plan stages; remove direct store mutation from inference module.  
6) **Compiled executor MVP**: Add prepared-query caching for constraints/paths; expose `--engine compiled` behind a feature flag.  
7) **Cleanup & API polish**: Remove legacy contexts, collapse duplicate types, document the new crate boundaries, and update CLI/python bindings to choose engines.

## Risks & Mitigations
- **Spec regressions**: Keep v1 interpreter as oracle; reuse W3C + advanced fixtures as regression suite each step.  
- **Performance surprises**: Add microbenchmarks comparing interpreter vs compiled backends on representative datasets; gate compiled engine behind opt-in flag until stable.  
- **Cache invalidation**: Fingerprint `ShapeIR` + backend capabilities to invalidate compiled artifacts safely.  
- **Backend portability**: Start with Oxigraph adapter; design `GraphBackend` to allow RDFlib/SPARQL endpoint adapters without leaking Oxigraph types.

## Immediate Next Steps
- Define `GraphBackend` and `TraceSink` traits in a new crate; implement Oxigraph adapter with current store.  
- Extract parsing output into a serializable `ShapeIR` struct (reuse existing `ComponentDescriptor`, `NodeShape`, `PropertyShape` with store-free fields).  
- Prototype a trivial `ValidationPlan` struct and execute it via the interpreter shim to prove the split end-to-end.
