# Architecture Overview

This document describes the core components of the `shifty` validation engine and how they fit
together. The library is organized as a pipeline: parse shapes → build an internal model →
plan execution → validate/infer → produce reports and diagnostics.

## High-Level Pipeline

1. **Inputs**: Shapes + data graphs are loaded from `Source` (files or named graphs). If shapes are not explicitly supplied, the data source is reused as the shapes source.
2. **Parsing**: `parser` converts RDF into a data-only model (`ShapesModel`).
3. **IR**: `ir` and `shifty::shacl_ir` serialize/deserialize that model for caching and reuse.
4. **Planning**: `planning` builds a dependency-aware execution order.
5. **Validation**: `validate` executes shape targets and components in parallel.
6. **Reporting**: `report` renders results to Turtle/Graph and captures traces.
7. **Inference (optional)**: `inference` evaluates SHACL rules over the data graph.

## Data Model

**Primary types**

- `NodeShape` and `PropertyShape` in `model/shapes.rs` are immutable descriptors.
- Each shape contains:
  - `targets`: target selectors (class, node, subjects-of, etc.).
  - `constraints`: component IDs (cardinality, datatype, SPARQL, etc.).
  - `property_shapes` (node shapes only): the `sh:property` links, preserved for IR and diagnostics.
- Components are parsed into `ComponentDescriptor` (data-only) and later instantiated into
  runtime `Component`s.

**Identifiers**

The model uses stable integer IDs (`ID`, `PropShapeID`, `ComponentID`, `RuleID`) stored in
lookup tables so shapes and components can be referenced cheaply in caches and IR.

## Parsing Layer

`parser/mod.rs` is responsible for turning RDF triples into the model:

- **Node shapes** are collected from `rdf:type sh:NodeShape` and shape-based constraints
  (`sh:and`, `sh:or`, `sh:not`, etc.).
- **Property shapes** are collected from `rdf:type sh:PropertyShape` and `sh:property` links.
- **Constraints** are parsed via `parser/components.rs`, which maps SHACL predicates to
  `ComponentDescriptor` instances (datatype, cardinality, sparql, etc.).
- **Custom components** are parsed when SHACL-AF is enabled.

The parser is intentionally decoupled from execution: it produces descriptors and IDs only.

## ShapeIR (Caching)

`ir.rs` builds a serialized `ShapeIR` (defined in `shifty::shacl_ir`) that captures:

- Shape descriptors (node + property shapes).
- Constraint descriptors.
- Shape graph triples.
- Rule definitions.

`ir_cache.rs` reads/writes `ShapeIR` to disk to avoid re-parsing shapes for repeated runs.

## Validation Planning

`planning.rs` builds a **validation plan**:

- Shapes are treated as nodes in a dependency graph.
- Dependencies are derived from constraint descriptors (e.g., `sh:node`, `sh:property`,
  `sh:qualifiedValueShape`).
- The plan groups shapes into independent trees so validation can run in parallel without
  violating dependencies.

## Validation Runtime

The runtime execution happens in `validate.rs` and `runtime/`:

1. **Context setup**: `ValidationContext` holds the model, caches, and SPARQL services.
2. **Targets**: Shapes compute focus nodes from targets and cache results.
3. **Constraint evaluation**:
   - Each `ComponentDescriptor` is instantiated into a runtime `Component`.
   - Components are invoked against a `Context` (focus/value nodes, source shape).
4. **Property shapes**:
   - As standalone shapes: run against their own targets.
   - As `sh:property` constraints: evaluated with the node shape’s focus node as context.

### Component System

- `runtime/engine.rs` builds runtime components from descriptors.
- `runtime/validators/*` implement individual constraint behaviors.
- `runtime/validators/shape_based.rs` handles shape-based constraints (`sh:node`,
  `sh:property`, `sh:qualifiedValueShape`).

### Reporting & Tracing

- `report.rs` builds the validation report graph and serializes it to Turtle/RDF.
- `trace.rs` collects execution traces, which are referenced by failures to explain
  *why* a violation occurred.
- The CLI can render graphviz/heatmap outputs using the same trace data.

## SPARQL Subsystem

`sparql/` provides:

- Parsing and execution helpers (`SparqlServices`).
- Pre-binding validation (`validate_prebound_variable_usage`) to ensure required variables
  are referenced.
- Query execution helpers used by SPARQL constraints and advanced targets.

Custom components and advanced targets reuse this subsystem for query evaluation.

## Inference (SHACL Rules)

`inference/` evaluates SHACL rules to produce inferred triples:

- Supports triple rules and SPARQL rules.
- Operates over a graph union (data + shapes when enabled).
- Tracks and de-duplicates inferred triples to avoid loops.

## Graph Handling & Skolemization

- Shapes and data graphs can be unioned for validation (`--no-union-graphs` disables).
- Blank nodes are optionally skolemized for stable identifiers in reports and caching.

## Concurrency

Validation uses Rayon to:

- Execute independent shape trees in parallel.
- Evaluate per-target checks concurrently.

Thread-local state (e.g., subclass closures, original value indices) is propagated so
parallel execution preserves semantics.

## Optimization Parity (Core vs Compiler)

The `shacl-compiler` backend tracks the same major optimization themes used by the core
`lib` validator:

- **Import/cache loading controls**:
  - Core: `ValidatorBuilder` supports imports toggling, refresh strategy, and import depth.
  - Compiler: `crates/shacl-compiler/src/main.rs` now forwards `--no-imports`,
    `--force-refresh`, and `--import-depth` into the same builder path before codegen.
- **Data-dependent shape pruning**:
  - Core: shape optimization can prune inactive shapes from runtime execution.
  - Compiler: generated `run` module prunes node/property validators at runtime via
    `active_validators_for_runtime_data`.
- **Class hierarchy indexing**:
  - Core: fixed-bitset class-closure index accelerates `rdf:type` and subclass checks.
  - Compiler: generated helpers build and cache a `ClassConstraintIndex` with the same
    bitset strategy.
- **Closed-world caching**:
  - Core: closed-world checks are batched and memoized per shape/query key.
  - Compiler: generated code caches closed-world violations per shape/graph and reuses
    them across focus nodes.
- **Parallel execution**:
  - Core: validation and inference stages use Rayon where safe.
  - Compiler: generated validation and inference use `par_iter` over active validators and
    inferred candidates.
- **SPARQL cache reuse**:
  - Core: prepared/algebra/prefix caches are reused across calls.
  - Compiler: generated helpers cache prepared queries, prefix expansion, and advanced-target
    query results.
