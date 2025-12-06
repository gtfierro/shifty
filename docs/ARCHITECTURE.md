# Module Layout Plan

## Model Layer
- `model/shapes.rs`: owns `NodeShape`, `PropertyShape`, `TargetSpec`, and supporting value types required to describe SHACL structures without behaviour.
- `model/components.rs`: describes parsed constraint metadata (e.g., datatype, cardinality, sparql fragments) as inert descriptors.
- `model/mod.rs`: re-exports structural DTOs and coordinates cross-cutting helpers like ID lookup tables.

## Parser Layer
- `parser/mod.rs`: converts Oxigraph triples into model descriptors; depends on `ParsingContext` utilities and produces a `ModelBundle` aggregate.
- `parser/components.rs`: translates SHACL predicates into `ComponentDescriptor` instances, intentionally unaware of evaluator implementations.

## Runtime Layer
- `runtime/engine.rs`: builds executable constraint evaluators from descriptors and coordinates validation passes.
- `runtime/validators/`: houses component-specific logic (e.g., `cardinality.rs`, `string_based.rs`) implementing a shared `ConstraintEvaluator` trait.
- `runtime/shapes.rs`: hosts `ValidateShape` implementations that orchestrate evaluator invocation against model data.

## Context & Facade
- `context.rs`: narrowed to store runtime state (graphs, traces, evaluator registry) and remain free of parsing responsibilities.
- `lib.rs`: wires the pipeline (`Source` → parser → optimizer → runtime) and exposes stable entry points (`Validator`, `ShapesModel`).

## CLI & Integration
- CLI layer (`cli/src/main.rs`) consumes the facade without observing parser or runtime internals.
- Integration tests (`lib/tests/`) interact through the facade and can opt into parser-only verification helpers for descriptor inspection.
