# SHACL-RS Architecture

This document explains the moving parts that make up the SHACL processor and how they collaborate during a validation run. It focuses on the coarse-grained components — parsers, models, runtime, and reporting — without drilling down into the implementation of individual SHACL constraint types. Module paths below refer to files under `lib/src/` unless otherwise noted.

## Crate Layout

- `lib/` — the reusable validation engine. Its public `shacl::Validator` facade (`lib/src/lib.rs`) wires together parsing, optimization, validation, and reporting. Internally it is organised into modules such as `parser` (`parser/`), `optimize` (`optimize.rs`), `runtime` (`runtime/`), `validate` (`validate.rs`), and `report` (`report.rs`).
- `cli/` — the command-line interface (`cli/src/main.rs`) that exposes validation, tracing, and visualization commands. It consumes the `Validator` facade so the CLI stays thin while the engine evolves independently.
- `lib/tests/test-suite/` — W3C-aligned fixtures that exercise the engine via integration tests.

## Validation Lifecycle

1. **Source loading** — `Validator::from_sources` (`lib/src/lib.rs`) accepts either local files or named graphs (via `OntoEnv`) for the shapes graph and data graph. Everything is loaded into a shared `oxigraph::Store`, giving the engine SPARQL querying, quad storage, and reasoning primitives.
2. **Canonicalization** — both graphs are skolemized by `canonicalization::skolemize` (`canonicalization.rs`) so that blank nodes become stable IRIs. Deterministic IDs are critical for consistent SPARQL queries, tracing, and report serialization.
3. **Parsing** — a `ParsingContext` (`context.rs`) walks the shapes graph through the `parser` module (`parser/mod.rs`). The parser records:
   - `NodeShape` and `PropertyShape` definitions referenced by compact numeric IDs.
   - Aggregate lists of component descriptors describing which SHACL constraints apply to each shape.
   - Auxiliary lookup tables tying RDF terms to internal IDs.
4. **Optimization** — the `Optimizer` (`optimize.rs`) normalizes the parsed shapes graph (e.g., deduplicating components, precomputing shape relationships) before the engine freezes it into an immutable `ShapesModel`.
5. **Model freezing** — `ShapesModel` (`context.rs`) is an immutable snapshot of the shapes graph: it owns all shapes, component descriptors, lookup tables, the shared store, and the `OntoEnv` handle. This model is safe to reuse across multiple validation runs over different data graphs.
6. **Runtime wiring** — constructing a `ValidationContext` (`context.rs`) pairs the static `ShapesModel` with the active data graph IRI and builds executable runtime components from the stored descriptors (see below). The context also hosts execution traces and utilities for graph introspection.
7. **Execution** — `validate::validate` (`validate.rs`) drives the processing loop. It iterates over every node shape and property shape, resolves their targets, and delegates constraint evaluation to runtime components. The runtime produces `ComponentValidationResult` values that are aggregated into a `ValidationReportBuilder`.
8. **Reporting & visualization** — once validation succeeds, the `report` module (`report.rs`) materializes a `ValidationReport` that can serialize to Turtle, RDF/XML, or N-Triples. The same context powers heatmaps, Graphviz exports, and execution traces that the CLI surfaces through subcommands like `graphviz-heatmap` and `trace`.

## Validation Engine Internals

- **Target discovery** — each shape carries a list of `Target`s (derived from `sh:targetClass`, `sh:targetNode`, etc.). `Target::get_target_nodes` (`types.rs`) executes tailored SPARQL queries against the data graph via `oxigraph::sparql`, returning `Context` objects that encapsulate the focus node, an optional property path, and provenance back to the originating shape.
- **Context objects** — `context::Context` (`context.rs`) tracks the current focus node, the path that produced the value nodes, any intermediate value nodes, and the `SourceShape` responsible for the check. Contexts also own a trace index that ties runtime activity to the execution-trace buffer for later visualization.
- **Node shape loop** — `ValidateShape` for `NodeShape` (`validate.rs`) iterates over resolved contexts. For every constraint ID on the shape it fetches the cached runtime `Component` (`context.rs`), records a trace entry, and hands the per-target `Context` to the component’s `ValidateComponent::validate` implementation (`runtime/component.rs`).
- **Property shape loop** — Property shapes first determine the focus nodes exactly like node shapes. They then expand each focus node through the shape’s SPARQL property path (`PropertyShape::sparql_path` in `model/shapes.rs`), building new value-node lists and recursing into nested property shapes when `sh:property` constraints are encountered. This keeps traversal iterative while still expressing hierarchical shapes.
- **Component execution** — runtime components implement `ValidateComponent` (`runtime/component.rs`) and return `ComponentValidationResult` items. A pass simply bubbles the (possibly updated) context back; a failure packages a `ValidationFailure` containing the violated component ID, offending value node, optional overridden result path, and severity. Logical components (`sh:and`, `sh:not`, etc.) leverage helper reports to combine results from multiple delegated shape executions.
- **Report aggregation** — the shared `ValidationReportBuilder` (`report.rs`) collects failures, enriches them with human-readable labels (resolving IDs back through lookup tables), and computes aggregate metrics like component invocation counts. Once validation completes, the builder finalizes into a `ValidationReport` that the CLI and library consumers serialize or post-process.

## Constraint Components in Aggregate

- **Structural descriptors** — during parsing, every constraint encountered (e.g., `sh:maxCount`, `sh:class`, `sh:qualifiedValueShape`) turns into a `ComponentDescriptor` (`model/components/mod.rs`). These descriptors capture just the parameters required to execute a constraint and are keyed by stable `ComponentID`s.
- **Shape-to-component wiring** — each `NodeShape` or `PropertyShape` (`model/shapes.rs`) stores an ordered list of component IDs. Shapes therefore depend on components indirectly, allowing the optimizer to deduplicate shared constraints and enabling runtime caching.
- **Runtime components** — when a `ValidationContext` is created, it eagerly converts every `ComponentDescriptor` into a concrete runtime `Component` (via `runtime::build_component_from_descriptor` in `runtime/engine.rs`). Runtime components encapsulate validation logic, SPARQL queries, and trace labelling. Because the conversion happens once per context, validation runs avoid repeated descriptor interpretation.
- **Execution strategy** — during validation, shapes iterate their component IDs and fetch pre-built runtime components from the context. Each runtime component validates focus/value nodes and yields zero or more `ComponentValidationResult`s. This shared pipeline means new constraint kinds can be added by extending the descriptor enum plus the runtime factory without touching the shape traversal code.

## Storage, IDs, and Context

- **ID lookups** — `context::IDLookupTable` (`context.rs`) assigns compact numeric IDs (`ID`, `PropShapeID`, `ComponentID`) to RDF terms. These IDs keep maps dense and provide deterministic labels for traces and graph visualizations.
- **Data vs. shapes graph** — `ValidationContext` (`context.rs`) knows both the data graph and shapes graph IRIs (and their skolem bases). Helper methods distinguish skolem IRIs originating from shapes or data, which is useful when interpreting results or generating human-friendly labels.
- **Stores and environments** — `ShapesModel` (`context.rs`) keeps the `oxigraph::Store` and the `OntoEnv` it originated from. While the validator façade currently constructs both together, the separation means advanced callers could reuse a model against multiple datasets without reparsing the shapes graph.

## Reporting, Instrumentation, and CLI Integration

- **Validation reports** — `ValidationReport` (`report.rs`) packages the final conformant/non-conformant state, detailed failures, and component frequency metrics. The CLI exposes the report through the `validate` subcommand with selectable serialization formats.
- **Diagnostics** — execution traces, Graphviz exports, and heatmaps all read from the same trace buffer maintained by `ValidationContext` (`context.rs`). CLI commands (`cli/src/main.rs`) simply trigger validation and then request the desired derived artifact.
- **Extensibility points** — because parsing, runtime execution, and reporting are modular, adding new features often requires changing only one layer. For example, adding a new visualization uses existing traces; introducing a new constraint extends descriptors plus runtime validators; hooking in a different input source only requires adapting `Validator::from_sources`.

With these components and data flows in mind, contributors can quickly orient themselves: the CLI triggers the `Validator`, which assembles a `ValidationContext` from parsed shapes, runs the runtime components against focus nodes drawn from the data graph, and hands back a report along with the traces that power the project's diagnostic tooling.
