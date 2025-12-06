# Code Review & Restructure Plan

## High-Level Themes
- The current layering between parsing, modelling, runtime execution, and tooling is porous; SHACL-AF work piles on top of large multi-purpose modules instead of extending clear seams.
- SHACL component support is hand-coded across the parser, model, and runtime. Every new constraint or AF feature requires touching several 600–1500 line files, increasing defect risk.
- Diagnostics, reporting, and CLI affordances sit inside the same crates without isolation, inflating the library surface and forcing advanced users to pay for CLI-oriented allocations.

## Detailed Findings
- **Monolithic component parsing** (`lib/src/parser/components.rs:16`): `parse_components` mixes discovery for every core constraint, bespoke key construction, and SHACL-AF custom-component plumbing in a 700+ line function. The duplication (e.g., per-predicate `if let` blocks for simple constraints) makes it difficult to reason about coverage and nearly guarantees drift when new components are added.
- **Parser ↔ runtime coupling** (`lib/src/parser/components.rs:627`, `lib/src/runtime/validators/sparql.rs:947`): the parser pulls in runtime helpers for SPARQL pre-binding checks and custom component extraction. This circular dependency breaks the intended architecture (parser → model → runtime), complicates testing, and forces SHACL-AF work to modify runtime internals just to compile.
- **SPARQL validator mega-module** (`lib/src/runtime/validators/sparql.rs:1`): the 1,500-line file interleaves prefix harvesting, query parsing, execution, message templating, severity resolution, and performance notes. The all-in-one design makes targeted optimisation or SHACL-AF extensions (templates, functions, rules) risky because every change shares the same global state.
- **Bloated context & graph tooling** (`lib/src/context.rs:1`, `lib/src/context.rs:347`): `context.rs` simultaneously defines ID tables, the immutable shapes model, runtime validation context, trace buffering, Graphviz emitters, and heatmap logic. Graph rendering concerns bleed into the validation hot path, dragging UI-oriented cloning and formatting into core execution.
- **Validator setup does too much** (`lib/src/lib.rs:87`): `Validator::from_sources` handles OntoEnv wiring, skolemization, store optimisation, exhaustive debug printing, and context construction. The single entry point makes it hard to reuse the core engine programmatically (e.g., to plug in an existing store or to skip skolemization in specialised environments).
- **Property validation builds ad hoc SPARQL** (`lib/src/validate.rs:63`): property shapes generate raw `SELECT` strings per focus node, re-parsing each query with Oxigraph. The tight coupling between shape traversal and query generation rules out caching and will explode in complexity once AF introduces expression-based paths.
- **Runtime component enum churn** (`lib/src/runtime/component.rs:66`, `lib/src/runtime/engine.rs:16`): every constraint maps manually across descriptor, enum variant, builder, Graphviz output, and validation trait implementation. Adding AF constructs multiplies touch points and keeps the binary size high because unused variants are eagerly instantiated.

## Restructure Plan

### Phase 1 — Re-establish Layered Boundaries
1. Split `context.rs` into focused modules: `ids.rs` (lookup tables), `model.rs` (immutable shapes graph), `validation.rs` (runtime context & execution traces), and `graphviz.rs` (visualisation). Move CLI-only logic (heatmaps, DOT formatting) behind a feature flag so the library surface stays lean.
2. Introduce a `ValidatorBuilder` that separates source loading, model construction, and run configuration (`lib/src/lib.rs`). This allows tests or alternative front-ends to inject pre-built stores or toggle skolemization.

### Phase 2 — Data-driven Component Registry
1. Replace the imperative `parse_components` avalanche with a registry describing each constraint’s predicates, cardinality, and conversion into descriptors. Encode the registry once (e.g., static tables or declarative macros) so SHACL-AF additions only register metadata.
2. Generate runtime `Component` factories from the same registry. Instead of the 200-line `match` in `runtime/engine.rs`, derive constructors and Graphviz labels via traits implemented per constraint family (cardinality, string, logical, etc.).

### Phase 3 — Isolate SPARQL / SHACL-AF Engine
1. Extract a `sparql` sub-crate or module group that owns prefix resolution, query compilation, pre-binding validation, and execution. Provide clear service traits (`SparqlExecutor`, `MessageTemplater`) that both core SPARQL constraints and AF templates/functions can share.
2. Cache prefix lookups and parsed queries per shapes graph (`lib/src/runtime/validators/sparql.rs:900`). Move the custom constraint discovery logic out of the runtime and expose it via the parser’s registry to keep the dependency direction clean.
3. Add explicit feature toggles in the builder/config (`--enable-af`, `--enable-rules`) so advanced functionality can evolve without destabilising core validation.

### Phase 4 — Simplify Validation Loop
1. Model node and property shape traversal with composable iterators so `validate.rs` no longer assembles SPARQL strings ad hoc. A dedicated `PathEvaluator` service (backed by cached algebra or compiled plans) will shrink `PropertyShape::validate` and clarify hand-off points for AF path expressions.
2. Push trace collection and report aggregation behind interfaces so the runtime can operate without CLI artefacts, and so alternate reporters (metrics, structured logs) can be plugged in without editing the validator core.

### Phase 5 — Testing & Tooling
1. Add focused unit tests around the new registries, SPARQL services, and builders to catch regressions before hitting the heavy W3C manifest suite.
2. Document the new extension points (`docs/shacl-af.md` + updates to `ARCHITECTURE.md`) so contributors can add AF features by wiring through the registry instead of editing multiple monoliths.

## Additional Suggestions
- Remove the pervasive `#[allow(deprecated)]` attributes by updating Oxigraph/spargebra usages; modern APIs will reduce future churn.
- Replace `eprintln!` debug statements with structured `log` calls. This keeps CLI noise down and allows library consumers to integrate with their logging stack.
- Consider moving the CLI into its own crate feature (or a separate binary crate) to keep the library dependency graph slim for downstream users embedding the validator.
