# Plan for Implementing SHACL Advanced Features (SHACL-AF)

This roadmap builds on the current architecture (see `ARCHITECTURE.md`) and outlines the incremental work needed to support the W3C SHACL Advanced Features specification (<https://www.w3.org/TR/shacl-af/>). Each workstream references the engine modules that will require changes or extensions.

## 0. Baseline Assessment

- [x] **Inventory existing SHACL Core coverage** — `ComponentDescriptor` variants (`model/components/mod.rs`) match SHACL Core constraints; parser modules (`parser/mod.rs`, `parser/components.rs`) populate node/property shapes, and runtime executors (`runtime/`) plus `validate.rs` implement the core validation pipeline.
- [x] **Audit current SPARQL/custom constraint support** — `ComponentDescriptor::Custom` and `runtime/validators/sparql.rs` handle ASK/SELECT validators with prefix collection; manifest tests (`lib/tests/manifest_test.rs`) skip pre-binding scenarios, confirming SHACL-AF requirements (e.g., mandatory pre-binding) are not yet satisfied.
- [x] **Set up advanced test harness scaffold** — Added `lib/tests/test-suite/advanced/` with placeholder manifest and README describing how to populate W3C fixtures; root manifest now includes the directory, so build tooling recognises the future suite.
- [x] **Mirror W3C SHACL-AF fixtures** — Imported the SHACL-AF test graphs from the W3C suite, generated per-directory manifests that include every `.test.ttl`, and wired the hierarchy into the root manifest so the harness can discover them behind the existing opt-in flags.

## 1. SPARQL-Based Constraint Enhancements

Tasks that refine the existing SPARQL machinery to reach spec compliance before adding brand-new concepts.

- [x] **Pre-bound variable semantics**
  - [x] Extend validation-time binding logic in `runtime/validators/sparql.rs` to enforce `sh:select`/`sh:ask` pre-binding rules (partial scaffolding already exists via `ensure_pre_binding_semantics`).
  - [x] Add parser checks in `parser/components.rs` to reject ill-formed validators (e.g., missing `$this`, `$PATH` usage when required).
- [x] **Message templates & severity inheritance**
  - [x] Support `sh:message`, `sh:severity`, and other metadata on SHACL-AF SPARQL constraints, threading them through `ComponentDescriptor::Custom` and the `ValidationFailure` used in `runtime/component.rs`.
- [x] **Inferred parameters and optional bindings**
  - [x] Align parameter binding logic in `parser/components.rs` with SHACL-AF’s rules on required/optional parameters, default values, and path resolution.
- [x] **Testing**
  - [x] Add targeted unit tests in `runtime/validators/sparql.rs` and integration fixtures that cover ASK vs SELECT validators, optional parameters, and prefix declarations.

## 2. Templates (`sh:ConstraintComponent` & `sh:Shape` templates)

- [x] **Modeling template definitions**
  - [x] Introduce `model/templates.rs` and extend `ShapesModel` to carry template metadata (parameters, validators, messages).
  - [x] Extend `model/components/sparql.rs` so shared validator structures can populate template definitions.
  - [x] Modify `parser/components.rs` to parse `sh:declare` blocks and instantiate templates into concrete `ComponentDescriptor::Custom` entries.
- [x] **Runtime instantiation**
  - [x] Enhance `runtime/engine.rs` to instantiate template-based components lazily so that repeated uses share compiled SPARQL.
  - [x] Cache expansion results inside `ValidationContext` (`context.rs`) to avoid re-materializing templates per focus node.
- [ ] **CLI ergonomics**
  - [ ] Update validation diagnostics to show template names/labels by resolving the originating template IRI when present (`report.rs` & CLI output formatting).
- [x] **Tests**
  - [x] Introduce fixtures with nested templates and optional params; verify reporting via `cargo test --workspace`.
  - _Status_: Shape template instantiation results now persist in `ShapesModel::shape_template_cache`, so identical parameterisations reuse existing node shapes. Next up: surface template provenance in CLI diagnostics.

## 3. SHACL Functions (`sh:Function` and node expressions)

- [ ] **Expression evaluation engine**
  - [ ] Add an `expressions` module (`lib/src/expressions/`) implementing the SHACL-AF node/shape expressions evaluation model.
  - [ ] Implement function execution that returns RDF terms/booleans, leveraging SPARQL queries for function bodies.
- [ ] **Parser support**
  - [ ] Extend `parser/mod.rs` to discover `sh:Function`, `sh:parameter`, and link them into new descriptors (`model/functions.rs`).
- [ ] **Runtime integration**
  - [ ] Provide a function registry on `ValidationContext` so components and rules can invoke `sh:Function` definitions with proper pre-binding.
  - [ ] Allow functions within constraint parameters (e.g., `sh:nodeEx`, `sh:valueExpr`) by updating property- and node-shape execution paths in `validate.rs`.
- [ ] **Testing**
  - [ ] Implement targeted evaluator tests (unit) plus integration fixtures covering ASK-based and SELECT-based functions, optional parameters, and recursion guards.

## 4. Advanced Target Selectors (SPARQL-based targets & expressions)

- [x] **Parsing**
  - [x] Update `parser/mod.rs` to recognize `sh:target`, `sh:targetValidator`, and expression-based targets introduced by SHACL-AF.
  - [x] Map them into an extended `Target` enum (`types.rs`) or a new `TargetExpression` type stored on `NodeShape`/`PropertyShape`.
- [x] **Evaluation**
  - [x] Modify `Target::get_target_nodes` in `types.rs` to dispatch to SPARQL-based evaluators, reusing the expression engine from §3 where possible.
- [x] **Caching & performance**
  - [x] Cache target query results per validation run inside `ValidationContext` to avoid re-running identical SPARQL for each shape.
  - _Status_: Cached focus nodes are memoised per selector term, preventing duplicate SPARQL execution while preserving existing target plumbing.

## 5. SHACL Rules (`sh:Rule`, `sh:TripleRule`, `sh:SPARQLRule`)

- [x] **Representation**
  - [x] Introduce `model/rules.rs` capturing rule metadata: conditions, construct triples, SPARQL queries.
  - [x] Parse rule declarations during shapes parsing (`parser/mod.rs`) and persist them in `ShapesModel`.
- [x] **Execution engine**
  - [x] Add an inference module (`lib/src/inference/mod.rs`) to execute rules after constraint validation (or as a configurable phase before/after depending on spec requirements).
  - [x] Ensure rules operate on the shared `Store`.
- [ ] Add provenance tracking for inferred triples in validation reports.
- [x] **Lifecycle integration**
  - [x] Extend `Validator::validate` (and CLI commands) with an optional rule execution mode flag (e.g., `--run-inference` and related tuning options).
- [x] **Testing**
  - [x] Add fixtures covering triple rules and SPARQL rules, asserting both data graph augmentation and validation results when rules create additional focus nodes.

## 6. Profiles, Conformance Levels, and Configuration

- [ ] **Engine flags**
  - [ ] Introduce configuration knobs (e.g., `ValidatorBuilder` in `lib/src/lib.rs`) to enable/disable SHACL-AF features per run, keeping default behaviour aligned with SHACL Core.
- [ ] **CLI exposure**
  - [ ] Extend CLI argument parsing in `cli/src/main.rs` to toggle advanced features (targets, functions, rules). Provide helpful errors when users request SHACL-AF behaviour but advanced support is disabled.
- [ ] **Documentation**
  - [ ] Update `README.md`, `ARCHITECTURE.md`, and create `docs/shacl-af.md` capturing usage patterns, limitations, and performance considerations.

## 7. Testing & Compliance Strategy

- [ ] **Automated test suite**
  - [x] Mirror official SHACL-AF manifests in `lib/tests/test-suite/advanced/` and wire them into `manifest_test.rs` (gated behind `SHACL_W3C_ALLOW_AF=1` for now).
  - [ ] Add end-to-end regression tests for rule execution, function evaluation, and template expansion.
- [ ] **Performance regression testing**
  - [ ] Benchmark representative datasets with and without SHACL-AF features enabled (using existing `target` profiling artefacts) to ensure acceptable overhead.

## 8. Incremental Delivery Milestones

- [ ] *Milestone A*: SPARQL constraint compliance (Section 1) + template support (Section 2).
- [ ] *Milestone B*: Functions and expressions (Section 3) with extended targets (Section 4).
- [ ] *Milestone C*: Rule engine (Section 5) and CLI/documentation updates (Section 6).
- [ ] *Milestone D*: Comprehensive tests, benchmarks, and polishing (Section 7).

Each milestone should be merged with green `cargo fmt`, `cargo clippy`, and `cargo test --workspace`. Feature flags should gate partially complete functionality so that core validation remains stable throughout the rollout.
