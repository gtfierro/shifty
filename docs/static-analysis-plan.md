# Static Analysis Plan

## Goal

Build backend-agnostic static analysis over `ShapeProgram` that can:

- slice irrelevant shapes, rules, constraints, targets, and components away
- classify the graph/context footprint of each shape and rule
- identify recursive regions and dependency structure
- detect structurally duplicate constraints, targets, and rules
- surface the results in a dedicated inspection CLI command

## Decisions

- [x] Put the checklist plan in `docs/`
- [x] Prioritize slicing and context reduction before deeper equivalence work
- [x] Add a new `static-analyze` CLI command instead of overloading `analyze`
- [x] Keep the APIs backend-agnostic and operate only on normalized `ShapeProgram`

## Phase 1 Checklist

- [x] Create the static-analysis checklist document
- [x] Add a dedicated static-analysis module in `shacl-core`
- [x] Define `StaticAnalysisSummary`
- [x] Define `SliceRoots` and `ProgramSlice`
- [x] Define `ContextFootprint` and `ContextFootprintReport`
- [x] Define `FingerprintReport`
- [x] Implement target-root slicing
- [x] Implement retained vs dropped inventory for shapes, rules, constraints, targets, and components
- [x] Implement reduced-program construction for slices
- [x] Implement shape context-footprint classification
- [x] Implement rule context-footprint classification
- [x] Reuse dependency components/SCCs in static analysis output
- [x] Implement stable structural fingerprints for constraints
- [x] Implement stable structural fingerprints for targets
- [x] Implement stable structural fingerprints for rules
- [x] Add duplicate-group reporting from fingerprints
- [x] Export the static-analysis APIs from `shacl-core`
- [x] Add a `static-analyze` CLI subcommand
- [x] Add text output for slice/context/fingerprint summaries
- [x] Add JSON output for the full static-analysis payload
- [x] Add `--prune-deactivated`
- [x] Add fixture-backed regression tests

## Data Types / APIs

- `analyze_static(program: &ShapeProgram) -> StaticAnalysisSummary`
- `slice_program(program: &ShapeProgram, roots: SliceRoots) -> ProgramSlice`
- `context_requirements(program: &ShapeProgram) -> ContextFootprintReport`
- `fingerprint_program(program: &ShapeProgram) -> FingerprintReport`

## CLI Output

- text summary
- JSON
- target-root slice by default
- optional deactivation pruning before static analysis

## Tests

- slicing from target-bearing roots removes irrelevant subgraphs
- recursive SCCs remain intact in slices
- SPARQL features classify as global context
- local scalar constraints classify as node-local context
- path/property/reference constraints classify as path-local context
- identical instantiated custom-component constraints land in duplicate groups
- changed bindings produce different fingerprints

## Deferred Work

- semantic subsumption and implication analysis
- backend-specific cost hints
- explicit user-selected root-shape slicing in the CLI
- proof-oriented equivalence instead of candidate duplicate groups
