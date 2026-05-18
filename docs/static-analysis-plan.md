# Static Analysis Plan

## Goal

Build backend-agnostic static analysis over `ShapeProgram` that can:

- slice irrelevant shapes, rules, constraints, targets, and components away
- classify the graph/context footprint of each shape and rule
- identify recursive regions and dependency structure
- detect structurally duplicate constraints, targets, and rules
- surface the results in a dedicated inspection CLI command
- drive conservative semantics-preserving rewrites over normalized shape programs

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

## Phase 2 Checklist

- [x] Add explicit root-shape slicing in the library API
- [x] Add CLI root selection by shape IRI / normalized key
- [x] Record dropped-item reasons in `ProgramSlice`
- [x] Record retained-item inclusion reasons for slice explainability
- [x] Preserve per-root reachability summaries for explicit root sets
- [x] Refine context classes beyond `PathLocal`
- [x] Distinguish single-hop path access from bounded traversal
- [x] Distinguish shape-reference traversal from simple property access
- [x] Add shared-work candidate analysis on duplicate fingerprints
- [x] Report duplicate instantiated custom-component uses explicitly
- [x] Report duplicate SPARQL constraints and rules explicitly
- [x] Add backend-agnostic static cost hints per shape and rule
- [x] Surface slice reasons and cost hints in `static-analyze`
- [x] Add JSON fields for shared-work candidates and cost hints
- [x] Add fixture-backed tests for explicit-root slicing
- [x] Add fixture-backed tests for dropped-item reasons
- [x] Add fixture-backed tests for refined context classification
- [x] Add fixture-backed tests for shared-work candidate reporting

## Phase 3 Checklist

- [x] Add an explicit rewrite-pass API over `ShapeProgram`
- [x] Define `RewriteOptions` and `RewriteSummary`
- [x] Add a reusable root-slice rewrite pass
- [x] Add dead-structure elimination after slicing
- [x] Prune unreferenced components after slicing
- [x] Prune unreferenced rules after slicing when their owner shapes are dropped
- [x] Preserve provenance and diagnostics through rewrite passes
- [x] Rebuild indexes and inspection graphs after rewrites
- [x] Add analysis-informed deterministic constraint ordering
- [x] Add analysis-informed deterministic rule ordering
- [x] Prioritize local/cheap constraints ahead of global/SPARQL constraints
- [x] Prioritize local/cheap rules ahead of global/SPARQL rules
- [x] Add explicit recursive-region annotations or groups in rewritten programs
- [x] Surface rewrite summaries in CLI inspection output
- [x] Add JSON output for rewrite summaries
- [x] Add fixture-backed tests for slice-driven rewriting
- [x] Add fixture-backed tests for dead-structure elimination
- [x] Add fixture-backed tests for ordering rewrites
- [x] Add fixture-backed tests for recursive-region annotation

## Data Types / APIs

- `analyze_static(program: &ShapeProgram) -> StaticAnalysisSummary`
- `slice_program(program: &ShapeProgram, roots: SliceRoots) -> ProgramSlice`
- `context_requirements(program: &ShapeProgram) -> ContextFootprintReport`
- `fingerprint_program(program: &ShapeProgram) -> FingerprintReport`

Planned additions:

- `slice_program` support for explicit root shape sets
- dropped/retained reason types on `ProgramSlice`
- refined context categories and explanation payloads
- shared-work candidate report derived from fingerprints
- static cost-hint report for shapes and rules

Rewrite-phase additions:

- `rewrite_program(program: &ShapeProgram, options: RewriteOptions) -> RewrittenProgram`
- explicit rewrite passes for:
  - root slicing
  - dead-structure elimination
  - unreferenced-component pruning
  - analysis-informed ordering
  - recursive-region annotation
- rewrite summaries that explain which passes ran and what they changed

## CLI Output

- text summary
- JSON
- target-root slice by default
- optional deactivation pruning before static analysis

Planned additions:

- explicit `--root-shape` selection
- slice inclusion/exclusion reasons
- refined context histogram
- shared-work candidate summary
- static cost-hint summary

Rewrite-phase additions:

- rewrite summary output in text mode
- rewrite summary output in JSON mode
- optional inspection command support for rewritten-program output

## Tests

- slicing from target-bearing roots removes irrelevant subgraphs
- recursive SCCs remain intact in slices
- SPARQL features classify as global context
- local scalar constraints classify as node-local context
- path/property/reference constraints classify as path-local context
- identical instantiated custom-component constraints land in duplicate groups
- changed bindings produce different fingerprints

Planned additions:

- explicit-root slicing by shape IRI
- explicit-root slicing by normalized key for anonymous shapes
- dropped-item reasons for unreachable and pruned items
- refined context buckets for single-hop vs traversal-heavy shapes
- shared-work candidate summaries for duplicate SPARQL and component uses

Rewrite-phase additions:

- root-slice rewrites preserve the expected reachable subgraph
- dead-structure elimination removes dropped owners and dangling references
- rewritten programs rebuild indexes consistently
- ordering rewrites are deterministic and analysis-informed
- recursive-region annotations match SCC analysis

## Deferred Work

- semantic subsumption and implication analysis
- duplicate-shape merging or semantic factoring
- proof-oriented equivalence instead of candidate duplicate groups
- backend-specific planning heuristics beyond backend-agnostic cost hints
- backend-specific execution rewrites or physical planning
