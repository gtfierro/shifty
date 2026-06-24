# 02 — Layered Roadmap

Build order for a formalism-first SHACL + SHACL-AF engine (inference +
validation) that is *correct first, then highly optimized*. Each layer is
independently testable and leaves the tree green. References to **D0/C1/AF-R/…**
point at [`01-gap-analysis.md`](01-gap-analysis.md); the IR is
[`00-formalism.md`](00-formalism.md).

Guiding principles:
1. **One reference semantics, many engines.** The naive denotational evaluator
   (Layer 3) is the oracle. Every optimization (Layers 5–7) must produce
   identical conformance results.
2. **Lower early, optimize late.** All vocabulary sugar collapses into the small
   core IR at parse time (Layer 2); optimizers never see W3C vocabulary.
3. **Fix semantics before optimizing.** Recursion/fixpoint (Layer 4) is pinned
   down before any rewrite that depends on it.

---

## Layer 0 — Clean-room scaffolding  *(next step; needs go-ahead)*

- New Cargo workspace; **retire** the old spec-shaped crates (`shacl-core`,
  `shacl-core-inmemory`, `shacl-core-cli`, `shacl-core-oxigraph`) — kept on the
  `bluesky-shacl-core` branch as reference, removed here.
- Crate skeleton:
  - `shifty-algebra` — the core IR + reference semantics (Layers 1, 3).
  - `shifty-parse` — RDF shapes graph → IR lowering (Layer 2).
  - `shifty-opt` — static analysis, normalization, planning (Layers 4–5).
  - `shifty-engine` — validation + AF inference execution (Layers 3, 6, 7).
  - `shifty-cli` — inspection / validate / infer commands.
- Pull in the W3C test-suite manifests as a git submodule or vendored corpus.
- **Deliverable:** workspace builds empty; CI runs `cargo test` (no tests yet).

## Layer 1 — Core algebra IR  (`shifty-algebra`)

- Implement `Term`, `Path`, `ValueType`, `Shape`, `Selector`, `Schema`,
  `NodeExpr`, and the `Rule` skeleton from doc 00 §2–§4 and gap **AF-E/AF-R**.
- Shape arena with interned references (cyclic-ref capable; semantics later).
- Smart constructors that maintain light invariants (flattened `And`/`Or`,
  `Seq`/`Alt` normalization).
- **Deliverable:** IR types + builders + unit tests; `serde` round-trip.

## Layer 2 — Parser & lowering  (`shifty-parse`)

- Read shapes graph with `oxrdf` (Turtle/N-Triples first).
- Shape discovery (explicit `sh:NodeShape`/`sh:PropertyShape` + implicit-by-usage).
- Lower **all** Core + AF vocabulary into the IR, applying every sugar rule:
  `minCount/maxCount → Count`, `xone → ∧∨¬`, `class → path` (**C1**),
  `zeroOrOne/oneOrMore → ∪id / ·*` (**P1**), `node/property` nesting, qualified
  cardinality → `Count`, closed-set computation, etc.
- Implement the extensions: `TestKind` (**K1**), widened consts (**V1**),
  `Lt`/`Le` (**O1**), `LangIn`/`UniqueLang` (**L1**).
- Parse AF rules + node expressions (**AF-R/AF-E**); mark SPARQL/JS leaves opaque
  (**AF-C/AF-T/AF-CC/AF-F**) with diagnostics, never silent.
- **Deliverable:** shapes-graph → `Schema`; diagnostics for unsupported; golden
  round-trip tests on hand-written shapes covering each gap-analysis row.

## Layer 3 — Reference semantics & conformance oracle  (`shifty-algebra` + `shifty-engine`)

- Naive denotational evaluator: relational `⟦π⟧` over an in-memory triple store,
  structural `G,v ⊨ φ`, schema validation `G ⊨ S` producing a SHACL
  validation report (`sh:ValidationReport`/`sh:ValidationResult`).
- Wire the **W3C SHACL Core + SPARQL + AF test suite**; track pass/fail per
  gap-analysis id (§E).
- **Deliverable:** correctness oracle; documented conformance matrix. Speed does
  not matter yet.

## Layer 4 — Static analysis, recursion semantics & normalization  (`shifty-opt`)

- Shape dependency graph with **polarity-aware** edges (un-fuse `Count` min/max);
  SCC + **stratification** per the *decided* semantics in
  [`03-recursion-semantics.md`](03-recursion-semantics.md) — stratified, gfp
  validation / lfp inference, diagnose non-stratifiable. Build the shared
  fixpoint framework and **replace the Layer 3 stack guard**.
- Normal forms: NNF/CNF-ish for `φ`; canonicalize `Path` via Kleene-with-converse
  laws (`(π·π′)⁻ = π′⁻·π⁻`, `id` units, `∪` ACI, `*` idempotence).
- Sound simplifications: constant folding, `test`-type intersection, dead-branch
  elimination, `closed` set tightening, unsat detection (`φ ∧ ¬φ`,
  incompatible `test(τ)` facets).
- **Deliverable:** normalizer + analyzer; rewrites proven semantics-preserving
  against the Layer 3 oracle (property tests: optimize-then-validate ≡ validate).

## Layer 5 — Logical → physical planning  (`shifty-opt`)

- Per `(selector, φ)`: choose evaluation strategy. Target-driven seeding (pull
  focus nodes from selectors) vs. graph scan.
- Path compilation: BGP/reachability plans for `Seq/Alt/Star/Inverse`; pick index
  orders (SPO/POS/OSP) from a predicate-statistics model.
- Compile the supported Spargebra subset into native scans, joins, path scans,
  and correlated semi/anti-joins over a shared immutable indexed dataset.
  Unsupported whole queries fall back to Oxigraph/Spareval. See
  [`05-sparql-execution.md`](05-sparql-execution.md).
- Common-subexpression sharing across shapes; push `test`/`closed` filters down;
  short-circuit Boolean evaluation ordered by selectivity.
- Incremental / focus-delta scheduling (only re-check affected focus nodes).
- **Deliverable:** physical plan IR + plan-inspection CLI; benchmarked speedups
  with identical conformance results.

## Layer 6 — SHACL-AF inference  (`shifty-engine`)  **(AF-R / AF-E)**

- Node-expression evaluator (reuses `Path` + functions).
- Rule engine: evaluate body (selector + condition shapes), materialize head
  triples; **semi-naive fixpoint** over the rule set with `sh:order` priority
  groups and recursion semantics from Layer 4. The first implementation uses
  conservative predicate-delta scheduling at rule granularity; native paths
  expose exact reads while opaque/domain-sensitive constructs fall back to
  wildcard rescheduling.
- Interaction model: inference-then-validation, with provenance for inferred
  triples.
- **Deliverable:** AF rule conformance; fixpoint termination guarantees
  documented.

## Layer 7 — Compilation / JIT  (`shifty-engine`)

- Compile shapes/plans to specialized executors: staged closures first, then
  optional codegen (Cranelift JIT) for hot validators; batch/vectorized
  evaluation over focus sets; parallel strata.
- **Deliverable:** compiled-mode engine selectable via CLI; benchmark vs. naive
  and planned modes; conformance unchanged.

---

## Status

| Layer | state |
|-------|-------|
| Docs 00–02 | ✅ |
| 0 Scaffolding | ✅ workspace builds; old crates retired; 5 crates stubbed |
| 1 Core algebra IR | ✅ IR types, smart constructors, cyclic arena, serde round-trip |
| 2 Parser & lowering | ✅ Turtle → IR for Core + targets + paths; AF/SPARQL diagnosed; `shacl inspect` stage viewer |
| 3 Reference semantics | ✅ denotational evaluator (`G ⊨ S`); `shacl validate`; W3C core 94/113 pass, 0 fail, 19 skip |
| 4 Static analysis & recursion | 🔨 stratification + gfp semantics wired; normalization Tier-1 (CSE, compaction, Boolean, NNF, count-merge — see [`04-normalization.md`](04-normalization.md)) |
| 5 Planning | 🔨 plan IR + planner + executor (`validate_plan`); class-target seeding ≈23× vs scan on a synthetic graph; W3C cross-checked. Indexed paths and native Spargebra execution are designed in [`05-sparql-execution.md`](05-sparql-execution.md) |
| 6 AF inference | 🔨 rule lowering + global lfp fixpoint engine (`shacl infer`); TripleRules, SPARQLRules, node exprs, and predicate-delta scheduling. Functions next |
| 7 Compilation/JIT | ⬜ not started |

Layer 1 landed in `shifty-algebra`: `Term`/`NodeKindSet`, the `Path` algebra,
`ValueType` facets, the `Shape` grammar over a cyclic-capable `ShapeArena`,
`Selector`, `NodeExpr`, the `Rule` skeleton, opaque SPARQL leaves, and `Schema`
— with light always-sound smart constructors and serde round-trip (cycles
encoded as indices).

Layer 2 landed in `shifty-parse`: Turtle loading (`oxttl`/`oxrdf`), shape
discovery, full path parsing (incl. inverse/alt/seq lists and the `*`/`+`/`?`
operators), and lowering of Core constraints, targets, qualified counts,
property pairs, and `closed` into the IR — with per-value constraints encoded as
`∀π = ∃≤0 π.¬φ` and recursive `sh:node` references preserved through the cyclic
arena. Unsupported AF/SPARQL constructs emit diagnostics. Debug tooling: `shacl
inspect --stage rdf|algebra --format text|json` shows each layer's view (the
text dump is a cycle-safe, reachability-filtered notation rendering).

Not yet lowered (tracked for later in Layer 2 / Layer 6): SHACL-AF rules and
node expressions, SPARQL constraints/targets, and custom constraint components —
all currently diagnosed rather than dropped.

Layer 3 landed in `shifty-engine`: a naive denotational evaluator — relational
path evaluation (`succ`/`pred` with `Inverse` swapping direction), value-type /
ordering checks, and shape/schema satisfaction `G ⊨ S` with a cycle-breaking
stack guard for recursive schemas (provisional pending Layer 4). `shacl validate
--shapes … --data …` runs it. The vendored **W3C core suite** is wired as a
conformance gate (`tests/w3c_core.rs`): 94 pass, 0 fail, 19 skip — skips are
tests using features we diagnose as unsupported (SPARQL/rules/custom
components/JS). A bug surfaced and was fixed: nested `sh:property` is value-scoped
(under `∀path`), not focus-scoped.

Known oracle limitations (deferred): validation *reports* are focus-node level,
not full `sh:ValidationResult` provenance (needs a provenance side-table, since
the IR is pure); term ordering covers numeric + xsd:string only.

Next: Layer 4 — static analysis (shape dependency graph, stratification), the
**recursion-semantics decision** (the load-bearing open question), and
semantics-preserving normalization — all validated against this oracle.
