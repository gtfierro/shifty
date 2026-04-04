# SPARQL Compilation Feasibility

This note evaluates whether Shifty should use `spargebra` as the front end for
compiling SHACL/SPARQL queries into generated Rust over graph-store iterators
and shape/data-driven indexes.

## Short Answer

Yes, for a large and useful subset.

The repository already has most of the scaffolding needed for this:

- `lib/src/sparql/mod.rs` already parses queries with `spargebra` and lowers a
  few recurring query families into native plans.
- `lib/src/context/validation.rs` already builds reusable adjacency and class
  indexes over the data graph.
- `crates/shacl-srcgen-compiler` already carries SPARQL metadata through the
  compile pipeline and emits generated validators/inference code.

What is missing is not parsing. What is missing is a real query IR, an indexed
execution interface, and a systematic lowering/codegen pipeline for the common
query families.

## Measured Snapshot On The Representative 223 Shape Graph

I used the checked-in `shapes.ir` and lowered it through the current srcgen
compiler:

```bash
cargo run -p shacl-srcgen-compiler -- \
  shapes.ir \
  --ir-out /tmp/feasibility-srcgen-ir.json \
  --out /tmp/feasibility-srcgen-out
```

From that representative shape graph:

- 74 SPARQL constraint components
- 32 SPARQL rules
- 28 / 74 SPARQL constraints already lower into native non-SPARQL kernels
- 46 / 74 SPARQL constraints still fall back to generic query execution
- 43 / 46 of those generic constraints do not use obviously hard operators such
  as `OPTIONAL`, `UNION`, `BIND`, `GROUP BY`, `COUNT`, `VALUES`, or `SERVICE`
- 16 / 32 SPARQL rules are single-pattern path-copy rules of the form:
  - `CONSTRUCT { $this P ?x } WHERE { $this PATH ?x . }`
- 28 / 32 SPARQL rules also avoid those obviously hard operators

Current lowered constraint coverage breaks down as:

- 17 `LocalSetCompatibility`
- 9 `MissingRelatedNode`
- 1 `AdjacentPredicateWhitelist`
- 1 `RequiredPathSupport`

That is the key feasibility result: the workload is not “arbitrary SPARQL”.
Most queries are `$this`-anchored graph joins and path checks over a small set
of recurring structural patterns.

## What `spargebra` Is Good For Here

`spargebra` is a good fit for:

- parsing and validating query syntax
- exposing a structured algebra/graph-pattern tree
- canonicalizing recurring query shapes
- driving lowering decisions from the AST instead of from brittle string regexes

`spargebra` is not the missing execution engine. The code generation,
specialized join planning, path evaluation, and index selection still have to
be implemented by Shifty.

That is still a good trade: the parser/algebra problem is already solved, which
lets the work focus on lowering and execution.

## Why This Is More Feasible Than A Full SPARQL Compiler

The current codebase already proves three important points:

1. Query-family lowering works.
   Existing `spargebra`-based lowering in `lib/src/sparql/mod.rs` recognizes
   several SHACL-specific `SELECT` families and runs them without the generic
   SPARQL engine.

2. Useful indexes already exist.
   `ValidationContext` already builds:
   - outgoing adjacency by focus and predicate
   - incoming adjacency by focus and predicate
   - `predicate -> subjects`
   - `predicate -> objects`
   - per-focus predicate counts
   - class/subclass bitmap indexes

3. Generated execution already exists.
   `crates/shacl-srcgen-compiler` already emits generated validators and
   generated inference code, including SPARQL-aware fallback paths and lowered
   fast paths.

So this is not a greenfield compiler project. It is mostly a generalization of
existing specialized paths.

## The Main Limitation In The Current Approach

The current inference fast-path recognizers in `lib/src/inference/mod.rs` and
the mirrored srcgen logic are string-regex based and expect expanded `<IRI>`
tokens. The representative 223 rule corpus uses prefixed names like
`s223:connectedTo`, so those regexes likely miss most or all of the corpus.

That is a strong argument for switching rule compilation to AST-based matching
through `spargebra` rather than extending regexes further.

## Recommended Index Model

The likely winning index set is not a generic database-style explosion of all
permutations. It is a compact shape-driven graph index layer:

- `out[s][p] -> objects`
- `in[o][p] -> subjects`
- `out_count[s][p]`
- `in_count[o][p]`
- `subjects_by_predicate[p]`
- `objects_by_predicate[p]`
- `instances_of_class[c]`
- subclass/transitive-type bitsets
- memoized path cache:
  - `(start_node, lowered_path_id) -> result_nodes`

Optional second-stage indexes, only if benchmarks justify them:

- transitive-closure caches for hot `*` / `+` predicates such as containment or
  composition
- shape-specific join caches for repeated anchor/value traversals in local-set
  compatibility checks

This is very close to the indexes Shifty is already building informally in
`ValidationContext`; the work is mostly to make them explicit, reusable, and
available to generated code.

## Likely Compilation Strategy

### 1. Canonicalize Queries Through `spargebra`

For each query:

- parse with `spargebra`
- resolve prefixes to canonical predicate identifiers
- normalize `$this`, `$PATH`, `currentShape`, and `shapesGraph` handling
- lower to a small internal query IR rather than keeping raw SPARQL strings

### 2. Compile Query Families, Not Raw Queries

The first useful families are:

- single-pattern path-copy `CONSTRUCT` rules
- path-copy plus simple negative condition (`FILTER NOT EXISTS`)
- `$this`-anchored existence/non-existence checks
- missing-related-node checks
- path-support implication checks
- local-set compatibility joins

These are already present in the corpus and already partially recognized today.

### 3. Generate Rust Over An Indexed Backend

Compiled queries should emit Rust that:

- starts from the most selective bound anchor, usually `$this`
- walks adjacency indexes directly
- uses memoized lowered-path evaluation for `/`, `^`, `*`, `+`, and `?`
- performs small hash-set joins in Rust
- materializes only the bindings or constructed triples actually needed

### 4. Keep Generic SPARQL Fallback

Do not try to compile everything.

Unsupported features should continue to fall back to Oxigraph/SPARQL execution:

- complex `OPTIONAL` interactions
- general `UNION`
- aggregation-heavy queries
- query forms that depend on semantics not yet mirrored in the compiled runtime

## Architectural Changes Needed

### 1. Add A Real Query IR

Right now the lowering logic jumps almost directly from parsed algebra to
ad-hoc execution helpers. That should become an explicit IR shared by runtime
and srcgen.

### 2. Strengthen The Backend Interface

`GraphBackend` is a useful start, but many call sites still materialize `Vec`s`
or escape to the raw Oxigraph store. A compiled query engine wants iterator-like
or visitor-style indexed access as the primary interface.

### 3. Unify Runtime And Srcgen Lowering

Today similar SPARQL logic is duplicated in runtime and generated code. The
compiler should produce one lowering decision that both paths can use.

### 4. Handle Inference Invalidations Explicitly

The data-side indexes and path caches must be invalidated or delta-updated when
inference inserts new triples.

## Risks

- Property-path semantics are the hardest part, especially `*`, `+`, and mixed
  inverse/sequence paths.
- Result fidelity matters for SHACL:
  - `?value`
  - `?path`
  - message substitution
  - failure semantics
- Generated code size can grow quickly if every query becomes a separate large
  Rust function.
- A partial compiler without a principled fallback boundary will become hard to
  reason about.

## Recommended Rollout

1. Replace rule regex matching with `spargebra` AST matching.
2. Extract a shared lowered-query IR from `lib/src/sparql/mod.rs`.
3. Formalize the existing adjacency/class summaries as a reusable indexed
   backend API.
4. Compile the single-pattern path-copy `CONSTRUCT` family first.
5. Expand `SELECT` lowering to the remaining simple `$this`-anchored negative
   checks.
6. Add a memoized lowered-path cache.
7. Only then consider more aggressive query-family coverage.

## Bottom Line

Using `spargebra` to compile Shifty’s SPARQL workload into generated Rust is
feasible and probably worthwhile, but only if it is framed correctly:

- use `spargebra` as the parser/algebra front end
- compile recurring query families, not arbitrary SPARQL text
- execute against a small explicit set of graph/path/type indexes
- keep generic SPARQL fallback for the remaining edge cases

For the representative 223 corpus, the current measurements suggest that this
approach would cover a large fraction of the expensive workload without
requiring a full general-purpose SPARQL compiler.
