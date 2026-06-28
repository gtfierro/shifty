# 01 — Gap Analysis: W3C SHACL / SHACL-AF vs. the Formalism

This is the holes register. The paper's SHACL core
([`00-formalism.md`](00-formalism.md)) is elegant but deliberately narrow: it
targets *common graphs* (not RDF), covers *validation only*, and is
*non-recursive*. Real-world coverage means W3C **SHACL Core**
(https://www.w3.org/TR/shacl/) **+ SHACL-AF**
(https://www.w3.org/TR/shacl-af/). This doc enumerates every divergence, marks
it as *covered / extension / escape-hatch*, and records the fix. Each item has a
stable id (e.g. **D0**, **C1**) referenced from the IR sketch and the roadmap.

Legend:
- ✅ **covered** — expressible in the paper's core as-is or via documented sugar.
- 🔧 **extension** — needs a new IR constructor; still algebraic / optimizable.
- 🚪 **escape-hatch** — opaque (SPARQL/JS); compile a subset, fall back otherwise.

---

## A. Data-model deviations

| id | item | status | notes |
|----|------|--------|-------|
| **D0** | RDF re-unifies edges `P` and keys `K` | 🔧 | Paper keeps `E`/`ρ` apart so paths never cross values. RDF is just triples; we let `⟦π⟧ ⊆ Term×Term` traverse any triple. Removes an artificial wall; everything else still holds. |
| **D1** | RDF terms are IRIs/Blanks/Literals; node identity is *visible* | 🔧 | Paper hides node identity (Appendix A.1) which is *why* it drops `sh:nodeKind`, class info, and node-constant comparisons. Making identity visible re-enables **K1**, **C1**, **V1**. |

---

## B. SHACL Core constraint components

### Cleanly covered by the core (✅)

| W3C component | maps to | note |
|---------------|---------|------|
| `sh:datatype` | `test(τ)` `Datatype` | |
| `sh:minExclusive/minInclusive/maxExclusive/maxInclusive` | `test(τ)` `NumericRange` | paper's "value types as interval bounds" (App. B) |
| `sh:minLength/maxLength` | `test(τ)` `Length` | |
| `sh:pattern` (+`sh:flags`) | `test(τ)` `Pattern` | |
| `sh:minCount n` (on path π) | `∃≥ⁿ π.⊤` | |
| `sh:maxCount n` | `∃≤ⁿ π.⊤` | |
| `sh:qualifiedValueShape` + `qualifiedMin/MaxCount` | `∃≥ⁿ π.φ` / `∃≤ⁿ π.φ` | the canonical use of qualified counting |
| `sh:node S` | nest `φ_S` (via `∀π.φ_S = ∃≤⁰π.¬φ_S` in a property shape) | |
| `sh:property` | nested property shape = its `(path, φ)` | |
| `sh:and` / `sh:or` / `sh:not` | `φ∧φ′` / `φ∨φ′` / `¬φ` | |
| `sh:xone` | `⋁ᵢ (φᵢ ∧ ⋀_{j≠i} ¬φⱼ)` | normalized in parser |
| `sh:closed` (+`sh:ignoredProperties`) | `closed(Q)`, `Q = {paths-that-are-predicates} ∪ ignored` | |
| `sh:equals` | `eq(π, p)` | |
| `sh:disjoint` | `disj(π, p)` | |
| `sh:hasValue c` (literal `c`) | `∃≥¹ π.test(c)` | node-valued `c` needs **V1** |
| `sh:in (c₁…cₖ)` (literals) | `∃≥¹ π.(test(c₁)∨…∨test(cₖ))` | node-valued members need **V1** |

### Holes (🔧 extension)

| id | W3C component | why it's missing | fix |
|----|---------------|------------------|-----|
| **K1** | `sh:nodeKind` (IRI/Blank/Literal & unions) | paper abstracts node identity (D1) | add `Shape::TestKind(NodeKindSet)`, a primitive on `kind(v)` |
| **C1** | `sh:class C` + `sh:targetClass` / implicit class target | common model has *no class info* (App. A.3) | **sugar via path**: `sh:class C` ≡ `∃≥¹ (rdf:type · rdfs:subClassOf*).test_node(C)`; `targetClass` ≡ selector `∃≥¹ (rdf:type·rdfs:subClassOf*).test_node(C)` (needs `Selector::HasPath`, **V1**, and a configurable class-hierarchy path) |
| **V1** | comparisons with **node** constants (`sh:hasValue`/`sh:in`/`sh:targetNode` with IRIs) | paper restricts `test(c)` to `c ∈ V` (values only) | widen `test(c)` to `c ∈ Term` (`TestConst(Term)`); semantics unchanged, just a bigger constant domain |
| **O1** | `sh:lessThan` / `sh:lessThanOrEquals` | paper drops RDF term ordering (App. B) | add `Shape::Lt(π,p)` / `Shape::Le(π,p)` over the SPARQL `<`/`≤` ordering |
| **L1** | `sh:languageIn` / `sh:uniqueLang` | common model has no language tags (App. B) | extend literals with lang tags; `languageIn` → `test(τ)` `LangIn`; `uniqueLang` → `Shape::UniqueLang(π)` (a counting-over-langtags constraint) |
| **P1** | `sh:zeroOrOnePath` / `sh:oneOrMorePath` | paper omits `π?` / `π⁺` | normalizing sugar: `π? = π∪id`, `π⁺ = π·π*` (parser-side, no IR change) |

### Targets (selectors)

| id | W3C target | status | fix |
|----|-----------|--------|-----|
| — | `sh:targetNode` | ✅/🔧 | `Selector::IsConst(Term)` (needs **V1** for IRI nodes) |
| — | `sh:targetSubjectsOf p` | ✅ | `∃p.⊤` = `HasOut(p)` |
| — | `sh:targetObjectsOf p` | ✅ | `∃p⁻.⊤` = `HasIn(p)` |
| **C1** | `sh:targetClass` + implicit class target (shape is a class) | 🔧 | `Selector::HasPath` over `rdf:type·rdfs:subClassOf*` (see C1) |

---

## C. SHACL-AF (Advanced Features) — entirely absent from the paper

The paper covers none of SHACL-AF. This is the largest body of new work and the
heart of the "inference + validation" goal.

| id | feature | status | plan |
|----|---------|--------|------|
| **AF-R** | **Rules** (`sh:rule`: `sh:TripleRule`, `sh:SPARQLRule`) | 🔧/🚪 | new **rule algebra**: `Rule { selector, conditions: Vec<Shape>, head: RuleHead, order }`. `TripleRule` head = `(subjectExpr, predicate, objectExpr)` built from **node expressions** (AF-E); `SPARQLRule` head = CONSTRUCT (🚪). Semantics = add inferred triples; iterate the rule set to a **fixpoint**, respecting `sh:order` and `sh:condition`. See roadmap Layer 6. |
| **AF-E** | **Node expressions** (`sh:path`, focus `sh:this`, `sh:filterShape`, function application, `sh:intersection`/`sh:union`, constants) | 🔧 | node expressions are *value-producing*; they map almost 1:1 onto our `Path` algebra plus literals plus function calls. Define `NodeExpr` reusing `Path`; this is the shared substrate for rule heads, AF targets, and computed values. |
| **AF-T** | **SPARQL-based targets** (`sh:target` + `sh:select`) | 🚪 | `Selector::Sparql`; try to recognize/compile simple BGP selectors back into `HasOut/HasIn/HasPath`, else evaluate opaquely. |
| **AF-C** | **SPARQL-based constraints** (`sh:sparql` with `sh:select`/`sh:ask`) | 🚪 | opaque `Shape::Sparql` leaf; subset (single-BGP, no aggregates) recognizable → algebra. |
| **AF-X** | **Expression constraints** (`sh:expression`, SHACL-AF §5) | 🔧 *(done)* | `Shape::Expression(NodeExpr)`: conform iff the node expression over `?this` yields only `true`; each non-true value is one `sh:ExpressionConstraintComponent` result. Function-bearing expressions diagnosed. |
| **AF-CC** | **Custom constraint components** (`sh:parameter` + `sh:validator`/`sh:nodeValidator`/`sh:propertyValidator`) | 🚪/🔧 | parameterized macro: bind parameters, then either expand the validator's SPARQL to algebra (preferred) or keep opaque. |
| **AF-F** | **SHACL functions** (`sh:SPARQLFunction`, `sh:JSFunction`) | 🚪 | callable from node expressions; SPARQL functions get a compiled/interpreted path, JS stays opaque/unsupported initially. |

`sh:js*` (JavaScript) features are explicitly **out of scope** initially
(unsupported, with a clear diagnostic).

---

## D. Recursion (**decided** — see [`03-recursion-semantics.md`](03-recursion-semantics.md))

The paper assumes **non-recursive** shapes; the W3C spec leaves recursion
**undefined**. We extend with a pinned semantics:

- **Stratified** over the polarity-aware shape/rule dependency graph;
  non-stratifiable schemas (a cycle through a negative edge) are **diagnosed**,
  not guessed.
- Within a stratum: **validation = greatest fixpoint** (coinductive; a clean
  cycle conforms), **inference = least fixpoint** (only finitely-justified
  triples). Separate phases, so the directions don't conflict.

Polarity is computed *semantically* (the `∃≤0 π.¬φ` encoding of `∀π.φ` is
positive overall); `Count{min,max}` is un-fused for analysis. Full rationale and
worked examples in doc 03. This replaces the Layer 3 provisional stack guard.

---

## E. Conformance corpus (how we prove "covered" is real)

- **W3C SHACL test suite** (core + sparql): https://w3c.github.io/data-shapes/data-shapes-test-suite/
- **W3C SHACL-AF tests** (rules, targets, expressions, components).
- Each ✅/🔧 row above must be backed by ≥1 passing manifest case; each 🚪 row by
  a case that either passes or emits the documented "unsupported" diagnostic
  (never a silent wrong answer). Tracked in roadmap Layer 3.

---

## F. Summary of new IR surface beyond the paper

Validation shapes: `TestKind` (K1), widened `TestConst` (V1), `Lt`/`Le` (O1),
`UniqueLang` (L1), `Sparql` leaf (AF-C). Value types: `LangIn` (L1). Paths:
`?`/`⁺` sugar (P1). Selectors: `HasPath` (C1), `Sparql` (AF-T). Plus two whole
new subsystems: the **rule algebra** (AF-R) with **node expressions** (AF-E), and
a **recursion/fixpoint** semantics (D). Class handling (C1) is notable for being
*pure sugar over the path algebra* — no class-specific machinery in the core.
