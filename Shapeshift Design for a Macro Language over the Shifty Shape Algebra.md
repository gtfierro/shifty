# Shapeshift: Design for a Macro Language over the Shifty Shape Algebra

Nov 8, 2026 · @Gabe Fierro

## Goals and non-goals

Shapeshift is a small text language for writing RDF validation rules that compiles to shifty's existing shape algebra. Its primary users are people new to RDF and to validation who need to write correct rules on day one. Its secondary users are domain experts who want to package a vocabulary of reusable rules (a domain validation language) for the first group.

The mental model it teaches has one sentence: **a rule picks some nodes and states what must be true of each one.** Everything a user writes is either a *shape* (a named statement about one focus node) or a *target* (which nodes a shape applies to). There is no NodeShape/PropertyShape split, no blank nodes, no `sh:` vocabulary to memorize.

Goals, in priority order:

1. Correct rules from novices, with build-time errors for the mistakes SHACL normally reveals only at validation time (typos in class IRIs, class-vs-shape confusion, universal-vs-existential confusion, ill-founded recursion).
2. Macros defined in the language itself, compiled to the kernel, and reusable from every interface (CLI, Python, WASM, C++) without reimplementation.
3. Lossless round trip to SHACL: every Shapeshift file exports to readable SHACL; every SHACL Core graph can be imported and referenced.
4. One algebra for authoring, validation, evidence and repair, so a violation can be reported against the macro the author wrote.

Non-goals for the first version: a graphical editor; a Python-first API (it follows later as a thin layer over the same AST); SHACL 1.2 features shifty does not yet implement; JavaScript constraints; replacing Turtle for data.

## Foundations: the algebra shifty already has

The kernel is not new. Shifty's `shifty-algebra` crate already implements the SHACL abstraction of [Common Foundations for SHACL, ShEx, and PG-Schema](https://arxiv.org/abs/2502.01295) (Ahmetaj et al., WWW 2025), extended with the operators real SHACL needs. Shapeshift compiles into these types unchanged.

The paper's SHACL abstraction has three sorts. Paths: `id | p | π⁻ | π·π | π ∪ π | π*`. Shapes: `⊤ | test(c) | test(V) | closed(Q) | eq(π,p) | disj(π,p) | ¬φ | φ∧φ | φ∨φ | ∃≥n π.φ | ∃≤n π.φ`. Selectors: `∃q.⊤ | ∃q⁻.⊤ | test(c)`. A schema is a set of (selector, shape) pairs; a graph is valid when every node matching a selector satisfies its shape.

Shifty's IR, as it stands in `crates/shifty-algebra/src`:

| Type | Variants | Notes |
| --- | --- | --- |
| `Path` | `Id`, `Pred`, `Inverse`, `Seq`, `Alt`, `Star` | Exactly the paper's grammar |
| `Shape` | `Top`, `TestConst`, `TestType(ValueType)`, `TestKind`, `Closed`, `Eq`, `Disj`, `Lt`, `Le`, `UniqueLang`, `Not`, `And`, `Or`, `Count{path,min,max,qualifier}`, `Sparql`, `Expression`, `Annotated{severity,messages}`, `Pending` | Arena of `ShapeId`; `Count` fuses the two counting forms; `Pending` is a reserved slot for building cycles |
| `ValueType` | `Any`, `Datatype`, `NumericRange`, `Length`, `Pattern`, `LangIn`, `And` | The paper's `test(V)`, widened to SHACL facets |
| `Selector` | `HasOut(p)`, `HasIn(p)`, `IsConst(t)`, `HasPath(π, φ)`, `Sparql` | `HasPath` is shifty's shape-based target, beyond SHACL Core |
| `Schema` | `arena`, `statements`, `rules`, `names`, `sources`, `prefixes` | Serde-serializable; `names` keeps every authored IRI per slot |

Two existing behaviours that Shapeshift relies on:

- `sh:class` and `sh:targetClass` lower to the path `rdf:type/rdfs:subClassOf*` (`class_path()` in `shifty-parse/src/lower.rs`). Subclass closure is in the algebra, not special-cased in the evaluator.
- Smart constructors already normalize lightly: `and` flattens and drops `⊤`, `not` is an involution, `xone` desugars to `⋁ᵢ(φᵢ ∧ ⋀ⱼ≠ᵢ ¬φⱼ)`, `count` with no bounds collapses to `⊤`.

What shifty does **not** have today, and Shapeshift adds: a text front end other than SHACL RDF; a macro/template layer; raising from the algebra back to SHACL RDF (only lowering and φ/π pretty-printing exist); import declarations in `Schema`; macro-level provenance for reports.

## Architecture

Four new crates sit between the interfaces and shifty. Nothing above `shifty-algebra` defines a constraint of its own: every surface construct and every macro bottoms out in an arena `Shape`, so one macro library serves the CLI, Python, WASM and the LSP.

&#91;embedded content: architecture · interfaces, four new shapely crates, existing shifty crates\]

Text flows down into the arena and SHACL flows both ways: imported SHACL graphs are lowered by the existing `shifty-parse`, and the finished `Schema` is raised back to SHACL by `shapeshift-raise`.

The four layers and what each owns:

1. **Kernel** (`shifty-algebra`, unchanged apart from the deltas listed later). Owns meaning. Normalization, CSE, admission, evaluation and evidence all work here.
2. **Macro templates** (`shapeshift-macro`). A template is a kernel term with typed holes (`node`, `path`, `shape`, `int`, `list<...>`). Expansion is substitution into the arena plus a provenance record. Templates are data, serialized as part of the file's AST, so a binding can ship a template and have Rust expand it.
3. **Domain validation language** (a `.shm` file or a set of them). A bundle of named shapes and macros plus the ontology imports that give it a vocabulary. "Brick validation" or "223P validation" is one of these, authored by a domain expert, consumed by novices.
4. **Bindings** (CLI, Python, WASM, C++, LSP). Parse text, or construct the AST directly, and hand it to Rust. They add ergonomics, never semantics.

The serialized AST, not the text, is the interchange format between layers. The text grammar is one rendering of it; a Python builder or a visual editor is another.

## Surface language

The surface is a record frame ("a class with fields") on the outside and a six-word logic vocabulary on the inside. This follows the usability evidence rather than the algebra's shape: the algebra is quantifiers and connectives, but novices read records and sentences.

Design principles, each with its source:

- **Words over symbols.** Stefik and Siebert (2013, replicated 2023) found C-style syntax no more learnable by novices than randomly generated keywords, while keyword-driven languages were significantly better. Shapeshift has no `&&`, `|`, `!`, `->` or S-expressions.
- **Few rigid frames over a flexible expression grammar.** Cedar's readability over Rego comes from a fixed sentence structure (effect, scope, conditions) in the application's own vocabulary. Shapeshift has three frames: the shape block, the field line, and the boolean block. Users never write free-form logic.
- **The record frame is the one non-programmers already know.** ShEx compact syntax, SHACL-C, GraphQL schemas and LinkML all use it, and LinkML is the one that reached domain scientists. Its known failure, a thin escape hatch to full SHACL, is what the kernel fixes: every field line is a `Count`, so there is nothing the frame cannot reach.
- **Make the two classic confusions impossible to write ambiguously.** Universal versus existential (`each` versus `is`/`at least n is`) and class versus shape (`a` versus `matches`) are different words, never inferred from context.
- **No loops, no type annotations in user code.** Both were rated unintuitive by novices. Iteration lives in built-in macros (`any of`, `at least n of`); parameter sorts are inferred from use and checked, never written by the author.

### Grammar

```
file        := header* (shape | macro)*
header      := 'base' IRI | 'prefix' NAME ':' IRI | 'import' source ('as' NAME)?
source      := IRI | STRING                        -- URL, or a relative path

comment     := '--' ... end of line                -- ignored by the compiler, kept by the formatter
doc         := 'doc' STRING                        -- docstring: attaches to the next shape or line

shape       := doc? 'shape' NAME params? target* '{' line* '}'
params      := '(' NAME (',' NAME)* ')'            -- sorts inferred from use
target      := 'for' 'every' node                  -- class, with subclass closure
             | 'for' 'every' 'node' 'with' node    -- subjects of a predicate
             | 'for' 'every' 'value' 'of' node     -- objects of a predicate
             | 'for' 'node' node                   -- one specific node
             | 'for' 'every' 'node' 'matching' NAME -- shape-based target (shifty)
             | 'for' 'sparql' STRING               -- escape hatch

line        := doc? (field | 'include' call | boolean | 'only' 'these' 'properties')
             | ('severity' | 'message') ...        -- annotations, apply to the next line
field       := (NAME ':')? path count? (',' qualifier)*   -- one-line form
             | NAME ':' '{' path count? qualifier* '}'      -- block form: one qualifier per line
sep         := NEWLINE | ';'                                -- lines end at a newline or a semicolon
count       := 'required' | 'optional' | 'exactly' INT | 'at least' INT
             | 'at most' INT | 'between' INT 'and' INT
qualifier   := 'each' test                         -- all values satisfy
             | 'at least' INT 'is' test            -- qualified count (same for at most, exactly)
             | 'same as' node | 'different from' node
             | 'less than' node | 'at most' node   -- sh:lessThan / sh:lessThanOrEquals
             | 'unique language'
test        := 'a' node                           -- class membership, closure
             | 'matches' call                      -- conforms to a named shape
             | 'is' node                           -- exactly this node
             | 'one of' node+
             | datatype | 'iri' | 'literal' | 'blank'
             | 'length' INT ('to' INT)? | 'from' NUM 'to' NUM | 'pattern' STRING
             | 'language' STRING+

boolean     := 'any of' block+ | 'exactly one of' block+ | 'none of' block+
             | 'all of' block+                     -- explicit; the default inside braces
             | 'when' block 'then' block
block       := '{' line* '}'
call        := NAME ('(' arg (',' arg)* ')')?      -- NAME may be alias.NAME
arg         := node | path | call | INT
path        := node | '^' path | path '/' path | path 'or' path | path '*' | 'self'
node        := PREFIXED_NAME | '<' IRI '>' | NAME  -- NAME when bound as a parameter
```

### Worked example

```
base   <https://example.org/shapes/hvac>
import <https://brickschema.org/schema/1.4/Brick.ttl>
import "./sensors.shm" as sensors
prefix brick: <https://brickschema.org/schema/Brick#>

shape HasPointOf(cls) {
  brick:hasPoint      at least 1 is a cls
}

shape VAV for every brick:VAV {
  include HasPointOf(brick:Supply_Air_Temperature_Sensor)
  brick:hasPoint      each matches sensors.Sensor
  brick:isFedBy       exactly 1, a brick:AHU
  brick:hasUnit       optional, each one of brick:Degree_Celsius brick:Degree_Fahrenheit
  when { brick:hasPoint at least 1 is a brick:Economizer_Status }
  then { brick:hasPoint at least 1 is a brick:Outside_Air_Damper }
}

shape Space for every brick:Space {
  brick:hasPart       each matches Space
  only these properties
}
```

Reading rules that make the frame safe: lines inside braces are conjoined; a field with no count is unconstrained in number; `each` with no count accepts zero values (so `required, each a X` is the common "one or more, all of type X"); `only these properties` closes the shape to the predicates named in it.

### Comments and docstrings

Two kinds, with different fates. A `--` comment runs to the end of the line; the compiler discards it and the formatter preserves it, as in any language. A `doc "..."` docstring is part of the program: it attaches to the shape or line that follows it, is stored in the AST, and travels to every output.

```
doc "A VAV must report supply-air temperature and be fed by exactly one AHU."
shape VAV for every brick:VAV {
  include HasPointOf(brick:Supply_Air_Temperature_Sensor)
  doc "Downstream of one AHU only; dual-duct VAVs are modelled as two VAVs."
  brick:isFedBy       exactly 1, a brick:AHU
}
```

| Where a docstring sits | Stored as | Exported as | Shown in |
| --- | --- | --- | --- |
| before `shape S` | `Schema.docs[S]` (sidecar, like `origins`) | `rdfs:comment` on the `sh:NodeShape` | LSP hover on `S` and on `matches S`; report header for failures of `S` |
| before a field line | `Schema.docs[slot of that Count]` | `sh:description` on the property shape | report line for that field, under the failure, before any hint |
| before a boolean block or `include` | `Schema.docs[slot]` | `rdfs:comment` on the blank node or `sh:and` member | report, LSP |
| before a template `shape S(x)` | on the template, copied to each expansion | `rdfs:comment` on each expanded shape, with the arguments appended | hover on each call site |

Docstrings are distinct from `message`: a `message` is what the validator says when the line fails, written for the data author; a `doc` is what the line is for, written for the next person reading the shapes. Both may appear on one line. Imported SHACL graphs map `rdfs:comment` and `sh:description` back to docstrings, so hover works on `bs:PointShape` too.

The algebra needs one more sidecar for this, `docs: HashMap<ShapeId, String>` on `Schema`, alongside `origins` in the deltas.

## Kernel mapping

Every surface construct compiles to one arena term, built with shifty's existing smart constructors. `CLASS` below is the path `rdf:type/rdfs:subClassOf*` that shifty already uses for `sh:class`.

| Surface | Algebra term | SHACL on export |
| --- | --- | --- |
| `shape S { l1 ... ln }` | `And([l1 ... ln])`, slot named `S` | `sh:NodeShape` with IRI `<base>#S` |
| `shape S(x) { ... }` | template; one slot per distinct argument tuple | one `sh:NodeShape` per expansion, annotated with the call |
| `p required` | `Count{p, min 1, ⊤}` | `sh:property [ sh:path p ; sh:minCount 1 ]` |
| `p optional` | `⊤` (collapsed by `count`) | `sh:property [ sh:path p ]` for documentation only |
| `p exactly n` / `at least n` / `at most n` / `between a and b` | `Count{p, min, max, ⊤}` | `sh:minCount` / `sh:maxCount` |
| `p ..., each T` | `Count{p, max 0, Not(T)}` (∀) | `sh:class`, `sh:node`, `sh:datatype`, `sh:in` ... on the property shape |
| `p at least n is T` | `Count{p, min n, T}` | `sh:qualifiedValueShape` + `sh:qualifiedMinCount` |
| `p at most n is T` | `Count{p, max n, T}` | `sh:qualifiedValueShape` + `sh:qualifiedMaxCount` |
| `a X`, or a bare `X` where a test is expected | `Count{CLASS, min 1, TestConst(X)}` | `sh:class X` |
| `matches S`, or a bare `S` | reference to slot `S` | `sh:node S` |
| `is "lit"` / `is one of X` | `TestConst("lit")` / `TestConst(X)` | `sh:hasValue` |
| `one of X Y` | `Or([TestConst(X), TestConst(Y)])` | `sh:in (X Y)` |
| `string`, `integer`, `datatype D` | `TestType(Datatype(D))` | `sh:datatype` |
| `iri` / `literal` / `blank` | `TestKind` | `sh:nodeKind` |
| `length a to b`, `from a to b`, `pattern`, `language` | `TestType(Length / NumericRange / Pattern / LangIn)` | the matching facet |
| `p same as q` / `different from q` | `Eq(p, q)` / `Disj(p, q)` | `sh:equals` / `sh:disjoint` |
| `p less than q` / `p at most q` | `Lt(p, q)` / `Le(p, q)` | `sh:lessThan` / `sh:lessThanOrEquals` |
| `p unique language` | `UniqueLang(p)` | `sh:uniqueLang true` |
| `only these properties` | `Closed(Q)`, Q = predicates named in the block | `sh:closed true` |
| `S(...)` or `include S(...)` alone on a line | `And` member referencing `S`'s slot | `sh:and` or inlined, see Raising |
| `any of {A} {B}` | `Or([A, B])` | `sh:or` |
| `exactly one of {A} {B}` | `xone([A, B])` | `sh:xone` |
| `none of {A} {B}` | `Not(Or([A, B]))` | `sh:not [ sh:or (...) ]` |
| `when {C} then {B}` | `Or([Not(C), B])` | `sh:or ( [sh:not C] B )` |
| `severity warning` on a line | `Annotated{severity, shape}` | `sh:severity sh:Warning` |
| `message "..."` on a line | `Annotated{messages, shape}` | `sh:message` |
| `for every X` | `Statement{HasPath(CLASS, TestConst(X)), S}` | `sh:targetClass X` |
| `for every node with p` | `Statement{HasOut(p), S}` | `sh:targetSubjectsOf p` |
| `for every value of p` | `Statement{HasIn(p), S}` | `sh:targetObjectsOf p` |
| `for node n` | `Statement{IsConst(n), S}` | `sh:targetNode n` |
| `for every node matching T` | `Statement{HasPath(Id, T), S}` | universal sh:SPARQLTarget + sh:or ( \[sh:not T\] S ), plus shapeshift:targetShape T for shifty |
| `for sparql "..."` | `Statement{Sparql(q), S}` | `sh:target [ a sh:SPARQLTarget ]` |

Paths map one to one: `p` to `Pred`, `^p` to `Inverse`, `p/q` to `Seq`, `p or q` to `Alt`, `p*` to `Star`, `self` to `Id`.

Two properties of this table matter for the rest of the design. First, the mapping is total: there is no surface construct without an algebra term, so the evaluator, evidence and repair need no knowledge of Shapeshift. Second, it is nearly injective on the un-normalized arena, which is what lets the raiser emit readable SHACL instead of canonical algebra.

## Names, macros and composition

Every shape has a name, and a name is the unit of reuse. There are no anonymous shapes in the surface language except the blocks inside boolean frames, so nothing the author writes becomes a blank node on export.

**A named shape is a zero-argument macro.** `shape Sensor { ... }` with no `for` clause is a reusable definition: an arena slot with `Sensor` in `Schema.names`. Adding `for` clauses makes it a rule as well; the slot is the same.

**A parameterized shape is a template.** `shape HasPointOf(cls) { brick:hasPoint at least 1 is a cls }` is stored as a kernel term with a hole. Parameter sorts (`node`, `path`, `shape`, `int`, `list<...>`) are inferred from how each parameter is used and checked at definition time: using `cls` both after `a` (needs a node) and after `matches` (needs a shape) is an error at the definition, not at the call. Each distinct argument tuple expands to its own arena slot, so `HasPointOf(brick:AHU)` called twice shares one slot (arena interning does this already).

**Templates are data.** The serialized AST of a `.shm` file carries templates unexpanded, with their parameter sorts. A binding in another language can therefore ship a template definition to Rust and have it expanded, checked and raised there. Templates can call templates; the expansion is a tree.

**`include` is conjunction.** `include S(args)` inside a block adds `S`'s slot to the enclosing `And`. Including a shape that has `for` clauses brings its constraints and not its targets: `S`'s `Statement`s stay where they are. Severity and message annotations travel with the included constraints, since they are part of the constraint's meaning (`Annotated` is transparent to `And`). Overriding them would be a separate, explicit form if ever needed.

**Named fields.** A field line may carry a name: `supplyTemp: brick:hasPoint at least 1 is a brick:Supply_Air_Temperature_Sensor`. The name is optional and changes nothing about the constraint; it gives the line an identity. The `Count` slot gets `VAV.supplyTemp` in `Schema.names`, so on export it becomes a named `sh:PropertyShape` (`<base>#VAV.supplyTemp`) instead of a blank node, reports and `message` templates can refer to it, and other shapes can pull in that one line with `include VAV.supplyTemp`, which is how a shape says "like VAV, but only the point rule". Named fields are also the natural key for shifty's property witnesses (`witnesses(key_path=...)`): the key becomes a name rather than a path string. Unnamed fields stay blank nodes on export, as they are in most hand-written SHACL.

Two spellings, same algebra. The one-line form suits a field with one or two qualifiers; the block form opens a named field into its own braces, one qualifier per line, for fields that carry several qualifiers or a docstring and message of their own. A `;` ends a line anywhere a newline would, so short blocks can stay on one line.

```
shape VAV for every brick:VAV {
  -- one-line form
  supplyTemp: brick:hasPoint at least 1 is a brick:Supply_Air_Temperature_Sensor

  -- block form: the same field, opened up
  fedBy: {
    brick:isFedBy
    exactly 1
    a brick:AHU
    doc "Dual-duct VAVs are modelled as two VAVs."
    message "A VAV must be fed by exactly one AHU."
  }

  -- block form on one line, with semicolons
  unit: { brick:hasUnit; optional; each one of brick:Degree_Celsius brick:Degree_Fahrenheit }
}

shape ReheatVAV for every brick:RVAV {
  include VAV.supplyTemp          -- one named field, not the whole shape
  include VAV.fedBy
  reheat: brick:hasPoint at least 1 is a brick:Reheat_Valve_Command
}
```

Inside a field block the first line is the path; `count`, qualifiers, `doc` and `message` follow in any order. The block compiles to the same single `Count` as the one-line form, so `include VAV.fedBy` and the export are identical either way.

**Scoping and IRIs.** Names are file-local. `import "x.shm" as m` exposes `m.Name`. On export, a shape's IRI is minted under the base of the file that *defines* it, so two files importing `sensors.shm` export the same IRI for `sensors.Sensor` and downstream `owl:imports` graphs can refer to it stably. A file with no `base` cannot be imported by name and exports with blank-node shapes; the CLI warns.

**Provenance.** Expansion writes a sidecar map `ShapeId -> Vec<Origin>` where `Origin` is one of `Shape(name)`, `MacroCall{name, args}`, `Include{from, into}`, `Field{shape, path}`, `BooleanBranch{frame, index}`. Shifty's `Schema.sources` already maps slots to RDF source nodes; the sidecar extends the same idea to Shapeshift constructs. Normalization's CSE may merge structurally identical slots, and `names` already keeps every authored name per slot, so the sidecar does the same: a slot can have several origins, all of which are reported.

**Built-in macros.** A small standard library ships with the language and is implemented in Rust against the kernel: `any of`, `exactly one of`, `none of`, `when/then`, `at least n of {...} {...}` (n of k blocks hold; a native Threshold node, see Algebra deltas, so no combinatorial expansion). These are the only places iteration happens; user templates are pure substitution.

## Boolean frames

Disjunction and negation are written as blocks with a leading phrase, never as infix operators. Each block is an anonymous shape, and the phrase names the connective.

```
shape Sensor {
  any of          { include TempSensor } { include PressureSensor } { include FlowSensor }
  exactly one of  { brick:hasUnit required } { brick:hasTag required }
  none of         { brick:hasPart required }
  when            { brick:hasPoint at least 1 is a brick:Economizer_Status }
  then            { brick:hasPoint at least 1 is a brick:Outside_Air_Damper }
}
```

| Phrase | Compiles to | Why it exists |
| --- | --- | --- |
| `all of` (implicit inside braces) | `And` | the record frame |
| `any of` | `Or` | alternatives |
| `exactly one of` | `xone` (shifty's existing desugaring) | mutually exclusive alternatives |
| `none of` | `Not(Or(...))` | prohibitions; also the only way to write negation |
| `when C then B` | `Or([Not(C), B])` | the most common real rule: "if it has X it must have Y"; novices will not find it as `not A or B` |
| `at least n of` | disjunction of conjunctions | "two of these three point types"; capped expansion |

Value-level disjunction stays on the field line as `one of X Y Z`, which is `Or` over `TestConst`. There is deliberately no `or` between field lines, no bare `not`, and no parenthesized expressions: a reader can always tell a block's meaning from its first word.

Polarity for recursion follows directly: `any of` and `all of` are positive edges, `none of` is negative, `when` is negative on its condition and positive on its body. See Recursion.

## Targets

A shape takes zero or more `for` clauses, one per selector. Each compiles to one `Statement` sharing the shape's `ShapeId`, exactly as shifty handles a SHACL shape with several `sh:target*` declarations.

```
shape VAV
  for every brick:VAV                      -- class target, with rdfs:subClassOf closure
  for every node with brick:hasDamper      -- subjects of a predicate
  for every value of brick:feeds           -- objects of a predicate
  for node ex:vav-7                        -- one node
  for every node matching Sensor           -- shape-based target (shifty extension)
  for sparql "SELECT ?this WHERE { ... }"  -- escape hatch
{ ... }
```

**Class semantics match SHACL exactly.** `for every X` and `a X` both use the path `rdf:type/rdfs:subClassOf*`, the same `class_path()` shifty's lowering produces for `sh:targetClass` and `sh:class`. The builder and the SHACL importer therefore agree by construction, and the raiser can emit `sh:targetClass`/`sh:class` back out. An author who wants the literal edge writes `rdf:type exactly 1, is X` as an ordinary field line; no keyword is needed.

Closure is computed over `rdfs:subClassOf` triples visible to the evaluator, per SHACL. If the ontology is not loaded alongside the data, `a brick:Point` will not accept a `brick:Temperature_Sensor`. Shapeshift's import mechanism (next section) makes ontology imports part of the shapes view so this works by default; the report heuristics (later) catch the case where it does not.

**Shape-based targets** (`for every node matching S`) are expressible because shifty has `Selector::HasPath`. They have no SHACL Core equivalent, but they do have a portable SHACL-SPARQL encoding: a universal SPARQL target (every node that appears as a subject or object) on a shape whose body is when {S} then {B}. The raiser emits that by default, so any engine with SHACL-SPARQL support (shifty, pySHACL, Jena, TopBraid) evaluates it; shifty additionally reads its own shapeshift:targetShape annotation and evaluates the HasPath selector directly, which is faster. Only a pure-Core export (--core) refuses them.

**No implicit targets.** A shape with no `for` clause never fires on its own. This is a deliberate difference from SHACL, where `sh:targetClass` is often added as an afterthought and implicit class targets (`shapeClass` in SHACL-C) hide scope. In Shapeshift scope is always visible on the shape header.

## Imports

A file declares what it depends on with one keyword; resolution is a separate pass, so a parsed file can list its imports without any network access.

```
base   <https://example.org/shapes/hvac>
import <https://brickschema.org/schema/1.4/Brick.ttl>                 -- ontology
import <https://brickschema.org/schema/1.4/Brick-shapes.ttl> as bs    -- SHACL shapes graph
import "./sensors.shm" as sensors                                     -- another Shapeshift file
```

Three kinds of import, told apart by what is at the other end, not by syntax:

| Kind | Contributes | How it enters the arena |
| --- | --- | --- |
| `.shm` file | named shapes and templates, under its alias | parsed and expanded into the same arena; names prefixed `alias.` |
| RDF graph with SHACL | shapes, referenced as `alias:LocalName` | lowered with the existing `shifty-parse`; its `Statement`s are kept, so its rules fire |
| RDF ontology | vocabulary for build-time checks; `rdfs:subClassOf` for closure at validation time | added to the shapes view of the session; classes and properties indexed for diagnostics |

One graph can be both of the last two. Brick is.

### Algebra extension

```rust
pub enum ImportSource { Iri(NamedNode), Path(String) }
pub enum ImportKind { Shapeshift, Rdf }

pub struct Import {
    pub source: ImportSource,
    pub alias: Option<String>,
    pub kind: Option<ImportKind>,          // None until resolved; sniffed by extension or content type
    pub resolved_iri: Option<NamedNode>,   // canonical identity after resolution, for dedupe and export
}
// Schema gains:  pub imports: Vec<Import>   (serde default, like `names` and `prefixes`)
```

### Resolver

```rust
pub trait Resolver {
    fn fetch(&self, src: &ImportSource) -> Result<Fetched, ResolveError>;
}
pub enum Fetched { Shapeshift(String), Rdf(Graph) }
```

Shapeshift ships a `NoResolver` (lists imports, resolves nothing) and an ontoenv-backed resolver as the default: ontoenv already provides caching, offline mode and `owl:imports` closure, and shifty-lsp already uses it to build the shapes graph. `shapeshift imports file.shm` prints the transitive closure without fetching; `shapeshift imports --resolve` fetches and prints canonical IRIs and content hashes, which is what a CI step pins.

### Merge rules

- `.shm` imports are deduplicated by canonical IRI (a file's identity is its `base`). Cycles are allowed: resolution is a fixpoint over the import graph, and names are bound after all files are parsed.
- Two imports binding the same alias is an error. Two files defining the same IRI with different bodies is an error; identical bodies merge (arena interning makes this free).
- Imported SHACL graphs go through `shifty-parse` with their own prefixes; lowering diagnostics are reported against the import line.
- Ontology imports populate a `Vocabulary` index (classes, properties, their labels) consulted by the checker: an unknown prefixed name in a vocabulary that is loaded is an error with a nearest-match suggestion; a name in a prefix with no loaded vocabulary is a warning.

### Export

The raised graph gets an ontology header, `<base> a owl:Ontology ; owl:imports <...>`, with one entry per RDF import, relayed rather than inlined. `.shm` imports have no RDF identity, so their shapes are emitted inline under the defining file's base with an `shapeshift:definedIn` annotation. `--inline` materializes the whole closure into one self-contained file for consumers that do not follow `owl:imports`.

### Pinning

`import <url>` resolves to whatever the URL serves today. A lockfile (`shapeshift.lock`, in ontoenv's format) records the resolved IRI and content hash per import; `--locked` fails on drift. This keeps version pins out of the surface language.

## Recursion

Recursion is a first-class feature with shifty's semantics, not a warning. Shapeshift admits exactly what shifty admits and rejects at build time what shifty would refuse at validation time.

**What shifty does.** Shape references form a dependency graph with signed edges. `And`, `Or` and `Annotated` are positive; `Not` is negative; a `Count` lower bound is positive with respect to its qualifier and an upper bound is negative. Tarjan SCCs are computed, and within each recursive SCC sign potentials must satisfy `σ(b) = σ(a) × sign(a → b)`. A component whose cycles are sign-balanced is admitted and evaluated with greatest-fixpoint semantics for validation (least fixpoint for inference), following Corman, Reutter and Savković. A component with a net-negative cycle is refused with the offending shapes named. The W3C spec is silent on recursive shapes; shifty pins these semantics and never guesses.

**What it means in Shapeshift terms.**

```
shape Space for every brick:Space {
  brick:hasPart   each matches Space          -- admitted: each = max 0 . not, two negatives
}

shape Odd {
  none of { ex:next required, each matches Odd }   -- refused: one negation on the cycle
}
```

`each matches S` is `Count{max 0, Not(S)}`: the upper bound and the negation each flip polarity, so the dependency is positive and `S := ∀p.S` is admitted, as shifty's docs note it must be. `at most n is matches S` alone is a negative edge; `none of` is a negative edge; `when {C} then {B}` is negative on `C`. `include`, `any of`, `at least n is matches S` and field lines with `required` are positive.

**Build-time admission.** `shape` declarations are two-phase: every name is reserved as a `Pending` slot before any body is expanded, so forward and mutual references resolve without ordering rules. After expansion, the builder runs shifty-engine's admission check on the un-normalized arena. A refused SCC is reported as a path, in the author's names, with the sign-flipping steps marked:

```
error: shape `Odd` has no consistent meaning
  Odd  --none of-->  (not)  --ex:next each matches-->  Odd
  this loop passes through one negation; a node could be neither valid nor invalid
  hint: recursion through `none of`, `at most ... is matches`, or `when` conditions must be balanced
```

**The novice-facing rule**, as it appears in the language guide: a shape may refer to itself or to other shapes, directly or through other shapes, as long as no loop passes through `none of`, an `at most` qualifier, or a `when` condition an odd number of times. `each matches` is always safe.

**Export.** A recursive schema raises to legal SHACL (`sh:node` references), but its meaning is undefined under the W3C spec. The raiser attaches `shapeshift:recursive true` to the ontology header and the CLI warns that the export is portable only to engines that pin greatest-fixpoint semantics.

## Raising to SHACL

Raising is the inverse of shifty's lowering: `Schema` to SHACL RDF. It is the largest new piece, because shifty today only pretty-prints the algebra as φ/π text. The goal is SHACL a human would have written, not canonical algebra dumped into `sh:` vocabulary.

**Raise the authored arena, not the normalized one.** Normalization (CSE, NNF, count merging) erases the forms that make SHACL readable. The builder keeps the un-normalized arena with its provenance sidecar and raises from that; normalization runs afterwards for validation, as it does today for parsed SHACL.

**Pattern-directed emission.** The raiser matches arena patterns back to SHACL idioms in priority order:

| Arena pattern | Emitted as |
| --- | --- |
| `Count{p, min, max, ⊤}` | property shape with `sh:minCount` / `sh:maxCount` |
| `Count{p, max 0, Not(φ)}` | property shape with φ inlined: `sh:class`, `sh:datatype`, `sh:node`, `sh:in`, facets |
| `Count{p, min/max, φ}` otherwise | `sh:qualifiedValueShape` with `sh:qualifiedMinCount` / `MaxCount` |
| `Count{CLASS, min 1, TestConst(X)}` | `sh:class X` |
| several `Count` on the same path in one `And` | merged into one property shape |
| `Or([Not(C), B])` from a `when` origin | `sh:or ( [ sh:not C ] B )`, with `shapeshift:when` annotation |
| `xone` expansion with an `exactly one of` origin | `sh:xone`, re-folded from provenance |
| slot with a name | `sh:NodeShape` with IRI; referenced by IRI, never inlined |
| slot without a name (boolean block) | blank node, inlined once; shared slots get a generated IRI |
| `Annotated` | `sh:severity`, `sh:message` on the enclosing shape |
| `HasPath(Id, S)` selector | `universal SPARQL target + when/then encoding, annotated shapeshift:targetShape; refused only under --core` |

Provenance makes the `when` and `exactly one of` cases possible: after expansion, `xone` is a disjunction of conjunctions and `when` is a plain `Or`; the origin record is what tells the raiser which idiom to restore.

**Round-trip property.** Lowering the raised graph with `shifty-parse` and normalizing must yield an arena equal to normalizing the builder's arena. This is the correctness test for the raiser and runs in CI over the examples corpus.

**`include` on export.** An included named shape is emitted as `sh:node <S>` on the including shape when `S` is a node-level shape. Where `include` would create a property shape nesting that SHACL cannot express as `sh:node`, the raiser uses `sh:and ( <S> )`. Both round-trip.

**Portability flags.** The ontology header records anything a strict SHACL Core engine will not understand: `shapeshift:recursive`, `shapeshift:shapeTargets`, `shapeshift:sparqlTargets`. `shapeshift export --core` fails if any flag is set, for pipelines that must stay portable.

**Imports on export** are relayed as `owl:imports`, as described under Imports.

## Validation reports and error messages

A violation is reported against the line the author wrote, through every macro and include it passed through. This is the payoff of compiling to one algebra: shifty's `Reason.constraint` already identifies the algebra node that failed, and the provenance sidecar maps that node back to Shapeshift origins.

```
ex:vav-7 fails VAV (hvac.shm:12)
  via include HasPointOf(brick:Supply_Air_Temperature_Sensor)   (hvac.shm:7)
  brick:hasPoint  at least 1 is a brick:Supply_Air_Temperature_Sensor
  found 3 values of brick:hasPoint; none is a brick:Supply_Air_Temperature_Sensor
    ex:p1  a brick:Zone_Air_Temperature_Sensor
    ex:p2  a brick:Supply_Air_Flow_Sensor
    ex:p3  a brick:Damper_Position_Command
```

Compare today's `sh:QualifiedMinCountConstraintComponent` on a blank-node property shape. The content comes from shifty's evidence (the rejected candidates and why each was rejected), and the framing comes from provenance.

**Three report products**, all from the same evidence:

- Shapeshift text and JSON, as above, for the CLI, the LSP and bindings. The JSON carries the origin chain as structured data.
- W3C `sh:ValidationReport`, produced by shifty's existing W3C path, for interoperability. Shapeshift adds `shapeshift:origin` to each result so a consumer can map back if it wants to.
- Evidence objects, unchanged, for repair.

**Heuristics built on evidence.** Some failures have a likely cause that the engine can see but a novice cannot:

| Observation in the evidence | Hint added to the report |
| --- | --- |
| `a X` fails; the node has some `rdf:type` whose class has no `rdfs:subClassOf` edges in the graph | "the ontology that defines these classes may not be loaded; see `import`" |
| `a X` fails; a value's type is a subclass of `X` in an imported ontology that is not in the data view | same hint, pointing at the specific class |
| a field fails `required` and the node has the property under a different, lexically similar predicate | "did you mean `brick:hasPoint`? the data uses `brick:hasPoints`" |
| `each a X` fails on every value and `at least 1 is a X` would pass | "this line requires *all* values to be `X`; use `at least 1 is a X` if one is enough" |
| `only these properties` fails on `rdf:type` | "add `rdf:type` to the shape, or remove `only these properties`" |

Hints are advisory, separate from the result, and never change conformance.

**Build-time diagnostics**, before any data is seen: unknown prefix; unknown name in a loaded vocabulary with nearest match; parameter used at two sorts; `matches` given a class or `a` given a shape; `for every X` where `X` is a shape name; recursion refused (see Recursion); a `when` with no `then`; cardinalities with `min > max`; an `at least n of` expansion over the size cap.

## Algebra deltas

Changes proposed to existing shifty crates. Everything else lives in the new `shapeshift-*` crates.

| Crate | Change | Why |
| --- | --- | --- |
| `shifty-algebra` | `Schema.imports: Vec<Import>` with `Import`, `ImportSource`, `ImportKind` (serde default) | import declarations travel with the schema; resolution happens outside shifty |
| `shifty-algebra` | `Schema.origins: HashMap<ShapeId, Vec<Origin>>` or an equivalent sidecar type exported from the crate | provenance through macros and includes, parallel to `sources` |
| `shifty-algebra` | make `ShapeArena::reserve()` / `fill(id, shape)` public (the `Pending` protocol) if not already | two-phase construction of mutually recursive named shapes |
| `shifty-algebra` | `Schema::merge(other, alias)` that re-indexes `ShapeId`s and prefixes `names` | merging imported schemas into one arena |
| `shifty-engine` | expose the admission check (`CompiledShapes` sign-balance) as a function on an un-normalized `Schema`, returning offending SCCs as `ShapeId` paths with edge polarities | build-time recursion errors in author's names |
| `shifty-engine` | `Reason` / findings carry `ShapeId` so the sidecar can be joined to results | report provenance |
| `shifty-parse` | lowering accepts a name prefix for `names` and reports diagnostics with a source label | imported SHACL graphs under an alias |
| `shifty-cli` | `shapeshift` subcommands or a separate binary: `build`, `check`, `imports`, `export`, `explain` | the user-facing surface |
| `python/`, `shifty-wasm` | accept `.shm` text and Shapeshift JSON AST wherever SHACL Turtle is accepted today | every interface, no reimplementation |

No change to `Shape`, `Path`, `Selector` or `ValueType` variants is required: the surface language is total over the existing constructors. The one case that would need a new variant, `at least n of` as a native operator rather than an expansion, is now in scope (decided 2026-11-09): a Threshold{min, max, shapes} arena node, evaluated in O(k) by counting which children hold, with Count's polarity rules (children positive for the lower bound, negative for the upper). It raises to SHACL Core without combinatorial blow-up through a memoized dynamic-programming encoding: T(i, j) = (S\_i and T(i+1, j-1)) or T(i+1, j), which is O(k times n) shared named shapes forming a DAG, not k-choose-n terms.

## Open decisions

Decisions made in this design, for the record: the surface is a record frame with words, not S-expressions or infix logic; macros are templates compiled to the kernel and defined in the language, with host bindings later; `a` and `for every` use SHACL's subclass closure, and class membership is spelled a (with is a as a synonym); `include` may bring in targeted shapes, constraints only; recursion follows shifty's sign-balance rule and is checked at build time; imports are declared in `Schema` and resolved outside shifty.

Still open, with a recommendation each:

| Decision | Options | Recommendation |
| --- | --- | --- |
| Name of the language and file extension | Shapeshift (decided 2026-11-09; was Shapely) / `.shm`; ShiftyAST (pun on "shiftiest", and literally the AST shifty consumes) / \`.sast\`; something else | keep `.shm` as the working name; decide before the first public release |
| `type is X` vs `a X` for class membership | `a` is Turtle-familiar and short; `type is` is more explicit | `a`, with `is a` accepted as a synonym; decided 2026-11-09 (`at least 1 is a X` reads as English either way) |
| Separator inside a field line | commas between count and qualifiers; `with`; newlines | commas; the LSP formatter normalizes |
| Where severity and message attach | per line (annotation on the next line); per shape; both | both: per-line overrides per-shape |
| Iteration in user templates | none (built-ins only); `for each` over `list` params | none in v1; revisit if built-ins prove insufficient |
| `at least n of` expansion cap | 64, 256, 1024 terms | decided 2026-11-09: no cap. Native Threshold node in the algebra; export uses the O(k times n) DP encoding, see Algebra deltas |
| IRI minting for template expansions | hash of args in the fragment; sequential; author-named | hash of canonical args, stable across builds |
| Export of `include` | always `sh:node`; always `sh:and`; pattern-chosen | pattern-chosen, as described in Raising |
| Shape-based and SPARQL targ ets in `--core` export | refuse; drop with warning | decided 2026-11-09: support both in normal export. SPARQL targets emit as sh:SPARQLTarget (SHACL-SPARQL, part of the W3C rec). Shape-based targets emit as a universal SPARQL target plus when {S} then {B}, which any SHACL-SPARQL engine runs. --core (pure Core, no SPARQL) refuses both |
| Lockfile format | ontoenv's; Shapeshift's own | ontoenv's, to share one cache and one pin |
| Ontology imports and the data view | shapes view only (SHACL-correct); also visible to validation as data | shapes view, with `--ontology-as-data` for the common Brick case where closure must see the ontology |
| Blank-node shapes in imported SHACL | expose under generated names; hidden, reachable only via their parent | hidden; `matches` works only for named shapes |
| Python binding shape | tracer that produces AST; fluent builder; both | tracer first, since the AST is already JSON |

## Roadmap

Phases are ordered so each produces something usable and each later phase relies only on earlier ones. Validation runs on shifty from phase 1.

1. **Kernel builder and admission** (`shapeshift-macro`, `shifty-algebra` deltas). Two-phase named shapes over the arena, template expansion with sort inference, provenance sidecar, build-time admission with path reports. Exit: the five worked constraints from this doc build, validate on shifty, and report with provenance in JSON.
2. **Text front end** (`shapeshift-parse`). Grammar as specified, formatter, build-time diagnostics. Exit: the worked example parses; a corpus of 30 shapes drawn from Brick and 223P round-trips parse, format, parse.
3. **Raising** (`shapeshift-raise`). Pattern-directed emission, round-trip test against `shifty-parse`, portability flags. Exit: every corpus shape raises to SHACL that lowers and normalizes to the same arena.
4. **Imports** (`shapeshift-import`, `Schema.imports`). Resolver trait, ontoenv resolver, `.shm` and SHACL and ontology imports, vocabulary checks, `owl:imports` export, lockfile. Exit: the Brick shapes graph imports and its shapes can be referenced with `matches`.
5. **Reports and heuristics.** Text and JSON reports from evidence plus provenance; the five heuristics; `shapeshift:origin` on W3C results. Exit: a usability session with three RDF newcomers can read every report without help.
6. **Interfaces.** `.shm` accepted by pyshifty, WASM workbench and shifty-lsp; LSP diagnostics for build-time errors. Exit: the workbench edits `.shm` live with validation.
7. **Bindings and a domain language.** Python tracer producing the AST; one complete domain validation language (Brick) written in Shapeshift and published as the reference example.

Usability testing belongs in phases 2 and 5: Stefik-style token accuracy on the keyword set (`each`, `is`, `a`, `matches`, `any of`, `when`) with people who have not used RDF, before the vocabulary is frozen.

## References

- Ahmetaj, Boneva, Hidders, Hose, Jakubowski, Labra-Gayo, Martens, Mogavero, Murlak, Okulmus, Polleres, Savković, Šimkus, Tomaszuk. [Common Foundations for SHACL, ShEx, and PG-Schema](https://arxiv.org/abs/2502.01295). WWW 2025.
- Shifty: [shifty.gtf.fyi](https://shifty.gtf.fyi/), [GitHub](https://github.com/gtfierro/shifty), [architecture notes](https://shifty.gtf.fyi/explanation/architecture.html). Development docs read for this design: `development_docs/evaluation.md` (recursion polarity and admission), `compiler-pipeline.md`; crates `shifty-algebra`, `shifty-parse`.
- Fierro. [Evidence-Carrying Validation for Knowledge Graphs](https://gtf.fyi/papers/fierro2026evidence.pdf). 2026.
- Corman, Reutter, Savković. Semantics and Validation of Recursive SHACL. ISWC 2018.
- Stefik, Siebert. [An Empirical Investigation into Programming Language Syntax](https://www.vidarholen.net/~vidar/An_Empirical_Investigation_into_Programming_Language_Syntax.pdf). ACM TOCE 13(4), 2013.
- Lappi, Tirronen, Itkonen. [A replication study on the intuitiveness of programming language syntax](https://link.springer.com/article/10.1007/s11219-023-09631-7). Software Quality Journal 31(4), 2023.
- Cutler et al. [Cedar: A New Language for Expressive, Fast, Safe, and Analyzable Authorization](https://arxiv.org/abs/2403.04651). OOPSLA 2024; [AWS design post](https://aws.amazon.com/blogs/security/how-we-designed-cedar-to-be-intuitive-to-use-fast-and-safe/).
- [SHACL Compact Syntax](https://w3c.github.io/shacl/shacl-compact-syntax/), SHACL Community Group report.
- Jakubowski, Tomaszuk, Hose. [A Community Survey on SHACL and ShEx](https://www.alphaxiv.org/abs/2606.03502). 2026.
- [LinkML](https://linkml.io/) and the [RO-Crate LinkML evaluation notes](https://hackmd.io/@elichad/rJQpnZEwyg), on where a record-frame schema language reached domain experts and where its SHACL export fell short.
