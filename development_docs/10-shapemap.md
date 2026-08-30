# 10 — Shape maps v2: typed keys, typed values, and authored names

Status: design, ready to implement.
Prereq reading: `09-evidence.md` (evidence trees, progress, the coverage
horizon), `python/shifty/shapemap.py` (the v1 implementation this revises).

## Where we are

`shifty.shape_map()` / `shifty.ShapeMap` (v1, currently uncommitted on
`evidence-explanations`) provides a ShEx-shapemap-style view one level above
the evidence trees: one `Mapping` per selected `(shape, focus)` pair, each a
key→`Binding` record of the shape's property obligations. Bound keys carry the
values the data supplied — exact even on partially-conforming foci, via the
engine primitive `PreparedEvidenceValidator::explain_constraint`
(`EvidenceSession.evidence_for` in Python), added because a failing
conjunction's `Witness` elides its passing children. Supporting additions
already in place: `shape_name` getters on `StatementEvaluation` / `Failure` /
`Satisfaction`, tests in `python/tests/test_shapemap.py`, example in
`python/examples/shape_map_point_list.py`.

**v1 has not shipped. There are no compatibility constraints on the shapemap
module itself; break its surface freely.** Do not break anything outside it
(`EvidenceSession`, `EvidenceRun`, `witnesses()`, …).

### v1 mechanics an implementer must know

The builder (`ShapeMap.from_run`) works off `run.to_dict()` plus the pyclass
objects, zipped in statement/focus order:

- A statement's per-property decomposition comes from
  `FocusEvaluation.progress.evaluated_children`: one entry per direct child of
  the authored conjunction, carrying `source_constraint_ref` (raw arena id,
  the binding's identity) and `normalized_constraint_ref` (normalized arena
  id, used to locate evidence).
- **Annotated wrappers**: progress refs point at `Annotated` wrapper
  constraints; nodes inside evidence trees carry the *unwrapped* ids. Always
  resolve through `_Catalog.unwrap` before matching subtrees. The wrapper is
  also where `severity` lives (see §6).
- **Elided passes**: a failing focus's witness contains only failing children.
  Passing children's evidence is materialized lazily through
  `EvidenceSession.evidence_for(focus, normalized_ref)`. A child whose
  `normalized_constraint_ref` is `None` was normalized away as trivially true.
- **Term encoding** in all evidence JSON is SPARQL-JSON:
  `{"type": "uri"|"bnode"|"literal", "value": …, "datatype"?: …, "xml:lang"?: …}`.
- **Path encoding** is the externally-tagged serde of `shifty_algebra::Path`:
  `"Id"`, `{"Pred": {"value": iri}}`, `{"Inverse": p}`, `{"Seq": [p…]}`,
  `{"Alt": [p…]}`, `{"Star": p}`.
- **Encodings to recognize**: `sh:class C` lowers to
  `Count{path: Seq[Pred(rdf:type), Star(Pred(rdfs:subClassOf))], qualifier: TestConst(C)}`
  (class membership); `∀π.φ` lowers to `∃≤0 π.¬φ` (so a qualifier label may sit
  under a `Not`).
- **Counting trap**: `count_low.rejected_candidates[*].failure` contains
  nested `count_low`s describing the *candidate value*, not the binding.
  Shortfall/rejection collectors must descend only through AND/OR containers
  (`all_held`/`any_held`/`all`/`any`) and stop at the first count node
  (`_top_counts` in v1 does this correctly — keep that discipline).
- Binding order follows lowering order, not document order. Normalized ids are
  not stable across runs; raw (source) ids are stable for a fixed shapes
  document. CSE can map two distinct source children to one normalized id —
  both bindings then share one evidence subtree, which is correct.

## Goals of v2

1. Keys and values become typed, hashable, pattern-matchable structures
   instead of display strings.
2. Bindings can carry the **author's name** for the slot (`sh:name` or a
   configurable path), evaluated from the property shape node **over the
   shapes graph** — same semantics as `PreparedValidator.witnesses(key_path=…)`.
3. Bound values can carry **data-graph annotations** (e.g. a timeseries id
   reached from the matched point node), evaluated from each value node **over
   the data graph**.
4. Ergonomics: symmetric iteration, config projection, focus-first access,
   cardinality and severity on bindings.

Keys name the *slot* and are constant per shape (shapes-graph side). Value
annotations name the *rows* and vary per focus (data-graph side). Do not
conflate them; the API keeps them as separate features (`name_path` vs
`value_paths`).

---

## 1. Typed term vocabulary (`Term`)

New frozen dataclasses in `python/shifty/shapemap.py` (or a small
`python/shifty/terms.py` if preferred — keep them importable from `shifty`):

```python
@dataclass(frozen=True)
class Iri:
    __match_args__ = ("value",)
    value: str                          # the IRI text, no angle brackets

@dataclass(frozen=True)
class Literal:
    __match_args__ = ("value", "datatype", "language")
    value: str                          # lexical form
    datatype: Optional[str] = None      # IRI text; None for plain/xsd:string
    language: Optional[str] = None

@dataclass(frozen=True)
class BNode:
    __match_args__ = ("id",)
    id: str

Term = Union[Iri, Literal, BNode]
```

Required behavior:

- `from_json(dict) -> Term` (SPARQL-JSON, above) and `parse(str) -> Term`
  (N-Triples spelling — needed to convert `FocusEvaluation.focus`, which the
  pyclasses render as `<…>` / `"…"` / `_:…`).
- `.n3() -> str`: the N-Triples spelling v1 used (keep escaping identical to
  v1's `_render_term`; xsd:string datatype elided).
- `str(term)`: same as `.n3()` for `Iri`/`BNode`; for `Literal` the bare
  lexical form is more useful — decide once, document, and keep `to_dict()`
  JSON using `.n3()` so summaries stay unambiguous.
- `Literal.to_python()`: coerce by datatype (int/float/bool/decimal/str;
  fall back to the lexical form).
- `.to_rdflib()`: lazy `import rdflib`, mirror the graph-input helpers in
  `__init__.py`.

Everywhere v1 exposed rendered strings now speaks `Term`: `Mapping.focus`,
`Binding.values`, `partial_values`, `rejected_values`. `ShapeMap.to_dict()`
keeps plain strings (`.n3()`).

## 2. Typed keys (`Key`, `Path`, `Qualifier`)

```python
# Paths — mirror shifty_algebra::Path, frozen, with __match_args__:
Id, Pred(iri: str), Inv(path), Seq(parts: tuple[Path, ...]),
Alt(parts: tuple[Path, ...]), Star(path)

# Qualifiers:
@dataclass(frozen=True)
class Cls:       iri: str          # sh:class C / class-membership
@dataclass(frozen=True)
class Const:     term: Term        # sh:hasValue / TestConst
@dataclass(frozen=True)
class Datatype:  iri: str          # sh:datatype (TestType)
@dataclass(frozen=True)
class ShapeRef:  iri: str          # sh:node <named shape>  (see §7)
Qualifier = Union[Cls, Const, Datatype, ShapeRef]

@dataclass(frozen=True)
class Key:
    __match_args__ = ("path", "qualifier")
    path: Optional[Path]            # None for pathless constraints (nodeKind…)
    qualifier: Optional[Qualifier]
    ordinal: int = 1                # disambiguates identical (path, qualifier)
    kind: str = "count"             # constraint tag fallback for pathless keys
```

- `Key` derivation replaces v1's `_derive_key` string logic but follows the
  same source-catalog walk (unwrap `Annotated`; `Count` → path + qualifier;
  `And`/`Or` sharing one path collapse to it; recognize the class-membership
  and `∀`-encodings; unwrap `Not` when labeling a `∀` qualifier).
- `ordinal` replaces the `#2` suffix: the *n*-th binding in lowering order
  with the same `(path, qualifier, kind)` gets `ordinal=n`. Included in
  equality/hash so `Mapping.bindings: dict[Key, Binding]` stays a real dict.
  Stable for a fixed shapes document (raw arena order is deterministic).
- `str(Key)` produces the v1 display form (`hasPoint→Supply_Air_Flow_Sensor`,
  `label`, `label#2`, `nodekind`), using local-name compaction. Keep the
  rendering helpers.
- `Mapping.__getitem__` accepts a `Key` or its `str()` display form.
- `Binding.key: Key`; `Binding.source_constraint_id` remains as the exact
  in-run identity.

## 3. Symmetric iteration and config projection

- `Mapping.successful` and `Mapping.unsuccessful` both yield
  `(Key, Binding)` pairs (v1 asymmetrically yielded `(key, values)` on the
  successful side).
- `Mapping` implements `collections.abc.Mapping[Key, Binding]`
  (`__len__`, `__iter__` over keys, `__getitem__`) — drop v1's
  `__iter__`-over-bindings in favor of the standard protocol; `.bindings`
  stays as the ordered dict.
- New projection for the configuration use case:

```python
mapping.value_map() -> dict[Key, list[Term]]          # bound keys only
mapping.value_map(by="name") -> dict[str, list[Term]] # keys by binding.name,
                                                      # falling back to str(key)
mapping.value_map(python=True)                        # Literal.to_python() applied,
                                                      # Iri -> iri string
```

## 4. Focus-first access

`ShapeMap.for_focus(focus: Term | str) -> list[Mapping]` — every mapping whose
focus is that node, across shapes. Build a lazy index on first use. Accept
either a `Term` or an N-Triples/plain-IRI string (normalize via
`Term.parse`; also accept a bare IRI without angle brackets).

## 5. Cardinality on `Binding`

Expose what the evidence already contains, read from the binding's *own*
top-level count nodes (same `_top_counts` descent):

- `min: Optional[int]`, `max: Optional[int]` — from the source constraint
  (`Count.min`/`Count.max`), not the evidence, so they're present even when
  evidence wasn't materialized. For collapsed `And`s (e.g. datatype +
  minCount on one path) take the tightest bounds across the children.
- `observed: Optional[int]` — from evidence (`count_held.observed_count`,
  `count_low.have`; `None` when evidence is unavailable).
- `expects_single: bool` — `min == max == 1`.
- `missing` stays as-is.

## 6. Severity on `Binding`

The normalized progress ref points at the `Annotated` wrapper whose catalog
entry is `{"Annotated": {"severity": "Violation"|"Warning"|"Info", …}}`.
Surface it as `Binding.severity: str` (lowercase). Fall back to the
statement-level wrapper when the child has no annotation of its own; default
`"violation"`.

## 7. Source provenance + `name_path` (authored slot names)

The blocker: bindings are identified by raw arena `ShapeId`s, and the arena
records no originating RDF node, so nothing can be evaluated *from the
property shape's node*. Fix it at the parser:

**Rust — `shifty-parse` / `shifty-algebra`:**

- Add to `Schema` (in `crates/shifty-algebra/src/schema.rs`), parallel to
  `names`:

  ```rust
  /// Originating shapes-graph node for arena slots lowered from an RDF node
  /// (named or blank). Synthetic slots introduced by lowering have no entry.
  #[serde(default, skip_serializing_if = "HashMap::is_empty")]
  pub sources: HashMap<ShapeId, Term>,
  ```

- Populate it during lowering in `shifty-parse` for at least: every node
  shape, every `sh:property` property shape, and every
  `sh:qualifiedValueShape`/`sh:node` target it references. Record the id of
  the shape's *outermost* slot for that RDF node (the `Annotated` wrapper if
  one is created), since progress children reference that id. Only the raw
  schema needs this; do not try to carry it through normalization.
- This must not disturb CSE/normalization or any serde snapshot tests
  (`sources` is skip-if-empty, and the normalized schema simply won't have
  entries).

**Rust — python bindings (`python/src/repair.rs`):**

- `EvidenceSession.binding_names(name_path: Option<&str>) -> dict[int, list[str]]`:
  map raw constraint id → the values `name_path` reaches from that
  constraint's source node, evaluated **over the shapes graph**. Parse the
  path with `shifty_parse::parse_property_path(&expr, &loaded)` — the same
  call `PreparedValidator.witnesses(key_path=…)` makes
  (`python/src/lib.rs`, `witnesses`); it resolves bare `prefix:local` steps
  against `Loaded.prefixes`. Path evaluation over the shapes graph can follow
  the reporter's approach in `crates/shifty-engine/src/report.rs`
  (`collect_property_binding`). Default `name_path=None` means `sh:name`.
  Requires `EvidenceSession` to retain the loaded shapes graph — it currently
  drops the `Loaded` at the end of `new`; keep it as a field, exactly as the
  `PreparedValidator` pyclass already does (`self.shapes: Loaded`).
- Also expose the raw-schema names table (or a
  `shape_name_of(constraint_id)` lookup) so qualifier derivation can render
  `sh:node <named shape>` qualifiers as `ShapeRef(iri)` — v1 degrades these
  to path-only keys because the run catalog has no names per arena id.

**Python:**

- `shape_map(..., name_path: Optional[str] = "sh:name")` and
  `ShapeMap.from_run(..., name_path=…)`. Fetch `binding_names` once per map
  (needs the session; without one, names are `None`).
- `Binding.name: Optional[str]` — first value, or `None`. Multiple values:
  keep the list reachable (`Binding.names`), use the first for `.name`.
- `Mapping.by_name(name) -> Binding` (KeyError on absence; names are not
  guaranteed unique — return the first in binding order, and note this).
- Drop the v1 `key=` callable — structured `Key` plus `name_path` covers its
  uses. (If kept, it must receive the new `Key`.)

## 8. `value_paths` (data-graph value annotations)

Names the matched *values* — e.g. a point's timeseries id or BACnet
reference — evaluated from each bound value node **over the evaluation/data
graph**.

**Rust:** `EvidenceSession.resolve_path(nodes: Vec<String>, path: &str) ->
dict[str, list[str]]` — batch-evaluate one SPARQL property path from each
given term (N-Triples spelling in, N-Triples spellings out), over the
session's evaluation graph (the same graph validation read — under `union`
mode that is data ∪ shapes; match `graph_mode` semantics). Reuse
`parse_property_path` from §7; it already accepts full-IRI `<iri>` steps, so
callers aren't hostage to the shapes doc's prefixes.

**Python:**

```python
smap = shifty.shape_map(data, shapes,
    name_path="sh:name",
    value_paths={"ts": "ref:hasTimeseriesReference/ref:hasTimeseriesId"})

binding.values            # list[Term] — unchanged
binding.annotations       # dict[str, dict[Term, list[Term]]]: label -> value -> reached
binding.annotated_values  # list[BoundValue]; BoundValue(term, annotations: dict[str, list[Term]])
```

Resolve lazily and in batch: collect all bound value terms for the map on
first access, one `resolve_path` call per label. Keep `value_paths` entirely
optional — no cost when absent.

## 9. Misc

- `Mapping.__match_args__ = ("focus", "shape_name", "conforms")`.
- `ShapeMap.to_dict()` gains `"name"` per binding when known; keys serialize
  as `str(key)`.
- Update `python/examples/shape_map_point_list.py` to show `name_path` +
  `value_paths` + a `match` statement; update the `shape_map` blurb in
  `python/shifty/__init__.py`'s docstring and the CHANGELOG (extend the
  existing Unreleased entries — v1 never shipped, so amend rather than
  append).
- Docs: add a page under `docs/python-api/` if the sphinx tree grows one for
  shapemap; at minimum keep module docstrings current.

## Non-goals (this round)

- Per-binding `repair_tree()` (needs engine synthesize over a witness
  subtree).
- `ShapeMap.diff(other)` — users can build it once keys are stable/hashable.
- Focus labels from the data graph — one rdflib lookup by the caller.
- Carrying provenance through normalization.

## Test plan

Extend `python/tests/test_shapemap.py` (keep all v1 cases green, updated to
the new types):

1. `Key` equality/hash/`str()`; ordinal disambiguation for duplicate
   `(path, qualifier)`; `match` statements over `Key` and `Term` actually
   exercised in a test.
2. `Term.parse` ↔ `.n3()` round-trips: IRI, plain literal, typed literal,
   language-tagged literal, bnode, literals containing quotes/newlines.
3. `name_path`: direct `sh:name`; a multi-hop path (`ex:role/ex:roleName`
   style); absent annotation → `name is None`; `by_name` lookup;
   `value_map(by="name")` fallback to `str(key)`.
4. `sources` provenance: named and blank property shapes both resolve;
   deterministic across two parses of the same document.
5. `ShapeRef` qualifier: `sh:node :named-shape` yields
   `Key(path=Pred(...), qualifier=ShapeRef("…"))` and a `hasPart→heating-coil`
   display.
6. `value_paths` on a small graph with a two-hop reference; a value with no
   annotation → empty list; batch behavior (no per-value session call —
   assert via call counting on a wrapped session if practical).
7. Cardinality: `min`/`max`/`observed`/`expects_single` for minCount,
   maxCount, qualified counts, and the collapsed datatype+minCount case;
   severity from `sh:severity sh:Warning`.
8. `for_focus` across two shapes selecting the same node.
9. Rust: unit tests for `sources` population in `shifty-parse` and for
   `binding_names`/`resolve_path` behavior via the Python suite (the pyo3
   layer has no separate Rust test harness — `uv run pytest` from `python/`
   after `uv run maturin develop`).

## Suggested order

1. §1 `Term` + §2 `Key` (pure Python, biggest churn — do first, migrate v1
   tests).
2. §3–§6 (pure Python, small).
3. §7 provenance + `binding_names` (Rust: algebra + parse + pyo3), then wire
   `name_path`/`ShapeRef` in Python.
4. §8 `resolve_path` + `value_paths`.
5. Examples, docs, CHANGELOG amendments.

Build loop: `cd python && uv run maturin develop && uv run pytest -q`; run
`cargo test -q` at the workspace root after the Rust changes, and `cargo fmt`
**only on touched files** (the repo is not fmt-clean; a bare `cargo fmt`
pollutes the diff).
