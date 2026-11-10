# Shape-map projection

A shape map projects evidence into one mapping per selected `(shape, focus)`
pair, with typed keys for authored property obligations and typed RDF values.
The projection preserves source identity even when normalization shares or
elides executable constraints.

Implementation: [shapemap.py](../python/shifty/shapemap.py),
[terms.py](../python/shifty/terms.py), and
[repair.rs](../python/src/repair.rs).
Public usage is in [the shape-map reference](../docs/reference/shape-maps.rst).

## Typed vocabulary

Terms are frozen, hashable `Iri`, `Literal`, and `BNode` values. `from_json`
decodes SPARQL-JSON evidence terms; `parse` decodes rendered terms. `.n3()`
provides an unambiguous RDF spelling, and literal Python conversion is explicit.

Paths mirror the algebra with `Id`, `Pred`, `Inv`, `Seq`, `Alt`, and `Star`.
Qualifiers are `Cls`, `Const`, `Datatype`, or `ShapeRef`. A `Key` combines path,
qualifier, kind, and ordinal. Ordinals distinguish authored obligations with
the same structural key; display strings compact IRIs but do not replace typed
identity.

`Key.kind` describes authored SHACL families, with `compound` for several
families, `custom` for one custom component, and `unconstrained` for a source
with no components. Ordinary cardinalities share `count`, and qualified
cardinalities share `qualified_count`. `Binding.algebra_kind` retains the
lowered tag. `Binding.constraint_components` retains authored IRIs, including
activated custom components, as a sorted tuple from the binding's source shape. This metadata
is present on session-free projections too; nested qualifiers remain separate.
`ShapeMap.to_dict()` includes components, position, and string-encoded decimal
order alongside the value/status summary.

`Binding.explain()` summarizes authored components and the binding's status,
counts, qualifying values, and rejected values. Component labels describe
declarations, not per-component verdicts. `explain(algebra=True)` retains the
evidence-tree rendering, which is also the fallback without source metadata.
Summary extraction shares the binding's lazy observation cache.

Evidence readers classify each count using its raw-to-normalized constraint
mapping, retained alongside the run's binding layout. A nonempty `per_value`
list is not enough to identify a universal: negated qualified counts can have
the same witness structure. Universal successes constrain path values but do
not add matches to an authored count's extraction domain. Their counterexample
counts also do not contribute to the authored count's observation. If source
roles cannot be distinguished after normalization, extraction remains unknown.

`Mapping` implements the standard mapping protocol over `Key → Binding`.
Successful and unsuccessful views both yield `(Key, Binding)` pairs.
`value_map` projects bound values, optionally by authored name and with Python
literal conversion. `ShapeMap.for_focus` builds its lookup lazily.

## Source-to-evidence matching

`ShapeMap.from_run` uses both source and normalized constraint catalogs.
`EvaluationProgress.evaluated_children` describes the authored conjunction's
immediate children and carries source and normalized references. If no such
progress exists, the statement root supplies the obligation.

Annotated wrappers are transparent logically, but hold severity and source
identity. Evidence nodes can carry unwrapped IDs, so subtree matching unwraps
annotations first. Source-owner metadata then groups sibling constraints that
came from one property shape back into one binding, including singleton
properties whose wrapper was elided during lowering.

Normalization may map distinct source children to the same executable ID.
Bindings retain distinct source identities and can share evidence. Ordinal
assignment follows stable binding order: ascending decimal `sh:order`, then
unordered sources, with source IRIs or canonical anonymous identities breaking
ties. `Binding.position` numbers the full ordered set without changing key
equality. Lazy shapes-side metadata is shared by sessions and runs, so this
order also survives `ShapeMap.from_run(run)` without a session. Canonical
anonymous identities are local to a fixed graph and library version, not public
persistent IDs. Normalized IDs are local to a compiled schema.

## Lazy evidence and values

A failing conjunction's canonical witness omits passing siblings. Their binding
status remains known from progress, while evidence is resolved on demand with
the originating session's `_evidence_for`. Without that session, unavailable
values remain `None` rather than being mistaken for an empty value set.

Source property ownership also permits `_binding_values` to recover authored
paths and qualifiers. An unbounded qualified value shape is vacuous for
validation and can normalize to top, but still has useful matched values.
Session-backed extraction evaluates its authored path and filters candidate
values against the raw qualifier instead of assuming top means no values.

Collectors stop at the binding's own count boundary. Nested `count_low` nodes
inside rejected-candidate failures describe a candidate's obligations, not the
outer property's cardinality. Recursing through them would miscount missing or
matched values.

`Binding.values` contains qualifying values, including near-matches for a
failing count. `partial_values`, `rejected_values`, and `missing` describe the
failure. Declared `min` and `max` come from source constraints, using tightest
bounds for combined obligations. A lazy session observation recovers accepted
and rejected candidates when canonical universal failures prune successes.
`partial_values` equals `values` on failure, including `None` when the complete
qualifying set is unknown. `observed` counts the authored cardinality domain,
or path values for a universal check; combining distinct count domains gives
`None`. Recovery only consults a matching session snapshot, never a stale
session after revalidation. Severity comes from the relevant annotation with
statement fallback.

## Names and annotations

Slot names and value annotations use different graph roles:

| Projection | Starting node | Read graph |
| --- | --- | --- |
| `name_path`, default `sh:name` | Authored property-shape source | Shapes graph |
| `value_paths` | Each matched value | Session evaluation graph |

`Schema.sources` retains RDF source nodes for authored arena slots.
Transparent annotation chains are followed when locating singleton property
names. `Binding.names` retains all names; `.name` and `Mapping.by_name` select
the first in binding order. Names are not required to be unique.

Value annotations resolve lazily across the map, with one batch path call per
configured label. Their graph respects validation mode, including union reads.
`BoundValue` pairs each term with its annotations. With no session, graph-based
name and value resolution is unavailable.

The property-path parser accepts full IRIs and document prefixes. `sh:` has a
built-in fallback for the public default, while explicit document prefix
bindings take precedence.

## Serialization and identity

`to_dict()` renders typed keys as display strings and terms with RDF spellings
for JSON consumers. The typed in-memory keys retain their structural identity.
Authored source IDs, normalized IDs, and display names serve different purposes
and must not be interchanged when caching or matching bindings.
