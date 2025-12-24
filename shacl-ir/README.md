# shacl-ir

`shacl-ir` defines a compact, serializable intermediate representation (IR) for SHACL shapes and rules. It is used by the validator to move from parsed RDF graphs to a typed, engine-friendly model that supports fast validation, inference, and caching.

The crate is intentionally "data-only": it exposes Rust structs/enums plus `serde` derives for easy JSON/CBOR/etc. serialization, and uses Oxigraph's RDF terms (`Term`, `NamedNode`, `Quad`) for RDF values.

## High-level goals

- Provide a stable, explicit model for SHACL shapes (node shapes + property shapes)
- Capture constraint components and rule definitions in a uniform, inspectable form
- Preserve identifiers and source terms for traceability back to the RDF graph
- Enable caching and reuse (e.g., saving shape IR and reloading it later)

## Core ID types

All major graph entities are represented by numeric IDs:

- `ID`: node shape identifiers
- `PropShapeID`: property shape identifiers
- `ComponentID`: constraint component identifiers
- `RuleID`: SHACL rule identifiers

Each ID is a `u64` newtype with a `Display` impl and a stable Graphviz-friendly prefix.

## Paths and targets

SHACL property paths are captured in `Path`:

- `Simple(Term)`
- `Inverse`, `Sequence`, `Alternative`
- `ZeroOrMore`, `OneOrMore`, `ZeroOrOne`

Targets are represented as:

- `Target::Class`, `Target::Node`, `Target::SubjectsOf`, `Target::ObjectsOf`, `Target::Advanced`

Severity is captured in `Severity` (`Info`, `Warning`, `Violation`, or `Custom`).

## Components

Constraint components are modeled with `ComponentDescriptor`. It includes core SHACL components (class, datatype, nodeKind, counts, value ranges, patterns, language constraints, list/set constraints, property comparisons, logic combinators, closed shapes, hasValue, in, sparql) plus a `Custom` variant for user-defined components.

Custom components include:

- a definition (`CustomConstraintComponentDefinition`)
- parameter bindings (`ParameterBindings`)
- optional SPARQL validators and messages

## Rules

SHACL rules are represented by:

- `Rule::Sparql` (SPARQL-based rule)
- `Rule::Triple` (triple template rule)

Rules store ordering, activation flags, and condition shapes. Rule pattern terms are expressed via `TriplePatternTerm` (`This`, `Constant`, `Path`).

## Templates

The IR can carry component and shape template definitions:

- `ComponentTemplateDefinition`
- `ShapeTemplateDefinition`
- `TemplateParameter` and `TemplateValidators`

This enables template-aware processing without re-parsing RDF source graphs.

## Shape IR container

`ShapeIR` is the top-level container:

- `shape_graph`, optional `data_graph`
- `node_shapes`, `property_shapes`
- `components`, `rules`
- `node_shape_terms`, `property_shape_terms` (for traceability)
- `shape_quads` (original shape graph quads)
- rule index maps (`node_shape_rules`, `prop_shape_rules`)
- feature toggles (`FeatureToggles`)

## Typical usage

`shacl-ir` is designed to be:

1. Constructed by a parser/loader.
2. Passed to the validation engine.
3. Optionally serialized and cached for reuse.

See `lib/src/ir.rs` and the validator builder in `lib/src/lib.rs` for integration details.
