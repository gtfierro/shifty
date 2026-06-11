# Human-Readable SHACL Validation Output Design

## Overview

This document describes the design for an enhanced validation output mode that interprets the formal algebraic messages into human-actionable feedback. The goal is to help users understand **what** is wrong, **why** it's wrong, and **how** to fix it.

## Current Output (Algebraic)

```
conforms: false
violations: 3
  <http://example.org/person1>  [target: ∃≥1 rdf:type/rdfs:subClassOf* . φ]
      - (<http://example.org/email>) "invalid-email" → test(datatype(xsd:string) & pattern(/^[^@]+@[^@]+\.[^@]+$/)) not satisfied
```

## Proposed Output (Human-Readable)

```
❌ Validation Failed: 3 violations found

================================================================================
VIOLATION 1
================================================================================
Entity: <http://example.org/person1>
Problem: Invalid email format

Description:
  The email address "invalid-email" does not match the required pattern for
  email addresses. Email values must be strings that match the format:
  local-part@domain.tld

Constraint:
  Property: <http://example.org/email>
  Required: A string matching pattern /^[^@]+@[^@]+\.[^@]+$/

How to Fix:
  Option A (Change the value):
    ex:person1 ex:email "valid@example.org" .

  Option B (Remove the value):
    # Delete the invalid triple entirely

  Option C (Relax the constraint - if appropriate):
    # In your shapes graph, modify sh:pattern to be less restrictive
    # Example: Change sh:pattern "^[^@]+@[^@]+\\.[^@]+$" to ".*"

================================================================================
VIOLATION 2
================================================================================
Entity: <http://example.org/person2>
Problem: Age out of valid range

Description:
  The age value 200 is outside the allowed range of 0 to 150.

Constraint:
  Property: <http://example.org/age>
  Required: An xsd:integer between 0 and 150 (inclusive)

How to Fix:
  Option A (Change the value to a valid number):
    ex:person2 ex:age "25"^^xsd:integer .

  Option B (Remove the value):
    # Delete the invalid triple

  Option C (Adjust the constraint bounds - if appropriate):
    # In your shapes graph, modify sh:maxInclusive
    # Example: Change sh:maxInclusive "150"^^xsd:integer to "300"^^xsd:integer

================================================================================
VIOLATION 3
================================================================================
Entity: <http://example.org/person3>
Problem: Wrong datatype for age

Description:
  The value "30" is not an xsd:integer. It appears to be a plain string
  without a datatype annotation.

Constraint:
  Property: <http://example.org/age>
  Required: An xsd:integer

How to Fix:
  Option A (Add proper datatype):
    ex:person3 ex:age "30"^^xsd:integer .

  Option B (Remove the value):
    # Delete the invalid triple

  Option C (Relax the datatype constraint - if appropriate):
    # In your shapes graph, remove or modify sh:datatype
    # Example: Remove the sh:datatype triple from the property shape
```

## Constraint Type Mappings

### 1. Cardinality Violations

| Algebraic Form | Human Description | Fix Suggestions |
|----------------|-------------------|-----------------|
| `∃[..0] p . ¬φ` / "at most 0, found N" | "Too many values along path X" | Remove N-Nmax values, or adjust maxCount |
| `∃[1..] p . φ` / "at least 1, found 0" | "Missing required value along path X" | Add a value, or adjust minCount to 0 |
| `∃[N..M] p . φ` / "expected N-M, found K" | "Wrong number of values for X (expected N-M, got K)" | Add/remove values, or adjust bounds |

### 2. Value Type Violations

| Algebraic Form | Human Description | Fix Suggestions |
|----------------|-------------------|-----------------|
| `test(datatype(X))` | "Wrong datatype: expected X" | Add correct datatype annotation |
| `test(pattern(R))` | "Does not match pattern R" | Change value to match pattern |
| `test(range(lo, hi))` | "Value out of range [lo, hi]" | Change value to be within range |
| `test(length[min, max])` | "String length out of range [min, max]" | Shorten or lengthen the string |
| `test(langIn(L))` | "Language tag not in allowed set L" | Change language tag or allowed set |

### 3. Logical Composition Violations

| Algebraic Form | Human Description | Fix Suggestions |
|----------------|-------------------|-----------------|
| `¬φ` (NOT) | "Value should NOT satisfy X" | Change value so it doesn't match X |
| `φ₁ ∧ φ₂` (AND) | "Value must satisfy ALL of: X, Y, ..." | Fix each failing sub-constraint |
| `φ₁ ∨ φ₂` (OR) | "Value must satisfy AT LEAST ONE of: X, Y" | Make value satisfy at least one option |

### 4. Closed Shape Violations

| Algebraic Form | Human Description | Fix Suggestions |
|----------------|-------------------|-----------------|
| `closed{p1, p2, ...}` | "Unexpected property Y found" | Remove the property, or add to allowed list |

## Existential Variable / TGD-Inspired Suggestions

When a violation involves **missing values** (existential requirements), we can provide TGD-inspired suggestions:

### Pattern: Missing Required Property

```
Problem: Missing required property

TGD-inspired interpretation:
  The constraint requires that for this entity, there exists some value
  reachable via the path <http://example.org/hasEmail>.

Suggested triples to add:
  # Option 1: Add a simple literal value
  <http://example.org/person1> <http://example.org/hasEmail> "person@example.org" .

  # Option 2: If the value should be another resource
  <http://example.org/person1> <http://example.org/hasEmail> <http://example.org/email1> .
  <http://example.org/email1> a <http://example.org/Email> ;
      <http://example.org/address> "person@example.org" .
```

### Pattern: Qualified Count Violation

```
Problem: Qualified count violation

TGD-inspired interpretation:
  The constraint requires between N and M values along path P that satisfy
  the qualifier shape Q. Currently, K values satisfy the qualifier.

  Qualifier shape Q requires:
    - Property X must have a value of datatype Y
    - Property Z must not exist

Suggested approach:
  1. Identify existing values along path P
  2. For values that don't satisfy Q, either:
     - Modify them to satisfy Q (add missing properties, fix datatypes)
     - Remove them from the relationship
  3. If K < N, add new values that satisfy Q
  4. If K > M, remove excess values that satisfy Q
```

## Implementation Architecture

### New CLI Mode

Add a new output format `--format human` to the validate command:

```bash
shacl validate --shapes shapes.ttl --data data.ttl --format human
```

### Components

1. **AlgebraInterpreter**: Maps algebraic shape expressions to human-readable descriptions
   - `shape_to_description(arena, shape_id)` → String
   - `constraint_to_fix_suggestions(shape_id, actual_value)` → FixSuggestions

2. **FixSuggestionGenerator**: Generates actionable fix examples
   - For datatype errors: suggest proper literal form
   - For pattern errors: show example matching values
   - For cardinality errors: show add/remove triple examples
   - For range errors: show valid value examples

3. **TGDHintGenerator**: For existential requirements, generates TGD-style hints
   - Analyzes the path and qualifier to suggest concrete triples

### Data Structures

```rust
pub struct HumanReadableViolation {
    pub focus: Term,
    pub path: Option<Term>,
    pub value: Option<Term>,
    pub title: String,           // Short problem description
    pub description: String,     // Detailed explanation
    pub constraint_details: ConstraintDetails,
    pub fix_suggestions: Vec<FixSuggestion>,
    pub severity: Severity,
}

pub struct FixSuggestion {
    pub label: String,           // "Option A", "Option B", etc.
    pub action: FixAction,       // What kind of fix
    pub example_triples: String, // Turtle-formatted example
    pub explanation: String,     // When to use this fix
}

pub enum FixAction {
    ChangeValue { new_value_example: String },
    RemoveValue,
    RelaxConstraint { shape_modification: String },
    AddMissingValue { suggested_triples: String },
}
```

## Example: Brick Schema Violations

For the Brick schema violations we saw earlier:

```
================================================================================
VIOLATION
================================================================================
Entity: <http://qudt.org/vocab/quantitykind/AbsoluteHumidity>
Problem: Missing required QUDT reference

Description:
  This quantity kind should have a <https://brickschema.org/schema/Brick#hasQuantity>
  relationship pointing to a valid quantity reference. Currently, no such
  relationship exists.

Constraint:
  Path: rdf:type/rdfs:subClassOf*
  Requirement: At least 1 value must exist

TGD-inspired Suggestion:
  Add a hasQuantity relationship to this entity:

  <http://qudt.org/vocab/quantitykind/AbsoluteHumidity>
      <https://brickschema.org/schema/Brick#hasQuantity>
          <http://qudt.org/2.1/vocab/quantitykind/AbsoluteHumidity> .

  Or verify that the QUDT vocabulary is properly imported and the
  class hierarchy is correctly defined.
```

## Future Enhancements

1. **Interactive Mode**: Allow users to select a fix suggestion and generate the diff
2. **Constraint Learning**: Suggest constraint relaxations based on data patterns
3. **Schema Repair**: Auto-generate corrected shapes graphs
4. **Data Repair**: Auto-generate corrected data graphs with proposed fixes
