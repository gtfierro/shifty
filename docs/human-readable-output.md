# Human-Readable SHACL Validation Output Design

## Overview

This document describes the design for an enhanced validation output mode that interprets the formal algebraic messages into human-actionable feedback. The goal is to help users understand **what** is wrong, **why** it's wrong, and **how** to fix it.

## Current Output (Algebraic)

```
conforms: false
violations: 3
  <http://example.org/person1>  [target: class(<http://example.org/Person>)]
      - (<http://example.org/email>) "invalid-email" → test(datatype(xsd:string) & pattern(/^[^@]+@[^@]+\.[^@]+$/)) not satisfied
```

(The target renderer resolves a path selector's qualifier against the arena, so
a class target reads `class(C)` rather than the bare `∃≥1 rdf:type/rdfs:subClassOf* . φ`.)

## Proposed Output (Human-Readable)

### Normal Mode (default human format)

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
  local-part@domain.tld.

Constraint Path:
  Statement: ∃≥1 rdf:type/rdfs:subClassOf* . <http://example.org/Person>
    → @0 = @1 ∧ @7 (must satisfy ALL)
      → @7 = ∃[..0] ex:email . ¬@11 (all emails must pass check)
        → @11 = ¬@10 (double negation)
          → @10 = test(datatype(xsd:string)) ∧ test(pattern(...))
            → ❌ pattern check FAILED

What you have:
  <http://example.org/person1> <http://example.org/email> "invalid-email" .

How to Fix:
  Option A - Change the value:
    <http://example.org/person1> <http://example.org/email> <value-matching-pattern> .

  Option B - Remove the value:
    # Delete: <http://example.org/person1> <http://example.org/email> "invalid-email" .

================================================================================
VIOLATION 2
================================================================================
Entity: <http://example.org/person2>
Problem: Age out of valid range

Description:
  The age value 200 is outside the allowed range of 0 to 150.

Constraint Path:
  Statement: ∃≥1 rdf:type/rdfs:subClassOf* . <http://example.org/Person>
    → @0 = @1 ∧ @7 (must satisfy ALL)
      → @1 = ∃[..0] ex:age . ¬@5 (all ages must pass check)
        → @5 = ¬@4 (double negation)
          → @4 = test(datatype(xsd:integer)) ∧ test(range(≥0, ≤150))
            → ❌ range check FAILED (value 200 > 150)

What you have:
  <http://example.org/person2> <http://example.org/age> "200"^^xsd:integer .

How to Fix:
  Option A - Change to valid value:
    <http://example.org/person2> <http://example.org/age> <integer-between-0-and-150> .

  Option B - Remove the value:
    # Delete: <http://example.org/person2> <http://example.org/age> "200"^^xsd:integer .

================================================================================
VIOLATION 3
================================================================================
Entity: <http://example.org/person3>
Problem: Missing required property

Description:
  The entity is missing required properties. For ex:Person shapes, the shape
  requires a specific relationship along ex:hasEmail.

Constraint Path:
  Statement: ∃≥1 rdf:type/rdfs:subClassOf* . <http://example.org/Person>
    → @0 = @1 ∧ @7 (must satisfy ALL)
      → @1 = ∃[..0] ex:age . ¬@5 (all ages must pass check)

How to Fix:
  Option A - Add missing property:
    <http://example.org/person3> <http://example.org/age> <new-age-value> .

  Option B - Check if property is required (minCount > 0):
    # If this property is optional, the shape may need adjustment
    # (Note: This version only suggests data fixes, not schema changes)
```





## Implementation Architecture

### CLI Commands

```bash
# Normal mode: human-readable with constraint path
shacl validate --shapes shapes.ttl --data data.ttl --format human

# Verbose mode: include full TGD dependency chain
shacl validate --shapes shapes.ttl --data data.ttl --format human --verbose-tgd
```

## TGD Chain (with --verbose-tgd flag)

```
TGD Dependency Chain (--verbose-tgd):
  Statement: ∃≥1 rdf:type/rdfs:subClassOf* . <http://example.org/Person>
    ↓ (targets all ex:Person instances)
  @0 = @1 ∧ @7
    ↓ (requires BOTH constraints)
  @1 = ∃[..0] ex:age . ¬@5
    ↓ (all age values must pass check)
  @5 = ¬@4
    ↓ (double negation cancels)
  @4 = test(datatype(xsd:integer)) ∧ test(range(≥0, ≤150))
    ↓ (value must satisfy both)
  ❌ FAIL: value "200"^^xsd:integer not in range [0, 150]

  The rule pattern is: Person(?x) → ∃?y (age(?x, ?y) ∧ Integer(?y) ∧ Range(?y, 0, 150))
  
  To fix, add a value satisfying all constraints:
    <focus-node> ex:age <integer-between-0-and-150> .
```

## Data Fix Templates (No Schema Changes)

All suggestions focus on **data repairs only** - never schema modifications:

| Violation | Fix Template |
|-----------|-------------|
| Pattern mismatch | `<s> <p> <value-matching-pattern> .` |
| Datatype mismatch | `<s> <p> "value"^^<datatype> .` |
| Range violation | `<s> <p> <value-in-range> .` |
| Cardinality (min) | `<s> <p> <new-value> .` |
| Cardinality (max) | `# Delete: <s> <p> <excess-value> .` |
| Missing property | `<s> <p> <new-value> .` |
| Closed shape | `# Delete: <s> <unexpected-pred> <obj> .` |
