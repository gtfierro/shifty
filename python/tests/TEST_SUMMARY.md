# Test Summary for pyshifty

## Test Coverage Overview

**Total Tests**: 132 tests  
**Test Files**: 5 files  
**Code Coverage**: 100% of `shifty/__init__.py`

## Test Files

### 1. `test_errors.py` (33 tests)
Error handling tests covering:
- Invalid graph_mode values
- Parse errors for invalid RDF syntax
- Unsupported input types (int, dict, list, None, etc.)
- File path errors (nonexistent files)
- Empty/None inputs handling
- Blank node handling in validation and inference
- Large input handling (100+ triples)
- Temporary file operations

### 2. `test_convert.py` (22 tests)
Unit tests for type conversion:
- `_to_turtle_bytes()` function with all input types:
  - bytes (passthrough)
  - pathlib.Path (file reading)
  - str (file path or raw Turtle text)
  - rdflib.Graph (Turtle serialization)
- Error cases for unsupported types
- Integration with validate/validate_algebra/infer

### 3. `test_results.py` (30 tests)
Unit tests for result classes:
- `InferResult` class:
  - `inferred_count` property
  - `diagnostics` property
  - `graph_ntriples` property
  - `graph()` method
  - `__repr__()` method
- `AlgebraResult` class:
  - `conforms` property
  - `violations` list
  - `Violation` objects (focus_node, shape, reasons)
  - `Reason` objects (message, path, value)
  - `__repr__()` and bool() coercion
- Multiple violations handling
- Edge cases (empty shapes, empty data, complex paths)

### 4. `test_shacl_features.py` (16 tests)
SHACL feature coverage tests:
- Datatype constraints (sh:datatype)
- Cardinality constraints (sh:minCount, sh:maxCount)
- NodeKind constraints (sh:nodeKind)
- Value set constraints (sh:in)
- Pattern constraints (sh:pattern)
- Closed shapes (sh:closed)
- Target types (sh:targetClass, sh:targetNode)
- Logical operators (sh:and)
- Nested property paths

### 5. `test_shifty.py` (31 tests)
Existing integration tests (from original test suite):
- `validate()` function (pyshacl-compatible interface):
  - Return type verification
  - W3C ValidationReport graph generation
  - Human-readable text output
  - Various input types
- `validate_algebra()` function (algebraic interface):
  - AlgebraResult structure
  - Violation objects with reasons
  - Multiple violations handling
- `infer()` function (SHACL-AF inference):
  - Inferred triple counting
  - Graph construction
- `graph_mode` parameter (union, data, union-all)

## Running Tests

```bash
# Run all tests
cd python
.venv/bin/python -m pytest tests/

# Run specific test file
.venv/bin/python -m pytest tests/test_errors.py

# Run with coverage
.venv/bin/python -m pytest tests/ --cov=shifty --cov-report=term-missing

# Run with verbose output
.venv/bin/python -m pytest tests/ -v
```

## Implementation Status

### Completed
- ✅ Type conversion tests
- ✅ Error handling tests
- ✅ Result class unit tests
- ✅ SHACL constraint tests
- ✅ Integration tests
- ✅ 100% code coverage

### Future Enhancements
- 📝 SPARQL constraint tests (sh:sparql)
- 📝 Complex SHACL-AF inference tests (recursive rules)
- 📝 Blank node inference tests
- 📝 More complex SHACL patterns (sh:or, sh:not)

## Test Structure

```
python/tests/
├── test_convert.py          # Type conversion unit tests
├── test_errors.py           # Error handling tests
├── test_results.py          # Result class unit tests
├── test_shacl_features.py   # SHACL feature coverage tests
├── test_shifty.py           # Existing integration tests
└── TEST_SUMMARY.md          # This file
```
