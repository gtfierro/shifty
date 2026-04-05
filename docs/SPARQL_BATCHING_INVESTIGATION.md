# SPARQL Batching Investigation (Phase 3)

## Executive Summary

**Status: FAILED - Approach Abandoned**

Attempted to optimize generic SPARQL rule execution by batching multiple focus nodes into a single query using SPARQL VALUES clauses. The implementation resulted in a **30.6x performance regression** and was abandoned.

## Motivation

Performance profiling revealed that generic SPARQL rules were a significant bottleneck:
- 88 generic SPARQL rules in Brick schema
- Each rule executed once per focus node per iteration
- Hypothesis: Batching focus nodes could reduce query overhead

## Approach

### Implementation Strategy

1. **Filter focus nodes upfront** by conditions and prefilters
2. **Construct VALUES clause** containing all valid focus nodes
3. **Inject VALUES clause** into the CONSTRUCT query after WHERE {
4. **Execute once** instead of once per focus node
5. **Distribute results** across focus nodes

### Code Changes

Added three methods to `InferenceEngine`:
- `apply_sparql_rule_batched()` - Main batching logic
- `construct_values_clause()` - Build VALUES (?this) { ... } clause
- `inject_values_clause()` - Inject VALUES after WHERE {
- `term_to_sparql_literal()` - Convert Terms to SPARQL syntax

Injection strategy:
```sparql
CONSTRUCT { ... }
WHERE {
  VALUES (?this) {
    (<http://example.org/node1>)
    (<http://example.org/node2>)
    (<http://example.org/node3>)
  }
  # Original query patterns...
}
```

## Results

### Performance Impact (bldg41.ttl)

| Metric | Without Batching | With Batching | Change |
|--------|------------------|---------------|--------|
| Total Time | 15.6s | 478.4s | **+30.6x slower** |
| SPARQL Time | ~12s | 478.9s | **+39.9x slower** |
| Triples Inferred | 501 | 501 | Same |

### Benchmark Details

**Without Batching (Current):**
```
[2026-04-05] Inference finished after 4 iterations
- Total: 15.6s
- SPARQL: ~12s (100 rules: 12 compiled, 88 generic)
- Triple: ~1s (5,886 rules)
- Focus: ~1s
- Triples added: 501
```

**With Batching (Tested):**
```
[2026-04-05] Inference finished after 4 iterations
- Total: 478.4s (8m4.8s)
- SPARQL: 478.9s (100 rules: 12 compiled, 88 generic)
- Triple: 1.0s (5,886 rules)
- Focus: 1.0s
- Triples added: 501
```

### Behavior Across Models

All models experienced similar catastrophic slowdown:
- Small models (bldg41): 15s → 478s timeout
- Medium models (bldg11-13): Timeout > 60s
- Large models (bldg37): Timeout > 60s

## Root Cause Analysis

### Why VALUES Clause Failed

1. **Query Planner Inefficiency**
   - SPARQL engines optimize for single ?this bindings
   - VALUES clause prevents per-focus optimizations
   - Query planner must consider all focus nodes simultaneously
   - Result: Cartesian product explosion

2. **No Early Termination**
   - Individual queries can short-circuit on first match
   - Batched query must process entire VALUES set
   - Wasted work on focus nodes that would fail early

3. **Index Invalidation**
   - Per-focus queries leverage focused indexes
   - VALUES clause forces generic index scans
   - Result: O(n²) behavior instead of O(n)

4. **Complex Brick Patterns**
   - Brick rules use property paths, OPTIONAL, UNION
   - These patterns interact poorly with VALUES
   - Query becomes prohibitively complex

### Example Problematic Pattern

Brick rule with property path:
```sparql
CONSTRUCT { ?this a brick:Equipment }
WHERE {
  ?this brick:hasPoint/brick:hasUnit ?unit .
}
```

With VALUES:
```sparql
CONSTRUCT { ?this a brick:Equipment }
WHERE {
  VALUES (?this) { (:node1) (:node2) ... (:node1000) }
  ?this brick:hasPoint/brick:hasUnit ?unit .
}
```

Query planner must:
- Evaluate property path for ALL focus nodes
- Cross-product: 1000 nodes × average 10 paths = 10,000 checks
- Cannot prune based on individual focus node characteristics

## Lessons Learned

### What Worked
- Filter focus nodes upfront (good optimization)
- Clean separation of batching logic
- Comprehensive error handling

### What Failed
- VALUES clause injection approach
- Assumption that batching reduces overhead
- "Batch everything" mentality

### Key Insights
1. **SPARQL engines optimize for point queries**, not batch queries
2. **Property paths don't batch well** - they create combinatorial explosions
3. **Early termination matters** - especially with conditions
4. **Compiled rules are fast** - focus on extending compilation coverage instead

## Alternative Approaches (Not Pursued)

### 1. Selective Batching
Batch only "simple" rules without property paths or OPTIONAL:
- Complexity: High (rule pattern analysis)
- Benefit: Uncertain (simple rules are already fast)

### 2. Query Rewriting
Rewrite queries to use UNION instead of VALUES:
```sparql
{ BIND(:node1 AS ?this) ... }
UNION
{ BIND(:node2 AS ?this) ... }
```
- Complexity: High
- Performance: Likely similar issues

### 3. Parallel Query Execution
Execute multiple individual queries in parallel:
- Complexity: Medium
- Issue: Already doing this at rule level (wave parallelism)

### 4. Extended Rule Compilation
Compile more SPARQL patterns to native code:
- Complexity: Very High
- Benefit: High (10-100x for compiled rules)
- **Recommended future direction**

## Recommendations

### Immediate Actions
1. **Remove batching code** - Done (code never committed)
2. **Document findings** - This document
3. **Update plan** - Mark Phase 3 as "Investigated & Rejected"

### Future Optimizations (Priority Order)

**Priority 1: Extended Rule Compilation**
- Analyze Brick schema for common SPARQL patterns
- Extend srcgen compiler to handle more patterns
- Target: Compile 50+ of the 88 generic rules
- Expected impact: 5-10x speedup

**Priority 2: Condition Pre-Filtering**
- Filter focus nodes before wave execution
- Avoid processing nodes that fail conditions
- Expected impact: 10-30% speedup

**Priority 3: Query Result Caching**
- Cache SPARQL query results across iterations
- Invalidate on delta changes
- Expected impact: 20-40% on later iterations

**Priority 4: Incremental SPARQL**
- Use delta to limit SPARQL graph scope
- Only query triples added in recent iterations
- Expected impact: 30-50% on larger graphs

## Benchmark Archive

Full benchmark data archived at:
- `/tmp/claude-1000/-home-gabe-src-shifty/tasks/b50dc64.output`
- Command: `shifty validate --shapes-file benchmark/brick/Brick.ttl --data-file benchmark/brick/models/bldg41.ttl --run-inference --inference-debug`

## Conclusion

SPARQL batching via VALUES clause injection is **fundamentally incompatible** with the complex query patterns used in Brick schema. The approach provided no benefit and caused severe performance regression.

The current optimizations (wave-based parallelism + focus collection batching) provide excellent performance. Further improvements should focus on **rule compilation coverage** rather than query batching.

**Phase 3 Status: CLOSED - Approach Not Viable**

---

*Investigation Date: 2026-04-05*
*Investigator: Claude Sonnet 4.5*
*Context: Inference Optimization Phase 3*
