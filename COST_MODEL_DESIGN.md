# Data-Driven Cost Model Design

## Motivation

The current cost model uses fixed component costs (e.g., SPARQL=100, Pattern=5) which don't reflect actual execution characteristics. Like relational database query optimizers, we should estimate costs based on **statistics from the actual RDF graph**.

## Inspiration from Relational DBMSs

### Relational Query Optimization

Relational databases use statistics to estimate query costs:

1. **Cardinality Statistics**:
   - Table row counts
   - Index selectivity
   - Distribution histograms

2. **Selectivity Estimation**:
   - Predicate selectivity (e.g., WHERE age > 30)
   - Join selectivity
   - Filter cardinality reduction

3. **Cost Formulas**:
   ```
   Cost = CPU_cost + IO_cost
   CPU_cost = rows × CPU_per_row
   IO_cost = blocks × IO_per_block
   ```

### Translating to SHACL/RDF

| Relational Concept | SHACL/RDF Equivalent |
|-------------------|----------------------|
| Table row count | Class instance count (`?x a ex:Class`) |
| Index selectivity | Predicate cardinality (`?s ex:prop ?o`) |
| Join cardinality | Path navigation (`?s ex:path1/ex:path2 ?o`) |
| Predicate selectivity | Constraint selectivity (e.g., pattern match rate) |

## Proposed Statistics

### 1. Class Statistics

For each `sh:targetClass` and `sh:class` constraint:

```sparql
SELECT (COUNT(?x) as ?count) WHERE {
    ?x a ex:Class .
}
```

**Storage**:
```rust
struct ClassStats {
    class_iri: NamedNode,
    instance_count: u64,
}
```

**Use Case**: Estimate target cardinality for `sh:targetClass ex:Equipment`

### 2. Predicate Statistics

For each property in the data graph:

```sparql
SELECT ?predicate
       (COUNT(*) as ?total_triples)
       (COUNT(DISTINCT ?subject) as ?distinct_subjects)
       (COUNT(DISTINCT ?object) as ?distinct_objects)
WHERE {
    ?subject ?predicate ?object .
}
GROUP BY ?predicate
```

**Storage**:
```rust
struct PredicateStats {
    predicate: NamedNode,
    triple_count: u64,           // Total triples with this predicate
    distinct_subjects: u64,       // How many unique subjects
    distinct_objects: u64,        // How many unique objects
    avg_fanout: f64,             // triple_count / distinct_subjects
}
```

**Derived Metrics**:
- **Fanout**: Average objects per subject (e.g., `ex:hasProperty` might have fanout=3)
- **Inverse Fanout**: Average subjects per object
- **Selectivity**: `distinct_subjects / total_subjects` (if we know total subjects)

**Use Case**:
- Estimate path navigation cost (`sh:path ex:hasProperty`)
- Estimate property pair constraint cost (`sh:equals`, `sh:disjoint`)

### 3. Path Statistics

For complex paths (sequences, inverse, etc.), estimate cost recursively:

**Simple Predicate Path**:
```rust
fn estimate_path_cost(path: &Path, stats: &GraphStats) -> u64 {
    match path {
        Path::Simple(predicate) => {
            let pred_stats = stats.get_predicate(predicate);
            pred_stats.triple_count
        }
        Path::Inverse(inner) => {
            // Same cost but inverse fanout matters
            estimate_path_cost(inner, stats)
        }
        Path::Sequence(paths) => {
            // Multiplicative: fanout compounds
            paths.iter().map(|p| estimate_path_cost(p, stats)).product()
        }
        Path::Alternative(paths) => {
            // Additive: sum of all branches
            paths.iter().map(|p| estimate_path_cost(p, stats)).sum()
        }
        // ... other path types
    }
}
```

### 4. Constraint Selectivity

Estimate how many nodes pass/fail each constraint type:

**Type Constraints** (Class, Datatype, NodeKind):
- Already covered by class statistics
- Datatype: requires sampling or heuristics

**Cardinality Constraints** (MinCount, MaxCount):
- Use predicate fanout statistics
- Example: `sh:path ex:hasProperty ; sh:minCount 2`
  - If avg fanout = 3, most nodes pass
  - If avg fanout = 0.5, most nodes fail

**String Constraints** (Pattern, MinLength):
- Requires sampling or fixed selectivity heuristics
- Likely expensive regardless (regex evaluation)

**SPARQL Constraints**:
- Execute on sample data to estimate selectivity
- Or use conservative high cost (assume expensive)

## Statistics Collection

### Lazy Collection (Preferred)

Collect statistics on-demand during first validation run:

```rust
pub struct GraphStatsCollector {
    cache: RwLock<HashMap<StatKey, StatValue>>,
}

impl GraphStatsCollector {
    pub fn get_class_count(&self, class: &NamedNode, store: &Store) -> u64 {
        let cache_key = StatKey::ClassCount(class.clone());

        if let Some(cached) = self.cache.read().unwrap().get(&cache_key) {
            return cached.as_count();
        }

        // Execute counting query
        let count = execute_count_query(store, class);

        self.cache.write().unwrap().insert(cache_key, StatValue::Count(count));
        count
    }
}
```

**Pros**:
- No upfront cost
- Only collect needed statistics
- Adapt to actual query patterns

**Cons**:
- First run slower (cold cache)
- Cache invalidation needed if data changes

### Eager Collection (Alternative)

Collect all statistics upfront before validation:

```rust
pub fn collect_graph_stats(store: &Store, data_graph: GraphNameRef) -> GraphStats {
    let mut stats = GraphStats::default();

    // Collect predicate statistics
    let predicates = find_all_predicates(store, data_graph);
    for predicate in predicates {
        stats.predicate_stats.insert(predicate.clone(), collect_predicate_stats(store, &predicate));
    }

    // Collect class statistics
    let classes = find_all_classes(store, data_graph);
    for class in classes {
        stats.class_stats.insert(class.clone(), collect_class_stats(store, &class));
    }

    stats
}
```

**Pros**:
- Predictable upfront cost
- All statistics available immediately
- Can optimize collection queries (single scan)

**Cons**:
- Wasted work for unused statistics
- Upfront latency

## Cost Formulas (Revised)

### Target Collection Cost

```rust
fn estimate_target_cost(target: &Target, stats: &GraphStats) -> u64 {
    match target {
        Target::Class(class) => {
            // Direct lookup: how many instances?
            stats.get_class_count(class)
        }
        Target::Node(_) => {
            // Single node: constant cost
            1
        }
        Target::SubjectsOf(predicate) => {
            // How many distinct subjects for this predicate?
            stats.get_predicate_stats(predicate).distinct_subjects
        }
        Target::ObjectsOf(predicate) => {
            // How many distinct objects for this predicate?
            stats.get_predicate_stats(predicate).distinct_objects
        }
        // ...
    }
}
```

### Component Validation Cost

```rust
fn estimate_component_cost(
    component: &ComponentDescriptor,
    focus_count: u64,
    stats: &GraphStats,
) -> u64 {
    match component {
        ComponentDescriptor::Class { class } => {
            // Type check: O(1) per focus node
            focus_count * 1
        }
        ComponentDescriptor::MinCount { min_count } => {
            // Cardinality check: already have count from path navigation
            // Essentially free if we have focus-predicate summary
            focus_count * 1
        }
        ComponentDescriptor::Sparql { .. } => {
            // SPARQL execution: expensive, conservative estimate
            // Could sample to get actual cost
            focus_count * 100
        }
        ComponentDescriptor::Node { shape } => {
            // Recursive validation: depends on target shape
            // Need recursive cost estimation
            let nested_cost = estimate_shape_cost(shape, stats);
            focus_count * nested_cost
        }
        ComponentDescriptor::Property { shape } => {
            // Property shape: path navigation + nested validation
            let path_cost = estimate_path_cost(&shape.path, stats);
            let nested_cost = estimate_constraints_cost(&shape.constraints, stats);
            focus_count * (path_cost + nested_cost)
        }
        // ... other components
    }
}
```

### Shape Total Cost

```rust
fn estimate_shape_cost(shape: &ShapeRow, stats: &GraphStats) -> ShapeCost {
    // Estimate target count
    let target_count = shape.target_ids.iter()
        .map(|&id| {
            let target = stats.get_target(id);
            estimate_target_cost(target, stats)
        })
        .sum();

    // Estimate component complexity
    let component_cost = shape.component_ids.iter()
        .map(|&id| {
            let component = stats.get_component(id);
            estimate_component_cost(component, 1, stats) // Per-focus cost
        })
        .sum();

    ShapeCost::new(shape.id, target_count, component_cost)
}
```

## Implementation Plan

### Phase 1: Statistics Collection Framework

1. Define `GraphStats` structure
2. Implement lazy collectors for class and predicate statistics
3. Add caching layer with RwLock
4. Unit tests for statistics collection

### Phase 2: Cost Estimation Integration

1. Refactor `CostEstimationAnalyzer` to use `GraphStats`
2. Replace fixed component costs with data-driven estimates
3. Add path cost estimation
4. Benchmark improvements

### Phase 3: Advanced Features

1. **Sampling**: For expensive statistics, sample data instead of full scan
2. **Incremental Updates**: Invalidate cache when data changes
3. **Histogram Statistics**: Track value distribution for better selectivity
4. **Query Plan Visualization**: Show estimated vs actual costs

## Example: Cost Estimation Walkthrough

**Shape**: `sh:targetClass ex:Equipment ; sh:property [ sh:path ex:hasProperty ; sh:class ex:Property ]`

### Step 1: Estimate Target Count
```rust
let target_cost = stats.get_class_count("ex:Equipment");
// Suppose: 1000 instances
```

### Step 2: Estimate Component Costs

**Property Shape Component**:
```rust
// Path navigation: ex:hasProperty
let path_stats = stats.get_predicate_stats("ex:hasProperty");
let path_fanout = path_stats.avg_fanout; // Suppose: 3.5

// Class constraint on values
let class_check_cost = 1; // O(1) type check

let component_cost = path_fanout * class_check_cost;
// = 3.5 (navigate) × 1 (check) = 3.5
```

### Step 3: Total Shape Cost
```rust
let shape_cost = ShapeCost::new(
    shape.id,
    1000,                // target_count
    (3.5 + 1) as u64,   // component_complexity (path + check)
);
// total_cost = 1000 × 4 = 4000
```

## Benefits

1. **Accuracy**: Costs reflect actual data characteristics
2. **Adaptability**: Different datasets have different costs
3. **Optimization Opportunities**:
   - Identify expensive shapes early
   - Better parallelization decisions
   - Cache high-value intermediate results
4. **Debugging**: Explain why validation is slow

## Challenges

1. **Statistics Overhead**: Collecting stats has upfront cost
2. **Cache Invalidation**: Data updates require stat refresh
3. **Recursive Shapes**: Circular dependencies complicate cost estimation
4. **SPARQL Costs**: Hard to estimate without execution

## Future Extensions

1. **Adaptive Statistics**: Learn from execution traces
2. **Histogram-Based Selectivity**: Track value distributions
3. **Join Cardinality**: Estimate multi-path navigation costs
4. **Cost-Based Caching**: Cache based on reuse probability × computation cost
