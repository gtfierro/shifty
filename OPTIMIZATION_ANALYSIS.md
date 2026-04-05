# Optimization Performance Analysis

## Summary

Performance optimizations show dramatically different results across ontology types:
- **S223**: 99%+ improvement (target deduplication is critical)
- **Brick**: 56% regression (optimization overhead > benefits)

This demonstrates the need for **adaptive, data-driven optimization** that detects workload characteristics.

## Benchmark Results

### S223 (ASHRAE 223P)

**Characteristics**:
- Large data files (20KB - 2.5MB)
- ~1,698 target references → ~250 unique targets (85% sharing)
- Few shapes with many instances each
- High target expression reuse

**Results** (design-patterns.ttl, 442 triples):

| Platform | Before | After | Change |
|----------|--------|-------|--------|
| shifty-pre | 5.42s | - | baseline |
| shifty | - | 0.13s | **97.6% faster** |

**Larger models**:
- guideline36-2021-A-1.ttl: 22.13s → 0.23s (97x speedup)
- nist-bdg1-1.ttl: 622.50s → 1.30s (479x speedup)

**Key**: Target deduplication eliminated massive redundancy.

### Brick Schema

**Characteristics**:
- Small data file (529 triples)
- Large shape graph (Brick.ttl = 2.0MB, many small shapes)
- Low target sharing (many unique shapes)
- Different optimization trade-offs

**Results** (bldg1.ttl, 529 triples):

| Platform | Time (avg) | vs Baseline |
|----------|------------|-------------|
| shifty-pre | 25.1s | baseline |
| shifty | 39.3s | **56% SLOWER** |
| pyshacl | 238s | 9.5x slower than shifty-pre |

**Breakdown by run**:
```
shifty-pre: 21.4s, 26.7s, 27.2s (mean: 25.1s)
shifty:     31.1s, 42.2s, 44.5s (mean: 39.3s)
```

**Regression**: +14.2 seconds per validation

## Root Cause Analysis

### Why S223 Benefits Dramatically

1. **Target Deduplication**:
   - Before: ~1,698 independent target evaluations
   - After: ~250 unique evaluations (85% cache hits)
   - Each cache hit saves 100-500ms
   - Total savings: 100-500 seconds!

2. **Compounding Effect**:
   - High target count amplifies savings
   - Target queries are expensive (SPARQL)
   - Large data → expensive queries

3. **Path Batching**:
   - Some path sharing across property shapes
   - Batching reduces query count

### Why Brick Regresses

1. **Low Target Sharing**:
   - Many small shapes → many unique targets
   - Cache hit rate likely <20%
   - Cache miss overhead > savings

2. **Overhead Sources**:

   **a) RwLock Contention**:
   ```rust
   // Many shapes competing for same locks
   context.global_target_cache.read()   // Lock acquisition
   context.path_batch_cache.read()      // Lock acquisition
   context.component_memo_cache.read()  // Lock acquisition
   ```
   - With many small shapes executing in parallel
   - Lock contention increases linearly
   - Each lock acquisition: ~1-10μs × thousands of shapes

   **b) Hash Computation**:
   ```rust
   let target_hash = hash_target(target);  // Hash computation overhead
   ```
   - Cost: ~100-500ns per hash
   - With 1000+ unique targets: ~0.5ms total
   - Not much, but adds up with cache misses

   **c) Cache Miss Penalty**:
   ```rust
   if let Some(cached) = cache.read().unwrap().get(&key) {
       // Cache hit - fast
   } else {
       // Cache miss - still paid for:
       // 1. Lock acquisition
       // 2. Hash computation
       // 3. HashMap lookup
       // 4. Lock release
       // Then execute original logic anyway
   }
   ```
   - When hit rate is low, overhead is pure cost

   **d) Memory Allocation**:
   ```rust
   let arc_nodes: Arc<[Term]> = nodes.into();
   cache.write().unwrap().insert(hash, Arc::clone(&arc_nodes));
   ```
   - Arc allocations for each unique target
   - HashMap growth and rehashing
   - More memory pressure → worse cache locality

3. **Small Data = Fast Base Case**:
   - 529 triples → queries are already fast
   - Optimization overhead is significant % of total time
   - Diminishing returns

4. **Many Small Shapes**:
   - Parallelism is fine-grained
   - Lock contention worse with many small tasks
   - Better to have few large tasks

## Workload Classification

### High-Sharing Workload (S223-like)

**Indicators**:
- `shared_target_refs / total_target_refs > 0.5` (>50% sharing)
- `total_target_refs > 1000` (many references)
- `avg_target_count > 100` (large cardinality)

**Best Strategies**:
- ✅ Target deduplication (critical)
- ✅ Path batching (moderate benefit)
- ✅ Component memoization (if constraints shared)

### Low-Sharing Workload (Brick-like)

**Indicators**:
- `shared_target_refs / total_target_refs < 0.2` (<20% sharing)
- `shape_count > 500` (many shapes)
- `avg_target_count < 10` (small cardinality)

**Best Strategies**:
- ❌ Target deduplication (overhead > benefit)
- ❌ Path batching (overhead > benefit)
- ✅ Focus-predicate summary (already exists)
- ✅ Better parallelization strategy

### Mixed Workload

**Indicators**:
- Moderate sharing (20-50%)
- Variable shape sizes

**Best Strategies**:
- ✅ Selective caching (only for high-reuse targets)
- ✅ Threshold-based optimization
- ✅ Per-shape cost estimation

## Adaptive Optimization Strategy

### Phase 1: Workload Detection (Static Analysis)

Before validation, analyze the compiled program:

```rust
pub struct WorkloadProfile {
    pub total_shapes: u64,
    pub total_target_refs: u64,
    pub unique_target_ids: u64,
    pub sharing_ratio: f64,  // shared / total
    pub avg_components_per_shape: f64,
    pub shape_size_variance: f64,
}

impl WorkloadProfile {
    pub fn classify(&self) -> WorkloadType {
        if self.sharing_ratio > 0.5 && self.total_target_refs > 1000 {
            WorkloadType::HighSharing
        } else if self.total_shapes > 500 && self.sharing_ratio < 0.2 {
            WorkloadType::LowSharing
        } else {
            WorkloadType::Mixed
        }
    }
}
```

### Phase 2: Selective Optimization

Enable/disable optimizations based on workload:

```rust
pub struct OptimizationConfig {
    pub enable_target_cache: bool,
    pub enable_path_cache: bool,
    pub enable_component_memo: bool,
    pub cache_threshold: usize,  // Only cache if reuse_count > threshold
}

impl OptimizationConfig {
    pub fn from_profile(profile: &WorkloadProfile) -> Self {
        match profile.classify() {
            WorkloadType::HighSharing => Self {
                enable_target_cache: true,
                enable_path_cache: true,
                enable_component_memo: true,
                cache_threshold: 1,  // Cache everything
            },
            WorkloadType::LowSharing => Self {
                enable_target_cache: false,  // Disable - overhead > benefit
                enable_path_cache: false,
                enable_component_memo: false,
                cache_threshold: usize::MAX,  // Never cache
            },
            WorkloadType::Mixed => Self {
                enable_target_cache: true,
                enable_path_cache: true,
                enable_component_memo: true,
                cache_threshold: 5,  // Only cache if used 5+ times
            },
        }
    }
}
```

### Phase 3: Runtime Adaptation

Monitor cache hit rates during execution:

```rust
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        if hits + misses == 0 { return 0.0; }
        hits as f64 / (hits + misses) as f64
    }

    pub fn should_disable(&self) -> bool {
        // If hit rate < 10% and we've seen 100+ accesses, disable
        self.hit_rate() < 0.1 && (self.hits.load(Ordering::Relaxed) +
                                   self.misses.load(Ordering::Relaxed)) > 100
    }
}
```

Dynamically disable caches if hit rate is too low.

## Data-Driven Cost Model Integration

### Step 1: Collect Graph Statistics

```rust
pub struct GraphStats {
    pub class_instance_counts: HashMap<NamedNode, u64>,
    pub predicate_stats: HashMap<NamedNode, PredicateStats>,
}

pub struct PredicateStats {
    pub triple_count: u64,
    pub distinct_subjects: u64,
    pub distinct_objects: u64,
    pub avg_fanout: f64,
}
```

### Step 2: Estimate Optimization Benefit

For each optimization, estimate:
- **Potential savings**: If hit rate is X%, save Y seconds
- **Overhead cost**: Lock contention, hash computation, memory

Example for target deduplication:
```rust
let sharing_ratio = profile.shared_target_refs as f64 / profile.total_target_refs as f64;
let estimated_savings = sharing_ratio × avg_target_query_cost × total_targets;
let estimated_overhead = cache_overhead_per_access × total_targets;
let net_benefit = estimated_savings - estimated_overhead;

if net_benefit > 0.0 {
    enable_target_cache = true;
}
```

### Step 3: Dynamic Threshold Adjustment

```rust
// Only cache targets used more than N times
// Adjust N based on observed overhead

let cache_threshold = if profile.classify() == WorkloadType::HighSharing {
    1  // Cache everything
} else {
    // Calculate break-even point
    let overhead_per_cache = 10.0;  // μs
    let benefit_per_hit = 500.0;     // μs (typical query cost)
    (overhead_per_cache / benefit_per_hit).ceil() as usize
};
```

## Implementation Roadmap

### Phase 1: Workload Detection (Week 1)

- [x] Target sharing analyzer (already exists)
- [x] Path sharing analyzer (already exists)
- [ ] Workload classifier
- [ ] OptimizationConfig structure

### Phase 2: Conditional Caching (Week 2)

- [ ] Add config flags to ValidationContext
- [ ] Wrap cache accesses with config checks
- [ ] Benchmark S223 and Brick with selective caching

### Phase 3: Data-Driven Statistics (Week 3)

- [ ] GraphStats collector
- [ ] Predicate statistics
- [ ] Class instance counts
- [ ] Cost estimation based on stats

### Phase 4: Runtime Adaptation (Week 4)

- [ ] Cache hit rate monitoring
- [ ] Dynamic cache disabling
- [ ] Adaptive threshold adjustment

## Conclusion

The current optimizations are highly effective for **high-sharing workloads** (S223) but counterproductive for **low-sharing workloads** (Brick). This demonstrates that:

1. **No single optimization strategy fits all workloads**
2. **Overhead matters** - optimizations have real cost
3. **Adaptive optimization** is essential
4. **Data-driven decisions** (like DB query optimizers) are the right approach

Next steps:
1. Implement workload classification
2. Make caching conditional based on workload type
3. Collect graph statistics for cost estimation
4. Validate that adaptive strategy helps both S223 and Brick
