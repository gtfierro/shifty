# SHACL Shape Optimization Benchmarking Guide

This document describes the benchmarking infrastructure for measuring and analyzing the impact of SHACL shape optimizations in Shifty.

## Overview

The benchmarking infrastructure consists of three main components:

1. **Enhanced Trace Events** (`lib/src/trace.rs`) - Fine-grained instrumentation for profiling
2. **Benchmark Runner** (`run_benchmarks.py`) - Automated benchmark execution with trace collection
3. **Comparison Tool** (`benchmark/compare_benchmarks.py`) - Statistical analysis and regression detection

## Trace Events

The following trace events are emitted during validation:

### Target Collection Events
- `TargetCollectionStart(SourceShape, Instant)` - Emitted when target collection begins for a shape
- `TargetCollectionEnd(SourceShape, target_count, Instant)` - Emitted when target collection completes
- `TargetCacheHit(SourceShape, cached_count)` - Emitted when targets are retrieved from cache

### Component Execution Events
- `ComponentExecutionStart(ComponentID, SourceShape, Instant)` - Emitted before component validation
- `ComponentExecutionEnd(ComponentID, SourceShape, Instant)` - Emitted after component validation
- `ComponentCacheHit(ComponentID, SourceShape)` - Emitted when component result is retrieved from cache

## Running Benchmarks

### Basic Usage

Run benchmarks on all S223 models with default settings (3 runs per model):

```bash
./run_benchmarks.py
```

### Baseline Mode

For establishing baseline metrics before optimization work:

```bash
./run_benchmarks.py --baseline-mode \
  --output-csv benchmark/baseline-main.csv
```

Baseline mode automatically:
- Sets runs to 10 for statistical stability
- Enables trace statistics collection
- Outputs extended CSV with phase timings

### Custom Configuration

```bash
./run_benchmarks.py \
  --runs 5 \
  --models-glob "benchmark/s223/models/*.ttl" \
  --shapes-file "ttl/223p.ttl" \
  --output-csv benchmark/my-results.csv \
  --plot-output benchmark/my-plot.png \
  --trace-stats
```

### Trace Statistics

Enable detailed trace statistics to analyze target collection and component execution times:

```bash
./run_benchmarks.py --trace-stats \
  --output-csv benchmark/with-traces.csv
```

This adds the following columns to the CSV output:
- `target_collection_seconds` - Total time spent collecting targets
- `component_execution_seconds` - Total time spent executing components

## Comparing Benchmarks

### Basic Comparison

Compare two benchmark runs to measure optimization impact:

```bash
./benchmark/compare_benchmarks.py \
  benchmark/baseline-main.csv \
  benchmark/opt1-target-dedup.csv \
  --output-html benchmark/opt1-comparison.html
```

### Statistical Analysis

The comparison tool provides:

- **Mean/Median/P95 metrics** - Wall time distribution analysis
- **Statistical significance** - T-test with configurable threshold (default: p < 0.05)
- **Regression detection** - Flags slowdowns exceeding threshold (default: >5%)
- **Phase breakdown** - Per-phase timing comparisons when available

### Example Output

```
================================================================================
BENCHMARK COMPARISON REPORT
================================================================================

Platform: shifty
--------------------------------------------------------------------------------
  Runs: 190 (baseline) vs 190 (optimized)

  Wall Time:
    Mean:   2.456 s → 1.823 s (-25.8%)
    p50:    2.301 s → 1.752 s
    p95:    4.812 s → 3.621 s
    p-value: 0.0001 (significant)

  Phase Breakdown:
    Target Collection:
      Mean: 312.4 ms → 98.7 ms (-68.4%)
    Component Execution:
      Mean: 1.842 s → 1.523 s (-17.3%)
```

### Regression Detection

The tool exits with code 1 if any regressions are detected, making it suitable for CI/CD:

```bash
if ! ./benchmark/compare_benchmarks.py baseline.csv new.csv; then
  echo "Performance regression detected!"
  exit 1
fi
```

## Benchmark Data

### S223 Benchmark Suite

The default benchmark suite uses S223 (ASHRAE Standard 223P) models:

- **Models**: 19 building automation system models
- **Size range**: 20 KB - 2.5 MB (5K - 60K triples)
- **Shapes**: 223p.ttl (full ASHRAE 223P ontology)
- **Location**: `benchmark/s223/models/`

### Shape Variants

Test with different shape complexity levels:

- `ttl/small-223p.ttl` - Minimal shape set for quick tests
- `ttl/medium-223p.ttl` - Medium complexity for balanced testing
- `ttl/223p.ttl` - Full ASHRAE 223P shapes

## Output Files

### CSV Columns

Standard columns:
- `platform` - Validator platform (shifty, shifty-pre, pyshacl, etc.)
- `data_file` - Model file path
- `triples` - Number of triples in the data graph
- `run` - Run number (1 to N)
- `seconds` - Total wall time
- `timed_out` - Boolean indicating timeout
- `failed` - Boolean indicating failure

Phase timing columns (when `--benchmark-json` is available):
- `graph_fetching_seconds` - Time loading graphs
- `validate_seconds` - Time running validation
- `inference_seconds` - Time running inference
- `report_assembly_seconds` - Time assembling validation report

Trace statistics columns (when `--trace-stats` is enabled):
- `target_collection_seconds` - Time collecting targets
- `component_execution_seconds` - Time executing components

### Plot Output

The runner generates scatter plots showing:
- X-axis: Data graph size (triples)
- Y-axis: Mean runtime (seconds)
- Series: One per platform
- Error bars: Standard deviation across runs

## Optimization Workflow

### Phase 0: Baseline

1. Build release binary: `cargo build --release`
2. Run baseline benchmarks:
   ```bash
   ./run_benchmarks.py --baseline-mode \
     --output-csv benchmark/baseline-main.csv
   ```
3. Review results for hotspots

### Phase 1-4: Implement Optimizations

For each optimization phase:

1. Implement the optimization in a feature branch
2. Run benchmarks with same configuration:
   ```bash
   ./run_benchmarks.py --baseline-mode \
     --output-csv benchmark/opt${N}-${name}.csv
   ```
3. Compare against baseline:
   ```bash
   ./benchmark/compare_benchmarks.py \
     benchmark/baseline-main.csv \
     benchmark/opt${N}-${name}.csv \
     --output-html benchmark/opt${N}-comparison.html
   ```
4. Verify no regressions and expected improvements

### Phase 5: Integration

1. Merge all optimization branches
2. Run cumulative benchmark:
   ```bash
   ./run_benchmarks.py --runs 20 \
     --output-csv benchmark/opt-all-integrated.csv
   ```
3. Generate final comparison:
   ```bash
   ./benchmark/compare_benchmarks.py \
     benchmark/baseline-main.csv \
     benchmark/opt-all-integrated.csv \
     --output-html benchmark/final-comparison.html \
     --include-all-phases
   ```

## Performance Targets

Based on the optimization plan, expected improvements:

- **Phase 1 (Target Deduplication)**: 15-30% reduction in target_collection_s
- **Phase 2 (Cost-Based Scheduling)**: 10-20% wall time reduction
- **Phase 3 (Component Memoization)**: 20-40% reduction in component_execution_s
- **Phase 4 (Path Batching)**: 25-50% reduction in SPARQL queries, 15-30% wall time
- **Cumulative (All Phases)**: 40-75% overall wall time reduction

## CI/CD Integration

Example GitHub Actions workflow:

```yaml
name: Benchmark Regression Test
on: [pull_request]
jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run benchmarks
        run: |
          ./run_benchmarks.py --runs 5 \
            --output-csv benchmark/pr-results.csv
      - name: Compare against main
        run: |
          ./benchmark/compare_benchmarks.py \
            benchmark/baseline-main.csv \
            benchmark/pr-results.csv \
            --output-html benchmark/pr-comparison.html
      - name: Upload comparison report
        uses: actions/upload-artifact@v4
        with:
          name: benchmark-comparison
          path: benchmark/pr-comparison.html
```

## Troubleshooting

### Missing Trace Statistics

If `target_collection_seconds` and `component_execution_seconds` are null:
- Ensure `--trace-stats` flag is enabled
- Verify shifty supports `--trace-jsonl` flag
- Check trace file is created in /tmp during run

### High Variance

If standard deviation is >20% of mean:
- Increase `--runs` (try 10-20)
- Close background applications
- Use dedicated benchmark machine
- Consider CPU frequency scaling settings

### Timeouts

If benchmarks timeout:
- Increase `--timeout-seconds` (default: none)
- Check for memory issues (large graphs)
- Verify models are well-formed

## Reference

- **Optimization Plan**: See plan document for detailed optimization strategy
- **Trace Events**: `lib/src/trace.rs`
- **Benchmark Runner**: `run_benchmarks.py`
- **Comparison Tool**: `benchmark/compare_benchmarks.py`
- **S223 Ontology**: https://open223.info/
