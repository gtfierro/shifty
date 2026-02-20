# Optimization TODO

## In Progress
- No active implementation task.

## Pending
- [ ] Make trace collection opt-in (default to no-op sink unless tracing is requested).

## Completed
- [x] Re-enable compiler cache wiring (`sccache` in Python/release workflows and `CCACHE_*` setup in `scripts/test_compiled_shacl.sh`).
- [x] Switch compiled-manifest generated crate back to static RocksDB linkage (`oxigraph` without `rocksdb-pkg-config`) to avoid requiring `rocksdb.pc` on CI.
- [x] Mirror key runtime optimizations in `shacl-compiler` generated helpers (advanced target select scan + canonicalization allocation path).
- [x] Reduce repeated graph scans in advanced target select resolution and closed-world predicate classification.
- [x] Baseline profiling and hotspot identification on the core SHACL library.
- [x] Move `PropertyShape` constraint ordering out of the per-focus loop and remove obvious avoidable clones.
- [x] Add memoization for repeated `(shape_id, focus_node)` conformance checks.
- [x] Parameterize repeated per-focus SPARQL query strings to improve prepared-query cache hits.
- [x] Add a native fast-path comparator for common numeric literal comparisons in:
  - `value_range` constraints (`min/max inclusive/exclusive`)
  - `property_pair` constraints (`lessThan`, `lessThanOrEquals`)
- [x] Reduce heavy `Context` cloning in parallel value-node checks.
- [x] Reduce vector cloning in target caches and value canonicalization paths.
- [x] Run tests after the first implementation step (`cargo test -p shifty-shacl`).
- [x] Run tests after the second implementation step (`cargo test -p shifty-shacl`).
- [x] Run tests after reducing context cloning (`cargo test -p shifty-shacl`).
