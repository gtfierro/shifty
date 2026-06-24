//! Opt-in execution telemetry (doc §269). Collected via a thread-local so no
//! validation API signatures change. Enable with `enable()`, consume with
//! `take()`.
//!
//! Shape-cache counters are accumulated locally by each evaluator and published
//! once when it is dropped, avoiding a thread-local operation per lookup.

use std::cell::RefCell;
use web_time::Instant;

/// Per-query performance record.
#[derive(Debug, Clone)]
pub struct QueryRecord {
    /// Stable fingerprint derived from the canonical query text (first 64 chars).
    pub fingerprint: String,
    /// Whether the native executor handled this query (always Fallback in stage 1).
    pub executor: ExecutorKind,
    /// How many times this query was invoked (one per focus node in stage 1).
    pub invocations: u64,
    /// Total wall-clock execution time across all invocations, in microseconds.
    pub total_exec_us: u64,
}

/// Per-shape or per-rule wall-clock record. One entry per distinct label
/// (shape IRI, `@N` slot id, or `rule[N]`).
#[derive(Debug, Clone)]
pub struct ShapeRecord {
    /// Shape IRI (named shapes), `@N` arena slot (blank-node shapes), or
    /// `rule[N]` (inference rules).
    pub label: String,
    /// Number of evaluation calls (one per focus node for validation, one per
    /// rule firing for inference).
    pub invocations: u64,
    /// Total wall-clock time across all invocations, in microseconds.
    pub total_us: u64,
}

/// Aggregate shape-cache telemetry for one profiling session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShapeCacheRecord {
    /// Number of per-snapshot evaluators that reported cache activity.
    pub evaluators: u64,
    /// Lookups served from an existing memo entry.
    pub hits: u64,
    /// Lookups without an existing memo entry, including recursion back-edges.
    pub misses: u64,
    /// Completed results admitted to the memo.
    pub insertions: u64,
    /// Lookups that encountered the same `(ShapeId, Term)` on the active stack.
    pub recursion_back_edges: u64,
    /// Completed results not admitted because they depended on a back-edge.
    pub non_cacheable_results: u64,
    /// Largest final entry count reported by any one evaluator.
    pub peak_entries: usize,
    /// Approximate maximum bytes retained by any one evaluator.
    ///
    /// Includes hash-table bucket storage and owned RDF-term string payloads,
    /// but not allocator metadata.
    pub estimated_peak_bytes: usize,
}

/// One evaluator's cache counters, merged into [`ShapeCacheRecord`] on drop.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ShapeCacheSample {
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub recursion_back_edges: u64,
    pub non_cacheable_results: u64,
    pub entries: usize,
    pub estimated_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorKind {
    /// Spareval fallback; carries the capability-analysis reason if known.
    Fallback {
        reason: Option<String>,
    },
    Native,
}

#[derive(Debug, Default)]
pub struct ProfileCollector {
    records: Vec<QueryRecord>,
    shape_records: Vec<ShapeRecord>,
    shape_cache: ShapeCacheRecord,
}

impl ProfileCollector {
    pub fn new() -> Self {
        ProfileCollector::default()
    }

    pub fn record_invocation(&mut self, fingerprint: &str, exec_us: u64, executor: ExecutorKind) {
        if let Some(r) = self
            .records
            .iter_mut()
            .find(|r| r.fingerprint == fingerprint)
        {
            r.invocations += 1;
            r.total_exec_us += exec_us;
        } else {
            self.records.push(QueryRecord {
                fingerprint: fingerprint.to_string(),
                executor,
                invocations: 1,
                total_exec_us: exec_us,
            });
        }
    }

    pub fn record_shape_invocation(&mut self, label: &str, exec_us: u64) {
        if let Some(r) = self.shape_records.iter_mut().find(|r| r.label == label) {
            r.invocations += 1;
            r.total_us += exec_us;
        } else {
            self.shape_records.push(ShapeRecord {
                label: label.to_string(),
                invocations: 1,
                total_us: exec_us,
            });
        }
    }

    pub(crate) fn record_shape_cache(&mut self, sample: ShapeCacheSample) {
        self.shape_cache.evaluators += 1;
        self.shape_cache.hits += sample.hits;
        self.shape_cache.misses += sample.misses;
        self.shape_cache.insertions += sample.insertions;
        self.shape_cache.recursion_back_edges += sample.recursion_back_edges;
        self.shape_cache.non_cacheable_results += sample.non_cacheable_results;
        self.shape_cache.peak_entries = self.shape_cache.peak_entries.max(sample.entries);
        self.shape_cache.estimated_peak_bytes = self
            .shape_cache
            .estimated_peak_bytes
            .max(sample.estimated_bytes);
    }

    pub fn records(&self) -> &[QueryRecord] {
        &self.records
    }

    pub fn shape_records(&self) -> &[ShapeRecord] {
        &self.shape_records
    }

    pub fn shape_cache(&self) -> &ShapeCacheRecord {
        &self.shape_cache
    }

    pub fn print_summary(&self) {
        if !self.shape_records.is_empty() {
            println!(
                "profile: {} distinct shape(s)/rule(s)",
                self.shape_records.len()
            );
            let mut sorted = self.shape_records.to_vec();
            sorted.sort_by_key(|b| std::cmp::Reverse(b.total_us));
            for r in &sorted {
                let avg_us = r.total_us.checked_div(r.invocations).unwrap_or(0);
                println!(
                    "  {}: {} call(s), {}µs total, {}µs avg",
                    r.label, r.invocations, r.total_us, avg_us,
                );
            }
        }
        if self.shape_cache.evaluators > 0 {
            let lookups = self.shape_cache.hits + self.shape_cache.misses;
            let hit_rate = if lookups == 0 {
                0.0
            } else {
                self.shape_cache.hits as f64 * 100.0 / lookups as f64
            };
            println!(
                "profile: shape cache: {} evaluator(s), {} hit(s), {} miss(es), \
                 {hit_rate:.1}% hit rate",
                self.shape_cache.evaluators, self.shape_cache.hits, self.shape_cache.misses,
            );
            println!(
                "  {} insertion(s), {} recursion back-edge(s), {} non-cacheable result(s)",
                self.shape_cache.insertions,
                self.shape_cache.recursion_back_edges,
                self.shape_cache.non_cacheable_results,
            );
            println!(
                "  peak: {} entries, ~{} bytes",
                self.shape_cache.peak_entries, self.shape_cache.estimated_peak_bytes,
            );
        }
        if self.records.is_empty() {
            if self.shape_records.is_empty() && self.shape_cache.evaluators == 0 {
                println!("profile: no data collected");
            }
            return;
        }
        println!(
            "profile: {} distinct SPARQL query/queries",
            self.records.len()
        );
        let mut sorted = self.records.to_vec();
        sorted.sort_by_key(|b| std::cmp::Reverse(b.total_exec_us));
        for r in &sorted {
            let exec_str = match &r.executor {
                ExecutorKind::Fallback { reason: None } => "fallback".to_string(),
                ExecutorKind::Fallback { reason: Some(s) } => format!("fallback({s})"),
                ExecutorKind::Native => "native".to_string(),
            };
            let avg_us = r.total_exec_us.checked_div(r.invocations).unwrap_or(0);
            println!(
                "  [{exec_str}] {}: {} call(s), {}µs total, {}µs avg",
                r.fingerprint, r.invocations, r.total_exec_us, avg_us,
            );
        }
    }
}

thread_local! {
    static PROFILER: RefCell<Option<ProfileCollector>> = const { RefCell::new(None) };
}

/// Enable profiling for the current thread. Resets any previous collector.
pub fn enable() {
    PROFILER.with(|p| *p.borrow_mut() = Some(ProfileCollector::new()));
}

/// Disable profiling and return the collected data, if any.
pub fn take() -> Option<ProfileCollector> {
    PROFILER.with(|p| p.borrow_mut().take())
}

/// Whether telemetry is enabled for the current thread.
pub(crate) fn is_enabled() -> bool {
    PROFILER.with(|p| p.borrow().is_some())
}

/// Record one query invocation. No-op when profiling is disabled.
pub fn record(fingerprint: &str, exec_us: u64, executor: ExecutorKind) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.record_invocation(fingerprint, exec_us, executor);
        }
    });
}

/// Record one shape/rule evaluation. No-op when profiling is disabled.
pub fn record_shape(label: &str, exec_us: u64) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.record_shape_invocation(label, exec_us);
        }
    });
}

/// Merge one evaluator's shape-cache telemetry. No-op when profiling is
/// disabled.
pub(crate) fn record_shape_cache(sample: ShapeCacheSample) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.record_shape_cache(sample);
        }
    });
}

/// Helper: measure `f` and record the result under `fingerprint`. Returns the
/// value produced by `f`.
pub fn timed<T>(fingerprint: &str, f: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let result = f();
    let us = start.elapsed().as_micros() as u64;
    record(fingerprint, us, ExecutorKind::Fallback { reason: None });
    result
}

/// Derive a short fingerprint from a canonical query string.
pub fn fingerprint(query: &str) -> String {
    let trimmed = query.trim();
    let preview: String = trimmed.chars().take(60).collect();
    // Replace newlines/runs of whitespace with a single space for readability.
    preview.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregates_shape_cache_samples() {
        let mut collector = ProfileCollector::new();
        collector.record_shape_cache(ShapeCacheSample {
            hits: 3,
            misses: 5,
            insertions: 4,
            recursion_back_edges: 1,
            non_cacheable_results: 2,
            entries: 4,
            estimated_bytes: 400,
        });
        collector.record_shape_cache(ShapeCacheSample {
            hits: 7,
            misses: 2,
            insertions: 2,
            recursion_back_edges: 0,
            non_cacheable_results: 0,
            entries: 2,
            estimated_bytes: 250,
        });

        assert_eq!(
            collector.shape_cache(),
            &ShapeCacheRecord {
                evaluators: 2,
                hits: 10,
                misses: 7,
                insertions: 6,
                recursion_back_edges: 1,
                non_cacheable_results: 2,
                peak_entries: 4,
                estimated_peak_bytes: 400,
            }
        );
    }
}
