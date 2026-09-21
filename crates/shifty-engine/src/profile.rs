//! Opt-in execution telemetry (doc §269). Collected via a thread-local so no
//! validation API signatures change. Enable with `enable()`, consume with
//! `take()`.
//!
//! Shape-cache counters are accumulated locally by each evaluator and published
//! once when it is dropped, avoiding a thread-local operation per lookup.

use std::cell::{Cell, RefCell};
use web_time::Instant;

/// Per-query performance record.
#[derive(Debug, Clone)]
pub struct QueryRecord {
    /// Stable fingerprint derived from the canonical query text (first 160 chars).
    pub fingerprint: String,
    /// Whether the native executor or Spareval fallback handled this query.
    pub executor: ExecutorKind,
    /// How many execution batches or fallback probes invoked this query.
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
    /// Evidence-materialization node visits attributed to this label, when the
    /// caller reported them. Zero for conformance-only and inference records.
    pub visits: u64,
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

/// Storage work counted only while profiling is enabled. Counts are structural
/// evidence; wall-clock costs are reported separately.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageRecord {
    /// Time spent compiling data-independent read/write demand.
    pub access_catalog_us: u64,
    pub store_builds: u64,
    pub graph_union_builds: u64,
    pub graph_union_rows: u64,
    pub graph_projection_builds: u64,
    pub graph_projection_rows: u64,
    pub source_builds: u64,
    pub source_rows: u64,
    pub dataset_builds: u64,
    pub encoded_rows: u64,
    pub committed_rows: u64,
    pub commit_batches: u64,
    pub commit_us: u64,
    pub index_builds: u64,
    pub index_bytes: u64,
    pub index_declines: u64,
    /// Allocated PSO pair-buffer capacity, excluding map nodes and dictionaries.
    pub source_primary_bytes: u64,
    pub session_primary_bytes: u64,
    /// Time spent encoding the lazily shared source storage.
    pub source_encode_us: u64,
    /// Time spent encoding session datasets from data graphs.
    pub session_encode_us: u64,
}

/// One optional-index admission decision. The base PSO index is mandatory and
/// therefore does not appear here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexRecord {
    pub scope: &'static str,
    pub kind: &'static str,
    pub predicate: Option<String>,
    pub reason: &'static str,
    pub rows: usize,
    pub estimated_bytes: usize,
    pub actual_bytes: usize,
    pub budget_bytes: usize,
    pub build_us: u64,
    pub accepted: bool,
}

/// Candidate rows visited by one scan pattern. The mask uses S=1, P=2, O=4.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanRecord {
    pub calls: u64,
    pub candidate_rows: u64,
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
    storage: StorageRecord,
    indexes: Vec<IndexRecord>,
    scans: [[ScanRecord; 8]; 2],
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
        self.record_shape_work(label, exec_us, 0);
    }

    /// Record one evaluation together with the evidence nodes it visited.
    pub fn record_shape_work(&mut self, label: &str, exec_us: u64, visits: u64) {
        if let Some(r) = self.shape_records.iter_mut().find(|r| r.label == label) {
            r.invocations += 1;
            r.total_us += exec_us;
            r.visits += visits;
        } else {
            self.shape_records.push(ShapeRecord {
                label: label.to_string(),
                invocations: 1,
                total_us: exec_us,
                visits,
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

    pub fn storage(&self) -> &StorageRecord {
        &self.storage
    }

    pub fn indexes(&self) -> &[IndexRecord] {
        &self.indexes
    }

    pub fn scans(&self) -> &[[ScanRecord; 8]; 2] {
        &self.scans
    }

    pub fn print_summary(&self) {
        if self.storage.store_builds + self.storage.dataset_builds + self.storage.source_builds > 0
        {
            println!(
                "profile: storage: {} Store build(s), {} source build(s) / {} row(s), {} dataset build(s), {} encoded row(s), {} committed row(s), {} index build(s) / {} byte(s), {} declined",
                self.storage.store_builds,
                self.storage.source_builds,
                self.storage.source_rows,
                self.storage.dataset_builds,
                self.storage.encoded_rows,
                self.storage.committed_rows,
                self.storage.index_builds,
                self.storage.index_bytes,
                self.storage.index_declines,
            );
            println!(
                "profile: storage time: {} µs access catalog, {} µs shared source encode, {} µs session encode, {} µs across {} commit batch(es)",
                self.storage.access_catalog_us,
                self.storage.source_encode_us,
                self.storage.session_encode_us,
                self.storage.commit_us,
                self.storage.commit_batches,
            );
            println!(
                "profile: primary pair buffers: {} source byte(s), {} session byte(s)",
                self.storage.source_primary_bytes, self.storage.session_primary_bytes,
            );
            println!(
                "profile: graph materialization: {} union build(s) / {} row(s), {} compatibility projection(s) / {} row(s)",
                self.storage.graph_union_builds,
                self.storage.graph_union_rows,
                self.storage.graph_projection_builds,
                self.storage.graph_projection_rows,
            );
        }
        for index in &self.indexes {
            let predicate = index
                .predicate
                .as_deref()
                .map_or(String::new(), |predicate| format!(" <{predicate}>"));
            println!(
                "profile: index: {} {}{}: {}, {} row(s), {} estimated byte(s), {} allocated byte(s), {} byte budget, {} µs build, {}",
                index.scope,
                index.kind,
                predicate,
                index.reason,
                index.rows,
                index.estimated_bytes,
                index.actual_bytes,
                index.budget_bytes,
                index.build_us,
                if index.accepted { "built" } else { "declined" },
            );
        }
        for (scope, patterns) in ["source", "session"].into_iter().zip(&self.scans) {
            for (mask, scan) in patterns.iter().enumerate() {
                if scan.calls == 0 {
                    continue;
                }
                let pattern = [
                    if mask & 1 != 0 { 'S' } else { '-' },
                    if mask & 2 != 0 { 'P' } else { '-' },
                    if mask & 4 != 0 { 'O' } else { '-' },
                ];
                println!(
                    "profile: scan: {scope} {}{}{}: {} call(s), {} candidate row(s)",
                    pattern[0], pattern[1], pattern[2], scan.calls, scan.candidate_rows,
                );
            }
        }
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

pub(crate) fn record_store_build() {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.storage.store_builds += 1;
        }
    });
}

pub(crate) fn record_graph_union(rows: usize) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.storage.graph_union_builds += 1;
            col.storage.graph_union_rows += rows as u64;
        }
    });
}

pub(crate) fn record_graph_projection(rows: usize) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.storage.graph_projection_builds += 1;
            col.storage.graph_projection_rows += rows as u64;
        }
    });
}

pub(crate) fn record_source_build(rows: usize) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.storage.source_builds += 1;
            col.storage.source_rows += rows as u64;
        }
    });
}

pub(crate) fn record_source_encode_time(us: u64) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.storage.source_encode_us += us;
        }
    });
}

pub(crate) fn record_access_catalog_time(us: u64) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.storage.access_catalog_us = col.storage.access_catalog_us.saturating_add(us);
        }
    });
}

pub(crate) fn record_dataset_build(rows: usize) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.storage.dataset_builds += 1;
            col.storage.encoded_rows += rows as u64;
        }
    });
}

pub(crate) fn record_session_encode_time(us: u64) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.storage.session_encode_us += us;
        }
    });
}

pub(crate) fn record_dataset_commit(rows: usize, us: u64) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.storage.committed_rows += rows as u64;
            col.storage.commit_batches += 1;
            col.storage.commit_us += us;
        }
    });
}

pub(crate) fn record_index_decision(record: IndexRecord) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            if record.accepted {
                col.storage.index_builds += 1;
                col.storage.index_bytes += record.actual_bytes as u64;
            } else {
                col.storage.index_declines += 1;
            }
            col.indexes.push(record);
        }
    });
}

pub(crate) fn record_primary_index_bytes(scope: &'static str, old: usize, new: usize) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            let bytes = if scope == "source" {
                &mut col.storage.source_primary_bytes
            } else {
                &mut col.storage.session_primary_bytes
            };
            *bytes = bytes.saturating_sub(old as u64) + new as u64;
        }
    });
}

pub(crate) fn observe_shared_source_primary_bytes(bytes: usize) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.storage.source_primary_bytes = col.storage.source_primary_bytes.max(bytes as u64);
        }
    });
}

pub(crate) fn record_scan(scope: &'static str, mask: usize, candidates: usize) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            let scope = usize::from(scope == "session");
            let scan = &mut col.scans[scope][mask];
            scan.calls += 1;
            scan.candidate_rows += candidates as u64;
        }
    });
}

/// Record one evaluation plus its evidence-node visits. No-op when profiling
/// is disabled.
pub(crate) fn record_shape_work(label: &str, exec_us: u64, visits: u64) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.record_shape_work(label, exec_us, visits);
        }
    });
}

thread_local! {
    static EVIDENCE_VISITS: Cell<u64> = const { Cell::new(0) };
}

/// One evidence-materialization node visit.
///
/// Unlike the counters above this is always on: it is a `Cell` increment beside
/// a function that already clones a `Shape`, and it is the only way to see how
/// far the shared shape DAG expands into a per-pair evidence tree. Visits count
/// nodes *entered*, including those later pruned from the retained evidence.
pub(crate) fn record_evidence_visit() {
    EVIDENCE_VISITS.with(|visits| visits.set(visits.get().wrapping_add(1)));
}

/// Read and reset the evidence-visit counter for this thread.
pub fn take_evidence_visits() -> u64 {
    EVIDENCE_VISITS.with(|visits| visits.replace(0))
}

/// Read the evidence-visit counter without resetting it, for deltas around a
/// region of work.
pub fn evidence_visits() -> u64 {
    EVIDENCE_VISITS.with(Cell::get)
}

thread_local! {
    static PATH_PROBES: Cell<u64> = const { Cell::new(0) };
}

/// One `path_support` probe. Each probe re-derives a full successor set from
/// the backend to answer a single membership question, so probe count is the
/// multiplier on path work that evidence pays and conformance does not.
pub(crate) fn record_path_probe() {
    PATH_PROBES.with(|probes| probes.set(probes.get().wrapping_add(1)));
}

/// Read and reset the path-probe counter for this thread.
pub fn take_path_probes() -> u64 {
    PATH_PROBES.with(|probes| probes.replace(0))
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
    let preview: String = trimmed.chars().take(160).collect();
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
