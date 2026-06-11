//! Opt-in per-query telemetry (doc §269). Collected via a thread-local so
//! no validation API signatures change. Enable with `enable()`, consume with
//! `take()`.
//!
//! Overhead when disabled: one thread-local check per query invocation.

use std::cell::RefCell;
use std::time::Instant;

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

    pub fn records(&self) -> &[QueryRecord] {
        &self.records
    }

    pub fn print_summary(&self) {
        if self.records.is_empty() {
            println!("profile: no SPARQL queries invoked");
            return;
        }
        println!("profile: {} distinct query/queries", self.records.len());
        let mut sorted = self.records.to_vec();
        sorted.sort_by(|a, b| b.total_exec_us.cmp(&a.total_exec_us));
        for r in &sorted {
            let exec_str = match &r.executor {
                ExecutorKind::Fallback { reason: None } => "fallback".to_string(),
                ExecutorKind::Fallback { reason: Some(s) } => format!("fallback({s})"),
                ExecutorKind::Native => "native".to_string(),
            };
            let avg_us = if r.invocations > 0 {
                r.total_exec_us / r.invocations
            } else {
                0
            };
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

/// Record one query invocation. No-op when profiling is disabled.
pub fn record(fingerprint: &str, exec_us: u64, executor: ExecutorKind) {
    PROFILER.with(|p| {
        if let Some(col) = p.borrow_mut().as_mut() {
            col.record_invocation(fingerprint, exec_us, executor);
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
