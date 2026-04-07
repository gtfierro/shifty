//! Backend-agnostic tracing events for validation and inference.
//!
//! The current engine stores ad-hoc traces inside `ValidationContext`. V2 will
//! emit `TraceEvent`s through a `TraceSink`, allowing both interpreted and
//! compiled executors to share diagnostics plumbing.

use crate::context::SourceShape;
use crate::types::{ComponentID, ID, PropShapeID, RuleID};
use oxigraph::model::Term;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Structured trace events emitted during execution.
#[derive(Debug, Clone)]
pub enum TraceEvent {
    EnterShapeExecution(SourceShape, Instant),
    ExitShapeExecution(SourceShape, Instant),
    EnterNodeShape(ID, Instant),
    ExitNodeShape(ID, Instant),
    EnterPropertyShape(PropShapeID, Instant),
    ExitPropertyShape(PropShapeID, Instant),
    EnterComponent(ComponentID, Instant),
    ExitComponent(ComponentID, Instant),
    EnterRule(RuleID, Instant),
    ExitRule(RuleID, usize, Instant),
    ComponentPassed {
        component: ComponentID,
        focus: Term,
        value: Option<Term>,
        ts: Instant,
    },
    ComponentFailed {
        component: ComponentID,
        focus: Term,
        value: Option<Term>,
        message: Option<String>,
        ts: Instant,
    },
    SparqlQuery {
        label: String,
        ts: Instant,
    },
    RuleApplied {
        rule: RuleID,
        inserted: usize,
        ts: Instant,
    },
    /// Target collection started for a shape
    TargetCollectionStart(SourceShape, Instant),
    /// Target collection completed for a shape
    TargetCollectionEnd(SourceShape, usize /* target_count */, Instant),
    /// Target results retrieved from cache
    TargetCacheHit(SourceShape, usize /* cached_count */),
    /// Component execution started
    ComponentExecutionStart(ComponentID, SourceShape, Instant),
    /// Component execution completed
    ComponentExecutionEnd(ComponentID, SourceShape, Instant),
    /// Component result retrieved from cache (for memoization)
    ComponentCacheHit(ComponentID, SourceShape),
    /// Inference condition conformance cache hit
    /// (condition_shape_id, focus_node)
    InferenceConditionCacheHit(ID, Term),
    /// Parallel wave execution started
    ParallelWaveStarted {
        wave_index: usize,
        rules_count: usize,
        ts: Instant,
    },
    /// Parallel wave execution completed
    ParallelWaveCompleted {
        wave_index: usize,
        rules_count: usize,
        triples_added: usize,
        ts: Instant,
    },
}

/// Consumer for trace events. Implementations may buffer, stream, or drop.
pub trait TraceSink: Send + Sync {
    fn record(&self, event: TraceEvent);
    fn record_batch(&self, events: Vec<TraceEvent>) {
        for event in events {
            self.record(event);
        }
    }
}

/// No-op sink useful when callers do not care about traces.
pub struct NullTraceSink;

impl TraceSink for NullTraceSink {
    fn record(&self, _event: TraceEvent) {}
    fn record_batch(&self, _events: Vec<TraceEvent>) {}
}

/// In-memory sink that records all events for later inspection.
pub struct MemoryTraceSink {
    events: Arc<Mutex<Vec<TraceEvent>>>,
}

impl MemoryTraceSink {
    pub fn new(events: Arc<Mutex<Vec<TraceEvent>>>) -> Self {
        Self { events }
    }

    pub fn events(&self) -> Arc<Mutex<Vec<TraceEvent>>> {
        Arc::clone(&self.events)
    }
}

impl TraceSink for MemoryTraceSink {
    fn record(&self, event: TraceEvent) {
        if let Ok(mut guard) = self.events.lock() {
            guard.push(event);
        }
    }

    fn record_batch(&self, events: Vec<TraceEvent>) {
        if events.is_empty() {
            return;
        }
        if let Ok(mut guard) = self.events.lock() {
            guard.extend(events);
        }
    }
}
