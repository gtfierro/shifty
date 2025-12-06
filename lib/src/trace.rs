//! Backend-agnostic tracing events for validation and inference.
//!
//! The current engine stores ad-hoc traces inside `ValidationContext`. V2 will
//! emit `TraceEvent`s through a `TraceSink`, allowing both interpreted and
//! compiled executors to share diagnostics plumbing.

use crate::types::{ComponentID, PropShapeID, RuleID, ID};
use oxigraph::model::Term;
use std::sync::{Arc, Mutex};

/// Structured trace events emitted during execution.
#[derive(Debug, Clone)]
pub enum TraceEvent {
    EnterNodeShape(ID),
    EnterPropertyShape(PropShapeID),
    ComponentPassed {
        component: ComponentID,
        focus: Term,
        value: Option<Term>,
    },
    ComponentFailed {
        component: ComponentID,
        focus: Term,
        value: Option<Term>,
        message: Option<String>,
    },
    SparqlQuery {
        label: String,
    },
    RuleApplied {
        rule: RuleID,
        inserted: usize,
    },
}

/// Consumer for trace events. Implementations may buffer, stream, or drop.
pub trait TraceSink: Send + Sync {
    fn record(&self, event: TraceEvent);
}

/// No-op sink useful when callers do not care about traces.
pub struct NullTraceSink;

impl TraceSink for NullTraceSink {
    fn record(&self, _event: TraceEvent) {}
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
}
