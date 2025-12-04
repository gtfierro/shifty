//! Backend-agnostic tracing events for validation and inference.
//!
//! The current engine stores ad-hoc traces inside `ValidationContext`. V2 will
//! emit `TraceEvent`s through a `TraceSink`, allowing both interpreted and
//! compiled executors to share diagnostics plumbing.

use crate::types::{ComponentID, PropShapeID, RuleID, ID};
use oxigraph::model::Term;

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
