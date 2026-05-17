use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiagnosticSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceRef {
    pub graph_iri: String,
    pub locator: Option<String>,
    pub is_root: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub severity: DiagnosticSeverity,
    pub message: String,
    pub source: Option<SourceRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectionNode {
    pub id: String,
    pub kind: String,
    pub label: String,
    pub annotations: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectionEdge {
    pub source: String,
    pub target: String,
    pub kind: String,
    pub weight: u64,
    pub annotations: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectionGraph {
    pub nodes: Vec<InspectionNode>,
    pub edges: Vec<InspectionEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceEventSchema {
    EnterShape,
    ExitShape,
    EnterConstraint,
    ExitConstraint,
    EnterRule,
    ExitRule,
    TargetResolutionStart,
    TargetResolutionEnd,
    InferenceIterationStart,
    InferenceIterationEnd,
}

