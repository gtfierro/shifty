use crate::algebra::{ConstraintId, Severity, ShapeId};
use crate::diagnostics::{SourceRef, TraceEventSchema};
use crate::plan::ValidationPlan;
use crate::source::ResolvedShapeSet;
use oxrdf::Term;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub trait ValidationBackend {
    fn execute(
        &self,
        plan: &ValidationPlan,
        data: &ResolvedShapeSet,
    ) -> Result<ValidationResult, String>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationViolation {
    pub shape: ShapeId,
    pub constraint: Option<ConstraintId>,
    pub focus: Term,
    pub focus_node: String,
    pub value: Option<Term>,
    pub value_node: Option<String>,
    pub result_path: Option<Term>,
    pub message: String,
    pub severity: Severity,
    pub source: Option<SourceRef>,
    pub source_shape: Option<Term>,
    pub source_constraint: Option<Term>,
    pub source_constraint_component: Option<Term>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationUnsupported {
    pub shape: ShapeId,
    pub constraint: Option<ConstraintId>,
    pub focus: Option<Term>,
    pub focus_node: Option<String>,
    pub reason: String,
    pub kind: String,
    pub severity: Severity,
    pub source: Option<SourceRef>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidationCoverage {
    pub executed_constraints: usize,
    pub executed_rules: usize,
    pub unsupported_constraints: usize,
    pub deferred_recursions: usize,
    pub inference_iterations: usize,
    pub inferred_triples: usize,
    pub unsupported_by_kind: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationTraceEvent {
    pub event: TraceEventSchema,
    pub shape: Option<ShapeId>,
    pub constraint: Option<ConstraintId>,
    pub focus_node: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidationHeatmap {
    pub shape_hits: BTreeMap<String, usize>,
    pub constraint_hits: BTreeMap<String, usize>,
    pub rule_hits: BTreeMap<String, usize>,
    pub shape_violations: BTreeMap<String, usize>,
    pub constraint_violations: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub conforms: bool,
    pub focus_nodes_evaluated: usize,
    pub violations: Vec<ValidationViolation>,
    pub unsupported: Vec<ValidationUnsupported>,
    pub coverage: ValidationCoverage,
    pub trace: Vec<ValidationTraceEvent>,
    pub heatmap: ValidationHeatmap,
}
