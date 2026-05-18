use crate::algebra::{
    ConstraintExpr, ConstraintId, LogicalKind, PropertyPath, Severity, ShapeId, ShapeKind,
    ShapeProgram, TargetExpr,
};
use crate::diagnostics::{SourceRef, TraceEventSchema};
use crate::plan::{ValidationPlan, ValidationPlanNode};
use crate::source::ResolvedShapeSet;
use oxrdf::{NamedNode, NamedOrBlankNode, Quad, Term};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const SH_IRI: &str = "http://www.w3.org/ns/shacl#IRI";
const SH_LITERAL: &str = "http://www.w3.org/ns/shacl#Literal";
const SH_BLANK_NODE: &str = "http://www.w3.org/ns/shacl#BlankNode";
const SH_BLANK_NODE_OR_IRI: &str = "http://www.w3.org/ns/shacl#BlankNodeOrIRI";
const SH_BLANK_NODE_OR_LITERAL: &str = "http://www.w3.org/ns/shacl#BlankNodeOrLiteral";
const SH_IRI_OR_LITERAL: &str = "http://www.w3.org/ns/shacl#IRIOrLiteral";

pub trait ValidationBackend {
    fn execute(
        &self,
        plan: &ValidationPlan,
        data: &ResolvedShapeSet,
    ) -> Result<ValidationResult, String>;
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryValidationBackend;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationViolation {
    pub shape: ShapeId,
    pub constraint: Option<ConstraintId>,
    pub focus_node: String,
    pub value_node: Option<String>,
    pub message: String,
    pub severity: Severity,
    pub source: Option<SourceRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationUnsupported {
    pub shape: ShapeId,
    pub constraint: Option<ConstraintId>,
    pub focus_node: Option<String>,
    pub reason: String,
    pub kind: String,
    pub severity: Severity,
    pub source: Option<SourceRef>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidationCoverage {
    pub executed_constraints: usize,
    pub unsupported_constraints: usize,
    pub deferred_recursions: usize,
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

impl ValidationBackend for InMemoryValidationBackend {
    fn execute(
        &self,
        plan: &ValidationPlan,
        data: &ResolvedShapeSet,
    ) -> Result<ValidationResult, String> {
        let index = DataIndex::new(&data.quads);
        let mut state = ExecutionState::new(&plan.view.program, index);
        for node in &plan.nodes {
            if let ValidationPlanNode::TargetScan { shape, .. } = node {
                let targets = plan
                    .view
                    .program
                    .targets
                    .iter()
                    .filter(|target| target.owner == *shape)
                    .collect::<Vec<_>>();
                for target in targets {
                    state.trace.push(ValidationTraceEvent {
                        event: TraceEventSchema::TargetResolutionStart,
                        shape: Some(*shape),
                        constraint: None,
                        focus_node: None,
                        message: format!("resolving target {}", target.id.0),
                    });
                    let focuses = resolve_target(&state.index, &target.expr);
                    for focus in focuses {
                        state.focus_nodes_evaluated += 1;
                        let _ = eval_shape(*shape, &focus, &mut state)?;
                    }
                    state.trace.push(ValidationTraceEvent {
                        event: TraceEventSchema::TargetResolutionEnd,
                        shape: Some(*shape),
                        constraint: None,
                        focus_node: None,
                        message: format!("resolved target {}", target.id.0),
                    });
                }
            }
        }
        Ok(ValidationResult {
            conforms: state.violations.is_empty(),
            focus_nodes_evaluated: state.focus_nodes_evaluated,
            violations: state.violations,
            unsupported: state.unsupported,
            coverage: state.coverage,
            trace: state.trace,
            heatmap: state.heatmap,
        })
    }
}

struct ExecutionState<'a> {
    program: &'a ShapeProgram,
    index: DataIndex,
    violations: Vec<ValidationViolation>,
    unsupported: Vec<ValidationUnsupported>,
    coverage: ValidationCoverage,
    trace: Vec<ValidationTraceEvent>,
    heatmap: ValidationHeatmap,
    active: HashSet<(ShapeId, String)>,
    focus_nodes_evaluated: usize,
}

impl<'a> ExecutionState<'a> {
    fn new(program: &'a ShapeProgram, index: DataIndex) -> Self {
        Self {
            program,
            index,
            violations: Vec::new(),
            unsupported: Vec::new(),
            coverage: ValidationCoverage::default(),
            trace: Vec::new(),
            heatmap: ValidationHeatmap::default(),
            active: HashSet::new(),
            focus_nodes_evaluated: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutionStatus {
    Completed,
    DeferredRecursion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProbeOutcome {
    Conformant,
    NonConformant,
    Unsupported(String),
    DeferredRecursion,
}

#[derive(Debug, Clone)]
struct DataIndex {
    outgoing: HashMap<String, Vec<(NamedNode, Term)>>,
    incoming: HashMap<String, Vec<(NamedNode, Term)>>,
    types: HashMap<String, BTreeSet<String>>,
}

impl DataIndex {
    fn new(quads: &[Quad]) -> Self {
        let mut outgoing = HashMap::new();
        let mut incoming = HashMap::new();
        let mut types = HashMap::new();
        for quad in quads {
            let subject = subject_to_term(&quad.subject);
            let subject_key = subject.to_string();
            outgoing
                .entry(subject_key.clone())
                .or_insert_with(Vec::new)
                .push((quad.predicate.clone(), quad.object.clone()));
            let object_key = quad.object.to_string();
            incoming
                .entry(object_key.clone())
                .or_insert_with(Vec::new)
                .push((quad.predicate.clone(), subject.clone()));
            if quad.predicate.as_str() == RDF_TYPE {
                types
                    .entry(subject_key)
                    .or_insert_with(BTreeSet::new)
                    .insert(quad.object.to_string());
            }
        }
        Self {
            outgoing,
            incoming,
            types,
        }
    }
}

fn eval_shape(
    shape_id: ShapeId,
    focus: &Term,
    state: &mut ExecutionState<'_>,
) -> Result<ExecutionStatus, String> {
    let Some(shape) = state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == shape_id)
    else {
        return Err(format!("unknown shape {}", shape_id.0));
    };
    let key = (shape_id, focus.to_string());
    if !state.active.insert(key.clone()) {
        state.trace.push(ValidationTraceEvent {
            event: TraceEventSchema::EnterShape,
            shape: Some(shape_id),
            constraint: None,
            focus_node: Some(focus.to_string()),
            message: "deferring recursive re-entry".to_string(),
        });
        state.coverage.deferred_recursions += 1;
        return Ok(ExecutionStatus::DeferredRecursion);
    }

    state.trace.push(ValidationTraceEvent {
        event: TraceEventSchema::EnterShape,
        shape: Some(shape_id),
        constraint: None,
        focus_node: Some(focus.to_string()),
        message: format!("enter shape {}", shape.normalized_key),
    });
    *state
        .heatmap
        .shape_hits
        .entry(shape.normalized_key.clone())
        .or_insert(0) += 1;

    let status = match shape.kind {
        ShapeKind::Node => eval_node_shape(shape_id, focus, state)?,
        ShapeKind::Property => {
            state.active.remove(&key);
            return Err(format!(
                "property shape {} requires a parent node context",
                shape.normalized_key
            ));
        }
    };

    state.trace.push(ValidationTraceEvent {
        event: TraceEventSchema::ExitShape,
        shape: Some(shape_id),
        constraint: None,
        focus_node: Some(focus.to_string()),
        message: format!("exit shape {}", shape.normalized_key),
    });
    state.active.remove(&key);
    Ok(status)
}

fn eval_node_shape(
    shape_id: ShapeId,
    focus: &Term,
    state: &mut ExecutionState<'_>,
) -> Result<ExecutionStatus, String> {
    let shape = state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == shape_id)
        .ok_or_else(|| format!("unknown shape {}", shape_id.0))?;
    let mut status = ExecutionStatus::Completed;
    for constraint_id in &shape.constraints {
        let constraint = state
            .program
            .constraints
            .iter()
            .find(|constraint| constraint.id == *constraint_id)
            .ok_or_else(|| format!("unknown constraint {}", constraint_id.0))?;
        if eval_constraint(shape_id, constraint.id, &constraint.expr, focus, None, state)?
            == ExecutionStatus::DeferredRecursion
        {
            status = ExecutionStatus::DeferredRecursion;
        }
    }
    for property_shape_id in &shape.property_shapes {
        let property_shape = state
            .program
            .shapes
            .iter()
            .find(|shape| shape.id == *property_shape_id)
            .ok_or_else(|| format!("unknown property shape {}", property_shape_id.0))?;
        if let Some(path) = property_shape.path.as_ref() {
            if !path_is_executable(path) {
                record_unsupported(
                    *property_shape_id,
                    None,
                    Some(focus),
                    "property_path",
                    format!("property path {} is not executable in the in-memory backend", path_label(path)),
                    state,
                );
                continue;
            }
        }
        let values = property_shape
            .path
            .as_ref()
            .map(|path| eval_path(&state.index, focus, path))
            .unwrap_or_default();
        if eval_property_shape(*property_shape_id, focus, &values, state)?
            == ExecutionStatus::DeferredRecursion
        {
            status = ExecutionStatus::DeferredRecursion;
        }
    }
    Ok(status)
}

fn eval_property_shape(
    shape_id: ShapeId,
    parent_focus: &Term,
    values: &[Term],
    state: &mut ExecutionState<'_>,
) -> Result<ExecutionStatus, String> {
    let shape = state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == shape_id)
        .ok_or_else(|| format!("unknown property shape {}", shape_id.0))?;
    state.trace.push(ValidationTraceEvent {
        event: TraceEventSchema::EnterShape,
        shape: Some(shape_id),
        constraint: None,
        focus_node: Some(parent_focus.to_string()),
        message: format!("enter property shape {}", shape.normalized_key),
    });
    *state
        .heatmap
        .shape_hits
        .entry(shape.normalized_key.clone())
        .or_insert(0) += 1;
    let mut status = ExecutionStatus::Completed;
    for constraint_id in &shape.constraints {
        let constraint = state
            .program
            .constraints
            .iter()
            .find(|constraint| constraint.id == *constraint_id)
            .ok_or_else(|| format!("unknown constraint {}", constraint_id.0))?;
        if eval_constraint(
            shape_id,
            constraint.id,
            &constraint.expr,
            parent_focus,
            Some(values),
            state,
        )? == ExecutionStatus::DeferredRecursion
        {
            status = ExecutionStatus::DeferredRecursion;
        }
    }
    state.trace.push(ValidationTraceEvent {
        event: TraceEventSchema::ExitShape,
        shape: Some(shape_id),
        constraint: None,
        focus_node: Some(parent_focus.to_string()),
        message: format!("exit property shape {}", shape.normalized_key),
    });
    Ok(status)
}

fn eval_constraint(
    shape_id: ShapeId,
    constraint_id: ConstraintId,
    expr: &ConstraintExpr,
    focus: &Term,
    values: Option<&[Term]>,
    state: &mut ExecutionState<'_>,
) -> Result<ExecutionStatus, String> {
    state.trace.push(ValidationTraceEvent {
        event: TraceEventSchema::EnterConstraint,
        shape: Some(shape_id),
        constraint: Some(constraint_id),
        focus_node: Some(focus.to_string()),
        message: format!("enter constraint {}", constraint_id.0),
    });
    *state
        .heatmap
        .constraint_hits
        .entry(constraint_id.0.to_string())
        .or_insert(0) += 1;
    state.coverage.executed_constraints += 1;
    let local_values = values.unwrap_or(std::slice::from_ref(focus));
    let status = match expr {
        ConstraintExpr::Cardinality { min, max, .. } => {
            let count = local_values.len() as u64;
            if min.is_some_and(|min| count < min) || max.is_some_and(|max| count > max) {
                record_violation(
                    shape_id,
                    constraint_id,
                    focus,
                    None,
                    format!("cardinality violation: count={count}"),
                    state,
                );
            }
            ExecutionStatus::Completed
        }
        ConstraintExpr::HasValue(expected) => {
            if !local_values.iter().any(|value| value == expected) {
                record_violation(
                    shape_id,
                    constraint_id,
                    focus,
                    None,
                    format!("missing required value {}", expected),
                    state,
                );
            }
            ExecutionStatus::Completed
        }
        ConstraintExpr::In(allowed) => {
            for value in local_values {
                if !allowed.contains(value) {
                    record_violation(
                        shape_id,
                        constraint_id,
                        focus,
                        Some(value),
                        format!("value {} not in allowed set", value),
                        state,
                    );
                }
            }
            ExecutionStatus::Completed
        }
        ConstraintExpr::Datatype(expected) => {
            for value in local_values {
                if !matches_datatype(value, expected) {
                    record_violation(
                        shape_id,
                        constraint_id,
                        focus,
                        Some(value),
                        format!("value {} does not match datatype {}", value, expected),
                        state,
                    );
                }
            }
            ExecutionStatus::Completed
        }
        ConstraintExpr::NodeKind(expected) => {
            for value in local_values {
                if !matches_node_kind(value, expected) {
                    record_violation(
                        shape_id,
                        constraint_id,
                        focus,
                        Some(value),
                        format!("value {} does not match node kind {}", value, expected),
                        state,
                    );
                }
            }
            ExecutionStatus::Completed
        }
        ConstraintExpr::Class(expected) => {
            for value in local_values {
                if !has_class(&state.index, value, expected) {
                    record_violation(
                        shape_id,
                        constraint_id,
                        focus,
                        Some(value),
                        format!("value {} does not have class {}", value, expected),
                        state,
                    );
                }
            }
            ExecutionStatus::Completed
        }
        ConstraintExpr::StringConstraint {
            predicate,
            values: params,
        } => {
            if predicate.as_str() == "http://www.w3.org/ns/shacl#uniqueLang" {
                if unique_lang_enabled(params) {
                    for (value, message) in check_unique_lang(local_values) {
                        record_violation(
                            shape_id,
                            constraint_id,
                            focus,
                            value.as_ref(),
                            message,
                            state,
                        );
                    }
                }
            } else {
                for value in local_values {
                    match check_string_constraint(predicate.as_str(), params, value) {
                        Ok(Some(message)) => {
                            record_violation(
                                shape_id,
                                constraint_id,
                                focus,
                                Some(value),
                                message,
                                state,
                            );
                        }
                        Ok(None) => {}
                        Err(reason) => {
                            record_unsupported(
                                shape_id,
                                Some(constraint_id),
                                Some(focus),
                                constraint_kind_name(expr),
                                reason,
                                state,
                            );
                        }
                    }
                }
            }
            ExecutionStatus::Completed
        }
        ConstraintExpr::NumericRange { predicate, values } => {
            for value in local_values {
                match check_numeric_range(predicate.as_str(), values, value) {
                    Ok(Some(message)) => {
                        record_violation(shape_id, constraint_id, focus, Some(value), message, state);
                    }
                    Ok(None) => {}
                    Err(reason) => {
                        record_unsupported(
                            shape_id,
                            Some(constraint_id),
                            Some(focus),
                            constraint_kind_name(expr),
                            reason,
                            state,
                        );
                        break;
                    }
                }
            }
            ExecutionStatus::Completed
        }
        ConstraintExpr::PropertyComparison { predicate, values } => {
            match eval_property_comparison(
                &state.index,
                predicate.as_str(),
                focus,
                local_values,
                values,
            ) {
                Ok(messages) => {
                    for (value, message) in messages {
                        record_violation(
                            shape_id,
                            constraint_id,
                            focus,
                            value.as_ref(),
                            message,
                            state,
                        );
                    }
                }
                Err(reason) => {
                    record_unsupported(
                        shape_id,
                        Some(constraint_id),
                        Some(focus),
                        constraint_kind_name(expr),
                        reason,
                        state,
                    );
                }
            }
            ExecutionStatus::Completed
        }
        ConstraintExpr::Closed { ignored_properties } => {
            match eval_closed_shape(shape_id, focus, ignored_properties, state) {
                Ok(messages) => {
                    for message in messages {
                        record_violation(shape_id, constraint_id, focus, None, message, state);
                    }
                }
                Err(reason) => {
                    record_unsupported(
                        shape_id,
                        Some(constraint_id),
                        Some(focus),
                        constraint_kind_name(expr),
                        reason,
                        state,
                    );
                }
            }
            ExecutionStatus::Completed
        }
        ConstraintExpr::NodeRef {
            shape: Some(target),
            ..
        } => {
            let mut status = ExecutionStatus::Completed;
            for value in local_values {
                if eval_shape(*target, value, state)? == ExecutionStatus::DeferredRecursion {
                    status = ExecutionStatus::DeferredRecursion;
                    record_unsupported(
                        shape_id,
                        Some(constraint_id),
                        Some(focus),
                        constraint_kind_name(expr),
                        format!("deferred recursive validation while evaluating shape {}", target.0),
                        state,
                    );
                }
            }
            status
        }
        ConstraintExpr::PropertyRef {
            shape: Some(target),
            ..
        } => {
            let mut status = ExecutionStatus::Completed;
            for value in local_values {
                let nested_shape = state
                    .program
                    .shapes
                    .iter()
                    .find(|shape| shape.id == *target)
                    .ok_or_else(|| format!("unknown property ref shape {}", target.0))?;
                let nested_values = nested_shape
                    .path
                    .as_ref()
                    .map(|path| eval_path(&state.index, value, path))
                    .unwrap_or_default();
                if eval_property_shape(*target, value, &nested_values, state)?
                    == ExecutionStatus::DeferredRecursion
                {
                    status = ExecutionStatus::DeferredRecursion;
                    record_unsupported(
                        shape_id,
                        Some(constraint_id),
                        Some(focus),
                        constraint_kind_name(expr),
                        format!(
                            "deferred recursive validation while evaluating property shape {}",
                            target.0
                        ),
                        state,
                    );
                }
            }
            status
        }
        ConstraintExpr::QualifiedValueShape {
            shape: Some(target),
            min_count,
            max_count,
            disjoint,
            ..
        } => {
            if disjoint == &Some(true) {
                record_unsupported(
                    shape_id,
                    Some(constraint_id),
                    Some(focus),
                    constraint_kind_name(expr),
                    "qualifiedValueShapesDisjoint is not yet executable".to_string(),
                    state,
                );
                ExecutionStatus::Completed
            } else {
                let mut matching = 0u64;
                for value in local_values {
                    match probe_shape_conforms(*target, value, state)? {
                        ProbeOutcome::Conformant => matching += 1,
                        ProbeOutcome::NonConformant => {}
                        ProbeOutcome::Unsupported(reason) => {
                            record_unsupported(
                                shape_id,
                                Some(constraint_id),
                                Some(focus),
                                constraint_kind_name(expr),
                                reason,
                                state,
                            );
                            return Ok(ExecutionStatus::Completed);
                        }
                        ProbeOutcome::DeferredRecursion => {
                            record_unsupported(
                                shape_id,
                                Some(constraint_id),
                                Some(focus),
                                constraint_kind_name(expr),
                                format!(
                                    "deferred recursive validation while probing qualified shape {}",
                                    target.0
                                ),
                                state,
                            );
                            return Ok(ExecutionStatus::DeferredRecursion);
                        }
                    }
                }
                if min_count.is_some_and(|min| matching < min)
                    || max_count.is_some_and(|max| matching > max)
                {
                    record_violation(
                        shape_id,
                        constraint_id,
                        focus,
                        None,
                        format!("qualified value shape count violation: count={matching}"),
                        state,
                    );
                }
                ExecutionStatus::Completed
            }
        }
        ConstraintExpr::Not {
            shape: Some(target), ..
        } => {
            for value in local_values {
                match probe_shape_conforms(*target, value, state)? {
                    ProbeOutcome::Conformant => record_violation(
                        shape_id,
                        constraint_id,
                        focus,
                        Some(value),
                        format!("value {} conforms to forbidden shape {}", value, target.0),
                        state,
                    ),
                    ProbeOutcome::NonConformant => {}
                    ProbeOutcome::Unsupported(reason) => {
                        record_unsupported(
                            shape_id,
                            Some(constraint_id),
                            Some(focus),
                            constraint_kind_name(expr),
                            reason,
                            state,
                        );
                    }
                    ProbeOutcome::DeferredRecursion => {
                        record_unsupported(
                            shape_id,
                            Some(constraint_id),
                            Some(focus),
                            constraint_kind_name(expr),
                            format!("deferred recursive validation while probing not-shape {}", target.0),
                            state,
                        );
                        return Ok(ExecutionStatus::DeferredRecursion);
                    }
                }
            }
            ExecutionStatus::Completed
        }
        ConstraintExpr::Logical { kind, shapes } => {
            for value in local_values {
                let mut conformant = 0usize;
                for nested_shape in shapes {
                    match probe_shape_conforms(*nested_shape, value, state)? {
                        ProbeOutcome::Conformant => conformant += 1,
                        ProbeOutcome::NonConformant => {}
                        ProbeOutcome::Unsupported(reason) => {
                            record_unsupported(
                                shape_id,
                                Some(constraint_id),
                                Some(focus),
                                constraint_kind_name(expr),
                                reason,
                                state,
                            );
                            return Ok(ExecutionStatus::Completed);
                        }
                        ProbeOutcome::DeferredRecursion => {
                            record_unsupported(
                                shape_id,
                                Some(constraint_id),
                                Some(focus),
                                constraint_kind_name(expr),
                                "deferred recursive validation while probing logical constraint"
                                    .to_string(),
                                state,
                            );
                            return Ok(ExecutionStatus::DeferredRecursion);
                        }
                    }
                }
                let passes = match kind {
                    LogicalKind::And => conformant == shapes.len(),
                    LogicalKind::Or => conformant >= 1,
                    LogicalKind::Xone => conformant == 1,
                };
                if !passes {
                    record_violation(
                        shape_id,
                        constraint_id,
                        focus,
                        Some(value),
                        format!("logical constraint {:?} failed with {} matching shapes", kind, conformant),
                        state,
                    );
                }
            }
            ExecutionStatus::Completed
        }
        ConstraintExpr::Sparql(_)
        | ConstraintExpr::CustomComponent { .. }
        | ConstraintExpr::GenericPredicate { .. } => {
            record_unsupported(
                shape_id,
                Some(constraint_id),
                Some(focus),
                constraint_kind_name(expr),
                "constraint kind is not executable in the in-memory backend".to_string(),
                state,
            );
            ExecutionStatus::Completed
        }
        ConstraintExpr::NodeRef { shape: None, .. }
        | ConstraintExpr::PropertyRef { shape: None, .. }
        | ConstraintExpr::QualifiedValueShape { shape: None, .. }
        | ConstraintExpr::Not { shape: None, .. } => {
            record_unsupported(
                shape_id,
                Some(constraint_id),
                Some(focus),
                constraint_kind_name(expr),
                "constraint references an unresolved shape".to_string(),
                state,
            );
            ExecutionStatus::Completed
        }
    };
    state.trace.push(ValidationTraceEvent {
        event: TraceEventSchema::ExitConstraint,
        shape: Some(shape_id),
        constraint: Some(constraint_id),
        focus_node: Some(focus.to_string()),
        message: format!("exit constraint {}", constraint_id.0),
    });
    Ok(status)
}

fn resolve_target(index: &DataIndex, target: &TargetExpr) -> Vec<Term> {
    match target {
        TargetExpr::Node(term) => vec![term.clone()],
        TargetExpr::Class(term) => index
            .types
            .iter()
            .filter(|(_, types)| types.contains(&term.to_string()))
            .filter_map(|(subject, _)| parse_term(subject))
            .collect(),
        TargetExpr::SubjectsOf(predicate) => named_target_predicate(predicate)
            .map(|predicate| {
                index
                    .outgoing
                    .iter()
                    .filter(|(_, edges)| edges.iter().any(|(pred, _)| pred == &predicate))
                    .filter_map(|(subject, _)| parse_term(subject))
                    .collect()
            })
            .unwrap_or_default(),
        TargetExpr::ObjectsOf(predicate) => named_target_predicate(predicate)
            .map(|predicate| {
                index
                    .outgoing
                    .values()
                    .flat_map(|edges| {
                        edges.iter().filter_map(|(pred, object)| {
                            if pred == &predicate {
                                Some(object.clone())
                            } else {
                                None
                            }
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        TargetExpr::Advanced(_) => Vec::new(),
    }
}

fn eval_path(index: &DataIndex, focus: &Term, path: &PropertyPath) -> Vec<Term> {
    match path {
        PropertyPath::Predicate(predicate) => index
            .outgoing
            .get(&focus.to_string())
            .into_iter()
            .flatten()
            .filter_map(|(pred, object)| (pred == predicate).then(|| object.clone()))
            .collect(),
        PropertyPath::Inverse(inner) => match inner.as_ref() {
            PropertyPath::Predicate(predicate) => index
                .incoming
                .get(&focus.to_string())
                .into_iter()
                .flatten()
                .filter_map(|(pred, subject)| (pred == predicate).then(|| subject.clone()))
                .collect(),
            _ => Vec::new(),
        },
        PropertyPath::Sequence(parts) => {
            let mut current = vec![focus.clone()];
            for part in parts {
                let mut next = Vec::new();
                for term in &current {
                    next.extend(eval_path(index, term, part));
                }
                current = next;
            }
            current
        }
        PropertyPath::Alternative(parts) => {
            let mut out = Vec::new();
            for part in parts {
                out.extend(eval_path(index, focus, part));
            }
            dedup_terms(out)
        }
        PropertyPath::ZeroOrOne(inner) => {
            let mut out = vec![focus.clone()];
            out.extend(eval_path(index, focus, inner));
            dedup_terms(out)
        }
        PropertyPath::ZeroOrMore(inner) => eval_transitive_path(index, focus, inner, true),
        PropertyPath::OneOrMore(inner) => eval_transitive_path(index, focus, inner, false),
        PropertyPath::Unsupported(_) => Vec::new(),
    }
}

fn probe_shape_conforms(
    shape_id: ShapeId,
    focus: &Term,
    state: &ExecutionState<'_>,
) -> Result<ProbeOutcome, String> {
    let mut active = state.active.clone();
    probe_shape_conforms_inner(shape_id, focus, state, &mut active)
}

fn probe_shape_conforms_inner(
    shape_id: ShapeId,
    focus: &Term,
    state: &ExecutionState<'_>,
    active: &mut HashSet<(ShapeId, String)>,
) -> Result<ProbeOutcome, String> {
    let Some(shape) = state.program.shapes.iter().find(|shape| shape.id == shape_id) else {
        return Err(format!("unknown shape {}", shape_id.0));
    };
    let key = (shape_id, focus.to_string());
    if !active.insert(key.clone()) {
        return Ok(ProbeOutcome::DeferredRecursion);
    }
    let mut unsupported = None;
    let mut deferred = false;
    if shape.kind == ShapeKind::Node {
        for constraint_id in &shape.constraints {
            let constraint = state
                .program
                .constraints
                .iter()
                .find(|constraint| constraint.id == *constraint_id)
                .ok_or_else(|| format!("unknown constraint {}", constraint_id.0))?;
            match probe_constraint(shape_id, &constraint.expr, focus, None, state, active)? {
                ProbeOutcome::Conformant => {}
                ProbeOutcome::NonConformant => {
                    active.remove(&key);
                    return Ok(ProbeOutcome::NonConformant);
                }
                ProbeOutcome::Unsupported(reason) => unsupported = Some(reason),
                ProbeOutcome::DeferredRecursion => deferred = true,
            }
        }
        for property_shape_id in &shape.property_shapes {
            let property_shape = state
                .program
                .shapes
            .iter()
            .find(|shape| shape.id == *property_shape_id)
            .ok_or_else(|| format!("unknown property shape {}", property_shape_id.0))?;
        if let Some(path) = property_shape.path.as_ref() {
            if !path_is_executable(path) {
                return Ok(ProbeOutcome::Unsupported(format!(
                    "property path {} is not executable in the in-memory backend",
                    path_label(path)
                )));
            }
        }
        let values = property_shape
            .path
            .as_ref()
            .map(|path| eval_path(&state.index, focus, path))
            .unwrap_or_default();
            match probe_property_shape(*property_shape_id, focus, &values, state, active)? {
                ProbeOutcome::Conformant => {}
                ProbeOutcome::NonConformant => {
                    active.remove(&key);
                    return Ok(ProbeOutcome::NonConformant);
                }
                ProbeOutcome::Unsupported(reason) => unsupported = Some(reason),
                ProbeOutcome::DeferredRecursion => deferred = true,
            }
        }
    } else {
        active.remove(&key);
        return Ok(ProbeOutcome::Unsupported(
            "property shapes require a parent focus context".to_string(),
        ));
    }
    active.remove(&key);
    if let Some(reason) = unsupported {
        Ok(ProbeOutcome::Unsupported(reason))
    } else if deferred {
        Ok(ProbeOutcome::DeferredRecursion)
    } else {
        Ok(ProbeOutcome::Conformant)
    }
}

fn probe_property_shape(
    shape_id: ShapeId,
    parent_focus: &Term,
    values: &[Term],
    state: &ExecutionState<'_>,
    active: &mut HashSet<(ShapeId, String)>,
) -> Result<ProbeOutcome, String> {
    let shape = state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == shape_id)
        .ok_or_else(|| format!("unknown property shape {}", shape_id.0))?;
    let mut unsupported = None;
    let mut deferred = false;
    for constraint_id in &shape.constraints {
        let constraint = state
            .program
            .constraints
            .iter()
            .find(|constraint| constraint.id == *constraint_id)
            .ok_or_else(|| format!("unknown constraint {}", constraint_id.0))?;
        match probe_constraint(
            shape_id,
            &constraint.expr,
            parent_focus,
            Some(values),
            state,
            active,
        )? {
            ProbeOutcome::Conformant => {}
            ProbeOutcome::NonConformant => return Ok(ProbeOutcome::NonConformant),
            ProbeOutcome::Unsupported(reason) => unsupported = Some(reason),
            ProbeOutcome::DeferredRecursion => deferred = true,
        }
    }
    if let Some(reason) = unsupported {
        Ok(ProbeOutcome::Unsupported(reason))
    } else if deferred {
        Ok(ProbeOutcome::DeferredRecursion)
    } else {
        Ok(ProbeOutcome::Conformant)
    }
}

fn probe_constraint(
    shape_id: ShapeId,
    expr: &ConstraintExpr,
    focus: &Term,
    values: Option<&[Term]>,
    state: &ExecutionState<'_>,
    active: &mut HashSet<(ShapeId, String)>,
) -> Result<ProbeOutcome, String> {
    let local_values = values.unwrap_or(std::slice::from_ref(focus));
    match expr {
        ConstraintExpr::Cardinality { min, max, .. } => {
            let count = local_values.len() as u64;
            Ok(if min.is_some_and(|min| count < min) || max.is_some_and(|max| count > max) {
                ProbeOutcome::NonConformant
            } else {
                ProbeOutcome::Conformant
            })
        }
        ConstraintExpr::HasValue(expected) => Ok(
            if local_values.iter().any(|value| value == expected) {
                ProbeOutcome::Conformant
            } else {
                ProbeOutcome::NonConformant
            },
        ),
        ConstraintExpr::In(allowed) => Ok(
            if local_values.iter().all(|value| allowed.contains(value)) {
                ProbeOutcome::Conformant
            } else {
                ProbeOutcome::NonConformant
            },
        ),
        ConstraintExpr::Datatype(expected) => Ok(
            if local_values
                .iter()
                .all(|value| matches_datatype(value, expected))
            {
                ProbeOutcome::Conformant
            } else {
                ProbeOutcome::NonConformant
            },
        ),
        ConstraintExpr::NodeKind(expected) => Ok(
            if local_values
                .iter()
                .all(|value| matches_node_kind(value, expected))
            {
                ProbeOutcome::Conformant
            } else {
                ProbeOutcome::NonConformant
            },
        ),
        ConstraintExpr::Class(expected) => Ok(
            if local_values
                .iter()
                .all(|value| has_class(&state.index, value, expected))
            {
                ProbeOutcome::Conformant
            } else {
                ProbeOutcome::NonConformant
            },
        ),
        ConstraintExpr::StringConstraint {
            predicate,
            values: params,
        } => {
            if predicate.as_str() == "http://www.w3.org/ns/shacl#uniqueLang" {
                return Ok(if !unique_lang_enabled(params) || check_unique_lang(local_values).is_empty()
                {
                    ProbeOutcome::Conformant
                } else {
                    ProbeOutcome::NonConformant
                });
            }
            for value in local_values {
                match check_string_constraint(predicate.as_str(), params, value) {
                    Ok(Some(_)) => return Ok(ProbeOutcome::NonConformant),
                    Ok(None) => {}
                    Err(reason) => return Ok(ProbeOutcome::Unsupported(reason)),
                }
            }
            Ok(ProbeOutcome::Conformant)
        }
        ConstraintExpr::NumericRange { predicate, values } => {
            for value in local_values {
                match check_numeric_range(predicate.as_str(), values, value) {
                    Ok(Some(_)) => return Ok(ProbeOutcome::NonConformant),
                    Ok(None) => {}
                    Err(reason) => return Ok(ProbeOutcome::Unsupported(reason)),
                }
            }
            Ok(ProbeOutcome::Conformant)
        }
        ConstraintExpr::PropertyComparison { predicate, values } => {
            match eval_property_comparison(
                &state.index,
                predicate.as_str(),
                focus,
                local_values,
                values,
            ) {
                Ok(messages) if messages.is_empty() => Ok(ProbeOutcome::Conformant),
                Ok(_) => Ok(ProbeOutcome::NonConformant),
                Err(reason) => Ok(ProbeOutcome::Unsupported(reason)),
            }
        }
        ConstraintExpr::Closed { ignored_properties } => {
            match eval_closed_shape(shape_id, focus, ignored_properties, state) {
                Ok(messages) if messages.is_empty() => Ok(ProbeOutcome::Conformant),
                Ok(_) => Ok(ProbeOutcome::NonConformant),
                Err(reason) => Ok(ProbeOutcome::Unsupported(reason)),
            }
        }
        ConstraintExpr::NodeRef {
            shape: Some(target),
            ..
        } => probe_shape_conforms_inner(*target, focus, state, active),
        ConstraintExpr::PropertyRef {
            shape: Some(target),
            ..
        } => {
            for value in local_values {
                let nested_shape = state
                    .program
                    .shapes
                    .iter()
                    .find(|shape| shape.id == *target)
                    .ok_or_else(|| format!("unknown property ref shape {}", target.0))?;
                let nested_values = nested_shape
                    .path
                    .as_ref()
                    .map(|path| eval_path(&state.index, value, path))
                    .unwrap_or_default();
                match probe_property_shape(*target, value, &nested_values, state, active)? {
                    ProbeOutcome::Conformant => {}
                    other => return Ok(other),
                }
            }
            Ok(ProbeOutcome::Conformant)
        }
        ConstraintExpr::QualifiedValueShape {
            shape: Some(target),
            min_count,
            max_count,
            disjoint,
            ..
        } => {
            if disjoint == &Some(true) {
                return Ok(ProbeOutcome::Unsupported(
                    "qualifiedValueShapesDisjoint is not yet executable".to_string(),
                ));
            }
            let mut matching = 0u64;
            for value in local_values {
                match probe_shape_conforms_inner(*target, value, state, active)? {
                    ProbeOutcome::Conformant => matching += 1,
                    ProbeOutcome::NonConformant => {}
                    other => return Ok(other),
                }
            }
            Ok(
                if min_count.is_some_and(|min| matching < min)
                    || max_count.is_some_and(|max| matching > max)
                {
                    ProbeOutcome::NonConformant
                } else {
                    ProbeOutcome::Conformant
                },
            )
        }
        ConstraintExpr::Not {
            shape: Some(target), ..
        } => {
            for value in local_values {
                match probe_shape_conforms_inner(*target, value, state, active)? {
                    ProbeOutcome::Conformant => return Ok(ProbeOutcome::NonConformant),
                    ProbeOutcome::NonConformant => {}
                    other => return Ok(other),
                }
            }
            Ok(ProbeOutcome::Conformant)
        }
        ConstraintExpr::Logical { kind, shapes } => {
            for value in local_values {
                let mut conformant = 0usize;
                for target in shapes {
                    match probe_shape_conforms_inner(*target, value, state, active)? {
                        ProbeOutcome::Conformant => conformant += 1,
                        ProbeOutcome::NonConformant => {}
                        other => return Ok(other),
                    }
                }
                let passes = match kind {
                    LogicalKind::And => conformant == shapes.len(),
                    LogicalKind::Or => conformant >= 1,
                    LogicalKind::Xone => conformant == 1,
                };
                if !passes {
                    return Ok(ProbeOutcome::NonConformant);
                }
            }
            Ok(ProbeOutcome::Conformant)
        }
        ConstraintExpr::Sparql(_)
        | ConstraintExpr::CustomComponent { .. }
        | ConstraintExpr::GenericPredicate { .. } => Ok(ProbeOutcome::Unsupported(
            "constraint kind is not executable in the in-memory backend".to_string(),
        )),
        ConstraintExpr::NodeRef { shape: None, .. }
        | ConstraintExpr::PropertyRef { shape: None, .. }
        | ConstraintExpr::QualifiedValueShape { shape: None, .. }
        | ConstraintExpr::Not { shape: None, .. } => Ok(ProbeOutcome::Unsupported(
            "constraint references an unresolved shape".to_string(),
        )),
    }
}

fn record_violation(
    shape_id: ShapeId,
    constraint_id: ConstraintId,
    focus: &Term,
    value: Option<&Term>,
    message: String,
    state: &mut ExecutionState<'_>,
) {
    let shape_label = state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == shape_id)
        .map(|shape| shape.normalized_key.clone())
        .unwrap_or_else(|| shape_id.0.to_string());
    *state
        .heatmap
        .shape_violations
        .entry(shape_label)
        .or_insert(0) += 1;
    *state
        .heatmap
        .constraint_violations
        .entry(constraint_id.0.to_string())
        .or_insert(0) += 1;
    let (severity, source) = violation_metadata(state.program, shape_id, Some(constraint_id));
    state.violations.push(ValidationViolation {
        shape: shape_id,
        constraint: Some(constraint_id),
        focus_node: focus.to_string(),
        value_node: value.map(ToString::to_string),
        message,
        severity,
        source,
    });
}

fn record_unsupported(
    shape_id: ShapeId,
    constraint_id: Option<ConstraintId>,
    focus: Option<&Term>,
    kind: &'static str,
    reason: String,
    state: &mut ExecutionState<'_>,
) {
    state.coverage.unsupported_constraints += usize::from(constraint_id.is_some());
    *state
        .coverage
        .unsupported_by_kind
        .entry(kind.to_string())
        .or_insert(0) += 1;
    let (severity, source) = violation_metadata(state.program, shape_id, constraint_id);
    state.unsupported.push(ValidationUnsupported {
        shape: shape_id,
        constraint: constraint_id,
        focus_node: focus.map(ToString::to_string),
        reason,
        kind: kind.to_string(),
        severity,
        source,
    });
}

fn violation_metadata(
    program: &ShapeProgram,
    shape_id: ShapeId,
    constraint_id: Option<ConstraintId>,
) -> (Severity, Option<SourceRef>) {
    let shape = program.shapes.iter().find(|shape| shape.id == shape_id);
    let severity = shape
        .map(|shape| shape.severity.clone())
        .unwrap_or(Severity::Violation);
    let source = constraint_id
        .and_then(|id| {
            program
                .constraints
                .iter()
                .find(|constraint| constraint.id == id)
                .and_then(|constraint| constraint.provenance.first().cloned())
        })
        .or_else(|| shape.and_then(|shape| shape.provenance.first().cloned()));
    (severity, source)
}

fn constraint_kind_name(expr: &ConstraintExpr) -> &'static str {
    match expr {
        ConstraintExpr::NodeRef { .. } => "node_ref",
        ConstraintExpr::PropertyRef { .. } => "property_ref",
        ConstraintExpr::QualifiedValueShape { .. } => "qualified_value_shape",
        ConstraintExpr::Logical { .. } => "logical",
        ConstraintExpr::Not { .. } => "not",
        ConstraintExpr::Class(_) => "class",
        ConstraintExpr::Datatype(_) => "datatype",
        ConstraintExpr::NodeKind(_) => "node_kind",
        ConstraintExpr::Cardinality { .. } => "cardinality",
        ConstraintExpr::NumericRange { .. } => "numeric_range",
        ConstraintExpr::StringConstraint { .. } => "string_constraint",
        ConstraintExpr::PropertyComparison { .. } => "property_comparison",
        ConstraintExpr::Closed { .. } => "closed",
        ConstraintExpr::HasValue(_) => "has_value",
        ConstraintExpr::In(_) => "in",
        ConstraintExpr::Sparql(_) => "sparql",
        ConstraintExpr::CustomComponent { .. } => "custom_component",
        ConstraintExpr::GenericPredicate { .. } => "generic_predicate",
    }
}

fn matches_datatype(value: &Term, expected: &Term) -> bool {
    matches!(
        (value, expected),
        (Term::Literal(lit), Term::NamedNode(expected)) if lit.datatype() == expected.as_ref()
    )
}

fn matches_node_kind(value: &Term, expected: &Term) -> bool {
    match expected {
        Term::NamedNode(expected) if expected.as_str() == SH_IRI => {
            matches!(value, Term::NamedNode(_))
        }
        Term::NamedNode(expected) if expected.as_str() == SH_LITERAL => {
            matches!(value, Term::Literal(_))
        }
        Term::NamedNode(expected) if expected.as_str() == SH_BLANK_NODE => {
            matches!(value, Term::BlankNode(_))
        }
        Term::NamedNode(expected) if expected.as_str() == SH_BLANK_NODE_OR_IRI => {
            matches!(value, Term::BlankNode(_) | Term::NamedNode(_))
        }
        Term::NamedNode(expected) if expected.as_str() == SH_BLANK_NODE_OR_LITERAL => {
            matches!(value, Term::BlankNode(_) | Term::Literal(_))
        }
        Term::NamedNode(expected) if expected.as_str() == SH_IRI_OR_LITERAL => {
            matches!(value, Term::NamedNode(_) | Term::Literal(_))
        }
        _ => false,
    }
}

fn has_class(index: &DataIndex, value: &Term, expected: &Term) -> bool {
    index
        .types
        .get(&value.to_string())
        .map(|types| types.contains(&expected.to_string()))
        .unwrap_or(false)
}

fn check_string_constraint(
    predicate: &str,
    params: &[Term],
    value: &Term,
) -> Result<Option<String>, String> {
    let literal = match value {
        Term::Literal(lit) => lit,
        _ => return Ok(Some("value is not a literal".to_string())),
    };
    match predicate {
        "http://www.w3.org/ns/shacl#minLength" => {
            let Some(min) = params.first().and_then(literal_u64) else {
                return Err("minLength is missing an integer literal bound".to_string());
            };
            Ok((literal.value().chars().count() < min as usize)
                .then(|| format!("literal shorter than minimum length {min}")))
        }
        "http://www.w3.org/ns/shacl#maxLength" => {
            let Some(max) = params.first().and_then(literal_u64) else {
                return Err("maxLength is missing an integer literal bound".to_string());
            };
            Ok((literal.value().chars().count() > max as usize)
                .then(|| format!("literal longer than maximum length {max}")))
        }
        "http://www.w3.org/ns/shacl#pattern" => {
            let Some(needle) = params.first().and_then(term_string) else {
                return Err("pattern is missing a string literal".to_string());
            };
            Ok((!literal.value().contains(&needle))
                .then(|| format!("literal does not contain pattern {needle}")))
        }
        "http://www.w3.org/ns/shacl#languageIn" => {
            let ranges = params
                .iter()
                .filter_map(term_string)
                .collect::<Vec<_>>();
            if ranges.is_empty() {
                return Err("languageIn requires at least one language range".to_string());
            }
            let Some(language) = literal.language() else {
                return Ok(Some("literal is missing a language tag".to_string()));
            };
            let language_lower = language.to_ascii_lowercase();
            Ok((!ranges
                .iter()
                .any(|range| language_range_matches(range, &language_lower)))
            .then(|| format!("literal language tag {language} not allowed by languageIn")))
        }
        "http://www.w3.org/ns/shacl#uniqueLang" => {
            if literal.language().is_none() {
                return Ok(None);
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

fn check_numeric_range(
    predicate: &str,
    params: &[Term],
    value: &Term,
) -> Result<Option<String>, String> {
    let Some(bound) = params.first().and_then(term_number) else {
        return Err("numeric range is missing a numeric literal bound".to_string());
    };
    let Some(actual) = term_number(value) else {
        return Ok(Some("value is not a numeric literal".to_string()));
    };
    let violation = match predicate {
        "http://www.w3.org/ns/shacl#minExclusive" => (actual <= bound)
            .then(|| format!("numeric value {actual} is not greater than {bound}")),
        "http://www.w3.org/ns/shacl#minInclusive" => (actual < bound)
            .then(|| format!("numeric value {actual} is less than minimum {bound}")),
        "http://www.w3.org/ns/shacl#maxExclusive" => (actual >= bound)
            .then(|| format!("numeric value {actual} is not less than {bound}")),
        "http://www.w3.org/ns/shacl#maxInclusive" => (actual > bound)
            .then(|| format!("numeric value {actual} is greater than maximum {bound}")),
        _ => None,
    };
    Ok(violation)
}

fn eval_property_comparison(
    index: &DataIndex,
    predicate: &str,
    focus: &Term,
    local_values: &[Term],
    params: &[Term],
) -> Result<Vec<(Option<Term>, String)>, String> {
    let comparison_path = params
        .first()
        .and_then(named_target_predicate)
        .ok_or_else(|| "comparison constraint requires a named-node property path".to_string())?;
    let comparison_values = eval_path(index, focus, &PropertyPath::Predicate(comparison_path));
    let local_set: BTreeSet<_> = local_values.iter().map(ToString::to_string).collect();
    let comparison_set: BTreeSet<_> = comparison_values.iter().map(ToString::to_string).collect();
    match predicate {
        "http://www.w3.org/ns/shacl#equals" => {
            if local_set == comparison_set {
                Ok(Vec::new())
            } else {
                Ok(vec![(
                    None,
                    "property values do not equal values of comparison property".to_string(),
                )])
            }
        }
        "http://www.w3.org/ns/shacl#disjoint" => {
            let overlap = local_set.intersection(&comparison_set).next().cloned();
            Ok(overlap
                .into_iter()
                .map(|value| {
                    (
                        None,
                        format!("property values are not disjoint from comparison property at {value}"),
                    )
                })
                .collect())
        }
        "http://www.w3.org/ns/shacl#lessThan"
        | "http://www.w3.org/ns/shacl#lessThanOrEquals" => {
            let mut messages = Vec::new();
            for left in local_values {
                for right in &comparison_values {
                    let Some(ordering) = compare_terms(left, right) else {
                        return Err("lessThan comparison needs comparable literal or IRI terms".to_string());
                    };
                    let ok = if predicate.ends_with("lessThan") {
                        ordering == Ordering::Less
                    } else {
                        matches!(ordering, Ordering::Less | Ordering::Equal)
                    };
                    if !ok {
                        messages.push((
                            Some(left.clone()),
                            format!("value {} does not satisfy comparison against {}", left, right),
                        ));
                    }
                }
            }
            Ok(messages)
        }
        _ => Ok(Vec::new()),
    }
}

fn eval_closed_shape(
    shape_id: ShapeId,
    focus: &Term,
    ignored_properties: &[Term],
    state: &ExecutionState<'_>,
) -> Result<Vec<String>, String> {
    let shape = state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == shape_id)
        .ok_or_else(|| format!("unknown shape {}", shape_id.0))?;
    let mut allowed = BTreeSet::new();
    for property_shape_id in &shape.property_shapes {
        let property_shape = state
            .program
            .shapes
            .iter()
            .find(|shape| shape.id == *property_shape_id)
            .ok_or_else(|| format!("unknown property shape {}", property_shape_id.0))?;
        match property_shape.path.as_ref() {
            Some(PropertyPath::Predicate(predicate)) => {
                allowed.insert(predicate.as_str().to_string());
            }
            Some(_) => {
                return Err(
                    "closed shapes currently require direct predicate property paths".to_string(),
                )
            }
            None => {}
        }
    }
    for ignored in ignored_properties {
        let Some(predicate) = named_target_predicate(ignored) else {
            return Err("ignoredProperties must contain named-node predicates".to_string());
        };
        allowed.insert(predicate.as_str().to_string());
    }
    let mut violations = Vec::new();
    if let Some(edges) = state.index.outgoing.get(&focus.to_string()) {
        for (predicate, _) in edges {
            if !allowed.contains(predicate.as_str()) {
                violations.push(format!("predicate <{}> is not allowed by closed shape", predicate));
            }
        }
    }
    Ok(violations)
}

fn literal_u64(term: &Term) -> Option<u64> {
    match term {
        Term::Literal(lit) => lit.value().parse().ok(),
        _ => None,
    }
}

fn term_number(term: &Term) -> Option<f64> {
    match term {
        Term::Literal(lit) => lit.value().parse().ok(),
        _ => None,
    }
}

fn term_string(term: &Term) -> Option<String> {
    match term {
        Term::Literal(lit) => Some(lit.value().to_string()),
        Term::NamedNode(node) => Some(node.as_str().to_string()),
        _ => None,
    }
}

fn compare_terms(left: &Term, right: &Term) -> Option<Ordering> {
    match (left, right) {
        (Term::Literal(_), Term::Literal(_)) => {
            if let (Some(lhs), Some(rhs)) = (term_number(left), term_number(right)) {
                lhs.partial_cmp(&rhs)
            } else {
                term_string(left)?.partial_cmp(&term_string(right)?)
            }
        }
        (Term::NamedNode(lhs), Term::NamedNode(rhs)) => lhs.as_str().partial_cmp(rhs.as_str()),
        _ => None,
    }
}

fn subject_to_term(subject: &NamedOrBlankNode) -> Term {
    match subject {
        NamedOrBlankNode::NamedNode(node) => Term::NamedNode(node.clone()),
        NamedOrBlankNode::BlankNode(node) => Term::BlankNode(node.clone()),
    }
}

fn parse_term(value: &str) -> Option<Term> {
    if value.starts_with('<') && value.ends_with('>') {
        let iri = &value[1..value.len() - 1];
        NamedNode::new(iri).ok().map(Term::NamedNode)
    } else {
        None
    }
}

fn dedup_terms(values: Vec<Term>) -> Vec<Term> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for value in values {
        if seen.insert(value.to_string()) {
            out.push(value);
        }
    }
    out
}

fn named_target_predicate(term: &Term) -> Option<NamedNode> {
    match term {
        Term::NamedNode(node) => Some(node.clone()),
        _ => None,
    }
}

fn path_is_executable(path: &PropertyPath) -> bool {
    match path {
        PropertyPath::Predicate(_) => true,
        PropertyPath::Inverse(inner)
        | PropertyPath::ZeroOrMore(inner)
        | PropertyPath::OneOrMore(inner)
        | PropertyPath::ZeroOrOne(inner) => path_is_executable(inner),
        PropertyPath::Sequence(parts) | PropertyPath::Alternative(parts) => {
            parts.iter().all(path_is_executable)
        }
        PropertyPath::Unsupported(_) => false,
    }
}

fn path_label(path: &PropertyPath) -> String {
    match path {
        PropertyPath::Predicate(predicate) => format!("<{}>", predicate),
        PropertyPath::Inverse(inner) => format!("^{}", path_label(inner)),
        PropertyPath::Sequence(parts) => parts.iter().map(path_label).collect::<Vec<_>>().join("/"),
        PropertyPath::Alternative(parts) => {
            format!("({})", parts.iter().map(path_label).collect::<Vec<_>>().join("|"))
        }
        PropertyPath::ZeroOrMore(inner) => format!("({})*", path_label(inner)),
        PropertyPath::OneOrMore(inner) => format!("({})+", path_label(inner)),
        PropertyPath::ZeroOrOne(inner) => format!("({})?", path_label(inner)),
        PropertyPath::Unsupported(term) => format!("unsupported {}", term),
    }
}

fn eval_transitive_path(
    index: &DataIndex,
    focus: &Term,
    inner: &PropertyPath,
    include_focus: bool,
) -> Vec<Term> {
    let mut out = Vec::new();
    let mut queue = vec![focus.clone()];
    let mut seen = BTreeSet::new();
    if include_focus && seen.insert(focus.to_string()) {
        out.push(focus.clone());
    }
    while let Some(current) = queue.pop() {
        for next in eval_path(index, &current, inner) {
            let key = next.to_string();
            if seen.insert(key) {
                out.push(next.clone());
                queue.push(next);
            }
        }
    }
    out
}

fn language_range_matches(range: &str, language: &str) -> bool {
    let range = range.to_ascii_lowercase();
    if range == "*" {
        return true;
    }
    language == range
        || language
            .strip_prefix(&range)
            .is_some_and(|suffix| suffix.starts_with('-'))
}

fn unique_lang_enabled(params: &[Term]) -> bool {
    params
        .first()
        .and_then(term_bool)
        .unwrap_or(true)
}

fn check_unique_lang(values: &[Term]) -> Vec<(Option<Term>, String)> {
    let mut seen = BTreeSet::new();
    let mut violations = Vec::new();
    for value in values {
        let Term::Literal(literal) = value else {
            continue;
        };
        let Some(language) = literal.language() else {
            continue;
        };
        let language = language.to_ascii_lowercase();
        if !seen.insert(language.clone()) {
            violations.push((
                Some(value.clone()),
                format!("duplicate language tag {language} violates uniqueLang"),
            ));
        }
    }
    violations
}

fn term_bool(term: &Term) -> Option<bool> {
    match term {
        Term::Literal(lit) => match lit.value() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}
