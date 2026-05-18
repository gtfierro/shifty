use crate::algebra::{
    ConstraintExpr, ConstraintId, PropertyPath, ShapeId, ShapeKind, ShapeProgram, TargetExpr,
};
use crate::diagnostics::TraceEventSchema;
use crate::plan::{ValidationPlan, ValidationPlanNode};
use crate::source::ResolvedShapeSet;
use oxrdf::{NamedNode, NamedOrBlankNode, Quad, Term};
use serde::{Deserialize, Serialize};
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
                    let focuses = resolve_target(&state.index, &target.expr);
                    state.trace.push(ValidationTraceEvent {
                        event: TraceEventSchema::TargetResolutionStart,
                        shape: Some(*shape),
                        constraint: None,
                        focus_node: None,
                        message: format!("resolving target {}", target.id.0),
                    });
                    for focus in focuses {
                        state.focus_nodes_evaluated += 1;
                        eval_shape(*shape, &focus, &mut state)?;
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
            trace: state.trace,
            heatmap: state.heatmap,
        })
    }
}

struct ExecutionState<'a> {
    program: &'a ShapeProgram,
    index: DataIndex,
    violations: Vec<ValidationViolation>,
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
            trace: Vec::new(),
            heatmap: ValidationHeatmap::default(),
            active: HashSet::new(),
            focus_nodes_evaluated: 0,
        }
    }
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
) -> Result<(), String> {
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
            message: "skipping recursive re-entry".to_string(),
        });
        return Ok(());
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

    match shape.kind {
        ShapeKind::Node => eval_node_shape(shape_id, focus, state)?,
        ShapeKind::Property => {
            return Err(format!(
                "property shape {} requires a parent node context",
                shape.normalized_key
            ));
        }
    }

    state.trace.push(ValidationTraceEvent {
        event: TraceEventSchema::ExitShape,
        shape: Some(shape_id),
        constraint: None,
        focus_node: Some(focus.to_string()),
        message: format!("exit shape {}", shape.normalized_key),
    });
    state.active.remove(&key);
    Ok(())
}

fn eval_node_shape(
    shape_id: ShapeId,
    focus: &Term,
    state: &mut ExecutionState<'_>,
) -> Result<(), String> {
    let shape = state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == shape_id)
        .ok_or_else(|| format!("unknown shape {}", shape_id.0))?;
    for constraint_id in &shape.constraints {
        let constraint = state
            .program
            .constraints
            .iter()
            .find(|constraint| constraint.id == *constraint_id)
            .ok_or_else(|| format!("unknown constraint {}", constraint_id.0))?;
        eval_constraint(
            shape_id,
            constraint.id,
            &constraint.expr,
            focus,
            None,
            state,
        )?;
    }
    for property_shape_id in &shape.property_shapes {
        let property_shape = state
            .program
            .shapes
            .iter()
            .find(|shape| shape.id == *property_shape_id)
            .ok_or_else(|| format!("unknown property shape {}", property_shape_id.0))?;
        let values = property_shape
            .path
            .as_ref()
            .map(|path| eval_path(&state.index, focus, path))
            .unwrap_or_default();
        eval_property_shape(*property_shape_id, focus, &values, state)?;
    }
    Ok(())
}

fn eval_property_shape(
    shape_id: ShapeId,
    parent_focus: &Term,
    values: &[Term],
    state: &mut ExecutionState<'_>,
) -> Result<(), String> {
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
    for constraint_id in &shape.constraints {
        let constraint = state
            .program
            .constraints
            .iter()
            .find(|constraint| constraint.id == *constraint_id)
            .ok_or_else(|| format!("unknown constraint {}", constraint_id.0))?;
        eval_constraint(
            shape_id,
            constraint.id,
            &constraint.expr,
            parent_focus,
            Some(values),
            state,
        )?;
    }
    state.trace.push(ValidationTraceEvent {
        event: TraceEventSchema::ExitShape,
        shape: Some(shape_id),
        constraint: None,
        focus_node: Some(parent_focus.to_string()),
        message: format!("exit property shape {}", shape.normalized_key),
    });
    Ok(())
}

fn eval_constraint(
    shape_id: ShapeId,
    constraint_id: ConstraintId,
    expr: &ConstraintExpr,
    focus: &Term,
    values: Option<&[Term]>,
    state: &mut ExecutionState<'_>,
) -> Result<(), String> {
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
    let local_values = values.unwrap_or(std::slice::from_ref(focus));
    match expr {
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
        }
        ConstraintExpr::StringConstraint {
            predicate,
            values: params,
        } => {
            for value in local_values {
                if let Some(message) = check_string_constraint(predicate.as_str(), params, value) {
                    record_violation(shape_id, constraint_id, focus, Some(value), message, state);
                }
            }
        }
        ConstraintExpr::NodeRef {
            shape: Some(target),
            ..
        } => {
            for value in local_values {
                eval_shape(*target, value, state)?;
            }
        }
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
                eval_property_shape(*target, value, &nested_values, state)?;
            }
        }
        _ => {}
    }
    state.trace.push(ValidationTraceEvent {
        event: TraceEventSchema::ExitConstraint,
        shape: Some(shape_id),
        constraint: Some(constraint_id),
        focus_node: Some(focus.to_string()),
        message: format!("exit constraint {}", constraint_id.0),
    });
    Ok(())
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
        PropertyPath::ZeroOrMore(_) | PropertyPath::OneOrMore(_) | PropertyPath::Unsupported(_) => {
            Vec::new()
        }
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
    state.violations.push(ValidationViolation {
        shape: shape_id,
        constraint: Some(constraint_id),
        focus_node: focus.to_string(),
        value_node: value.map(ToString::to_string),
        message,
    });
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

fn check_string_constraint(predicate: &str, params: &[Term], value: &Term) -> Option<String> {
    let literal = match value {
        Term::Literal(lit) => lit,
        _ => return Some("value is not a literal".to_string()),
    };
    match predicate {
        "http://www.w3.org/ns/shacl#minLength" => {
            let min = params.first().and_then(literal_u64)?;
            (literal.value().chars().count() < min as usize)
                .then(|| format!("literal shorter than minimum length {min}"))
        }
        "http://www.w3.org/ns/shacl#maxLength" => {
            let max = params.first().and_then(literal_u64)?;
            (literal.value().chars().count() > max as usize)
                .then(|| format!("literal longer than maximum length {max}"))
        }
        "http://www.w3.org/ns/shacl#pattern" => {
            let needle = params.first().and_then(term_string)?;
            (!literal.value().contains(&needle))
                .then(|| format!("literal does not contain pattern {needle}"))
        }
        _ => None,
    }
}

fn literal_u64(term: &Term) -> Option<u64> {
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
