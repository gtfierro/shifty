use oxigraph::sparql::{QueryResults, SparqlEvaluator, Variable};
use oxigraph::store::Store;
use oxrdf::{NamedNode, NamedOrBlankNode, Quad, Term};
use oxsdatatypes::{Boolean, Date, DateTime, Decimal, Double, Float, Integer, Time};
use regex::Regex;
use shifty_shacl_core::algebra::{
    AdvancedTarget, ComponentDefId, Constraint, ConstraintExpr, ConstraintId, LogicalKind,
    PrefixDeclaration, PropertyPath, Rule, RuleExpr, Severity, Shape, ShapeId, ShapeKind,
    ShapeProgram, SparqlConstraint, SparqlValidator, TargetExpr, Template, TemplateBinding,
    TemplatePart, TemplateSlotKind, TriplePatternTerm,
};
use shifty_shacl_core::diagnostics::{SourceRef, TraceEventSchema};
use shifty_shacl_core::plan::{ValidationPlan, ValidationPlanNode};
use shifty_shacl_core::source::ResolvedShapeSet;
use shifty_shacl_core::{
    ValidationBackend, ValidationCoverage, ValidationHeatmap, ValidationResult,
    ValidationTraceEvent, ValidationUnsupported, ValidationViolation,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::str::FromStr;

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const SH_IRI: &str = "http://www.w3.org/ns/shacl#IRI";
const SH_LITERAL: &str = "http://www.w3.org/ns/shacl#Literal";
const SH_BLANK_NODE: &str = "http://www.w3.org/ns/shacl#BlankNode";
const SH_BLANK_NODE_OR_IRI: &str = "http://www.w3.org/ns/shacl#BlankNodeOrIRI";
const SH_BLANK_NODE_OR_LITERAL: &str = "http://www.w3.org/ns/shacl#BlankNodeOrLiteral";
const SH_IRI_OR_LITERAL: &str = "http://www.w3.org/ns/shacl#IRIOrLiteral";

#[derive(Debug, Clone, Default)]
pub struct InMemoryValidationBackend;

impl ValidationBackend for InMemoryValidationBackend {
    fn execute(
        &self,
        plan: &ValidationPlan,
        data: &ResolvedShapeSet,
    ) -> Result<ValidationResult, String> {
        let index = DataIndex::new(&data.quads);
        let store = build_query_store(data)?;
        let mut state = ExecutionState::new(&plan.view.program, index, store);
        execute_rules_to_fixed_point(&plan.executable_rules, &mut state)?;
        for node in &plan.nodes {
            if let ValidationPlanNode::TargetScan { shape, .. } = node {
                if state
                    .program
                    .shapes
                    .iter()
                    .find(|candidate| candidate.id == *shape)
                    .is_some_and(|candidate| candidate.deactivated)
                {
                    continue;
                }
                let targets = plan
                    .view
                    .program
                    .targets
                    .iter()
                    .filter(|target| target.owner == *shape)
                    .collect::<Vec<_>>();
                let mut focuses = Vec::new();
                for target in targets {
                    state.trace.push(ValidationTraceEvent {
                        event: TraceEventSchema::TargetResolutionStart,
                        shape: Some(*shape),
                        constraint: None,
                        focus_node: None,
                        message: format!("resolving target {}", target.id.0),
                    });
                    focuses.extend(resolve_target(*shape, &target.expr, &mut state)?);
                    state.trace.push(ValidationTraceEvent {
                        event: TraceEventSchema::TargetResolutionEnd,
                        shape: Some(*shape),
                        constraint: None,
                        focus_node: None,
                        message: format!("resolved target {}", target.id.0),
                    });
                }
                for focus in dedup_terms(focuses) {
                    state.focus_nodes_evaluated += 1;
                    let _ = eval_shape(*shape, &focus, &mut state)?;
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
    store: Store,
    violations: Vec<ValidationViolation>,
    unsupported: Vec<ValidationUnsupported>,
    coverage: ValidationCoverage,
    trace: Vec<ValidationTraceEvent>,
    heatmap: ValidationHeatmap,
    active: HashSet<(ShapeId, String)>,
    focus_nodes_evaluated: usize,
}

impl<'a> ExecutionState<'a> {
    fn new(program: &'a ShapeProgram, index: DataIndex, store: Store) -> Self {
        Self {
            program,
            index,
            store,
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
struct SelectViolation {
    value: Option<Term>,
    path: Option<Term>,
}

#[derive(Debug, Clone)]
struct DataIndex {
    outgoing: HashMap<String, Vec<(NamedNode, Term)>>,
    types: HashMap<String, BTreeSet<String>>,
    superclasses: HashMap<String, BTreeSet<String>>,
}

impl DataIndex {
    fn new(quads: &[Quad]) -> Self {
        let mut outgoing = HashMap::new();
        let mut types = HashMap::new();
        let mut superclasses = HashMap::new();
        for quad in quads {
            let subject = subject_to_term(&quad.subject);
            let subject_key = subject.to_string();
            outgoing
                .entry(subject_key.clone())
                .or_insert_with(Vec::new)
                .push((quad.predicate.clone(), quad.object.clone()));
            if quad.predicate.as_str() == RDF_TYPE {
                types
                    .entry(subject_key.clone())
                    .or_insert_with(BTreeSet::new)
                    .insert(quad.object.to_string());
            }
            if quad.predicate.as_str() == RDFS_SUBCLASS_OF {
                superclasses
                    .entry(subject_key)
                    .or_insert_with(BTreeSet::new)
                    .insert(quad.object.to_string());
            }
        }
        Self {
            outgoing,
            types,
            superclasses,
        }
    }

    fn insert_quad(&mut self, quad: &Quad) {
        let subject = subject_to_term(&quad.subject);
        let subject_key = subject.to_string();
        let edges = self.outgoing.entry(subject_key.clone()).or_default();
        let edge = (quad.predicate.clone(), quad.object.clone());
        if !edges.contains(&edge) {
            edges.push(edge);
        }
        if quad.predicate.as_str() == RDF_TYPE {
            self.types
                .entry(subject_key.clone())
                .or_default()
                .insert(quad.object.to_string());
        }
        if quad.predicate.as_str() == RDFS_SUBCLASS_OF {
            self.superclasses
                .entry(subject_key)
                .or_default()
                .insert(quad.object.to_string());
        }
    }
}

fn execute_rules_to_fixed_point(
    rules: &[Rule],
    state: &mut ExecutionState<'_>,
) -> Result<(), String> {
    if rules.is_empty() {
        return Ok(());
    }
    let max_iterations = 32;
    for iteration in 0..max_iterations {
        state.coverage.inference_iterations += 1;
        state.trace.push(ValidationTraceEvent {
            event: TraceEventSchema::InferenceIterationStart,
            shape: None,
            constraint: None,
            focus_node: None,
            message: format!("starting inference iteration {}", iteration + 1),
        });
        let mut new_triples = 0usize;
        for rule in rules {
            new_triples += execute_rule(rule, state)?;
        }
        state.trace.push(ValidationTraceEvent {
            event: TraceEventSchema::InferenceIterationEnd,
            shape: None,
            constraint: None,
            focus_node: None,
            message: format!(
                "finished inference iteration {} with {} inferred triples",
                iteration + 1,
                new_triples
            ),
        });
        if new_triples == 0 {
            return Ok(());
        }
    }
    Ok(())
}

fn execute_rule(rule: &Rule, state: &mut ExecutionState<'_>) -> Result<usize, String> {
    state.trace.push(ValidationTraceEvent {
        event: TraceEventSchema::EnterRule,
        shape: Some(rule.owner),
        constraint: None,
        focus_node: None,
        message: format!("enter rule {}", rule.id.0),
    });
    *state
        .heatmap
        .rule_hits
        .entry(rule.id.0.to_string())
        .or_insert(0) += 1;
    state.coverage.executed_rules += 1;

    let owner_shape = state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == rule.owner)
        .ok_or_else(|| format!("unknown rule owner shape {}", rule.owner.0))?;
    let mut focuses = Vec::new();
    for target_id in &owner_shape.targets {
        let target = state
            .program
            .targets
            .iter()
            .find(|target| target.id == *target_id)
            .ok_or_else(|| format!("unknown target {}", target_id.0))?;
        focuses.extend(resolve_target(rule.owner, &target.expr, state)?);
    }
    let focuses = dedup_terms(focuses);

    let mut inferred = 0usize;
    for focus in focuses {
        if !rule_conditions_hold(rule, &focus, state)? {
            continue;
        }
        inferred += match &rule.expr {
            RuleExpr::Triple {
                subject,
                predicate,
                object,
                ..
            } => execute_triple_rule(rule, subject, predicate.as_ref(), object, &focus, state)?,
            RuleExpr::Sparql {
                query,
                declarations,
                ..
            } => execute_sparql_rule(rule, query.as_deref(), declarations, &focus, state)?,
            RuleExpr::Generic { .. } => {
                record_unsupported(
                    rule.owner,
                    None,
                    Some(&focus),
                    "rule",
                    format!(
                        "generic AF rule {} is not executable in the in-memory backend",
                        rule.id.0
                    ),
                    state,
                );
                0
            }
        };
    }

    state.trace.push(ValidationTraceEvent {
        event: TraceEventSchema::ExitRule,
        shape: Some(rule.owner),
        constraint: None,
        focus_node: None,
        message: format!(
            "exit rule {} after inferring {} triples",
            rule.id.0, inferred
        ),
    });
    Ok(inferred)
}

fn rule_conditions_hold(
    rule: &Rule,
    focus: &Term,
    state: &ExecutionState<'_>,
) -> Result<bool, String> {
    let conditions = match &rule.expr {
        RuleExpr::Triple { conditions, .. }
        | RuleExpr::Sparql { conditions, .. }
        | RuleExpr::Generic { conditions, .. } => conditions,
    };
    if conditions.is_empty() {
        return Ok(true);
    }
    let mut active = state.active.clone();
    for condition in conditions {
        match probe_shape_conforms_inner(*condition, focus, state, &mut active)? {
            ProbeOutcome::Conformant => {}
            ProbeOutcome::NonConformant
            | ProbeOutcome::Unsupported(_)
            | ProbeOutcome::DeferredRecursion => return Ok(false),
        }
    }
    Ok(true)
}

fn execute_triple_rule(
    rule: &Rule,
    subject: &Option<TriplePatternTerm>,
    predicate: Option<&NamedNode>,
    object: &Option<TriplePatternTerm>,
    focus: &Term,
    state: &mut ExecutionState<'_>,
) -> Result<usize, String> {
    let Some(subject) = subject else {
        record_unsupported(
            rule.owner,
            None,
            Some(focus),
            "rule",
            format!("triple rule {} is missing sh:subject", rule.id.0),
            state,
        );
        return Ok(0);
    };
    let Some(predicate) = predicate else {
        record_unsupported(
            rule.owner,
            None,
            Some(focus),
            "rule",
            format!("triple rule {} is missing sh:predicate", rule.id.0),
            state,
        );
        return Ok(0);
    };
    let Some(object) = object else {
        record_unsupported(
            rule.owner,
            None,
            Some(focus),
            "rule",
            format!("triple rule {} is missing sh:object", rule.id.0),
            state,
        );
        return Ok(0);
    };

    let subjects = eval_triple_pattern_term(subject, focus, state)?;
    let objects = eval_triple_pattern_term(object, focus, state)?;
    let mut inferred = 0usize;
    for subject in subjects {
        let Some(subject) = term_to_subject(&subject) else {
            continue;
        };
        for object in &objects {
            inferred += insert_inferred_quad(
                Quad::new(
                    subject.clone(),
                    predicate.clone(),
                    object.clone(),
                    oxrdf::GraphName::DefaultGraph,
                ),
                state,
            )?;
        }
    }
    Ok(inferred)
}

fn execute_sparql_rule(
    rule: &Rule,
    query: Option<&str>,
    declarations: &[PrefixDeclaration],
    focus: &Term,
    state: &mut ExecutionState<'_>,
) -> Result<usize, String> {
    let Some(query) = query else {
        record_unsupported(
            rule.owner,
            None,
            Some(focus),
            "rule",
            format!("SPARQL rule {} is missing sh:construct", rule.id.0),
            state,
        );
        return Ok(0);
    };
    let owner_shape = state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == rule.owner)
        .ok_or_else(|| format!("unknown rule owner shape {}", rule.owner.0))?;
    let query = substitute_path_placeholder(query, owner_shape.path.as_ref())?;
    let full_query = with_prefixes(declarations, &query);
    reject_unsupported_query_variables(&full_query)?;
    let bindings = query_bindings(
        full_query.as_str(),
        focus,
        Some(&owner_shape.source),
        None,
        &[],
    );
    let graph_triples = match execute_prepared_query(state, &full_query, &bindings)? {
        QueryResults::Graph(triples) => Some(
            triples
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?,
        ),
        QueryResults::Boolean(_) | QueryResults::Solutions(_) => None,
    };
    match graph_triples {
        Some(triples) => {
            let mut inferred = 0usize;
            for triple in triples {
                inferred += insert_inferred_quad(
                    Quad::new(
                        triple.subject,
                        triple.predicate,
                        triple.object,
                        oxrdf::GraphName::DefaultGraph,
                    ),
                    state,
                )?;
            }
            Ok(inferred)
        }
        None => {
            record_unsupported(
                rule.owner,
                None,
                Some(focus),
                "rule",
                format!("SPARQL rule {} did not return a graph result", rule.id.0),
                state,
            );
            Ok(0)
        }
    }
}

fn eval_triple_pattern_term(
    term: &TriplePatternTerm,
    focus: &Term,
    state: &mut ExecutionState<'_>,
) -> Result<Vec<Term>, String> {
    match term {
        TriplePatternTerm::This => Ok(vec![focus.clone()]),
        TriplePatternTerm::Constant(term) => Ok(vec![term.clone()]),
        TriplePatternTerm::Path(path) => {
            if !path_is_executable(path) {
                return Err(format!(
                    "rule path {} is not executable in the in-memory backend",
                    path_label(path)
                ));
            }
            Ok(eval_path(&state.index, focus, path))
        }
    }
}

fn term_to_subject(term: &Term) -> Option<NamedOrBlankNode> {
    match term {
        Term::NamedNode(node) => Some(NamedOrBlankNode::NamedNode(node.clone())),
        Term::BlankNode(node) => Some(NamedOrBlankNode::BlankNode(node.clone())),
        _ => None,
    }
}

fn insert_inferred_quad(quad: Quad, state: &mut ExecutionState<'_>) -> Result<usize, String> {
    if state
        .store
        .contains(&quad)
        .map_err(|error| error.to_string())?
    {
        return Ok(0);
    }
    state
        .store
        .insert(&quad)
        .map_err(|error| error.to_string())?;
    state.index.insert_quad(&quad);
    state.coverage.inferred_triples += 1;
    Ok(1)
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
    if shape.deactivated {
        return Ok(ExecutionStatus::Completed);
    }
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
            let Some(path) = shape.path.as_ref() else {
                state.active.remove(&key);
                return Err(format!(
                    "property shape {} is missing sh:path",
                    shape.normalized_key
                ));
            };
            if !path_is_executable(path) {
                record_unsupported(
                    shape_id,
                    None,
                    Some(focus),
                    "property_path",
                    format!(
                        "property path {} is not executable in the in-memory backend",
                        path_label(path)
                    ),
                    state,
                );
                ExecutionStatus::Completed
            } else {
                let values = eval_path(&state.index, focus, path);
                eval_property_shape(shape_id, focus, &values, state)?
            }
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
        if eval_constraint(
            shape_id,
            constraint.id,
            &constraint.expr,
            focus,
            None,
            state,
        )? == ExecutionStatus::DeferredRecursion
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
        if property_shape.deactivated {
            continue;
        }
        if let Some(path) = property_shape.path.as_ref() {
            if !path_is_executable(path) {
                record_unsupported(
                    *property_shape_id,
                    None,
                    Some(focus),
                    "property_path",
                    format!(
                        "property path {} is not executable in the in-memory backend",
                        path_label(path)
                    ),
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
    if shape.deactivated {
        return Ok(ExecutionStatus::Completed);
    }
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
    for nested_property_shape_id in &shape.property_shapes {
        let nested_property_shape = state
            .program
            .shapes
            .iter()
            .find(|candidate| candidate.id == *nested_property_shape_id)
            .ok_or_else(|| format!("unknown property shape {}", nested_property_shape_id.0))?;
        if nested_property_shape.deactivated {
            continue;
        }
        let Some(path) = nested_property_shape.path.as_ref() else {
            continue;
        };
        if !path_is_executable(path) {
            record_unsupported(
                *nested_property_shape_id,
                None,
                Some(parent_focus),
                "property_path",
                format!(
                    "property path {} is not executable in the in-memory backend",
                    path_label(path)
                ),
                state,
            );
            continue;
        }
        for value in values {
            let nested_values = eval_path(&state.index, value, path);
            if eval_property_shape(*nested_property_shape_id, value, &nested_values, state)?
                == ExecutionStatus::DeferredRecursion
            {
                status = ExecutionStatus::DeferredRecursion;
            }
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
                        None,
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
                        None,
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
                        None,
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
                        None,
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
                            None,
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
                                None,
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
                        record_violation(
                            shape_id,
                            constraint_id,
                            focus,
                            Some(value),
                            None,
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
                            None,
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
                        record_violation(
                            shape_id,
                            constraint_id,
                            focus,
                            None,
                            None,
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
                        format!(
                            "deferred recursive validation while evaluating shape {}",
                            target.0
                        ),
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
                        None,
                        format!("qualified value shape count violation: count={matching}"),
                        state,
                    );
                }
                ExecutionStatus::Completed
            }
        }
        ConstraintExpr::Not {
            shape: Some(target),
            ..
        } => {
            for value in local_values {
                match probe_shape_conforms(*target, value, state)? {
                    ProbeOutcome::Conformant => record_violation(
                        shape_id,
                        constraint_id,
                        focus,
                        Some(value),
                        None,
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
                            format!(
                                "deferred recursive validation while probing not-shape {}",
                                target.0
                            ),
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
                        None,
                        format!(
                            "logical constraint {:?} failed with {} matching shapes",
                            kind, conformant
                        ),
                        state,
                    );
                }
            }
            ExecutionStatus::Completed
        }
        ConstraintExpr::Sparql(sparql) => {
            execute_sparql_constraint(shape_id, constraint_id, sparql, focus, local_values, state)?;
            ExecutionStatus::Completed
        }
        ConstraintExpr::CustomComponent {
            component,
            bindings,
            message_templates,
            label_template,
            ..
        } => {
            execute_custom_component_constraint(
                shape_id,
                constraint_id,
                *component,
                bindings,
                message_templates,
                label_template.as_ref(),
                focus,
                local_values,
                state,
            )?;
            ExecutionStatus::Completed
        }
        ConstraintExpr::GenericPredicate { .. } => {
            if matches!(expr, ConstraintExpr::GenericPredicate { predicate, .. } if predicate.as_str().starts_with("http://www.w3.org/ns/shacl#"))
            {
                record_unsupported(
                    shape_id,
                    Some(constraint_id),
                    Some(focus),
                    constraint_kind_name(expr),
                    "constraint kind is not executable in the in-memory backend".to_string(),
                    state,
                );
            }
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

fn resolve_target(
    owner: ShapeId,
    target: &TargetExpr,
    state: &mut ExecutionState<'_>,
) -> Result<Vec<Term>, String> {
    match target {
        TargetExpr::Node(term) => Ok(vec![term.clone()]),
        TargetExpr::Class(term) => Ok(state
            .index
            .types
            .iter()
            .filter(|(_, types)| {
                types.iter().any(|candidate| {
                    class_is_or_subclass_of(
                        candidate,
                        &term.to_string(),
                        &state.index.superclasses,
                        &mut HashSet::new(),
                    )
                })
            })
            .filter_map(|(subject, _)| parse_term_or_blank(subject))
            .collect()),
        TargetExpr::SubjectsOf(predicate) => Ok(named_target_predicate(predicate)
            .map(|predicate| {
                state
                    .index
                    .outgoing
                    .iter()
                    .filter(|(_, edges)| edges.iter().any(|(pred, _)| pred == &predicate))
                    .filter_map(|(subject, _)| parse_term(subject))
                    .collect()
            })
            .unwrap_or_default()),
        TargetExpr::ObjectsOf(predicate) => Ok(named_target_predicate(predicate)
            .map(|predicate| {
                state
                    .index
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
            .unwrap_or_default()),
        TargetExpr::Advanced(target) => resolve_advanced_target(owner, target, state),
    }
}

fn resolve_advanced_target(
    owner: ShapeId,
    target: &AdvancedTarget,
    state: &mut ExecutionState<'_>,
) -> Result<Vec<Term>, String> {
    let mut candidates = if let Some(select) = target.select.as_deref() {
        execute_target_select_query(select, &target.declarations, state)?
    } else if let Some(shape_id) = target.target_shape_id {
        resolve_shape_targets(shape_id, state, &mut HashSet::new())?
    } else {
        all_subject_terms(&state.index)
    };

    if let Some(filter_shape_id) = target.filter_shape_id {
        let mut filtered = Vec::new();
        for candidate in candidates {
            if matches!(
                probe_shape_conforms(filter_shape_id, &candidate, state)?,
                ProbeOutcome::Conformant
            ) {
                filtered.push(candidate);
            }
        }
        candidates = filtered;
    }

    if let Some(ask) = target.ask.as_deref() {
        let mut filtered = Vec::new();
        for candidate in candidates {
            if execute_target_ask_query(ask, &target.declarations, &candidate, owner, state)? {
                filtered.push(candidate);
            }
        }
        candidates = filtered;
    }

    Ok(dedup_terms(candidates))
}

fn resolve_shape_targets(
    shape_id: ShapeId,
    state: &mut ExecutionState<'_>,
    active: &mut HashSet<ShapeId>,
) -> Result<Vec<Term>, String> {
    if !active.insert(shape_id) {
        return Ok(Vec::new());
    }
    if state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == shape_id)
        .is_some_and(|shape| shape.deactivated)
    {
        active.remove(&shape_id);
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for target in state
        .program
        .targets
        .iter()
        .filter(|target| target.owner == shape_id)
    {
        out.extend(resolve_target(shape_id, &target.expr, state)?);
    }
    active.remove(&shape_id);
    Ok(dedup_terms(out))
}

fn execute_target_select_query(
    query: &str,
    declarations: &[PrefixDeclaration],
    state: &mut ExecutionState<'_>,
) -> Result<Vec<Term>, String> {
    let full_query = with_prefixes(declarations, query);
    reject_unsupported_query_variables(&full_query)?;
    let results = execute_prepared_query(state, &full_query, &[])?;
    extract_solution_terms(results, "this")
}

fn execute_target_ask_query(
    query: &str,
    declarations: &[PrefixDeclaration],
    focus: &Term,
    owner: ShapeId,
    state: &mut ExecutionState<'_>,
) -> Result<bool, String> {
    let full_query = with_prefixes(declarations, query);
    reject_unsupported_query_variables(&full_query)?;
    let current_shape = state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == owner)
        .map(|shape| shape.source.clone());
    let bindings = query_bindings(
        full_query.as_str(),
        focus,
        current_shape.as_ref(),
        None,
        &[],
    );
    match execute_prepared_query(state, &full_query, &bindings)? {
        QueryResults::Boolean(result) => Ok(result),
        QueryResults::Solutions(mut solutions) => {
            while let Some(solution) = solutions.next() {
                if solution
                    .map_err(|error| error.to_string())?
                    .get("this")
                    .is_some()
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        QueryResults::Graph(_) => Err("advanced target query returned a graph result".to_string()),
    }
}

fn execute_sparql_constraint(
    shape_id: ShapeId,
    constraint_id: ConstraintId,
    sparql: &SparqlConstraint,
    focus: &Term,
    local_values: &[Term],
    state: &mut ExecutionState<'_>,
) -> Result<(), String> {
    let query = sparql
        .select
        .as_deref()
        .or(sparql.ask.as_deref())
        .ok_or_else(|| "SPARQL constraint does not include sh:select or sh:ask".to_string())?;
    let owner_shape = state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == shape_id)
        .ok_or_else(|| format!("unknown shape {}", shape_id.0))?;
    let query = substitute_path_placeholder(query, owner_shape.path.as_ref())?;
    let full_query = with_prefixes(&sparql.declarations, &query);
    reject_unsupported_query_variables(&full_query)?;

    if sparql.ask.is_some() {
        if local_values.len() > 1 && query_mentions_var(&full_query, "value") {
            for value in local_values {
                let bindings = query_bindings(
                    full_query.as_str(),
                    focus,
                    Some(&owner_shape.source),
                    Some(value),
                    &[],
                );
                let conforms = execute_ask_or_select_probe(state, &full_query, &bindings)?;
                if !conforms {
                    record_violation(
                        shape_id,
                        constraint_id,
                        focus,
                        Some(value),
                        None,
                        first_message_literal(&sparql.messages)
                            .unwrap_or_else(|| "SPARQL ASK constraint failed".to_string()),
                        state,
                    );
                }
            }
        } else {
            let value_binding = if query_mentions_var(&full_query, "value") {
                local_values.first()
            } else {
                None
            };
            let bindings = query_bindings(
                full_query.as_str(),
                focus,
                Some(&owner_shape.source),
                value_binding,
                &[],
            );
            let conforms = execute_ask_or_select_probe(state, &full_query, &bindings)?;
            if !conforms {
                record_violation(
                    shape_id,
                    constraint_id,
                    focus,
                    None,
                    None,
                    first_message_literal(&sparql.messages)
                        .unwrap_or_else(|| "SPARQL ASK constraint failed".to_string()),
                    state,
                );
            }
        }
        return Ok(());
    }

    let bindings = query_bindings(
        full_query.as_str(),
        focus,
        Some(&owner_shape.source),
        None,
        &[],
    );
    let violations = execute_select_violations(state, &full_query, &bindings)?;
    if violations.is_empty() {
        return Ok(());
    }
    let message = first_message_literal(&sparql.messages)
        .unwrap_or_else(|| "SPARQL constraint failed".to_string());
    for violation in violations {
        record_violation(
            shape_id,
            constraint_id,
            focus,
            violation.value.as_ref(),
            violation.path.as_ref(),
            message.clone(),
            state,
        );
    }
    Ok(())
}

fn execute_custom_component_constraint(
    shape_id: ShapeId,
    constraint_id: ConstraintId,
    component_id: Option<ComponentDefId>,
    bindings: &[TemplateBinding],
    message_templates: &[Template],
    label_template: Option<&Template>,
    focus: &Term,
    local_values: &[Term],
    state: &mut ExecutionState<'_>,
) -> Result<(), String> {
    let Some(component_id) = component_id else {
        record_unsupported(
            shape_id,
            Some(constraint_id),
            Some(focus),
            "custom_component",
            "custom component attachment did not resolve to a definition".to_string(),
            state,
        );
        return Ok(());
    };
    let component = state
        .program
        .constraint_components
        .iter()
        .find(|component| component.id == component_id)
        .ok_or_else(|| format!("unknown component {}", component_id.0))?;
    if component.validators.is_empty() {
        record_unsupported(
            shape_id,
            Some(constraint_id),
            Some(focus),
            "custom_component",
            "custom component does not define a SPARQL validator".to_string(),
            state,
        );
        return Ok(());
    }
    let owner_shape = state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == shape_id)
        .ok_or_else(|| format!("unknown shape {}", shape_id.0))?;
    let binding_values = template_binding_values(bindings);
    for validator in &component.validators {
        execute_custom_component_validator(
            shape_id,
            constraint_id,
            owner_shape,
            validator,
            &binding_values,
            message_templates,
            label_template,
            focus,
            local_values,
            state,
        )?;
    }
    Ok(())
}

fn execute_custom_component_validator(
    shape_id: ShapeId,
    constraint_id: ConstraintId,
    owner_shape: &Shape,
    validator: &SparqlValidator,
    binding_values: &BTreeMap<String, Term>,
    message_templates: &[Template],
    label_template: Option<&Template>,
    focus: &Term,
    local_values: &[Term],
    state: &mut ExecutionState<'_>,
) -> Result<(), String> {
    let query = validator
        .select
        .as_deref()
        .or(validator.ask.as_deref())
        .ok_or_else(|| {
            "custom component validator does not provide sh:select or sh:ask".to_string()
        })?;
    let query = substitute_path_placeholder(query, owner_shape.path.as_ref())?;
    let full_query = with_prefixes(&validator.declarations, &query);
    reject_unsupported_query_variables(&full_query)?;
    let extra_bindings = component_query_bindings(full_query.as_str(), binding_values);
    let fallback_message = first_message_literal(&validator.messages)
        .or_else(|| render_templates(message_templates, binding_values))
        .or_else(|| label_template.map(|template| render_template(template, binding_values)))
        .unwrap_or_else(|| "custom component constraint failed".to_string());

    if validator.ask.is_some() {
        if local_values.len() > 1 && query_mentions_var(&full_query, "value") {
            for value in local_values {
                let bindings = query_bindings(
                    full_query.as_str(),
                    focus,
                    Some(&owner_shape.source),
                    Some(value),
                    &extra_bindings,
                );
                let conforms = execute_ask_or_select_probe(state, &full_query, &bindings)?;
                if !conforms {
                    record_violation(
                        shape_id,
                        constraint_id,
                        focus,
                        Some(value),
                        None,
                        fallback_message.clone(),
                        state,
                    );
                }
            }
        } else {
            let value_binding = if query_mentions_var(&full_query, "value") {
                local_values.first()
            } else {
                None
            };
            let bindings = query_bindings(
                full_query.as_str(),
                focus,
                Some(&owner_shape.source),
                value_binding,
                &extra_bindings,
            );
            let conforms = execute_ask_or_select_probe(state, &full_query, &bindings)?;
            if !conforms {
                record_violation(
                    shape_id,
                    constraint_id,
                    focus,
                    None,
                    None,
                    fallback_message,
                    state,
                );
            }
        }
        return Ok(());
    }

    let bindings = query_bindings(
        full_query.as_str(),
        focus,
        Some(&owner_shape.source),
        None,
        &extra_bindings,
    );
    let violations = execute_select_violations(state, &full_query, &bindings)?;
    if violations.is_empty() {
        return Ok(());
    }
    for value in violations {
        record_violation(
            shape_id,
            constraint_id,
            focus,
            value.value.as_ref(),
            value.path.as_ref(),
            fallback_message.clone(),
            state,
        );
    }
    Ok(())
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
        PropertyPath::Inverse(inner) => eval_inverse_path(index, focus, inner),
        PropertyPath::Sequence(parts) => {
            let mut current = vec![focus.clone()];
            for part in parts {
                let mut next = Vec::new();
                for term in &current {
                    next.extend(eval_path(index, term, part));
                }
                current = dedup_terms(next);
            }
            dedup_terms(current)
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
    let Some(shape) = state
        .program
        .shapes
        .iter()
        .find(|shape| shape.id == shape_id)
    else {
        return Err(format!("unknown shape {}", shape_id.0));
    };
    if shape.deactivated {
        return Ok(ProbeOutcome::Conformant);
    }
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
            if property_shape.deactivated {
                continue;
            }
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
            Ok(
                if min.is_some_and(|min| count < min) || max.is_some_and(|max| count > max) {
                    ProbeOutcome::NonConformant
                } else {
                    ProbeOutcome::Conformant
                },
            )
        }
        ConstraintExpr::HasValue(expected) => {
            Ok(if local_values.iter().any(|value| value == expected) {
                ProbeOutcome::Conformant
            } else {
                ProbeOutcome::NonConformant
            })
        }
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
                return Ok(
                    if !unique_lang_enabled(params) || check_unique_lang(local_values).is_empty() {
                        ProbeOutcome::Conformant
                    } else {
                        ProbeOutcome::NonConformant
                    },
                );
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
            shape: Some(target),
            ..
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
        ConstraintExpr::Sparql(_) | ConstraintExpr::CustomComponent { .. } => {
            Ok(ProbeOutcome::Unsupported(
                "constraint kind is not executable in the in-memory backend".to_string(),
            ))
        }
        ConstraintExpr::GenericPredicate { predicate, .. } => {
            if predicate
                .as_str()
                .starts_with("http://www.w3.org/ns/shacl#")
            {
                Ok(ProbeOutcome::Unsupported(
                    "constraint kind is not executable in the in-memory backend".to_string(),
                ))
            } else {
                Ok(ProbeOutcome::Conformant)
            }
        }
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
    result_path: Option<&Term>,
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
    let metadata = violation_metadata(state.program, shape_id, Some(constraint_id));
    state.violations.push(ValidationViolation {
        shape: shape_id,
        constraint: Some(constraint_id),
        focus: focus.clone(),
        focus_node: focus.to_string(),
        value: value.cloned(),
        value_node: value.map(ToString::to_string),
        result_path: result_path.cloned(),
        message,
        severity: metadata.severity,
        source: metadata.source,
        source_shape: metadata.source_shape,
        source_constraint: metadata.source_constraint,
        source_constraint_component: metadata.source_constraint_component,
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
    let metadata = violation_metadata(state.program, shape_id, constraint_id);
    state.unsupported.push(ValidationUnsupported {
        shape: shape_id,
        constraint: constraint_id,
        focus: focus.cloned(),
        focus_node: focus.map(ToString::to_string),
        reason,
        kind: kind.to_string(),
        severity: metadata.severity,
        source: metadata.source,
    });
}

#[derive(Debug, Clone)]
struct ViolationMetadata {
    severity: Severity,
    source: Option<SourceRef>,
    source_shape: Option<Term>,
    source_constraint: Option<Term>,
    source_constraint_component: Option<Term>,
}

fn violation_metadata(
    program: &ShapeProgram,
    shape_id: ShapeId,
    constraint_id: Option<ConstraintId>,
) -> ViolationMetadata {
    let shape = program.shapes.iter().find(|shape| shape.id == shape_id);
    let severity = shape
        .map(|shape| shape.severity.clone())
        .unwrap_or(Severity::Violation);
    let constraint = constraint_id.and_then(|id| {
        program
            .constraints
            .iter()
            .find(|constraint| constraint.id == id)
    });
    let source = constraint_id
        .and_then(|id| {
            program
                .constraints
                .iter()
                .find(|constraint| constraint.id == id)
                .and_then(|constraint| constraint.provenance.first().cloned())
        })
        .or_else(|| shape.and_then(|shape| shape.provenance.first().cloned()));
    ViolationMetadata {
        severity,
        source,
        source_shape: shape.map(|shape| shape.source.clone()),
        source_constraint: constraint.and_then(source_constraint_term),
        source_constraint_component: constraint
            .and_then(|constraint| source_constraint_component_term(program, &constraint.expr)),
    }
}

fn source_constraint_term(constraint: &Constraint) -> Option<Term> {
    match &constraint.expr {
        ConstraintExpr::Sparql(sparql) => Some(sparql.node.clone()),
        ConstraintExpr::CustomComponent { component: _, .. } => None,
        ConstraintExpr::NodeRef { source, .. }
        | ConstraintExpr::PropertyRef { source, .. }
        | ConstraintExpr::QualifiedValueShape { source, .. }
        | ConstraintExpr::Not { source, .. } => Some(source.clone()),
        _ => None,
    }
}

fn source_constraint_component_term(program: &ShapeProgram, expr: &ConstraintExpr) -> Option<Term> {
    let iri = match expr {
        ConstraintExpr::NodeRef { .. } => {
            Some("http://www.w3.org/ns/shacl#NodeConstraintComponent")
        }
        ConstraintExpr::PropertyRef { .. } => {
            Some("http://www.w3.org/ns/shacl#PropertyConstraintComponent")
        }
        ConstraintExpr::QualifiedValueShape {
            min_count,
            max_count,
            ..
        } => {
            if min_count.is_some() {
                Some("http://www.w3.org/ns/shacl#QualifiedMinCountConstraintComponent")
            } else if max_count.is_some() {
                Some("http://www.w3.org/ns/shacl#QualifiedMaxCountConstraintComponent")
            } else {
                None
            }
        }
        ConstraintExpr::Logical { kind, .. } => Some(match kind {
            LogicalKind::And => "http://www.w3.org/ns/shacl#AndConstraintComponent",
            LogicalKind::Or => "http://www.w3.org/ns/shacl#OrConstraintComponent",
            LogicalKind::Xone => "http://www.w3.org/ns/shacl#XoneConstraintComponent",
        }),
        ConstraintExpr::Not { .. } => Some("http://www.w3.org/ns/shacl#NotConstraintComponent"),
        ConstraintExpr::Class(_) => Some("http://www.w3.org/ns/shacl#ClassConstraintComponent"),
        ConstraintExpr::Datatype(_) => {
            Some("http://www.w3.org/ns/shacl#DatatypeConstraintComponent")
        }
        ConstraintExpr::NodeKind(_) => {
            Some("http://www.w3.org/ns/shacl#NodeKindConstraintComponent")
        }
        ConstraintExpr::Cardinality { predicate, .. } => match predicate.as_str() {
            "http://www.w3.org/ns/shacl#minCount" => {
                Some("http://www.w3.org/ns/shacl#MinCountConstraintComponent")
            }
            "http://www.w3.org/ns/shacl#maxCount" => {
                Some("http://www.w3.org/ns/shacl#MaxCountConstraintComponent")
            }
            _ => None,
        },
        ConstraintExpr::NumericRange { predicate, .. } => match predicate.as_str() {
            "http://www.w3.org/ns/shacl#minExclusive" => {
                Some("http://www.w3.org/ns/shacl#MinExclusiveConstraintComponent")
            }
            "http://www.w3.org/ns/shacl#minInclusive" => {
                Some("http://www.w3.org/ns/shacl#MinInclusiveConstraintComponent")
            }
            "http://www.w3.org/ns/shacl#maxExclusive" => {
                Some("http://www.w3.org/ns/shacl#MaxExclusiveConstraintComponent")
            }
            "http://www.w3.org/ns/shacl#maxInclusive" => {
                Some("http://www.w3.org/ns/shacl#MaxInclusiveConstraintComponent")
            }
            _ => None,
        },
        ConstraintExpr::StringConstraint { predicate, .. } => match predicate.as_str() {
            "http://www.w3.org/ns/shacl#minLength" => {
                Some("http://www.w3.org/ns/shacl#MinLengthConstraintComponent")
            }
            "http://www.w3.org/ns/shacl#maxLength" => {
                Some("http://www.w3.org/ns/shacl#MaxLengthConstraintComponent")
            }
            "http://www.w3.org/ns/shacl#pattern" => {
                Some("http://www.w3.org/ns/shacl#PatternConstraintComponent")
            }
            "http://www.w3.org/ns/shacl#languageIn" => {
                Some("http://www.w3.org/ns/shacl#LanguageInConstraintComponent")
            }
            "http://www.w3.org/ns/shacl#uniqueLang" => {
                Some("http://www.w3.org/ns/shacl#UniqueLangConstraintComponent")
            }
            _ => None,
        },
        ConstraintExpr::PropertyComparison { predicate, .. } => match predicate.as_str() {
            "http://www.w3.org/ns/shacl#equals" => {
                Some("http://www.w3.org/ns/shacl#EqualsConstraintComponent")
            }
            "http://www.w3.org/ns/shacl#disjoint" => {
                Some("http://www.w3.org/ns/shacl#DisjointConstraintComponent")
            }
            "http://www.w3.org/ns/shacl#lessThan" => {
                Some("http://www.w3.org/ns/shacl#LessThanConstraintComponent")
            }
            "http://www.w3.org/ns/shacl#lessThanOrEquals" => {
                Some("http://www.w3.org/ns/shacl#LessThanOrEqualsConstraintComponent")
            }
            _ => None,
        },
        ConstraintExpr::Closed { .. } => {
            Some("http://www.w3.org/ns/shacl#ClosedConstraintComponent")
        }
        ConstraintExpr::HasValue(_) => {
            Some("http://www.w3.org/ns/shacl#HasValueConstraintComponent")
        }
        ConstraintExpr::In(_) => Some("http://www.w3.org/ns/shacl#InConstraintComponent"),
        ConstraintExpr::Sparql(_) => Some("http://www.w3.org/ns/shacl#SPARQLConstraintComponent"),
        ConstraintExpr::CustomComponent {
            component,
            predicate,
            ..
        } => {
            if let Some(component_id) = component {
                return program
                    .constraint_components
                    .iter()
                    .find(|candidate| candidate.id == *component_id)
                    .map(|component| component.subject.clone());
            }
            return Some(Term::NamedNode(predicate.clone()));
        }
        ConstraintExpr::GenericPredicate { .. } => None,
    };
    iri.and_then(|iri| NamedNode::new(iri).ok().map(Term::NamedNode))
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
    matches!((value, expected), (Term::Literal(lit), Term::NamedNode(expected))
        if lit.datatype() == expected.as_ref() && literal_has_valid_lexical_form(&lit, expected.as_str()))
}

fn literal_has_valid_lexical_form(literal: &oxrdf::Literal, datatype_iri: &str) -> bool {
    let value = literal.value();
    match datatype_iri {
        "http://www.w3.org/2001/XMLSchema#string" => true,
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString" => literal.language().is_some(),
        "http://www.w3.org/2001/XMLSchema#boolean" => Boolean::from_str(value).is_ok(),
        "http://www.w3.org/2001/XMLSchema#decimal" => Decimal::from_str(value).is_ok(),
        "http://www.w3.org/2001/XMLSchema#integer" => Integer::from_str(value).is_ok(),
        "http://www.w3.org/2001/XMLSchema#long" => {
            parse_integer_in_range(value, i64::MIN as i128, i64::MAX as i128)
        }
        "http://www.w3.org/2001/XMLSchema#int" => {
            parse_integer_in_range(value, i32::MIN as i128, i32::MAX as i128)
        }
        "http://www.w3.org/2001/XMLSchema#short" => {
            parse_integer_in_range(value, i16::MIN as i128, i16::MAX as i128)
        }
        "http://www.w3.org/2001/XMLSchema#byte" => {
            parse_integer_in_range(value, i8::MIN as i128, i8::MAX as i128)
        }
        "http://www.w3.org/2001/XMLSchema#nonNegativeInteger" => {
            parse_integer_in_range(value, 0, i128::MAX)
        }
        "http://www.w3.org/2001/XMLSchema#positiveInteger" => {
            parse_integer_in_range(value, 1, i128::MAX)
        }
        "http://www.w3.org/2001/XMLSchema#nonPositiveInteger" => {
            parse_integer_in_range(value, i128::MIN, 0)
        }
        "http://www.w3.org/2001/XMLSchema#negativeInteger" => {
            parse_integer_in_range(value, i128::MIN, -1)
        }
        "http://www.w3.org/2001/XMLSchema#unsignedLong" => {
            parse_integer_in_range(value, 0, u64::MAX as i128)
        }
        "http://www.w3.org/2001/XMLSchema#unsignedInt" => {
            parse_integer_in_range(value, 0, u32::MAX as i128)
        }
        "http://www.w3.org/2001/XMLSchema#unsignedShort" => {
            parse_integer_in_range(value, 0, u16::MAX as i128)
        }
        "http://www.w3.org/2001/XMLSchema#unsignedByte" => {
            parse_integer_in_range(value, 0, u8::MAX as i128)
        }
        "http://www.w3.org/2001/XMLSchema#float" => Float::from_str(value).is_ok(),
        "http://www.w3.org/2001/XMLSchema#double" => Double::from_str(value).is_ok(),
        "http://www.w3.org/2001/XMLSchema#date" => Date::from_str(value).is_ok(),
        "http://www.w3.org/2001/XMLSchema#dateTime" => DateTime::from_str(value).is_ok(),
        "http://www.w3.org/2001/XMLSchema#time" => Time::from_str(value).is_ok(),
        _ => true,
    }
}

fn parse_integer_in_range(value: &str, min: i128, max: i128) -> bool {
    Integer::from_str(value)
        .ok()
        .and_then(|parsed| i128::from_str(&parsed.to_string()).ok())
        .is_some_and(|parsed| parsed >= min && parsed <= max)
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
    let expected = expected.to_string();
    index.types.get(&value.to_string()).is_some_and(|types| {
        types.iter().any(|candidate| {
            class_is_or_subclass_of(
                candidate,
                &expected,
                &index.superclasses,
                &mut HashSet::new(),
            )
        })
    })
}

fn class_is_or_subclass_of(
    candidate: &str,
    expected: &str,
    superclasses: &HashMap<String, BTreeSet<String>>,
    active: &mut HashSet<String>,
) -> bool {
    if candidate == expected {
        return true;
    }
    if !active.insert(candidate.to_string()) {
        return false;
    }
    let result = superclasses.get(candidate).is_some_and(|parents| {
        parents
            .iter()
            .any(|parent| class_is_or_subclass_of(parent, expected, superclasses, active))
    });
    active.remove(candidate);
    result
}

fn check_string_constraint(
    predicate: &str,
    params: &[Term],
    value: &Term,
) -> Result<Option<String>, String> {
    match predicate {
        "http://www.w3.org/ns/shacl#minLength" => {
            let Some(min) = params.first().and_then(literal_u64) else {
                return Err("minLength is missing an integer literal bound".to_string());
            };
            let Some(text) = string_constraint_text(value) else {
                return Ok(Some("value is a blank node".to_string()));
            };
            Ok((text.chars().count() < min as usize)
                .then(|| format!("literal shorter than minimum length {min}")))
        }
        "http://www.w3.org/ns/shacl#maxLength" => {
            let Some(max) = params.first().and_then(literal_u64) else {
                return Err("maxLength is missing an integer literal bound".to_string());
            };
            let Some(text) = string_constraint_text(value) else {
                return Ok(Some("value is a blank node".to_string()));
            };
            Ok((text.chars().count() > max as usize)
                .then(|| format!("literal longer than maximum length {max}")))
        }
        "http://www.w3.org/ns/shacl#pattern" => {
            let Some(needle) = params.first().and_then(term_string) else {
                return Err("pattern is missing a string literal".to_string());
            };
            let mut builder = regex::RegexBuilder::new(&needle);
            if params.iter().skip(1).filter_map(term_string).any(|flags| flags.contains('i')) {
                builder.case_insensitive(true);
            }
            let regex = builder
                .build()
                .map_err(|error| format!("pattern is not a valid regex: {error}"))?;
            let Some(text) = string_constraint_text(value) else {
                return Ok(Some("value is a blank node".to_string()));
            };
            Ok((!regex.is_match(&text))
                .then(|| format!("literal does not match pattern {needle}")))
        }
        "http://www.w3.org/ns/shacl#languageIn" => {
            let ranges = params.iter().filter_map(term_string).collect::<Vec<_>>();
            if ranges.is_empty() {
                return Err("languageIn requires at least one language range".to_string());
            }
            let Term::Literal(literal) = value else {
                return Ok(Some("literal is missing a language tag".to_string()));
            };
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
            let Term::Literal(literal) = value else {
                return Ok(None);
            };
            if literal.language().is_none() {
                return Ok(None);
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

fn string_constraint_text(value: &Term) -> Option<String> {
    match value {
        Term::Literal(literal) => Some(literal.value().to_string()),
        Term::NamedNode(node) => Some(node.as_str().to_string()),
        Term::BlankNode(_) => None,
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
    let violation =
        match predicate {
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
                        format!(
                            "property values are not disjoint from comparison property at {value}"
                        ),
                    )
                })
                .collect())
        }
        "http://www.w3.org/ns/shacl#lessThan" | "http://www.w3.org/ns/shacl#lessThanOrEquals" => {
            let mut messages = Vec::new();
            for left in local_values {
                for right in &comparison_values {
                    let Some(ordering) = compare_terms(left, right) else {
                        return Err(
                            "lessThan comparison needs comparable literal or IRI terms".to_string()
                        );
                    };
                    let ok = if predicate.ends_with("lessThan") {
                        ordering == Ordering::Less
                    } else {
                        matches!(ordering, Ordering::Less | Ordering::Equal)
                    };
                    if !ok {
                        messages.push((
                            Some(left.clone()),
                            format!(
                                "value {} does not satisfy comparison against {}",
                                left, right
                            ),
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
                );
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
                violations.push(format!(
                    "predicate <{}> is not allowed by closed shape",
                    predicate
                ));
            }
        }
    }
    Ok(violations)
}

fn build_query_store(data: &ResolvedShapeSet) -> Result<Store, String> {
    let store = Store::new().map_err(|error| error.to_string())?;
    for quad in &data.quads {
        store.insert(quad).map_err(|error| error.to_string())?;
    }
    Ok(store)
}

fn execute_prepared_query<'a>(
    state: &'a ExecutionState<'_>,
    query: &str,
    bindings: &[(Variable, Term)],
) -> Result<QueryResults<'a>, String> {
    let mut rewritten = query.to_string();
    let mut remaining = bindings.to_vec();
    loop {
        let mut prepared = SparqlEvaluator::new()
            .parse_query(&rewritten)
            .map_err(|error| format!("Failed to parse SPARQL query: {error}"))?;
        prepared.dataset_mut().set_default_graph_as_union();
        let mut bound = prepared.on_store(&state.store);
        for (variable, term) in &remaining {
            bound = bound.substitute_variable(variable.clone(), term.clone());
        }
        match bound.execute() {
            Ok(results) => return Ok(results),
            Err(error) => {
                let message = error.to_string();
                let Some(var_name) = extract_non_projected_variable(&message) else {
                    return Err(message);
                };
                let Some(index) = remaining
                    .iter()
                    .position(|(variable, _)| variable.as_str() == var_name)
                else {
                    return Err(message);
                };
                let (_, term) = remaining.remove(index);
                rewritten = apply_textual_bindings(
                    &rewritten,
                    &[(Variable::new_unchecked(var_name), term)],
                );
            }
        }
    }
}

fn execute_ask_or_select_probe(
    state: &ExecutionState<'_>,
    query: &str,
    bindings: &[(Variable, Term)],
) -> Result<bool, String> {
    match execute_prepared_query(state, query, bindings)? {
        QueryResults::Boolean(result) => Ok(result),
        QueryResults::Solutions(mut solutions) => Ok(solutions
            .next()
            .transpose()
            .map_err(|error| error.to_string())?
            .is_some()),
        QueryResults::Graph(_) => Err("SPARQL query returned a graph result".to_string()),
    }
}

fn execute_select_violations(
    state: &ExecutionState<'_>,
    query: &str,
    bindings: &[(Variable, Term)],
) -> Result<Vec<SelectViolation>, String> {
    match execute_prepared_query(state, query, bindings)? {
        QueryResults::Solutions(solutions) => {
            let mut out = Vec::new();
            for solution in solutions {
                let solution = solution.map_err(|error| error.to_string())?;
                out.push(SelectViolation {
                    value: solution.get("value").cloned(),
                    path: solution.get("path").cloned(),
                });
            }
            Ok(out)
        }
        QueryResults::Boolean(result) => Ok(if result {
            Vec::new()
        } else {
            vec![SelectViolation {
                value: None,
                path: None,
            }]
        }),
        QueryResults::Graph(_) => Err("SPARQL query returned a graph result".to_string()),
    }
}

fn extract_solution_terms(results: QueryResults<'_>, variable: &str) -> Result<Vec<Term>, String> {
    match results {
        QueryResults::Solutions(solutions) => {
            let mut out = Vec::new();
            for solution in solutions {
                let solution = solution.map_err(|error| error.to_string())?;
                if let Some(term) = solution.get(variable) {
                    out.push(term.clone());
                }
            }
            Ok(out)
        }
        QueryResults::Boolean(result) => Ok(if result { Vec::new() } else { Vec::new() }),
        QueryResults::Graph(_) => Err("SPARQL query returned a graph result".to_string()),
    }
}

fn all_subject_terms(index: &DataIndex) -> Vec<Term> {
    let mut keys = index.outgoing.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    keys.into_iter()
        .filter_map(|key| parse_term_or_blank(&key))
        .collect()
}

fn with_prefixes(declarations: &[PrefixDeclaration], query: &str) -> String {
    let prefixes = declarations
        .iter()
        .filter_map(|declaration| {
            Some(format!(
                "PREFIX {}: <{}>",
                declaration.prefix.as_deref()?,
                term_string(declaration.namespace.as_ref()?)?
            ))
        })
        .collect::<Vec<_>>();
    if prefixes.is_empty() {
        query.to_string()
    } else {
        format!("{}\n{}", prefixes.join("\n"), query)
    }
}

fn substitute_path_placeholder(query: &str, path: Option<&PropertyPath>) -> Result<String, String> {
    if !query.contains("$PATH") {
        return Ok(query.to_string());
    }
    let Some(path) = path else {
        return Err("SPARQL query uses $PATH without a property path".to_string());
    };
    Ok(query.replace("$PATH", &sparql_path(path)?))
}

fn query_bindings(
    query: &str,
    focus: &Term,
    current_shape: Option<&Term>,
    value: Option<&Term>,
    extra: &[(Variable, Term)],
) -> Vec<(Variable, Term)> {
    let mut bindings = Vec::new();
    if query_mentions_var(query, "this") {
        bindings.push((Variable::new_unchecked("this"), focus.clone()));
    }
    if query_mentions_var(query, "currentShape") {
        if let Some(current_shape) = current_shape {
            bindings.push((
                Variable::new_unchecked("currentShape"),
                current_shape.clone(),
            ));
        }
    }
    if let Some(value) = value
        && query_mentions_var(query, "value")
    {
        bindings.push((Variable::new_unchecked("value"), value.clone()));
    }
    bindings.extend_from_slice(extra);
    bindings
}

fn component_query_bindings(
    query: &str,
    binding_values: &BTreeMap<String, Term>,
) -> Vec<(Variable, Term)> {
    let mut bindings = Vec::new();
    let mut names = binding_values.keys().cloned().collect::<Vec<_>>();
    names.sort();
    for name in names {
        if query_mentions_var(query, &name)
            && let Some(value) = binding_values.get(&name)
        {
            bindings.push((Variable::new_unchecked(name), value.clone()));
        }
    }
    bindings
}

fn first_message_literal(messages: &[Term]) -> Option<String> {
    messages.first().and_then(term_string)
}

fn template_binding_values(bindings: &[TemplateBinding]) -> BTreeMap<String, Term> {
    let mut out = BTreeMap::new();
    for binding in bindings {
        if let Some(value) = binding.values.first() {
            out.insert(binding.name.clone(), value.clone());
        }
    }
    out
}

fn render_templates(templates: &[Template], bindings: &BTreeMap<String, Term>) -> Option<String> {
    templates
        .first()
        .map(|template| render_template(template, bindings))
}

fn render_template(template: &Template, bindings: &BTreeMap<String, Term>) -> String {
    let mut out = String::new();
    for part in &template.parts {
        match part {
            TemplatePart::Text(text) => out.push_str(text),
            TemplatePart::Slot {
                kind: TemplateSlotKind::Variable | TemplateSlotKind::Parameter,
                name,
            } => out.push_str(
                bindings
                    .get(name)
                    .and_then(term_string)
                    .as_deref()
                    .unwrap_or(""),
            ),
        }
    }
    out
}

fn query_mentions_var(query: &str, var: &str) -> bool {
    query.contains(&format!("?{var}")) || query.contains(&format!("${var}"))
}

fn apply_textual_bindings(query: &str, bindings: &[(Variable, Term)]) -> String {
    let mut rendered = query.to_string();
    for (variable, term) in bindings {
        let escaped = regex::escape(variable.as_str());
        let pattern = Regex::new(&format!(r"(?P<prefix>[\?\$]){}(?P<suffix>\b)", escaped))
            .expect("binding regex should compile");
        let replacement = format_term_for_sparql(term);
        rendered = pattern
            .replace_all(&rendered, replacement.as_str())
            .to_string();
    }
    rendered
}

fn extract_non_projected_variable(message: &str) -> Option<&str> {
    let marker = "variable ?";
    let start = message.find(marker)? + marker.len();
    let end = message[start..]
        .find(|ch: char| ch.is_whitespace())
        .map(|offset| start + offset)
        .unwrap_or(message.len());
    Some(&message[start..end])
}

fn format_term_for_sparql(term: &Term) -> String {
    match term {
        Term::NamedNode(node) => format!("<{}>", node.as_str()),
        Term::BlankNode(node) => format!("_:{}", node.as_str()),
        Term::Literal(literal) => {
            let value = literal.value().replace('\\', "\\\\").replace('"', "\\\"");
            if let Some(language) = literal.language() {
                format!("\"{}\"@{}", value, language)
            } else if literal.datatype().as_str() != "http://www.w3.org/2001/XMLSchema#string" {
                format!("\"{}\"^^<{}>", value, literal.datatype().as_str())
            } else {
                format!("\"{}\"", value)
            }
        }
    }
}

fn reject_unsupported_query_variables(query: &str) -> Result<(), String> {
    if query_mentions_var(query, "shapesGraph") {
        return Err(
            "SPARQL execution with ?shapesGraph/$shapesGraph is not yet supported".to_string(),
        );
    }
    Ok(())
}

fn sparql_path(path: &PropertyPath) -> Result<String, String> {
    match path {
        PropertyPath::Predicate(predicate) => Ok(format!("<{}>", predicate.as_str())),
        PropertyPath::Inverse(inner) => Ok(format!("^{}", sparql_path(inner)?)),
        PropertyPath::Sequence(parts) => Ok(parts
            .iter()
            .map(sparql_path)
            .collect::<Result<Vec<_>, _>>()?
            .join("/")),
        PropertyPath::Alternative(parts) => Ok(format!(
            "({})",
            parts
                .iter()
                .map(sparql_path)
                .collect::<Result<Vec<_>, _>>()?
                .join("|")
        )),
        PropertyPath::ZeroOrMore(inner) => Ok(format!("({})*", sparql_path(inner)?)),
        PropertyPath::OneOrMore(inner) => Ok(format!("({})+", sparql_path(inner)?)),
        PropertyPath::ZeroOrOne(inner) => Ok(format!("({})?", sparql_path(inner)?)),
        PropertyPath::Unsupported(term) => Err(format!("unsupported SPARQL path form {term}")),
    }
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

fn parse_term_or_blank(value: &str) -> Option<Term> {
    if let Some(term) = parse_term(value) {
        return Some(term);
    }
    value
        .strip_prefix("_:")
        .and_then(|id| oxrdf::BlankNode::new(id).ok())
        .map(Term::BlankNode)
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
            format!(
                "({})",
                parts.iter().map(path_label).collect::<Vec<_>>().join("|")
            )
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

fn eval_inverse_path(index: &DataIndex, focus: &Term, inner: &PropertyPath) -> Vec<Term> {
    let focus_key = focus.to_string();
    let mut candidates = index.outgoing.keys().cloned().collect::<Vec<_>>();
    candidates.sort();
    let mut out = Vec::new();
    for candidate in candidates {
        let Some(candidate_term) = parse_term_or_blank(&candidate) else {
            continue;
        };
        if eval_path(index, &candidate_term, inner)
            .iter()
            .any(|term| term.to_string() == focus_key)
        {
            out.push(candidate_term);
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
        .map(|term| matches!(term, Term::Literal(lit) if lit.value() == "true"))
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
