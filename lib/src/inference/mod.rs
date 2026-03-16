//! SHACL rules inference engine.
//!
//! Evaluates SHACL rules (triple rules and SPARQL rules) over the data graph,
//! emitting inferred triples into a separate inference graph.

use crate::backend::Binding;
use crate::context::{Context, SourceShape, ValidationContext};
use crate::model::rules::{Rule, RuleCondition, SparqlRule, TriplePatternTerm, TripleRule};
use crate::runtime::component::{check_conformance_for_node, ConformanceReport};
use crate::trace::TraceEvent;
use crate::types::{ComponentID, PropShapeID, RuleID, TargetEvalExt, TraceItem, ID};
use log::{debug, info};
use oxigraph::model::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use oxigraph::sparql::{QueryResults, Variable};
use rayon::prelude::*;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, OnceLock};

type CandidateTriple = (Term, NamedNode, Term);
type CandidateBatches = Result<Vec<Vec<CandidateTriple>>, InferenceError>;

/// Configuration options governing inference execution.
#[derive(Debug, Clone)]
pub struct InferenceConfig {
    pub min_iterations: usize,
    pub max_iterations: usize,
    pub run_until_converged: bool,
    pub error_on_blank_nodes: bool,
    pub trace: bool,
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            min_iterations: 1,
            max_iterations: 8,
            run_until_converged: true,
            error_on_blank_nodes: false,
            trace: false,
        }
    }
}

/// Summary statistics describing an inference run.
#[derive(Debug, Clone)]
pub struct InferenceOutcome {
    pub iterations_executed: usize,
    pub triples_added: usize,
    pub converged: bool,
    pub inferred_quads: Vec<Quad>,
}

#[derive(Debug)]
pub struct BlankNodeProducedError {
    rule_id: RuleID,
    subject: Term,
    predicate: NamedNode,
    object: Term,
}

impl BlankNodeProducedError {
    fn new(rule_id: RuleID, subject: Term, predicate: NamedNode, object: Term) -> Self {
        Self {
            rule_id,
            subject,
            predicate,
            object,
        }
    }
}

/// Errors that can arise during inference.
#[derive(Debug)]
pub enum InferenceError {
    BlankNodeProduced(Box<BlankNodeProducedError>),
    RuleExecution {
        rule_id: RuleID,
        message: String,
    },
    TargetResolution {
        shape_id: ID,
        message: String,
    },
    PropertyShapeTargetResolution {
        shape_id: PropShapeID,
        message: String,
    },
    Configuration(String),
}

impl InferenceError {
    fn blank_node_produced(
        rule_id: RuleID,
        subject: Term,
        predicate: NamedNode,
        object: Term,
    ) -> Self {
        Self::BlankNodeProduced(Box::new(BlankNodeProducedError::new(
            rule_id, subject, predicate, object,
        )))
    }
}

impl fmt::Display for InferenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InferenceError::BlankNodeProduced(error) => write!(
                f,
                "Rule {} produced a blank node triple ({}, {}, {}) which is disallowed by configuration",
                error.rule_id, error.subject, error.predicate, error.object
            ),
            InferenceError::RuleExecution { rule_id, message } => {
                write!(f, "Rule {} failed to execute: {}", rule_id, message)
            }
            InferenceError::TargetResolution { shape_id, message } => {
                write!(f, "Failed to resolve targets for shape {}: {}", shape_id, message)
            }
            InferenceError::PropertyShapeTargetResolution { shape_id, message } => {
                write!(
                    f,
                    "Failed to resolve targets for property shape {}: {}",
                    shape_id, message
                )
            }
            InferenceError::Configuration(message) => f.write_str(message),
        }
    }
}

impl Error for InferenceError {}

/// Builds a graph describing relationships between shapes, components, and rules.
#[derive(Debug, Clone)]
pub struct InferenceGraph {
    pub node_shape_components: HashMap<ID, Vec<ComponentID>>,
    pub property_shape_components: HashMap<PropShapeID, Vec<ComponentID>>,
    pub node_shape_rules: HashMap<ID, Vec<RuleID>>,
    pub property_shape_rules: HashMap<PropShapeID, Vec<RuleID>>,
    pub rules: HashMap<RuleID, Rule>,
}

impl InferenceGraph {
    fn from_context(context: &ValidationContext) -> Self {
        let ir = context.shape_ir();
        let node_shape_components = ir
            .node_shapes
            .iter()
            .map(|shape| (shape.id, shape.constraints.clone()))
            .collect();

        let property_shape_components = ir
            .property_shapes
            .iter()
            .map(|shape| (shape.id, shape.constraints.clone()))
            .collect();

        Self {
            node_shape_components,
            property_shape_components,
            node_shape_rules: ir.node_shape_rules.clone(),
            property_shape_rules: ir.prop_shape_rules.clone(),
            rules: ir.rules.clone(),
        }
    }
}

/// Executes rule-based inference against a `ValidationContext`.
pub struct InferenceEngine<'a> {
    context: &'a ValidationContext,
    config: InferenceConfig,
    graph: InferenceGraph,
    plans: Vec<RulePlan>,
    dependency_index: HashMap<FocusDependency, Vec<usize>>,
    always_run_plans: Vec<usize>,
}

impl<'a> InferenceEngine<'a> {
    pub fn new(
        context: &'a ValidationContext,
        config: InferenceConfig,
    ) -> Result<Self, InferenceError> {
        let engine = Self {
            context,
            config,
            graph: InferenceGraph::from_context(context),
            plans: Vec::new(),
            dependency_index: HashMap::new(),
            always_run_plans: Vec::new(),
        };
        let plans = engine.build_rule_plans()?;
        let (dependency_index, always_run_plans) = build_dependency_index(&plans);
        let engine = Self {
            plans,
            dependency_index,
            always_run_plans,
            ..engine
        };
        engine.validate_config()?;
        Ok(engine)
    }

    pub fn run(&self) -> Result<InferenceOutcome, InferenceError> {
        self.validate_config()?;
        if self.graph.node_shape_rules.is_empty() && self.graph.property_shape_rules.is_empty() {
            return Ok(InferenceOutcome {
                iterations_executed: 0,
                triples_added: 0,
                converged: true,
                inferred_quads: Vec::new(),
            });
        }

        let mut total_added = 0usize;
        let mut iterations_executed = 0usize;
        let mut converged = false;
        let mut inferred_quads = Vec::new();
        let mut delta = DeltaIndex::initial();

        if self.config.trace {
            info!(
                "Starting inference: {} node-shape rule set(s), {} property-shape rule set(s)",
                self.graph.node_shape_rules.len(),
                self.graph.property_shape_rules.len()
            );
        }

        for iteration in 1..=self.config.max_iterations {
            iterations_executed = iteration;
            let added_this_round = self.apply_rules_for_delta(&delta, &mut inferred_quads)?;
            total_added += added_this_round;
            if self.config.trace {
                info!(
                    "Inference iteration {} added {} triple(s)",
                    iteration, added_this_round
                );
            }
            delta =
                DeltaIndex::from_quads(&inferred_quads[inferred_quads.len() - added_this_round..]);

            let reached_min = iteration >= self.config.min_iterations;
            if added_this_round == 0 {
                if reached_min {
                    converged = true;
                    if self.config.run_until_converged {
                        break;
                    }
                }
            } else {
                converged = false;
            }

            if iteration == self.config.max_iterations {
                converged = added_this_round == 0;
            }
        }

        if self.config.trace {
            info!(
                "Inference finished after {} iteration(s); triples added={}; converged={}",
                iterations_executed, total_added, converged
            );
        }

        Ok(InferenceOutcome {
            iterations_executed,
            triples_added: total_added,
            converged,
            inferred_quads,
        })
    }

    fn validate_config(&self) -> Result<(), InferenceError> {
        if self.config.min_iterations == 0 {
            return Err(InferenceError::Configuration(
                "Inference min_iterations must be at least 1".to_string(),
            ));
        }
        if self.config.max_iterations == 0 {
            return Err(InferenceError::Configuration(
                "Inference max_iterations must be at least 1".to_string(),
            ));
        }
        if self.config.max_iterations < self.config.min_iterations {
            return Err(InferenceError::Configuration(
                "Inference max_iterations must be greater than or equal to min_iterations"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn build_rule_plans(&self) -> Result<Vec<RulePlan>, InferenceError> {
        let mut plans = Vec::new();

        for (shape_id, rule_ids) in &self.graph.node_shape_rules {
            let shape_ir = self
                .context
                .shape_ir()
                .node_shapes
                .iter()
                .find(|s| &s.id == shape_id)
                .ok_or_else(|| {
                    InferenceError::Configuration(format!(
                        "Node shape {:?} referenced in rules but missing from IR",
                        shape_id
                    ))
                })?;
            for rule_id in rule_ids {
                let rule = self.graph.rules.get(rule_id).ok_or_else(|| {
                    InferenceError::Configuration(format!(
                        "Rule {:?} referenced by shape {:?} but missing from model",
                        rule_id, shape_id
                    ))
                })?;
                if rule.is_deactivated() {
                    continue;
                }
                plans.push(RulePlan::for_node_shape(shape_ir, rule.clone()));
            }
        }

        for (shape_id, rule_ids) in &self.graph.property_shape_rules {
            let shape_ir = self
                .context
                .shape_ir()
                .property_shapes
                .iter()
                .find(|s| &s.id == shape_id)
                .ok_or_else(|| {
                    InferenceError::Configuration(format!(
                        "Property shape {:?} referenced in rules but missing from IR",
                        shape_id
                    ))
                })?;
            for rule_id in rule_ids {
                let rule = self.graph.rules.get(rule_id).ok_or_else(|| {
                    InferenceError::Configuration(format!(
                        "Rule {:?} referenced by property shape {:?} but missing from model",
                        rule_id, shape_id
                    ))
                })?;
                if rule.is_deactivated() {
                    continue;
                }
                plans.push(RulePlan::for_property_shape(shape_ir, rule.clone()));
            }
        }

        Ok(plans)
    }

    fn apply_rules_for_delta(
        &self,
        delta: &DeltaIndex,
        collected: &mut Vec<Quad>,
    ) -> Result<usize, InferenceError> {
        let mut iteration_added = 0usize;
        let mut seen_new: HashSet<(Term, NamedNode, Term)> = HashSet::new();
        let scheduled = self.scheduled_plan_indices(delta);

        for plan_index in scheduled {
            let plan = &self.plans[plan_index];
            let Some(focus_nodes) = self.focus_nodes_for_plan(plan, delta)? else {
                continue;
            };

            if self.config.trace {
                debug!(
                    "Rule {:?} owner {:?} scheduled for {} focus node(s)",
                    plan.rule.id(),
                    plan.owner,
                    focus_nodes.len()
                );
            }

            let added = match &plan.rule {
                Rule::Sparql(sparql_rule) => self.apply_sparql_rule(
                    sparql_rule,
                    plan.sparql_prefilter.as_ref(),
                    plan.native_sparql.as_ref(),
                    delta,
                    &focus_nodes,
                    &mut seen_new,
                    collected,
                )?,
                Rule::Triple(triple_rule) => self.apply_triple_rule(
                    triple_rule,
                    delta,
                    &focus_nodes,
                    &mut seen_new,
                    collected,
                )?,
            };
            iteration_added += added;

            if added > 0 {
                self.context.trace_sink.record(TraceEvent::RuleApplied {
                    rule: plan.rule.id(),
                    inserted: added,
                });
            }

            if self.config.trace && added > 0 {
                debug!(
                    "Rule {:?} owner {:?} produced {} triple(s)",
                    plan.rule.id(),
                    plan.owner,
                    added
                );
            }
        }

        Ok(iteration_added)
    }

    fn scheduled_plan_indices(&self, delta: &DeltaIndex) -> Vec<usize> {
        if delta.is_initial {
            return (0..self.plans.len()).collect();
        }

        let mut scheduled = HashSet::new();
        scheduled.extend(self.always_run_plans.iter().copied());
        for key in delta.dependency_keys() {
            if let Some(plan_indices) = self.dependency_index.get(&key) {
                scheduled.extend(plan_indices.iter().copied());
            }
        }

        let mut ordered: Vec<usize> = scheduled.into_iter().collect();
        ordered.sort_unstable();
        ordered
    }

    fn focus_nodes_for_plan(
        &self,
        plan: &RulePlan,
        delta: &DeltaIndex,
    ) -> Result<Option<Vec<Term>>, InferenceError> {
        let trigger = plan.trigger(delta);
        if trigger.is_none() {
            return Ok(None);
        }

        let all_focus_nodes = match plan.owner {
            RuleOwner::NodeShape(shape_id) => {
                let shape = self
                    .context
                    .shape_ir()
                    .node_shapes
                    .iter()
                    .find(|shape| shape.id == shape_id)
                    .ok_or_else(|| {
                        InferenceError::Configuration(format!(
                            "Node shape {:?} referenced in rules but missing from IR",
                            shape_id
                        ))
                    })?;
                self.focus_nodes_for_shape(shape)?
            }
            RuleOwner::PropertyShape(shape_id) => {
                let shape = self
                    .context
                    .shape_ir()
                    .property_shapes
                    .iter()
                    .find(|shape| shape.id == shape_id)
                    .ok_or_else(|| {
                        InferenceError::Configuration(format!(
                            "Property shape {:?} referenced in rules but missing from IR",
                            shape_id
                        ))
                    })?;
                self.focus_nodes_for_property_shape(shape)?
            }
        };

        let filtered = match trigger {
            TriggerSet::All => all_focus_nodes,
            TriggerSet::Nodes(nodes) => all_focus_nodes
                .into_iter()
                .filter(|focus| nodes.contains(focus))
                .collect(),
        };

        if filtered.is_empty() {
            return Ok(None);
        }
        Ok(Some(filtered))
    }

    fn focus_nodes_for_shape(
        &self,
        shape: &crate::types::NodeShapeIR,
    ) -> Result<Vec<Term>, InferenceError> {
        if let Some(cached) = self.context.cached_node_targets(&shape.id) {
            return Ok(cached.as_ref().to_vec());
        }
        let mut collected = HashSet::new();
        for target in &shape.targets {
            let contexts = target
                .get_target_nodes(self.context, SourceShape::NodeShape(shape.id))
                .map_err(|e| InferenceError::TargetResolution {
                    shape_id: shape.id,
                    message: e,
                })?;
            for ctx in contexts {
                collected.insert(ctx.focus_node().clone());
            }
        }
        let nodes: Vec<Term> = collected.into_iter().collect();
        let cached: Arc<[Term]> = nodes.clone().into();
        self.context.store_node_targets(shape.id, cached);
        Ok(nodes)
    }

    fn focus_nodes_for_property_shape(
        &self,
        shape: &crate::types::PropertyShapeIR,
    ) -> Result<Vec<Term>, InferenceError> {
        if let Some(cached) = self.context.cached_prop_targets(&shape.id) {
            return Ok(cached.as_ref().to_vec());
        }
        let mut collected = HashSet::new();
        for target in &shape.targets {
            let contexts = target
                .get_target_nodes(self.context, SourceShape::PropertyShape(shape.id))
                .map_err(|e| InferenceError::PropertyShapeTargetResolution {
                    shape_id: shape.id,
                    message: e,
                })?;
            for ctx in contexts {
                collected.insert(ctx.focus_node().clone());
            }
        }
        let nodes: Vec<Term> = collected.into_iter().collect();
        let cached: Arc<[Term]> = nodes.clone().into();
        self.context.store_prop_targets(shape.id, cached);
        Ok(nodes)
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_sparql_rule(
        &self,
        rule: &SparqlRule,
        prefilter: Option<&SparqlPrefilter>,
        native_rule: Option<&NativeSparqlRule>,
        delta: &DeltaIndex,
        focus_nodes: &[Term],
        seen_new: &mut HashSet<(Term, NamedNode, Term)>,
        collected: &mut Vec<Quad>,
    ) -> Result<usize, InferenceError> {
        if let Some(native_rule) = native_rule {
            return self.apply_native_sparql_rule(
                rule,
                native_rule,
                prefilter,
                delta,
                focus_nodes,
                seen_new,
                collected,
            );
        }

        let var_this = Variable::new("this").map_err(|e| InferenceError::RuleExecution {
            rule_id: rule.id,
            message: e.to_string(),
        })?;

        // Phase 1: parallel SPARQL execution — each thread prepares its own
        // query (hits the prepared-query cache) and collects candidate triples.
        let candidates: CandidateBatches = focus_nodes
            .par_iter()
            .map(|focus| {
                if !self.conditions_satisfied(rule.id, focus, &rule.condition_shapes)? {
                    return Ok(Vec::new());
                }
                if let Some(prefilter) = prefilter {
                    if !prefilter.matches_delta(delta, focus) {
                        return Ok(Vec::new());
                    }
                }

                let prepared = self.context.prepare_query(&rule.query).map_err(|e| {
                    InferenceError::RuleExecution {
                        rule_id: rule.id,
                        message: e,
                    }
                })?;

                let substitutions: Vec<Binding> = vec![(var_this.clone(), focus.clone())];
                let results = self
                    .context
                    .execute_prepared(&rule.query, &prepared, &substitutions, false)
                    .map_err(|e| InferenceError::RuleExecution {
                        rule_id: rule.id,
                        message: e,
                    })?;

                let mut batch = Vec::new();
                if let QueryResults::Graph(mut triples) = results {
                    for triple_res in &mut triples {
                        let triple = triple_res.map_err(|e| InferenceError::RuleExecution {
                            rule_id: rule.id,
                            message: e.to_string(),
                        })?;
                        let subject_term = named_or_blank_to_term(triple.subject).map_err(|m| {
                            InferenceError::RuleExecution {
                                rule_id: rule.id,
                                message: m,
                            }
                        })?;
                        batch.push((subject_term, triple.predicate, triple.object));
                    }
                } else {
                    return Err(InferenceError::RuleExecution {
                        rule_id: rule.id,
                        message: "SPARQL CONSTRUCT rule returned a non-graph result".to_string(),
                    });
                }
                Ok(batch)
            })
            .collect();

        // Phase 2: sequential dedup + insert
        let mut added = 0usize;
        for batch in candidates? {
            for (subject_term, predicate, object_term) in batch {
                if self.record_inferred_triple(
                    rule.id,
                    subject_term,
                    predicate,
                    object_term,
                    seen_new,
                    collected,
                )? {
                    added += 1;
                }
            }
        }

        Ok(added)
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_native_sparql_rule(
        &self,
        rule: &SparqlRule,
        native_rule: &NativeSparqlRule,
        prefilter: Option<&SparqlPrefilter>,
        delta: &DeltaIndex,
        focus_nodes: &[Term],
        seen_new: &mut HashSet<(Term, NamedNode, Term)>,
        collected: &mut Vec<Quad>,
    ) -> Result<usize, InferenceError> {
        let candidates: CandidateBatches = focus_nodes
            .par_iter()
            .map(|focus| {
                if !self.conditions_satisfied(rule.id, focus, &rule.condition_shapes)? {
                    return Ok(Vec::new());
                }
                if let Some(prefilter) = prefilter {
                    if !prefilter.matches_delta(delta, focus) {
                        return Ok(Vec::new());
                    }
                }

                let mut batch = Vec::new();
                match native_rule {
                    NativeSparqlRule::DirectCopy {
                        construct_predicate,
                        source_predicate,
                    } => {
                        let values = self
                            .native_direct_values(focus, source_predicate, delta)
                            .map_err(|message| InferenceError::RuleExecution {
                                rule_id: rule.id,
                                message,
                            })?;
                        for value in values {
                            batch.push((focus.clone(), construct_predicate.clone(), value));
                        }
                    }
                    NativeSparqlRule::TwoHopCopy {
                        construct_predicate,
                        first_predicate,
                        second_predicate,
                    } => {
                        let mids = self
                            .context
                            .focus_objects_for_predicate(
                                focus,
                                &Term::NamedNode(first_predicate.clone()),
                            )
                            .map_err(|message| InferenceError::RuleExecution {
                                rule_id: rule.id,
                                message,
                            })?;
                        let mut unique = HashSet::new();
                        for mid in mids {
                            let values = self
                                .native_direct_values(&mid, second_predicate, delta)
                                .map_err(|message| InferenceError::RuleExecution {
                                rule_id: rule.id,
                                message,
                            })?;
                            for value in values {
                                if unique.insert(value.clone()) {
                                    batch.push((focus.clone(), construct_predicate.clone(), value));
                                }
                            }
                        }
                    }
                    NativeSparqlRule::EqualityConstant {
                        construct_predicate,
                        left_predicate,
                        right_predicate,
                        object,
                    } => {
                        let left_values = self
                            .context
                            .focus_objects_for_predicate(
                                focus,
                                &Term::NamedNode(left_predicate.clone()),
                            )
                            .map_err(|message| InferenceError::RuleExecution {
                                rule_id: rule.id,
                                message,
                            })?;
                        let right_values = self
                            .context
                            .focus_objects_for_predicate(
                                focus,
                                &Term::NamedNode(right_predicate.clone()),
                            )
                            .map_err(|message| InferenceError::RuleExecution {
                                rule_id: rule.id,
                                message,
                            })?;
                        let right_set: HashSet<Term> = right_values.into_iter().collect();
                        if left_values
                            .into_iter()
                            .any(|value| right_set.contains(&value))
                        {
                            batch.push((
                                focus.clone(),
                                construct_predicate.clone(),
                                object.clone(),
                            ));
                        }
                    }
                }
                Ok(batch)
            })
            .collect();

        let mut added = 0usize;
        for batch in candidates? {
            for (subject_term, predicate, object_term) in batch {
                if self.record_inferred_triple(
                    rule.id,
                    subject_term,
                    predicate,
                    object_term,
                    seen_new,
                    collected,
                )? {
                    added += 1;
                }
            }
        }

        Ok(added)
    }

    fn native_direct_values(
        &self,
        focus: &Term,
        predicate: &NamedNode,
        delta: &DeltaIndex,
    ) -> Result<Vec<Term>, String> {
        if !delta.is_initial {
            if let Some(values) = delta.outgoing_values_for_focus(focus, predicate) {
                return Ok(values);
            }
        }
        self.context
            .focus_objects_for_predicate(focus, &Term::NamedNode(predicate.clone()))
    }

    fn apply_triple_rule(
        &self,
        rule: &TripleRule,
        delta: &DeltaIndex,
        focus_nodes: &[Term],
        seen_new: &mut HashSet<(Term, NamedNode, Term)>,
        collected: &mut Vec<Quad>,
    ) -> Result<usize, InferenceError> {
        // Phase 1: parallel template evaluation — each thread evaluates
        // subjects/objects and builds the cross-product of candidate triples.
        let candidates: CandidateBatches = focus_nodes
            .par_iter()
            .map(|focus| {
                if !self.conditions_satisfied(rule.id, focus, &rule.condition_shapes)? {
                    return Ok(Vec::new());
                }

                let subjects =
                    self.evaluate_template(rule.id, &rule.subject, focus, Some(delta))?;
                let objects = self.evaluate_template(rule.id, &rule.object, focus, Some(delta))?;

                let mut batch = Vec::with_capacity(subjects.len() * objects.len());
                for subject_term in &subjects {
                    for object_term in &objects {
                        batch.push((
                            subject_term.clone(),
                            rule.predicate.clone(),
                            object_term.clone(),
                        ));
                    }
                }
                Ok(batch)
            })
            .collect();

        // Phase 2: sequential dedup + insert
        let mut added = 0usize;
        for batch in candidates? {
            for (subject_term, predicate, object_term) in batch {
                if self.record_inferred_triple(
                    rule.id,
                    subject_term,
                    predicate,
                    object_term,
                    seen_new,
                    collected,
                )? {
                    added += 1;
                }
            }
        }

        Ok(added)
    }

    fn evaluate_template(
        &self,
        rule_id: RuleID,
        template: &TriplePatternTerm,
        focus_node: &Term,
        delta: Option<&DeltaIndex>,
    ) -> Result<Vec<Term>, InferenceError> {
        match template {
            TriplePatternTerm::This => Ok(vec![focus_node.clone()]),
            TriplePatternTerm::Constant(term) => Ok(vec![term.clone()]),
            TriplePatternTerm::Path(path) => self.evaluate_path(rule_id, path, focus_node, delta),
        }
    }

    fn evaluate_path(
        &self,
        rule_id: RuleID,
        path: &crate::types::Path,
        focus_node: &Term,
        delta: Option<&DeltaIndex>,
    ) -> Result<Vec<Term>, InferenceError> {
        if let Some(values) = self.evaluate_path_native(rule_id, path, focus_node, delta)? {
            return Ok(values);
        }

        let sparql_path = path
            .to_sparql_path()
            .map_err(|e| InferenceError::RuleExecution {
                rule_id,
                message: e,
            })?;

        let query = format!(
            "SELECT DISTINCT ?valueNode WHERE {{ {} {} ?valueNode . }}",
            focus_node, sparql_path
        );

        let prepared =
            self.context
                .prepare_query(&query)
                .map_err(|e| InferenceError::RuleExecution {
                    rule_id,
                    message: e,
                })?;

        let results = self
            .context
            .execute_prepared(&query, &prepared, &[], false)
            .map_err(|e| InferenceError::RuleExecution {
                rule_id,
                message: e,
            })?;

        let mut values = Vec::new();
        match results {
            QueryResults::Solutions(solutions) => {
                let var =
                    Variable::new("valueNode").map_err(|e| InferenceError::RuleExecution {
                        rule_id,
                        message: e.to_string(),
                    })?;
                for solution in solutions {
                    let binding = solution.map_err(|e| InferenceError::RuleExecution {
                        rule_id,
                        message: e.to_string(),
                    })?;
                    if let Some(term) = binding.get(&var) {
                        values.push(term.clone());
                    }
                }
            }
            _ => {
                return Err(InferenceError::RuleExecution {
                    rule_id,
                    message: "Path evaluation returned non-solution results".to_string(),
                });
            }
        }
        let mut unique = HashSet::new();
        values.retain(|term| unique.insert(term.clone()));
        Ok(values)
    }

    fn evaluate_path_native(
        &self,
        rule_id: RuleID,
        path: &crate::types::Path,
        focus_node: &Term,
        delta: Option<&DeltaIndex>,
    ) -> Result<Option<Vec<Term>>, InferenceError> {
        match path {
            crate::types::Path::Simple(Term::NamedNode(predicate)) => {
                let values = if let Some(values) =
                    delta.and_then(|d| d.outgoing_values_for_focus(focus_node, predicate))
                {
                    values
                } else {
                    self.context
                        .focus_objects_for_predicate(
                            focus_node,
                            &Term::NamedNode(predicate.clone()),
                        )
                        .map_err(|message| InferenceError::RuleExecution { rule_id, message })?
                };
                Ok(Some(values))
            }
            crate::types::Path::Inverse(inner) => match inner.as_ref() {
                crate::types::Path::Simple(Term::NamedNode(predicate)) => {
                    let values = if let Some(values) =
                        delta.and_then(|d| d.incoming_values_for_focus(focus_node, predicate))
                    {
                        values
                    } else {
                        self.context
                            .focus_subjects_for_inverse_predicate(
                                focus_node,
                                &Term::NamedNode(predicate.clone()),
                            )
                            .map_err(|message| InferenceError::RuleExecution { rule_id, message })?
                    };
                    Ok(Some(values))
                }
                _ => Ok(None),
            },
            crate::types::Path::Sequence(paths) => {
                if paths.is_empty() {
                    return Ok(Some(Vec::new()));
                }
                let mut frontier = vec![focus_node.clone()];
                for (index, segment) in paths.iter().enumerate() {
                    let segment_delta = if index + 1 == paths.len() {
                        delta
                    } else {
                        None
                    };
                    let mut next = Vec::new();
                    for node in &frontier {
                        let Some(values) =
                            self.evaluate_path_native(rule_id, segment, node, segment_delta)?
                        else {
                            return Ok(None);
                        };
                        next.extend(values);
                    }
                    let mut unique = HashSet::new();
                    next.retain(|term| unique.insert(term.clone()));
                    frontier = next;
                    if frontier.is_empty() {
                        break;
                    }
                }
                Ok(Some(frontier))
            }
            _ => Ok(None),
        }
    }

    fn conditions_satisfied(
        &self,
        rule_id: RuleID,
        focus_node: &Term,
        conditions: &[RuleCondition],
    ) -> Result<bool, InferenceError> {
        for condition in conditions {
            match condition {
                RuleCondition::NodeShape(shape_id) => {
                    let conforms = node_conforms_to_shape(self.context, focus_node, *shape_id)
                        .map_err(|e| InferenceError::RuleExecution {
                            rule_id,
                            message: e,
                        })?;
                    if !conforms {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    fn record_inferred_triple(
        &self,
        rule_id: RuleID,
        subject_term: Term,
        predicate: NamedNode,
        object_term: Term,
        seen_new: &mut HashSet<(Term, NamedNode, Term)>,
        collected: &mut Vec<Quad>,
    ) -> Result<bool, InferenceError> {
        if self.config.error_on_blank_nodes
            && (matches!(subject_term, Term::BlankNode(_))
                || matches!(object_term, Term::BlankNode(_)))
        {
            return Err(InferenceError::blank_node_produced(
                rule_id,
                subject_term,
                predicate,
                object_term,
            ));
        }

        let subject = term_to_named_or_blank(subject_term.clone())
            .map_err(|message| InferenceError::RuleExecution { rule_id, message })?;

        let key = (subject_term.clone(), predicate.clone(), object_term.clone());
        if seen_new.contains(&key) {
            return Ok(false);
        }

        let graph = GraphName::NamedNode(self.context.data_graph_iri.clone());
        let quad = Quad::new(
            subject.clone(),
            predicate.clone(),
            object_term.clone(),
            graph.clone(),
        );

        if self
            .context
            .contains_quad(&quad)
            .map_err(|e| InferenceError::RuleExecution {
                rule_id,
                message: e,
            })?
        {
            return Ok(false);
        }

        self.context
            .insert_quads(std::slice::from_ref(&quad))
            .map_err(|e| InferenceError::RuleExecution {
                rule_id,
                message: e,
            })?;

        seen_new.insert(key);
        collected.push(quad);
        Ok(true)
    }
}

/// Convenience helper to execute inference with the provided context and configuration.
pub fn run_inference(
    context: &ValidationContext,
    config: InferenceConfig,
) -> Result<InferenceOutcome, InferenceError> {
    let engine = InferenceEngine::new(context, config)?;
    engine.run()
}

#[derive(Debug, Clone, Copy)]
enum RuleOwner {
    NodeShape(ID),
    PropertyShape(PropShapeID),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum FocusDependency {
    All,
    TargetClass(Term),
    TargetSubjectsOf(NamedNode),
    TargetObjectsOf(NamedNode),
    OutgoingPredicate(NamedNode),
    IncomingPredicate(NamedNode),
    AnyPredicateParticipant(NamedNode),
}

#[derive(Debug, Clone)]
struct RulePlan {
    owner: RuleOwner,
    rule: Rule,
    dependencies: Vec<FocusDependency>,
    sparql_prefilter: Option<SparqlPrefilter>,
    native_sparql: Option<NativeSparqlRule>,
}

impl RulePlan {
    fn for_node_shape(shape: &crate::types::NodeShapeIR, rule: Rule) -> Self {
        let mut dependencies = dependencies_for_targets(&shape.targets);
        dependencies.extend(dependencies_for_rule(&rule));
        dedup_preserve_order(&mut dependencies);
        Self {
            owner: RuleOwner::NodeShape(shape.id),
            sparql_prefilter: sparql_prefilter_for_rule(&rule),
            native_sparql: native_sparql_rule(&rule),
            rule,
            dependencies,
        }
    }

    fn for_property_shape(shape: &crate::types::PropertyShapeIR, rule: Rule) -> Self {
        let mut dependencies = dependencies_for_targets(&shape.targets);
        dependencies.extend(dependencies_for_rule(&rule));
        dedup_preserve_order(&mut dependencies);
        Self {
            owner: RuleOwner::PropertyShape(shape.id),
            sparql_prefilter: sparql_prefilter_for_rule(&rule),
            native_sparql: native_sparql_rule(&rule),
            rule,
            dependencies,
        }
    }

    fn trigger(&self, delta: &DeltaIndex) -> TriggerSet {
        if delta.is_initial {
            return TriggerSet::All;
        }

        let mut focus_nodes = HashSet::new();
        for dependency in &self.dependencies {
            match dependency {
                FocusDependency::All => return TriggerSet::All,
                FocusDependency::TargetClass(class) => {
                    if let Some(nodes) = delta.typed_subjects_by_class.get(class) {
                        focus_nodes.extend(nodes.iter().cloned());
                    }
                }
                FocusDependency::TargetSubjectsOf(predicate)
                | FocusDependency::OutgoingPredicate(predicate) => {
                    if let Some(nodes) = delta.subjects_by_predicate.get(predicate) {
                        focus_nodes.extend(nodes.iter().cloned());
                    }
                }
                FocusDependency::TargetObjectsOf(predicate)
                | FocusDependency::IncomingPredicate(predicate) => {
                    if let Some(nodes) = delta.objects_by_predicate.get(predicate) {
                        focus_nodes.extend(nodes.iter().cloned());
                    }
                }
                FocusDependency::AnyPredicateParticipant(predicate) => {
                    if let Some(nodes) = delta.participants_by_predicate.get(predicate) {
                        focus_nodes.extend(nodes.iter().cloned());
                    }
                }
            }
        }

        TriggerSet::Nodes(focus_nodes)
    }
}

#[derive(Debug, Clone, Default)]
struct SparqlPrefilter {
    direct_outgoing_predicates: Vec<NamedNode>,
    direct_incoming_predicates: Vec<NamedNode>,
    direct_classes: Vec<Term>,
}

#[derive(Debug, Clone)]
enum NativeSparqlRule {
    DirectCopy {
        construct_predicate: NamedNode,
        source_predicate: NamedNode,
    },
    TwoHopCopy {
        construct_predicate: NamedNode,
        first_predicate: NamedNode,
        second_predicate: NamedNode,
    },
    EqualityConstant {
        construct_predicate: NamedNode,
        left_predicate: NamedNode,
        right_predicate: NamedNode,
        object: Term,
    },
}

impl SparqlPrefilter {
    fn matches_delta(&self, delta: &DeltaIndex, focus: &Term) -> bool {
        if delta.is_initial {
            return true;
        }

        self.direct_outgoing_predicates
            .iter()
            .any(|predicate| delta.subject_has_predicate(focus, predicate))
            || self
                .direct_incoming_predicates
                .iter()
                .any(|predicate| delta.object_has_predicate(focus, predicate))
            || self
                .direct_classes
                .iter()
                .any(|class| delta.subject_has_class(focus, class))
    }

    fn is_empty(&self) -> bool {
        self.direct_outgoing_predicates.is_empty()
            && self.direct_incoming_predicates.is_empty()
            && self.direct_classes.is_empty()
    }
}

#[derive(Debug, Clone)]
enum TriggerSet {
    All,
    Nodes(HashSet<Term>),
}

impl TriggerSet {
    fn is_none(&self) -> bool {
        matches!(self, TriggerSet::Nodes(nodes) if nodes.is_empty())
    }
}

#[derive(Debug, Default, Clone)]
struct DeltaIndex {
    is_initial: bool,
    subjects_by_predicate: HashMap<NamedNode, HashSet<Term>>,
    objects_by_predicate: HashMap<NamedNode, HashSet<Term>>,
    participants_by_predicate: HashMap<NamedNode, HashSet<Term>>,
    typed_subjects_by_class: HashMap<Term, HashSet<Term>>,
    outgoing_values_by_focus: HashMap<Term, HashMap<NamedNode, HashSet<Term>>>,
    incoming_values_by_focus: HashMap<Term, HashMap<NamedNode, HashSet<Term>>>,
}

impl DeltaIndex {
    fn initial() -> Self {
        Self {
            is_initial: true,
            ..Self::default()
        }
    }

    fn from_quads(quads: &[Quad]) -> Self {
        let mut index = Self::default();
        for quad in quads {
            let Ok(subject_term) = named_or_blank_to_term(quad.subject.clone()) else {
                continue;
            };
            let predicate = quad.predicate.clone();
            index
                .subjects_by_predicate
                .entry(predicate.clone())
                .or_default()
                .insert(subject_term.clone());
            index
                .objects_by_predicate
                .entry(predicate.clone())
                .or_default()
                .insert(quad.object.clone());
            let participants = index
                .participants_by_predicate
                .entry(predicate.clone())
                .or_default();
            participants.insert(subject_term.clone());
            participants.insert(quad.object.clone());
            index
                .outgoing_values_by_focus
                .entry(subject_term.clone())
                .or_default()
                .entry(predicate.clone())
                .or_default()
                .insert(quad.object.clone());
            index
                .incoming_values_by_focus
                .entry(quad.object.clone())
                .or_default()
                .entry(predicate.clone())
                .or_default()
                .insert(subject_term.clone());
            if predicate.as_str() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                && matches!(quad.object, Term::NamedNode(_))
            {
                index
                    .typed_subjects_by_class
                    .entry(quad.object.clone())
                    .or_default()
                    .insert(subject_term);
            }
        }
        index
    }

    fn dependency_keys(&self) -> Vec<FocusDependency> {
        let mut keys = Vec::new();
        for predicate in self.subjects_by_predicate.keys() {
            keys.push(FocusDependency::TargetSubjectsOf(predicate.clone()));
            keys.push(FocusDependency::OutgoingPredicate(predicate.clone()));
        }
        for predicate in self.objects_by_predicate.keys() {
            keys.push(FocusDependency::TargetObjectsOf(predicate.clone()));
            keys.push(FocusDependency::IncomingPredicate(predicate.clone()));
        }
        for predicate in self.participants_by_predicate.keys() {
            keys.push(FocusDependency::AnyPredicateParticipant(predicate.clone()));
        }
        for class in self.typed_subjects_by_class.keys() {
            keys.push(FocusDependency::TargetClass(class.clone()));
        }
        keys
    }

    fn outgoing_values_for_focus(&self, focus: &Term, predicate: &NamedNode) -> Option<Vec<Term>> {
        self.outgoing_values_by_focus
            .get(focus)
            .and_then(|by_predicate| by_predicate.get(predicate))
            .map(|values| values.iter().cloned().collect())
    }

    fn incoming_values_for_focus(&self, focus: &Term, predicate: &NamedNode) -> Option<Vec<Term>> {
        self.incoming_values_by_focus
            .get(focus)
            .and_then(|by_predicate| by_predicate.get(predicate))
            .map(|values| values.iter().cloned().collect())
    }

    fn subject_has_predicate(&self, focus: &Term, predicate: &NamedNode) -> bool {
        self.subjects_by_predicate
            .get(predicate)
            .is_some_and(|subjects| subjects.contains(focus))
    }

    fn object_has_predicate(&self, focus: &Term, predicate: &NamedNode) -> bool {
        self.objects_by_predicate
            .get(predicate)
            .is_some_and(|objects| objects.contains(focus))
    }

    fn subject_has_class(&self, focus: &Term, class: &Term) -> bool {
        self.typed_subjects_by_class
            .get(class)
            .is_some_and(|subjects| subjects.contains(focus))
    }
}

fn dependencies_for_targets(targets: &[crate::types::Target]) -> Vec<FocusDependency> {
    let mut dependencies = Vec::new();
    for target in targets {
        match target {
            crate::types::Target::Class(class) => {
                dependencies.push(FocusDependency::TargetClass(class.clone()))
            }
            crate::types::Target::SubjectsOf(Term::NamedNode(predicate)) => {
                dependencies.push(FocusDependency::TargetSubjectsOf(predicate.clone()))
            }
            crate::types::Target::ObjectsOf(Term::NamedNode(predicate)) => {
                dependencies.push(FocusDependency::TargetObjectsOf(predicate.clone()))
            }
            crate::types::Target::Advanced(_) => dependencies.push(FocusDependency::All),
            crate::types::Target::Node(_)
            | crate::types::Target::SubjectsOf(_)
            | crate::types::Target::ObjectsOf(_) => {}
        }
    }
    dependencies
}

fn dependencies_for_rule(rule: &Rule) -> Vec<FocusDependency> {
    match rule {
        Rule::Triple(rule) => dependencies_for_triple_rule(rule),
        Rule::Sparql(rule) => dependencies_for_sparql_rule(rule),
    }
}

fn dependencies_for_triple_rule(rule: &TripleRule) -> Vec<FocusDependency> {
    let mut dependencies = Vec::new();
    dependencies.extend(dependencies_for_template(&rule.subject));
    dependencies.extend(dependencies_for_template(&rule.object));
    if !rule.condition_shapes.is_empty() {
        dependencies.push(FocusDependency::All);
    }
    dependencies
}

fn dependencies_for_template(template: &TriplePatternTerm) -> Vec<FocusDependency> {
    match template {
        TriplePatternTerm::This | TriplePatternTerm::Constant(_) => Vec::new(),
        TriplePatternTerm::Path(path) => dependencies_for_path(path, false),
    }
}

fn dependencies_for_path(path: &crate::types::Path, any_participant: bool) -> Vec<FocusDependency> {
    match path {
        crate::types::Path::Simple(Term::NamedNode(predicate)) => {
            if any_participant {
                vec![FocusDependency::AnyPredicateParticipant(predicate.clone())]
            } else {
                vec![FocusDependency::OutgoingPredicate(predicate.clone())]
            }
        }
        crate::types::Path::Inverse(inner) => match inner.as_ref() {
            crate::types::Path::Simple(Term::NamedNode(predicate)) => {
                vec![FocusDependency::IncomingPredicate(predicate.clone())]
            }
            other => dependencies_for_path(other, true),
        },
        crate::types::Path::Sequence(paths) | crate::types::Path::Alternative(paths) => paths
            .iter()
            .flat_map(|path| dependencies_for_path(path, true))
            .collect(),
        crate::types::Path::ZeroOrMore(inner)
        | crate::types::Path::OneOrMore(inner)
        | crate::types::Path::ZeroOrOne(inner) => dependencies_for_path(inner, true),
        crate::types::Path::Simple(_) => vec![FocusDependency::All],
    }
}

fn dependencies_for_sparql_rule(rule: &SparqlRule) -> Vec<FocusDependency> {
    let where_body = rule
        .query
        .split_once("WHERE")
        .map(|(_, tail)| tail)
        .unwrap_or(rule.query.as_str());
    let mut dependencies = extract_sparql_dependencies(where_body);
    if !rule.condition_shapes.is_empty() {
        dependencies.push(FocusDependency::All);
    }
    if dependencies.is_empty() {
        dependencies.push(FocusDependency::All);
    }
    dependencies
}

fn build_dependency_index(
    plans: &[RulePlan],
) -> (HashMap<FocusDependency, Vec<usize>>, Vec<usize>) {
    let mut index: HashMap<FocusDependency, Vec<usize>> = HashMap::new();
    let mut always_run = Vec::new();

    for (plan_index, plan) in plans.iter().enumerate() {
        for dependency in &plan.dependencies {
            if *dependency == FocusDependency::All {
                always_run.push(plan_index);
            } else {
                index
                    .entry(dependency.clone())
                    .or_default()
                    .push(plan_index);
            }
        }
    }

    (index, always_run)
}

fn dedup_preserve_order<T: Eq + std::hash::Hash + Clone>(items: &mut Vec<T>) {
    let mut seen = HashSet::new();
    items.retain(|item| seen.insert(item.clone()));
}

fn sparql_prefilter_for_rule(rule: &Rule) -> Option<SparqlPrefilter> {
    let Rule::Sparql(rule) = rule else {
        return None;
    };

    let where_body = rule
        .query
        .split_once("WHERE")
        .map(|(_, tail)| tail)
        .unwrap_or(rule.query.as_str());
    let upper = where_body.to_ascii_uppercase();
    if upper.contains("OPTIONAL")
        || upper.contains("UNION")
        || upper.contains("MINUS")
        || upper.contains("NOT EXISTS")
    {
        return None;
    }

    static DIRECT_OUTGOING_RE: OnceLock<Regex> = OnceLock::new();
    static DIRECT_INCOMING_RE: OnceLock<Regex> = OnceLock::new();
    static DIRECT_TYPE_RE: OnceLock<Regex> = OnceLock::new();

    let mut prefilter = SparqlPrefilter::default();

    let direct_outgoing_re = DIRECT_OUTGOING_RE.get_or_init(|| {
        Regex::new(
            r#"(?i)\$this\s+(<[^>]+>)\s+(?:\?[A-Za-z_][A-Za-z0-9_]*|\$this|<[^>]+>|\"[^\"]*\")"#,
        )
        .expect("valid direct outgoing regex")
    });
    for capture in direct_outgoing_re.captures_iter(where_body) {
        if let Some(predicate) = capture
            .get(1)
            .and_then(|m| NamedNode::new(&m.as_str()[1..m.as_str().len() - 1]).ok())
        {
            prefilter.direct_outgoing_predicates.push(predicate);
        }
    }

    let direct_incoming_re = DIRECT_INCOMING_RE.get_or_init(|| {
        Regex::new(r#"(?i)(?:\?[A-Za-z_][A-Za-z0-9_]*|<[^>]+>|\"[^\"]*\")\s+(<[^>]+>)\s+\$this"#)
            .expect("valid direct incoming regex")
    });
    for capture in direct_incoming_re.captures_iter(where_body) {
        if let Some(predicate) = capture
            .get(1)
            .and_then(|m| NamedNode::new(&m.as_str()[1..m.as_str().len() - 1]).ok())
        {
            prefilter.direct_incoming_predicates.push(predicate);
        }
    }

    let direct_type_re = DIRECT_TYPE_RE.get_or_init(|| {
        Regex::new(
            r#"(?i)\$this\s+(?:a|<http://www\.w3\.org/1999/02/22-rdf-syntax-ns#type>)\s+(<[^>]+>)"#,
        )
        .expect("valid direct type regex")
    });
    for capture in direct_type_re.captures_iter(where_body) {
        if let Some(class) = capture
            .get(1)
            .and_then(|m| NamedNode::new(&m.as_str()[1..m.as_str().len() - 1]).ok())
        {
            prefilter.direct_classes.push(Term::NamedNode(class));
        }
    }

    prefilter.direct_outgoing_predicates.sort();
    prefilter.direct_outgoing_predicates.dedup();
    prefilter.direct_incoming_predicates.sort();
    prefilter.direct_incoming_predicates.dedup();
    dedup_preserve_order(&mut prefilter.direct_classes);

    (!prefilter.is_empty()).then_some(prefilter)
}

fn native_sparql_rule(rule: &Rule) -> Option<NativeSparqlRule> {
    let Rule::Sparql(rule) = rule else {
        return None;
    };
    let normalized = normalize_sparql(&rule.query);

    native_direct_copy_rule(&normalized)
        .or_else(|| native_two_hop_copy_rule(&normalized))
        .or_else(|| native_equality_constant_rule(&normalized))
}

fn normalize_sparql(query: &str) -> String {
    query.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn native_direct_copy_rule(query: &str) -> Option<NativeSparqlRule> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(
            r#"(?i)CONSTRUCT\s*\{\s*\$this\s+(<[^>]+>)\s+(\?[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*\}\s*WHERE\s*\{\s*\$this\s+(<[^>]+>)\s+(\?[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*\}"#,
        )
        .expect("valid native direct copy regex")
    });
    let captures = re.captures(query)?;
    if captures.get(2)?.as_str() != captures.get(4)?.as_str() {
        return None;
    }
    Some(NativeSparqlRule::DirectCopy {
        construct_predicate: parse_iri_capture(&captures, 1)?,
        source_predicate: parse_iri_capture(&captures, 3)?,
    })
}

fn native_two_hop_copy_rule(query: &str) -> Option<NativeSparqlRule> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(
            r#"(?i)CONSTRUCT\s*\{\s*\$this\s+(<[^>]+>)\s+(\?[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*\}\s*WHERE\s*\{\s*\$this\s+(<[^>]+>)\s+(\?[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*(\?[A-Za-z_][A-Za-z0-9_]*)\s+(<[^>]+>)\s+(\?[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*\}"#,
        )
        .expect("valid native two-hop copy regex")
    });
    let captures = re.captures(query)?;
    if captures.get(4)?.as_str() != captures.get(5)?.as_str()
        || captures.get(2)?.as_str() != captures.get(7)?.as_str()
    {
        return None;
    }
    Some(NativeSparqlRule::TwoHopCopy {
        construct_predicate: parse_iri_capture(&captures, 1)?,
        first_predicate: parse_iri_capture(&captures, 3)?,
        second_predicate: parse_iri_capture(&captures, 6)?,
    })
}

fn native_equality_constant_rule(query: &str) -> Option<NativeSparqlRule> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(
            r#"(?i)CONSTRUCT\s*\{\s*\$this\s+(<[^>]+>)\s+(.+?)\s*\.\s*\}\s*WHERE\s*\{\s*\$this\s+(<[^>]+>)\s+(\?[A-Za-z_][A-Za-z0-9_]*)\s*;\s*(<[^>]+>)\s+(\?[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*FILTER\s*\(\s*(\?[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(\?[A-Za-z_][A-Za-z0-9_]*)\s*\)\s*\}"#,
        )
        .expect("valid native equality constant regex")
    });
    let captures = re.captures(query)?;
    let left_var = captures.get(4)?.as_str();
    let right_var = captures.get(6)?.as_str();
    let filter_left = captures.get(7)?.as_str();
    let filter_right = captures.get(8)?.as_str();
    let filter_matches = (left_var == filter_left && right_var == filter_right)
        || (left_var == filter_right && right_var == filter_left);
    if !filter_matches {
        return None;
    }
    let object = parse_term(captures.get(2)?.as_str())?;
    Some(NativeSparqlRule::EqualityConstant {
        construct_predicate: parse_iri_capture(&captures, 1)?,
        left_predicate: parse_iri_capture(&captures, 3)?,
        right_predicate: parse_iri_capture(&captures, 5)?,
        object,
    })
}

fn parse_iri_capture(captures: &regex::Captures<'_>, index: usize) -> Option<NamedNode> {
    captures
        .get(index)
        .and_then(|m| parse_iri_token(m.as_str()))
}

fn parse_iri_token(token: &str) -> Option<NamedNode> {
    token
        .strip_prefix('<')
        .and_then(|s| s.strip_suffix('>'))
        .and_then(|iri| NamedNode::new(iri).ok())
}

fn parse_term(token: &str) -> Option<Term> {
    if let Some(iri) = parse_iri_token(token) {
        return Some(Term::NamedNode(iri));
    }
    if token == "true" || token == "false" {
        return Some(Term::Literal(oxigraph::model::Literal::from(
            token == "true",
        )));
    }
    if let Some(value) = token.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        return Some(Term::Literal(oxigraph::model::Literal::new_simple_literal(
            value,
        )));
    }
    None
}

fn extract_sparql_dependencies(query: &str) -> Vec<FocusDependency> {
    static TYPE_CLASS_RE: OnceLock<Regex> = OnceLock::new();
    static PREDICATE_RE: OnceLock<Regex> = OnceLock::new();

    let mut dependencies = Vec::new();
    let type_class_re = TYPE_CLASS_RE.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:\?[A-Za-z_][A-Za-z0-9_]*|\$this)\s+(?:a|<http://www\.w3\.org/1999/02/22-rdf-syntax-ns#type>)\s+(<[^>]+>)"#,
        )
        .expect("valid rdf:type regex")
    });
    for capture in type_class_re.captures_iter(query) {
        if let Some(class) = capture
            .get(1)
            .and_then(|m| NamedNode::new(&m.as_str()[1..m.as_str().len() - 1]).ok())
        {
            dependencies.push(FocusDependency::TargetClass(Term::NamedNode(class)));
        }
    }

    let predicate_re = PREDICATE_RE.get_or_init(|| {
        Regex::new(r#"(?i)(?:\?[A-Za-z_][A-Za-z0-9_]*|\$this)\s+(<[^>]+>)\s+(?:\?[A-Za-z_][A-Za-z0-9_]*|\$this|<[^>]+>|\"[^\"]*\")"#)
            .expect("valid predicate regex")
    });
    for capture in predicate_re.captures_iter(query) {
        if let Some(predicate) = capture
            .get(1)
            .and_then(|m| NamedNode::new(&m.as_str()[1..m.as_str().len() - 1]).ok())
        {
            dependencies.push(FocusDependency::AnyPredicateParticipant(predicate));
        }
    }

    dependencies
}

fn term_to_named_or_blank(term: Term) -> Result<NamedOrBlankNode, String> {
    match term {
        Term::NamedNode(node) => Ok(node.into()),
        Term::BlankNode(bn) => Ok(bn.into()),
        other => Err(format!(
            "Inferred triple subject must be an IRI or blank node, found {:?}",
            other
        )),
    }
}

fn node_conforms_to_shape(
    vc: &ValidationContext,
    focus_node: &Term,
    shape_id: ID,
) -> Result<bool, String> {
    let Some(shape) = vc.model.get_node_shape_by_id(&shape_id) else {
        return Err(format!("shape {:?} not found", shape_id));
    };
    let mut ctx = Context::new(
        focus_node.clone(),
        None,
        Some(vec![focus_node.clone()]),
        SourceShape::NodeShape(shape_id),
        vc.new_trace(),
    );
    let mut trace: Vec<TraceItem> = Vec::new();
    match check_conformance_for_node(&mut ctx, shape, vc, &mut trace)? {
        ConformanceReport::Conforms => Ok(true),
        ConformanceReport::NonConforms(_) => Ok(false),
    }
}

fn named_or_blank_to_term(subject: NamedOrBlankNode) -> Result<Term, String> {
    match subject {
        NamedOrBlankNode::NamedNode(nn) => Ok(Term::NamedNode(nn)),
        NamedOrBlankNode::BlankNode(bn) => Ok(Term::BlankNode(bn)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::GraphBackend, Source, Validator};
    use oxigraph::model::{Literal, NamedNode, Quad, Term};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn write_temp_files(shapes: &str, data: &str) -> (PathBuf, PathBuf) {
        let base = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = base.join(format!("shacl_inference_{}", nanos));
        fs::create_dir_all(&dir).unwrap();
        let shapes_path = dir.join("shapes.ttl");
        fs::write(&shapes_path, shapes).unwrap();
        let data_path = dir.join("data.ttl");
        fs::write(&data_path, data).unwrap();
        (shapes_path, data_path)
    }

    fn build_validator(shapes: &str, data: &str) -> Validator {
        let (shapes_path, data_path) = write_temp_files(shapes, data);
        Validator::builder()
            .with_shapes_source(Source::File(shapes_path))
            .with_data_source(Source::File(data_path))
            .build()
            .expect("validator should build")
    }

    #[test]
    fn sparql_rule_infers_square_flag() {
        let shapes = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:RectangleShape a sh:NodeShape ;
    sh:targetClass ex:Rectangle ;
    sh:rule [
        a sh:SPARQLRule ;
        sh:construct """
            PREFIX ex: <http://example.com/ns#>
            CONSTRUCT {
                $this ex:isSquare true .
            }
            WHERE {
                $this ex:width ?w ;
                      ex:height ?h .
                FILTER(?w = ?h)
            }
        """ ;
    ] .
"#;

        let data = r#"@prefix ex: <http://example.com/ns#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:rect1 a ex:Rectangle ;
    ex:width 4 ;
    ex:height 4 .

ex:rect2 a ex:Rectangle ;
    ex:width 2 ;
    ex:height 3 .
"#;

        let validator = build_validator(shapes, data);
        let context = validator.context();
        let config = InferenceConfig::default();
        let outcome = run_inference(context, config.clone()).expect("inference should succeed");
        assert_eq!(outcome.triples_added, 1);
        assert_eq!(outcome.inferred_quads.len(), 1);
        assert!(outcome.converged);
        assert!(outcome.iterations_executed >= 1);

        let subject = NamedNode::new("http://example.com/ns#rect1").unwrap();
        let predicate = NamedNode::new("http://example.com/ns#isSquare").unwrap();
        let object = Term::Literal(Literal::new_typed_literal(
            "true",
            NamedNode::new("http://www.w3.org/2001/XMLSchema#boolean").unwrap(),
        ));
        let quad = Quad::new(
            subject.clone(),
            predicate,
            object,
            GraphName::NamedNode(context.data_graph_iri.clone()),
        );
        assert!(
            context
                .contains_quad(&quad)
                .expect("quad lookup should succeed"),
            "quad should be present after inference"
        );

        // second run should add nothing
        let outcome_second = run_inference(context, config).expect("second run succeeds");
        assert_eq!(outcome_second.triples_added, 0);
        assert!(outcome_second.inferred_quads.is_empty());
    }

    #[test]
    fn triple_rule_adds_constant_literal() {
        let shapes = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .

ex:RectangleShape a sh:NodeShape ;
    sh:targetClass ex:Rectangle ;
    sh:rule [
        a sh:TripleRule ;
        sh:subject sh:this ;
        sh:predicate ex:tag ;
        sh:object "rectangle" ;
    ] .
"#;

        let data = r#"@prefix ex: <http://example.com/ns#> .

ex:rect1 a ex:Rectangle .
ex:rect2 a ex:Rectangle .
"#;

        let validator = build_validator(shapes, data);
        let context = validator.context();
        let outcome = run_inference(context, InferenceConfig::default()).expect("inference");
        assert_eq!(outcome.triples_added, 2);
        assert_eq!(outcome.inferred_quads.len(), 2);

        let predicate = NamedNode::new("http://example.com/ns#tag").unwrap();
        let literal = Term::Literal(Literal::new_simple_literal("rectangle"));
        for subject_iri in ["http://example.com/ns#rect1", "http://example.com/ns#rect2"] {
            let quad = Quad::new(
                NamedNode::new(subject_iri).unwrap(),
                predicate.clone(),
                literal.clone(),
                GraphName::NamedNode(context.data_graph_iri.clone()),
            );
            assert!(
                context.contains_quad(&quad).expect("quad lookup"),
                "quad should exist in store"
            );
        }
    }

    #[test]
    fn blank_node_guard_errors() {
        let shapes = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .

ex:ThingShape a sh:NodeShape ;
    sh:targetClass ex:Thing ;
    sh:rule [
        a sh:SPARQLRule ;
        sh:construct """
            PREFIX ex: <http://example.com/ns#>
            CONSTRUCT { _:b0 ex:relatedTo $this . }
            WHERE {}
        """ ;
    ] .
"#;

        let data = r#"@prefix ex: <http://example.com/ns#> .

ex:item a ex:Thing .
"#;

        let validator = build_validator(shapes, data);
        let context = validator.context();
        let config = InferenceConfig {
            error_on_blank_nodes: true,
            ..InferenceConfig::default()
        };
        let err = run_inference(context, config).expect_err("should error on blank nodes");
        match err {
            InferenceError::BlankNodeProduced(_) => {}
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn property_shape_rules_execute() {
        let shapes = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .

ex:PropRuleShape a sh:PropertyShape ;
    sh:path ex:value ;
    sh:targetNode ex:Focus ;
    sh:rule [
        a sh:TripleRule ;
        sh:subject sh:this ;
        sh:predicate ex:tag ;
        sh:object "derived" ;
    ] .
"#;

        let data = r#"@prefix ex: <http://example.com/ns#> .

ex:Focus ex:value "foo" .
"#;

        let validator = build_validator(shapes, data);
        let context = validator.context();
        let outcome = run_inference(context, InferenceConfig::default()).expect("inference");
        assert_eq!(outcome.triples_added, 1);
        assert_eq!(outcome.inferred_quads.len(), 1);

        let predicate = NamedNode::new("http://example.com/ns#tag").unwrap();
        let quad = Quad::new(
            NamedNode::new("http://example.com/ns#Focus").unwrap(),
            predicate.clone(),
            Term::Literal(Literal::new_simple_literal("derived")),
            GraphName::NamedNode(context.data_graph_iri.clone()),
        );
        assert!(context
            .backend
            .store()
            .contains(quad.as_ref())
            .expect("quad lookup"));
    }

    #[test]
    fn delta_driven_rules_wake_predicate_target_rules() {
        let shapes = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .

ex:BaseShape a sh:NodeShape ;
    sh:targetClass ex:Base ;
    sh:rule [
        a sh:SPARQLRule ;
        sh:order 0 ;
        sh:construct """
            PREFIX ex: <http://example.com/ns#>
            CONSTRUCT { $this ex:flag "derived" . }
            WHERE { $this ex:marker "seeded" . }
        """ ;
    ] ;
    sh:rule [
        a sh:TripleRule ;
        sh:order 1 ;
        sh:subject sh:this ;
        sh:predicate ex:marker ;
        sh:object "seeded" ;
    ] .
"#;

        let data = r#"@prefix ex: <http://example.com/ns#> .

ex:item a ex:Base .
"#;

        let validator = build_validator(shapes, data);
        let context = validator.context();
        let outcome = run_inference(context, InferenceConfig::default()).expect("inference");
        assert_eq!(outcome.triples_added, 2);
        assert!(outcome.iterations_executed >= 2);

        let marker_quad = Quad::new(
            NamedNode::new("http://example.com/ns#item").unwrap(),
            NamedNode::new("http://example.com/ns#marker").unwrap(),
            Term::Literal(Literal::new_simple_literal("seeded")),
            GraphName::NamedNode(context.data_graph_iri.clone()),
        );
        assert!(context.contains_quad(&marker_quad).expect("quad lookup"));

        let flag_quad = Quad::new(
            NamedNode::new("http://example.com/ns#item").unwrap(),
            NamedNode::new("http://example.com/ns#flag").unwrap(),
            Term::Literal(Literal::new_simple_literal("derived")),
            GraphName::NamedNode(context.data_graph_iri.clone()),
        );
        assert!(context.contains_quad(&flag_quad).expect("quad lookup"));
    }

    #[test]
    fn semi_naive_inference_reaches_transitive_closure() {
        let shapes = r#"@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example.com/ns#> .

ex:NodeShape a sh:NodeShape ;
    sh:targetClass ex:Node ;
    sh:rule [
        a sh:SPARQLRule ;
        sh:construct """
            PREFIX ex: <http://example.com/ns#>
            CONSTRUCT { $this ex:reachable ?next . }
            WHERE { $this ex:next ?next . }
        """ ;
    ] ;
    sh:rule [
        a sh:SPARQLRule ;
        sh:construct """
            PREFIX ex: <http://example.com/ns#>
            CONSTRUCT { $this ex:reachable ?reach . }
            WHERE {
                $this ex:next ?mid .
                ?mid ex:reachable ?reach .
            }
        """ ;
    ] .
"#;

        let data = r#"@prefix ex: <http://example.com/ns#> .

ex:a a ex:Node ; ex:next ex:b .
ex:b a ex:Node ; ex:next ex:c .
ex:c a ex:Node ; ex:next ex:d .
ex:d a ex:Node .
"#;

        let validator = build_validator(shapes, data);
        let context = validator.context();
        let outcome = run_inference(context, InferenceConfig::default()).expect("inference");
        assert_eq!(outcome.triples_added, 6);
        assert!(outcome.iterations_executed >= 2);

        let reachable = NamedNode::new("http://example.com/ns#reachable").unwrap();
        for (subject, object) in [
            ("http://example.com/ns#a", "http://example.com/ns#b"),
            ("http://example.com/ns#a", "http://example.com/ns#c"),
            ("http://example.com/ns#a", "http://example.com/ns#d"),
            ("http://example.com/ns#b", "http://example.com/ns#c"),
            ("http://example.com/ns#b", "http://example.com/ns#d"),
            ("http://example.com/ns#c", "http://example.com/ns#d"),
        ] {
            let quad = Quad::new(
                NamedNode::new(subject).unwrap(),
                reachable.clone(),
                NamedNode::new(object).unwrap(),
                GraphName::NamedNode(context.data_graph_iri.clone()),
            );
            assert!(context.contains_quad(&quad).expect("quad lookup"));
        }
    }
}
