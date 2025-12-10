use crate::backend::Binding;
use crate::context::{Context, SourceShape, ValidationContext};
use crate::model::rules::{Rule, RuleCondition, SparqlRule, TriplePatternTerm, TripleRule};
use crate::runtime::component::{check_conformance_for_node, ConformanceReport};
use crate::types::{ComponentID, PropShapeID, RuleID, TargetEvalExt, TraceItem, ID};
use log::{debug, info};
use oxigraph::model::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use oxigraph::sparql::{QueryResults, Variable};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

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
    skipped_rules: RefCell<HashSet<RuleID>>,
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
            skipped_rules: RefCell::new(HashSet::new()),
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

        if self.config.trace {
            info!(
                "Starting inference: {} node-shape rule set(s), {} property-shape rule set(s)",
                self.graph.node_shape_rules.len(),
                self.graph.property_shape_rules.len()
            );
        }

        for iteration in 1..=self.config.max_iterations {
            self.context.advanced_target_cache.write().unwrap().clear();
            iterations_executed = iteration;
            let added_this_round = self.apply_rules_once(&mut inferred_quads)?;
            total_added += added_this_round;
            if self.config.trace {
                info!(
                    "Inference iteration {} added {} triple(s)",
                    iteration, added_this_round
                );
            }

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

    fn apply_rules_once(&self, collected: &mut Vec<Quad>) -> Result<usize, InferenceError> {
        let mut iteration_added = 0usize;
        let mut seen_new: HashSet<(Term, NamedNode, Term)> = HashSet::new();

        for (shape_id, rule_ids) in &self.graph.node_shape_rules {
            let Some(shape_ir) = self
                .context
                .shape_ir()
                .node_shapes
                .iter()
                .find(|s| &s.id == shape_id)
            else {
                return Err(InferenceError::Configuration(format!(
                    "Node shape {:?} referenced in rules but missing from IR",
                    shape_id
                )));
            };
            let focus_nodes = self.focus_nodes_for_shape(shape_ir)?;
            if self.config.trace {
                debug!(
                    "Node shape {:?} has {} focus node(s) and {} rule(s)",
                    shape_id,
                    focus_nodes.len(),
                    rule_ids.len()
                );
            }
            for rule_id in rule_ids {
                if self.skipped_rules.borrow().contains(rule_id) {
                    continue;
                }
                let Some(rule) = self.graph.rules.get(rule_id) else {
                    return Err(InferenceError::Configuration(format!(
                        "Rule {:?} referenced by shape {:?} but missing from model",
                        rule_id, shape_id
                    )));
                };
                if rule.is_deactivated() {
                    continue;
                }
                if focus_nodes.is_empty() && shape_ir.targets.is_empty() {
                    if self.config.trace {
                        debug!(
                            "Skipping rule {:?} for node shape {:?} (no targets, no focus nodes)",
                            rule_id, shape_id
                        );
                    }
                    self.skipped_rules.borrow_mut().insert(*rule_id);
                    continue;
                }
                let added = match rule {
                    Rule::Sparql(sparql_rule) => {
                        self.apply_sparql_rule(sparql_rule, &focus_nodes, &mut seen_new, collected)?
                    }
                    Rule::Triple(triple_rule) => {
                        self.apply_triple_rule(triple_rule, &focus_nodes, &mut seen_new, collected)?
                    }
                };
                iteration_added += added;
                if self.config.trace && added > 0 {
                    debug!(
                        "Rule {:?} for node shape {:?} produced {} triple(s)",
                        rule_id, shape_id, added
                    );
                }
            }
        }

        for (shape_id, rule_ids) in &self.graph.property_shape_rules {
            let Some(shape_ir) = self
                .context
                .shape_ir()
                .property_shapes
                .iter()
                .find(|s| &s.id == shape_id)
            else {
                return Err(InferenceError::Configuration(format!(
                    "Property shape {:?} referenced in rules but missing from IR",
                    shape_id
                )));
            };
            let focus_nodes = self.focus_nodes_for_property_shape(shape_ir)?;
            if self.config.trace {
                debug!(
                    "Property shape {:?} has {} focus node(s) and {} rule(s)",
                    shape_id,
                    focus_nodes.len(),
                    rule_ids.len()
                );
            }
            for rule_id in rule_ids {
                if self.skipped_rules.borrow().contains(rule_id) {
                    continue;
                }
                let Some(rule) = self.graph.rules.get(rule_id) else {
                    return Err(InferenceError::Configuration(format!(
                        "Rule {:?} referenced by property shape {:?} but missing from model",
                        rule_id, shape_id
                    )));
                };
                if rule.is_deactivated() {
                    continue;
                }
                if focus_nodes.is_empty() && shape_ir.targets.is_empty() {
                    if self.config.trace {
                        debug!(
                            "Skipping rule {:?} for property shape {:?} (no targets, no focus nodes)",
                            rule_id, shape_id
                        );
                    }
                    self.skipped_rules.borrow_mut().insert(*rule_id);
                    continue;
                }
                let added = match rule {
                    Rule::Sparql(sparql_rule) => {
                        self.apply_sparql_rule(sparql_rule, &focus_nodes, &mut seen_new, collected)?
                    }
                    Rule::Triple(triple_rule) => {
                        self.apply_triple_rule(triple_rule, &focus_nodes, &mut seen_new, collected)?
                    }
                };
                iteration_added += added;
                if self.config.trace && added > 0 {
                    debug!(
                        "Rule {:?} for property shape {:?} produced {} triple(s)",
                        rule_id, shape_id, added
                    );
                }
            }
        }

        Ok(iteration_added)
    }

    fn focus_nodes_for_shape(
        &self,
        shape: &crate::types::NodeShapeIR,
    ) -> Result<Vec<Term>, InferenceError> {
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
        Ok(collected.into_iter().collect())
    }

    fn focus_nodes_for_property_shape(
        &self,
        shape: &crate::types::PropertyShapeIR,
    ) -> Result<Vec<Term>, InferenceError> {
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
        Ok(collected.into_iter().collect())
    }

    fn apply_sparql_rule(
        &self,
        rule: &SparqlRule,
        focus_nodes: &[Term],
        seen_new: &mut HashSet<(Term, NamedNode, Term)>,
        collected: &mut Vec<Quad>,
    ) -> Result<usize, InferenceError> {
        let prepared =
            self.context
                .prepare_query(&rule.query)
                .map_err(|e| InferenceError::RuleExecution {
                    rule_id: rule.id,
                    message: e,
                })?;

        let var_this = Variable::new("this").map_err(|e| InferenceError::RuleExecution {
            rule_id: rule.id,
            message: e.to_string(),
        })?;

        let mut added = 0usize;
        for focus in focus_nodes {
            if !self.conditions_satisfied(rule.id, focus, &rule.condition_shapes)? {
                continue;
            }
            let substitutions: Vec<Binding> = vec![(var_this.clone(), focus.clone())];
            let results = self
                .context
                .execute_prepared(&rule.query, &prepared, &substitutions, false)
                .map_err(|e| InferenceError::RuleExecution {
                    rule_id: rule.id,
                    message: e,
                })?;

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
                    let predicate = triple.predicate;
                    let object_term = triple.object;
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
            } else {
                return Err(InferenceError::RuleExecution {
                    rule_id: rule.id,
                    message: "SPARQL CONSTRUCT rule returned a non-graph result".to_string(),
                });
            }
        }

        Ok(added)
    }

    fn apply_triple_rule(
        &self,
        rule: &TripleRule,
        focus_nodes: &[Term],
        seen_new: &mut HashSet<(Term, NamedNode, Term)>,
        collected: &mut Vec<Quad>,
    ) -> Result<usize, InferenceError> {
        let mut added = 0usize;

        for focus in focus_nodes {
            if !self.conditions_satisfied(rule.id, focus, &rule.condition_shapes)? {
                continue;
            }

            let subjects = self.evaluate_template(rule.id, &rule.subject, focus)?;
            let objects = self.evaluate_template(rule.id, &rule.object, focus)?;

            for subject_term in &subjects {
                for object_term in &objects {
                    if self.record_inferred_triple(
                        rule.id,
                        subject_term.clone(),
                        rule.predicate.clone(),
                        object_term.clone(),
                        seen_new,
                        collected,
                    )? {
                        added += 1;
                    }
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
    ) -> Result<Vec<Term>, InferenceError> {
        match template {
            TriplePatternTerm::This => Ok(vec![focus_node.clone()]),
            TriplePatternTerm::Constant(term) => Ok(vec![term.clone()]),
            TriplePatternTerm::Path(path) => self.evaluate_path(rule_id, path, focus_node),
        }
    }

    fn evaluate_path(
        &self,
        rule_id: RuleID,
        path: &crate::types::Path,
        focus_node: &Term,
    ) -> Result<Vec<Term>, InferenceError> {
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
}
