use crate::context::{Context, SourceShape, ValidationContext};
use crate::report::ValidationReportBuilder;
use crate::runtime::{ComponentValidationResult, ToSubjectRef};
use crate::shape::{NodeShape, PropertyShape, ValidateShape};
use crate::sparql::SparqlExecutor;
use crate::types::{PropShapeID, TraceItem};
use log::{debug, info};
use oxigraph::model::{Literal, Term};
use oxigraph::sparql::{QueryResults, Variable};
use std::collections::{HashMap, HashSet, VecDeque};

pub(crate) fn validate(context: &ValidationContext) -> Result<ValidationReportBuilder, String> {
    let mut report_builder = ValidationReportBuilder::new();
    // Validate all node shapes
    for shape in context.model.node_shapes.values() {
        shape.process_targets(context, &mut report_builder)?;
    }
    // Validate all property shapes
    for shape in context.model.prop_shapes.values() {
        shape.process_targets(context, &mut report_builder)?;
    }
    Ok(report_builder)
}

fn canonicalize_value_nodes(
    validation_context: &ValidationContext,
    shape: &PropertyShape,
    focus_node: &Term,
    mut nodes: Vec<Term>,
) -> Vec<Term> {
    if nodes.is_empty() {
        return nodes;
    }

    let predicate = match shape.path_term() {
        Term::NamedNode(nn) => nn,
        _ => return nodes,
    };

    let subject = match focus_node.try_to_subject_ref() {
        Ok(subject) => subject,
        Err(_) => return nodes,
    };

    let raw_objects: Vec<Term> = validation_context
        .model
        .store()
        .quads_for_pattern(
            Some(subject),
            Some(predicate.as_ref()),
            None,
            Some(validation_context.data_graph_iri_ref()),
        )
        .filter_map(Result::ok)
        .map(|q| q.object)
        .collect();

    if raw_objects.is_empty() && validation_context.model.original_values.is_none() {
        return nodes;
    }

    let mut exact_matches: HashSet<Term> = raw_objects.iter().cloned().collect();
    let mut literals_by_signature: HashMap<(String, Option<String>), VecDeque<Term>> =
        HashMap::new();

    for term in &raw_objects {
        if let Term::Literal(lit) = term {
            let key = literal_signature(lit);
            literals_by_signature
                .entry(key)
                .or_insert_with(VecDeque::new)
                .push_back(term.clone());
        }
    }

    let original_index = validation_context.model.original_values.as_ref();

    for node in &mut nodes {
        let current = node.clone();

        if let Term::Literal(ref lit) = current {
            if let Some(index) = original_index {
                if let Some(original) = index.resolve_literal(focus_node, &predicate, lit) {
                    if original != current {
                        exact_matches.remove(&original);
                        *node = original;
                        continue;
                    }
                }
            }
        }

        if exact_matches.remove(&current) {
            continue;
        }

        if let Term::Literal(ref lit) = current {
            if let Some(term) =
                lookup_by_signature(&mut literals_by_signature, &mut exact_matches, lit)
            {
                *node = term;
            }
        }
    }

    nodes
}

fn literal_signature(lit: &Literal) -> (String, Option<String>) {
    (
        lit.value().to_string(),
        lit.language().map(|lang| lang.to_ascii_lowercase()),
    )
}

fn lookup_by_signature(
    buckets: &mut HashMap<(String, Option<String>), VecDeque<Term>>,
    exact_matches: &mut HashSet<Term>,
    lit: &Literal,
) -> Option<Term> {
    let key = literal_signature(lit);
    if let Some(queue) = buckets.get_mut(&key) {
        if let Some(term) = queue.pop_front() {
            exact_matches.remove(&term);
            return Some(term);
        }
    }
    None
}

impl ValidateShape for NodeShape {
    fn process_targets(
        &self,
        context: &ValidationContext,
        report_builder: &mut ValidationReportBuilder,
    ) -> Result<(), String> {
        if self.is_deactivated() {
            return Ok(());
        }
        // first gather all of the targets
        let mut target_contexts = HashSet::new();
        for target in self.targets.iter() {
            info!(
                "get targets from target: {:?} on shape {}",
                target,
                self.identifier()
            );
            target_contexts.extend(
                target.get_target_nodes(context, SourceShape::NodeShape(*self.identifier()))?,
            );
        }

        for mut target_context in target_contexts.into_iter() {
            let trace_index = {
                let mut traces = context.execution_traces.borrow_mut();
                traces.push(Vec::new());
                traces.len() - 1
            };
            target_context.set_trace_index(trace_index);

            {
                let mut traces = context.execution_traces.borrow_mut();
                let trace = &mut traces[trace_index];
                trace.push(TraceItem::NodeShape(*self.identifier())); // Record NodeShape visit

                // for each target, validate the constraints
                let constraints = self.constraints();
                debug!(
                    "Node shape {} has {} constraints",
                    self.identifier(),
                    constraints.len()
                );
                for constraint_id in constraints {
                    debug!(
                        "Evaluating node shape constraint {} for shape {}",
                        constraint_id,
                        self.identifier()
                    );
                    // constraint_id is &ComponentID
                    let comp = context
                        .get_component(constraint_id)
                        .ok_or_else(|| format!("Component not found: {}", constraint_id))?;

                    // Call the component's own validation logic.
                    match comp.validate(*constraint_id, &mut target_context, context, trace) {
                        Ok(validation_results) => {
                            for result in validation_results {
                                if let ComponentValidationResult::Fail(ctx, failure) = result {
                                    report_builder.add_failure(&ctx, failure);
                                }
                            }
                        }
                        Err(e) => {
                            // This is a processing error, not a validation failure. Propagate it.
                            return Err(e);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl ValidateShape for PropertyShape {
    fn process_targets(
        &self,
        context: &ValidationContext,
        report_builder: &mut ValidationReportBuilder,
    ) -> Result<(), String> {
        if self.is_deactivated() {
            return Ok(());
        }
        // first gather all of the targets
        let mut target_contexts = HashSet::new();
        for target in self.targets.iter() {
            info!(
                "get targets from target: {:?} on shape {}",
                target,
                self.identifier()
            );
            target_contexts.extend(
                target.get_target_nodes(context, SourceShape::PropertyShape(*self.identifier()))?,
            );
        }

        for mut target_context in target_contexts.into_iter() {
            let trace_index = {
                let mut traces = context.execution_traces.borrow_mut();
                traces.push(Vec::new());
                traces.len() - 1
            };
            target_context.set_trace_index(trace_index);

            {
                let mut traces = context.execution_traces.borrow_mut();
                let trace = &mut traces[trace_index];

                match self.validate(&mut target_context, context, trace) {
                    Ok(validation_results) => {
                        for result in validation_results {
                            if let ComponentValidationResult::Fail(ctx, failure) = result {
                                report_builder.add_failure(&ctx, failure);
                            }
                        }
                    }
                    Err(e) => {
                        // This is a processing error, not a validation failure. Propagate it.
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }
}

impl PropertyShape {
    /// Validates a context against this property shape.
    ///
    /// This involves finding the value nodes for the property shape's path from the
    /// focus node in the `focus_context`, and then validating those value nodes
    /// against all the constraints of this property shape.
    pub(crate) fn validate(
        &self,
        focus_context: &mut Context,
        context: &ValidationContext,
        trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        if self.is_deactivated() {
            return Ok(vec![]);
        }
        trace.push(TraceItem::PropertyShape(*self.identifier()));

        let mut all_results: Vec<ComponentValidationResult> = Vec::new();

        // If the incoming context has value nodes, those are our focus nodes (for nested property shapes).
        // Otherwise, the focus node of the incoming context is our single focus node (for top-level property shapes).
        let focus_nodes_for_this_shape = if let Some(value_nodes) = focus_context.value_nodes() {
            value_nodes.clone()
        } else {
            vec![focus_context.focus_node().clone()]
        };

        for focus_node in focus_nodes_for_this_shape {
            let sparql_path = self.sparql_path();
            let query_str = format!(
                "SELECT DISTINCT ?valueNode WHERE {{ {} {} ?valueNode . }}",
                focus_node.to_string(),
                sparql_path
            );

            let prepared = context
                .model
                .sparql
                .prepared_query(&query_str)
                .map_err(|e| {
                    format!(
                        "Failed to prepare query for PropertyShape {}: {}",
                        self.identifier(),
                        e
                    )
                })?;

            let results = context
                .model
                .sparql
                .execute_with_substitutions(&query_str, &prepared, context.model.store(), &[])
                .map_err(|e| {
                    format!(
                        "Failed to execute query for PropertyShape {}: {}",
                        self.identifier(),
                        e
                    )
                })?;

            let value_nodes_vec: Vec<Term> = match results {
                QueryResults::Solutions(solutions) => {
                    let value_node_var = Variable::new("valueNode")
                        .map_err(|e| format!("Internal error creating SPARQL variable: {}", e))?;

                    let mut nodes = Vec::new();
                    for solution_res in solutions {
                        let solution = solution_res.map_err(|e| e.to_string())?;
                        if let Some(term) = solution.get(&value_node_var) {
                            nodes.push(term.clone());
                        } else {
                            return Err(format!(
                                "Missing valueNode in solution for PropertyShape {}",
                                self.identifier()
                            ));
                        }
                    }
                    canonicalize_value_nodes(context, self, &focus_node, nodes)
                }
                QueryResults::Boolean(_) => {
                    return Err(format!(
                        "Unexpected boolean result for PropertyShape {} query",
                        self.identifier()
                    ));
                }
                QueryResults::Graph(_) => {
                    return Err(format!(
                        "Unexpected graph result for PropertyShape {} query",
                        self.identifier()
                    ));
                }
            };

            let value_nodes_opt = if value_nodes_vec.is_empty() {
                None
            } else {
                Some(value_nodes_vec)
            };

            let mut constraint_validation_context = Context::new(
                focus_node.clone(),
                Some(self.path().clone()),
                value_nodes_opt,
                SourceShape::PropertyShape(PropShapeID(self.identifier().0)),
                focus_context.trace_index(),
            );

            let constraints = self.constraints();
            debug!(
                "Property shape {} has {} constraints",
                self.identifier(),
                constraints.len()
            );
            for constraint_id in constraints {
                debug!(
                    "Evaluating property shape constraint {} for shape {}",
                    constraint_id,
                    self.identifier()
                );
                let component = context
                    .get_component(constraint_id)
                    .ok_or_else(|| format!("Component not found: {}", constraint_id))?;

                match component.validate(
                    *constraint_id,
                    &mut constraint_validation_context,
                    context,
                    trace,
                ) {
                    Ok(results) => {
                        all_results.extend(results);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        }

        Ok(all_results)
    }
}
