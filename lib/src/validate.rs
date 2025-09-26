use crate::context::{Context, SourceShape, ValidationContext};
use crate::report::ValidationReportBuilder;
use crate::runtime::ComponentValidationResult;
use crate::shape::{NodeShape, PropertyShape, ValidateShape};
use crate::types::{PropShapeID, TraceItem};
use log::{debug, info};
use oxigraph::model::Term;
use oxigraph::sparql::{Query, QueryOptions, QueryResults, Variable};
use std::collections::HashSet;

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

impl ValidateShape for NodeShape {
    fn process_targets(
        &self,
        context: &ValidationContext,
        report_builder: &mut ValidationReportBuilder,
    ) -> Result<(), String> {
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

            let mut query = Query::parse(&query_str, None).map_err(|e| {
                format!(
                    "Failed to parse query for PropertyShape {}: {}",
                    self.identifier(),
                    e
                )
            })?;
            query.dataset_mut().set_default_graph_as_union();

            let results = context
                .model
                .store()
                .query_opt(query, QueryOptions::default())
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
                    nodes
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
