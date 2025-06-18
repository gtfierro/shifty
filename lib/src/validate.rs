use crate::components::ComponentValidationResult;
use crate::context::{Context, SourceShape, ValidationContext};
use crate::report::ValidationReportBuilder;
use std::collections::HashSet;

use crate::shape::{NodeShape, PropertyShape, ValidateShape};
use crate::types::PropShapeID;
use log::info;
use oxigraph::model::Term;
use oxigraph::sparql::{Query, QueryOptions, QueryResults, Variable};

impl ValidateShape for NodeShape {
    fn validate(
        &self,
        context: &ValidationContext,
        rb: &mut ValidationReportBuilder,
    ) -> Result<(), String> {
        // first gather all of the targets
        let mut target_contexts = HashSet::new();
        for target in self.targets.iter() {
            info!(
                "get targets from target: {:?} on shape {}",
                target,
                self.identifier()
            );
            target_contexts.extend(target.get_target_nodes(context, *self.identifier())?);
        }

        for mut target_context in target_contexts.into_iter() {
            // Iterate mutably
            target_context.record_node_shape_visit(*self.identifier()); // Record NodeShape visit
                                                                        // for each target, validate the constraints
            for constraint_id in self.constraints() {
                // constraint_id is &ComponentID
                let comp = context
                    .get_component_by_id(constraint_id)
                    .ok_or_else(|| format!("Component not found: {}", constraint_id))?;

                // Call the component's own validation logic.
                // It now takes component_id, &mut Context, &ValidationContext
                // and returns Result<Vec<ComponentValidationResult>, String>
                match comp.validate(*constraint_id, &mut target_context, context) {
                    Ok(validation_results) => {
                        use crate::components::ComponentValidationResult;
                        for result in validation_results {
                            if let ComponentValidationResult::Fail(ctx, failure) = result {
                                rb.add_error(&ctx, failure.message);
                            }
                        }
                    }
                    Err(e) => {
                        // This error 'e' comes from the component's own validate method.
                        // NodeShape is responsible for adding this to the report builder.
                        rb.add_error(&target_context, e);
                    }
                }
            }
        }
        Ok(())
    }
}

impl PropertyShape {
    pub fn validate(
        &self,
        focus_context: &mut Context,
        context: &ValidationContext,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        focus_context.record_property_shape_visit(*self.identifier());

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
                    let value_node_var = Variable::new("valueNode").map_err(|e| {
                        format!("Internal error creating SPARQL variable: {}", e)
                    })?;

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
            );
            constraint_validation_context.execution_trace = focus_context.execution_trace().clone();

            for constraint_id in self.constraints() {
                let component = context
                    .get_component_by_id(constraint_id)
                    .ok_or_else(|| format!("Component not found: {}", constraint_id))?;

                match component.validate(
                    *constraint_id,
                    &mut constraint_validation_context,
                    context,
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
