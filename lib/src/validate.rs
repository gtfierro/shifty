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
                // and returns Result<ComponentValidationResult, String>
                match comp.validate(*constraint_id, &mut target_context, context) {
                    Ok(validation_result) => {
                        use crate::components::ComponentValidationResult;
                        match validation_result {
                            ComponentValidationResult::Pass(_) => {
                                // Component passed, do nothing.
                            }
                            ComponentValidationResult::SubShape(results) => {
                                // A sub-shape validation produced results. Add them to the report.
                                for (ctx, err) in results {
                                    rb.add_error(&ctx, err);
                                }
                            }
                            ComponentValidationResult::Fail(failure) => {
                                // The component failed, add the failure message to the report.
                                rb.add_error(&target_context, failure.message);
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
        // Changed to a single mutable Context reference
        focus_context: &mut Context, // Changed to &mut Context
        context: &ValidationContext,
    ) -> Result<Vec<(Context, String)>, String> {
        focus_context.record_property_shape_visit(*self.identifier()); // Record PropertyShape visit

        let mut validation_results = Vec::new();

        // The loop is removed as we now operate on a single focus_context.
        // to get the set of value nodes.

        let focus_node_term = focus_context.focus_node();
        let sparql_path = self.sparql_path();

        let query_str = format!(
            "SELECT DISTINCT ?valueNode WHERE {{ {} {} ?valueNode . }}",
            focus_node_term.to_string(),
            sparql_path
        );

        //println!(
        //    "Executing SPARQL query for PropertyShape {}: {}",
        //    self.identifier(),
        //    query_str
        //);
        let mut query = Query::parse(&query_str, None).map_err(|e| {
            format!(
                "Failed to parse query for PropertyShape {}: {}",
                self.identifier(),
                e
            )
        })?;
        query.dataset_mut().set_default_graph_as_union();

        //println!("num triples in focus context: {}", context.store().len().unwrap());
        // print out triples
        //for triple in context.store().quads_for_pattern(None, None, None, None) {
        //    println!("Triple: {:?}", triple);
        //}
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
            // Renamed to avoid conflict
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

        //if !value_nodes_vec.is_empty() {
        //    println!(
        //        "Validating PropertyShape with identifier: {}",
        //        self.identifier()
        //    );
        //    println!("Path: {:?}", self.sparql_path());
        //}

        let value_nodes_opt = if value_nodes_vec.is_empty() {
            // Renamed to avoid conflict
            None
        } else {
            Some(value_nodes_vec)
        };

        let mut value_node_context = Context::new(
            // Made mutable
            focus_node_term.clone(),
            Some(self.path().clone()), // PShapePath from self.path()
            value_nodes_opt,           // Use the renamed Option<Vec<Term>>
            SourceShape::PropertyShape(PropShapeID(self.identifier().0)),
        );

        for constraint_id in self.constraints() {
            // constraint_id is &ComponentID
            let component = context
                .get_component_by_id(constraint_id)
                .ok_or_else(|| format!("Component not found: {}", constraint_id))?;

            // Call the component's own validation logic.
            // It now takes component_id, &mut Context, &ValidationContext
            // and returns Result<ComponentValidationResult, String>
            match component.validate(*constraint_id, &mut value_node_context, context) {
                Ok(validation_result) => {
                    use crate::components::ComponentValidationResult;
                    match validation_result {
                        ComponentValidationResult::Pass(_) => {
                            // Component passed, do nothing.
                        }
                        ComponentValidationResult::SubShape(results) => {
                            // A sub-shape validation produced results. Add them to the report.
                            validation_results.extend(results);
                        }
                        ComponentValidationResult::Fail(failure) => {
                            // The component failed, add the failure message to the report.
                            validation_results
                                .push((value_node_context.clone(), failure.message));
                        }
                    }
                }
                Err(e) => {
                    // This error 'e' comes from the component's own validate method.
                    // PropertyShape is responsible for adding this to the report builder.
                    validation_results.push((value_node_context.clone(), e));
                }
            }
        }
        Ok(validation_results)
    }
}
