use crate::context::{Context, ValidationContext};
use crate::report::ValidationReportBuilder;

use crate::types::ID;
use crate::shape::{NodeShape, PropertyShape, ValidateShape};
use oxigraph::model::Term;
use oxigraph::sparql::{Query, QueryOptions, QueryResults, Variable};

impl ValidateShape for NodeShape {
    fn validate(
        &self,
        context: &ValidationContext,
        rb: &mut ValidationReportBuilder,
    ) -> Result<(), String> {
        // first gather all of the targets
        let target_contexts = self
            .targets()
            .iter()
            .map(|t| t.get_target_nodes(context, *self.identifier()))
            .flatten();
        let mut target_contexts: Vec<_> = target_contexts.collect(); // Made mutable

        for target_context in target_contexts.iter_mut() { // Iterate mutably
            target_context.record_node_shape_visit(*self.identifier()); // Record NodeShape visit
            // for each target, validate the constraints
            for constraint_id in self.constraints() { // constraint_id is &ComponentID
                let comp = context
                    .get_component_by_id(constraint_id)
                    .ok_or_else(|| format!("Component not found: {}", constraint_id))?;

                // Call the component's own validation logic.
                // It now takes component_id, &mut Context, &ValidationContext
                // and returns Result<ComponentValidationResult, String>
                match comp.validate(*constraint_id, target_context, context) { // Pass mutably
                    Ok(_validation_result) => {
                        // If the component is a PropertyConstraint, then we need to
                        // trigger the validation of the referenced PropertyShape.
                        // The PropertyConstraintComponent's validate method itself no longer does this.
                        if let crate::components::Component::PropertyConstraint(pc_comp) = comp {
                            let prop_shape = context
                                .get_prop_shape_by_id(pc_comp.shape()) // Use accessor
                                .ok_or_else(|| {
                                    // This case should ideally be caught by pc_comp.validate if it checks existence
                                    format!("Property shape not found for ID: {}", pc_comp.shape()) // Use accessor
                                })?;
                            
                            // PropertyShape::validate now takes &mut Context, &ValidationContext, &mut ValidationReportBuilder
                            // target_context is the correct context to pass here, mutably.
                            if let Err(e) = prop_shape.validate(target_context, context, rb) { // Pass mutably
                                // Errors from PropertyShape::validate itself (e.g. query parsing, or its own components failing)
                                rb.add_error(target_context, e);
                            }
                        }
                        // For other component types, if their .validate passed, no further action here.
                        // Any "failure" for them would have been an Err(String) from their validate method, caught below.
                    }
                    Err(e) => {
                        // This error 'e' comes from the component's own validate method.
                        // NodeShape is responsible for adding this to the report builder.
                        rb.add_error(target_context, e);
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
        rb: &mut ValidationReportBuilder,
    ) -> Result<(), String> {
        focus_context.record_property_shape_visit(*self.identifier()); // Record PropertyShape visit
        // The loop is removed as we now operate on a single focus_context.
        // to get the set of value nodes.

        let focus_node_term = focus_context.focus_node();
        let sparql_path = self.sparql_path();

        let query_str = format!(
            "SELECT ?valueNode WHERE {{ {} {} ?valueNode . }}",
            focus_node_term.to_string(), sparql_path
        );

        let query = match Query::parse(&query_str, None) {
            Ok(q) => q,
            Err(e) => {
                return Err(format!(
                    "Failed to parse query for PropertyShape {}: {}",
                    self.identifier(),
                    e
                ));
            }
        };

        let results = match context.store().query_opt(query, QueryOptions::default()) {
            Ok(r) => r,
            Err(e) => {
                return Err(format!(
                    "Failed to execute query for PropertyShape {}: {}",
                    self.identifier(),
                    e
                ));
            }
        };

        let value_nodes_vec: Vec<Term> = match results { // Renamed to avoid conflict
            QueryResults::Solutions(solutions) => {
                let value_node_var = Variable::new_unchecked("valueNode");
                solutions
                    .filter_map(Result::ok)
                    .filter_map(|solution| solution.get(&value_node_var).map(|term| term.clone()))
                    .collect()
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

        if value_nodes_vec.len() > 0 {
            println!("Validating PropertyShape with identifier: {}", self.identifier());
            println!("Path: {:?}", self.sparql_path());
        }

        let value_nodes_opt = if value_nodes_vec.is_empty() { // Renamed to avoid conflict
            None
        } else {
            Some(value_nodes_vec)
        };

        let mut value_node_context = Context::new( // Made mutable
            focus_node_term.clone(),
            Some(self.path().clone()), // PShapePath from self.path()
            value_nodes_opt, // Use the renamed Option<Vec<Term>>
            ID(self.identifier().0) // Source shape is this PropertyShape
        );

        for constraint_id in self.constraints() { // constraint_id is &ComponentID
            let component = context
                .get_component_by_id(constraint_id)
                .ok_or_else(|| format!("Component not found: {}", constraint_id))?;
            
            // Call the component's own validation logic.
            // It now takes component_id, &mut Context, &ValidationContext
            // and returns Result<ComponentValidationResult, String>
            match component.validate(*constraint_id, &mut value_node_context, context) { // Pass mutably
                Ok(_validation_result) => {
                    // If a component's validate passes, no direct error to add to rb here by PropertyShape.
                    // The component itself passed. If it were to cause a validation failure
                    // (e.g. MinCount), its validate method would return Err(String).
                }
                Err(e) => {
                    // This error 'e' comes from the component's own validate method.
                    // PropertyShape is responsible for adding this to the report builder.
                    rb.add_error(&value_node_context, e);
                }
            }
        }
        Ok(())
    }
}
