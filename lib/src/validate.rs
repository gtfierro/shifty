use crate::context::{Context, ValidationContext};
use crate::report::ValidationReportBuilder;

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
            .map(|t| t.get_target_nodes(context))
            .flatten();
        let target_contexts: Vec<_> = target_contexts.collect();

        if target_contexts.len() > 0 {
            println!("Validating NodeShape with identifier: {}", self.identifier());
            println!("Targets: {:?}", target_contexts.len());
        }

        for target_context in target_contexts { // Renamed 'target' to 'target_context' for clarity
            // for each target, validate the constraints
            for constraint in self.constraints() {
                let comp = context
                    .get_component_by_id(constraint)
                    .ok_or_else(|| format!("Component not found: {}", constraint))?;
                // Pass a single context reference
                if let Err(e) = comp.validate(&target_context, context, rb) {
                    rb.add_error(&target_context, e);
                }
            }
        }
        Ok(())
    }
}

impl PropertyShape {
    pub fn validate(
        &self,
        // Changed to a single Context reference
        focus_context: &Context,
        context: &ValidationContext,
        rb: &mut ValidationReportBuilder,
    ) -> Result<(), String> {
        // The loop is removed as we now operate on a single focus_context.
        // to get the set of value nodes.
        println!("Validating PropertyShape with identifier: {}", self.identifier());
        println!("Path: {:?}", self.sparql_path());

        let focus_node_term = focus_context.focus_node();
        let sparql_path = self.sparql_path();

        let query_str = format!(
            "SELECT ?valueNode WHERE {{ {} {} ?valueNode . }}",
            focus_node_term.to_string(), sparql_path
        );
        println!("Executing query: {}", query_str);

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

        let value_nodes_opt = if value_nodes_vec.is_empty() { // Renamed to avoid conflict
            None
        } else {
            Some(value_nodes_vec)
        };

        let value_node_context = Context::new(
            focus_node_term.clone(),
            Some(self.path().clone()), // PShapePath from self.path()
            value_nodes_opt // Use the renamed Option<Vec<Term>>
        );

        for constraint_id in self.constraints() {
            let component = context
                .get_component_by_id(constraint_id)
                .ok_or_else(|| format!("Component not found: {}", constraint_id))?;
            // Pass a single context reference
            if let Err(e) = component.validate(&value_node_context, context, rb) {
                rb.add_error(&value_node_context, e);
            }
        }
        Ok(())
    }
}
