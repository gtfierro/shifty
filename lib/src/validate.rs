use crate::context::{ValidationContext, Context};
use crate::report::ValidationReportBuilder;
use crate::types::{ComponentID, ID};
use crate::shape::{ValidateShape, NodeShape, PropertyShape};
use oxigraph::model::Term;
use oxigraph::sparql::{Query, QueryOptions, QueryResults, Variable};

impl ValidateShape for NodeShape {
    fn validate(
        &self,
        context: &ValidationContext,
        rb: &mut ValidationReportBuilder,
    ) -> Result<(), String> {
        println!("Validating NodeShape with identifier: {}", self.identifier());
        // first gather all of the targets
        println!("targets: {:?}", self.targets());
        let target_contexts = self
            .targets()
            .iter()
            .map(|t| t.get_target_nodes(context))
            .flatten();
        let target_contexts: Vec<_> = target_contexts.collect();

        if target_contexts.len() > 0 {
            println!("Targets: {:?}", target_contexts.len());
        }

        for target in target_contexts {
            // for each target, validate the constraints
            for constraint in self.constraints() {
                let comp = context
                    .get_component_by_id(constraint)
                    .ok_or_else(|| format!("Component not found: {}", constraint))?;
                if let Err(e) = comp.validate(&[&target], context, rb) {
                    rb.add_error(&target, e);
                }
            }
        }
        Ok(())
    }
}

impl PropertyShape {
    pub fn validate(
        &self,
        c: &[&Context],
        context: &ValidationContext,
        rb: &mut ValidationReportBuilder,
    ) -> Result<(), String> {
        // for each context, follow the path from the target_node
        // to get the set of value nodes.
        println!("Validating PropertyShape with identifier: {}", self.identifier());
        println!("Path: {:?}", self.path());

        for focus_context in c {
            let focus_node_term = focus_context.focus_node();
            let sparql_path = self.sparql_path();

            let query_str = format!(
                "SELECT ?valueNode WHERE {{ <{}> {} ?valueNode . }}",
                focus_node_term, sparql_path
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

            let value_nodes: Vec<Term> = match results {
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

            if value_nodes.is_empty() {
                // If there are no value nodes, we might still need to validate constraints
                // like sh:minCount 0, or if the path itself is optional.
                // For now, let's create a context with no value nodes.
                // Some constraints (like minCount) might specifically check for the number of value nodes.
                let value_node_context = Context::new(
                    focus_node_term.clone(),
                    Some(self.path().clone()), // PShapePath from self.path()
                    Some(Vec::new()),
                );
                for constraint_id in self.constraints() {
                    let component = context
                        .get_component_by_id(constraint_id)
                        .ok_or_else(|| format!("Component not found: {}", constraint_id))?;
                    if let Err(e) = component.validate(&[&value_node_context], context, rb) {
                        rb.add_error(&value_node_context, e);
                    }
                }
            } else {
                // Create a single context containing all value nodes for this focus node and path
                let value_node_context = Context::new(
                    focus_node_term.clone(),
                    Some(self.path().clone()), // PShapePath from self.path()
                    Some(value_nodes),
                );

                for constraint_id in self.constraints() {
                    let component = context
                        .get_component_by_id(constraint_id)
                        .ok_or_else(|| format!("Component not found: {}", constraint_id))?;
                    // Pass the single context with all value nodes to each component validator
                    if let Err(e) = component.validate(&[&value_node_context], context, rb) {
                        rb.add_error(&value_node_context, e);
                    }
                }
            }
        }
        Ok(())
    }
}
