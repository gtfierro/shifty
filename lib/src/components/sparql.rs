use crate::components::{
    ComponentValidationResult, GraphvizOutput, ToSubjectRef, ValidateComponent,
};
use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::named_nodes::SHACL;
use crate::types::ComponentID;
use oxigraph::model::vocab::xsd;
use oxigraph::model::{Literal, Term};
use oxigraph::sparql::{Query, QueryOptions, QueryResults, Variable};

#[derive(Debug, Clone)]
pub struct SPARQLConstraintComponent {
    pub constraint_node: Term,
}

impl SPARQLConstraintComponent {
    pub fn new(constraint_node: Term) -> Self {
        SPARQLConstraintComponent { constraint_node }
    }
}

impl GraphvizOutput for SPARQLConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        context: &ValidationContext,
    ) -> String {
        let shacl = SHACL::new();
        let subject = self.constraint_node.to_subject_ref();
        let select_query_opt = context
            .store()
            .quads_for_pattern(
                Some(subject),
                Some(shacl.select),
                None,
                Some(context.shape_graph_iri_ref()),
            )
            .next()
            .and_then(|res| res.ok())
            .map(|quad| quad.object);

        let label_str = if let Some(Term::Literal(lit)) = select_query_opt {
            let query_str = lit.value().replace('\\', "\\\\").replace('"', "\\\"");
            format!("SPARQLConstraint: {}", query_str)
        } else {
            format!(
                "SPARQLConstraint: {}",
                format_term_for_label(&self.constraint_node)
            )
        };

        format!(
            "{} [label=\"{}\"];",
            component_id.to_graphviz_id(),
            label_str
        )
    }
}

impl ValidateComponent for SPARQLConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let shacl = SHACL::new();
        let subject = self.constraint_node.to_subject_ref();

        // 1. Check if deactivated
        if let Some(quad_res) = context
            .store()
            .quads_for_pattern(
                Some(subject),
                Some(shacl.deactivated),
                None,
                Some(context.shape_graph_iri_ref()),
            )
            .next()
        {
            if let Ok(quad) = quad_res {
                if quad.object == Term::from(Literal::from(true)) {
                    return Ok(ComponentValidationResult::Pass(component_id));
                }
            }
        }

        // 2. Get SELECT query
        let query_str = if let Some(quad_res) = context
            .store()
            .quads_for_pattern(
                Some(subject),
                Some(shacl.select),
                None,
                Some(context.shape_graph_iri_ref()),
            )
            .next()
        {
            if let Ok(quad) = quad_res {
                if let Term::Literal(lit) = quad.object {
                    lit.value().to_string()
                } else {
                    return Err(format!(
                        "sh:select value must be a literal for constraint {:?}",
                        self.constraint_node
                    ));
                }
            } else {
                return Err(format!(
                    "Error reading sh:select for constraint {:?}",
                    self.constraint_node
                ));
            }
        } else {
            return Err(format!(
                "sh:select not found for constraint {:?}",
                self.constraint_node
            ));
        };

        // TODO: Handle sh:prefixes. For now, assuming prefixes are in the query string.

        // 3. Substitute $PATH for property shapes
        let final_query_str = if let Some(path) = c.path() {
            let sparql_path = path.to_sparql_path()?;
            query_str.replace("$PATH", &sparql_path)
        } else {
            query_str
        };

        // 4. Parse query
        let query = Query::parse(&final_query_str, None).map_err(|e| {
            format!(
                "Failed to parse SPARQL constraint query for {:?}: {}",
                self.constraint_node, e
            )
        })?;

        // 5. Pre-bind variables
        let substitutions = vec![(
            Variable::new_unchecked("this"),
            c.focus_node().clone(),
        )];

        // 6. Execute query
        let results = context
            .store()
            .query_opt_with_substituted_variables(query, QueryOptions::default(), substitutions)
            .map_err(|e| {
                format!(
                    "Failed to execute SPARQL constraint query for {:?}: {}",
                    self.constraint_node, e
                )
            })?;

        // 7. Process results
        if let QueryResults::Solutions(solutions) = results {
            let mut solutions_vec = Vec::new();
            for solution in solutions {
                solutions_vec.push(solution.map_err(|e| e.to_string())?);
            }

            if let Some(first_solution) = solutions_vec.first() {
                // Violation found, use the first solution to generate an error message

                // Check for failure
                if let Some(Term::Literal(lit)) = first_solution.get("failure") {
                    if lit.datatype() == xsd::BOOLEAN && lit.value() == "true" {
                        return Err(format!(
                            "SPARQL constraint failure reported by query for constraint {:?}",
                            self.constraint_node
                        ));
                    }
                }

                // Get message
                let message_template = context
                    .store()
                    .quads_for_pattern(
                        Some(subject),
                        Some(shacl.message),
                        None,
                        Some(context.shape_graph_iri_ref()),
                    )
                    .filter_map(Result::ok)
                    .map(|q| q.object)
                    .next(); // Taking the first message for simplicity

                let message = if let Some(Term::Literal(lit)) = message_template {
                    let mut msg = lit.value().to_string();
                    for (var, val) in first_solution.iter() {
                        let var_name = format!("{{?{}}}", var.as_str());
                        let var_name_dollar = format!("{{${}}}", var.as_str());
                        let val_str = format_term_for_label(val);
                        msg = msg.replace(&var_name, &val_str);
                        msg = msg.replace(&var_name_dollar, &val_str);
                    }
                    msg
                } else {
                    format!(
                        "SPARQL constraint violated for focus node {}",
                        c.focus_node()
                    )
                };

                // NOTE: This implementation returns an error for the first violating solution.
                // The SHACL spec says there should be a validation result for each solution.
                // This would require changes to the validation architecture.
                return Err(message);
            }
        } else {
            return Err(format!(
                "Unexpected query result type for SPARQL constraint {:?}",
                self.constraint_node
            ));
        }

        // No solutions, so validation passes
        Ok(ComponentValidationResult::Pass(component_id))
    }
}
