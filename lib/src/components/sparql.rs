use crate::components::{
    ComponentValidationResult, GraphvizOutput, ToSubjectRef, ValidateComponent, ValidationFailure,
};
use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::named_nodes::SHACL;
use crate::types::{ComponentID, TraceItem};
use ontoenv::api::ResolveTarget;
use oxigraph::model::vocab::xsd;
use oxigraph::model::{Literal, NamedNode, Term};
use oxigraph::sparql::{Query, QueryOptions, QueryResults, Variable};

#[derive(Debug, Clone)]
pub struct SPARQLConstraintComponent {
    pub constraint_node: Term,
}

impl SPARQLConstraintComponent {
    pub fn new(constraint_node: Term) -> Self {
        SPARQLConstraintComponent { constraint_node }
    }
    fn get_sparql_prefixes(&self, context: &ValidationContext) -> Result<String, String> {
        // call context.env()
        let graphid = context
            .env()
            .resolve(ResolveTarget::Graph(context.shape_graph_iri.clone()))
            .unwrap();
        let ont = context
            .env()
            .get_ontology(&graphid)
            .expect("Failed to get ontology for SPARQL constraint prefixes");
        let namespaces = ont.namespace_map();
        // format the namespaces into a String
        // PREFIX pfx: <iri> \n
        // etc...

        let prefix_strs: Vec<String> = namespaces
            .iter()
            .map(|(prefix, iri)| format!("PREFIX {}: <{}>", prefix, iri))
            .collect();
        Ok(prefix_strs.join("\n"))
    }
}

impl GraphvizOutput for SPARQLConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#SPARQLConstraintComponent")
    }

    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String {
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
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
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
                    return Ok(vec![]);
                }
            }
        }

        // 2. Get SELECT query
        let mut query_str = if let Some(quad_res) = context
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

        // Handle sh:prefixes. For now, assuming prefixes are in the query string.
        query_str.insert_str(0, &self.get_sparql_prefixes(context)?);

        // 3. Substitute $PATH for property shapes
        let final_query_str = if let Some(path) = c.result_path() {
            let sparql_path = path.to_sparql_path()?;
            query_str.replace("$PATH", &sparql_path)
        } else {
            query_str
        };

        // 4. Parse query
        let mut query = Query::parse(&final_query_str, None).map_err(|e| {
            format!(
                "Failed to parse SPARQL constraint query for {:?}: {}",
                self.constraint_node, e
            )
        })?;
        query.dataset_mut().set_default_graph_as_union();

        // 5. Pre-bind variables
        let substitutions = vec![(Variable::new_unchecked("this"), c.focus_node().clone())];

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
            let mut validation_results = Vec::new();

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
                .next();

            for solution_res in solutions {
                let solution = solution_res.map_err(|e| e.to_string())?;

                // Check for failure variable
                if let Some(Term::Literal(lit)) = solution.get("failure") {
                    if lit.datatype() == xsd::BOOLEAN && lit.value() == "true" {
                        return Err(format!(
                            "SPARQL constraint failure reported by query for constraint {:?}",
                            self.constraint_node
                        ));
                    }
                }

                let message = if let Some(Term::Literal(lit)) = &message_template {
                    let mut msg = lit.value().to_string();
                    for (var, val) in solution.iter() {
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

                let failed_node = solution
                    .get("value")
                    .cloned()
                    .unwrap_or_else(|| c.focus_node().clone());

                let mut error_context = c.clone();
                error_context.with_value(failed_node.clone());

                let failure = ValidationFailure {
                    component_id,
                    failed_value_node: Some(failed_node),
                    message,
                };
                validation_results.push(ComponentValidationResult::Fail(error_context, failure));
            }

            return Ok(validation_results);
        } else {
            return Err(format!(
                "Unexpected query result type for SPARQL constraint {:?}",
                self.constraint_node
            ));
        }
    }
}
