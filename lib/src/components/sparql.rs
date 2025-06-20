use super::{
    ComponentValidationResult, GraphvizOutput, ToSubjectRef, ValidateComponent, ValidationFailure,
};
use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::named_nodes::SHACL;
use crate::types::{ComponentID, TraceItem};
use ontoenv::api::ResolveTarget;
use oxigraph::model::vocab::xsd;
use oxigraph::model::{Literal, NamedNode, NamedNodeRef, Term};
use oxigraph::sparql::{Query, QueryOptions, QueryResults, Variable};
use std::collections::HashMap;

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

        println!(
            "Executing SPARQL constraint query for {:?}:\n{}",
            self.constraint_node, final_query_str
        );

        // 4. Parse query
        let mut query = Query::parse(&final_query_str, None).map_err(|e| {
            format!(
                "Failed to parse SPARQL constraint query for {:?}: {}",
                self.constraint_node, e
            )
        }).unwrap();
        query.dataset_mut().set_default_graph_as_union();
        println!("Parsed SPARQL query: {:?}", query);

        // 5. Pre-bind variables
        let mut substitutions = vec![(Variable::new_unchecked("this"), c.focus_node().clone())];
        if let Some(current_shape_term) = c.source_shape().get_term(context) {
            substitutions.push((
                Variable::new_unchecked("currentShape"),
                current_shape_term,
            ));
        }
        substitutions.push((
            Variable::new_unchecked("shapesGraph"),
            context.shape_graph_iri.clone().into(),
        ));

        println!(
            "Substitutions for SPARQL constraint query: {:?}",
            substitutions
        );

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
                println!("SPARQL solution: {:?}", solution);

                // Check for failure variable
                if let Some(Term::Literal(lit)) = solution.get("failure") {
                    if lit.datatype() == xsd::BOOLEAN && lit.value() == "true" {
                        return Err(format!(
                            "SPARQL constraint failure reported by query for constraint {:?}",
                            self.constraint_node
                        ));
                    }
                }

                let message = if let Some(message_term) = solution.get("message") {
                    message_term.to_string()
                } else if let Some(Term::Literal(lit)) = &message_template {
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

                let mut error_context = c.clone();
                error_context.with_source_constraint(self.constraint_node.clone());

                let failed_node = if let Some(value_term) = solution.get("value") {
                    // Rule 1: Use the binding for the variable `value`
                    Some(value_term.clone())
                } else {
                    // Rule 2: Use "the value node".
                    // For a node shape, this is the focus node.
                    // For a property shape, there's no single value node, so we leave it empty.
                    if c.source_shape().as_node_id().is_some() {
                        Some(c.focus_node().clone())
                    } else {
                        None
                    }
                };

                if let Some(node) = &failed_node {
                    error_context.with_value(node.clone());
                }

                if let Some(path_term) = solution.get("path") {
                    if matches!(path_term, Term::NamedNode(_)) {
                        error_context.with_result_path(path_term.clone());
                    }
                }

                let failure = ValidationFailure {
                    component_id,
                    failed_value_node: failed_node,
                    message,
                };
                println!(
                    "SPARQL constraint failure for {}: {}",
                    format_term_for_label(&c.focus_node()),
                    failure.message
                );
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

// NEW STRUCTS for SPARQL-based Constraint Components

#[derive(Debug, Clone)]
pub struct Parameter {
    pub path: NamedNode,
    pub optional: bool,
}

#[derive(Debug, Clone)]
pub struct SPARQLValidator {
    pub query: String,
    pub is_ask: bool,
    pub messages: Vec<Term>,
}

#[derive(Debug, Clone)]
pub struct CustomConstraintComponentDefinition {
    pub iri: NamedNode,
    pub parameters: Vec<Parameter>,
    pub validator: Option<SPARQLValidator>,
    pub node_validator: Option<SPARQLValidator>,
    pub property_validator: Option<SPARQLValidator>,
}

#[derive(Debug, Clone)]
pub struct CustomConstraintComponent {
    pub definition: CustomConstraintComponentDefinition,
    pub parameter_values: HashMap<NamedNode, Vec<Term>>,
}

impl CustomConstraintComponent {
    pub(crate) fn local_name(&self) -> String {
        local_name(&self.definition.iri)
    }
}

fn local_name(iri: &NamedNode) -> String {
    let iri_str = iri.as_str();
    if let Some(hash_idx) = iri_str.rfind('#') {
        iri_str[hash_idx + 1..].to_string()
    } else if let Some(slash_idx) = iri_str.rfind('/') {
        if slash_idx < iri_str.len() - 1 {
            iri_str[slash_idx + 1..].to_string()
        } else {
            // trailing slash
            let end = slash_idx;
            let mut start = slash_idx;
            if let Some(prev_slash) = iri_str[..end].rfind('/') {
                start = prev_slash + 1;
            }
            iri_str[start..end].to_string()
        }
    } else {
        iri_str.to_string()
    }
}

pub(crate) fn parse_custom_constraint_components(
    context: &ValidationContext,
) -> (
    HashMap<NamedNode, CustomConstraintComponentDefinition>,
    HashMap<NamedNode, Vec<NamedNode>>,
) {
    let mut definitions = HashMap::new();
    let mut param_to_component: HashMap<NamedNode, Vec<NamedNode>> = HashMap::new();

    let query = "SELECT ?cc WHERE { ?cc a sh:ConstraintComponent }";
    if let Ok(QueryResults::Solutions(solutions)) =
        context.store().query_opt(query, QueryOptions::default())
    {
        for solution in solutions {
            if let Some(Term::NamedNode(cc_iri)) = solution.unwrap().get("cc") {
                let mut parameters = vec![];
                let param_query = format!(
                    "SELECT ?param ?path ?optional WHERE {{ <{}> sh:parameter ?param . ?param sh:path ?path . OPTIONAL {{ ?param sh:optional ?optional }} }}",
                    cc_iri.as_str()
                );

                if let Ok(QueryResults::Solutions(param_solutions)) = context
                    .store()
                    .query_opt(&param_query, QueryOptions::default())
                {
                    for param_solution in param_solutions {
                        if let Ok(p_sol) = param_solution {
                            if let Some(Term::NamedNode(path)) = p_sol.get("path") {
                                let optional = p_sol
                                    .get("optional")
                                    .and_then(|t| match t {
                                        Term::Literal(l) => l.value().parse::<bool>().ok(),
                                        _ => None,
                                    })
                                    .unwrap_or(false);
                                parameters.push(Parameter {
                                    path: path.clone(),
                                    optional,
                                });
                                param_to_component
                                    .entry(path.clone())
                                    .or_default()
                                    .push(cc_iri.clone());
                            }
                        }
                    }
                }

                let mut validator = None;
                let mut node_validator = None;
                let mut property_validator = None;

                // Helper to parse a validator
                let parse_validator = |v_term: &Term, is_ask: bool| -> Option<SPARQLValidator> {
                    let query_prop = if is_ask { "ask" } else { "select" };
                    let v_query = format!(
                        "SELECT ?query (GROUP_CONCAT(?msg; separator='|||') as ?messages) WHERE {{ <{}> sh:{} ?query . OPTIONAL {{ <{}> sh:message ?msg }} }} GROUP BY ?query",
                        v_term, query_prop, v_term
                    );

                    if let Ok(QueryResults::Solutions(v_solutions)) =
                        context.store().query_opt(&v_query, QueryOptions::default())
                    {
                        if let Some(Ok(v_sol)) = v_solutions.into_iter().next() {
                            if let Some(Term::Literal(query_lit)) = v_sol.get("query") {
                                let messages = v_sol.get("messages").map_or(vec![], |t| {
                                    if let Term::Literal(lit) = t {
                                        // This is a hack because group_concat doesn't preserve term type
                                        // A proper implementation would parse the list of messages properly
                                        vec![Term::Literal(Literal::new_simple_literal(lit.value()))]
                                    } else {
                                        vec![]
                                    }
                                });
                                return Some(SPARQLValidator {
                                    query: query_lit.value().to_string(),
                                    is_ask,
                                    messages,
                                });
                            }
                        }
                    }
                    None
                };

                let validator_prop =
                    NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#validator");
                let node_validator_prop =
                    NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#nodeValidator");
                let property_validator_prop =
                    NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#propertyValidator");

                if let Some(v_term) = context
                    .store()
                    .quads_for_pattern(
                        Some(cc_iri.as_ref().into()),
                        Some(validator_prop),
                        None,
                        Some(context.shape_graph_iri_ref()),
                    )
                    .filter_map(Result::ok)
                    .map(|q| q.object)
                    .next()
                {
                    validator = parse_validator(&v_term, true);
                }
                if let Some(v_term) = context
                    .store()
                    .quads_for_pattern(
                        Some(cc_iri.as_ref().into()),
                        Some(node_validator_prop),
                        None,
                        Some(context.shape_graph_iri_ref()),
                    )
                    .filter_map(Result::ok)
                    .map(|q| q.object)
                    .next()
                {
                    node_validator = parse_validator(&v_term, false);
                }
                if let Some(v_term) = context
                    .store()
                    .quads_for_pattern(
                        Some(cc_iri.as_ref().into()),
                        Some(property_validator_prop),
                        None,
                        Some(context.shape_graph_iri_ref()),
                    )
                    .filter_map(Result::ok)
                    .map(|q| q.object)
                    .next()
                {
                    property_validator = parse_validator(&v_term, false);
                }

                definitions.insert(
                    cc_iri.clone(),
                    CustomConstraintComponentDefinition {
                        iri: cc_iri.clone(),
                        parameters,
                        validator,
                        node_validator,
                        property_validator,
                    },
                );
            }
        }
    }

    (definitions, param_to_component)
}

impl GraphvizOutput for CustomConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let label = format!(
            "Custom: {}\\n{}",
            local_name(&self.definition.iri),
            self.parameter_values
                .iter()
                .map(|(p, vs)| format!(
                    "{}: {}",
                    local_name(p),
                    vs.iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .collect::<Vec<_>>()
                .join("\\n")
        );
        format!(
            "  {} [label=\"{}\", shape=box];",
            component_id.to_graphviz_id(),
            label
        )
    }

    fn component_type(&self) -> NamedNode {
        self.definition.iri.clone()
    }
}

impl ValidateComponent for CustomConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let is_prop_shape = c.source_shape().as_prop_id().is_some();

        let validator = if is_prop_shape {
            self.definition
                .property_validator
                .as_ref()
                .or(self.definition.validator.as_ref())
        } else {
            self.definition
                .node_validator
                .as_ref()
                .or(self.definition.validator.as_ref())
        };

        let validator = match validator {
            Some(v) => v,
            None => return Ok(vec![]), // No suitable validator
        };

        let mut results = vec![];
        let mut substitutions = vec![(
            Variable::new_unchecked("this"),
            c.focus_node().clone(),
        )];

        if let Some(current_shape_term) = c.source_shape().get_term(context) {
            substitutions.push((
                Variable::new_unchecked("currentShape"),
                current_shape_term,
            ));
        }
        substitutions.push((
            Variable::new_unchecked("shapesGraph"),
            context.shape_graph_iri.clone().into(),
        ));

        for (param_path, values) in &self.parameter_values {
            if let Some(value) = values.first() {
                let param_name = local_name(param_path);
                substitutions.push((Variable::new_unchecked(param_name), value.clone()));
            }
        }

        if validator.is_ask {
            if let Some(value_nodes) = c.value_nodes() {
                for value_node in value_nodes {
                    let mut ask_substitutions = substitutions.clone();
                    ask_substitutions
                        .push((Variable::new_unchecked("value"), value_node.clone()));
                    match context.store().query_opt_with_substituted_variables(
                        &validator.query,
                        QueryOptions::default(),
                        ask_substitutions,
                    ) {
                        Ok(QueryResults::Boolean(conforms)) => {
                            if !conforms {
                                results.push(ComponentValidationResult::Fail(
                                    c.clone(),
                                    ValidationFailure {
                                        component_id,
                                        failed_value_node: Some(value_node.clone()),
                                        message: validator
                                            .messages
                                            .first()
                                            .map(|t| t.to_string())
                                            .unwrap_or_else(|| {
                                                format!(
                                                    "Value does not conform to custom constraint {}",
                                                    self.definition.iri
                                                )
                                            }),
                                    },
                                ));
                            }
                        }
                        Err(e) => return Err(format!("SPARQL query failed: {}", e)),
                        _ => {} // Other query results types are ignored for ASK
                    }
                }
            }
        } else {
            // SELECT validator
            let mut query = validator.query.clone();
            if is_prop_shape {
                if let Some(prop_id) = c.source_shape().as_prop_id() {
                    if let Some(prop_shape) = context.get_prop_shape_by_id(prop_id) {
                        let path_str = prop_shape.sparql_path();
                        query = query.replace("$PATH", &path_str);
                    }
                }
            }

            match context.store().query_opt_with_substituted_variables(
                &query,
                QueryOptions::default(),
                substitutions,
            ) {
                Ok(QueryResults::Solutions(solutions)) => {
                    for solution in solutions {
                        if let Ok(solution) = solution {
                            let value = solution.get("value").or_else(|| c.value()).cloned();
                            results.push(ComponentValidationResult::Fail(
                                c.clone(),
                                ValidationFailure {
                                    component_id,
                                    failed_value_node: value,
                                    message: solution
                                        .get("message")
                                        .map(|t| t.to_string())
                                        .or_else(|| {
                                            validator.messages.first().map(|t| t.to_string())
                                        })
                                        .unwrap_or_else(|| {
                                            format!(
                                                "Value does not conform to custom constraint {}",
                                                self.definition.iri
                                            )
                                        }),
                                },
                            ));
                        }
                    }
                }
                Err(e) => return Err(format!("SPARQL query failed: {}", e)),
                _ => {}
            }
        }

        Ok(results)
    }
}
