use super::{
    ComponentValidationResult, GraphvizOutput, ToSubjectRef, ValidateComponent, ValidationFailure,
};
use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::named_nodes::SHACL;
use crate::types::{ComponentID, Path, TraceItem};
use ontoenv::api::ResolveTarget;
use oxigraph::model::vocab::xsd;
use oxigraph::model::{Literal, NamedNode, NamedNodeRef, Term, TermRef};
use oxigraph::sparql::{Query, QueryOptions, QueryResults, Variable};
use std::collections::HashMap;

fn get_prefixes_for_sparql_node(
    sparql_node: TermRef,
    context: &ValidationContext,
) -> Result<String, String> {
    let shacl = SHACL::new();
    let prefixes_subjects: Vec<Term> = context
        .store()
        .quads_for_pattern(
            Some(sparql_node.try_to_subject_ref()?),
            Some(shacl.prefixes),
            None,
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok)
        .map(|q| q.object)
        .collect();

    let mut collected_prefixes: HashMap<String, String> = HashMap::new();

    for prefixes_subject in prefixes_subjects {
        // As per spec, sh:prefixes values can be ontology IRIs or nodes with sh:declare.
        // Per user request, we only handle ontology IRIs here.
        if let Term::NamedNode(ontology_iri) = &prefixes_subject {
            let graphid = context
                .env()
                .resolve(ResolveTarget::Graph(ontology_iri.clone()));
            let graphid = match graphid {
                Some(id) => id,
                None => continue,
            };
            if let Ok(ont) = context.env().get_ontology(&graphid) {
                for (prefix, namespace) in ont.namespace_map().iter() {
                    if let Some(existing_namespace) = collected_prefixes.get(prefix.as_str()) {
                        if existing_namespace != namespace {
                            return Err(format!(
                                "Duplicate prefix '{}' with different namespaces: '{}' and '{}'",
                                prefix, existing_namespace, namespace
                            ));
                        }
                    } else {
                        collected_prefixes.insert(prefix.clone(), namespace.clone());
                    }
                }
            }
        } else {
            return Err(format!(
                "sh:prefixes value must be an IRI (ontology IRI), but found: {}",
                prefixes_subject
            ));
        }
    }

    let prefix_strs: Vec<String> = collected_prefixes
        .iter()
        .map(|(prefix, iri)| format!("PREFIX {}: <{}>", prefix, iri))
        .collect();
    Ok(prefix_strs.join("\n"))
}

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
        let constraint_subject = self.constraint_node.to_subject_ref();

        // 1. Check if deactivated
        if let Some(Ok(deactivated_quad)) = context
            .store()
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.deactivated),
                None,
                Some(context.shape_graph_iri_ref()),
            )
            .next()
        {
            if let Term::Literal(lit) = &deactivated_quad.object {
                if lit.datatype() == xsd::BOOLEAN && lit.value() == "true" {
                    return Ok(vec![]);
                }
            }
        }

        // 2. Get SELECT query
        let mut select_query = if let Some(Ok(quad)) = context
            .store()
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.select),
                None,
                Some(context.shape_graph_iri_ref()),
            )
            .next()
        {
            if let Term::Literal(lit) = &quad.object {
                lit.value().to_string()
            } else {
                return Err("sh:select value must be a literal string".to_string());
            }
        } else {
            return Err("SPARQL constraint is missing sh:select".to_string());
        };

        // 3. SPARQL syntax checks from Appendix A
        // Note: These are simplified checks. More robust checks would need a full SPARQL parser
        // or more complex regular expressions to avoid matching inside comments or strings.
        if select_query.to_uppercase().contains(" MINUS ") {
            // with spaces to avoid matching variable names
            return Err("A SPARQL Constraint must not contain a MINUS clause.".to_string());
        }
        if select_query.to_uppercase().contains(" VALUES ") {
            return Err("A SPARQL Constraint must not contain a VALUES clause.".to_string());
        }
        if select_query.to_uppercase().contains(" SERVICE ") {
            return Err(
                "A SPARQL Constraint must not contain a federated query (SERVICE).".to_string(),
            );
        }

        // 4. Get prefixes
        let prefixes = get_prefixes_for_sparql_node(self.constraint_node.as_ref(), context)?;

        // 5. Handle $PATH substitution for property shapes
        if c.source_shape().as_prop_id().is_some() {
            if let Some(prop_id) = c.source_shape().as_prop_id() {
                if let Some(prop_shape) = context.get_prop_shape_by_id(prop_id) {
                    let path_str = prop_shape.sparql_path();
                    select_query = select_query.replace("$PATH", &path_str);
                }
            }
        }

        let full_query_str = if !prefixes.is_empty() {
            format!("{}\n{}", prefixes, select_query)
        } else {
            select_query
        };

        let mut query = Query::parse(&full_query_str, None)
            .map_err(|e| format!("Failed to parse SPARQL constraint query: {}", e))?;
        query.dataset_mut().set_default_graph_as_union();

        // 6. Prepare pre-bound variables
        let mut substitutions =
            vec![(Variable::new_unchecked("this"), c.focus_node().clone())];

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

        // 7. Get messages
        let messages: Vec<Term> = context
            .store()
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.message),
                None,
                Some(context.shape_graph_iri_ref()),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .collect();

        // 8. Execute query
        let query_results = context.store().query_opt_with_substituted_variables(
            query,
            QueryOptions::default(),
            substitutions,
        );

        match query_results {
            Ok(QueryResults::Solutions(solutions)) => {
                let mut results = vec![];
                for solution_res in solutions {
                    let solution = solution_res.map_err(|e| e.to_string())?;

                    if let Some(Term::Literal(failure)) = solution.get("failure") {
                        if failure.datatype() == xsd::BOOLEAN && failure.value() == "true" {
                            return Err("SPARQL query reported a failure.".to_string());
                        }
                    }

                    let failed_value_node = if let Some(val) = solution.get("value") {
                        Some(val.clone())
                    } else if c.source_shape().as_node_id().is_some() {
                        Some(c.focus_node().clone())
                    } else {
                        None
                    };

                    let mut message = solution
                        .get("message")
                        .map(|t| t.to_string())
                        .or_else(|| messages.first().map(|t| t.to_string()))
                        .unwrap_or_else(|| "Node does not conform to SPARQL constraint".to_string());

                    // Substitute variables in message
                    for var in solution.variables() {
                        if let Some(term) = solution.get(var) {
                            let var_name = var.as_str();
                            let placeholder1 = format!("{{?{}}}", var_name);
                            let placeholder2 = format!("{{${}}}", var_name);
                            message = message.replace(&placeholder1, &term.to_string());
                            message = message.replace(&placeholder2, &term.to_string());
                        }
                    }

                    // The path for the validation result is taken from the ?path variable if bound,
                    // otherwise it's taken from the context `c`.
                    let result_path_override =
                        if let Some(Term::NamedNode(path_iri)) = solution.get("path") {
                            Some(Path::Simple(Term::NamedNode(path_iri.clone())))
                        } else {
                            None
                        };

                    results.push(ComponentValidationResult::Fail(
                        c.clone(),
                        ValidationFailure {
                            component_id,
                            failed_value_node,
                            message,
                            result_path: result_path_override,
                        },
                    ));
                }
                Ok(results)
            }
            Err(e) => Err(format!("SPARQL query failed: {}", e)),
            _ => Ok(vec![]), // Other query result types are ignored
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
    pub prefixes: String,
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
        for solution_res in solutions {
            if let Ok(solution) = solution_res {
                if let Some(Term::NamedNode(cc_iri)) = solution.get("cc") {
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
                    let parse_validator =
                        |v_term: &Term, is_ask: bool, context: &ValidationContext| -> Option<SPARQLValidator> {
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
                                                vec![Term::Literal(Literal::new_simple_literal(
                                                    lit.value(),
                                                ))]
                                            } else {
                                                vec![]
                                            }
                                        });
                                        let prefixes =
                                            get_prefixes_for_sparql_node(v_term.as_ref(), context)
                                                .unwrap_or_default();
                                        return Some(SPARQLValidator {
                                            query: query_lit.value().to_string(),
                                            is_ask,
                                            messages,
                                            prefixes,
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
                        validator = parse_validator(&v_term, true, context);
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
                        node_validator = parse_validator(&v_term, false, context);
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
                        property_validator = parse_validator(&v_term, false, context);
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

                    let mut query_str = validator.query.clone();
                    if !validator.prefixes.is_empty() {
                        query_str.insert_str(0, "\n");
                        query_str.insert_str(0, &validator.prefixes);
                    }

                    match context.store().query_opt_with_substituted_variables(
                        &query_str,
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
                                        result_path: None,
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

            if !validator.prefixes.is_empty() {
                query.insert_str(0, "\n");
                query.insert_str(0, &validator.prefixes);
            }

            match context.store().query_opt_with_substituted_variables(
                &query,
                QueryOptions::default(),
                substitutions,
            ) {
                Ok(QueryResults::Solutions(solutions)) => {
                    for solution in solutions {
                        if let Ok(solution) = solution {
                            let value = if let Some(val) = solution.get("value") {
                                Some(val.clone())
                            } else if c.source_shape().as_node_id().is_some() {
                                Some(c.focus_node().clone())
                            } else {
                                None
                            };

                            let result_path_override =
                                if let Some(Term::NamedNode(path_iri)) = solution.get("path") {
                                    Some(Path::Simple(Term::NamedNode(path_iri.clone())))
                                } else {
                                    None
                                };

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
                                    result_path: result_path_override,
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
