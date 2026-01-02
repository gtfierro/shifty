#![allow(deprecated)]
use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::model::components::sparql::CustomConstraintComponentDefinition;
use crate::named_nodes::SHACL;
use crate::runtime::{
    ComponentValidationResult, GraphvizOutput, ToSubjectRef, ValidateComponent, ValidationFailure,
};
use crate::sparql::{
    ensure_pre_binding_semantics, validate_prebound_variable_usage, MessageTemplater,
    SparqlExecutor,
};
use crate::types::{ComponentID, Path, Severity, TraceItem};
use log::debug;
use oxigraph::model::vocab::xsd;
use oxigraph::model::{NamedNode, Term, TermRef};
use oxigraph::sparql::{QueryResults, Variable};
use std::collections::{HashMap, HashSet};

fn query_mentions_var(query: &str, var: &str) -> bool {
    fn contains(query: &str, prefix: char, var: &str) -> bool {
        let mut start = 0;
        let bytes = query.as_bytes();
        let var_bytes = var.as_bytes();
        while let Some(pos) = query[start..].find(prefix) {
            let idx = start + pos + 1; // skip prefix itself
            if bytes.len() >= idx + var_bytes.len()
                && &bytes[idx..idx + var_bytes.len()] == var_bytes
            {
                let after = idx + var_bytes.len();
                if after >= bytes.len() {
                    return true;
                }
                let next = bytes[after] as char;
                if !next.is_ascii_alphanumeric() && next != '_' {
                    return true;
                }
            }
            start += pos + 1;
        }
        false
    }

    contains(query, '?', var) || contains(query, '$', var)
}

fn gather_default_substitutions(
    context: &Context,
    current_shape_term: Option<&Term>,
    value_term: Option<&Term>,
    path_override: Option<&String>,
) -> Vec<(String, String)> {
    let mut substitutions = Vec::new();
    substitutions.push((
        "this".to_string(),
        term_to_message_value(context.focus_node()),
    ));

    if let Some(shape_term) = current_shape_term {
        substitutions.push((
            "currentShape".to_string(),
            term_to_message_value(shape_term),
        ));
    }

    if let Some(value) = value_term {
        substitutions.push(("value".to_string(), term_to_message_value(value)));
    }

    if let Some(path) = path_override {
        substitutions.push(("PATH".to_string(), path.clone()));
    }

    substitutions
}

fn term_to_message_value(term: &Term) -> String {
    match term {
        Term::Literal(lit) => lit.value().to_string(),
        _ => format_term_for_label(term),
    }
}

fn term_ref_to_message_value(term: TermRef<'_>) -> String {
    term_to_message_value(&term.into_owned())
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
            .quads_for_pattern(
                Some(subject),
                Some(shacl.select),
                None,
                Some(context.shape_graph_iri_ref()),
            )
            .unwrap_or_default()
            .into_iter()
            .map(|q| q.object)
            .find_map(|object| match object {
                Term::Literal(lit) => Some(lit.value().to_string()),
                _ => None,
            });

        let label_str = match select_query_opt {
            Some(query) => format!("SPARQL constraint\n{}", query.replace('\n', " ")),
            None => format!(
                "SPARQL constraint\n{}",
                format_term_for_label(&self.constraint_node)
            ),
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
        let sparql_services = context.sparql_services();
        let constraint_subject = self.constraint_node.to_subject_ref();

        if context.skip_sparql_blank_targets() && matches!(c.focus_node(), Term::BlankNode(_)) {
            debug!(
                "Skipping SPARQL constraint {} for blank focus node {}",
                format_term_for_label(&self.constraint_node),
                format_term_for_label(c.focus_node())
            );
            return Ok(vec![]);
        }

        // 1. Check if deactivated
        if let Some(deactivated_quad) = context
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.deactivated),
                None,
                Some(context.shape_graph_iri_ref()),
            )?
            .into_iter()
            .next()
        {
            if let Term::Literal(lit) = &deactivated_quad.object {
                if lit.datatype() == xsd::BOOLEAN && lit.value() == "true" {
                    return Ok(vec![]);
                }
            }
        }

        // 2. Get SELECT query
        let mut select_query = if let Some(quad) = context
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.select),
                None,
                Some(context.shape_graph_iri_ref()),
            )?
            .into_iter()
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

        validate_prebound_variable_usage(
            &select_query,
            &format!(
                "SPARQL constraint {}",
                format_term_for_label(&self.constraint_node)
            ),
            true,
            false,
        )?;

        // Collect prefixes using the shared SPARQL services
        let prefixes = context.prefixes_for_node(&self.constraint_node)?;

        // Handle $PATH substitution for property shapes
        let mut path_substitution_value: Option<String> = None;
        if c.source_shape().as_prop_id().is_some() {
            if let Some(prop_id) = c.source_shape().as_prop_id() {
                if let Some(prop_shape) = context.model.get_prop_shape_by_id(prop_id) {
                    let path_str = prop_shape.sparql_path();
                    path_substitution_value = Some(path_str.clone());
                    select_query = select_query.replace("$PATH", &path_str);
                }
            }
        }

        let full_query_str = if !prefixes.is_empty() {
            format!("{}\n{}", prefixes, select_query)
        } else {
            select_query
        };

        let algebra_query = sparql_services
            .algebra(&full_query_str)
            .map_err(|e| format!("Failed to parse SPARQL constraint query: {}", e))?;

        let mut prebound_vars: HashSet<Variable> = HashSet::new();
        let mut optional_prebound_vars: HashSet<Variable> = HashSet::new();

        if query_mentions_var(&full_query_str, "this") {
            prebound_vars.insert(Variable::new_unchecked("this"));
        }

        if query_mentions_var(&full_query_str, "currentShape") {
            let var = Variable::new_unchecked("currentShape");
            optional_prebound_vars.insert(var.clone());
            prebound_vars.insert(var);
        }

        if query_mentions_var(&full_query_str, "shapesGraph") {
            let var = Variable::new_unchecked("shapesGraph");
            optional_prebound_vars.insert(var.clone());
            prebound_vars.insert(var);
        }

        ensure_pre_binding_semantics(
            &algebra_query,
            "SPARQL constraint query",
            &prebound_vars,
            &optional_prebound_vars,
        )?;

        let prepared_query = context
            .prepare_query(&full_query_str)
            .map_err(|e| format!("Failed to prepare SPARQL constraint query: {}", e))?;

        // Prepare pre-bound variables
        let mut substitutions = vec![];

        let current_shape_term = c.source_shape().get_term(context);

        if query_mentions_var(&full_query_str, "this") {
            // Only add if the query uses it
            substitutions.push((Variable::new_unchecked("this"), c.focus_node().clone()));
        }

        if let Some(shape_term) = current_shape_term.clone() {
            if query_mentions_var(&full_query_str, "currentShape") {
                // Only add if the query uses it
                substitutions.push((Variable::new_unchecked("currentShape"), shape_term));
            }
        }
        if query_mentions_var(&full_query_str, "shapesGraph") {
            // Only add if the query uses it
            substitutions.push((
                Variable::new_unchecked("shapesGraph"),
                context.model.shape_graph_iri.clone().into(),
            ));
        }

        // Get messages
        let messages: Vec<Term> = context
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.message),
                None,
                Some(context.shape_graph_iri_ref()),
            )?
            .into_iter()
            .map(|q| q.object)
            .collect();

        let severity = context
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.severity),
                None,
                Some(context.shape_graph_iri_ref()),
            )?
            .into_iter()
            .map(|q| q.object)
            .find_map(|term| <Severity as crate::types::SeverityExt>::from_term(&term));

        // Execute query
        let query_outcome =
            context.execute_prepared(&full_query_str, &prepared_query, &substitutions, true);

        match query_outcome {
            Ok(QueryResults::Solutions(solutions)) => {
                let mut results = vec![];
                let mut seen_solutions = HashSet::new();
                #[cfg(debug_assertions)]
                let debug_prebinding = std::env::var("SHACL_DEBUG_PRE_BINDING").is_ok();
                #[cfg(not(debug_assertions))]
                let debug_prebinding = false;
                let mut solution_count = 0usize;
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
                    if !seen_solutions.insert(failed_value_node.clone()) {
                        // Skip duplicate solutions
                        continue;
                    }

                    let mut message_templates = Vec::new();
                    if let Some(term) = solution.get("message") {
                        message_templates.push(term.clone());
                    }
                    if message_templates.is_empty() && !messages.is_empty() {
                        message_templates.extend(messages.clone());
                    }

                    let mut substitutions_for_messages = gather_default_substitutions(
                        c,
                        current_shape_term.as_ref(),
                        failed_value_node.as_ref(),
                        path_substitution_value.as_ref(),
                    );
                    for var in solution.variables() {
                        if let Some(term) = solution.get(var) {
                            substitutions_for_messages.push((
                                var.as_str().to_string(),
                                term_ref_to_message_value(term.into()),
                            ));
                        }
                    }

                    let (message_opt, message_terms) = sparql_services
                        .instantiate_messages(&message_templates, &substitutions_for_messages);
                    let message = message_opt.unwrap_or_else(|| {
                        "Node does not conform to SPARQL constraint".to_string()
                    });

                    // The path for the validation result is taken from the ?path variable if bound,
                    // otherwise it's taken from the context `c`.
                    let result_path_override =
                        if let Some(Term::NamedNode(path_iri)) = solution.get("path") {
                            Some(Path::Simple(Term::NamedNode(path_iri.clone())))
                        } else {
                            None
                        };

                    let failure = ValidationFailure::new(
                        component_id,
                        failed_value_node.clone(),
                        message,
                        result_path_override,
                        Some(self.constraint_node.clone()),
                    )
                    .with_severity(severity.clone())
                    .with_message_terms(message_terms);

                    results.push(ComponentValidationResult::Fail(c.clone(), failure));
                    solution_count += 1;
                }
                #[cfg(debug_assertions)]
                if debug_prebinding {
                    let debug_label = format_term_for_label(&self.constraint_node);
                    log::debug!(
                        "SPARQL constraint {} produced {} solutions",
                        debug_label,
                        solution_count
                    );
                }
                Ok(results)
            }
            Err(e) => Err(format!("SPARQL query failed: {}", e)),
            _ => Ok(vec![]), // Other query result types are ignored
        }
    }
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
        let sparql_services = context.sparql_services();
        let is_prop_shape = c.source_shape().as_prop_id().is_some();

        if context.skip_sparql_blank_targets() && matches!(c.focus_node(), Term::BlankNode(_)) {
            debug!(
                "Skipping custom SPARQL constraint {} for blank focus node {}",
                self.definition.iri,
                format_term_for_label(c.focus_node())
            );
            return Ok(vec![]);
        }

        enum ValidatorScope {
            Node,
            Property,
            General,
        }

        let (validator, _scope) = if is_prop_shape {
            if let Some(v) = self.definition.property_validator.as_ref() {
                (Some(v), ValidatorScope::Property)
            } else {
                (self.definition.validator.as_ref(), ValidatorScope::General)
            }
        } else if let Some(v) = self.definition.node_validator.as_ref() {
            (Some(v), ValidatorScope::Node)
        } else {
            (self.definition.validator.as_ref(), ValidatorScope::General)
        };

        let validator = match validator {
            Some(v) => v,
            None => return Ok(vec![]),
        };

        let require_path = validator.require_path;
        let require_this = validator.require_this;

        validate_prebound_variable_usage(
            &validator.query,
            &format!("Custom constraint {}", self.definition.iri),
            require_this,
            require_path,
        )?;

        let mut query_body = validator.query.clone();
        let mut path_substitution_value: Option<String> = None;

        if is_prop_shape {
            if let Some(prop_id) = c.source_shape().as_prop_id() {
                if let Some(prop_shape) = context.model.get_prop_shape_by_id(prop_id) {
                    let path_str = prop_shape.sparql_path();
                    path_substitution_value = Some(path_str.clone());
                    query_body = query_body.replace("$PATH", &path_str);
                }
            }
        }

        let current_shape_term = c.source_shape().get_term(context);

        let mut substitutions: Vec<(Variable, Term)> = Vec::new();
        let mut prebound_vars: HashSet<Variable> = HashSet::new();
        let mut optional_prebound_vars: HashSet<Variable> = HashSet::new();

        if query_mentions_var(&query_body, "this") {
            let var = Variable::new_unchecked("this");
            substitutions.push((var.clone(), c.focus_node().clone()));
            if !require_this {
                optional_prebound_vars.insert(var.clone());
            }
            prebound_vars.insert(var);
        }

        if let Some(term) = current_shape_term.clone() {
            if query_mentions_var(&query_body, "currentShape") {
                let var = Variable::new_unchecked("currentShape");
                substitutions.push((var.clone(), term));
                optional_prebound_vars.insert(var.clone());
                prebound_vars.insert(var);
            }
        }

        if query_mentions_var(&query_body, "shapesGraph") {
            let var = Variable::new_unchecked("shapesGraph");
            substitutions.push((var.clone(), context.model.shape_graph_iri.clone().into()));
            optional_prebound_vars.insert(var.clone());
            prebound_vars.insert(var);
        }

        for (param_path, values) in &self.parameter_values {
            let param_meta = self
                .definition
                .parameters
                .iter()
                .find(|p| p.path == *param_path);
            let var_name = match param_meta.and_then(|p| p.var_name.clone()) {
                Some(name) => name,
                None => local_name(param_path),
            };

            if !query_mentions_var(&query_body, &var_name) {
                // Skip optional parameters that are unused in the query.
                if let Some(param) = param_meta {
                    if !param.optional {
                        return Err(format!(
                            "Custom constraint {} expects query variable ?{} for parameter {}, but it was not referenced.",
                            self.definition.iri,
                            var_name,
                            param.path
                        ));
                    }
                }
                continue;
            }

            let value = values.first().ok_or_else(|| {
                format!(
                    "Custom constraint {} is missing a value for parameter {} needed by its SPARQL query.",
                    self.definition.iri,
                    var_name
                )
            })?;
            let var = Variable::new_unchecked(&var_name);
            substitutions.push((var.clone(), value.clone()));
            prebound_vars.insert(var.clone());
            if param_meta.map(|p| p.optional).unwrap_or(false) {
                optional_prebound_vars.insert(var);
            }
        }

        for param in &self.definition.parameters {
            let var_name = param
                .var_name
                .clone()
                .unwrap_or_else(|| local_name(&param.path));
            if !query_mentions_var(&query_body, &var_name) {
                continue;
            }
            if self.parameter_values.contains_key(&param.path) {
                continue;
            }
            let var = Variable::new_unchecked(&var_name);
            if param.optional {
                optional_prebound_vars.insert(var);
                continue;
            }
            return Err(format!(
                "Custom constraint {} is missing required parameter {} for query variable ?{}.",
                self.definition.iri, param.path, var_name
            ));
        }

        let include_value = validator.is_ask && query_mentions_var(&query_body, "value");
        if include_value {
            prebound_vars.insert(Variable::new_unchecked("value"));
        }

        let query_with_prefixes = if validator.prefixes.is_empty() {
            query_body.clone()
        } else {
            format!(
                "{}
{}",
                validator.prefixes, query_body
            )
        };

        let context_label = if validator.is_ask {
            format!("SPARQL ASK validator {}", self.definition.iri)
        } else {
            format!("SPARQL SELECT validator {}", self.definition.iri)
        };

        let algebra_query = sparql_services
            .algebra(&query_with_prefixes)
            .map_err(|e| format!("Failed to parse SPARQL validator query: {}", e))?;

        ensure_pre_binding_semantics(
            &algebra_query,
            &context_label,
            &prebound_vars,
            &optional_prebound_vars,
        )?;

        let prepared_query = context
            .prepare_query(&query_with_prefixes)
            .map_err(|e| format!("Failed to prepare SPARQL validator query: {}", e))?;

        let mut results = Vec::new();

        if validator.is_ask {
            if let Some(value_nodes) = c.value_nodes() {
                for value_node in value_nodes {
                    let mut ask_substitutions = substitutions.clone();
                    if include_value {
                        ask_substitutions
                            .push((Variable::new_unchecked("value"), value_node.clone()));
                    }

                    match context.execute_prepared(
                        &query_with_prefixes,
                        &prepared_query,
                        &ask_substitutions,
                        true,
                    ) {
                        Ok(QueryResults::Boolean(conforms)) => {
                            if !conforms {
                                let message_templates = if !validator.messages.is_empty() {
                                    validator.messages.clone()
                                } else {
                                    self.definition.messages.clone()
                                };
                                let mut substitutions_for_messages = gather_default_substitutions(
                                    c,
                                    current_shape_term.as_ref(),
                                    Some(value_node),
                                    path_substitution_value.as_ref(),
                                );
                                for (param_path, values) in &self.parameter_values {
                                    if let Some(val) = values.first() {
                                        substitutions_for_messages.push((
                                            local_name(param_path),
                                            term_to_message_value(val),
                                        ));
                                    }
                                }
                                let (message_opt, message_terms) = sparql_services
                                    .instantiate_messages(
                                        &message_templates,
                                        &substitutions_for_messages,
                                    );
                                let message = message_opt.unwrap_or_else(|| {
                                    format!(
                                        "Value does not conform to custom constraint {}",
                                        self.definition.iri
                                    )
                                });
                                let severity_override = validator
                                    .severity
                                    .clone()
                                    .or_else(|| self.definition.severity.clone());
                                let failure = ValidationFailure::new(
                                    component_id,
                                    Some(value_node.clone()),
                                    message,
                                    None,
                                    None,
                                )
                                .with_severity(severity_override)
                                .with_message_terms(message_terms);

                                results.push(ComponentValidationResult::Fail(c.clone(), failure));
                            }
                        }
                        Ok(_) => {}
                        Err(e) => return Err(format!("SPARQL query failed: {}", e)),
                    }
                }
            }
        } else {
            match context.execute_prepared(
                &query_with_prefixes,
                &prepared_query,
                &substitutions,
                true,
            ) {
                Ok(QueryResults::Solutions(solutions)) => {
                    let mut seen_solutions = HashSet::new();
                    for solution_res in solutions {
                        let solution = solution_res.map_err(|e| e.to_string())?;

                        if let Some(Term::Literal(failure)) = solution.get("failure") {
                            if failure.datatype() == xsd::BOOLEAN && failure.value() == "true" {
                                return Err("SPARQL validator reported a failure.".to_string());
                            }
                        }

                        let failed_value_node = if let Some(val) = solution.get("value") {
                            Some(val.clone())
                        } else if c.source_shape().as_node_id().is_some() {
                            Some(c.focus_node().clone())
                        } else {
                            None
                        };

                        if !seen_solutions.insert(failed_value_node.clone()) {
                            continue;
                        }

                        let mut message_templates = Vec::new();
                        if let Some(term) = solution.get("message") {
                            message_templates.push(term.clone());
                        }
                        if message_templates.is_empty() && !validator.messages.is_empty() {
                            message_templates.extend(validator.messages.clone());
                        }
                        if message_templates.is_empty() && !self.definition.messages.is_empty() {
                            message_templates.extend(self.definition.messages.clone());
                        }

                        let mut substitutions_for_messages = gather_default_substitutions(
                            c,
                            current_shape_term.as_ref(),
                            failed_value_node.as_ref(),
                            path_substitution_value.as_ref(),
                        );
                        for (param_path, values) in &self.parameter_values {
                            if let Some(val) = values.first() {
                                substitutions_for_messages.push((
                                    local_name(param_path),
                                    term_ref_to_message_value(val.as_ref()),
                                ));
                            }
                        }
                        for var in solution.variables() {
                            if let Some(term) = solution.get(var) {
                                substitutions_for_messages.push((
                                    var.as_str().to_string(),
                                    term_ref_to_message_value(term.into()),
                                ));
                            }
                        }

                        let (message_opt, message_terms) = sparql_services
                            .instantiate_messages(&message_templates, &substitutions_for_messages);
                        let message = message_opt.unwrap_or_else(|| {
                            format!(
                                "Node does not conform to custom constraint {}",
                                self.definition.iri
                            )
                        });

                        let severity_override = validator
                            .severity
                            .clone()
                            .or_else(|| self.definition.severity.clone());
                        let failure = ValidationFailure::new(
                            component_id,
                            failed_value_node.clone(),
                            message,
                            None,
                            None,
                        )
                        .with_severity(severity_override)
                        .with_message_terms(message_terms);

                        results.push(ComponentValidationResult::Fail(c.clone(), failure));
                    }
                }
                Ok(_) => {}
                Err(e) => return Err(format!("SPARQL query failed: {}", e)),
            }
        }

        Ok(results)
    }
}
