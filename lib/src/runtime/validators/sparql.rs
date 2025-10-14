#![allow(deprecated)]
use crate::context::{format_term_for_label, Context, ParsingContext, ValidationContext};
use crate::model::components::sparql::{
    CustomConstraintComponentDefinition, Parameter, SPARQLValidator,
};
use crate::named_nodes::SHACL;
use crate::runtime::{
    ComponentValidationResult, GraphvizOutput, ToSubjectRef, ValidateComponent, ValidationFailure,
};
use crate::types::{ComponentID, Path, Severity, TraceItem};
use ontoenv::api::{OntoEnv, ResolveTarget};
use oxigraph::model::vocab::xsd;
use oxigraph::model::{GraphNameRef, Literal, NamedNode, NamedNodeRef, Term, TermRef};
use oxigraph::sparql::{Query, QueryOptions, QueryResults, Variable};
use oxigraph::store::Store;
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression};
use spargebra::Query as AlgebraQuery;
use std::collections::{HashMap, HashSet};

// TODO : stop grabbing prefixes/declaratiosn from *everywhere*
fn get_prefixes_for_sparql_node(
    sparql_node: TermRef,
    store: &Store,
    env: &OntoEnv,
    shape_graph_iri_ref: GraphNameRef,
) -> Result<String, String> {
    let shacl = SHACL::new();
    let mut prefixes_subjects: HashSet<Term> = store
        .quads_for_pattern(
            Some(sparql_node.try_to_subject_ref()?),
            Some(shacl.prefixes),
            None,
            Some(shape_graph_iri_ref),
        )
        .filter_map(Result::ok)
        .map(|q| q.object)
        .collect();

    // extend with sh:declare subjects
    prefixes_subjects.extend(
        store
            .quads_for_pattern(None, Some(shacl.declare), None, None)
            .filter_map(Result::ok)
            .map(|q| q.subject.into()),
    );

    let mut collected_prefixes: HashMap<String, String> = HashMap::new();

    for prefixes_subject in prefixes_subjects {
        // Handle sh:declare on the prefixes_subject
        let declarations: Vec<Term> = store
            .quads_for_pattern(
                Some(prefixes_subject.try_to_subject_ref()?),
                Some(shacl.declare),
                None,
                None,
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .collect();

        for declaration in declarations {
            let decl_subject = match declaration.try_to_subject_ref() {
                Ok(s) => s,
                Err(_) => {
                    return Err(format!(
                        "sh:declare value must be an IRI or blank node, but found: {}",
                        declaration
                    ))
                }
            };

            let prefix_val = store
                .quads_for_pattern(Some(decl_subject), Some(shacl.prefix), None, None)
                .next()
                .and_then(|res| res.ok())
                .map(|q| q.object);

            let namespace_val = store
                .quads_for_pattern(Some(decl_subject), Some(shacl.namespace), None, None)
                .next()
                .and_then(|res| res.ok())
                .map(|q| q.object);

            if let (Some(Term::Literal(prefix_lit)), Some(Term::Literal(namespace_lit))) =
                (prefix_val, namespace_val)
            {
                let prefix = prefix_lit.value().to_string();
                let namespace = namespace_lit.value().to_string();
                if let Some(existing_namespace) = collected_prefixes.get(&prefix) {
                    if existing_namespace != &namespace {
                        return Err(format!(
                            "Duplicate prefix '{}' with different namespaces: '{}' and '{}'",
                            prefix, existing_namespace, namespace
                        ));
                    }
                } else {
                    collected_prefixes.insert(prefix, namespace);
                }
            } else {
                return Err(format!(
                    "Ill-formed prefix declaration: {}. Missing sh:prefix or sh:namespace.",
                    declaration
                ));
            }
        }

        // Handle ontology IRI with ontoenv
        if let Term::NamedNode(ontology_iri) = &prefixes_subject {
            let graphid = env.resolve(ResolveTarget::Graph(ontology_iri.clone()));
            let graphid = match graphid {
                Some(id) => id,
                None => continue,
            };
            if let Ok(ont) = env.get_ontology(&graphid) {
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
        }
    }

    let prefix_strs: Vec<String> = collected_prefixes
        .iter()
        .map(|(prefix, iri)| format!("PREFIX {}: <{}>", prefix, iri))
        .collect();
    Ok(prefix_strs.join("\n"))
}

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

pub(crate) fn validate_prebound_variable_usage(
    query: &str,
    context_label: &str,
    require_this: bool,
    require_path: bool,
) -> Result<(), String> {
    if require_this && !query_mentions_var(query, "this") {
        return Err(format!(
            "{} must reference the pre-bound variable $this (or ?this).",
            context_label
        ));
    }

    if require_path && !query_mentions_var(query, "PATH") {
        return Err(format!(
            "{} must reference the pre-bound path placeholder $PATH.",
            context_label
        ));
    }

    Ok(())
}

pub(crate) fn validate_constraint_prebound_usage(
    context: &ParsingContext,
    constraint_term: &Term,
    _is_property_shape: bool,
) -> Result<(), String> {
    let shacl = SHACL::new();
    let subject_node = constraint_term
        .as_ref()
        .try_to_subject_ref()
        .map_err(|e| {
            format!(
                "Invalid sh:sparql constraint node {:?}: {}",
                constraint_term, e
            )
        })?
        .into_owned();
    let mut found_query = false;

    let mut validate = |predicate: NamedNodeRef<'_>, is_ask: bool| -> Result<(), String> {
        let subject_ref = subject_node.as_ref();
        let graph = context.shape_graph_iri_ref();
        for quad in context
            .store
            .quads_for_pattern(Some(subject_ref.into()), Some(predicate), None, Some(graph))
            .filter_map(Result::ok)
        {
            let query_term = quad.object;
            let query_str = match &query_term {
                Term::Literal(lit) => lit.value().to_string(),
                _ => {
                    return Err(format!(
                        "SPARQL constraint {} must provide its {} query as a literal.",
                        format_term_for_label(constraint_term),
                        if is_ask { "sh:ask" } else { "sh:select" }
                    ))
                }
            };
            validate_prebound_variable_usage(
                &query_str,
                &format!(
                    "SPARQL constraint {}",
                    format_term_for_label(constraint_term)
                ),
                true,
                false,
            )?;
            found_query = true;
        }
        Ok(())
    };

    validate(shacl.select, false)?;
    let ask_pred = NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#ask");
    validate(ask_pred, true)?;

    if !found_query {
        return Err(format!(
            "SPARQL constraint {} must declare sh:select or sh:ask.",
            format_term_for_label(constraint_term)
        ));
    }

    Ok(())
}

fn substitute_placeholders(message: &str, substitutions: &[(String, String)]) -> String {
    let mut text = message.to_string();
    for (name, value) in substitutions {
        let placeholder_q = format!("{{?{}}}", name);
        let placeholder_dollar = format!("{{${}}}", name);
        text = text.replace(&placeholder_q, value);
        text = text.replace(&placeholder_dollar, value);
    }
    text
}

fn instantiate_message_terms(
    templates: &[Term],
    substitutions: &[(String, String)],
) -> (Option<String>, Vec<Term>) {
    if templates.is_empty() {
        return (None, Vec::new());
    }

    let mut first_message = None;
    let mut instantiated_terms = Vec::with_capacity(templates.len());

    for template in templates {
        match template {
            Term::Literal(lit) => {
                let substituted = substitute_placeholders(lit.value(), substitutions);
                if first_message.is_none() {
                    first_message = Some(substituted.clone());
                }
                let instantiated_literal = if let Some(lang) = lit.language() {
                    Literal::new_language_tagged_literal(substituted.clone(), lang)
                        .map(Term::Literal)
                        .unwrap_or_else(|_| Term::Literal(Literal::from(substituted.clone())))
                } else {
                    let datatype = lit.datatype();
                    if datatype.as_str() == xsd::STRING {
                        Term::Literal(Literal::from(substituted.clone()))
                    } else {
                        Term::Literal(Literal::new_typed_literal(
                            substituted.clone(),
                            NamedNode::new_unchecked(datatype.as_str()),
                        ))
                    }
                };
                instantiated_terms.push(instantiated_literal);
            }
            other => {
                let substituted = substitute_placeholders(&other.to_string(), substitutions);
                if first_message.is_none() {
                    first_message = Some(substituted.clone());
                }
                instantiated_terms.push(Term::Literal(Literal::from(substituted)));
            }
        }
    }

    (first_message, instantiated_terms)
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

fn ensure_pre_binding_semantics(
    query: &AlgebraQuery,
    context_label: &str,
    prebound: &HashSet<Variable>,
    optional: &HashSet<Variable>,
) -> Result<(), String> {
    match query {
        AlgebraQuery::Select { pattern, .. }
        | AlgebraQuery::Ask { pattern, .. }
        | AlgebraQuery::Construct { pattern, .. }
        | AlgebraQuery::Describe { pattern, .. } => {
            check_graph_pattern(pattern, context_label, prebound, optional, true)
        }
    }
}

fn check_graph_pattern(
    pattern: &GraphPattern,
    context_label: &str,
    prebound: &HashSet<Variable>,
    optional: &HashSet<Variable>,
    is_root: bool,
) -> Result<(), String> {
    match pattern {
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } => Ok(()),
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Lateral { left, right } => {
            check_graph_pattern(left, context_label, prebound, optional, false)?;
            check_graph_pattern(right, context_label, prebound, optional, false)
        }
        GraphPattern::Graph { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => {
            // Wrapper patterns around the root SELECT should not be treated as subqueries.
            check_graph_pattern(inner, context_label, prebound, optional, is_root)
        }
        GraphPattern::Filter { expr, inner } => {
            check_expression(expr, context_label, prebound, optional)?;
            check_graph_pattern(inner, context_label, prebound, optional, false)
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            check_graph_pattern(left, context_label, prebound, optional, false)?;
            check_graph_pattern(right, context_label, prebound, optional, false)?;
            if let Some(expr) = expression {
                check_expression(expr, context_label, prebound, optional)?;
            }
            Ok(())
        }
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            if prebound.contains(variable) {
                return Err(format!(
                    "{} must not reassign the pre-bound variable ?{}.",
                    context_label,
                    variable.as_str()
                ));
            }
            check_expression(expression, context_label, prebound, optional)?;
            check_graph_pattern(inner, context_label, prebound, optional, false)
        }
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            for (variable, aggregate) in aggregates {
                if prebound.contains(variable) {
                    return Err(format!(
                        "{} must not reassign the pre-bound variable ?{}.",
                        context_label,
                        variable.as_str()
                    ));
                }
                check_aggregate_expression(aggregate, context_label, prebound, optional)?;
            }
            check_graph_pattern(inner, context_label, prebound, optional, false)
        }
        GraphPattern::Project { inner, variables } => {
            if !is_root {
                for variable in prebound {
                    if optional.contains(variable) {
                        continue;
                    }
                    if !variables.iter().any(|v| v == variable) {
                        return Err(format!(
                            "{} subqueries must project the pre-bound variable ?{}.",
                            context_label,
                            variable.as_str()
                        ));
                    }
                }
            }
            check_graph_pattern(inner, context_label, prebound, optional, false)
        }
        GraphPattern::Values { .. } => Err(format!(
            "{} must not contain a VALUES clause.",
            context_label
        )),
        GraphPattern::Minus { .. } => Err(format!(
            "{} must not contain a MINUS clause.",
            context_label
        )),
        GraphPattern::Service { .. } => Err(format!(
            "{} must not contain a federated query (SERVICE).",
            context_label
        )),
        GraphPattern::OrderBy { inner, expression } => {
            for expr in expression {
                check_order_expression(expr, context_label, prebound, optional)?;
            }
            // ORDER BY wrapping the root query should not flip is_root
            check_graph_pattern(inner, context_label, prebound, optional, is_root)
        }
    }
}

fn check_order_expression(
    order: &OrderExpression,
    context_label: &str,
    prebound: &HashSet<Variable>,
    optional: &HashSet<Variable>,
) -> Result<(), String> {
    match order {
        OrderExpression::Asc(expr) | OrderExpression::Desc(expr) => {
            check_expression(expr, context_label, prebound, optional)
        }
    }
}

fn check_aggregate_expression(
    aggregate: &AggregateExpression,
    context_label: &str,
    prebound: &HashSet<Variable>,
    optional: &HashSet<Variable>,
) -> Result<(), String> {
    match aggregate {
        AggregateExpression::CountSolutions { .. } => Ok(()),
        AggregateExpression::FunctionCall { expr, .. } => {
            check_expression(expr, context_label, prebound, optional)
        }
    }
}

fn check_expression(
    expr: &Expression,
    context_label: &str,
    prebound: &HashSet<Variable>,
    optional: &HashSet<Variable>,
) -> Result<(), String> {
    match expr {
        Expression::NamedNode(_) | Expression::Literal(_) | Expression::Variable(_) => Ok(()),
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            check_expression(inner, context_label, prebound, optional)
        }
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            check_expression(left, context_label, prebound, optional)?;
            check_expression(right, context_label, prebound, optional)
        }
        Expression::In(item, items) => {
            check_expression(item, context_label, prebound, optional)?;
            for it in items {
                check_expression(it, context_label, prebound, optional)?;
            }
            Ok(())
        }
        Expression::FunctionCall(_, args) => {
            for arg in args {
                check_expression(arg, context_label, prebound, optional)?;
            }
            Ok(())
        }
        Expression::If(condition, then_branch, else_branch) => {
            check_expression(condition, context_label, prebound, optional)?;
            check_expression(then_branch, context_label, prebound, optional)?;
            check_expression(else_branch, context_label, prebound, optional)
        }
        Expression::Coalesce(expressions) => {
            for expression in expressions {
                check_expression(expression, context_label, prebound, optional)?;
            }
            Ok(())
        }
        Expression::Exists(pattern) => {
            check_graph_pattern(pattern, context_label, prebound, optional, false)
        }
        Expression::Bound(_) => Ok(()),
    }
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
            .model
            .store()
            .quads_for_pattern(
                Some(subject),
                Some(shacl.select),
                None,
                Some(context.model.shape_graph_iri_ref()),
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
            .model
            .store()
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.deactivated),
                None,
                Some(context.model.shape_graph_iri_ref()),
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
            .model
            .store()
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.select),
                None,
                Some(context.model.shape_graph_iri_ref()),
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

        validate_prebound_variable_usage(
            &select_query,
            &format!(
                "SPARQL constraint {}",
                format_term_for_label(&self.constraint_node)
            ),
            true,
            false,
        )?;

        // Collect prefixes
        let prefixes = get_prefixes_for_sparql_node(
            self.constraint_node.as_ref(),
            &context.model.store,
            &context.model.env,
            context.model.shape_graph_iri_ref(),
        )?;

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

        let algebra_query = AlgebraQuery::parse(&full_query_str, None)
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

        let mut query = Query::parse(&full_query_str, None)
            .map_err(|e| format!("Failed to parse SPARQL constraint query: {}", e))?;
        query.dataset_mut().set_default_graph_as_union();

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
            .model
            .store()
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.message),
                None,
                Some(context.model.shape_graph_iri_ref()),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .collect();

        let severity = context
            .model
            .store()
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.severity),
                None,
                Some(context.model.shape_graph_iri_ref()),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .find_map(|term| Severity::from_term(&term));

        // Execute query
        let query_results = context.model.store().query_opt_with_substituted_variables(
            query,
            QueryOptions::default(),
            substitutions,
        );

        match query_results {
            Ok(QueryResults::Solutions(solutions)) => {
                let mut results = vec![];
                let mut seen_solutions = HashSet::new();
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

                    let (message_opt, message_terms) =
                        instantiate_message_terms(&message_templates, &substitutions_for_messages);
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

/*
Performance note: parse_custom_constraint_components can be very slow on large shapes graphs.

Primary reasons:
1) Many SPARQL round-trips:
   - One SELECT to discover all custom constraint components (?cc).
   - For each component: a separate SELECT to fetch its parameters.
   - For each component: up to three lookups (validator/nodeValidator/propertyValidator) to find validator nodes,
     and then for each such validator parse_validator runs another SPARQL query to extract the actual query string
     (and grouped messages).

2) Repeated prefix collection with global scans:
   - parse_validator calls get_prefixes_for_sparql_node for each validator node.
   - get_prefixes_for_sparql_node does multiple quads_for_pattern calls, including
     quads_for_pattern(None, Some(sh:declare), None, None) which scans across all graphs.
   - It also iterates sh:declare values per prefixes subject and merges OntoEnv namespace maps.
   - There is no caching, so these global scans repeat per validator.

3) Unrestricted graph lookups:
   - Some store lookups omit the shapes graph (graph = None), causing global scans over the entire store.
     This is much slower than restricting to the shapes graph via context.shape_graph_iri_ref().

4) Query construction and parsing overhead:
   - Each loop iteration creates fresh query strings and re-parses them (both Oxigraph SPARQL and Spargebra algebra).
   - GROUP_CONCAT in the validator query adds extra work even though we only use the first row.

5) Minor inefficiencies:
   - Collecting solution iterators into Vec just to take .next() (extra allocation).
   - No reuse of computed prefixes per validator/component, and no batching of parameter discovery.

Typical hotspots in profiling:
- get_prefixes_for_sparql_node
- context.store.query_opt(...) inside the per-component/per-validator loops

Potential improvements (future work):
- Cache prefixes per (validator node, shapes graph) and/or precompute once per shapes graph.
- Restrict all store scans to the shapes graph where possible (avoid None graph unless necessary).
- Batch parameter discovery (one query returning all params for all components).
- Avoid GROUP_CONCAT and fetch messages with simple quad iteration if possible.
- Avoid collecting iterators into Vec when only the first item is needed.
- Consider building validators with a single query that returns (component, validator, query, messages) tuples.
*/
pub(crate) fn parse_custom_constraint_components(
    context: &ParsingContext,
) -> Result<
    (
        HashMap<NamedNode, CustomConstraintComponentDefinition>,
        HashMap<NamedNode, Vec<NamedNode>>,
    ),
    String,
> {
    let mut definitions = HashMap::new();
    let mut param_to_component: HashMap<NamedNode, Vec<NamedNode>> = HashMap::new();
    let shacl = SHACL::new();

    let shapes_graph_iri = context.shape_graph_iri.as_str();
    let query = format!(
        "PREFIX sh: <http://www.w3.org/ns/shacl#>\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nSELECT DISTINCT ?cc FROM <{}> WHERE {{ ?cc a ?ccType . ?ccType rdfs:subClassOf* sh:ConstraintComponent }}",
        shapes_graph_iri
    );
    if let Ok(QueryResults::Solutions(solutions)) =
        context.store.query_opt(&query, QueryOptions::default())
    {
        for solution_res in solutions {
            if let Ok(solution) = solution_res {
                if let Some(Term::NamedNode(cc_iri)) = solution.get("cc") {
                    let mut parameters = vec![];
                    let param_query = format!(
                        "PREFIX sh: <http://www.w3.org/ns/shacl#>\nSELECT ?param ?path ?optional FROM <{}> WHERE {{ <{}> sh:parameter ?param . ?param sh:path ?path . OPTIONAL {{ ?param sh:optional ?optional }} }}",
                        shapes_graph_iri,
                        cc_iri.as_str()
                    );

                    if let Ok(QueryResults::Solutions(param_solutions)) = context
                        .store
                        .query_opt(&param_query, QueryOptions::default())
                    {
                        for param_solution in param_solutions {
                            if let Ok(p_sol) = param_solution {
                                if let Some(Term::NamedNode(path)) = p_sol.get("path") {
                                    let optional = p_sol
                                        .get("optional")
                                        .and_then(|t| match t {
                                            Term::Literal(l) => match l.value() {
                                                v if v.eq_ignore_ascii_case("true") || v == "1" => {
                                                    Some(true)
                                                }
                                                v if v.eq_ignore_ascii_case("false")
                                                    || v == "0" =>
                                                {
                                                    Some(false)
                                                }
                                                _ => None,
                                            },
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

                    let component_messages: Vec<Term> = context
                        .store
                        .quads_for_pattern(
                            Some(cc_iri.as_ref().into()),
                            Some(shacl.message),
                            None,
                            Some(context.shape_graph_iri_ref()),
                        )
                        .filter_map(Result::ok)
                        .map(|q| q.object)
                        .collect();

                    let component_severity = context
                        .store
                        .quads_for_pattern(
                            Some(cc_iri.as_ref().into()),
                            Some(shacl.severity),
                            None,
                            Some(context.shape_graph_iri_ref()),
                        )
                        .filter_map(Result::ok)
                        .map(|q| q.object)
                        .find_map(|term| Severity::from_term(&term));

                    // Helper to parse a validator
                    let parse_validator =
                        |v_term: &Term,
                         is_ask: bool,
                         context: &ParsingContext,
                         require_path: bool|
                         -> Result<Option<SPARQLValidator>, String> {
                            let subject = match v_term {
                            Term::NamedNode(nn) => Some(nn.as_ref().into()),
                            Term::BlankNode(bn) => Some(bn.as_ref().into()),
                            _ => None,
                        }
                        .ok_or_else(|| {
                            format!(
                                "Custom constraint validator term {:?} must be a node or blank node.",
                                v_term
                            )
                        })?;

                            let term_ref = match v_term {
                            Term::NamedNode(nn) => TermRef::NamedNode(nn.as_ref()),
                            Term::BlankNode(bn) => TermRef::BlankNode(bn.as_ref()),
                            _ => {
                                return Err(format!(
                                    "Custom constraint validator term {:?} must be a node or blank node.",
                                    v_term
                                ))
                            }
                        };

                            let ask_pred =
                                NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#ask");
                            let query_pred = if is_ask { ask_pred } else { shacl.select };

                            let query_object = context
                                .store
                                .quads_for_pattern(
                                    Some(subject),
                                    Some(query_pred),
                                    None,
                                    Some(context.shape_graph_iri_ref()),
                                )
                                .filter_map(Result::ok)
                                .map(|q| q.object)
                                .next()
                                .ok_or_else(|| {
                                    format!(
                                        "Custom constraint validator {:?} is missing the {} query.",
                                        v_term,
                                        if is_ask { "sh:ask" } else { "sh:select" }
                                    )
                                })?;

                            let query_str = match query_object {
                            Term::Literal(ref lit) => lit.value().to_string(),
                            _ => {
                                return Err(format!(
                                    "Custom constraint validator {:?} must supply its query as a literal.",
                                    v_term
                                ))
                            }
                        };

                            validate_prebound_variable_usage(
                                &query_str,
                                &format!("Custom constraint {}", cc_iri),
                                true,
                                require_path,
                            )?;

                            let messages: Vec<Term> = context
                                .store
                                .quads_for_pattern(
                                    Some(subject),
                                    Some(shacl.message),
                                    None,
                                    Some(context.shape_graph_iri_ref()),
                                )
                                .filter_map(Result::ok)
                                .map(|q| q.object)
                                .collect();

                            let severity = context
                                .store
                                .quads_for_pattern(
                                    Some(subject),
                                    Some(shacl.severity),
                                    None,
                                    Some(context.shape_graph_iri_ref()),
                                )
                                .filter_map(Result::ok)
                                .map(|q| q.object)
                                .find_map(|term| Severity::from_term(&term));

                            let prefixes = get_prefixes_for_sparql_node(
                                term_ref,
                                &context.store,
                                &context.env,
                                context.shape_graph_iri_ref(),
                            )
                            .unwrap_or_default();

                            Ok(Some(SPARQLValidator {
                                query: query_str,
                                is_ask,
                                messages,
                                prefixes,
                                severity,
                            }))
                        };

                    let validator_prop =
                        NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#validator");
                    let node_validator_prop =
                        NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#nodeValidator");
                    let property_validator_prop =
                        NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#propertyValidator");

                    if let Some(v_term) = context
                        .store
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
                        validator = parse_validator(&v_term, true, context, false)?;
                    }
                    if let Some(v_term) = context
                        .store
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
                        node_validator = parse_validator(&v_term, false, context, false)?;
                    }
                    if let Some(v_term) = context
                        .store
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
                        property_validator = parse_validator(&v_term, false, context, true)?;
                    }

                    definitions.insert(
                        cc_iri.clone(),
                        CustomConstraintComponentDefinition {
                            iri: cc_iri.clone(),
                            parameters,
                            validator,
                            node_validator,
                            property_validator,
                            messages: component_messages,
                            severity: component_severity,
                        },
                    );
                }
            }
        }
    }

    Ok((definitions, param_to_component))
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

        validate_prebound_variable_usage(
            &validator.query,
            &format!("Custom constraint {}", self.definition.iri),
            true,
            is_prop_shape,
        )?;

        let mut results = vec![];
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
            let param_name = local_name(param_path);
            if query_mentions_var(&query_body, &param_name) {
                let value = values.first().ok_or_else(|| {
                    format!(
                        "Custom constraint {} is missing a value for parameter {} needed by its SPARQL query.",
                        self.definition.iri,
                        param_name
                    )
                })?;
                let var = Variable::new_unchecked(&param_name);
                substitutions.push((var.clone(), value.clone()));
                prebound_vars.insert(var);
            }
        }

        let include_value = validator.is_ask && query_mentions_var(&query_body, "value");
        if include_value {
            prebound_vars.insert(Variable::new_unchecked("value"));
        }

        let apply_prefixes = |body: &str| {
            if validator.prefixes.is_empty() {
                body.to_string()
            } else {
                format!("{}\n{}", validator.prefixes, body)
            }
        };

        let query_with_prefixes = apply_prefixes(&query_body);
        let context_label = if validator.is_ask {
            format!("SPARQL ASK validator {}", self.definition.iri)
        } else {
            format!("SPARQL SELECT validator {}", self.definition.iri)
        };

        let algebra_query = AlgebraQuery::parse(&query_with_prefixes, None)
            .map_err(|e| format!("Failed to parse SPARQL validator query: {}", e))?;

        ensure_pre_binding_semantics(
            &algebra_query,
            &context_label,
            &prebound_vars,
            &optional_prebound_vars,
        )?;

        if validator.is_ask {
            if let Some(value_nodes) = c.value_nodes() {
                for value_node in value_nodes {
                    let mut ask_substitutions = substitutions.clone();
                    if include_value {
                        ask_substitutions
                            .push((Variable::new_unchecked("value"), value_node.clone()));
                    }

                    let mut parsed_query = Query::parse(&query_with_prefixes, None)
                        .map_err(|e| format!("Failed to parse SPARQL validator query: {}", e))?;
                    parsed_query.dataset_mut().set_default_graph_as_union();

                    match context.model.store().query_opt_with_substituted_variables(
                        parsed_query,
                        QueryOptions::default(),
                        ask_substitutions,
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
                                let (message_opt, message_terms) = instantiate_message_terms(
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
                        Err(e) => return Err(format!("SPARQL query failed: {}", e)),
                        _ => {} // Other query results types are ignored for ASK
                    }
                }
            }
        } else {
            // SELECT validator
            let mut parsed_query = Query::parse(&query_with_prefixes, None)
                .map_err(|e| format!("Failed to parse SPARQL validator query: {}", e))?;
            parsed_query.dataset_mut().set_default_graph_as_union();

            match context.model.store().query_opt_with_substituted_variables(
                parsed_query,
                QueryOptions::default(),
                substitutions.clone(),
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
                            // Skip duplicate solutions
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
                        for var in solution.variables() {
                            if let Some(term) = solution.get(var) {
                                substitutions_for_messages.push((
                                    var.as_str().to_string(),
                                    term_ref_to_message_value(term.into()),
                                ));
                            }
                        }
                        for (param_path, values) in &self.parameter_values {
                            if let Some(val) = values.first() {
                                substitutions_for_messages
                                    .push((local_name(param_path), term_to_message_value(val)));
                            }
                        }
                        let (message_opt, message_terms) = instantiate_message_terms(
                            &message_templates,
                            &substitutions_for_messages,
                        );
                        let message = message_opt.unwrap_or_else(|| {
                            format!(
                                "Node does not conform to custom constraint {}",
                                self.definition.iri
                            )
                        });

                        let result_path_override =
                            if let Some(Term::NamedNode(path_iri)) = solution.get("path") {
                                Some(Path::Simple(Term::NamedNode(path_iri.clone())))
                            } else {
                                None
                            };

                        let severity_override = validator
                            .severity
                            .clone()
                            .or_else(|| self.definition.severity.clone());

                        let failure = ValidationFailure::new(
                            component_id,
                            failed_value_node.clone(),
                            message,
                            result_path_override,
                            None,
                        )
                        .with_severity(severity_override)
                        .with_message_terms(message_terms);

                        results.push(ComponentValidationResult::Fail(c.clone(), failure));
                    }
                }
                Err(e) => return Err(format!("SPARQL query failed: {}", e)),
                _ => {}
            }
        }

        Ok(results)
    }
}
