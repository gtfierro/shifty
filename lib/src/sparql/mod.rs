#![allow(clippy::manual_flatten)]

use crate::context::ParsingContext;
use crate::model::components::sparql::{
    CustomConstraintComponentDefinition, Parameter, SPARQLValidator,
};
use crate::named_nodes::{RDF, SHACL};
use crate::types::Severity;
use log::warn;
use ontoenv::api::{OntoEnv, ResolveTarget};
use oxigraph::model::{
    GraphNameRef, Literal, NamedNode, NamedNodeRef, NamedOrBlankNodeRef as SubjectRef, Term,
};
use oxigraph::sparql::{PreparedSparqlQuery, QueryResults, SparqlEvaluator, Variable};
use oxigraph::store::Store;
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression};
use spargebra::term::GroundTerm;
use spargebra::{Query as AlgebraQuery, SparqlParser};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;

type CustomComponentMaps = (
    HashMap<NamedNode, CustomConstraintComponentDefinition>,
    HashMap<NamedNode, Vec<NamedNode>>,
);

fn is_builtin_component(iri: &NamedNode) -> bool {
    let iri_str = iri.as_str();
    iri_str.starts_with("http://www.w3.org/ns/shacl#")
        || iri_str.starts_with("https://www.w3.org/ns/shacl#")
}

/// Executes SHACL SPARQL queries with prefix and prepared-query caching.
pub trait SparqlExecutor {
    /// Resolves `sh:declare` and inline prefixes for a SPARQL node.
    fn prefixes_for_node(
        &self,
        node: &Term,
        store: &Store,
        env: &OntoEnv,
        shape_graph_iri_ref: GraphNameRef<'_>,
    ) -> Result<String, String>;

    /// Compiles and caches a prepared SPARQL query for reuse.
    fn prepared_query(&self, query_str: &str) -> Result<PreparedSparqlQuery, String>;

    /// Produces the algebraic representation of a query to validate pre-binding rules.
    fn algebra(&self, query_str: &str) -> Result<AlgebraQuery, String>;

    /// Executes a prepared query with variable substitutions against a store.
    fn execute_with_substitutions<'a>(
        &self,
        query_str: &str,
        prepared: &PreparedSparqlQuery,
        store: &'a Store,
        substitutions: &[(Variable, Term)],
        enforce_values_clause: bool,
    ) -> Result<QueryResults<'a>, String>;
}

/// Instantiates localized message templates.
pub trait MessageTemplater {
    fn instantiate_messages(
        &self,
        templates: &[Term],
        substitutions: &[(String, String)],
    ) -> (Option<String>, Vec<Term>);
}

#[derive(Default)]
pub struct SparqlServices {
    prefix_cache: Mutex<HashMap<Term, String>>,
    prepared_cache: Mutex<HashMap<String, PreparedSparqlQuery>>,
    algebra_cache: Mutex<HashMap<String, AlgebraQuery>>,
}

impl SparqlServices {
    pub fn new() -> Self {
        Self::default()
    }

    fn cache_key(query_str: &str) -> String {
        query_str.to_string()
    }
}

impl SparqlExecutor for SparqlServices {
    fn prefixes_for_node(
        &self,
        node: &Term,
        store: &Store,
        env: &OntoEnv,
        shape_graph_iri_ref: GraphNameRef<'_>,
    ) -> Result<String, String> {
        if let Some(prefixes) = self.prefix_cache.lock().unwrap().get(node) {
            return Ok(prefixes.clone());
        }

        let subject_ref = to_subject_ref(node)?;
        let shacl = SHACL::new();

        let mut prefixes_subjects: HashSet<Term> = store
            .quads_for_pattern(
                Some(subject_ref),
                Some(shacl.prefixes),
                None,
                Some(shape_graph_iri_ref),
            )
            .filter_map(Result::ok)
            .map(|q| q.object)
            .collect();

        prefixes_subjects.extend(
            store
                .quads_for_pattern(None, Some(shacl.declare), None, None)
                .filter_map(Result::ok)
                .map(|q| q.subject.into()),
        );

        let mut collected_prefixes: HashMap<String, String> = HashMap::new();

        for prefixes_subject in prefixes_subjects {
            let declarations: Vec<Term> = store
                .quads_for_pattern(
                    Some(to_subject_ref(&prefixes_subject)?),
                    Some(shacl.declare),
                    None,
                    None,
                )
                .filter_map(Result::ok)
                .map(|q| q.object)
                .collect();

            for declaration in declarations {
                let decl_subject = to_subject_ref(&declaration).map_err(|_| {
                    format!(
                        "sh:declare value must be an IRI or blank node, but found: {}",
                        declaration
                    )
                })?;

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

            if let Term::NamedNode(ontology_iri) = &prefixes_subject {
                if let Some(graphid) = env.resolve(ResolveTarget::Graph(ontology_iri.clone())) {
                    if let Ok(ont) = env.get_ontology(&graphid) {
                        for (prefix, namespace) in ont.namespace_map().iter() {
                            if let Some(existing_namespace) =
                                collected_prefixes.get(prefix.as_str())
                            {
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
        }

        const DEFAULT_PREFIXES: &[(&str, &str)] = &[
            ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
            ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
            ("xsd", "http://www.w3.org/2001/XMLSchema#"),
            ("owl", "http://www.w3.org/2002/07/owl#"),
            ("sh", "http://www.w3.org/ns/shacl#"),
        ];

        for (prefix, namespace) in DEFAULT_PREFIXES {
            collected_prefixes
                .entry(prefix.to_string())
                .or_insert_with(|| namespace.to_string());
        }

        let prefix_strs: Vec<String> = collected_prefixes
            .iter()
            .map(|(prefix, iri)| format!("PREFIX {}: <{}>", prefix, iri))
            .collect();
        let joined = prefix_strs.join("\n");
        self.prefix_cache
            .lock()
            .unwrap()
            .insert(node.clone(), joined.clone());
        Ok(joined)
    }

    fn prepared_query(&self, query_str: &str) -> Result<PreparedSparqlQuery, String> {
        let key = Self::cache_key(query_str);
        if let Some(cached) = self.prepared_cache.lock().unwrap().get(&key) {
            return Ok(cached.clone());
        }

        let mut prepared = SparqlEvaluator::new()
            .parse_query(query_str)
            .map_err(|e| format!("Failed to parse SPARQL query: {}", e))?;
        prepared.dataset_mut().set_default_graph_as_union();
        self.prepared_cache
            .lock()
            .unwrap()
            .insert(key.clone(), prepared.clone());
        Ok(prepared)
    }

    fn algebra(&self, query_str: &str) -> Result<AlgebraQuery, String> {
        let key = Self::cache_key(query_str);
        if let Some(cached) = self.algebra_cache.lock().unwrap().get(&key) {
            return Ok(cached.clone());
        }

        let algebra = SparqlParser::new()
            .parse_query(query_str)
            .map_err(|e| format!("SPARQL parse error: {}", e))?;
        self.algebra_cache
            .lock()
            .unwrap()
            .insert(key, algebra.clone());
        Ok(algebra)
    }

    fn execute_with_substitutions<'a>(
        &self,
        query_str: &str,
        prepared: &PreparedSparqlQuery,
        store: &'a Store,
        substitutions: &[(Variable, Term)],
        enforce_values_clause: bool,
    ) -> Result<QueryResults<'a>, String> {
        if enforce_values_clause && !substitutions.is_empty() {
            return execute_with_values_clause(query_str, prepared, store, substitutions, None);
        }

        let mut bound = prepared.clone().on_store(store);
        for (var, term) in substitutions {
            bound = bound.substitute_variable(var.clone(), term.clone());
        }
        match bound.execute() {
            Ok(results) => Ok(results),
            Err(e) => {
                let message = e.to_string();
                if !message.contains("does not contains variable") {
                    return Err(message);
                }
                execute_with_values_clause(query_str, prepared, store, substitutions, Some(message))
            }
        }
    }
}

impl MessageTemplater for SparqlServices {
    fn instantiate_messages(
        &self,
        templates: &[Term],
        substitutions: &[(String, String)],
    ) -> (Option<String>, Vec<Term>) {
        instantiate_message_terms(templates, substitutions)
    }
}

fn to_subject_ref(term: &Term) -> Result<SubjectRef<'_>, String> {
    match term {
        Term::NamedNode(n) => Ok(n.as_ref().into()),
        Term::BlankNode(b) => Ok(b.as_ref().into()),
        _ => Err(format!("Invalid subject term {:?}", term)),
    }
}

fn extract_template_literal(
    context: &ParsingContext,
    subject_term: &Term,
    predicate: NamedNodeRef<'_>,
) -> Option<String> {
    let subject_ref = to_subject_ref(subject_term).ok()?;
    context
        .store
        .quads_for_pattern(
            Some(subject_ref),
            Some(predicate),
            None,
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok)
        .find_map(|quad| match quad.object {
            Term::Literal(lit) => Some(lit.value().to_string()),
            _ => None,
        })
}

fn collect_template_extras(
    context: &ParsingContext,
    subject_term: &Term,
    ignored_predicates: &[NamedNode],
) -> BTreeMap<NamedNode, Vec<Term>> {
    let mut extras = BTreeMap::new();
    let subject_ref = match to_subject_ref(subject_term) {
        Ok(subject) => subject,
        Err(_) => return extras,
    };
    let ignored: HashSet<String> = ignored_predicates
        .iter()
        .map(|pred| pred.as_str().to_string())
        .collect();
    let rdf = RDF::new();
    for quad in context
        .store
        .quads_for_pattern(
            Some(subject_ref),
            None,
            None,
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok)
    {
        let predicate_owned = quad.predicate.clone();
        if ignored.contains(predicate_owned.as_str())
            || predicate_owned.as_str() == rdf.type_.as_str()
        {
            continue;
        }
        extras
            .entry(predicate_owned)
            .or_insert_with(Vec::new)
            .push(quad.object);
    }
    extras
}

fn query_mentions_var(query: &str, var: &str) -> bool {
    fn contains(query: &str, prefix: char, var: &str) -> bool {
        let mut start = 0;
        let bytes = query.as_bytes();
        let var_bytes = var.as_bytes();
        while let Some(pos) = query[start..].find(prefix) {
            let idx = start + pos + 1;
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

fn execute_with_values_clause<'a>(
    query_str: &str,
    prepared: &PreparedSparqlQuery,
    store: &'a Store,
    substitutions: &[(Variable, Term)],
    original_error: Option<String>,
) -> Result<QueryResults<'a>, String> {
    let mut value_vars = Vec::new();
    let mut value_row = Vec::new();
    let mut remaining = Vec::new();

    for (var, term) in substitutions {
        match GroundTerm::try_from(term.clone()) {
            Ok(ground) => {
                value_vars.push(var.clone());
                value_row.push(Some(ground));
            }
            Err(_) => {
                remaining.push((var.clone(), term.clone()));
            }
        }
    }

    if value_vars.is_empty() {
        if let Some(err) = original_error {
            return Err(err);
        }
        return Err("No ground terms available for VALUES clause".to_string());
    }

    let mut query = SparqlParser::new()
        .parse_query(query_str)
        .map_err(|e| format!("Failed to reparse SPARQL query with substitutions: {}", e))?;

    let values_pattern = GraphPattern::Values {
        variables: value_vars.clone(),
        bindings: vec![value_row],
    };

    query = wrap_with_values(query, values_pattern);

    let dataset_snapshot = prepared.dataset().clone();
    let mut fallback_prepared = SparqlEvaluator::new().for_query(query);
    *fallback_prepared.dataset_mut() = dataset_snapshot;

    let mut bound = fallback_prepared.on_store(store);
    for (var, term) in remaining {
        bound = bound.substitute_variable(var, term);
    }

    bound.execute().map_err(|e| e.to_string())
}

fn wrap_with_values(query: spargebra::Query, values: GraphPattern) -> spargebra::Query {
    match query {
        spargebra::Query::Select {
            dataset,
            pattern,
            base_iri,
        } => spargebra::Query::Select {
            dataset,
            base_iri,
            pattern: prepend_values(pattern, &values),
        },
        spargebra::Query::Construct {
            template,
            dataset,
            pattern,
            base_iri,
        } => spargebra::Query::Construct {
            template,
            dataset,
            base_iri,
            pattern: prepend_values(pattern, &values),
        },
        spargebra::Query::Describe {
            dataset,
            pattern,
            base_iri,
        } => spargebra::Query::Describe {
            dataset,
            base_iri,
            pattern: prepend_values(pattern, &values),
        },
        spargebra::Query::Ask {
            dataset,
            pattern,
            base_iri,
        } => spargebra::Query::Ask {
            dataset,
            base_iri,
            pattern: prepend_values(pattern, &values),
        },
    }
}

fn prepend_values(pattern: GraphPattern, values: &GraphPattern) -> GraphPattern {
    match pattern {
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(prepend_values(*inner, values)),
            variables,
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(prepend_values(*inner, values)),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(prepend_values(*inner, values)),
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(prepend_values(*inner, values)),
            start,
            length,
        },
        GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
            inner: Box::new(prepend_values(*inner, values)),
            expression,
        },
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => GraphPattern::Group {
            inner: Box::new(prepend_values(*inner, values)),
            variables,
            aggregates,
        },
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => GraphPattern::Extend {
            inner: Box::new(prepend_values(*inner, values)),
            variable,
            expression,
        },
        GraphPattern::Filter { expr, inner } => GraphPattern::Filter {
            expr,
            inner: Box::new(prepend_values(*inner, values)),
        },
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(prepend_values(*left, values)),
            right: Box::new(prepend_values(*right, values)),
        },
        GraphPattern::Union { left, right } => GraphPattern::Union {
            left: Box::new(prepend_values(*left, values)),
            right: Box::new(prepend_values(*right, values)),
        },
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => GraphPattern::LeftJoin {
            left: Box::new(prepend_values(*left, values)),
            right: Box::new(prepend_values(*right, values)),
            expression,
        },
        GraphPattern::Lateral { left, right } => GraphPattern::Lateral {
            left: Box::new(prepend_values(*left, values)),
            right: Box::new(prepend_values(*right, values)),
        },
        GraphPattern::Minus { left, right } => GraphPattern::Minus {
            left: Box::new(prepend_values(*left, values)),
            right: Box::new(prepend_values(*right, values)),
        },
        GraphPattern::Graph { name, inner } => GraphPattern::Graph {
            name,
            inner: Box::new(prepend_values(*inner, values)),
        },
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => GraphPattern::Service {
            name,
            inner: Box::new(prepend_values(*inner, values)),
            silent,
        },
        GraphPattern::Values { .. } => GraphPattern::Join {
            left: Box::new(values.clone()),
            right: Box::new(pattern),
        },
        _ => GraphPattern::Join {
            left: Box::new(values.clone()),
            right: Box::new(pattern),
        },
    }
}

pub fn validate_prebound_variable_usage(
    query: &str,
    context_label: &str,
    require_this: bool,
    require_path: bool,
) -> Result<(), String> {
    if require_this && !query_mentions_var(query, "this") {
        return Err(format!(
            "{} must reference the pre-bound variable $this (or ?this).\n{}",
            context_label, query
        ));
    }

    if require_path && !query_mentions_var(query, "PATH") {
        // SHACL allows property-shaped constraints to omit $PATH even though it is pre-bound.
        // We still advertise the binding but no longer reject queries that do not reference it.
        return Ok(());
    }

    Ok(())
}

pub fn ensure_pre_binding_semantics(
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
            inner, expression, ..
        } => {
            check_graph_pattern(inner, context_label, prebound, optional, false)?;
            check_expression(expression, context_label, prebound, optional)?;
            Ok(())
        }
        GraphPattern::Minus { left, right } => {
            check_graph_pattern(left, context_label, prebound, optional, false)?;
            check_graph_pattern(right, context_label, prebound, optional, false)
        }
        GraphPattern::Service { inner, .. } => {
            check_graph_pattern(inner, context_label, prebound, optional, false)
        }
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            check_graph_pattern(inner, context_label, prebound, optional, false)?;
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
            Ok(())
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
        GraphPattern::OrderBy { inner, expression } => {
            for expr in expression {
                check_order_expression(expr, context_label, prebound, optional)?;
            }
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

pub fn parse_custom_constraint_components<E: SparqlExecutor>(
    context: &ParsingContext,
    services: &E,
) -> Result<CustomComponentMaps, String> {
    let mut definitions = HashMap::new();
    let mut param_to_component: HashMap<NamedNode, Vec<NamedNode>> = HashMap::new();
    let shacl = SHACL::new();
    let strict = context.strict_custom_constraints;

    let shapes_graph_iri = context.shape_graph_iri.as_str();
    let query = format!(
        "PREFIX sh: <http://www.w3.org/ns/shacl#>\nPREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\nSELECT DISTINCT ?cc FROM <{}> WHERE {{ ?cc a ?ccType . ?ccType rdfs:subClassOf* sh:ConstraintComponent }}",
        shapes_graph_iri
    );
    let prepared_components = services
        .prepared_query(&query)
        .map_err(|e| format!("Failed to prepare constraint component query: {}", e))?;

    if let Ok(QueryResults::Solutions(solutions)) = services.execute_with_substitutions(
        &query,
        &prepared_components,
        &context.store,
        &[],
        false,
    ) {
        for solution_res in solutions {
            if let Ok(solution) = solution_res {
                if let Some(Term::NamedNode(cc_iri)) = solution.get("cc") {
                    if is_builtin_component(cc_iri) {
                        continue;
                    }
                    let parse_result = (|| -> Result<(), String> {
                        // Quick structural validation before running heavier SPARQL queries.
                        let has_validator = context
                            .store
                            .quads_for_pattern(
                                Some(cc_iri.as_ref().into()),
                                None,
                                None,
                                Some(context.shape_graph_iri_ref()),
                            )
                            .filter_map(Result::ok)
                            .any(|quad| {
                                let predicate = quad.predicate.as_ref();
                                predicate == shacl.validator
                                    || predicate == shacl.node_validator
                                    || predicate == shacl.property_validator
                            });
                        if !has_validator {
                            return Err(format!(
                                "Custom constraint component {} must declare at least one validator.",
                                cc_iri
                            ));
                        }

                        let has_parameter = context
                            .store
                            .quads_for_pattern(
                                Some(cc_iri.as_ref().into()),
                                Some(shacl.parameter),
                                None,
                                Some(context.shape_graph_iri_ref()),
                            )
                            .filter_map(Result::ok)
                            .next()
                            .is_some();
                        if !has_parameter {
                            return Err(format!(
                                "Custom constraint component {} must declare at least one sh:parameter.",
                                cc_iri
                            ));
                        }

                        let mut parameters = vec![];
                        let mut parameter_paths = Vec::new();
                        for param_quad in context
                            .store
                            .quads_for_pattern(
                                Some(cc_iri.as_ref().into()),
                                Some(shacl.parameter),
                                None,
                                Some(context.shape_graph_iri_ref()),
                            )
                            .filter_map(Result::ok)
                        {
                            let param_term = param_quad.object;
                            let param_subject = to_subject_ref(&param_term).map_err(|_| {
                                format!(
                                    "Custom constraint parameter {:?} must be an IRI or blank node.",
                                    param_term
                                )
                            })?;
                            let path = context
                                .store
                                .quads_for_pattern(
                                    Some(param_subject),
                                    Some(shacl.path),
                                    None,
                                    Some(context.shape_graph_iri_ref()),
                                )
                                .filter_map(Result::ok)
                                .find_map(|q| match q.object {
                                    Term::NamedNode(nn) => Some(nn),
                                    _ => None,
                                })
                                .ok_or_else(|| {
                                    format!(
                                        "Custom constraint parameter {:?} is missing sh:path.",
                                        param_term
                                    )
                                })?;
                            let optional = context
                                .store
                                .quads_for_pattern(
                                    Some(param_subject),
                                    Some(shacl.optional),
                                    None,
                                    Some(context.shape_graph_iri_ref()),
                                )
                                .filter_map(Result::ok)
                                .any(|q| match q.object {
                                    Term::Literal(ref lit) => {
                                        let v = lit.value();
                                        v.eq_ignore_ascii_case("true") || v == "1"
                                    }
                                    _ => false,
                                });
                            let default_values: Vec<Term> = context
                                .store
                                .quads_for_pattern(
                                    Some(param_subject),
                                    Some(shacl.default_value),
                                    None,
                                    Some(context.shape_graph_iri_ref()),
                                )
                                .filter_map(Result::ok)
                                .map(|q| q.object)
                                .collect();
                            let var_name =
                                extract_template_literal(context, &param_term, shacl.var_name);
                            let name = extract_template_literal(context, &param_term, shacl.name);
                            let description =
                                extract_template_literal(context, &param_term, shacl.description);
                            let extra = collect_template_extras(
                                context,
                                &param_term,
                                &[
                                    shacl.path.into_owned(),
                                    shacl.optional.into_owned(),
                                    shacl.var_name.into_owned(),
                                    shacl.default_value.into_owned(),
                                    shacl.name.into_owned(),
                                    shacl.description.into_owned(),
                                ],
                            );
                            parameters.push(Parameter {
                                subject: param_term.clone(),
                                path: path.clone(),
                                optional,
                                var_name,
                                default_values,
                                name,
                                description,
                                extra,
                            });
                            parameter_paths.push(path);
                        }

                        parameters.sort_by(|a, b| {
                            let order = a.path.as_str().cmp(b.path.as_str());
                            if order != std::cmp::Ordering::Equal {
                                return order;
                            }
                            let order = a.var_name.as_deref().cmp(&b.var_name.as_deref());
                            if order != std::cmp::Ordering::Equal {
                                return order;
                            }
                            a.subject.to_string().cmp(&b.subject.to_string())
                        });

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
                            .find_map(|term| {
                                <Severity as crate::types::SeverityExt>::from_term(&term)
                            });

                        let parse_validator =
                            |v_term: &Term,
                             is_ask: bool,
                             context: &ParsingContext,
                             services: &E,
                             require_path: bool,
                             require_this: bool|
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
                                    require_this,
                                    require_path,
                                )?;

                                let prefixes = services.prefixes_for_node(
                                    v_term,
                                    &context.store,
                                    &context.env,
                                    context.shape_graph_iri_ref(),
                                )?;

                                let full_query = if prefixes.is_empty() {
                                    query_str.clone()
                                } else {
                                    format!("{}\n{}", prefixes, query_str)
                                };

                                if !require_path {
                                    let _ = services.prepared_query(&full_query)?;
                                    let mut prebound = HashSet::new();
                                    if require_this {
                                        prebound.insert(Variable::new_unchecked("this"));
                                    }
                                    if query_mentions_var(&full_query, "currentShape") {
                                        prebound.insert(Variable::new_unchecked("currentShape"));
                                    }
                                    if query_mentions_var(&full_query, "shapesGraph") {
                                        prebound.insert(Variable::new_unchecked("shapesGraph"));
                                    }
                                    let optional = HashSet::new();
                                    let algebra = services.algebra(&full_query)?;
                                    ensure_pre_binding_semantics(
                                        &algebra,
                                        &format!("Custom constraint {}", cc_iri),
                                        &prebound,
                                        &optional,
                                    )?;
                                } else {
                                    let mut prebound = HashSet::new();
                                    if require_this {
                                        prebound.insert(Variable::new_unchecked("this"));
                                    }
                                    prebound.insert(Variable::new_unchecked("PATH"));
                                    let normalized = full_query.replace("$PATH", "?PATH");
                                    let algebra = services.algebra(&normalized)?;
                                    ensure_pre_binding_semantics(
                                        &algebra,
                                        &format!("Custom constraint {}", cc_iri),
                                        &prebound,
                                        &HashSet::new(),
                                    )?;
                                }

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
                                    .find_map(|term| {
                                        <Severity as crate::types::SeverityExt>::from_term(&term)
                                    });

                                Ok(Some(SPARQLValidator {
                                    query: query_str,
                                    is_ask,
                                    messages,
                                    prefixes,
                                    severity,
                                    require_this,
                                    require_path,
                                }))
                            };

                        let validator_prop =
                            NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#validator");
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
                            validator =
                                parse_validator(&v_term, true, context, services, false, false)?;
                        }

                        let node_validator_prop =
                            NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#nodeValidator");
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
                            node_validator =
                                parse_validator(&v_term, false, context, services, false, true)?;
                        }

                        let property_validator_prop = NamedNodeRef::new_unchecked(
                            "http://www.w3.org/ns/shacl#propertyValidator",
                        );
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
                            property_validator =
                                parse_validator(&v_term, false, context, services, true, true)?;
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
                                template: context.component_templates.get(cc_iri).cloned(),
                            },
                        );

                        for path in parameter_paths {
                            param_to_component
                                .entry(path)
                                .or_default()
                                .push(cc_iri.clone());
                        }

                        Ok(())
                    })();

                    if let Err(err) = parse_result {
                        if strict {
                            return Err(err);
                        }
                        warn!("Skipping custom constraint component {}: {}", cc_iri, err);
                    }
                }
            }
        }
    }

    Ok((definitions, param_to_component))
}

fn substitute_placeholders(message: &str, substitutions: &[(String, String)]) -> String {
    let mut text = message.to_string();
    for (name, value) in substitutions {
        let placeholder_q = format!("{{?{}}}", name);
        let placeholder_dollar = format!("{{$${}}}", name);
        text = text.replace(&placeholder_q, value);
        text = text.replace(&placeholder_dollar, value);
    }
    text
}

pub fn instantiate_message_terms(
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
                    if datatype.as_str() == oxigraph::model::vocab::xsd::STRING {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_instantiation_handles_multiple_templates() {
        let templates = vec![
            Term::Literal(Literal::from("Value {?x}")),
            Term::Literal(Literal::from("Other {?x}")),
        ];
        let (first, instantiated) =
            instantiate_message_terms(&templates, &[("x".into(), "42".into())]);
        assert_eq!(first.as_deref(), Some("Value 42"));
        assert_eq!(instantiated.len(), 2);
    }
}
