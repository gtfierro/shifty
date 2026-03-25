#![allow(clippy::manual_flatten)]

use crate::context::ParsingContext;
use crate::context::format_term_for_label;
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
use spargebra::algebra::{
    AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression,
};
use spargebra::term::{BlankNode, GroundTerm, NamedNodePattern, TermPattern, TriplePattern};
use spargebra::{Query as AlgebraQuery, SparqlParser};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

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
        env: &Arc<RwLock<OntoEnv>>,
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

    /// Executes a prepared query with a batched VALUES clause for a single variable.
    fn execute_with_value_rows<'a>(
        &self,
        query_str: &str,
        prepared: &PreparedSparqlQuery,
        store: &'a Store,
        value_var: &Variable,
        values: &[Term],
        substitutions: &[(Variable, Term)],
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ThisPredicateDirection {
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ThisPredicateRequirement {
    pub predicate: NamedNode,
    pub direction: ThisPredicateDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoweredSparqlQueryKind {
    AdjacentPredicateWhitelist(AdjacentPredicateWhitelistPlan),
    RequiredPathSupport(RequiredPathSupportPlan),
    MissingRelatedNode(MissingRelatedNodePlan),
    LocalSetCompatibility(Box<LocalSetCompatibilityPlan>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompiledTargetSelectQuery {
    ClassInstances { class: NamedNode },
    SubjectsOf { predicate: NamedNode },
    ObjectsOf { predicate: NamedNode },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdjacentPredicateWhitelistPlan {
    pub anchor_path: LoweredPropertyPath,
    pub allowed_predicates: Vec<NamedNode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequiredPathSupportPlan {
    pub antecedent_path: LoweredPropertyPath,
    pub support_path: LoweredPropertyPath,
    pub target_variable: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingRelatedNodePlan {
    pub related_path: LoweredPropertyPath,
    pub related_variable: String,
    pub related_class: Option<Term>,
    pub required_path: LoweredPropertyPath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalSetCompatibilityMode {
    PurePure,
    CompositeVsPure { composite_side: CompatibilitySide },
    CompositeVsComposite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilitySide {
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSetCompatibilityPlan {
    pub left_anchor_path: LoweredPropertyPath,
    pub right_anchor_path: LoweredPropertyPath,
    pub left_anchor_var: String,
    pub right_anchor_var: String,
    pub left_class: Option<Term>,
    pub right_class: Option<Term>,
    pub left_value_path: LoweredPropertyPath,
    pub right_value_path: LoweredPropertyPath,
    pub left_value_var: String,
    pub right_value_var: String,
    pub distinct_anchors: bool,
    pub composed_of_predicate: NamedNode,
    pub constituent_path: Option<LoweredPropertyPath>,
    pub mode: LocalSetCompatibilityMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoweredPropertyPath {
    SelfNode,
    NamedNode(NamedNode),
    ReverseNamedNode(NamedNode),
    ZeroOrOne(Box<LoweredPropertyPath>),
    ZeroOrMore(Box<LoweredPropertyPath>),
    OneOrMore(Box<LoweredPropertyPath>),
    Sequence(Vec<LoweredPropertyPath>),
    Alternative(Vec<LoweredPropertyPath>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompiledSparqlRule {
    PathCopy {
        construct_predicate: NamedNode,
        source_path: LoweredPropertyPath,
    },
    EqualityConstant {
        construct_predicate: NamedNode,
        left_path: LoweredPropertyPath,
        right_path: LoweredPropertyPath,
        object: Term,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CompiledIndexRequirement {
    OutgoingValues { predicate: NamedNode },
    IncomingValues { predicate: NamedNode },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledPredicateIndexPlan {
    pub predicate: NamedNode,
    pub include_outgoing: bool,
    pub include_incoming: bool,
}

pub trait CompiledPathResolver: Clone {
    type Error;

    fn direct_values(&self, focus: &Term, predicate: &NamedNode) -> Result<Vec<Term>, Self::Error>;

    fn inverse_values(&self, focus: &Term, predicate: &NamedNode)
    -> Result<Vec<Term>, Self::Error>;

    fn without_delta(&self) -> Self;
}

pub fn evaluate_compiled_path<R>(
    resolver: &R,
    focus: &Term,
    path: &LoweredPropertyPath,
) -> Result<Vec<Term>, R::Error>
where
    R: CompiledPathResolver,
{
    match path {
        LoweredPropertyPath::SelfNode => Ok(vec![focus.clone()]),
        LoweredPropertyPath::NamedNode(predicate) => resolver.direct_values(focus, predicate),
        LoweredPropertyPath::ReverseNamedNode(predicate) => {
            resolver.inverse_values(focus, predicate)
        }
        LoweredPropertyPath::ZeroOrOne(inner) => {
            let mut results = HashSet::from([focus.clone()]);
            let base = resolver.without_delta();
            results.extend(evaluate_compiled_path(&base, focus, inner)?);
            Ok(results.into_iter().collect())
        }
        LoweredPropertyPath::ZeroOrMore(inner) => {
            let mut results = HashSet::new();
            let mut frontier = vec![focus.clone()];
            let base = resolver.without_delta();
            while let Some(node) = frontier.pop() {
                if !results.insert(node.clone()) {
                    continue;
                }
                frontier.extend(evaluate_compiled_path(&base, &node, inner)?);
            }
            Ok(results.into_iter().collect())
        }
        LoweredPropertyPath::OneOrMore(inner) => {
            let mut results = HashSet::new();
            let base = resolver.without_delta();
            let mut frontier = evaluate_compiled_path(&base, focus, inner)?;
            while let Some(node) = frontier.pop() {
                if !results.insert(node.clone()) {
                    continue;
                }
                frontier.extend(evaluate_compiled_path(&base, &node, inner)?);
            }
            Ok(results.into_iter().collect())
        }
        LoweredPropertyPath::Sequence(segments) => {
            let mut frontier = vec![focus.clone()];
            let base = resolver.without_delta();
            for (index, segment) in segments.iter().enumerate() {
                let mut next = Vec::new();
                let segment_resolver = if index + 1 == segments.len() {
                    resolver
                } else {
                    &base
                };
                for node in &frontier {
                    next.extend(evaluate_compiled_path(segment_resolver, node, segment)?);
                }
                let mut unique = HashSet::new();
                next.retain(|term| unique.insert(term.clone()));
                frontier = next;
                if frontier.is_empty() {
                    break;
                }
            }
            Ok(frontier)
        }
        LoweredPropertyPath::Alternative(alternatives) => {
            let mut results = HashSet::new();
            let base = resolver.without_delta();
            for alternative in alternatives {
                results.extend(evaluate_compiled_path(&base, focus, alternative)?);
            }
            Ok(results.into_iter().collect())
        }
    }
}

pub fn execute_compiled_rule<R>(
    resolver: &R,
    rule: &CompiledSparqlRule,
    focus: &Term,
) -> Result<Vec<(Term, NamedNode, Term)>, R::Error>
where
    R: CompiledPathResolver,
{
    match rule {
        CompiledSparqlRule::PathCopy {
            construct_predicate,
            source_path,
        } => Ok(evaluate_compiled_path(resolver, focus, source_path)?
            .into_iter()
            .map(|value| (focus.clone(), construct_predicate.clone(), value))
            .collect()),
        CompiledSparqlRule::EqualityConstant {
            construct_predicate,
            left_path,
            right_path,
            object,
        } => {
            let base = resolver.without_delta();
            let left_values = evaluate_compiled_path(&base, focus, left_path)?;
            let right_values = evaluate_compiled_path(&base, focus, right_path)?;
            let right_set: HashSet<Term> = right_values.into_iter().collect();
            if left_values
                .into_iter()
                .any(|value| right_set.contains(&value))
            {
                Ok(vec![(
                    focus.clone(),
                    construct_predicate.clone(),
                    object.clone(),
                )])
            } else {
                Ok(Vec::new())
            }
        }
    }
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
        env: &Arc<RwLock<OntoEnv>>,
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
                let env = env
                    .read()
                    .map_err(|_| "OntoEnv lock poisoned".to_string())?;
                if let Some(graphid) = env.resolve(ResolveTarget::Graph(ontology_iri.clone()))
                    && let Ok(ont) = env.get_ontology(&graphid)
                {
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

    fn execute_with_value_rows<'a>(
        &self,
        query_str: &str,
        prepared: &PreparedSparqlQuery,
        store: &'a Store,
        value_var: &Variable,
        values: &[Term],
        substitutions: &[(Variable, Term)],
    ) -> Result<QueryResults<'a>, String> {
        execute_with_values_rows_clause(
            query_str,
            prepared,
            store,
            value_var,
            values,
            substitutions,
        )
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

fn execute_with_values_rows_clause<'a>(
    query_str: &str,
    prepared: &PreparedSparqlQuery,
    store: &'a Store,
    value_var: &Variable,
    values: &[Term],
    substitutions: &[(Variable, Term)],
) -> Result<QueryResults<'a>, String> {
    if values.is_empty() {
        return Err("No VALUES rows available".to_string());
    }

    let mut bindings = Vec::with_capacity(values.len());
    for term in values {
        let ground = GroundTerm::try_from(term.clone())
            .map_err(|_| format!("VALUES batching requires a ground term for ?{}", value_var))?;
        bindings.push(vec![Some(ground)]);
    }

    let mut query = SparqlParser::new()
        .parse_query(query_str)
        .map_err(|e| format!("Failed to reparse batched SPARQL query: {}", e))?;

    let values_pattern = GraphPattern::Values {
        variables: vec![value_var.clone()],
        bindings,
    };

    query = wrap_with_values(query, values_pattern);
    query = ensure_projected_variable(query, value_var);

    let dataset_snapshot = prepared.dataset().clone();
    let mut fallback_prepared = SparqlEvaluator::new().for_query(query);
    *fallback_prepared.dataset_mut() = dataset_snapshot;

    let mut bound = fallback_prepared.on_store(store);
    for (var, term) in substitutions {
        bound = bound.substitute_variable(var.clone(), term.clone());
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

fn ensure_projected_variable(query: spargebra::Query, variable: &Variable) -> spargebra::Query {
    match query {
        spargebra::Query::Select {
            dataset,
            pattern,
            base_iri,
        } => spargebra::Query::Select {
            dataset,
            base_iri,
            pattern: ensure_projected_variable_in_pattern(pattern, variable),
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
            pattern,
        },
        spargebra::Query::Describe {
            dataset,
            pattern,
            base_iri,
        } => spargebra::Query::Describe {
            dataset,
            base_iri,
            pattern,
        },
        spargebra::Query::Ask {
            dataset,
            pattern,
            base_iri,
        } => spargebra::Query::Ask {
            dataset,
            base_iri,
            pattern,
        },
    }
}

fn ensure_projected_variable_in_pattern(
    pattern: GraphPattern,
    variable: &Variable,
) -> GraphPattern {
    match pattern {
        GraphPattern::Project {
            inner,
            mut variables,
        } => {
            if !variables.iter().any(|candidate| candidate == variable) {
                variables.push(variable.clone());
            }
            GraphPattern::Project { inner, variables }
        }
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(ensure_projected_variable_in_pattern(*inner, variable)),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(ensure_projected_variable_in_pattern(*inner, variable)),
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(ensure_projected_variable_in_pattern(*inner, variable)),
            start,
            length,
        },
        GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
            inner: Box::new(ensure_projected_variable_in_pattern(*inner, variable)),
            expression,
        },
        other => GraphPattern::Project {
            inner: Box::new(other),
            variables: vec![variable.clone()],
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

pub fn required_this_predicates(query: &AlgebraQuery) -> HashSet<ThisPredicateRequirement> {
    match query {
        AlgebraQuery::Select { pattern, .. }
        | AlgebraQuery::Ask { pattern, .. }
        | AlgebraQuery::Construct { pattern, .. }
        | AlgebraQuery::Describe { pattern, .. } => {
            required_this_predicates_in_graph_pattern(pattern)
        }
    }
}

pub fn lowered_sparql_query_kind(query: &AlgebraQuery) -> Option<LoweredSparqlQueryKind> {
    match query {
        AlgebraQuery::Select { pattern, .. }
        | AlgebraQuery::Ask { pattern, .. }
        | AlgebraQuery::Construct { pattern, .. }
        | AlgebraQuery::Describe { pattern, .. } => {
            lowered_sparql_query_kind_in_graph_pattern(pattern)
        }
    }
}

pub fn compiled_sparql_rule(query: &AlgebraQuery) -> Option<CompiledSparqlRule> {
    compiled_path_copy_rule(query).or_else(|| compiled_equality_constant_rule(query))
}

pub fn compiled_sparql_index_requirements(
    rule: &CompiledSparqlRule,
) -> Vec<CompiledIndexRequirement> {
    let mut requirements = Vec::new();
    match rule {
        CompiledSparqlRule::PathCopy { source_path, .. } => {
            collect_compiled_path_index_requirements(source_path, &mut requirements);
        }
        CompiledSparqlRule::EqualityConstant {
            left_path,
            right_path,
            ..
        } => {
            collect_compiled_path_index_requirements(left_path, &mut requirements);
            collect_compiled_path_index_requirements(right_path, &mut requirements);
        }
    }
    requirements.sort_by(|left, right| {
        compiled_index_requirement_sort_key(left).cmp(&compiled_index_requirement_sort_key(right))
    });
    requirements.dedup();
    requirements
}

pub fn compiled_target_select_query_from_str(query: &str) -> Option<CompiledTargetSelectQuery> {
    let algebra = SparqlParser::new().parse_query(query).ok()?;
    compiled_target_select_query(&algebra)
}

pub fn compiled_target_select_query(query: &AlgebraQuery) -> Option<CompiledTargetSelectQuery> {
    let AlgebraQuery::Select { pattern, .. } = query else {
        return None;
    };
    let target_var = target_select_projection_var(pattern)?;
    compiled_target_select_query_in_graph_pattern(pattern, target_var)
}

pub fn compiled_sparql_index_plan<'a>(
    requirements: impl IntoIterator<Item = &'a CompiledIndexRequirement>,
) -> Vec<CompiledPredicateIndexPlan> {
    let mut planned = Vec::<CompiledPredicateIndexPlan>::new();
    for requirement in requirements {
        match requirement {
            CompiledIndexRequirement::OutgoingValues { predicate } => {
                if let Some(existing) = planned.iter_mut().find(|item| item.predicate == *predicate)
                {
                    existing.include_outgoing = true;
                } else {
                    planned.push(CompiledPredicateIndexPlan {
                        predicate: predicate.clone(),
                        include_outgoing: true,
                        include_incoming: false,
                    });
                }
            }
            CompiledIndexRequirement::IncomingValues { predicate } => {
                if let Some(existing) = planned.iter_mut().find(|item| item.predicate == *predicate)
                {
                    existing.include_incoming = true;
                } else {
                    planned.push(CompiledPredicateIndexPlan {
                        predicate: predicate.clone(),
                        include_outgoing: false,
                        include_incoming: true,
                    });
                }
            }
        }
    }
    planned.sort_by(|left, right| left.predicate.as_str().cmp(right.predicate.as_str()));
    planned
}

fn lowered_sparql_query_kind_in_graph_pattern(
    pattern: &GraphPattern,
) -> Option<LoweredSparqlQueryKind> {
    let pattern = unwrap_projection_like_pattern(pattern);
    if let Some(plan) = lowered_local_set_compatibility(pattern) {
        return Some(LoweredSparqlQueryKind::LocalSetCompatibility(Box::new(
            plan,
        )));
    }
    if let Some(plan) = lowered_missing_related_node(pattern) {
        return Some(LoweredSparqlQueryKind::MissingRelatedNode(plan));
    }
    let GraphPattern::Filter { expr, inner } = pattern else {
        return None;
    };

    if let Some(plan) = lowered_required_path_support(inner.as_ref(), expr) {
        return Some(LoweredSparqlQueryKind::RequiredPathSupport(plan));
    }

    let anchor_path = match unwrap_projection_like_pattern(inner.as_ref()) {
        GraphPattern::Path {
            subject,
            path,
            object,
        } if term_pattern_is_this(subject) => {
            let TermPattern::Variable(anchor_var) = object else {
                return None;
            };
            lowered_adjacent_whitelist(anchor_var.as_str(), path, expr)?
        }
        GraphPattern::Bgp { patterns } if patterns.len() == 1 => {
            let triple = &patterns[0];
            if !term_pattern_is_this(&triple.subject) {
                return None;
            }
            let spargebra::term::NamedNodePattern::NamedNode(predicate) = &triple.predicate else {
                return None;
            };
            let TermPattern::Variable(anchor_var) = &triple.object else {
                return None;
            };
            lowered_adjacent_whitelist(
                anchor_var.as_str(),
                &PropertyPathExpression::NamedNode(predicate.clone()),
                expr,
            )?
        }
        _ => return None,
    };

    Some(LoweredSparqlQueryKind::AdjacentPredicateWhitelist(
        anchor_path,
    ))
}

fn lowered_required_path_support(
    inner_pattern: &GraphPattern,
    expr: &Expression,
) -> Option<RequiredPathSupportPlan> {
    let Expression::Not(inner) = expr else {
        return None;
    };
    let Expression::Exists(support_pattern) = inner.as_ref() else {
        return None;
    };

    let (target_variable, antecedent_path) = match unwrap_projection_like_pattern(inner_pattern) {
        GraphPattern::Path {
            subject,
            path,
            object,
        } if term_pattern_is_this(subject) => {
            let TermPattern::Variable(variable) = object else {
                return None;
            };
            (variable.as_str().to_string(), lower_property_path(path)?)
        }
        GraphPattern::Bgp { patterns } if patterns.len() == 1 => {
            let triple = &patterns[0];
            if !term_pattern_is_this(&triple.subject) {
                return None;
            }
            let spargebra::term::NamedNodePattern::NamedNode(predicate) = &triple.predicate else {
                return None;
            };
            let TermPattern::Variable(variable) = &triple.object else {
                return None;
            };
            (
                variable.as_str().to_string(),
                LoweredPropertyPath::NamedNode(predicate.clone()),
            )
        }
        _ => return None,
    };

    let support_path = match unwrap_projection_like_pattern(support_pattern) {
        GraphPattern::Path {
            subject,
            path,
            object,
        } if term_pattern_is_this(subject)
            && term_pattern_matches_var(object, &target_variable) =>
        {
            lower_property_path(path)?
        }
        GraphPattern::Bgp { patterns } if patterns.len() == 1 => {
            let triple = &patterns[0];
            if !term_pattern_is_this(&triple.subject)
                || !term_pattern_matches_var(&triple.object, &target_variable)
            {
                return None;
            }
            let spargebra::term::NamedNodePattern::NamedNode(predicate) = &triple.predicate else {
                return None;
            };
            LoweredPropertyPath::NamedNode(predicate.clone())
        }
        _ => return None,
    };

    Some(RequiredPathSupportPlan {
        antecedent_path,
        support_path,
        target_variable,
    })
}

fn lowered_missing_related_node(pattern: &GraphPattern) -> Option<MissingRelatedNodePlan> {
    let (atoms, filters) = flatten_conjunctive_atoms(pattern)?;
    let mut split_filters = Vec::new();
    for filter in filters {
        split_conjunctive_filters(filter, &mut split_filters);
    }

    let (required_path, _missing_var) = split_filters
        .iter()
        .find_map(|expr| extract_missing_focus_path(expr))?;

    let mut class_filters: HashMap<String, Term> = HashMap::new();
    let mut candidate_paths: Vec<(String, LoweredPropertyPath)> = Vec::new();

    for atom in &atoms {
        match atom {
            LoweredAtom::Triple(pattern) => {
                if let Some(candidate) = lowered_related_candidate_from_triple(pattern, &atoms) {
                    candidate_paths.push(candidate);
                    continue;
                }
            }
            LoweredAtom::Path {
                subject,
                path,
                object,
            } => {
                if let Some(candidate) = lowered_related_candidate_from_path(subject, path, object)
                {
                    candidate_paths.push(candidate);
                    continue;
                }
                if let Some(subject_var) = term_pattern_var_name(subject)
                    && let Some(class_term) =
                        extract_type_subclass_constraint(subject, path, object)
                {
                    class_filters.insert(subject_var.to_string(), class_term);
                }
            }
        }
    }

    let (related_variable, related_path) = candidate_paths
        .into_iter()
        .find(|(var, _)| var != "this" && !var_is_only_filter_local(var, &split_filters))?;

    Some(MissingRelatedNodePlan {
        related_path,
        related_variable: related_variable.clone(),
        related_class: class_filters.remove(&related_variable),
        required_path,
    })
}

#[derive(Debug)]
enum LoweredAtom<'a> {
    Triple(&'a spargebra::term::TriplePattern),
    Path {
        subject: &'a TermPattern,
        path: &'a PropertyPathExpression,
        object: &'a TermPattern,
    },
}

fn term_pattern_blank_node(term: &TermPattern) -> Option<&BlankNode> {
    match term {
        TermPattern::BlankNode(node) => Some(node),
        _ => None,
    }
}

fn combine_lowered_paths(
    left: LoweredPropertyPath,
    right: LoweredPropertyPath,
) -> LoweredPropertyPath {
    match (left, right) {
        (
            LoweredPropertyPath::Sequence(mut left_items),
            LoweredPropertyPath::Sequence(right_items),
        ) => {
            left_items.extend(right_items);
            LoweredPropertyPath::Sequence(left_items)
        }
        (LoweredPropertyPath::Sequence(mut left_items), right) => {
            left_items.push(right);
            LoweredPropertyPath::Sequence(left_items)
        }
        (left, LoweredPropertyPath::Sequence(right_items)) => {
            let mut items = vec![left];
            items.extend(right_items);
            LoweredPropertyPath::Sequence(items)
        }
        (left, right) => LoweredPropertyPath::Sequence(vec![left, right]),
    }
}

fn blanknode_followup<'a>(
    blank: &BlankNode,
    atoms: &'a [LoweredAtom<'a>],
) -> Option<(LoweredPropertyPath, &'a TermPattern)> {
    atoms.iter().find_map(|atom| match atom {
        LoweredAtom::Triple(pattern)
            if term_pattern_blank_node(&pattern.subject) == Some(blank) =>
        {
            let spargebra::term::NamedNodePattern::NamedNode(predicate) = &pattern.predicate else {
                return None;
            };
            Some((
                LoweredPropertyPath::NamedNode(predicate.clone()),
                &pattern.object,
            ))
        }
        LoweredAtom::Path {
            subject,
            path,
            object,
        } if term_pattern_blank_node(subject) == Some(blank) => {
            Some((lower_property_path(path)?, object))
        }
        _ => None,
    })
}

fn lowered_related_candidate_from_triple(
    pattern: &spargebra::term::TriplePattern,
    _atoms: &[LoweredAtom<'_>],
) -> Option<(String, LoweredPropertyPath)> {
    let spargebra::term::NamedNodePattern::NamedNode(predicate) = &pattern.predicate else {
        return None;
    };
    if term_pattern_is_this(&pattern.subject) {
        let object_var = term_pattern_var_name(&pattern.object)?;
        return Some((
            object_var.to_string(),
            LoweredPropertyPath::NamedNode(predicate.clone()),
        ));
    }
    if term_pattern_is_this(&pattern.object) {
        let subject_var = term_pattern_var_name(&pattern.subject)?;
        return Some((
            subject_var.to_string(),
            LoweredPropertyPath::ReverseNamedNode(predicate.clone()),
        ));
    }
    None
}

fn lowered_related_candidate_from_path(
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
) -> Option<(String, LoweredPropertyPath)> {
    if term_pattern_is_this(subject) {
        let object_var = term_pattern_var_name(object)?;
        return Some((object_var.to_string(), lower_property_path(path)?));
    }
    if term_pattern_is_this(object) {
        let subject_var = term_pattern_var_name(subject)?;
        return Some((
            subject_var.to_string(),
            reverse_lowered_property_path(&lower_property_path(path)?)?,
        ));
    }
    None
}

fn reverse_lowered_property_path(path: &LoweredPropertyPath) -> Option<LoweredPropertyPath> {
    match path {
        LoweredPropertyPath::SelfNode => Some(LoweredPropertyPath::SelfNode),
        LoweredPropertyPath::NamedNode(predicate) => {
            Some(LoweredPropertyPath::ReverseNamedNode(predicate.clone()))
        }
        LoweredPropertyPath::ReverseNamedNode(predicate) => {
            Some(LoweredPropertyPath::NamedNode(predicate.clone()))
        }
        LoweredPropertyPath::Sequence(items) => {
            let mut reversed = Vec::with_capacity(items.len());
            for item in items.iter().rev() {
                reversed.push(reverse_lowered_property_path(item)?);
            }
            Some(LoweredPropertyPath::Sequence(reversed))
        }
        _ => None,
    }
}

fn extract_type_subclass_constraint_from_lowered(
    path: &LoweredPropertyPath,
    object: &TermPattern,
) -> Option<Term> {
    if !matches!(
        path,
        LoweredPropertyPath::Sequence(items)
            if items.len() == 2
                && matches!(&items[0], LoweredPropertyPath::NamedNode(predicate)
                    if predicate.as_str() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                && matches!(&items[1], LoweredPropertyPath::ZeroOrMore(inner)
                    if matches!(inner.as_ref(), LoweredPropertyPath::NamedNode(predicate)
                        if predicate.as_str() == "http://www.w3.org/2000/01/rdf-schema#subClassOf"))
    ) {
        return None;
    }
    match object {
        TermPattern::NamedNode(node) => Some(Term::NamedNode(node.clone())),
        TermPattern::BlankNode(node) => Some(Term::BlankNode(node.clone())),
        _ => None,
    }
}

fn lowered_local_set_compatibility(pattern: &GraphPattern) -> Option<LocalSetCompatibilityPlan> {
    let (atoms, filters) = flatten_conjunctive_atoms(pattern)?;
    let mut all_filters = Vec::new();
    for filter in filters {
        split_conjunctive_filters(filter, &mut all_filters);
    }

    let distinct_pair = extract_distinct_anchor_pair(&all_filters);

    let mut anchor_patterns: Vec<(String, LoweredPropertyPath)> = Vec::new();
    let mut class_filters: HashMap<String, Term> = HashMap::new();
    let mut this_patterns: Vec<(String, LoweredPropertyPath)> = Vec::new();
    let mut value_paths: Vec<(String, String, LoweredPropertyPath)> = Vec::new();
    let mut composite_witness_vars: HashSet<String> = HashSet::new();

    for atom in &atoms {
        match atom {
            LoweredAtom::Triple(pattern) => {
                if term_pattern_is_this(&pattern.subject) {
                    let spargebra::term::NamedNodePattern::NamedNode(predicate) =
                        &pattern.predicate
                    else {
                        continue;
                    };
                    let Some(object_var) = term_pattern_var_name(&pattern.object) else {
                        continue;
                    };
                    this_patterns.push((
                        object_var.to_string(),
                        LoweredPropertyPath::NamedNode(predicate.clone()),
                    ));
                    continue;
                }
                if let Some(subject_var) = term_pattern_var_name(&pattern.subject) {
                    let spargebra::term::NamedNodePattern::NamedNode(predicate) =
                        &pattern.predicate
                    else {
                        continue;
                    };
                    if let Some(blank) = term_pattern_blank_node(&pattern.object)
                        && let Some((follow_path, final_object)) = blanknode_followup(blank, &atoms)
                    {
                        let combined_path = combine_lowered_paths(
                            LoweredPropertyPath::NamedNode(predicate.clone()),
                            follow_path,
                        );
                        if let Some(object_var) = term_pattern_var_name(final_object) {
                            value_paths.push((
                                subject_var.to_string(),
                                object_var.to_string(),
                                combined_path.clone(),
                            ));
                            if is_lowered_constituent_path(&combined_path) {
                                composite_witness_vars.insert(subject_var.to_string());
                            }
                            continue;
                        }
                        if let Some(class_term) = extract_type_subclass_constraint_from_lowered(
                            &combined_path,
                            final_object,
                        ) {
                            class_filters.insert(subject_var.to_string(), class_term);
                            continue;
                        }
                    }
                    if predicate.as_str() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" {
                        continue;
                    }
                    if let Some(object_var) = term_pattern_var_name(&pattern.object) {
                        value_paths.push((
                            subject_var.to_string(),
                            object_var.to_string(),
                            LoweredPropertyPath::NamedNode(predicate.clone()),
                        ));
                    }
                }
            }
            LoweredAtom::Path {
                subject,
                path,
                object,
            } => {
                if term_pattern_is_this(subject) {
                    let Some(object_var) = term_pattern_var_name(object) else {
                        continue;
                    };
                    this_patterns.push((object_var.to_string(), lower_property_path(path)?));
                    continue;
                }
                if let Some(subject_var) = term_pattern_var_name(subject) {
                    if let Some(object_var) = term_pattern_var_name(object) {
                        if let Some(path_lowered) = lower_property_path(path) {
                            value_paths.push((
                                subject_var.to_string(),
                                object_var.to_string(),
                                path_lowered.clone(),
                            ));
                            if matches!(
                                path,
                                PropertyPathExpression::Sequence(_, _)
                                    | PropertyPathExpression::NamedNode(_)
                            ) && is_composed_constituent_path(path)
                            {
                                composite_witness_vars.insert(subject_var.to_string());
                            }
                        }
                    } else if let Some(class_term) =
                        extract_type_subclass_constraint(subject, path, object)
                    {
                        class_filters.insert(subject_var.to_string(), class_term);
                    }
                }
            }
        }
    }

    let mut explicit_anchor_vars: HashSet<String> = class_filters.keys().cloned().collect();
    explicit_anchor_vars.extend(value_paths.iter().filter_map(|(anchor_var, _, path)| {
        if is_lowered_constituent_path(path) {
            None
        } else {
            Some(anchor_var.clone())
        }
    }));
    if let Some((left, right)) = distinct_pair.as_ref() {
        explicit_anchor_vars.insert(left.clone());
        explicit_anchor_vars.insert(right.clone());
    }
    for (object_var, path) in this_patterns {
        if explicit_anchor_vars.contains(&object_var) {
            anchor_patterns.push((object_var, path));
        } else {
            value_paths.push(("this".to_string(), object_var, path));
        }
    }

    let composed_of_predicate = extract_composed_of_predicate(&all_filters)?;
    let composite_anchor_var = composite_witness_vars
        .iter()
        .find_map(|witness_var| root_anchor_for_var(witness_var, &value_paths));

    let (left_anchor_var, right_anchor_var) = if let Some((left, right)) = distinct_pair.clone() {
        (left, right)
    } else {
        infer_anchor_pair(&value_paths, composite_anchor_var.as_deref())?
    };

    let compatibility_mode = extract_compatibility_mode(
        composite_anchor_var.as_deref(),
        &left_anchor_var,
        &right_anchor_var,
    )?;
    let constituent_path = match compatibility_mode {
        LocalSetCompatibilityMode::PurePure => None,
        _ => Some(extract_constituent_path(&atoms)?),
    };

    let left_anchor_path = if left_anchor_var == "this" {
        LoweredPropertyPath::SelfNode
    } else {
        anchor_patterns
            .iter()
            .find(|(var, _)| var == &left_anchor_var)
            .map(|(_, path)| path.clone())
            .unwrap_or(LoweredPropertyPath::SelfNode)
    };
    let right_anchor_path = if right_anchor_var == "this" {
        LoweredPropertyPath::SelfNode
    } else {
        anchor_patterns
            .iter()
            .find(|(var, _)| var == &right_anchor_var)
            .map(|(_, path)| path.clone())
            .unwrap_or(LoweredPropertyPath::SelfNode)
    };

    let (left_value_var, left_value_path) = value_paths
        .iter()
        .find(|(anchor_var, _, _)| anchor_var == &left_anchor_var)
        .map(|(_, value_var, path)| (value_var.clone(), path.clone()))?;
    let (right_value_var, right_value_path) = value_paths
        .iter()
        .find(|(anchor_var, _, _)| anchor_var == &right_anchor_var)
        .map(|(_, value_var, path)| (value_var.clone(), path.clone()))?;

    Some(LocalSetCompatibilityPlan {
        left_anchor_path,
        right_anchor_path,
        left_anchor_var: left_anchor_var.clone(),
        right_anchor_var: right_anchor_var.clone(),
        left_class: class_filters.remove(&left_anchor_var),
        right_class: class_filters.remove(&right_anchor_var),
        left_value_path,
        right_value_path,
        left_value_var,
        right_value_var,
        distinct_anchors: distinct_pair.is_some(),
        composed_of_predicate,
        constituent_path,
        mode: compatibility_mode,
    })
}

fn split_conjunctive_filters<'a>(expr: &'a Expression, output: &mut Vec<&'a Expression>) {
    match expr {
        Expression::And(left, right) => {
            split_conjunctive_filters(left, output);
            split_conjunctive_filters(right, output);
        }
        other => output.push(other),
    }
}

fn extract_missing_focus_path(expr: &Expression) -> Option<(LoweredPropertyPath, String)> {
    let Expression::Not(inner) = expr else {
        return None;
    };
    let Expression::Exists(pattern) = inner.as_ref() else {
        return None;
    };
    match unwrap_projection_like_pattern(pattern) {
        GraphPattern::Path {
            subject,
            path,
            object,
        } if term_pattern_is_this(subject) => Some((
            lower_property_path(path)?,
            term_pattern_var_name(object)?.to_string(),
        )),
        GraphPattern::Bgp { patterns } if patterns.len() == 1 => {
            let triple = &patterns[0];
            if !term_pattern_is_this(&triple.subject) {
                return None;
            }
            let spargebra::term::NamedNodePattern::NamedNode(predicate) = &triple.predicate else {
                return None;
            };
            Some((
                LoweredPropertyPath::NamedNode(predicate.clone()),
                term_pattern_var_name(&triple.object)?.to_string(),
            ))
        }
        _ => None,
    }
}

fn expression_mentions_var(expr: &Expression, var_name: &str) -> bool {
    match expr {
        Expression::Variable(variable) => variable.as_str() == var_name,
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            expression_mentions_var(inner, var_name)
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
            expression_mentions_var(left, var_name) || expression_mentions_var(right, var_name)
        }
        Expression::In(item, items) => {
            expression_mentions_var(item, var_name)
                || items
                    .iter()
                    .any(|item| expression_mentions_var(item, var_name))
        }
        Expression::If(left, middle, right) => {
            expression_mentions_var(left, var_name)
                || expression_mentions_var(middle, var_name)
                || expression_mentions_var(right, var_name)
        }
        Expression::Coalesce(items) | Expression::FunctionCall(_, items) => items
            .iter()
            .any(|item| expression_mentions_var(item, var_name)),
        Expression::Exists(pattern) => graph_pattern_mentions_var(pattern, var_name),
        _ => false,
    }
}

fn graph_pattern_mentions_var(pattern: &GraphPattern, var_name: &str) -> bool {
    match unwrap_projection_like_pattern(pattern) {
        GraphPattern::Bgp { patterns } => patterns.iter().any(|pattern| {
            term_pattern_matches_var(&pattern.subject, var_name)
                || matches!(
                    &pattern.predicate,
                    spargebra::term::NamedNodePattern::Variable(variable)
                        if variable.as_str() == var_name
                )
                || term_pattern_matches_var(&pattern.object, var_name)
        }),
        GraphPattern::Path {
            subject, object, ..
        } => {
            term_pattern_matches_var(subject, var_name)
                || term_pattern_matches_var(object, var_name)
        }
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Lateral { left, right } => {
            graph_pattern_mentions_var(left, var_name)
                || graph_pattern_mentions_var(right, var_name)
        }
        GraphPattern::Filter { expr, inner } => {
            expression_mentions_var(expr, var_name) || graph_pattern_mentions_var(inner, var_name)
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            graph_pattern_mentions_var(left, var_name)
                || graph_pattern_mentions_var(right, var_name)
                || expression
                    .as_ref()
                    .is_some_and(|expr| expression_mentions_var(expr, var_name))
        }
        GraphPattern::Graph { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Service { inner, .. }
        | GraphPattern::Minus { left: inner, .. } => graph_pattern_mentions_var(inner, var_name),
        GraphPattern::Values { variables, .. } => {
            variables.iter().any(|var| var.as_str() == var_name)
        }
    }
}

fn var_is_only_filter_local(var_name: &str, filters: &[&Expression]) -> bool {
    filters
        .iter()
        .all(|expr| expression_mentions_var(expr, var_name))
}

fn flatten_conjunctive_atoms<'a>(
    pattern: &'a GraphPattern,
) -> Option<(Vec<LoweredAtom<'a>>, Vec<&'a Expression>)> {
    let mut atoms = Vec::new();
    let mut filters = Vec::new();
    flatten_conjunctive_atoms_inner(
        unwrap_projection_like_pattern(pattern),
        &mut atoms,
        &mut filters,
    )?;
    Some((atoms, filters))
}

fn flatten_conjunctive_atoms_inner<'a>(
    pattern: &'a GraphPattern,
    atoms: &mut Vec<LoweredAtom<'a>>,
    filters: &mut Vec<&'a Expression>,
) -> Option<()> {
    match unwrap_projection_like_pattern(pattern) {
        GraphPattern::Bgp { patterns } => {
            atoms.extend(patterns.iter().map(LoweredAtom::Triple));
            Some(())
        }
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            atoms.push(LoweredAtom::Path {
                subject,
                path,
                object,
            });
            Some(())
        }
        GraphPattern::Join { left, right } => {
            flatten_conjunctive_atoms_inner(left, atoms, filters)?;
            flatten_conjunctive_atoms_inner(right, atoms, filters)
        }
        GraphPattern::Filter { expr, inner } => {
            filters.push(expr);
            flatten_conjunctive_atoms_inner(inner, atoms, filters)
        }
        _ => None,
    }
}

fn extract_type_subclass_constraint(
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
) -> Option<Term> {
    if !matches!(
        path,
        PropertyPathExpression::Sequence(left, right)
            if matches!(left.as_ref(), PropertyPathExpression::NamedNode(predicate)
                if predicate.as_str() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                && matches!(right.as_ref(), PropertyPathExpression::ZeroOrMore(inner)
                    if matches!(inner.as_ref(), PropertyPathExpression::NamedNode(predicate)
                        if predicate.as_str() == "http://www.w3.org/2000/01/rdf-schema#subClassOf"))
    ) {
        return None;
    }
    let _ = term_pattern_var_name(subject)?;
    match object {
        TermPattern::NamedNode(node) => Some(Term::NamedNode(node.clone())),
        TermPattern::BlankNode(node) => Some(Term::BlankNode(node.clone())),
        _ => None,
    }
}

fn is_composed_constituent_path(path: &PropertyPathExpression) -> bool {
    matches!(
        path,
        PropertyPathExpression::Sequence(left, right)
            if matches!(left.as_ref(), PropertyPathExpression::NamedNode(predicate)
                if predicate.as_str().ends_with("composedOf"))
                && matches!(right.as_ref(), PropertyPathExpression::NamedNode(predicate)
                    if predicate.as_str().ends_with("ofConstituent"))
    )
}

fn extract_constituent_path(atoms: &[LoweredAtom<'_>]) -> Option<LoweredPropertyPath> {
    atoms.iter().find_map(|atom| match atom {
        LoweredAtom::Path { path, .. } if is_composed_constituent_path(path) => {
            lower_property_path(path)
        }
        LoweredAtom::Triple(pattern) => {
            let spargebra::term::NamedNodePattern::NamedNode(predicate) = &pattern.predicate else {
                return None;
            };
            let blank = term_pattern_blank_node(&pattern.object)?;
            let (follow_path, _) = blanknode_followup(blank, atoms)?;
            let combined = combine_lowered_paths(
                LoweredPropertyPath::NamedNode(predicate.clone()),
                follow_path,
            );
            if is_lowered_constituent_path(&combined) {
                Some(combined)
            } else {
                None
            }
        }
        _ => None,
    })
}

fn extract_composed_of_predicate(filters: &[&Expression]) -> Option<NamedNode> {
    filters
        .iter()
        .find_map(|filter| extract_composed_of_predicate_from_expr(filter))
}

fn extract_distinct_anchor_pair(filters: &[&Expression]) -> Option<(String, String)> {
    filters.iter().find_map(|expr| match expr {
        Expression::Not(inner) => extract_distinct_anchor_pair(&[inner.as_ref()]),
        Expression::And(left, right) | Expression::Or(left, right) => {
            extract_distinct_anchor_pair(&[left.as_ref(), right.as_ref()])
        }
        Expression::SameTerm(left, right) | Expression::Equal(left, right) => {
            let Expression::Variable(left_var) = left.as_ref() else {
                return None;
            };
            let Expression::Variable(right_var) = right.as_ref() else {
                return None;
            };
            Some((
                left_var.as_str().to_string(),
                right_var.as_str().to_string(),
            ))
        }
        _ => None,
    })
}

fn extract_composed_of_predicate_from_expr(expr: &Expression) -> Option<NamedNode> {
    match expr {
        Expression::And(left, right) | Expression::Or(left, right) => {
            extract_composed_of_predicate_from_expr(left)
                .or_else(|| extract_composed_of_predicate_from_expr(right))
        }
        Expression::Not(inner) => extract_composed_of_predicate_from_expr(inner),
        Expression::Exists(pattern) => match unwrap_projection_like_pattern(pattern) {
            GraphPattern::Bgp { patterns } => patterns.first().and_then(|first| {
                let spargebra::term::NamedNodePattern::NamedNode(predicate) = &first.predicate
                else {
                    return None;
                };
                predicate
                    .as_str()
                    .ends_with("composedOf")
                    .then(|| predicate.clone())
            }),
            GraphPattern::Join { left, .. } => {
                let GraphPattern::Bgp { patterns } = unwrap_projection_like_pattern(left) else {
                    return None;
                };
                patterns.first().and_then(|first| {
                    let spargebra::term::NamedNodePattern::NamedNode(predicate) = &first.predicate
                    else {
                        return None;
                    };
                    predicate
                        .as_str()
                        .ends_with("composedOf")
                        .then(|| predicate.clone())
                })
            }
            _ => None,
        },
        _ => None,
    }
}

fn extract_compatibility_mode(
    composite_anchor_var: Option<&str>,
    left_anchor_var: &str,
    right_anchor_var: &str,
) -> Option<LocalSetCompatibilityMode> {
    match composite_anchor_var {
        None => Some(LocalSetCompatibilityMode::PurePure),
        Some(_) if left_anchor_var == right_anchor_var => {
            Some(LocalSetCompatibilityMode::CompositeVsComposite)
        }
        Some(var) => {
            let side = if var == left_anchor_var {
                CompatibilitySide::Left
            } else if var == right_anchor_var {
                CompatibilitySide::Right
            } else {
                return None;
            };
            Some(LocalSetCompatibilityMode::CompositeVsPure {
                composite_side: side,
            })
        }
    }
}

fn infer_anchor_pair(
    value_paths: &[(String, String, LoweredPropertyPath)],
    composite_anchor_var: Option<&str>,
) -> Option<(String, String)> {
    match composite_anchor_var {
        None => {
            let anchors = unique_root_anchor_vars(value_paths);
            let left = anchors.first()?.clone();
            let right = anchors.get(1).cloned().unwrap_or_else(|| left.clone());
            Some((left, right))
        }
        Some(composite_anchor) => {
            let anchors = unique_root_anchor_vars(value_paths);
            let other = anchors
                .iter()
                .find(|anchor| anchor.as_str() != composite_anchor)
                .cloned()
                .unwrap_or_else(|| composite_anchor.to_string());
            Some((composite_anchor.to_string(), other))
        }
    }
}

fn root_anchor_for_var(
    var: &str,
    value_paths: &[(String, String, LoweredPropertyPath)],
) -> Option<String> {
    if var == "this" {
        return Some("this".to_string());
    }

    let mut current = var;
    let mut seen = HashSet::new();
    loop {
        if !seen.insert(current.to_string()) {
            return None;
        }
        let Some((anchor_var, _, _)) = value_paths
            .iter()
            .find(|(_, value_var, _)| value_var == current)
        else {
            return Some(current.to_string());
        };
        if anchor_var == "this" {
            return Some("this".to_string());
        }
        current = anchor_var;
    }
}

fn is_lowered_constituent_path(path: &LoweredPropertyPath) -> bool {
    matches!(
        path,
        LoweredPropertyPath::Sequence(items)
            if items.len() == 2
                && matches!(&items[0], LoweredPropertyPath::NamedNode(predicate)
                    if predicate.as_str().ends_with("composedOf"))
                && matches!(&items[1], LoweredPropertyPath::NamedNode(predicate)
                    if predicate.as_str().ends_with("ofConstituent"))
    )
}

fn unique_root_anchor_vars(value_paths: &[(String, String, LoweredPropertyPath)]) -> Vec<String> {
    let mut anchors = Vec::new();
    for (anchor_var, _, _) in value_paths {
        let Some(root) = root_anchor_for_var(anchor_var, value_paths) else {
            continue;
        };
        if !anchors.contains(&root) {
            anchors.push(root);
        }
    }
    anchors
}

fn lowered_adjacent_whitelist(
    anchor_var: &str,
    anchor_path: &PropertyPathExpression,
    expr: &Expression,
) -> Option<AdjacentPredicateWhitelistPlan> {
    Some(AdjacentPredicateWhitelistPlan {
        anchor_path: lower_property_path(anchor_path)?,
        allowed_predicates: match_adjacent_whitelist_expr(expr, anchor_var)?,
    })
}

fn match_adjacent_whitelist_expr(expr: &Expression, anchor_var: &str) -> Option<Vec<NamedNode>> {
    let Expression::Not(inner) = expr else {
        return None;
    };
    let Expression::Exists(pattern) = inner.as_ref() else {
        return None;
    };
    match_adjacent_whitelist_exists(pattern, anchor_var)
}

fn match_adjacent_whitelist_exists(
    pattern: &GraphPattern,
    anchor_var: &str,
) -> Option<Vec<NamedNode>> {
    let GraphPattern::Filter { expr, inner } = unwrap_projection_like_pattern(pattern) else {
        return None;
    };
    let predicate_var = match_not_in_namednode_filter(expr)?;
    let union = unwrap_projection_like_pattern(inner.as_ref());
    let GraphPattern::Union { left, right } = union else {
        return None;
    };
    let outgoing_ok = branch_matches_adjacent_union(left, anchor_var, &predicate_var, true);
    let incoming_ok = branch_matches_adjacent_union(right, anchor_var, &predicate_var, false);
    let swapped_outgoing = branch_matches_adjacent_union(right, anchor_var, &predicate_var, true);
    let swapped_incoming = branch_matches_adjacent_union(left, anchor_var, &predicate_var, false);
    if !(outgoing_ok && incoming_ok || swapped_outgoing && swapped_incoming) {
        return None;
    }
    named_nodes_from_not_in_filter(expr)
}

fn branch_matches_adjacent_union(
    pattern: &GraphPattern,
    anchor_var: &str,
    predicate_var: &str,
    outgoing: bool,
) -> bool {
    let GraphPattern::Bgp { patterns } = unwrap_projection_like_pattern(pattern) else {
        return false;
    };
    if patterns.len() != 1 {
        return false;
    }
    let triple = &patterns[0];
    let spargebra::term::NamedNodePattern::Variable(var) = &triple.predicate else {
        return false;
    };
    if var.as_str() != predicate_var {
        return false;
    }
    if outgoing {
        term_pattern_matches_var(&triple.subject, anchor_var)
            && matches!(triple.object, TermPattern::Variable(_))
    } else {
        term_pattern_matches_var(&triple.object, anchor_var)
            && matches!(triple.subject, TermPattern::Variable(_))
    }
}

fn match_not_in_namednode_filter(expr: &Expression) -> Option<String> {
    let Expression::Not(inner) = expr else {
        return None;
    };
    let Expression::In(item, items) = inner.as_ref() else {
        return None;
    };
    let Expression::Variable(variable) = item.as_ref() else {
        return None;
    };
    if items
        .iter()
        .all(|expr| matches!(expr, Expression::NamedNode(_)))
    {
        Some(variable.as_str().to_string())
    } else {
        None
    }
}

fn named_nodes_from_not_in_filter(expr: &Expression) -> Option<Vec<NamedNode>> {
    let Expression::Not(inner) = expr else {
        return None;
    };
    let Expression::In(_, items) = inner.as_ref() else {
        return None;
    };
    let mut predicates = Vec::with_capacity(items.len());
    for item in items {
        let Expression::NamedNode(node) = item else {
            return None;
        };
        predicates.push(node.clone());
    }
    predicates.sort();
    predicates.dedup();
    Some(predicates)
}

fn term_pattern_matches_var(term: &TermPattern, var_name: &str) -> bool {
    matches!(term, TermPattern::Variable(variable) if variable.as_str() == var_name)
}

fn term_pattern_var_name(term: &TermPattern) -> Option<&str> {
    match term {
        TermPattern::Variable(variable) => Some(variable.as_str()),
        _ => None,
    }
}

fn compiled_path_copy_rule(query: &AlgebraQuery) -> Option<CompiledSparqlRule> {
    let AlgebraQuery::Construct {
        template, pattern, ..
    } = query
    else {
        return None;
    };
    let [triple] = template.as_slice() else {
        return None;
    };
    if !term_pattern_is_this(&triple.subject) {
        return None;
    }
    let construct_predicate = named_node_pattern_named_node(&triple.predicate)?;
    let target_var = term_pattern_var_name(&triple.object)?;
    let source_path = compiled_path_from_graph_pattern(pattern, target_var)?;
    Some(CompiledSparqlRule::PathCopy {
        construct_predicate,
        source_path,
    })
}

fn collect_compiled_path_index_requirements(
    path: &LoweredPropertyPath,
    requirements: &mut Vec<CompiledIndexRequirement>,
) {
    match path {
        LoweredPropertyPath::SelfNode => {}
        LoweredPropertyPath::NamedNode(predicate) => {
            requirements.push(CompiledIndexRequirement::OutgoingValues {
                predicate: predicate.clone(),
            });
        }
        LoweredPropertyPath::ReverseNamedNode(predicate) => {
            requirements.push(CompiledIndexRequirement::IncomingValues {
                predicate: predicate.clone(),
            });
        }
        LoweredPropertyPath::ZeroOrOne(inner)
        | LoweredPropertyPath::ZeroOrMore(inner)
        | LoweredPropertyPath::OneOrMore(inner) => {
            collect_compiled_path_index_requirements(inner, requirements);
        }
        LoweredPropertyPath::Sequence(items) | LoweredPropertyPath::Alternative(items) => {
            for item in items {
                collect_compiled_path_index_requirements(item, requirements);
            }
        }
    }
}

fn compiled_index_requirement_sort_key(requirement: &CompiledIndexRequirement) -> (&str, &str) {
    match requirement {
        CompiledIndexRequirement::OutgoingValues { predicate } => ("outgoing", predicate.as_str()),
        CompiledIndexRequirement::IncomingValues { predicate } => ("incoming", predicate.as_str()),
    }
}

fn compiled_equality_constant_rule(query: &AlgebraQuery) -> Option<CompiledSparqlRule> {
    let AlgebraQuery::Construct {
        template, pattern, ..
    } = query
    else {
        return None;
    };
    let [triple] = template.as_slice() else {
        return None;
    };
    if !term_pattern_is_this(&triple.subject) {
        return None;
    }
    let construct_predicate = named_node_pattern_named_node(&triple.predicate)?;
    let object = term_pattern_constant(&triple.object)?;
    let GraphPattern::Filter { expr, inner } = unwrap_projection_like_pattern(pattern) else {
        return None;
    };
    let Expression::Equal(left, right) = expr else {
        return None;
    };
    let left_var = expression_variable_name(left.as_ref())?;
    let right_var = expression_variable_name(right.as_ref())?;
    let GraphPattern::Bgp { patterns } = unwrap_projection_like_pattern(inner.as_ref()) else {
        return None;
    };
    let [left_pattern, right_pattern] = patterns.as_slice() else {
        return None;
    };
    if !term_pattern_is_this(&left_pattern.subject) || !term_pattern_is_this(&right_pattern.subject)
    {
        return None;
    }
    let first_var = term_pattern_var_name(&left_pattern.object)?;
    let second_var = term_pattern_var_name(&right_pattern.object)?;
    let filter_matches = (first_var == left_var && second_var == right_var)
        || (first_var == right_var && second_var == left_var);
    if !filter_matches {
        return None;
    }
    Some(CompiledSparqlRule::EqualityConstant {
        construct_predicate,
        left_path: compiled_single_triple_path(left_pattern)?,
        right_path: compiled_single_triple_path(right_pattern)?,
        object,
    })
}

fn compiled_path_from_graph_pattern(
    pattern: &GraphPattern,
    target_var: &str,
) -> Option<LoweredPropertyPath> {
    match unwrap_projection_like_pattern(pattern) {
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            if !term_pattern_is_this(subject) || term_pattern_var_name(object)? != target_var {
                return None;
            }
            lower_property_path(path)
        }
        other => {
            let mut patterns = Vec::new();
            collect_named_triple_patterns(other, &mut patterns)?;
            compiled_path_from_named_triples(&patterns, target_var)
        }
    }
}

fn compiled_single_triple_path(triple: &TriplePattern) -> Option<LoweredPropertyPath> {
    if !term_pattern_is_this(&triple.subject) {
        return None;
    }
    Some(LoweredPropertyPath::NamedNode(
        named_node_pattern_named_node(&triple.predicate)?,
    ))
}

fn collect_named_triple_patterns<'a>(
    pattern: &'a GraphPattern,
    patterns: &mut Vec<&'a TriplePattern>,
) -> Option<()> {
    match unwrap_projection_like_pattern(pattern) {
        GraphPattern::Bgp {
            patterns: bgp_patterns,
        } => {
            for triple in bgp_patterns {
                named_node_pattern_named_node(&triple.predicate)?;
                patterns.push(triple);
            }
            Some(())
        }
        GraphPattern::Join { left, right } => {
            collect_named_triple_patterns(left.as_ref(), patterns)?;
            collect_named_triple_patterns(right.as_ref(), patterns)
        }
        _ => None,
    }
}

fn compiled_path_from_named_triples(
    patterns: &[&TriplePattern],
    target_var: &str,
) -> Option<LoweredPropertyPath> {
    let mut used = vec![false; patterns.len()];
    let mut segments = Vec::new();
    let mut current = TermPattern::Variable(spargebra::term::Variable::new_unchecked("this"));

    loop {
        if term_pattern_var_name(&current) == Some(target_var) {
            break;
        }

        let mut next_match: Option<(usize, LoweredPropertyPath, TermPattern)> = None;
        for (index, triple) in patterns.iter().enumerate() {
            if used[index] {
                continue;
            }
            let predicate = named_node_pattern_named_node(&triple.predicate)?;
            let candidate = if triple.subject == current {
                Some((
                    index,
                    LoweredPropertyPath::NamedNode(predicate),
                    triple.object.clone(),
                ))
            } else if triple.object == current {
                Some((
                    index,
                    LoweredPropertyPath::ReverseNamedNode(predicate),
                    triple.subject.clone(),
                ))
            } else {
                None
            };
            if let Some(candidate) = candidate {
                if next_match.is_some() {
                    return None;
                }
                next_match = Some(candidate);
            }
        }

        let (index, segment, next) = next_match?;
        used[index] = true;
        segments.push(segment);
        current = next;
    }

    if used.iter().any(|used| !used) || segments.is_empty() {
        return None;
    }

    Some(if segments.len() == 1 {
        segments.pop().expect("single segment")
    } else {
        LoweredPropertyPath::Sequence(segments)
    })
}

fn named_node_pattern_named_node(pattern: &NamedNodePattern) -> Option<NamedNode> {
    match pattern {
        NamedNodePattern::NamedNode(node) => Some(node.clone()),
        NamedNodePattern::Variable(_) => None,
    }
}

fn term_pattern_constant(term: &TermPattern) -> Option<Term> {
    match term {
        TermPattern::NamedNode(node) => Some(Term::NamedNode(node.clone())),
        TermPattern::Literal(literal) => Some(Term::Literal(literal.clone())),
        _ => None,
    }
}

fn expression_variable_name(expression: &Expression) -> Option<&str> {
    match expression {
        Expression::Variable(variable) => Some(variable.as_str()),
        _ => None,
    }
}

fn unwrap_projection_like_pattern(mut pattern: &GraphPattern) -> &GraphPattern {
    loop {
        pattern = match pattern {
            GraphPattern::Project { inner, .. }
            | GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner }
            | GraphPattern::Slice { inner, .. }
            | GraphPattern::OrderBy { inner, .. }
            | GraphPattern::Group { inner, .. }
            | GraphPattern::Extend { inner, .. } => inner.as_ref(),
            other => return other,
        };
    }
}

fn target_select_projection_var(mut pattern: &GraphPattern) -> Option<&str> {
    let mut selected = None;
    loop {
        pattern = match pattern {
            GraphPattern::Project { inner, variables } => {
                if selected.is_none() {
                    selected = preferred_target_projection_var(variables);
                }
                inner.as_ref()
            }
            GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner }
            | GraphPattern::Slice { inner, .. }
            | GraphPattern::OrderBy { inner, .. }
            | GraphPattern::Group { inner, .. }
            | GraphPattern::Extend { inner, .. } => inner.as_ref(),
            _ => return selected,
        };
    }
}

fn preferred_target_projection_var(variables: &[Variable]) -> Option<&str> {
    variables
        .iter()
        .find(|variable| variable.as_str() == "this")
        .or_else(|| {
            variables
                .iter()
                .find(|variable| variable.as_str() == "target")
        })
        .or_else(|| variables.first())
        .map(Variable::as_str)
}

fn lower_property_path(path: &PropertyPathExpression) -> Option<LoweredPropertyPath> {
    match path {
        PropertyPathExpression::NamedNode(predicate) => {
            Some(LoweredPropertyPath::NamedNode(predicate.clone()))
        }
        PropertyPathExpression::Reverse(inner) => match inner.as_ref() {
            PropertyPathExpression::NamedNode(predicate) => {
                Some(LoweredPropertyPath::ReverseNamedNode(predicate.clone()))
            }
            _ => None,
        },
        PropertyPathExpression::ZeroOrOne(inner) => Some(LoweredPropertyPath::ZeroOrOne(Box::new(
            lower_property_path(inner.as_ref())?,
        ))),
        PropertyPathExpression::ZeroOrMore(inner) => Some(LoweredPropertyPath::ZeroOrMore(
            Box::new(lower_property_path(inner.as_ref())?),
        )),
        PropertyPathExpression::OneOrMore(inner) => Some(LoweredPropertyPath::OneOrMore(Box::new(
            lower_property_path(inner.as_ref())?,
        ))),
        PropertyPathExpression::Sequence(left, right) => {
            let mut segments = Vec::new();
            flatten_sequence(left.as_ref(), &mut segments)?;
            flatten_sequence(right.as_ref(), &mut segments)?;
            Some(LoweredPropertyPath::Sequence(segments))
        }
        PropertyPathExpression::Alternative(left, right) => {
            let mut alternatives = Vec::new();
            flatten_alternative(left.as_ref(), &mut alternatives)?;
            flatten_alternative(right.as_ref(), &mut alternatives)?;
            Some(LoweredPropertyPath::Alternative(alternatives))
        }
        _ => None,
    }
}

fn flatten_sequence(
    path: &PropertyPathExpression,
    segments: &mut Vec<LoweredPropertyPath>,
) -> Option<()> {
    match path {
        PropertyPathExpression::Sequence(left, right) => {
            flatten_sequence(left.as_ref(), segments)?;
            flatten_sequence(right.as_ref(), segments)
        }
        other => {
            segments.push(lower_property_path(other)?);
            Some(())
        }
    }
}

fn flatten_alternative(
    path: &PropertyPathExpression,
    alternatives: &mut Vec<LoweredPropertyPath>,
) -> Option<()> {
    match path {
        PropertyPathExpression::Alternative(left, right) => {
            flatten_alternative(left.as_ref(), alternatives)?;
            flatten_alternative(right.as_ref(), alternatives)
        }
        other => {
            alternatives.push(lower_property_path(other)?);
            Some(())
        }
    }
}

fn compiled_target_select_query_in_graph_pattern(
    pattern: &GraphPattern,
    target_var: &str,
) -> Option<CompiledTargetSelectQuery> {
    match unwrap_projection_like_pattern(pattern) {
        GraphPattern::Bgp { patterns } if patterns.len() == 1 => {
            compiled_target_select_query_in_triple_pattern(&patterns[0], target_var)
        }
        GraphPattern::Path {
            subject,
            path,
            object,
        } => compiled_target_select_query_in_path_pattern(subject, path, object, target_var),
        _ => None,
    }
}

fn compiled_target_select_query_in_triple_pattern(
    pattern: &TriplePattern,
    target_var: &str,
) -> Option<CompiledTargetSelectQuery> {
    let NamedNodePattern::NamedNode(predicate) = &pattern.predicate else {
        return None;
    };

    if term_pattern_matches_var(&pattern.subject, target_var) {
        return match (&pattern.object, predicate.as_str()) {
            (TermPattern::NamedNode(class), "http://www.w3.org/1999/02/22-rdf-syntax-ns#type") => {
                Some(CompiledTargetSelectQuery::ClassInstances {
                    class: class.clone(),
                })
            }
            (TermPattern::Variable(_), _) => Some(CompiledTargetSelectQuery::SubjectsOf {
                predicate: predicate.clone(),
            }),
            _ => None,
        };
    }

    if term_pattern_matches_var(&pattern.object, target_var)
        && term_pattern_var_name(&pattern.subject).is_some()
    {
        return Some(CompiledTargetSelectQuery::ObjectsOf {
            predicate: predicate.clone(),
        });
    }

    None
}

fn compiled_target_select_query_in_path_pattern(
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
    target_var: &str,
) -> Option<CompiledTargetSelectQuery> {
    if term_pattern_matches_var(subject, target_var) && term_pattern_var_name(object).is_some() {
        return match path {
            PropertyPathExpression::NamedNode(predicate) => {
                Some(CompiledTargetSelectQuery::SubjectsOf {
                    predicate: predicate.clone(),
                })
            }
            PropertyPathExpression::Reverse(inner) => match inner.as_ref() {
                PropertyPathExpression::NamedNode(predicate) => {
                    Some(CompiledTargetSelectQuery::ObjectsOf {
                        predicate: predicate.clone(),
                    })
                }
                _ => None,
            },
            _ => None,
        };
    }

    if term_pattern_matches_var(object, target_var) && term_pattern_var_name(subject).is_some() {
        return match path {
            PropertyPathExpression::NamedNode(predicate) => {
                Some(CompiledTargetSelectQuery::ObjectsOf {
                    predicate: predicate.clone(),
                })
            }
            PropertyPathExpression::Reverse(inner) => match inner.as_ref() {
                PropertyPathExpression::NamedNode(predicate) => {
                    Some(CompiledTargetSelectQuery::SubjectsOf {
                        predicate: predicate.clone(),
                    })
                }
                _ => None,
            },
            _ => None,
        };
    }

    None
}

fn required_this_predicates_in_graph_pattern(
    pattern: &GraphPattern,
) -> HashSet<ThisPredicateRequirement> {
    match pattern {
        GraphPattern::Bgp { patterns } => patterns
            .iter()
            .filter_map(required_this_predicate_in_triple_pattern)
            .collect(),
        GraphPattern::Path {
            subject,
            path,
            object,
        } => required_this_predicate_in_path_pattern(subject, path, object)
            .into_iter()
            .collect(),
        GraphPattern::Join { left, right } => {
            let mut requirements = required_this_predicates_in_graph_pattern(left);
            requirements.extend(required_this_predicates_in_graph_pattern(right));
            requirements
        }
        GraphPattern::Union { left, right } => {
            let left_requirements = required_this_predicates_in_graph_pattern(left);
            let right_requirements = required_this_predicates_in_graph_pattern(right);
            left_requirements
                .intersection(&right_requirements)
                .cloned()
                .collect()
        }
        GraphPattern::LeftJoin { left, .. } | GraphPattern::Minus { left, .. } => {
            required_this_predicates_in_graph_pattern(left)
        }
        GraphPattern::Graph { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Service { inner, .. } => required_this_predicates_in_graph_pattern(inner),
        GraphPattern::Lateral { left, right } => {
            let mut requirements = required_this_predicates_in_graph_pattern(left);
            requirements.extend(required_this_predicates_in_graph_pattern(right));
            requirements
        }
        GraphPattern::Values { .. } => HashSet::new(),
    }
}

fn required_this_predicate_in_triple_pattern(
    pattern: &spargebra::term::TriplePattern,
) -> Option<ThisPredicateRequirement> {
    let predicate = match &pattern.predicate {
        spargebra::term::NamedNodePattern::NamedNode(predicate) => predicate.clone(),
        spargebra::term::NamedNodePattern::Variable(_) => return None,
    };

    if term_pattern_is_this(&pattern.subject) {
        return Some(ThisPredicateRequirement {
            predicate,
            direction: ThisPredicateDirection::Outgoing,
        });
    }

    if term_pattern_is_this(&pattern.object) {
        return Some(ThisPredicateRequirement {
            predicate,
            direction: ThisPredicateDirection::Incoming,
        });
    }

    None
}

fn required_this_predicate_in_path_pattern(
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
) -> Option<ThisPredicateRequirement> {
    match (
        term_pattern_is_this(subject),
        path,
        term_pattern_is_this(object),
    ) {
        (true, PropertyPathExpression::NamedNode(predicate), _) => Some(ThisPredicateRequirement {
            predicate: predicate.clone(),
            direction: ThisPredicateDirection::Outgoing,
        }),
        (true, PropertyPathExpression::Reverse(inner), _) => match inner.as_ref() {
            PropertyPathExpression::NamedNode(predicate) => Some(ThisPredicateRequirement {
                predicate: predicate.clone(),
                direction: ThisPredicateDirection::Incoming,
            }),
            _ => None,
        },
        (_, PropertyPathExpression::NamedNode(predicate), true) => Some(ThisPredicateRequirement {
            predicate: predicate.clone(),
            direction: ThisPredicateDirection::Incoming,
        }),
        (_, PropertyPathExpression::Reverse(inner), true) => match inner.as_ref() {
            PropertyPathExpression::NamedNode(predicate) => Some(ThisPredicateRequirement {
                predicate: predicate.clone(),
                direction: ThisPredicateDirection::Outgoing,
            }),
            _ => None,
        },
        _ => None,
    }
}

fn term_pattern_is_this(term: &TermPattern) -> bool {
    matches!(term, TermPattern::Variable(variable) if variable.as_str() == "this")
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

pub(crate) fn parse_custom_constraint_components<E: SparqlExecutor>(
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
            if let Ok(solution) = solution_res
                && let Some(Term::NamedNode(cc_iri)) = solution.get("cc")
            {
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
                        .find_map(|term| <Severity as crate::types::SeverityExt>::from_term(&term));

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
                                    ));
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

                    let property_validator_prop =
                        NamedNodeRef::new_unchecked("http://www.w3.org/ns/shacl#propertyValidator");
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

    Ok((definitions, param_to_component))
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

pub(crate) fn parse_prefix_lines(prefixes: &str) -> Vec<(String, String)> {
    prefixes
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if !trimmed.starts_with("PREFIX ") {
                return None;
            }
            let rest = &trimmed["PREFIX ".len()..];
            let (prefix, namespace) = rest.split_once(':')?;
            let namespace = namespace.trim();
            let namespace = namespace.strip_prefix('<')?.strip_suffix('>')?;
            Some((prefix.trim().to_string(), namespace.to_string()))
        })
        .collect()
}

fn namespace_aliases(prefixes: &[(String, String)]) -> Vec<(String, String)> {
    const STANDARD_PREFIXES: &[&str] = &["rdf", "rdfs", "xsd", "owl", "sh"];

    let mut namespaces: Vec<String> = prefixes
        .iter()
        .filter(|(prefix, _)| !STANDARD_PREFIXES.contains(&prefix.as_str()))
        .map(|(_, namespace)| namespace.clone())
        .collect();
    namespaces.dedup();
    namespaces.sort();
    namespaces
        .into_iter()
        .enumerate()
        .map(|(idx, namespace)| (format!("ns{}", idx + 1), namespace))
        .collect()
}

pub(crate) fn format_term_with_namespace_aliases(
    term: &Term,
    prefixes: &[(String, String)],
) -> String {
    match term {
        Term::NamedNode(nn) => {
            let iri = nn.as_str();
            let mut best_match: Option<(String, String)> = None;
            for (prefix, namespace) in namespace_aliases(prefixes) {
                if iri.starts_with(&namespace) {
                    match best_match {
                        Some((_, ref best_ns)) if best_ns.len() >= namespace.len() => {}
                        _ => best_match = Some((prefix, namespace)),
                    }
                }
            }
            if let Some((prefix, namespace)) = best_match {
                let local = &iri[namespace.len()..];
                if !local.is_empty() {
                    return format!("{}:{}", prefix, local);
                }
            }
            format!("<{}>", iri)
        }
        Term::Literal(lit) => lit.value().to_string(),
        _ => format_term_for_label(term),
    }
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
    use spargebra::SparqlParser;

    fn parse_query(query: &str) -> AlgebraQuery {
        SparqlParser::new().parse_query(query).unwrap()
    }

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

    #[test]
    fn message_instantiation_handles_dollar_placeholders() {
        let templates = vec![Term::Literal(Literal::from("Value {$this}"))];
        let (first, instantiated) =
            instantiate_message_terms(&templates, &[("this".into(), "ns1:Foo".into())]);
        assert_eq!(first.as_deref(), Some("Value ns1:Foo"));
        assert_eq!(
            instantiated[0],
            Term::Literal(Literal::from("Value ns1:Foo"))
        );
    }

    #[test]
    fn format_term_with_namespace_aliases_uses_ns_aliases() {
        let term = Term::NamedNode(NamedNode::new_unchecked("http://example.com/a#Value"));
        let prefixes = vec![
            (
                "rdf".to_string(),
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#".to_string(),
            ),
            ("a".to_string(), "http://example.com/a#".to_string()),
            ("b".to_string(), "http://example.com/b#".to_string()),
        ];
        assert_eq!(
            format_term_with_namespace_aliases(&term, &prefixes),
            "ns1:Value"
        );
    }

    #[test]
    fn format_term_with_namespace_aliases_uses_sorted_namespaces() {
        let term = Term::NamedNode(NamedNode::new_unchecked("http://example.com/c#Unit"));
        let prefixes = vec![
            ("b".to_string(), "http://example.com/b#".to_string()),
            ("c".to_string(), "http://example.com/c#".to_string()),
            ("a".to_string(), "http://example.com/a#".to_string()),
        ];
        assert_eq!(
            format_term_with_namespace_aliases(&term, &prefixes),
            "ns3:Unit"
        );
    }

    #[test]
    fn format_term_with_namespace_aliases_falls_back_to_iri() {
        let term = Term::NamedNode(NamedNode::new_unchecked("urn:nrel_example/nrel00000000"));
        assert_eq!(
            format_term_with_namespace_aliases(&term, &[]),
            "<urn:nrel_example/nrel00000000>"
        );
    }

    #[test]
    fn required_this_predicates_collects_direct_requirements() {
        let query = parse_query(
            "SELECT ?this WHERE { ?this <http://example.com/p> ?value . ?subject <http://example.com/q> ?this . }",
        );

        let requirements = required_this_predicates(&query);
        assert!(requirements.contains(&ThisPredicateRequirement {
            predicate: NamedNode::new_unchecked("http://example.com/p"),
            direction: ThisPredicateDirection::Outgoing,
        }));
        assert!(requirements.contains(&ThisPredicateRequirement {
            predicate: NamedNode::new_unchecked("http://example.com/q"),
            direction: ThisPredicateDirection::Incoming,
        }));
    }

    #[test]
    fn required_this_predicates_intersects_union_branches() {
        let query = parse_query(
            "SELECT ?this WHERE {
                { ?this <http://example.com/p> ?value . }
                UNION
                { ?this <http://example.com/p> ?other ; <http://example.com/q> ?extra . }
            }",
        );

        let requirements = required_this_predicates(&query);
        assert_eq!(requirements.len(), 1);
        assert!(requirements.contains(&ThisPredicateRequirement {
            predicate: NamedNode::new_unchecked("http://example.com/p"),
            direction: ThisPredicateDirection::Outgoing,
        }));
    }

    #[test]
    fn required_this_predicates_ignore_optional_branch_only_requirements() {
        let query = parse_query(
            "SELECT ?this WHERE {
                ?this <http://example.com/p> ?value .
                OPTIONAL { ?this <http://example.com/q> ?extra . }
            }",
        );

        let requirements = required_this_predicates(&query);
        assert_eq!(requirements.len(), 1);
        assert!(requirements.contains(&ThisPredicateRequirement {
            predicate: NamedNode::new_unchecked("http://example.com/p"),
            direction: ThisPredicateDirection::Outgoing,
        }));
    }

    #[test]
    fn compiles_prefixed_target_select_class_query() {
        let compiled = compiled_target_select_query_from_str(
            "PREFIX ex: <http://example.com/>
SELECT ?this WHERE { ?this a ex:Thing . }",
        );

        assert_eq!(
            compiled,
            Some(CompiledTargetSelectQuery::ClassInstances {
                class: NamedNode::new_unchecked("http://example.com/Thing"),
            })
        );
    }

    #[test]
    fn compiles_target_select_object_projection_query() {
        let query =
            parse_query("SELECT ?target WHERE { ?source <http://example.com/contains> ?target . }");

        assert_eq!(
            compiled_target_select_query(&query),
            Some(CompiledTargetSelectQuery::ObjectsOf {
                predicate: NamedNode::new_unchecked("http://example.com/contains"),
            })
        );
    }

    #[test]
    fn does_not_compile_target_select_with_constant_object_filter() {
        let query = parse_query("SELECT ?this WHERE { ?this <http://example.com/p> <urn:o> . }");

        assert_eq!(compiled_target_select_query(&query), None);
    }

    #[test]
    fn lowers_adjacent_predicate_whitelist_query() {
        let query = parse_query(
            "SELECT ?this WHERE {
                ?this (<http://example.com/hasPart>?|<http://example.com/connectedThrough>?) ?anchor .
                FILTER NOT EXISTS {
                    { ?anchor ?p ?o . }
                    UNION
                    { ?o ?p ?anchor . }
                    FILTER (?p NOT IN (
                        <http://example.com/allowed1>,
                        <http://example.com/allowed2>
                    ))
                }
            }",
        );

        let lowered = lowered_sparql_query_kind(&query);
        assert_eq!(
            lowered,
            Some(LoweredSparqlQueryKind::AdjacentPredicateWhitelist(
                AdjacentPredicateWhitelistPlan {
                    anchor_path: LoweredPropertyPath::Alternative(vec![
                        LoweredPropertyPath::ZeroOrOne(Box::new(LoweredPropertyPath::NamedNode(
                            NamedNode::new_unchecked("http://example.com/hasPart",)
                        ),)),
                        LoweredPropertyPath::ZeroOrOne(Box::new(LoweredPropertyPath::NamedNode(
                            NamedNode::new_unchecked("http://example.com/connectedThrough",)
                        ),)),
                    ]),
                    allowed_predicates: vec![
                        NamedNode::new_unchecked("http://example.com/allowed1"),
                        NamedNode::new_unchecked("http://example.com/allowed2"),
                    ],
                }
            ))
        );
    }

    #[test]
    fn lowers_required_path_support_query() {
        let query = parse_query(
            "SELECT ?this ?other WHERE {
                ?this <http://example.com/connected> ?other .
                FILTER NOT EXISTS {
                    ?this <http://example.com/link>+ ?other .
                }
            }",
        );

        let lowered = lowered_sparql_query_kind(&query);
        assert_eq!(
            lowered,
            Some(LoweredSparqlQueryKind::RequiredPathSupport(
                RequiredPathSupportPlan {
                    antecedent_path: LoweredPropertyPath::NamedNode(NamedNode::new_unchecked(
                        "http://example.com/connected",
                    )),
                    support_path: LoweredPropertyPath::OneOrMore(Box::new(
                        LoweredPropertyPath::NamedNode(NamedNode::new_unchecked(
                            "http://example.com/link",
                        )),
                    )),
                    target_variable: "other".to_string(),
                }
            ))
        );
    }

    #[test]
    fn lowers_missing_related_node_query() {
        let query = parse_query(
            "SELECT ?this ?something WHERE {
                ?something <http://example.com/composedOf> ?this .
                FILTER NOT EXISTS {
                    ?this <http://example.com/ofConstituent> ?someSubstance .
                }
            }",
        );

        let lowered = lowered_sparql_query_kind(&query);
        assert_eq!(
            lowered,
            Some(LoweredSparqlQueryKind::MissingRelatedNode(
                MissingRelatedNodePlan {
                    related_path: LoweredPropertyPath::ReverseNamedNode(NamedNode::new_unchecked(
                        "http://example.com/composedOf"
                    ),),
                    related_variable: "something".to_string(),
                    related_class: None,
                    required_path: LoweredPropertyPath::NamedNode(NamedNode::new_unchecked(
                        "http://example.com/ofConstituent"
                    ),),
                }
            ))
        );
    }

    #[test]
    fn lowers_local_set_compatibility_query() {
        let query = parse_query(
            "PREFIX ex: <urn:>
             PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
             PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
             SELECT $this ?m2 ?cp ?m1 WHERE {
                $this ex:cnx ?cp .
                ?cp a/rdfs:subClassOf* ex:ConnectionPoint .
                ?cp ex:hasMedium ?m1 .
                $this ex:hasMedium ?m2 .
                ?m2 ex:composedOf/ex:ofConstituent ?s2 .
                FILTER NOT EXISTS { ?m1 ex:composedOf ?c1 . }
                FILTER NOT EXISTS {
                    ?m2 ex:composedOf/ex:ofConstituent ?s12 .
                    { ?s12 rdfs:subClassOf* ?m1 } UNION { ?m1 rdfs:subClassOf* ?s12 } .
                }
            }",
        );

        let lowered = lowered_sparql_query_kind(&query);
        assert_eq!(
            lowered,
            Some(LoweredSparqlQueryKind::LocalSetCompatibility(Box::new(
                LocalSetCompatibilityPlan {
                    left_anchor_path: LoweredPropertyPath::SelfNode,
                    right_anchor_path: LoweredPropertyPath::NamedNode(NamedNode::new_unchecked(
                        "urn:cnx",
                    )),
                    left_anchor_var: "this".to_string(),
                    right_anchor_var: "cp".to_string(),
                    left_class: None,
                    right_class: Some(Term::NamedNode(NamedNode::new_unchecked(
                        "urn:ConnectionPoint",
                    ))),
                    left_value_path: LoweredPropertyPath::NamedNode(NamedNode::new_unchecked(
                        "urn:hasMedium",
                    )),
                    right_value_path: LoweredPropertyPath::NamedNode(NamedNode::new_unchecked(
                        "urn:hasMedium",
                    )),
                    left_value_var: "m2".to_string(),
                    right_value_var: "m1".to_string(),
                    distinct_anchors: false,
                    composed_of_predicate: NamedNode::new_unchecked("urn:composedOf"),
                    constituent_path: Some(LoweredPropertyPath::Sequence(vec![
                        LoweredPropertyPath::NamedNode(NamedNode::new_unchecked("urn:composedOf")),
                        LoweredPropertyPath::NamedNode(NamedNode::new_unchecked(
                            "urn:ofConstituent",
                        )),
                    ])),
                    mode: LocalSetCompatibilityMode::CompositeVsPure {
                        composite_side: CompatibilitySide::Left,
                    },
                }
            )))
        );
    }

    #[test]
    fn compiles_prefixed_path_copy_rule() {
        let query = parse_query(
            "PREFIX ex: <http://example.com/ns#>
             CONSTRUCT { $this ex:output ?value . }
             WHERE { $this ^ex:sourceOf/ex:value ?value . }",
        );

        assert_eq!(
            compiled_sparql_rule(&query),
            Some(CompiledSparqlRule::PathCopy {
                construct_predicate: NamedNode::new_unchecked("http://example.com/ns#output"),
                source_path: LoweredPropertyPath::Sequence(vec![
                    LoweredPropertyPath::ReverseNamedNode(NamedNode::new_unchecked(
                        "http://example.com/ns#sourceOf",
                    )),
                    LoweredPropertyPath::NamedNode(NamedNode::new_unchecked(
                        "http://example.com/ns#value",
                    )),
                ]),
            })
        );
    }

    #[test]
    fn compiles_prefixed_equality_constant_rule() {
        let query = parse_query(
            "PREFIX ex: <http://example.com/ns#>
             CONSTRUCT { $this ex:isSquare true . }
             WHERE {
               $this ex:width ?w ;
                     ex:height ?h .
               FILTER(?w = ?h)
             }",
        );

        assert_eq!(
            compiled_sparql_rule(&query),
            Some(CompiledSparqlRule::EqualityConstant {
                construct_predicate: NamedNode::new_unchecked("http://example.com/ns#isSquare"),
                left_path: LoweredPropertyPath::NamedNode(NamedNode::new_unchecked(
                    "http://example.com/ns#width",
                )),
                right_path: LoweredPropertyPath::NamedNode(NamedNode::new_unchecked(
                    "http://example.com/ns#height",
                )),
                object: Term::Literal(Literal::from(true)),
            })
        );
    }

    #[test]
    fn compiled_rule_reports_index_requirements() {
        let query = parse_query(
            "PREFIX ex: <http://example.com/ns#>
             CONSTRUCT { $this ex:output ?value . }
             WHERE { $this ^ex:sourceOf/ex:value ?value . }",
        );

        let compiled = compiled_sparql_rule(&query).expect("compiled rule");
        assert_eq!(
            compiled_sparql_index_requirements(&compiled),
            vec![
                CompiledIndexRequirement::IncomingValues {
                    predicate: NamedNode::new_unchecked("http://example.com/ns#sourceOf"),
                },
                CompiledIndexRequirement::OutgoingValues {
                    predicate: NamedNode::new_unchecked("http://example.com/ns#value"),
                },
            ]
        );
    }

    #[test]
    fn compiled_index_plan_merges_directions_per_predicate() {
        let marker = NamedNode::new("http://example.com/ns#marker").unwrap();
        let value = NamedNode::new("http://example.com/ns#value").unwrap();

        let plan = compiled_sparql_index_plan([
            &CompiledIndexRequirement::OutgoingValues {
                predicate: marker.clone(),
            },
            &CompiledIndexRequirement::IncomingValues {
                predicate: marker.clone(),
            },
            &CompiledIndexRequirement::OutgoingValues {
                predicate: value.clone(),
            },
        ]);

        assert_eq!(
            plan,
            vec![
                CompiledPredicateIndexPlan {
                    predicate: marker,
                    include_outgoing: true,
                    include_incoming: true,
                },
                CompiledPredicateIndexPlan {
                    predicate: value,
                    include_outgoing: true,
                    include_incoming: false,
                },
            ]
        );
    }

    #[derive(Clone, Default)]
    struct TestCompiledPathResolver {
        outgoing: HashMap<(Term, NamedNode), Vec<Term>>,
        incoming: HashMap<(Term, NamedNode), Vec<Term>>,
    }

    impl CompiledPathResolver for TestCompiledPathResolver {
        type Error = String;

        fn direct_values(
            &self,
            focus: &Term,
            predicate: &NamedNode,
        ) -> Result<Vec<Term>, Self::Error> {
            Ok(self
                .outgoing
                .get(&(focus.clone(), predicate.clone()))
                .cloned()
                .unwrap_or_default())
        }

        fn inverse_values(
            &self,
            focus: &Term,
            predicate: &NamedNode,
        ) -> Result<Vec<Term>, Self::Error> {
            Ok(self
                .incoming
                .get(&(focus.clone(), predicate.clone()))
                .cloned()
                .unwrap_or_default())
        }

        fn without_delta(&self) -> Self {
            self.clone()
        }
    }

    #[test]
    fn evaluate_compiled_path_handles_sequences_and_inverse_steps() {
        let source_of = NamedNode::new("http://example.com/ns#sourceOf").unwrap();
        let value = NamedNode::new("http://example.com/ns#value").unwrap();
        let focus = Term::NamedNode(NamedNode::new("http://example.com/ns#focus").unwrap());
        let source = Term::NamedNode(NamedNode::new("http://example.com/ns#source").unwrap());
        let flag = Term::Literal(Literal::new_simple_literal("flag"));

        let mut resolver = TestCompiledPathResolver::default();
        resolver
            .incoming
            .insert((focus.clone(), source_of.clone()), vec![source.clone()]);
        resolver
            .outgoing
            .insert((source.clone(), value.clone()), vec![flag.clone()]);

        let path = LoweredPropertyPath::Sequence(vec![
            LoweredPropertyPath::ReverseNamedNode(source_of),
            LoweredPropertyPath::NamedNode(value),
        ]);

        assert_eq!(
            evaluate_compiled_path(&resolver, &focus, &path).expect("path should resolve"),
            vec![flag]
        );
    }
}
