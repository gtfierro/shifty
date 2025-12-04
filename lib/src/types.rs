use crate::backend::Binding;
use crate::context::{Context, SourceShape, ValidationContext};
use crate::named_nodes::SHACL;
use crate::runtime::component::{check_conformance_for_node, ConformanceReport};
use oxigraph::model::{
    Literal, NamedNode, NamedNodeRef, NamedOrBlankNodeRef, Term, TermRef, Variable,
};
use oxigraph::sparql::QueryResults;
use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;

/// A unique identifier for a `NodeShape`.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct ID(pub u64);

impl From<u64> for ID {
    fn from(item: u64) -> Self {
        ID(item)
    }
}

impl fmt::Display for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ID {
    /// Converts the ID to a string suitable for use as a node identifier in Graphviz.
    pub fn to_graphviz_id(&self) -> String {
        format!("n{}", self.0)
    }
}

/// A unique identifier for a constraint `Component`.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct ComponentID(pub u64);

impl From<u64> for ComponentID {
    fn from(item: u64) -> Self {
        ComponentID(item)
    }
}

impl fmt::Display for ComponentID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ComponentID {
    /// Converts the ComponentID to a string suitable for use as a node identifier in Graphviz.
    pub fn to_graphviz_id(&self) -> String {
        format!("c{}", self.0)
    }
}

/// A unique identifier for a `Rule`.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct RuleID(pub u64);

impl From<u64> for RuleID {
    fn from(item: u64) -> Self {
        RuleID(item)
    }
}

impl fmt::Display for RuleID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl RuleID {
    /// Converts the RuleID to a string suitable for use as a node identifier in Graphviz.
    pub fn to_graphviz_id(&self) -> String {
        format!("r{}", self.0)
    }
}

/// A unique identifier for a `PropertyShape`.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct PropShapeID(pub u64);

impl From<u64> for PropShapeID {
    fn from(item: u64) -> Self {
        PropShapeID(item)
    }
}

impl fmt::Display for PropShapeID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PropShapeID {
    /// Converts the PropShapeID to a string suitable for use as a node identifier in Graphviz.
    pub fn to_graphviz_id(&self) -> String {
        format!("p{}", self.0)
    }
}

/// Represents a SHACL Property Path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Path {
    /// A simple path, which is a single IRI.
    Simple(Term),
    /// An inverse path (`sh:inversePath`).
    Inverse(Box<Path>),
    /// A sequence of paths (`sh:sequencePath`).
    Sequence(Vec<Path>),
    /// A set of alternative paths (`sh:alternativePath`).
    Alternative(Vec<Path>),
    /// A path that can be traversed zero or more times (`sh:zeroOrMorePath`).
    ZeroOrMore(Box<Path>),
    /// A path that can be traversed one or more times (`sh:oneOrMorePath`).
    OneOrMore(Box<Path>),
    /// A path that can be traversed zero or one time (`sh:zeroOrOnePath`).
    ZeroOrOne(Box<Path>),
}

impl Path {
    // Helper to format a NamedNode for SPARQL path
    fn format_named_node_for_sparql(nn: &oxigraph::model::NamedNode) -> String {
        format!("<{}>", nn.as_str())
    }

    /// Converts the SHACL path to its SPARQL 1.1 property path string representation.
    pub fn to_sparql_path(&self) -> Result<String, String> {
        match self {
            Path::Simple(term) => match term {
                Term::NamedNode(nn) => Ok(Self::format_named_node_for_sparql(nn)),
                _ => Err(format!("Simple path must be an IRI {:?}", self)),
            },
            Path::Inverse(inner_path) => {
                let inner_sparql = inner_path.to_sparql_path()?;
                // Wrap complex inner paths in parentheses to preserve precedence.
                // For simple IRIs, avoid extra parentheses.
                match &**inner_path {
                    Path::Simple(_) => Ok(format!("^{}", inner_sparql)),
                    _ => Ok(format!("^({})", inner_sparql)),
                }
            }
            Path::Sequence(paths) => {
                if paths.is_empty() {
                    return Err("Sequence path must have at least one element".to_string());
                }
                if paths.len() == 1 {
                    // Collapse single-element sequences to the inner path.
                    return paths[0].to_sparql_path();
                }
                let mut sparql_paths = Vec::new();
                for p in paths {
                    sparql_paths.push(p.to_sparql_path()?);
                }
                let result = sparql_paths.join(" / ");
                Ok(format!("({})", result)) // Always parenthesize sequence for clarity and safety.
            }
            Path::Alternative(paths) => {
                if paths.is_empty() {
                    return Err("Alternative path must have at least one element".to_string());
                }
                if paths.len() == 1 {
                    // Collapse single-element alternatives to the inner path.
                    return paths[0].to_sparql_path();
                }
                let mut sparql_paths = Vec::new();
                for p in paths {
                    sparql_paths.push(p.to_sparql_path()?);
                }
                let result = sparql_paths.join(" | ");
                Ok(format!("({})", result)) // Always parenthesize alternative for clarity and safety.
            }
            Path::ZeroOrMore(inner_path) => {
                let inner_sparql = inner_path.to_sparql_path()?;
                match &**inner_path {
                    Path::Simple(_) => Ok(format!("{}*", inner_sparql)),
                    _ => Ok(format!("({})*", inner_sparql)),
                }
            }
            Path::OneOrMore(inner_path) => {
                let inner_sparql = inner_path.to_sparql_path()?;
                match &**inner_path {
                    Path::Simple(_) => Ok(format!("{}+", inner_sparql)),
                    _ => Ok(format!("({})+", inner_sparql)),
                }
            }
            Path::ZeroOrOne(inner_path) => {
                let inner_sparql = inner_path.to_sparql_path()?;
                match &**inner_path {
                    Path::Simple(_) => Ok(format!("{}?", inner_sparql)),
                    _ => Ok(format!("({})?", inner_sparql)),
                }
            }
        }
    }

    pub fn is_simple_predicate(&self) -> bool {
        matches!(self, Path::Simple(Term::NamedNode(_)))
    }
}

/// Represents a SHACL target, which specifies the nodes to be validated against a shape.
#[derive(Debug, Clone)]
pub enum Target {
    /// Targets all instances of a given class (`sh:targetClass`).
    Class(Term),
    /// Targets a specific node (`sh:targetNode`).
    Node(Term),
    /// Targets all subjects of triples with a given predicate (`sh:targetSubjectsOf`).
    SubjectsOf(Term),
    /// Targets all objects of triples with a given predicate (`sh:targetObjectsOf`).
    ObjectsOf(Term),
    /// Targets nodes defined by an advanced target selector (`sh:target`, `sh:targetValidator`).
    Advanced(Term),
}

/// Represents the severity level of a validation result, corresponding to `sh:severity`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum Severity {
    /// Corresponds to `sh:Info`.
    Info,
    /// Corresponds to `sh:Warning`.
    Warning,
    /// Corresponds to `sh:Violation`.
    #[default]
    Violation,
    /// Custom severity IRI provided in the shapes graph.
    Custom(NamedNode),
}

impl Severity {
    /// Creates a `Severity` from a `Term` if it matches a SHACL severity IRI.
    pub(crate) fn from_term(term: &Term) -> Option<Self> {
        let shacl = SHACL::new();
        match term {
            Term::NamedNode(nn) if *nn == shacl.info => Some(Severity::Info),
            Term::NamedNode(nn) if *nn == shacl.warning => Some(Severity::Warning),
            Term::NamedNode(nn) if *nn == shacl.violation => Some(Severity::Violation),
            Term::NamedNode(nn) => Some(Severity::Custom(nn.clone())),
            _ => None,
        }
    }
}

impl Target {
    /// Creates a `Target` from a predicate and object `TermRef` if they correspond to a known SHACL target property.
    pub(crate) fn from_predicate_object(predicate: NamedNodeRef, object: TermRef) -> Option<Self> {
        let shacl = SHACL::new();
        if predicate == shacl.target_class {
            Some(Target::Class(object.into_owned()))
        } else if predicate == shacl.target_node {
            Some(Target::Node(object.into_owned()))
        } else if predicate == shacl.target_subjects_of {
            Some(Target::SubjectsOf(object.into_owned()))
        } else if predicate == shacl.target_objects_of {
            Some(Target::ObjectsOf(object.into_owned()))
        } else if predicate == shacl.target || predicate == shacl.target_validator {
            Some(Target::Advanced(object.into_owned()))
        } else {
            None
        }
    }

    /// Retrieves the set of focus nodes for this target from the data graph.
    pub(crate) fn get_target_nodes(
        &self,
        context: &ValidationContext,
        source_shape: SourceShape,
    ) -> Result<Vec<Context>, String> {
        match self {
            Target::Node(t) => Ok(contexts_from_terms(
                context,
                std::iter::once(t.clone()),
                source_shape,
            )),
            Target::Class(c) => {
                let query_str = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    SELECT DISTINCT ?inst ?target_class WHERE { ?inst rdf:type ?c . ?c rdfs:subClassOf* ?target_class }";
                let target_class_var = Variable::new("target_class").map_err(|e| e.to_string())?;

                let prepared = context.prepare_query(query_str).map_err(|e| {
                    format!(
                        "SPARQL parse error for Target::Class: {} {:?}",
                        query_str, e
                    )
                })?;

                let substitutions: Vec<Binding> = vec![(target_class_var, c.clone())];
                let results = context
                    .execute_prepared(query_str, &prepared, &substitutions, false)
                    .map_err(|e| {
                        format!("SPARQL query error for Target::Class: {} {}", query_str, e)
                    })?;

                match results {
                    QueryResults::Solutions(solutions) => solutions
                        .map(|solution_result| {
                            let solution = solution_result.map_err(|e| e.to_string())?;
                            solution
                                .get("inst")
                                .map(|term_ref| {
                                    build_context(
                                        context,
                                        term_ref.to_owned(),
                                        source_shape.clone(),
                                    )
                                })
                                .ok_or_else(|| {
                                    "Variable 'inst' not found in Target::Class query solution"
                                        .to_string()
                                })
                        })
                        .collect(),
                    _ => Err(format!(
                        "Unexpected result type for Target::Class: {}",
                        query_str
                    )),
                }
            }
            Target::SubjectsOf(p) => {
                if let Term::NamedNode(predicate_node) = p {
                    let query_str = format!(
                        "SELECT DISTINCT ?s WHERE {{ ?s <{}> ?any . }}",
                        predicate_node.as_str()
                    );
                    let prepared = context.prepare_query(&query_str).map_err(|e| {
                        format!(
                            "SPARQL parse error for Target::SubjectsOf: {} {:?}",
                            query_str, e
                        )
                    })?;

                    let results = context
                        .execute_prepared(&query_str, &prepared, &[], false)
                        .map_err(|e| e.to_string())?;

                    match results {
                        QueryResults::Solutions(solutions) => solutions
                            .map(|solution_result| {
                                let solution = solution_result.map_err(|e| e.to_string())?;
                                solution
                                .get("s")
                                .map(|term_ref| {
                                    build_context(
                                        context,
                                        term_ref.to_owned(),
                                        source_shape.clone(),
                                    )
                                })
                                .ok_or_else(|| {
                                    "Variable 's' not found in Target::SubjectsOf query solution"
                                        .to_string()
                                })
                            })
                            .collect(),
                        _ => Err("Unexpected result type for Target::SubjectsOf query".to_string()),
                    }
                } else {
                    Ok(vec![]) // Predicate for SubjectsOf must be an IRI
                }
            }
            Target::ObjectsOf(p) => {
                if let Term::NamedNode(predicate_node) = p {
                    let query_str = format!(
                        "SELECT DISTINCT ?o WHERE {{ ?any <{}> ?o . }}",
                        predicate_node.as_str()
                    );
                    let prepared = context.prepare_query(&query_str).map_err(|e| {
                        format!(
                            "SPARQL parse error for Target::ObjectsOf: {} {:?}",
                            query_str, e
                        )
                    })?;

                    let results = context
                        .execute_prepared(&query_str, &prepared, &[], false)
                        .map_err(|e| e.to_string())?;

                    match results {
                        QueryResults::Solutions(solutions) => solutions
                            .map(|solution_result| {
                                let solution = solution_result.map_err(|e| e.to_string())?;
                                solution
                                    .get("o")
                                    .map(|term_ref| {
                                        build_context(
                                            context,
                                            term_ref.to_owned(),
                                            source_shape.clone(),
                                        )
                                    })
                                    .ok_or_else(|| {
                                        "Variable 'o' not found in Target::ObjectsOf query solution"
                                            .to_string()
                                    })
                            })
                            .collect(),
                        _ => Err("Unexpected result type for Target::ObjectsOf query".to_string()),
                    }
                } else {
                    Ok(vec![]) // Predicate for ObjectsOf must be an IRI
                }
            }
            Target::Advanced(selector) => evaluate_advanced_target(context, selector, source_shape),
        }
    }
}

fn contexts_from_terms(
    context: &ValidationContext,
    terms: impl IntoIterator<Item = Term>,
    source_shape: SourceShape,
) -> Vec<Context> {
    terms
        .into_iter()
        .map(|term| build_context(context, term, source_shape.clone()))
        .collect()
}

fn build_context(context: &ValidationContext, term: Term, source_shape: SourceShape) -> Context {
    let trace_index = context.new_trace();
    Context::new(
        term.clone(),
        None,
        Some(vec![term]),
        source_shape,
        trace_index,
    )
}

fn evaluate_advanced_target(
    context: &ValidationContext,
    selector: &Term,
    source_shape: SourceShape,
) -> Result<Vec<Context>, String> {
    if let Some(cached) = context.cached_advanced_target(selector) {
        return Ok(contexts_from_terms(context, cached, source_shape));
    }

    let selector_ref = term_to_subject_ref(selector)?;
    let shacl = SHACL::new();
    let shape_graph = context.shape_graph_iri_ref();

    let mut focus_terms: Vec<Term> = Vec::new();

    // Reuse existing target handling for core target predicates nested inside the advanced selector.
    focus_terms.extend(collect_nested_targets(
        context,
        selector_ref,
        shacl.target_node,
        |term| Target::Node(term).get_target_nodes(context, source_shape.clone()),
        shape_graph,
    )?);
    focus_terms.extend(collect_nested_targets(
        context,
        selector_ref,
        shacl.target_class,
        |term| Target::Class(term).get_target_nodes(context, source_shape.clone()),
        shape_graph,
    )?);
    focus_terms.extend(collect_nested_targets(
        context,
        selector_ref,
        shacl.target_subjects_of,
        |term| Target::SubjectsOf(term).get_target_nodes(context, source_shape.clone()),
        shape_graph,
    )?);
    focus_terms.extend(collect_nested_targets(
        context,
        selector_ref,
        shacl.target_objects_of,
        |term| Target::ObjectsOf(term).get_target_nodes(context, source_shape.clone()),
        shape_graph,
    )?);
    focus_terms.extend(collect_target_shape_nodes(
        context,
        selector_ref,
        shape_graph,
    )?);

    // SPARQL-based selector via sh:select.
    for term in context.objects_for_predicate(selector_ref, shacl.select, shape_graph)? {
        if let Term::Literal(lit) = term {
            let query = build_prefixed_query(context, selector, &lit)?;
            let nodes = execute_select_query(context, &query)?;
            focus_terms.extend(nodes);
        }
    }

    let mut focus_terms = deduplicate_terms(focus_terms);
    focus_terms = apply_filter_shapes(context, selector_ref, focus_terms, shape_graph)?;
    focus_terms = apply_ask_validators(context, selector, selector_ref, focus_terms, shape_graph)?;

    context.store_advanced_target(selector, &focus_terms);
    Ok(contexts_from_terms(context, focus_terms, source_shape))
}

fn collect_nested_targets<F>(
    context: &ValidationContext,
    selector_ref: NamedOrBlankNodeRef<'_>,
    predicate: NamedNodeRef<'_>,
    mut evaluator: F,
    shape_graph: oxigraph::model::GraphNameRef<'_>,
) -> Result<Vec<Term>, String>
where
    F: FnMut(Term) -> Result<Vec<Context>, String>,
{
    let mut terms = Vec::new();
    let objects = context.objects_for_predicate(selector_ref, predicate, shape_graph)?;
    for object in objects {
        let evaluated = evaluator(object)?;
        terms.extend(contexts_to_terms(evaluated));
    }
    Ok(terms)
}

fn collect_target_shape_nodes(
    context: &ValidationContext,
    selector_ref: NamedOrBlankNodeRef<'_>,
    shape_graph: oxigraph::model::GraphNameRef<'_>,
) -> Result<Vec<Term>, String> {
    let shacl = SHACL::new();
    let shape_ids = collect_shape_ids(context, selector_ref, shacl.target_shape, shape_graph)?;
    let mut terms = Vec::new();
    for shape_id in shape_ids {
        terms.extend(target_nodes_for_shape(context, shape_id)?);
    }
    Ok(terms)
}

fn collect_shape_ids(
    context: &ValidationContext,
    selector_ref: NamedOrBlankNodeRef<'_>,
    predicate: NamedNodeRef<'_>,
    shape_graph: oxigraph::model::GraphNameRef<'_>,
) -> Result<Vec<ID>, String> {
    let mut ids = Vec::new();
    let objects = context.objects_for_predicate(selector_ref, predicate, shape_graph)?;
    for term in objects {
        let id = {
            let lookup = context.model.nodeshape_id_lookup().borrow();
            lookup.get(&term).ok_or_else(|| {
                format!(
                    "Shape {} referenced via {} is not recognised as a node shape",
                    term,
                    predicate.as_str()
                )
            })?
        };
        ids.push(id);
    }
    Ok(ids)
}

fn target_nodes_for_shape(context: &ValidationContext, shape_id: ID) -> Result<Vec<Term>, String> {
    let shape = context
        .model
        .get_node_shape_by_id(&shape_id)
        .ok_or_else(|| format!("Target shape {:?} not found in model", shape_id))?;

    let mut terms = Vec::new();
    for target in &shape.targets {
        let contexts = target.get_target_nodes(context, SourceShape::NodeShape(shape_id))?;
        terms.extend(contexts_to_terms(contexts));
    }
    Ok(deduplicate_terms(terms))
}

fn apply_filter_shapes(
    context: &ValidationContext,
    selector_ref: NamedOrBlankNodeRef<'_>,
    focus_terms: Vec<Term>,
    shape_graph: oxigraph::model::GraphNameRef<'_>,
) -> Result<Vec<Term>, String> {
    let shacl = SHACL::new();
    let filter_ids = collect_shape_ids(context, selector_ref, shacl.filter_shape, shape_graph)?;
    if filter_ids.is_empty() {
        return Ok(focus_terms);
    }

    let mut filtered = Vec::new();
    for term in focus_terms {
        let mut conforms = true;
        for shape_id in &filter_ids {
            if !node_conforms_to_shape(context, &term, *shape_id)? {
                conforms = false;
                break;
            }
        }
        if conforms {
            filtered.push(term);
        }
    }
    Ok(filtered)
}

fn apply_ask_validators(
    context: &ValidationContext,
    selector: &Term,
    selector_ref: NamedOrBlankNodeRef<'_>,
    focus_terms: Vec<Term>,
    shape_graph: oxigraph::model::GraphNameRef<'_>,
) -> Result<Vec<Term>, String> {
    let shacl = SHACL::new();
    let mut current_terms = focus_terms;
    for term in context.objects_for_predicate(selector_ref, shacl.ask, shape_graph)? {
        let lit = match term {
            Term::Literal(l) => l,
            other => {
                return Err(format!(
                    "sh:ask expects a literal query string, found {:?}",
                    other
                ))
            }
        };
        let query = build_prefixed_query(context, selector, &lit)?;
        let mut next_terms = Vec::new();
        for term in current_terms.into_iter() {
            if execute_ask_query(context, &query, &term)? {
                next_terms.push(term);
            }
        }
        current_terms = next_terms;
    }
    Ok(current_terms)
}

fn build_prefixed_query(
    context: &ValidationContext,
    selector: &Term,
    literal: &Literal,
) -> Result<String, String> {
    let prefixes = context
        .prefixes_for_node(selector)
        .map_err(|e| format!("Failed to resolve prefixes for advanced target: {}", e))?;

    let mut query = literal.value().to_string();
    let trimmed_prefixes = prefixes.trim();
    if !trimmed_prefixes.is_empty() {
        if query.starts_with('\n') {
            query = query.trim_start_matches('\n').to_string();
        }
        query = format!("{}\n{}", trimmed_prefixes, query);
    }
    Ok(query)
}

fn execute_select_query(context: &ValidationContext, query: &str) -> Result<Vec<Term>, String> {
    let prepared = context
        .prepare_query(query)
        .map_err(|e| format!("SPARQL parse error for advanced target: {} {:?}", query, e))?;
    let results = context
        .execute_prepared(query, &prepared, &[], false)
        .map_err(|e| format!("SPARQL execution error for advanced target: {}", e))?;

    match results {
        QueryResults::Solutions(solutions) => {
            let mut terms = Vec::new();
            for solution_result in solutions {
                let solution = solution_result.map_err(|e| e.to_string())?;
                if let Some(term) = solution.get("this") {
                    terms.push(term.clone());
                    continue;
                }
                let mut fallback = None;
                for var in solution.variables() {
                    if let Some(term) = solution.get(var) {
                        fallback = Some(term.clone());
                        break;
                    }
                }
                if let Some(term) = fallback {
                    terms.push(term);
                } else {
                    return Err(
                        "Advanced target query did not bind any variables for a solution"
                            .to_string(),
                    );
                }
            }
            Ok(terms)
        }
        _ => Err("Advanced target query must be a SELECT query".to_string()),
    }
}

fn term_to_subject_ref(term: &Term) -> Result<NamedOrBlankNodeRef<'_>, String> {
    match term {
        Term::NamedNode(nn) => Ok(nn.as_ref().into()),
        Term::BlankNode(bn) => Ok(bn.as_ref().into()),
        _ => Err(format!(
            "Advanced target selector must be a named node or blank node, found {:?}",
            term
        )),
    }
}

fn contexts_to_terms(contexts: Vec<Context>) -> Vec<Term> {
    contexts
        .into_iter()
        .map(|ctx| ctx.focus_node().clone())
        .collect()
}

fn deduplicate_terms(terms: Vec<Term>) -> Vec<Term> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for term in terms {
        if seen.insert(term.clone()) {
            deduped.push(term);
        }
    }
    deduped
}

pub(crate) fn node_conforms_to_shape(
    context: &ValidationContext,
    node: &Term,
    shape_id: ID,
) -> Result<bool, String> {
    let shape = context
        .model
        .get_node_shape_by_id(&shape_id)
        .ok_or_else(|| format!("Filter shape {:?} not found in model", shape_id))?;

    let trace_index = context.new_trace();
    let mut node_context = Context::new(
        node.clone(),
        None,
        Some(vec![node.clone()]),
        SourceShape::NodeShape(shape_id),
        trace_index,
    );
    let mut temp_trace = Vec::new();
    match check_conformance_for_node(&mut node_context, shape, context, &mut temp_trace)? {
        ConformanceReport::Conforms => Ok(true),
        ConformanceReport::NonConforms(_) => Ok(false),
    }
}

fn execute_ask_query(
    context: &ValidationContext,
    query: &str,
    term: &Term,
) -> Result<bool, String> {
    let prepared = context.prepare_query(query).map_err(|e| {
        format!(
            "SPARQL parse error for advanced target ASK: {} {:?}",
            query, e
        )
    })?;
    let var_this = Variable::new("this").map_err(|e| e.to_string())?;
    let results = context.execute_prepared(query, &prepared, &[(var_this, term.clone())], false)?;
    match results {
        QueryResults::Boolean(value) => Ok(value),
        _ => Err("Advanced target ASK query must return a boolean result".to_string()),
    }
}

/// An item in the execution trace of a validation, used for debugging and reporting.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TraceItem {
    /// A `NodeShape` was visited.
    NodeShape(ID),
    /// A `PropertyShape` was visited.
    PropertyShape(PropShapeID),
    /// A `Component` was validated.
    Component(ComponentID),
}

impl fmt::Display for TraceItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TraceItem::NodeShape(id) => write!(f, "NodeShape({})", id.to_graphviz_id()),
            TraceItem::PropertyShape(id) => write!(f, "PropertyShape({})", id.to_graphviz_id()),
            TraceItem::Component(id) => write!(f, "Component({})", id.to_graphviz_id()),
        }
    }
}
