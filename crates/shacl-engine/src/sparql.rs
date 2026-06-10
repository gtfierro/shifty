//! Shared SPARQL execution over an Oxigraph default graph.
//!
//! Queries are parsed and canonicalized by `shacl-parse`. This layer caches
//! Oxigraph's prepared form, applies SHACL prebindings, and executes against a
//! store that is kept in sync with rule inference.

use oxigraph::sparql::{PreparedSparqlQuery, QueryResults, SparqlEvaluator};
use oxigraph::store::Store;
use oxrdf::{
    Graph, GraphName, GraphNameRef, Literal, NamedNode, Quad, QuadRef, Term, Triple, Variable,
};
use shacl_algebra::{Path, SparqlConstraint};
use spargebra::algebra::{AggregateExpression, Expression, GraphPattern, OrderExpression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::{Query, SparqlParser};
use std::cell::RefCell;
use std::collections::HashMap;

/// The named graph the shapes graph is loaded under so `GRAPH $shapesGraph {…}`
/// can be evaluated; `$shapesGraph` is pre-bound to this IRI.
const SHAPES_GRAPH_IRI: &str = "urn:x-shacl:shapes-graph";

pub(crate) struct SparqlExecutor {
    store: Store,
    prepared: RefCell<HashMap<String, PreparedSparqlQuery>>,
    parsed: RefCell<HashMap<String, Query>>,
    /// IRI of the loaded shapes graph, pre-bound to `$shapesGraph`. `None` when
    /// no shapes graph was loaded (so `$shapesGraph` is left unsupported).
    shapes_graph: Option<NamedNode>,
}

#[derive(Debug)]
pub(crate) struct SparqlViolation {
    pub value: Option<Term>,
    pub path: Option<Term>,
}

impl SparqlExecutor {
    pub fn new(graph: &Graph) -> Result<Self, String> {
        Self::build(graph, None)
    }

    /// Like [`new`], but also loads `shapes` into a named graph so SHACL-SPARQL
    /// `$shapesGraph` / `GRAPH $shapesGraph {…}` can be evaluated.
    pub fn new_with_shapes(context: &Graph, shapes: &Graph) -> Result<Self, String> {
        Self::build(context, Some(shapes))
    }

    fn build(context: &Graph, shapes: Option<&Graph>) -> Result<Self, String> {
        let store = Store::new().map_err(|e| e.to_string())?;
        store
            .extend(context.iter().map(|triple| {
                Quad::new(
                    triple.subject.into_owned(),
                    triple.predicate.into_owned(),
                    triple.object.into_owned(),
                    GraphName::DefaultGraph,
                )
            }))
            .map_err(|e| e.to_string())?;
        let shapes_graph = match shapes {
            Some(shapes) => {
                let iri = NamedNode::new(SHAPES_GRAPH_IRI).expect("static IRI is valid");
                store
                    .extend(shapes.iter().map(|triple| {
                        Quad::new(
                            triple.subject.into_owned(),
                            triple.predicate.into_owned(),
                            triple.object.into_owned(),
                            GraphName::NamedNode(iri.clone()),
                        )
                    }))
                    .map_err(|e| e.to_string())?;
                Some(iri)
            }
            None => None,
        };
        Ok(Self {
            store,
            prepared: RefCell::new(HashMap::new()),
            parsed: RefCell::new(HashMap::new()),
            shapes_graph,
        })
    }

    pub fn insert(&self, triple: &Triple) -> Result<(), String> {
        self.store
            .insert(QuadRef::new(
                triple.subject.as_ref(),
                triple.predicate.as_ref(),
                triple.object.as_ref(),
                GraphNameRef::DefaultGraph,
            ))
            .map_err(|e| e.to_string())
    }

    pub fn target_nodes(&self, query: &str) -> Result<Vec<Term>, String> {
        let results = self
            .prepared(query)?
            .on_store(&self.store)
            .execute()
            .map_err(err)?;
        let QueryResults::Solutions(solutions) = results else {
            return Err("SPARQL target did not produce SELECT solutions".to_string());
        };
        let mut nodes = Vec::new();
        for solution in solutions {
            if let Some(node) = solution.map_err(err)?.get("this") {
                nodes.push(node.clone());
            }
        }
        Ok(nodes)
    }

    pub fn constraint_violations(
        &self,
        constraint: &SparqlConstraint,
        focus: &Term,
    ) -> Result<Vec<SparqlViolation>, String> {
        // SHACL pre-binding is defined as *substitution* of the bound variable
        // with its value throughout the query (SHACL §5.2.1), not as an initial
        // binding / outer join — the latter gives wrong answers for BIND,
        // sub-SELECT, and nested groups. Rewrite the parsed algebra directly.
        let mut query = self.parse(&constraint.query)?;
        if let Some(path) = &constraint.path {
            match path {
                Path::Pred(predicate) => substitute_query(
                    &mut query,
                    &variable("PATH"),
                    &Term::NamedNode(predicate.clone()),
                ),
                _ => {
                    return Err(
                        "SPARQL $PATH prebinding for complex SHACL paths requires query rewriting"
                            .to_string(),
                    );
                }
            }
        }
        substitute_query(&mut query, &variable("this"), focus);
        if let Some(shape) = &constraint.shape {
            substitute_query(&mut query, &variable("currentShape"), shape);
        }
        if let Some(graph) = &self.shapes_graph {
            substitute_query(
                &mut query,
                &variable("shapesGraph"),
                &Term::NamedNode(graph.clone()),
            );
        }
        let prepared = SparqlEvaluator::new().for_query(query);

        match prepared.on_store(&self.store).execute().map_err(err)? {
            QueryResults::Solutions(solutions) => solutions
                .map(|solution| {
                    let solution = solution.map_err(err)?;
                    Ok(SparqlViolation {
                        value: solution.get("value").cloned(),
                        path: solution.get("path").cloned(),
                    })
                })
                .collect(),
            QueryResults::Boolean(violates) => Ok(if violates {
                vec![SparqlViolation {
                    value: None,
                    path: None,
                }]
            } else {
                Vec::new()
            }),
            QueryResults::Graph(_) => {
                Err("SPARQL constraint unexpectedly produced graph results".to_string())
            }
        }
    }

    pub fn construct(&self, query: &str, focus: &Term) -> Result<Vec<Triple>, String> {
        // Pre-bind `$this` by algebra substitution (template included), matching
        // the constraint path rather than relying on initial bindings.
        let mut query = self.parse(query)?;
        substitute_query(&mut query, &variable("this"), focus);
        let prepared = SparqlEvaluator::new().for_query(query);
        let QueryResults::Graph(triples) = prepared.on_store(&self.store).execute().map_err(err)?
        else {
            return Err("SPARQL rule did not produce CONSTRUCT graph results".to_string());
        };
        triples.map(|triple| triple.map_err(err)).collect()
    }

    fn prepared(&self, query: &str) -> Result<PreparedSparqlQuery, String> {
        if let Some(prepared) = self.prepared.borrow().get(query) {
            return Ok(prepared.clone());
        }
        let prepared = SparqlEvaluator::new().parse_query(query).map_err(err)?;
        self.prepared
            .borrow_mut()
            .insert(query.to_string(), prepared.clone());
        Ok(prepared)
    }

    /// Parse (and cache) a canonical query string into its algebra form. The
    /// cached `Query` is cloned per call so callers can substitute pre-bound
    /// variables into it without affecting the cache.
    fn parse(&self, query: &str) -> Result<Query, String> {
        if let Some(parsed) = self.parsed.borrow().get(query) {
            return Ok(parsed.clone());
        }
        let parsed = SparqlParser::new().parse_query(query).map_err(err)?;
        self.parsed
            .borrow_mut()
            .insert(query.to_string(), parsed.clone());
        Ok(parsed)
    }
}

/// Substitute every occurrence of `var` with the pre-bound `value` throughout a
/// query, implementing SHACL-SPARQL pre-binding by algebra substitution.
fn substitute_query(query: &mut Query, var: &Variable, value: &Term) {
    match query {
        Query::Select { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => substitute_pattern(pattern, var, value),
        Query::Construct {
            template, pattern, ..
        } => {
            for triple in template {
                substitute_triple(triple, var, value);
            }
            substitute_pattern(pattern, var, value);
        }
    }
}

fn substitute_pattern(pattern: &mut GraphPattern, var: &Variable, value: &Term) {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for triple in patterns {
                substitute_triple(triple, var, value);
            }
        }
        GraphPattern::Path {
            subject, object, ..
        } => {
            substitute_term_pattern(subject, var, value);
            substitute_term_pattern(object, var, value);
        }
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right }
        | GraphPattern::Lateral { left, right } => {
            substitute_pattern(left, var, value);
            substitute_pattern(right, var, value);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            substitute_pattern(left, var, value);
            substitute_pattern(right, var, value);
            if let Some(expression) = expression {
                substitute_expr(expression, var, value);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            substitute_expr(expr, var, value);
            substitute_pattern(inner, var, value);
        }
        GraphPattern::Graph { name, inner } => {
            substitute_named_node_pattern(name, var, value);
            substitute_pattern(inner, var, value);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            // `variable` is the BIND target; pre-binding it is disallowed, so
            // only its inner pattern and defining expression are substituted.
            substitute_pattern(inner, var, value);
            substitute_expr(expression, var, value);
        }
        GraphPattern::OrderBy { inner, expression } => {
            substitute_pattern(inner, var, value);
            for order in expression {
                match order {
                    OrderExpression::Asc(e) | OrderExpression::Desc(e) => {
                        substitute_expr(e, var, value)
                    }
                }
            }
        }
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => substitute_pattern(inner, var, value),
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            substitute_pattern(inner, var, value);
            for (_, aggregate) in aggregates {
                if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                    substitute_expr(expr, var, value);
                }
            }
        }
        GraphPattern::Service { name, inner, .. } => {
            substitute_named_node_pattern(name, var, value);
            substitute_pattern(inner, var, value);
        }
        GraphPattern::Values { .. } => {}
    }
}

fn substitute_triple(triple: &mut TriplePattern, var: &Variable, value: &Term) {
    substitute_term_pattern(&mut triple.subject, var, value);
    substitute_named_node_pattern(&mut triple.predicate, var, value);
    substitute_term_pattern(&mut triple.object, var, value);
}

fn substitute_term_pattern(pattern: &mut TermPattern, var: &Variable, value: &Term) {
    if matches!(pattern, TermPattern::Variable(v) if v == var)
        && let Some(replacement) = term_to_term_pattern(value)
    {
        *pattern = replacement;
    }
}

fn substitute_named_node_pattern(pattern: &mut NamedNodePattern, var: &Variable, value: &Term) {
    if matches!(pattern, NamedNodePattern::Variable(v) if v == var)
        && let Term::NamedNode(node) = value
    {
        *pattern = NamedNodePattern::NamedNode(node.clone());
    }
}

fn substitute_expr(expr: &mut Expression, var: &Variable, value: &Term) {
    match expr {
        Expression::Variable(v) if v == var => {
            if let Some(replacement) = term_to_expr(value) {
                *expr = replacement;
            }
        }
        // A pre-bound variable is always bound, so `bound(var)` is constant-true.
        Expression::Bound(v) if v == var => *expr = Expression::Literal(Literal::from(true)),
        Expression::Variable(_)
        | Expression::Bound(_)
        | Expression::NamedNode(_)
        | Expression::Literal(_) => {}
        Expression::Or(a, b)
        | Expression::And(a, b)
        | Expression::Equal(a, b)
        | Expression::SameTerm(a, b)
        | Expression::Greater(a, b)
        | Expression::GreaterOrEqual(a, b)
        | Expression::Less(a, b)
        | Expression::LessOrEqual(a, b)
        | Expression::Add(a, b)
        | Expression::Subtract(a, b)
        | Expression::Multiply(a, b)
        | Expression::Divide(a, b) => {
            substitute_expr(a, var, value);
            substitute_expr(b, var, value);
        }
        Expression::In(a, list) => {
            substitute_expr(a, var, value);
            for e in list {
                substitute_expr(e, var, value);
            }
        }
        Expression::UnaryPlus(a) | Expression::UnaryMinus(a) | Expression::Not(a) => {
            substitute_expr(a, var, value)
        }
        Expression::Exists(pattern) => substitute_pattern(pattern, var, value),
        Expression::If(a, b, c) => {
            substitute_expr(a, var, value);
            substitute_expr(b, var, value);
            substitute_expr(c, var, value);
        }
        Expression::Coalesce(list) | Expression::FunctionCall(_, list) => {
            for e in list {
                substitute_expr(e, var, value);
            }
        }
    }
}

fn term_to_term_pattern(value: &Term) -> Option<TermPattern> {
    match value {
        Term::NamedNode(n) => Some(TermPattern::NamedNode(n.clone())),
        Term::BlankNode(b) => Some(TermPattern::BlankNode(b.clone())),
        Term::Literal(l) => Some(TermPattern::Literal(l.clone())),
        // rdf-star triple terms are not pre-bound by SHACL.
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

fn term_to_expr(value: &Term) -> Option<Expression> {
    match value {
        Term::NamedNode(n) => Some(Expression::NamedNode(n.clone())),
        Term::Literal(l) => Some(Expression::Literal(l.clone())),
        // Blank-node / triple terms cannot appear as SPARQL expression constants.
        _ => None,
    }
}

fn variable(name: &str) -> Variable {
    Variable::new(name).expect("static SPARQL variable name")
}

fn err(error: impl std::fmt::Display) -> String {
    error.to_string()
}
