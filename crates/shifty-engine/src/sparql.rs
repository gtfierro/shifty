//! Shared SPARQL execution over an Oxigraph default graph.
//!
//! Queries are parsed and canonicalized by `shifty-parse`. This layer caches
//! Oxigraph's prepared form, applies SHACL prebindings, and executes against a
//! store that is kept in sync with rule inference.

use crate::frozen::{FrozenIndexedDataset, TermId};
use crate::native_exec;
use crate::profile;
use oxigraph::sparql::{PreparedSparqlQuery, QueryResults, SparqlEvaluator};
use oxigraph::store::Store;
use oxrdf::{
    Graph, GraphName, GraphNameRef, Literal, NamedNode, NamedOrBlankNode, Quad, QuadRef, Term,
    Triple, Variable,
};
use shifty_algebra::{Path, SparqlConstraint};
use shifty_opt::{NativeQueryPlan, QueryForm, lower_query};
use spargebra::algebra::{
    AggregateExpression, Expression, GraphPattern, OrderExpression, PropertyPathExpression,
};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::{Query, SparqlParser};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// The named graph the shapes graph is loaded under so `GRAPH $shapesGraph {…}`
/// can be evaluated; `$shapesGraph` is pre-bound to this IRI.
pub(crate) use crate::frozen::SHAPES_GRAPH_IRI;

pub(crate) struct SparqlExecutor {
    /// Mutable store used by inference. Validation evaluates directly over
    /// `frozen`, so it does not allocate this second indexed representation.
    store: Option<Store>,
    /// Frozen dataset used for constraint/target SPARQL during validation. `None`
    /// during inference (where the store mutates between rule firings).
    frozen: Option<FrozenIndexedDataset>,
    prepared: RefCell<HashMap<String, PreparedSparqlQuery>>,
    parsed: RefCell<HashMap<String, Query>>,
    /// Per-constraint compilation cache (doc §72): static SHACL substitutions
    /// applied once, then either a native plan or a fallback query. Keyed by the
    /// constraint's query plus its static bindings (path / shape).
    compiled: RefCell<HashMap<CompileKey, Rc<Compiled>>>,
    /// Native plans for SPARQL rule WHERE clauses, keyed by canonical query.
    constructs: RefCell<HashMap<String, Rc<CompiledConstruct>>>,
    /// IRI of the loaded shapes graph, pre-bound to `$shapesGraph`. `None` when
    /// no shapes graph was loaded (so `$shapesGraph` is left unsupported).
    shapes_graph: Option<NamedNode>,
}

/// Cache key for a compiled constraint: the canonical query plus the static
/// bindings that change its plan. `$this` is bound per focus and so is *not*
/// part of the key (one plan serves every focus node).
#[derive(PartialEq, Eq, Hash)]
struct CompileKey {
    query: String,
    path: Option<String>,
    shape: Option<String>,
}

/// A compiled constraint. `query` is the statically substituted Spargebra query
/// with `$this` left free — used for fallback execution and, in debug builds, as
/// the differential oracle. `plan` is present when the query lowered to the
/// native subset.
struct Compiled {
    plan: Option<NativeQueryPlan>,
    query: Query,
}

struct CompiledConstruct {
    plan: Option<NativeQueryPlan>,
    template: Vec<TriplePattern>,
}

#[derive(Debug)]
pub(crate) struct SparqlViolation {
    pub value: Option<Term>,
    pub path: Option<Term>,
    /// The `?message` solution binding (SHACL §5.2.1) — a per-result, human
    /// authored explanation. `None` for `ASK` constraints and `SELECT` queries
    /// that do not project `?message`.
    pub message: Option<Term>,
    /// All projected solution bindings, keyed by variable name (without `?`).
    /// Used to substitute `{?varName}` placeholders in `sh:message` templates.
    pub bindings: HashMap<String, Term>,
}

impl SparqlExecutor {
    pub fn new(graph: &Graph) -> Result<Self, String> {
        Self::build(graph, None)
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
            store: Some(store),
            frozen: None,
            prepared: RefCell::new(HashMap::new()),
            parsed: RefCell::new(HashMap::new()),
            compiled: RefCell::new(HashMap::new()),
            constructs: RefCell::new(HashMap::new()),
            shapes_graph,
        })
    }

    pub fn from_frozen(frozen: FrozenIndexedDataset, has_shapes_graph: bool) -> Self {
        Self {
            store: None,
            frozen: Some(frozen),
            prepared: RefCell::new(HashMap::new()),
            parsed: RefCell::new(HashMap::new()),
            compiled: RefCell::new(HashMap::new()),
            constructs: RefCell::new(HashMap::new()),
            shapes_graph: has_shapes_graph
                .then(|| NamedNode::new(SHAPES_GRAPH_IRI).expect("static IRI is valid")),
        }
    }

    /// The attached frozen snapshot, if any. Validation uses it as the indexed
    /// `PathBackend` for `sh:path` traversal; inference leaves it `None` and
    /// falls back to its mutable graph.
    pub(crate) fn frozen(&self) -> Option<&FrozenIndexedDataset> {
        self.frozen.as_ref()
    }

    pub fn insert(&self, triple: &Triple) -> Result<(), String> {
        self.store()?
            .insert(QuadRef::new(
                triple.subject.as_ref(),
                triple.predicate.as_ref(),
                triple.object.as_ref(),
                GraphNameRef::DefaultGraph,
            ))
            .map_err(|e| e.to_string())
    }

    pub fn target_nodes(&self, query: &str) -> Result<Vec<Term>, String> {
        let fp = profile::fingerprint(query);
        let start = std::time::Instant::now();
        let results = if let Some(frozen) = &self.frozen {
            self.prepared(query)?
                .on_queryable_dataset(frozen)
                .execute()
                .map_err(err)?
        } else {
            self.prepared(query)?
                .on_store(self.store()?)
                .execute()
                .map_err(err)?
        };
        let QueryResults::Solutions(solutions) = results else {
            return Err("SPARQL target did not produce SELECT solutions".to_string());
        };
        let mut nodes = Vec::new();
        for solution in solutions {
            if let Some(node) = solution.map_err(err)?.get("this") {
                nodes.push(node.clone());
            }
        }
        profile::record(
            &fp,
            start.elapsed().as_micros() as u64,
            profile::ExecutorKind::Fallback { reason: None },
        );
        Ok(nodes)
    }

    pub fn constraint_violations(
        &self,
        constraint: &SparqlConstraint,
        focus: &Term,
    ) -> Result<Vec<SparqlViolation>, String> {
        let compiled = self.compile_constraint(constraint)?;
        let fp = profile::fingerprint(&constraint.query);
        let start = std::time::Instant::now();

        if let Some(plan) = &compiled.plan {
            let violations = self.run_native(plan, focus);
            // Differential gate (doc §254): in debug builds, every natively
            // executed query is re-run through Spareval over the same frozen
            // dataset and the two violation multisets must match exactly.
            #[cfg(debug_assertions)]
            {
                let reference = self.run_fallback(&compiled.query, focus)?;
                assert_violations_match(&constraint.query, focus, &violations, &reference);
            }
            profile::record(
                &fp,
                start.elapsed().as_micros() as u64,
                profile::ExecutorKind::Native,
            );
            Ok(violations)
        } else {
            let result = self.run_fallback(&compiled.query, focus)?;
            profile::record(
                &fp,
                start.elapsed().as_micros() as u64,
                profile::ExecutorKind::Fallback { reason: None },
            );
            Ok(result)
        }
    }

    /// Apply the static SHACL substitutions to a constraint's query and, when a
    /// frozen dataset is available, try to lower it to the native subset. The
    /// result is cached: `$this` is bound per focus, so one compiled entry serves
    /// every focus node (doc §72).
    fn compile_constraint(&self, constraint: &SparqlConstraint) -> Result<Rc<Compiled>, String> {
        let key = CompileKey {
            query: constraint.query.clone(),
            path: constraint.path.as_ref().map(path_key),
            shape: constraint.shape.as_ref().map(|s| s.to_string()),
        };
        if let Some(compiled) = self.compiled.borrow().get(&key) {
            return Ok(compiled.clone());
        }

        // SHACL pre-binding is *substitution* throughout the query (SHACL
        // §5.2.1), not an initial binding. `$this` is left free here — the native
        // executor binds it per focus, and the fallback substitutes it per call.
        let mut query = self.parse(&constraint.query)?;
        if let Some(path) = &constraint.path {
            match path {
                Path::Pred(predicate) => substitute_query(
                    &mut query,
                    &variable("PATH"),
                    &Term::NamedNode(predicate.clone()),
                ),
                complex => {
                    let sparql_path = path_to_property_path(complex).ok_or_else(|| {
                        format!(
                            "SHACL path cannot be expressed as a SPARQL property path: {complex:?}"
                        )
                    })?;
                    query = rewrite_path_query(query, &sparql_path);
                }
            }
        }
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

        // Native execution requires the frozen dataset as its storage backend.
        let plan = match &self.frozen {
            Some(_) => lower_query(&query).ok(),
            None => None,
        };
        let compiled = Rc::new(Compiled { plan, query });
        self.compiled.borrow_mut().insert(key, compiled.clone());
        Ok(compiled)
    }

    /// Execute a native plan for one focus node, mapping its solutions to SHACL
    /// violations.
    fn run_native(&self, plan: &NativeQueryPlan, focus: &Term) -> Vec<SparqlViolation> {
        let frozen = self
            .frozen
            .as_ref()
            .expect("native plan implies a frozen dataset (compile_constraint guards this)");
        let foci = [focus.clone()];
        let result = native_exec::execute(plan, frozen, &foci);
        let solutions = &result.solutions[0];
        match result.form {
            // ASK: any solution is a violation, matching the fallback's Boolean.
            QueryForm::Ask => {
                if solutions.is_empty() {
                    Vec::new()
                } else {
                    vec![SparqlViolation {
                        value: None,
                        path: None,
                        message: None,
                        bindings: HashMap::new(),
                    }]
                }
            }
            QueryForm::Select => solutions
                .iter()
                .map(|b| SparqlViolation {
                    value: b.get("value").cloned(),
                    path: b.get("path").cloned(),
                    message: b.get("message").cloned(),
                    bindings: b.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                })
                .collect(),
        }
    }

    /// Execute a statically substituted query through Spareval (over the frozen
    /// dataset when present, else the Store), substituting `$this` for this call.
    fn run_fallback(&self, query: &Query, focus: &Term) -> Result<Vec<SparqlViolation>, String> {
        let mut query = query.clone();
        substitute_query(&mut query, &variable("this"), focus);
        let prepared = SparqlEvaluator::new().for_query(query);
        let query_result = if let Some(frozen) = &self.frozen {
            prepared
                .on_queryable_dataset(frozen)
                .execute()
                .map_err(err)?
        } else {
            prepared.on_store(self.store()?).execute().map_err(err)?
        };
        match query_result {
            QueryResults::Solutions(solutions) => {
                let vars: Vec<String> = solutions
                    .variables()
                    .iter()
                    .map(|v| v.as_str().to_string())
                    .collect();
                solutions
                    .map(|solution| {
                        let solution = solution.map_err(err)?;
                        let bindings = vars
                            .iter()
                            .filter_map(|name| {
                                solution
                                    .get(name.as_str())
                                    .map(|t| (name.clone(), t.clone()))
                            })
                            .collect();
                        Ok(SparqlViolation {
                            value: solution.get("value").cloned(),
                            path: solution.get("path").cloned(),
                            message: solution.get("message").cloned(),
                            bindings,
                        })
                    })
                    .collect()
            }
            QueryResults::Boolean(violates) => Ok(if violates {
                vec![SparqlViolation {
                    value: None,
                    path: None,
                    message: None,
                    bindings: HashMap::new(),
                }]
            } else {
                Vec::new()
            }),
            QueryResults::Graph(_) => {
                Err("SPARQL constraint unexpectedly produced graph results".to_string())
            }
        }
    }

    /// Execute a SPARQL CONSTRUCT rule for multiple focus nodes.
    pub fn construct_many(
        &self,
        query: &str,
        foci: &[Term],
        frozen: Option<&FrozenIndexedDataset>,
    ) -> Result<Vec<Triple>, String> {
        const BATCH_SIZE: usize = 2048;

        if foci.is_empty() {
            return Ok(Vec::new());
        }

        let fp = profile::fingerprint(query);
        let start = std::time::Instant::now();
        let mut triples = Vec::new();
        let compiled = self.compile_construct(query)?;
        let executor = if let (Some(plan), Some(frozen)) = (&compiled.plan, frozen) {
            for chunk in foci.chunks(BATCH_SIZE) {
                let result = native_exec::execute_ids(plan, frozen, chunk);
                for (focus_index, solutions) in result.solutions.into_iter().enumerate() {
                    for bindings in solutions {
                        instantiate_template(
                            &compiled.template,
                            plan,
                            frozen,
                            &chunk[focus_index],
                            &bindings,
                            &mut triples,
                        );
                    }
                }
            }
            profile::ExecutorKind::Native
        } else {
            for focus in foci {
                triples.extend(self.construct_one(query, focus)?);
            }
            let store = self.store()?;
            triples.retain(|triple| {
                !store
                    .contains(QuadRef::new(
                        triple.subject.as_ref(),
                        triple.predicate.as_ref(),
                        triple.object.as_ref(),
                        GraphNameRef::DefaultGraph,
                    ))
                    .unwrap_or(false)
            });
            profile::ExecutorKind::Fallback { reason: None }
        };

        profile::record(&fp, start.elapsed().as_micros() as u64, executor);
        Ok(triples)
    }

    /// Return focus nodes whose native CONSTRUCT WHERE solutions involve the
    /// supplied default-graph delta. `None` requests full-focus evaluation
    /// because this query is not in the supported differential subset.
    pub fn construct_delta_foci(
        &self,
        query: &str,
        delta: &[Triple],
        frozen: Option<&FrozenIndexedDataset>,
    ) -> Result<Option<HashSet<Term>>, String> {
        let compiled = self.compile_construct(query)?;
        let (Some(plan), Some(frozen)) = (&compiled.plan, frozen) else {
            return Ok(None);
        };
        Ok(
            native_exec::delta_focus_ids(plan, frozen, delta).map(|ids| {
                ids.into_iter()
                    .filter_map(|id| frozen.externalize(id))
                    .collect()
            }),
        )
    }

    fn compile_construct(&self, query: &str) -> Result<Rc<CompiledConstruct>, String> {
        if let Some(compiled) = self.constructs.borrow().get(query) {
            return Ok(compiled.clone());
        }
        let parsed = self.parse(query)?;
        let Query::Construct {
            template,
            dataset,
            pattern,
            base_iri,
        } = parsed
        else {
            return Err("SPARQL rule did not contain a CONSTRUCT query".to_string());
        };

        let plan = if dataset.is_none() && !template.iter().any(triple_has_blank_node) {
            lower_query(&Query::Select {
                dataset: None,
                pattern,
                base_iri,
            })
            .ok()
        } else {
            None
        };
        let compiled = Rc::new(CompiledConstruct { plan, template });
        self.constructs
            .borrow_mut()
            .insert(query.to_string(), compiled.clone());
        Ok(compiled)
    }

    fn construct_one(&self, query: &str, focus: &Term) -> Result<Vec<Triple>, String> {
        let mut query = self.parse(query)?;
        substitute_query(&mut query, &variable("this"), focus);
        let prepared = SparqlEvaluator::new().for_query(query);
        match prepared.on_store(self.store()?).execute().map_err(err)? {
            QueryResults::Graph(triples) => triples.map(|triple| triple.map_err(err)).collect(),
            _ => Err("SPARQL rule did not produce CONSTRUCT graph results".to_string()),
        }
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

    fn store(&self) -> Result<&Store, String> {
        self.store
            .as_ref()
            .ok_or_else(|| "mutable SPARQL store is unavailable during validation".to_string())
    }

    #[cfg(test)]
    fn has_store(&self) -> bool {
        self.store.is_some()
    }
}

#[cfg(test)]
mod storage_tests {
    use super::*;

    #[test]
    fn validation_executor_does_not_allocate_mutable_store() {
        let executor =
            SparqlExecutor::from_frozen(FrozenIndexedDataset::from_graph(&Graph::new()), false);
        assert!(!executor.has_store());
    }
}

fn triple_has_blank_node(triple: &TriplePattern) -> bool {
    matches!(triple.subject, TermPattern::BlankNode(_))
        || matches!(triple.object, TermPattern::BlankNode(_))
}

fn instantiate_template(
    template: &[TriplePattern],
    plan: &NativeQueryPlan,
    frozen: &FrozenIndexedDataset,
    focus: &Term,
    bindings: &native_exec::NativeIdBindings,
    out: &mut Vec<Triple>,
) {
    for triple in template {
        let Some(subject_id) = resolve_template_id(&triple.subject, plan, frozen, focus, bindings)
        else {
            continue;
        };
        let Some(predicate_id) =
            resolve_template_predicate_id(&triple.predicate, plan, frozen, focus, bindings)
        else {
            continue;
        };
        let Some(object_id) = resolve_template_id(&triple.object, plan, frozen, focus, bindings)
        else {
            continue;
        };
        if frozen.contains_ids(subject_id, predicate_id, object_id) {
            continue;
        }

        let Some(subject) = frozen.externalize(subject_id).and_then(|term| match term {
            Term::NamedNode(node) => Some(NamedOrBlankNode::NamedNode(node)),
            Term::BlankNode(node) => Some(NamedOrBlankNode::BlankNode(node)),
            Term::Literal(_) => None,
        }) else {
            continue;
        };
        let Some(Term::NamedNode(predicate)) = frozen.externalize(predicate_id) else {
            continue;
        };
        let Some(object) = frozen.externalize(object_id) else {
            continue;
        };
        out.push(Triple::new(subject, predicate, object));
    }
}

fn resolve_template_id(
    pattern: &TermPattern,
    plan: &NativeQueryPlan,
    frozen: &FrozenIndexedDataset,
    focus: &Term,
    bindings: &native_exec::NativeIdBindings,
) -> Option<TermId> {
    match pattern {
        TermPattern::NamedNode(node) => Some(frozen.intern(&Term::NamedNode(node.clone()))),
        TermPattern::BlankNode(node) => Some(frozen.intern(&Term::BlankNode(node.clone()))),
        TermPattern::Literal(literal) => Some(frozen.intern(&Term::Literal(literal.clone()))),
        TermPattern::Variable(variable) if variable.as_str() == "this" => {
            Some(frozen.intern(focus))
        }
        TermPattern::Variable(variable) => {
            let id = plan.var_id(variable.as_str())?;
            bindings.get(&id).copied()
        }
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

fn resolve_template_predicate_id(
    pattern: &NamedNodePattern,
    plan: &NativeQueryPlan,
    frozen: &FrozenIndexedDataset,
    focus: &Term,
    bindings: &native_exec::NativeIdBindings,
) -> Option<TermId> {
    match pattern {
        NamedNodePattern::NamedNode(node) => Some(frozen.intern(&Term::NamedNode(node.clone()))),
        NamedNodePattern::Variable(variable) => {
            if variable.as_str() == "this" {
                Some(frozen.intern(focus))
            } else {
                let id = plan.var_id(variable.as_str())?;
                bindings.get(&id).copied()
            }
        }
    }
}

/// Convert a SHACL `Path` to a Spargebra `PropertyPathExpression` for use in
/// SPARQL property-path patterns. Returns `None` for paths that have no SPARQL
/// equivalent (`Path::Id`, and empty `Alt`/`Seq`).
fn path_to_property_path(path: &Path) -> Option<PropertyPathExpression> {
    match path {
        Path::Id => None,
        Path::Pred(n) => Some(PropertyPathExpression::NamedNode(n.clone())),
        Path::Inverse(inner) => {
            path_to_property_path(inner).map(|p| PropertyPathExpression::Reverse(Box::new(p)))
        }
        Path::Seq(parts) => {
            let sparql: Vec<_> = parts
                .iter()
                .map(path_to_property_path)
                .collect::<Option<Vec<_>>>()?;
            sparql
                .into_iter()
                .reduce(|a, b| PropertyPathExpression::Sequence(Box::new(a), Box::new(b)))
        }
        Path::Alt(parts) => {
            // `zero_or_one(p)` expands to `Alt([p, Id])`. Strip `Id` elements and,
            // if any were present, wrap the remainder in `ZeroOrOne`.
            let has_id = parts.iter().any(|p| matches!(p, Path::Id));
            let sparql: Vec<_> = parts
                .iter()
                .filter(|p| !matches!(p, Path::Id))
                .map(path_to_property_path)
                .collect::<Option<Vec<_>>>()?;
            if sparql.is_empty() {
                return None;
            }
            let base = sparql
                .into_iter()
                .reduce(|a, b| PropertyPathExpression::Alternative(Box::new(a), Box::new(b)))?;
            if has_id {
                Some(PropertyPathExpression::ZeroOrOne(Box::new(base)))
            } else {
                Some(base)
            }
        }
        Path::Star(inner) => {
            path_to_property_path(inner).map(|p| PropertyPathExpression::ZeroOrMore(Box::new(p)))
        }
    }
}

/// Rewrite a query by replacing every BGP triple whose predicate is `?PATH`
/// with a `GraphPattern::Path` using `sparql_path`. This is the correct
/// treatment for SHACL `$PATH` pre-binding when the path is complex (non-pred).
fn rewrite_path_query(query: Query, path: &PropertyPathExpression) -> Query {
    match query {
        Query::Select {
            dataset,
            pattern,
            base_iri,
        } => Query::Select {
            dataset,
            pattern: rewrite_path_pattern(pattern, path),
            base_iri,
        },
        Query::Ask {
            dataset,
            pattern,
            base_iri,
        } => Query::Ask {
            dataset,
            pattern: rewrite_path_pattern(pattern, path),
            base_iri,
        },
        other => other,
    }
}

fn rewrite_path_pattern(pattern: GraphPattern, path: &PropertyPathExpression) -> GraphPattern {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            let mut result = GraphPattern::Bgp { patterns: vec![] };
            let mut remaining = Vec::new();
            for triple in patterns {
                if matches!(&triple.predicate, NamedNodePattern::Variable(v) if v.as_str() == "PATH")
                {
                    let path_gp = GraphPattern::Path {
                        subject: triple.subject,
                        path: path.clone(),
                        object: triple.object,
                    };
                    result = GraphPattern::Join {
                        left: Box::new(result),
                        right: Box::new(path_gp),
                    };
                } else {
                    remaining.push(triple);
                }
            }
            if remaining.is_empty() {
                result
            } else {
                let bgp = GraphPattern::Bgp {
                    patterns: remaining,
                };
                GraphPattern::Join {
                    left: Box::new(bgp),
                    right: Box::new(result),
                }
            }
        }
        // Path patterns don't contain $PATH in predicate position.
        GraphPattern::Path { .. } => pattern,
        GraphPattern::Join { left, right } => GraphPattern::Join {
            left: Box::new(rewrite_path_pattern(*left, path)),
            right: Box::new(rewrite_path_pattern(*right, path)),
        },
        GraphPattern::Union { left, right } => GraphPattern::Union {
            left: Box::new(rewrite_path_pattern(*left, path)),
            right: Box::new(rewrite_path_pattern(*right, path)),
        },
        GraphPattern::Minus { left, right } => GraphPattern::Minus {
            left: Box::new(rewrite_path_pattern(*left, path)),
            right: Box::new(rewrite_path_pattern(*right, path)),
        },
        GraphPattern::Lateral { left, right } => GraphPattern::Lateral {
            left: Box::new(rewrite_path_pattern(*left, path)),
            right: Box::new(rewrite_path_pattern(*right, path)),
        },
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => GraphPattern::LeftJoin {
            left: Box::new(rewrite_path_pattern(*left, path)),
            right: Box::new(rewrite_path_pattern(*right, path)),
            expression,
        },
        GraphPattern::Filter { expr, inner } => GraphPattern::Filter {
            expr,
            inner: Box::new(rewrite_path_pattern(*inner, path)),
        },
        GraphPattern::Graph { name, inner } => GraphPattern::Graph {
            name,
            inner: Box::new(rewrite_path_pattern(*inner, path)),
        },
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => GraphPattern::Extend {
            inner: Box::new(rewrite_path_pattern(*inner, path)),
            variable,
            expression,
        },
        GraphPattern::OrderBy { inner, expression } => GraphPattern::OrderBy {
            inner: Box::new(rewrite_path_pattern(*inner, path)),
            expression,
        },
        GraphPattern::Project { inner, variables } => GraphPattern::Project {
            inner: Box::new(rewrite_path_pattern(*inner, path)),
            variables,
        },
        GraphPattern::Distinct { inner } => GraphPattern::Distinct {
            inner: Box::new(rewrite_path_pattern(*inner, path)),
        },
        GraphPattern::Reduced { inner } => GraphPattern::Reduced {
            inner: Box::new(rewrite_path_pattern(*inner, path)),
        },
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => GraphPattern::Slice {
            inner: Box::new(rewrite_path_pattern(*inner, path)),
            start,
            length,
        },
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => GraphPattern::Group {
            inner: Box::new(rewrite_path_pattern(*inner, path)),
            variables,
            aggregates,
        },
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => GraphPattern::Service {
            name,
            inner: Box::new(rewrite_path_pattern(*inner, path)),
            silent,
        },
        GraphPattern::Values { .. } => pattern,
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

/// Canonical cache-key fragment for a constraint's `$PATH` binding.
fn path_key(path: &Path) -> String {
    shifty_algebra::render::path_to_string(path)
}

/// Assert the native executor and the Spareval fallback produce the same
/// violation multiset for a focus node. Debug-only differential gate.
#[cfg(debug_assertions)]
fn assert_violations_match(
    query: &str,
    focus: &Term,
    native: &[SparqlViolation],
    reference: &[SparqlViolation],
) {
    fn canonical(violations: &[SparqlViolation]) -> Vec<(String, String)> {
        let mut keyed: Vec<(String, String)> = violations
            .iter()
            .map(|v| {
                (
                    v.value.as_ref().map(Term::to_string).unwrap_or_default(),
                    v.path.as_ref().map(Term::to_string).unwrap_or_default(),
                )
            })
            .collect();
        keyed.sort();
        keyed
    }
    let native = canonical(native);
    let reference = canonical(reference);
    assert_eq!(
        native, reference,
        "native vs Spareval disagreement for focus {focus}\n  query: {query}\n  native:   {native:?}\n  spareval: {reference:?}",
    );
}
