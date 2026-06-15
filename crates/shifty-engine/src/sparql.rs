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
use shifty_opt::{NativeQueryPlan, QueryForm, lower_query_with_stats};
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
    /// Per-focus violations precomputed in one batched fallback run (doc §189):
    /// the SELECT analog of the global CONSTRUCT batching. Populated by
    /// [`SparqlExecutor::prefetch_constraint`]; `None` until then. Only the
    /// per-focus Spareval fallback is batched here — native plans already
    /// evaluate a focus batch efficiently and keep the debug differential gate.
    batched: RefCell<Option<BatchedResults>>,
}

/// Results of a batched fallback run. `covered` is the focus set the single
/// query actually ranged over (named-node foci only — blank-node foci cannot be
/// matched back from relabeled query results); a focus outside it falls through
/// to per-focus execution.
struct BatchedResults {
    covered: HashSet<Term>,
    map: HashMap<Term, Vec<SparqlViolation>>,
}

struct CompiledConstruct {
    plan: Option<NativeQueryPlan>,
    template: Vec<TriplePattern>,
}

#[derive(Debug, Clone)]
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

        // A prior `prefetch_constraint` may have evaluated this focus in one
        // batched fallback run (doc §189). Serve from that map when the focus is
        // covered; uncovered foci (e.g. blank nodes) fall through to live exec.
        if let Some(batched) = compiled.batched.borrow().as_ref()
            && batched.covered.contains(focus)
        {
            let violations = batched.map.get(focus).cloned().unwrap_or_default();
            profile::record(
                &fp,
                start.elapsed().as_micros() as u64,
                profile::ExecutorKind::Fallback { reason: None },
            );
            return Ok(violations);
        }

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
            Some(frozen) => lower_query_with_stats(&query, Some(&frozen.plan_stats())).ok(),
            None => None,
        };
        let compiled = Rc::new(Compiled {
            plan,
            query,
            batched: RefCell::new(None),
        });
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

    /// Evaluate a fallback (non-native) constraint over a whole focus set in one
    /// `VALUES`-injected query, caching the per-focus violations on the compiled
    /// entry (doc §189). Subsequent [`Self::constraint_violations`] calls for any
    /// covered focus are served from the cache instead of re-running Spareval
    /// once per node. A no-op when the constraint lowered to a native plan
    /// (already batched), when fewer than two named-node foci are supplied, or
    /// when the query is outside the safe-to-batch subset.
    pub fn prefetch_constraint(
        &self,
        constraint: &SparqlConstraint,
        foci: &[Term],
    ) -> Result<(), String> {
        let compiled = self.compile_constraint(constraint)?;
        if compiled.plan.is_some() || compiled.batched.borrow().is_some() {
            return Ok(());
        }
        if foci.len() < 2 {
            return Ok(());
        }
        let fp = profile::fingerprint(&constraint.query);
        let start = std::time::Instant::now();
        if let Some(batched) = self.run_fallback_batch(&compiled.query, foci)? {
            *compiled.batched.borrow_mut() = Some(batched);
            profile::record(
                &fp,
                start.elapsed().as_micros() as u64,
                profile::ExecutorKind::Fallback { reason: None },
            );
        }
        Ok(())
    }

    /// Evaluate a constraint over a whole focus set in one free-`?this` run and
    /// bucket the solutions by `?this` (see [`plan_batch_query`]). Returns the
    /// covered named foci alongside their violations, or `None` when the query is
    /// outside the safe-batch subset.
    fn run_fallback_batch(
        &self,
        query: &Query,
        foci: &[Term],
    ) -> Result<Option<BatchedResults>, String> {
        let Some((batched, covered)) = plan_batch_query(query, foci) else {
            return Ok(None);
        };
        let covered: HashSet<Term> = covered.into_iter().collect();
        let prepared = SparqlEvaluator::new().for_query(batched);
        let query_result = if let Some(frozen) = &self.frozen {
            prepared
                .on_queryable_dataset(frozen)
                .execute()
                .map_err(err)?
        } else {
            prepared.on_store(self.store()?).execute().map_err(err)?
        };
        let QueryResults::Solutions(solutions) = query_result else {
            return Ok(None);
        };
        let vars: Vec<String> = solutions
            .variables()
            .iter()
            .map(|v| v.as_str().to_string())
            .collect();
        let mut map: HashMap<Term, Vec<SparqlViolation>> = HashMap::new();
        for solution in solutions {
            let solution = solution.map_err(err)?;
            let Some(this) = solution.get("this") else {
                continue;
            };
            // The free-`?this` strategy enumerates every matching `?this`, not
            // just our foci; keep only solutions for foci we were asked about.
            if !covered.contains(this) {
                continue;
            }
            // Mirror the per-focus binding set: `$this` is substituted away on the
            // per-focus path, so it must not appear as a violation binding here
            // (it would otherwise leak into `{?this}` message templates).
            let bindings = vars
                .iter()
                .filter(|name| name.as_str() != "this")
                .filter_map(|name| {
                    solution
                        .get(name.as_str())
                        .map(|t| (name.clone(), t.clone()))
                })
                .collect();
            map.entry(this.clone()).or_default().push(SparqlViolation {
                value: solution.get("value").cloned(),
                path: solution.get("path").cloned(),
                message: solution.get("message").cloned(),
                bindings,
            });
        }
        Ok(Some(BatchedResults { covered, map }))
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
            // Named-node foci: pre-bind ?this via algebra substitution before
            // evaluation, so Oxigraph can use subject–predicate index lookups.
            for focus in foci.iter().filter(|f| !matches!(f, Term::BlankNode(_))) {
                triples.extend(self.construct_one(query, focus)?);
            }

            // Blank-node foci: sparopt converts TermPattern::BlankNode to a
            // fresh query variable rather than a constant, so per-focus
            // substitution degrades to a full predicate scan. Run the CONSTRUCT
            // once with ?this free and filter the results by blank-node subject
            // identity instead.
            let blank_foci: Vec<&Term> = foci
                .iter()
                .filter(|f| matches!(f, Term::BlankNode(_)))
                .collect();
            if !blank_foci.is_empty() {
                let foci_set: HashSet<Term> = blank_foci.iter().map(|&f| f.clone()).collect();
                let store = self.store()?;
                let raw_triples: Vec<Triple> = match SparqlEvaluator::new()
                    .for_query(self.parse(query)?)
                    .on_store(store)
                    .execute()
                    .map_err(err)?
                {
                    QueryResults::Graph(iter) => iter
                        .filter_map(|t| t.ok())
                        .filter(|t| foci_set.contains(&Term::from(t.subject.clone())))
                        .collect(),
                    _ => return Err("SPARQL rule did not produce CONSTRUCT graph results".into()),
                };
                triples.extend(raw_triples);
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
            let stats = self.frozen.as_ref().map(|f| f.plan_stats());
            lower_query_with_stats(
                &Query::Select {
                    dataset: None,
                    pattern,
                    base_iri,
                },
                stats.as_ref(),
            )
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

    /// Invoke a `sh:SPARQLFunction` by substituting positional parameter
    /// bindings into the query and extracting `?result` terms.
    pub fn call_sparql_function(
        &self,
        raw_query: &str,
        params: &[(String, Term)],
    ) -> Result<Vec<Term>, String> {
        let mut query = self.parse(raw_query)?;
        for (name, value) in params {
            let var = Variable::new(name).map_err(err)?;
            substitute_query(&mut query, &var, value);
        }
        let prepared = SparqlEvaluator::new().for_query(query);
        let result = if let Some(frozen) = &self.frozen {
            prepared
                .on_queryable_dataset(frozen)
                .execute()
                .map_err(err)?
        } else {
            prepared.on_store(self.store()?).execute().map_err(err)?
        };
        match result {
            QueryResults::Solutions(solutions) => {
                let mut out = Vec::new();
                for sol in solutions {
                    if let Some(t) = sol.map_err(err)?.get("result") {
                        out.push(t.clone());
                    }
                }
                Ok(out)
            }
            QueryResults::Boolean(b) => Ok(if b {
                vec![Term::Literal(Literal::from(true))]
            } else {
                vec![]
            }),
            QueryResults::Graph(_) => Err("sh:SPARQLFunction produced graph results".to_string()),
        }
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

/// Rewrite a constraint query so one run covers a whole focus set, returning the
/// query and the named foci it covers.
///
/// The strategy is **free `?this`**: it applies only when `?this` is positively
/// bound by the *required* part of the WHERE clause (a non-optional,
/// non-negated triple or path). The natural join then already constrains
/// `?this`, so the query runs unmodified (just ensuring `?this` is projected)
/// and the foci are recovered from the solutions. This replaces N per-focus
/// Spareval runs with a single scan that a selective binding pattern drives.
///
/// Coverage is restricted to **named-node** foci. A blank node cannot be matched
/// back from query results — SPARQL relabels result blank nodes, so a solution's
/// `?this` never equals the focus blank node. Blank-node foci stay on the
/// per-focus path, where `$this` is substituted as a constant and identity is
/// preserved.
///
/// `None` (keep the per-focus path) when batching would be unsound or unhelpful:
/// `?this` is not positively bound (a pure `NOT EXISTS`/`FILTER` body, where a
/// `VALUES` table would just re-do the per-focus work), aggregation (`GROUP BY`),
/// result slicing (`LIMIT`/`OFFSET`), or fewer than two named foci. `ASK` becomes
/// `SELECT DISTINCT ?this`.
fn plan_batch_query(query: &Query, foci: &[Term]) -> Option<(Query, Vec<Term>)> {
    let this = Variable::new("this").ok()?;
    let (pattern, is_ask) = match query {
        Query::Select { pattern, .. } => (pattern, false),
        Query::Ask { pattern, .. } => (pattern, true),
        _ => return None,
    };
    if pattern_has_group_or_slice(pattern) || !this_required(pattern, &this) {
        return None;
    }

    let covered: Vec<Term> = foci
        .iter()
        .filter(|f| matches!(f, Term::NamedNode(_)))
        .cloned()
        .collect();
    if covered.len() < 2 {
        return None;
    }

    let (dataset, base_iri) = match query {
        Query::Select {
            dataset, base_iri, ..
        }
        | Query::Ask {
            dataset, base_iri, ..
        } => (dataset.clone(), base_iri.clone()),
        _ => unreachable!(),
    };

    let rewritten = if is_ask {
        // ASK conformance is existential: DISTINCT collapses each focus to at
        // most one row, matching the single Boolean violation the per-focus path
        // emits for a true ASK.
        Query::Select {
            dataset,
            pattern: GraphPattern::Project {
                inner: Box::new(GraphPattern::Distinct {
                    inner: Box::new(pattern.clone()),
                }),
                variables: vec![this],
            },
            base_iri,
        }
    } else {
        Query::Select {
            dataset,
            pattern: ensure_projected(pattern.clone(), &this)?,
            base_iri,
        }
    };
    Some((rewritten, covered))
}

/// Whether `var` is positively bound by the *required* part of the pattern —
/// the subject or object of a triple or path that is not under `OPTIONAL`
/// (`LeftJoin` right arm), `MINUS`, `FILTER`, or `EXISTS`. If so, running the
/// query with `var` free still constrains it to those bindings, so the focus
/// domain need not be supplied via `VALUES`. A `UNION` binds it only if *both*
/// arms do.
fn this_required(p: &GraphPattern, var: &Variable) -> bool {
    match p {
        GraphPattern::Bgp { patterns } => patterns
            .iter()
            .any(|t| term_pattern_is(&t.subject, var) || term_pattern_is(&t.object, var)),
        GraphPattern::Path {
            subject, object, ..
        } => term_pattern_is(subject, var) || term_pattern_is(object, var),
        GraphPattern::Join { left, right } | GraphPattern::Lateral { left, right } => {
            this_required(left, var) || this_required(right, var)
        }
        GraphPattern::Union { left, right } => {
            this_required(left, var) && this_required(right, var)
        }
        // Only the mandatory (left) side of OPTIONAL / MINUS binds `var`.
        GraphPattern::LeftJoin { left, .. } | GraphPattern::Minus { left, .. } => {
            this_required(left, var)
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Service { inner, .. } => this_required(inner, var),
        GraphPattern::Values {
            variables,
            bindings,
        } => {
            // A VALUES table binds `var` only if every row supplies it.
            variables.iter().position(|v| v == var).is_some_and(|col| {
                bindings
                    .iter()
                    .all(|row| row.get(col).is_some_and(Option::is_some))
            })
        }
    }
}

/// Descend through the solution modifiers (`DISTINCT`/`REDUCED`/`ORDER BY`) to
/// the projection and add `?this` to its variables if absent, so the free run's
/// solutions expose `?this` for demultiplexing. `None` if no projection is found
/// (e.g. a bare `SELECT *` without a `Project` node, which should not occur for
/// parsed constraints).
fn ensure_projected(pattern: GraphPattern, this: &Variable) -> Option<GraphPattern> {
    match pattern {
        GraphPattern::Project {
            inner,
            mut variables,
        } => {
            if !variables.iter().any(|v| v == this) {
                variables.push(this.clone());
            }
            Some(GraphPattern::Project { inner, variables })
        }
        GraphPattern::Distinct { inner } => Some(GraphPattern::Distinct {
            inner: Box::new(ensure_projected(*inner, this)?),
        }),
        GraphPattern::Reduced { inner } => Some(GraphPattern::Reduced {
            inner: Box::new(ensure_projected(*inner, this)?),
        }),
        GraphPattern::OrderBy { inner, expression } => Some(GraphPattern::OrderBy {
            inner: Box::new(ensure_projected(*inner, this)?),
            expression,
        }),
        _ => None,
    }
}

/// Whether the pattern (anywhere, including correlated sub-queries) contains an
/// aggregation or a slice, either of which makes per-focus batching unsound.
fn pattern_has_group_or_slice(p: &GraphPattern) -> bool {
    match p {
        GraphPattern::Group { .. } | GraphPattern::Slice { .. } => true,
        GraphPattern::Bgp { .. } | GraphPattern::Path { .. } | GraphPattern::Values { .. } => false,
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right }
        | GraphPattern::Lateral { left, right }
        | GraphPattern::LeftJoin { left, right, .. } => {
            pattern_has_group_or_slice(left) || pattern_has_group_or_slice(right)
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Service { inner, .. } => pattern_has_group_or_slice(inner),
    }
}

fn term_pattern_is(t: &TermPattern, var: &Variable) -> bool {
    matches!(t, TermPattern::Variable(v) if v == var)
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

#[cfg(test)]
mod batch_tests {
    use super::*;
    use shifty_algebra::SparqlQueryKind;

    fn nn(s: &str) -> NamedNode {
        NamedNode::new(s).unwrap()
    }

    fn subject(s: &str) -> Term {
        Term::NamedNode(nn(&format!("http://ex/{s}")))
    }

    /// Three subjects with `ex:value` 5/15/25 and no `ex:other`.
    fn sample_graph() -> Graph {
        let mut g = Graph::new();
        for (s, v) in [("a", 5i64), ("b", 15), ("c", 25)] {
            g.insert(&Triple::new(
                nn(&format!("http://ex/{s}")),
                nn("http://ex/value"),
                Literal::from(v),
            ));
        }
        g
    }

    fn foci() -> Vec<Term> {
        vec![subject("a"), subject("b"), subject("c")]
    }

    /// Per-focus violation keys `(focus, value)` over the whole focus set.
    fn per_focus(exec: &SparqlExecutor, c: &SparqlConstraint, foci: &[Term]) -> Vec<String> {
        let mut out = Vec::new();
        for f in foci {
            for v in exec.constraint_violations(c, f).unwrap() {
                out.push(format!("{f:?}|{:?}", v.value));
            }
        }
        out.sort();
        out
    }

    #[test]
    fn batched_select_matches_per_focus() {
        // OPTIONAL forces the non-native fallback path, where VALUES batching
        // applies. Every subject violates (no ex:other), projecting its value.
        let constraint = SparqlConstraint {
            kind: SparqlQueryKind::Select,
            query: "SELECT $this ?value WHERE { \
                $this <http://ex/value> ?value . \
                OPTIONAL { $this <http://ex/other> ?o } \
                FILTER(!bound(?o)) }"
                .to_string(),
            path: None,
            shape: None,
            messages: Vec::new(),
        };
        let g = sample_graph();
        let foci = foci();

        let baseline = SparqlExecutor::from_frozen(FrozenIndexedDataset::from_graph(&g), false);
        let want = per_focus(&baseline, &constraint, &foci);

        let batched = SparqlExecutor::from_frozen(FrozenIndexedDataset::from_graph(&g), false);
        batched.prefetch_constraint(&constraint, &foci).unwrap();
        let got = per_focus(&batched, &constraint, &foci);

        assert_eq!(want.len(), 3, "every subject should violate");
        assert_eq!(want, got);
    }

    #[test]
    fn batched_ask_matches_per_focus() {
        // ASK is true iff value > 10: only b and c violate.
        let constraint = SparqlConstraint {
            kind: SparqlQueryKind::Ask,
            query: "ASK { $this <http://ex/value> ?value . \
                OPTIONAL { $this <http://ex/other> ?o } \
                FILTER(?value > 10) }"
                .to_string(),
            path: None,
            shape: None,
            messages: Vec::new(),
        };
        let g = sample_graph();
        let foci = foci();

        let baseline = SparqlExecutor::from_frozen(FrozenIndexedDataset::from_graph(&g), false);
        let want: Vec<usize> = foci
            .iter()
            .map(|f| {
                baseline
                    .constraint_violations(&constraint, f)
                    .unwrap()
                    .len()
            })
            .collect();

        let batched = SparqlExecutor::from_frozen(FrozenIndexedDataset::from_graph(&g), false);
        batched.prefetch_constraint(&constraint, &foci).unwrap();
        let got: Vec<usize> = foci
            .iter()
            .map(|f| batched.constraint_violations(&constraint, f).unwrap().len())
            .collect();

        assert_eq!(want, vec![0, 1, 1]);
        assert_eq!(want, got);
    }

    #[test]
    fn blank_node_foci_fall_through_to_per_focus() {
        // Regression: the free-`?this` run cannot match blank-node foci back from
        // its (relabeled) solutions, so blank foci must stay on the per-focus
        // path. Mixed named/blank focus set; result must match per-focus exactly.
        use oxrdf::BlankNode;
        let mut g = Graph::new();
        let blank = BlankNode::default();
        // ex:a and the blank node are both "flagged"; ex:c is not.
        g.insert(&Triple::new(
            nn("http://ex/a"),
            nn("http://ex/flag"),
            Literal::from(true),
        ));
        g.insert(&Triple::new(
            blank.clone(),
            nn("http://ex/flag"),
            Literal::from(true),
        ));
        // referrers: ex:x -> ex:a, ex:y -> blank
        g.insert(&Triple::new(
            nn("http://ex/x"),
            nn("http://ex/ref"),
            nn("http://ex/a"),
        ));
        g.insert(&Triple::new(
            nn("http://ex/y"),
            nn("http://ex/ref"),
            blank.clone(),
        ));

        // `?this` is positively bound (subject of `?this ex:flag true`), so the
        // free strategy applies; OPTIONAL forces the non-native fallback.
        let constraint = SparqlConstraint {
            kind: SparqlQueryKind::Select,
            query: "SELECT ?s $this WHERE { \
                $this <http://ex/flag> true . \
                ?s <http://ex/ref> $this . \
                OPTIONAL { $this <http://ex/other> ?o } }"
                .to_string(),
            path: None,
            shape: None,
            messages: Vec::new(),
        };
        let foci = vec![
            Term::NamedNode(nn("http://ex/a")),
            Term::BlankNode(blank),
            subject("c"),
        ];

        let baseline = SparqlExecutor::from_frozen(FrozenIndexedDataset::from_graph(&g), false);
        let want = per_focus(&baseline, &constraint, &foci);

        let batched = SparqlExecutor::from_frozen(FrozenIndexedDataset::from_graph(&g), false);
        batched.prefetch_constraint(&constraint, &foci).unwrap();
        let got = per_focus(&batched, &constraint, &foci);

        // The named focus is served from the batch; the blank focus falls
        // through to the per-focus path. Either way the batched result must
        // equal the per-focus oracle exactly.
        assert!(!want.is_empty());
        assert_eq!(want, got);
    }

    #[test]
    fn aggregating_query_is_not_batched_but_still_correct() {
        // GROUP BY / COUNT is outside the safe-batch subset: build_batch_query
        // returns None, so prefetch is a no-op and per-focus results are used.
        let constraint = SparqlConstraint {
            kind: SparqlQueryKind::Select,
            query: "SELECT $this (COUNT(?value) AS ?n) WHERE { \
                $this <http://ex/value> ?value } \
                GROUP BY $this HAVING (COUNT(?value) > 5)"
                .to_string(),
            path: None,
            shape: None,
            messages: Vec::new(),
        };
        let g = sample_graph();
        let foci = foci();

        let baseline = SparqlExecutor::from_frozen(FrozenIndexedDataset::from_graph(&g), false);
        let want = per_focus(&baseline, &constraint, &foci);

        let batched = SparqlExecutor::from_frozen(FrozenIndexedDataset::from_graph(&g), false);
        batched.prefetch_constraint(&constraint, &foci).unwrap();
        let got = per_focus(&batched, &constraint, &foci);

        assert_eq!(want, got); // none violate; importantly, no panic / divergence
    }
}
