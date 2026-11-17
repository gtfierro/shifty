//! One function environment for node expressions and nested SPARQL calls.
use super::*;
use crate::frozen::FunctionDataset;
use std::sync::Mutex;

const MAX_FUNCTION_DEPTH: usize = 32;

pub(super) struct FunctionRuntime {
    definitions: Vec<(FunctionDef, Result<Query, String>)>,
    dataset: Option<FunctionDataset>,
    store: Option<Store>,
    pub(super) failure: Mutex<Option<String>>,
}

impl FunctionRuntime {
    pub(super) fn new(executor: &SparqlExecutor) -> Arc<Self> {
        // Parsed bodies belong to the document; callbacks and their dataset
        // snapshot are rebuilt after commits. Reuse the executor's static cache.
        Arc::new(Self {
            definitions: executor
                .functions
                .iter()
                .map(|f| (f.clone(), executor.parse(&f.query)))
                .collect(),
            dataset: executor
                .frozen
                .as_ref()
                .map(FrozenIndexedDataset::function_snapshot),
            store: executor.store.clone(),
            failure: Mutex::new(None),
        })
    }

    pub(super) fn evaluator(self: &Arc<Self>, depth: usize) -> SparqlEvaluator {
        let mut evaluator = SparqlEvaluator::new();
        for (index, (definition, _)) in self.definitions.iter().enumerate() {
            let runtime = Arc::clone(self);
            evaluator = evaluator.with_custom_function(definition.iri.clone(), move |args| {
                let (definition, query) = &runtime.definitions[index];
                if args.len() > definition.params.len()
                    || (args.len()..definition.params.len())
                        .any(|i| !definition.optional_params.get(i).copied().unwrap_or(false))
                {
                    return None;
                }
                let bindings: Vec<_> = definition
                    .params
                    .iter()
                    .cloned()
                    .zip(args.iter().cloned())
                    .collect();
                let result = query
                    .clone()
                    .and_then(|query| runtime.evaluate(query, &bindings, depth + 1));
                match result {
                    Ok(value) => value,
                    Err(message) => {
                        runtime
                            .failure
                            .lock()
                            .expect("function failure lock")
                            .get_or_insert_with(|| {
                                format!("function <{}>: {message}", definition.iri.as_str())
                            });
                        None
                    }
                }
            });
        }
        evaluator
    }

    pub(super) fn evaluate(
        self: &Arc<Self>,
        query: Query,
        params: &[(String, Term)],
        depth: usize,
    ) -> Result<Option<Term>, String> {
        if depth >= MAX_FUNCTION_DEPTH {
            return Err(format!(
                "recursive function invocation exceeded {MAX_FUNCTION_DEPTH} levels"
            ));
        }
        let result_name = function_result_name(&query);
        let (mut query, is_ask) = function_select_query(query);
        for (name, _) in params {
            if let Query::Select { pattern, .. } = &mut query {
                project_variable(pattern, &Variable::new(name).map_err(err)?);
            }
        }
        let mut prepared = self.evaluator(depth).for_query(query);
        for (name, value) in params {
            prepared =
                prepared.substitute_variable(Variable::new(name).map_err(err)?, value.clone());
        }
        let results = if let Some(dataset) = &self.dataset {
            prepared
                .on_queryable_dataset(dataset)
                .execute()
                .map_err(err)?
        } else {
            prepared
                .on_store(self.store.as_ref().ok_or("missing function dataset")?)
                .execute()
                .map_err(err)?
        };
        match results {
            QueryResults::Solutions(mut solutions) => {
                let row = solutions.next().transpose().map_err(err)?;
                if let Some(message) = self.failure.lock().expect("function failure lock").as_ref()
                {
                    return Err(message.clone());
                }
                if is_ask {
                    Ok(Some(Term::Literal(Literal::from(row.is_some()))))
                } else {
                    let name =
                        result_name.ok_or("sh:SPARQLFunction SELECT must project one variable")?;
                    Ok(row.and_then(|row| row.get(name.as_str()).cloned()))
                }
            }
            QueryResults::Boolean(value) => Ok(Some(Term::Literal(Literal::from(value)))),
            QueryResults::Graph(_) => Err("sh:SPARQLFunction produced graph results".into()),
        }
    }
}
