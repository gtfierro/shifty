use crate::codegen::render_tokens_as_module;
use crate::ir::{SrcGenIR, SrcGenRuleKind, SrcGenRuleObject, SrcGenRuleSubject};
use oxigraph::model::Term;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::LitStr;

fn term_expr(term: &Term) -> TokenStream {
    match term {
        Term::NamedNode(node) => {
            let iri = LitStr::new(node.as_str(), Span::call_site());
            quote! {
                oxigraph::model::Term::NamedNode(
                    oxigraph::model::NamedNode::new_unchecked(#iri),
                )
            }
        }
        Term::BlankNode(node) => {
            let id = LitStr::new(node.as_str(), Span::call_site());
            quote! {
                oxigraph::model::Term::BlankNode(
                    oxigraph::model::BlankNode::new_unchecked(#id),
                )
            }
        }
        Term::Literal(literal) => {
            let value = LitStr::new(literal.value(), Span::call_site());
            if let Some(language) = literal.language() {
                let language_lit = LitStr::new(language, Span::call_site());
                quote! {
                    oxigraph::model::Term::Literal(
                        oxigraph::model::Literal::new_language_tagged_literal(
                            #value,
                            #language_lit,
                        ).expect("invalid language-tagged literal in srcgen rule"),
                    )
                }
            } else if literal.datatype().as_str() == "http://www.w3.org/2001/XMLSchema#string" {
                quote! {
                    oxigraph::model::Term::Literal(oxigraph::model::Literal::new_simple_literal(#value))
                }
            } else {
                let datatype = LitStr::new(literal.datatype().as_str(), Span::call_site());
                quote! {
                    oxigraph::model::Term::Literal(
                        oxigraph::model::Literal::new_typed_literal(
                            #value,
                            oxigraph::model::NamedNode::new_unchecked(#datatype),
                        ),
                    )
                }
            }
        }
    }
}

pub fn generate(ir: &SrcGenIR) -> Result<String, String> {
    let generated_rule_count = ir.meta.rule_count;
    let fallback_rule_count = ir.rules.iter().filter(|rule| rule.fallback_only).count();
    let specialized_rule_count = ir
        .rules
        .iter()
        .filter(|rule| {
            !rule.fallback_only
                && matches!(
                    rule.kind,
                    SrcGenRuleKind::Triple { .. } | SrcGenRuleKind::Sparql { .. }
                )
        })
        .count();

    let mut specialized_rule_blocks: Vec<TokenStream> = Vec::new();
    for rule in &ir.rules {
        if rule.fallback_only {
            continue;
        }
        let rule_id = rule.id;
        let target_class_lits: Vec<LitStr> = rule
            .target_classes
            .iter()
            .map(|iri| LitStr::new(iri, Span::call_site()))
            .collect();
        match &rule.kind {
            SrcGenRuleKind::Triple {
                subject,
                predicate_iri,
                object,
                condition_shape_iris,
            } => {
                let predicate_lit = LitStr::new(predicate_iri, Span::call_site());
                let condition_shape_lits: Vec<LitStr> = condition_shape_iris
                    .iter()
                    .map(|iri| LitStr::new(iri, Span::call_site()))
                    .collect();
                let subject_setup = match subject {
                    SrcGenRuleSubject::This => quote! {
                        let subject_term: oxigraph::model::Term = focus.clone();
                    },
                    SrcGenRuleSubject::Constant(term) => {
                        let expr = term_expr(term);
                        quote! {
                            let subject_term: oxigraph::model::Term = #expr;
                        }
                    }
                };
                let object_setup_and_use = match object {
                    SrcGenRuleObject::Constant(term) => {
                        let expr = term_expr(term);
                        quote! {
                            let object_template: oxigraph::model::Term = #expr;
                            let object_term: oxigraph::model::Term = object_template.clone();
                        }
                    }
                    SrcGenRuleObject::This => quote! {
                        let object_term: oxigraph::model::Term = focus.clone();
                    },
                };
                specialized_rule_blocks.push(quote! {
                    {
                        let mut condition_cache: std::collections::HashMap<(String, String), bool> =
                            std::collections::HashMap::new();
                        let mut condition_validator: Option<shifty::Validator> = None;
                        let mut focus_nodes: Vec<oxigraph::model::Term> = Vec::new();
                        #(
                            focus_nodes.extend(focus_nodes_for_target_class_cached(
                                &mut target_class_index,
                                store,
                                data_graph,
                                #target_class_lits,
                            )?);
                        )*
                        focus_nodes.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
                        focus_nodes.dedup();

                        let predicate = oxigraph::model::NamedNode::new(#predicate_lit).map_err(|err| {
                            format!("invalid TripleRule predicate IRI for rule {}: {err}", #rule_id)
                        })?;
                        for focus in focus_nodes {
                            let mut conditions_met = true;
                            #(
                                let condition_key = (#condition_shape_lits.to_string(), focus.to_string());
                                let condition_conforms = if let Some(cached) = condition_cache.get(&condition_key) {
                                    *cached
                                } else {
                                    if condition_validator.is_none() {
                                        condition_validator = Some(build_runtime_validator(store, data_graph)?);
                                    }
                                    let validator = condition_validator
                                        .as_ref()
                                        .ok_or_else(|| "condition validator was not initialized".to_string())?;
                                    let conforms = validator
                                        .node_conforms_to_shape_id(&focus, #condition_shape_lits)?;
                                    condition_cache.insert(condition_key, conforms);
                                    conforms
                                };
                                if !condition_conforms {
                                    conditions_met = false;
                                }
                            )*
                            if !conditions_met {
                                continue;
                            }
                            #subject_setup
                            #object_setup_and_use
                            let Some(subject) = subject_ref_from_term(&subject_term) else {
                                continue;
                            };
                            let inferred = oxigraph::model::Quad::new(
                                subject.into_owned(),
                                predicate.clone(),
                                object_term,
                                graph_name.clone(),
                            );
                            if store
                                .contains(inferred.as_ref())
                                .map_err(|err| format!("failed to query inferred quad existence: {err}"))?
                            {
                                continue;
                            }
                            store
                                .insert(inferred.as_ref())
                                .map_err(|err| format!("failed to insert inferred quad: {err}"))?;
                            inserted += 1;
                            condition_cache.clear();
                            let _ = condition_validator.take();
                        }
                    }
                });
            }
            SrcGenRuleKind::Sparql {
                query,
                condition_shape_iris,
            } => {
                let normalized_query = query.replace('$', "?");
                let query_lit = LitStr::new(&normalized_query, Span::call_site());
                let condition_shape_lits: Vec<LitStr> = condition_shape_iris
                    .iter()
                    .map(|iri| LitStr::new(iri, Span::call_site()))
                    .collect();
                if condition_shape_lits.is_empty() {
                    specialized_rule_blocks.push(quote! {
                        {
                            let mut focus_nodes: Vec<oxigraph::model::Term> = Vec::new();
                            #(
                                focus_nodes.extend(focus_nodes_for_target_class_cached_cloned(
                                    &mut target_class_focus_cache,
                                    &mut target_class_index,
                                    store,
                                    data_graph,
                                    #target_class_lits,
                                )?);
                            )*
                            focus_nodes.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
                            focus_nodes.dedup();

                            let candidate_batches = focus_nodes
                                .par_iter()
                                .map(|focus| -> Result<
                                    Vec<(
                                        oxigraph::model::Term,
                                        oxigraph::model::NamedNode,
                                        oxigraph::model::Term,
                                    )>,
                                    String,
                                > {
                                    let sparql_bindings = [("this", focus.clone())];
                                    sparql_construct_triples_with_bindings(
                                        #query_lit,
                                        store,
                                        data_graph,
                                        &sparql_bindings,
                                    )
                                    .map_err(|err| {
                                        format!("SPARQLRule {} execution failed: {err}", #rule_id)
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()?;

                            let mut inferred_seen: std::collections::HashSet<(
                                oxigraph::model::Term,
                                oxigraph::model::NamedNode,
                                oxigraph::model::Term,
                            )> = std::collections::HashSet::new();
                            for constructed in candidate_batches {
                                for (subject_term, predicate, object_term) in constructed {
                                    if !inferred_seen.insert((
                                        subject_term.clone(),
                                        predicate.clone(),
                                        object_term.clone(),
                                    )) {
                                        continue;
                                    }
                                    let Some(subject) = subject_ref_from_term(&subject_term) else {
                                        continue;
                                    };
                                    let inferred = oxigraph::model::Quad::new(
                                        subject.into_owned(),
                                        predicate,
                                        object_term,
                                        graph_name.clone(),
                                    );
                                    if store
                                        .contains(inferred.as_ref())
                                        .map_err(|err| format!("failed to query inferred quad existence: {err}"))?
                                    {
                                        continue;
                                    }
                                    store
                                        .insert(inferred.as_ref())
                                        .map_err(|err| format!("failed to insert inferred quad: {err}"))?;
                                    inserted += 1;
                                }
                            }
                        }
                    });
                } else {
                    specialized_rule_blocks.push(quote! {
                        {
                            let mut condition_cache: std::collections::HashMap<(String, String), bool> =
                                std::collections::HashMap::new();
                            let mut condition_validator: Option<shifty::Validator> = None;
                            let mut focus_nodes: Vec<oxigraph::model::Term> = Vec::new();
                            #(
                                focus_nodes.extend(focus_nodes_for_target_class_cached_cloned(
                                    &mut target_class_focus_cache,
                                    &mut target_class_index,
                                    store,
                                    data_graph,
                                    #target_class_lits,
                                )?);
                            )*
                            focus_nodes.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
                            focus_nodes.dedup();

                            for focus in focus_nodes {
                                let mut conditions_met = true;
                                #(
                                    let condition_key = (#condition_shape_lits.to_string(), focus.to_string());
                                    let condition_conforms = if let Some(cached) = condition_cache.get(&condition_key) {
                                        *cached
                                    } else {
                                        if condition_validator.is_none() {
                                            condition_validator = Some(build_runtime_validator(store, data_graph)?);
                                        }
                                        let validator = condition_validator
                                            .as_ref()
                                            .ok_or_else(|| "condition validator was not initialized".to_string())?;
                                        let conforms = validator
                                            .node_conforms_to_shape_id(&focus, #condition_shape_lits)?;
                                        condition_cache.insert(condition_key, conforms);
                                        conforms
                                    };
                                    if !condition_conforms {
                                        conditions_met = false;
                                    }
                                )*
                                if !conditions_met {
                                    continue;
                                }

                                let sparql_bindings = [("this", focus.clone())];
                                let constructed = sparql_construct_triples_with_bindings(
                                    #query_lit,
                                    store,
                                    data_graph,
                                    &sparql_bindings,
                                ).map_err(|err| {
                                    format!("SPARQLRule {} execution failed: {err}", #rule_id)
                                })?;
                                let mut inserted_any = false;
                                for (subject_term, predicate, object_term) in constructed {
                                    let Some(subject) = subject_ref_from_term(&subject_term) else {
                                        continue;
                                    };
                                    let inferred = oxigraph::model::Quad::new(
                                        subject.into_owned(),
                                        predicate,
                                        object_term,
                                        graph_name.clone(),
                                    );
                                    if store
                                        .contains(inferred.as_ref())
                                        .map_err(|err| format!("failed to query inferred quad existence: {err}"))?
                                    {
                                        continue;
                                    }
                                    store
                                        .insert(inferred.as_ref())
                                        .map_err(|err| format!("failed to insert inferred quad: {err}"))?;
                                    inserted += 1;
                                    inserted_any = true;
                                }
                                if inserted_any {
                                    condition_cache.clear();
                                    let _ = condition_validator.take();
                                }
                            }
                        }
                    });
                }
            }
            SrcGenRuleKind::Unsupported { .. } => {}
        }
    }

    let tokens = quote! {
        use rayon::prelude::*;

        pub const GENERATED_INFERENCE_RULES: usize = #generated_rule_count;
        pub const GENERATED_SPECIALIZED_INFERENCE_RULES: usize = #specialized_rule_count;
        pub const GENERATED_FALLBACK_INFERENCE_RULES: usize = #fallback_rule_count;

        thread_local! {
            static SPARQL_RULE_PREPARED_CACHE: std::cell::RefCell<
                std::collections::HashMap<String, oxigraph::sparql::PreparedSparqlQuery>
            > = std::cell::RefCell::new(std::collections::HashMap::new());
            static SPARQL_RULE_USES_THIS_CACHE: std::cell::RefCell<
                std::collections::HashMap<String, bool>
            > = std::cell::RefCell::new(std::collections::HashMap::new());
        }

        fn sparql_construct_triples_with_bindings(
            query: &str,
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            bindings: &[(&str, oxigraph::model::Term)],
        ) -> Result<Vec<(oxigraph::model::Term, oxigraph::model::NamedNode, oxigraph::model::Term)>, String> {
            let mut prepared = SPARQL_RULE_PREPARED_CACHE.with(
                |cache| -> Result<oxigraph::sparql::PreparedSparqlQuery, String> {
                    let mut cache = cache.borrow_mut();
                    if let Some(cached) = cache.get(query) {
                        return Ok(cached.clone());
                    }
                    let parsed = oxigraph::sparql::SparqlEvaluator::new()
                        .parse_query(query)
                        .map_err(|err| format!("failed to parse SPARQL query: {err}"))?;
                    cache.insert(query.to_string(), parsed.clone());
                    Ok(parsed)
                }
            )?;

            let default_graphs: Vec<oxigraph::model::GraphName> = validation_graphs(data_graph)?
                .into_iter()
                .map(oxigraph::model::GraphName::NamedNode)
                .collect();
            prepared.dataset_mut().set_default_graph(default_graphs);

            let mut bound = prepared.on_store(store);
            let query_uses_this = SPARQL_RULE_USES_THIS_CACHE.with(|cache| {
                let mut cache = cache.borrow_mut();
                if let Some(cached) = cache.get(query) {
                    *cached
                } else {
                    let mentions_this = query_mentions_var(query, "this");
                    cache.insert(query.to_string(), mentions_this);
                    mentions_this
                }
            });
            for (name, term) in bindings {
                if *name == "this" && !query_uses_this {
                    continue;
                }
                bound = bound.substitute_variable(
                    oxigraph::sparql::Variable::new_unchecked(*name),
                    term.clone(),
                );
            }

            match bound.execute() {
                Ok(oxigraph::sparql::QueryResults::Graph(mut triples)) => {
                    let mut out = Vec::new();
                    for triple in &mut triples {
                        let triple =
                            triple.map_err(|err| format!("SPARQL graph triple error: {err}"))?;
                        out.push((oxigraph::model::Term::from(triple.subject), triple.predicate, triple.object));
                    }
                    Ok(out)
                }
                Ok(_) => Err("SPARQL CONSTRUCT rule returned a non-graph result".to_string()),
                Err(err) => Err(format!("failed to execute SPARQL query: {err}")),
            }
        }

        fn focus_nodes_for_target_class_cached_cloned(
            target_class_cache: &mut std::collections::HashMap<String, Vec<oxigraph::model::Term>>,
            target_class_index: &mut Option<TargetClassIndex>,
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            class_iri: &str,
        ) -> Result<Vec<oxigraph::model::Term>, String> {
            if let Some(cached) = target_class_cache.get(class_iri) {
                return Ok(cached.clone());
            }
            let nodes =
                focus_nodes_for_target_class_cached(target_class_index, store, data_graph, class_iri)?;
            target_class_cache.insert(class_iri.to_string(), nodes.clone());
            Ok(nodes)
        }

        pub fn run_generated_inference(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<usize, String> {
            if GENERATED_INFERENCE_RULES == 0 {
                return Ok(0);
            }

            let graph_name = oxigraph::model::GraphName::NamedNode(data_graph.clone());
            let mut inserted = 0usize;
            let mut target_class_index: Option<TargetClassIndex> = None;
            let mut target_class_focus_cache: std::collections::HashMap<
                String,
                Vec<oxigraph::model::Term>,
            > = std::collections::HashMap::new();
            #(#specialized_rule_blocks)*

            if GENERATED_FALLBACK_INFERENCE_RULES == 0 {
                return Ok(inserted);
            }

            let validator = build_runtime_validator(store, data_graph)?;
            let config = shifty::InferenceConfig::default();
            let outcome = validator
                .run_inference_with_config(config)
                .map_err(|err| format!("runtime inference failed: {err}"))?;
            for quad in outcome.inferred_quads {
                let inferred = oxigraph::model::Quad::new(
                    quad.subject,
                    quad.predicate,
                    quad.object,
                    graph_name.clone(),
                );
                if store
                    .contains(inferred.as_ref())
                    .map_err(|err| format!("failed to query inferred quad existence: {err}"))?
                {
                    continue;
                }
                store
                    .insert(inferred.as_ref())
                    .map_err(|err| format!("failed to insert inferred quad: {err}"))?;
                inserted += 1;
            }
            Ok(inserted)
        }
    };
    render_tokens_as_module(tokens)
}
