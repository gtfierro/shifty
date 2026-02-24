use crate::codegen::render_tokens_as_module;
use crate::ir::{SrcGenComponentKind, SrcGenIR};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use std::collections::HashMap;
use syn::LitStr;

fn sparql_uses_relation_projection_pattern(query: &str) -> bool {
    (query.contains("$this ?p ?o") || query.contains("?this ?p ?o"))
        && query.contains("?p a/rdfs:subClassOf* s223:Relation")
}

fn sparql_uses_closed_world_projection_pattern(query: &str) -> bool {
    (query.contains("$this ?p ?o") || query.contains("?this ?p ?o"))
        && query.contains("a/rdfs:subClassOf* ?class")
        && query.contains("sh:property/sh:path ?p")
        && query.contains("FILTER NOT EXISTS")
}

pub fn generate(ir: &SrcGenIR) -> Result<String, String> {
    let component_by_id: HashMap<u64, &SrcGenComponentKind> = ir
        .components
        .iter()
        .map(|component| (component.id, &component.kind))
        .collect();
    let property_path_by_id: HashMap<u64, String> = ir
        .property_shapes
        .iter()
        .filter_map(|shape| {
            shape
                .path_predicate
                .as_ref()
                .map(|predicate| (shape.id, predicate.clone()))
        })
        .collect();

    let mut node_arms: Vec<TokenStream> = Vec::new();
    let mut supported_shapes = 0usize;
    let mut supported_node_ids: Vec<u64> = Vec::new();

    for shape in ir.node_shapes.iter().filter(|shape| {
        shape.supported
            && (!shape.supported_constraints.is_empty() || !shape.property_shapes.is_empty())
    }) {
        supported_shapes += 1;
        supported_node_ids.push(shape.id);
        let shape_id = shape.id;

        let focus_sources: Vec<TokenStream> = shape
            .target_classes
            .iter()
            .map(|class_iri| {
                let class_lit = LitStr::new(class_iri, Span::call_site());
                quote! {
                    focus_nodes.extend(focus_nodes_for_target_class_cached(
                        &mut target_class_index,
                        store,
                        data_graph,
                        #class_lit,
                    )?);
                }
            })
            .collect();

        let mut shape_batched_checks: Vec<TokenStream> = Vec::new();
        let mut node_constraint_checks: Vec<TokenStream> = Vec::new();
        for component_id in &shape.supported_constraints {
            let component_id_value = *component_id;
            let Some(kind) = component_by_id.get(component_id) else {
                continue;
            };
            match kind {
                SrcGenComponentKind::Class { class_iri } => {
                    let class_lit = LitStr::new(class_iri, Span::call_site());
                    node_constraint_checks.push(quote! {
                        let valid = term_has_rdf_type(store, data_graph, focus, #class_lit)?;
                        if !valid {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: None,
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::Datatype { datatype_iri } => {
                    let datatype_lit = LitStr::new(datatype_iri, Span::call_site());
                    node_constraint_checks.push(quote! {
                        let valid = match focus {
                            oxigraph::model::Term::Literal(literal) => {
                                literal.datatype().as_str() == #datatype_lit
                            }
                            _ => false,
                        };
                        if !valid {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: Some(focus.clone()),
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::NodeKind { node_kind_iri } => {
                    let node_kind_lit = LitStr::new(node_kind_iri, Span::call_site());
                    node_constraint_checks.push(quote! {
                        let valid = shacl_node_kind_matches(focus, #node_kind_lit);
                        if !valid {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: None,
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::MinLength { min_length } => {
                    let min_length = *min_length as usize;
                    node_constraint_checks.push(quote! {
                        let valid = term_string_for_text_constraints(focus)
                            .map(|text| text.chars().count() >= #min_length)
                            .unwrap_or(false);
                        if !valid {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: None,
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::MaxLength { max_length } => {
                    let max_length = *max_length as usize;
                    node_constraint_checks.push(quote! {
                        let valid = term_string_for_text_constraints(focus)
                            .map(|text| text.chars().count() <= #max_length)
                            .unwrap_or(false);
                        if !valid {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: None,
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::Pattern { pattern, flags } => {
                    let pattern_lit = LitStr::new(pattern, Span::call_site());
                    let flags_token = if let Some(flags) = flags {
                        let flags_lit = LitStr::new(flags, Span::call_site());
                        quote! { Some(#flags_lit) }
                    } else {
                        quote! { None }
                    };
                    node_constraint_checks.push(quote! {
                        let regex = build_pattern_regex(#pattern_lit, #flags_token)?;
                        let valid = term_string_for_text_constraints(focus)
                            .map(|text| regex.is_match(&text))
                            .unwrap_or(false);
                        if !valid {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: None,
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::LanguageIn { languages } => {
                    let language_literals: Vec<LitStr> = languages
                        .iter()
                        .map(|language| LitStr::new(language, Span::call_site()))
                        .collect();
                    node_constraint_checks.push(quote! {
                        let allowed_languages: &[&str] = &[#(#language_literals),*];
                        let valid = term_matches_language_in(focus, allowed_languages);
                        if !valid {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: None,
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::HasValue { value_sparql } => {
                    let value_lit = LitStr::new(value_sparql, Span::call_site());
                    node_constraint_checks.push(quote! {
                        let required_value = #value_lit;
                        let valid = term_to_sparql(focus) == required_value;
                        if !valid {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: None,
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::In { values_sparql } => {
                    let value_literals: Vec<LitStr> = values_sparql
                        .iter()
                        .map(|value| LitStr::new(value, Span::call_site()))
                        .collect();
                    node_constraint_checks.push(quote! {
                        let allowed_values: std::collections::HashSet<&str> =
                            [#(#value_literals),*].into_iter().collect();
                        if allowed_values.is_empty() {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: None,
                                path: None,
                            });
                        } else if !allowed_values.contains(term_to_sparql(focus).as_str()) {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: Some(focus.clone()),
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::Sparql {
                    query,
                    prefixes,
                    requires_path: _,
                    ..
                } => {
                    let query_lit = LitStr::new(query, Span::call_site());
                    let prefixes_lit = LitStr::new(prefixes, Span::call_site());
                    let use_batched_focus_query = sparql_uses_relation_projection_pattern(query)
                        || sparql_uses_closed_world_projection_pattern(query);
                    if use_batched_focus_query {
                        let mode_token = if sparql_uses_relation_projection_pattern(query) {
                            quote! { ClosedWorldConstraintMode::S223Relation }
                        } else {
                            quote! { ClosedWorldConstraintMode::QudtPredicate }
                        };
                        shape_batched_checks.push(quote! {
                            let closed_world_violations = run_closed_world_batch_violations(
                                store,
                                data_graph,
                                &focus_nodes,
                                #mode_token,
                            )?;
                            for focus_term in closed_world_violations {
                                violations.push(Violation {
                                    shape_id: #shape_id,
                                    component_id: #component_id_value,
                                    focus: focus_term.clone(),
                                    value: Some(focus_term),
                                    path: None,
                                });
                            }
                        });
                    } else {
                        node_constraint_checks.push(quote! {
                            let query = String::from(#query_lit);
                            let mut bindings: Vec<(&str, oxigraph::model::Term)> = Vec::new();
                            bindings.push(("this", focus.clone()));
                            if query_mentions_var(&query, "currentShape") {
                                if let Some(current_shape_term) = shape_term_for_id(#shape_id) {
                                    bindings.push(("currentShape", current_shape_term));
                                }
                            }
                            if query_mentions_var(&query, "shapesGraph") {
                                if let Ok(shape_graph_node) = oxigraph::model::NamedNode::new(SHAPE_GRAPH) {
                                    bindings.push(("shapesGraph", oxigraph::model::Term::NamedNode(shape_graph_node)));
                                }
                            }

                            let solutions = sparql_select_solutions_with_bindings(
                                &query,
                                #prefixes_lit,
                                store,
                                data_graph,
                                &bindings,
                            )?;

                            let mut seen: std::collections::HashSet<oxigraph::model::Term> =
                                std::collections::HashSet::new();
                            for row in solutions {
                                if let Some(failure_term) = row.get("failure") {
                                    if term_is_true_boolean(failure_term) {
                                        return Err(format!(
                                            "SPARQL constraint query reported failure for shape {} component {}",
                                            #shape_id, #component_id_value
                                        ));
                                    }
                                }
                                let value = row
                                    .get("value")
                                    .cloned()
                                    .unwrap_or_else(|| focus.clone());
                                if !seen.insert(value.clone()) {
                                    continue;
                                }
                                let path = if let Some(oxigraph::model::Term::NamedNode(path_iri)) = row.get("path") {
                                    Some(ResultPath::Term(oxigraph::model::Term::NamedNode(path_iri.clone())))
                                } else {
                                    None
                                };
                                violations.push(Violation {
                                    shape_id: #shape_id,
                                    component_id: #component_id_value,
                                    focus: focus.clone(),
                                    value: Some(value),
                                    path,
                                });
                            }
                        });
                    }
                }
                SrcGenComponentKind::Node { shape_iri } => {
                    let shape_lit = LitStr::new(shape_iri, Span::call_site());
                    node_constraint_checks.push(quote! {
                        let valid = runtime_shape_conforms(store, data_graph, #shape_lit, focus)?;
                        if !valid {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: Some(focus.clone()),
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::Not { shape_iri } => {
                    let shape_lit = LitStr::new(shape_iri, Span::call_site());
                    node_constraint_checks.push(quote! {
                        let conforms = runtime_shape_conforms(store, data_graph, #shape_lit, focus)?;
                        if conforms {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: Some(focus.clone()),
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::And { shape_iris } => {
                    let shape_literals: Vec<LitStr> = shape_iris
                        .iter()
                        .map(|shape| LitStr::new(shape, Span::call_site()))
                        .collect();
                    node_constraint_checks.push(quote! {
                        let conjunct_shapes: &[&str] = &[#(#shape_literals),*];
                        let mut all_conform = true;
                        for conjunct in conjunct_shapes {
                            if !runtime_shape_conforms(store, data_graph, conjunct, focus)? {
                                all_conform = false;
                                break;
                            }
                        }
                        if !all_conform {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: Some(focus.clone()),
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::Or { shape_iris } => {
                    let shape_literals: Vec<LitStr> = shape_iris
                        .iter()
                        .map(|shape| LitStr::new(shape, Span::call_site()))
                        .collect();
                    node_constraint_checks.push(quote! {
                        let disjunct_shapes: &[&str] = &[#(#shape_literals),*];
                        let mut any_conforms = false;
                        for disjunct in disjunct_shapes {
                            if runtime_shape_conforms(store, data_graph, disjunct, focus)? {
                                any_conforms = true;
                                break;
                            }
                        }
                        if !any_conforms {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: Some(focus.clone()),
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::Xone { shape_iris } => {
                    let shape_literals: Vec<LitStr> = shape_iris
                        .iter()
                        .map(|shape| LitStr::new(shape, Span::call_site()))
                        .collect();
                    node_constraint_checks.push(quote! {
                        let xone_shapes: &[&str] = &[#(#shape_literals),*];
                        let mut conforms_count = 0usize;
                        for disjunct in xone_shapes {
                            if runtime_shape_conforms(store, data_graph, disjunct, focus)? {
                                conforms_count += 1;
                            }
                        }
                        if conforms_count != 1 {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: Some(focus.clone()),
                                path: None,
                            });
                        }
                    });
                }
                SrcGenComponentKind::Closed {
                    closed,
                    ignored_property_iris,
                } => {
                    if *closed {
                        let mut allowed_properties: Vec<String> = ignored_property_iris.clone();
                        for property_shape_id in &shape.property_shapes {
                            if let Some(path_predicate) = property_path_by_id.get(property_shape_id)
                            {
                                allowed_properties.push(path_predicate.clone());
                            }
                        }
                        allowed_properties.sort();
                        allowed_properties.dedup();
                        let allowed_property_literals: Vec<LitStr> = allowed_properties
                            .iter()
                            .map(|property| LitStr::new(property, Span::call_site()))
                            .collect();
                        node_constraint_checks.push(quote! {
                            if let Some(subject_ref) = subject_ref_from_term(focus) {
                                let allowed_predicates: std::collections::HashSet<&str> =
                                    [#(#allowed_property_literals),*].into_iter().collect();
                                let graph_ref =
                                    oxigraph::model::GraphNameRef::NamedNode(data_graph.as_ref());
                                for quad in store.quads_for_pattern(
                                    Some(subject_ref),
                                    None,
                                    None,
                                    Some(graph_ref),
                                ) {
                                    let quad = quad
                                        .map_err(|err| format!("store query failed: {err}"))?;
                                    let predicate = quad.predicate;
                                    if !allowed_predicates.contains(predicate.as_str()) {
                                        violations.push(Violation {
                                            shape_id: #shape_id,
                                            component_id: #component_id_value,
                                            focus: focus.clone(),
                                            value: Some(quad.object),
                                            path: Some(ResultPath::Term(
                                                oxigraph::model::Term::NamedNode(predicate),
                                            )),
                                        });
                                    }
                                }
                            }
                        });
                    }
                }
                SrcGenComponentKind::PropertyLink => {}
                SrcGenComponentKind::QualifiedValueShape { .. } => {}
                SrcGenComponentKind::MinCount { .. } => {}
                SrcGenComponentKind::MaxCount { .. } => {}
                SrcGenComponentKind::MinExclusive { .. } => {}
                SrcGenComponentKind::MinInclusive { .. } => {}
                SrcGenComponentKind::MaxExclusive { .. } => {}
                SrcGenComponentKind::MaxInclusive { .. } => {}
                SrcGenComponentKind::UniqueLang { .. } => {}
                SrcGenComponentKind::Equals { .. } => {}
                SrcGenComponentKind::Disjoint { .. } => {}
                SrcGenComponentKind::LessThan { .. } => {}
                SrcGenComponentKind::LessThanOrEquals { .. } => {}
                SrcGenComponentKind::Unsupported { .. } => {}
            }
        }

        let property_calls: Vec<TokenStream> = shape
            .property_shapes
            .iter()
            .map(|property_shape_id| {
                quote! {
                    validate_supported_property_shape(
                        #property_shape_id,
                        #shape_id,
                        store,
                        data_graph,
                        focus,
                        &mut violations,
                    )?;
                }
            })
            .collect();

        node_arms.push(quote! {
            #shape_id => {
                let mut focus_nodes: Vec<oxigraph::model::Term> = Vec::new();
                #(#focus_sources)*
                focus_nodes.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
                focus_nodes.dedup();

                #(#shape_batched_checks)*

                for focus in &focus_nodes {
                    #(#node_constraint_checks)*
                    #(#property_calls)*
                }
            }
        });
    }

    supported_node_ids.sort_unstable();

    let tokens = quote! {
        pub const GENERATED_NODE_VALIDATORS: usize = #supported_shapes;

        struct TargetClassIndex {
            subclasses_by_super:
                std::collections::HashMap<oxigraph::model::Term, Vec<oxigraph::model::Term>>,
            subjects_by_direct_type:
                std::collections::HashMap<oxigraph::model::Term, Vec<oxigraph::model::Term>>,
            descendant_cache: std::collections::HashMap<
                oxigraph::model::Term,
                std::collections::HashSet<oxigraph::model::Term>,
            >,
        }

        fn descendants_for_class(
            index: &mut TargetClassIndex,
            target_class: &oxigraph::model::Term,
        ) -> std::collections::HashSet<oxigraph::model::Term> {
            if let Some(cached) = index.descendant_cache.get(target_class) {
                return cached.clone();
            }
            let mut descendants: std::collections::HashSet<oxigraph::model::Term> =
                std::collections::HashSet::new();
            let mut stack: Vec<oxigraph::model::Term> = vec![target_class.clone()];
            while let Some(current) = stack.pop() {
                if !descendants.insert(current.clone()) {
                    continue;
                }
                if let Some(subclasses) = index.subclasses_by_super.get(&current) {
                    for subclass in subclasses {
                        stack.push(subclass.clone());
                    }
                }
            }
            index
                .descendant_cache
                .insert(target_class.clone(), descendants.clone());
            descendants
        }

        fn build_target_class_index(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<TargetClassIndex, String> {
            let mut subclasses_by_super: std::collections::HashMap<
                oxigraph::model::Term,
                Vec<oxigraph::model::Term>,
            > = std::collections::HashMap::new();
            for quad in store.quads_for_pattern(
                None,
                Some(oxigraph::model::NamedNodeRef::new_unchecked(
                    "http://www.w3.org/2000/01/rdf-schema#subClassOf",
                )),
                None,
                None,
            ) {
                let quad = quad.map_err(|err| format!("store query failed: {err}"))?;
                let superclass = match quad.object {
                    oxigraph::model::Term::NamedNode(node) => oxigraph::model::Term::NamedNode(node),
                    _ => continue,
                };
                let subclass = match quad.subject {
                    oxigraph::model::NamedOrBlankNode::NamedNode(node) => {
                        oxigraph::model::Term::NamedNode(node)
                    }
                    oxigraph::model::NamedOrBlankNode::BlankNode(_) => continue,
                };
                subclasses_by_super
                    .entry(superclass)
                    .or_default()
                    .push(subclass);
            }

            let mut subjects_by_direct_type: std::collections::HashMap<
                oxigraph::model::Term,
                Vec<oxigraph::model::Term>,
            > = std::collections::HashMap::new();
            let data_graph_ref = oxigraph::model::GraphNameRef::NamedNode(data_graph.as_ref());
            for quad in store.quads_for_pattern(
                None,
                Some(oxigraph::model::vocab::rdf::TYPE),
                None,
                Some(data_graph_ref),
            ) {
                let quad = quad.map_err(|err| format!("store query failed: {err}"))?;
                let class_term = match quad.object {
                    oxigraph::model::Term::NamedNode(node) => oxigraph::model::Term::NamedNode(node),
                    _ => continue,
                };
                subjects_by_direct_type
                    .entry(class_term)
                    .or_default()
                    .push(oxigraph::model::Term::from(quad.subject));
            }

            for subclasses in subclasses_by_super.values_mut() {
                subclasses.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
                subclasses.dedup();
            }
            for subjects in subjects_by_direct_type.values_mut() {
                subjects.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
                subjects.dedup();
            }

            Ok(TargetClassIndex {
                subclasses_by_super,
                subjects_by_direct_type,
                descendant_cache: std::collections::HashMap::new(),
            })
        }

        fn focus_nodes_for_target_class_cached(
            index: &mut Option<TargetClassIndex>,
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            class_iri: &str,
        ) -> Result<Vec<oxigraph::model::Term>, String> {
            if index.is_none() {
                *index = Some(build_target_class_index(store, data_graph)?);
            }
            let index = index
                .as_mut()
                .ok_or_else(|| "target class index not initialized".to_string())?;

            let class_node = oxigraph::model::NamedNode::new(class_iri)
                .map_err(|err| format!("invalid class target IRI {class_iri}: {err}"))?;
            let class_term = oxigraph::model::Term::NamedNode(class_node);
            let descendants = descendants_for_class(index, &class_term);
            let mut nodes: Vec<oxigraph::model::Term> = descendants
                .into_iter()
                .flat_map(|descendant| {
                    index
                        .subjects_by_direct_type
                        .get(&descendant)
                        .cloned()
                        .unwrap_or_default()
                })
                .collect();
            nodes.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
            nodes.dedup();
            Ok(nodes)
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        enum ClosedWorldConstraintMode {
            S223Relation,
            QudtPredicate,
        }

        fn closed_world_focus_terms(
            focus_nodes: &[oxigraph::model::Term],
        ) -> Vec<oxigraph::model::Term> {
            let mut seen: std::collections::HashSet<oxigraph::model::Term> =
                std::collections::HashSet::new();
            focus_nodes
                .iter()
                .filter(|term| {
                    matches!(
                        term,
                        oxigraph::model::Term::NamedNode(_) | oxigraph::model::Term::BlankNode(_)
                    )
                })
                .filter_map(|term| {
                    if seen.insert(term.clone()) {
                        Some(term.clone())
                    } else {
                        None
                    }
                })
                .collect()
        }

        fn closed_world_superclass_closure(
            class_term: &oxigraph::model::Term,
            direct_superclasses: &std::collections::HashMap<
                oxigraph::model::Term,
                Vec<oxigraph::model::Term>,
            >,
            memo: &mut std::collections::HashMap<
                oxigraph::model::Term,
                std::collections::HashSet<oxigraph::model::Term>,
            >,
        ) -> std::collections::HashSet<oxigraph::model::Term> {
            if let Some(cached) = memo.get(class_term) {
                return cached.clone();
            }
            let mut closure: std::collections::HashSet<oxigraph::model::Term> =
                std::collections::HashSet::new();
            closure.insert(class_term.clone());
            if let Some(parents) = direct_superclasses.get(class_term) {
                for parent in parents {
                    closure.extend(closed_world_superclass_closure(
                        parent,
                        direct_superclasses,
                        memo,
                    ));
                }
            }
            memo.insert(class_term.clone(), closure.clone());
            closure
        }

        fn run_closed_world_batch_violations(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            focus_nodes: &[oxigraph::model::Term],
            mode: ClosedWorldConstraintMode,
        ) -> Result<Vec<oxigraph::model::Term>, String> {
            let focus_terms = closed_world_focus_terms(focus_nodes);
            if focus_terms.is_empty() {
                return Ok(Vec::new());
            }

            let mut direct_superclasses: std::collections::HashMap<
                oxigraph::model::Term,
                Vec<oxigraph::model::Term>,
            > = std::collections::HashMap::new();
            for quad in store.quads_for_pattern(
                None,
                Some(oxigraph::model::NamedNodeRef::new_unchecked(
                    "http://www.w3.org/2000/01/rdf-schema#subClassOf",
                )),
                None,
                None,
            ) {
                let quad = quad.map_err(|err| format!("store query failed: {err}"))?;
                let superclass = quad.object;
                if !matches!(
                    superclass,
                    oxigraph::model::Term::NamedNode(_) | oxigraph::model::Term::BlankNode(_)
                ) {
                    continue;
                }
                let subclass = oxigraph::model::Term::from(quad.subject);
                direct_superclasses
                    .entry(subclass)
                    .or_default()
                    .push(superclass);
            }
            let mut superclass_memo: std::collections::HashMap<
                oxigraph::model::Term,
                std::collections::HashSet<oxigraph::model::Term>,
            > = std::collections::HashMap::new();

            let mut allowed_by_class: std::collections::HashMap<
                oxigraph::model::Term,
                std::collections::HashSet<oxigraph::model::NamedNode>,
            > = std::collections::HashMap::new();
            let allowed_query = r#"
PREFIX sh: <http://www.w3.org/ns/shacl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT DISTINCT ?class ?p
WHERE {
  { ?class sh:property/sh:path ?p . }
  UNION
  { ?class sh:xone/rdf:rest*/rdf:first/sh:property/sh:path ?p . }
  UNION
  { ?class sh:or/rdf:rest*/rdf:first/sh:property/sh:path ?p . }
}
"#;
            let allowed_rows = sparql_select_solutions_with_bindings(
                allowed_query,
                "",
                store,
                data_graph,
                &[],
            )?;
            for row in allowed_rows {
                let Some(class_term) = row.get("class").cloned() else {
                    continue;
                };
                let Some(oxigraph::model::Term::NamedNode(path_predicate)) = row.get("p").cloned()
                else {
                    continue;
                };
                allowed_by_class
                    .entry(class_term)
                    .or_default()
                    .insert(path_predicate);
            }

            let relation_root = oxigraph::model::Term::NamedNode(
                oxigraph::model::NamedNode::new_unchecked(
                    "http://data.ashrae.org/standard223#Relation",
                ),
            );
            let mut s223_relation_predicates: std::collections::HashSet<oxigraph::model::NamedNode> =
                std::collections::HashSet::new();
            if mode == ClosedWorldConstraintMode::S223Relation {
                for type_quad in store.quads_for_pattern(
                    None,
                    Some(oxigraph::model::vocab::rdf::TYPE),
                    None,
                    None,
                ) {
                    let type_quad = type_quad.map_err(|err| format!("store query failed: {err}"))?;
                    let predicate = match type_quad.subject {
                        oxigraph::model::NamedOrBlankNode::NamedNode(nn) => nn,
                        oxigraph::model::NamedOrBlankNode::BlankNode(_) => continue,
                    };
                    let predicate_class = type_quad.object;
                    if !matches!(
                        predicate_class,
                        oxigraph::model::Term::NamedNode(_) | oxigraph::model::Term::BlankNode(_)
                    ) {
                        continue;
                    }
                    let supers = closed_world_superclass_closure(
                        &predicate_class,
                        &direct_superclasses,
                        &mut superclass_memo,
                    );
                    if supers.contains(&relation_root) {
                        s223_relation_predicates.insert(predicate);
                    }
                }
            }

            let mut violations = Vec::new();
            for focus_term in focus_terms {
                let Some(focus_subject) = subject_ref_from_term(&focus_term) else {
                    continue;
                };

                let sh_node_shape = oxigraph::model::NamedNode::new_unchecked(
                    "http://www.w3.org/ns/shacl#NodeShape",
                );
                let mut is_node_shape = false;
                for quad in store.quads_for_pattern(
                    Some(focus_subject),
                    Some(oxigraph::model::vocab::rdf::TYPE),
                    Some(sh_node_shape.as_ref().into()),
                    None,
                ) {
                    quad.map_err(|err| format!("store query failed: {err}"))?;
                    is_node_shape = true;
                    break;
                }
                if is_node_shape {
                    continue;
                }

                let mut class_closure: std::collections::HashSet<oxigraph::model::Term> =
                    std::collections::HashSet::new();
                for class_quad in store.quads_for_pattern(
                    Some(focus_subject),
                    Some(oxigraph::model::vocab::rdf::TYPE),
                    None,
                    None,
                ) {
                    let class_quad = class_quad.map_err(|err| format!("store query failed: {err}"))?;
                    let class_term = class_quad.object;
                    if !matches!(
                        class_term,
                        oxigraph::model::Term::NamedNode(_) | oxigraph::model::Term::BlankNode(_)
                    ) {
                        continue;
                    }
                    class_closure.extend(closed_world_superclass_closure(
                        &class_term,
                        &direct_superclasses,
                        &mut superclass_memo,
                    ));
                }

                let mut has_violation = false;
                for quad in store.quads_for_pattern(Some(focus_subject), None, None, None) {
                    let quad = quad.map_err(|err| format!("store query failed: {err}"))?;
                    let predicate = quad.predicate.clone();
                    let predicate_allowed_scope = match mode {
                        ClosedWorldConstraintMode::QudtPredicate => predicate
                            .as_str()
                            .starts_with("http://qudt.org/schema/qudt"),
                        ClosedWorldConstraintMode::S223Relation => {
                            s223_relation_predicates.contains(&predicate)
                        }
                    };
                    if !predicate_allowed_scope {
                        continue;
                    }
                    let is_allowed_for_any_class = class_closure.iter().any(|class_term| {
                        allowed_by_class
                            .get(class_term)
                            .map(|predicates| predicates.contains(&predicate))
                            .unwrap_or(false)
                    });
                    if is_allowed_for_any_class {
                        continue;
                    }

                    has_violation = true;
                    break;
                }
                if has_violation {
                    violations.push(focus_term);
                }
            }

            Ok(violations)
        }

        pub fn run_specialized_node_validation(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<Vec<Violation>, String> {
            let mut violations: Vec<Violation> = Vec::new();
            let node_shape_ids: &[u64] = &[#(#supported_node_ids),*];
            let mut target_class_index: Option<TargetClassIndex> = None;

            for shape_id in node_shape_ids {
                match shape_id {
                    #(#node_arms,)*
                    _ => {}
                }
            }

            violations.sort_by(|a, b| {
                a.shape_id
                    .cmp(&b.shape_id)
                    .then_with(|| a.component_id.cmp(&b.component_id))
                    .then_with(|| a.focus.to_string().cmp(&b.focus.to_string()))
                    .then_with(|| {
                        let a_val = a
                            .value
                            .as_ref()
                            .map(|term: &oxigraph::model::Term| term.to_string())
                            .unwrap_or_default();
                        let b_val = b
                            .value
                            .as_ref()
                            .map(|term: &oxigraph::model::Term| term.to_string())
                            .unwrap_or_default();
                        a_val.cmp(&b_val)
                    })
            });

            Ok(violations)
        }
    };

    render_tokens_as_module(tokens)
}
