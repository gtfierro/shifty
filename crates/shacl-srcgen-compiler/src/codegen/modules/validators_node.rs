use crate::codegen::render_tokens_as_module;
use crate::ir::{SrcGenComponentKind, SrcGenIR};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use std::collections::HashMap;
use syn::LitStr;

pub fn generate(ir: &SrcGenIR) -> Result<String, String> {
    let component_by_id: HashMap<u64, &SrcGenComponentKind> = ir
        .components
        .iter()
        .map(|component| (component.id, &component.kind))
        .collect();

    let mut node_arms: Vec<TokenStream> = Vec::new();
    let mut supported_shapes = 0usize;
    let mut supported_node_ids: Vec<u64> = Vec::new();

    for shape in ir.node_shapes.iter().filter(|shape| shape.supported) {
        supported_shapes += 1;
        supported_node_ids.push(shape.id);
        let shape_id = shape.id;

        let focus_sources: Vec<TokenStream> = shape
            .target_classes
            .iter()
            .map(|class_iri| {
                let class_lit = LitStr::new(class_iri, Span::call_site());
                quote! {
                    focus_nodes.extend(focus_nodes_for_target_class(store, data_graph, #class_lit)?);
                }
            })
            .collect();

        let mut node_constraint_checks: Vec<TokenStream> = Vec::new();
        for component_id in &shape.constraints {
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
                SrcGenComponentKind::PropertyLink => {}
                SrcGenComponentKind::Datatype { .. } => {}
                SrcGenComponentKind::MinCount { .. } => {}
                SrcGenComponentKind::MaxCount { .. } => {}
                SrcGenComponentKind::Unsupported { .. } => {}
            }
        }

        let property_calls: Vec<TokenStream> = shape
            .property_shapes
            .iter()
            .map(|property_shape_id| {
                quote! {
                    validate_supported_property_shape(#property_shape_id, store, data_graph, focus, &mut violations)?;
                }
            })
            .collect();

        node_arms.push(quote! {
            #shape_id => {
                let mut focus_nodes: Vec<oxigraph::model::Term> = Vec::new();
                #(#focus_sources)*
                focus_nodes.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
                focus_nodes.dedup();

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

        fn focus_nodes_for_target_class(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            class_iri: &str,
        ) -> Result<Vec<oxigraph::model::Term>, String> {
            let class_node = oxigraph::model::NamedNode::new(class_iri)
                .map_err(|err| format!("invalid class target IRI {class_iri}: {err}"))?;
            let graph_ref = oxigraph::model::GraphNameRef::NamedNode(data_graph.as_ref());
            let mut nodes = Vec::new();
            for quad in store.quads_for_pattern(
                None,
                Some(oxigraph::model::vocab::rdf::TYPE),
                Some(class_node.as_ref().into()),
                Some(graph_ref),
            ) {
                let quad = quad.map_err(|err| format!("store query failed: {err}"))?;
                nodes.push(oxigraph::model::Term::from(quad.subject));
            }
            nodes.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
            nodes.dedup();
            Ok(nodes)
        }

        pub fn run_specialized_node_validation(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<Vec<Violation>, String> {
            let mut violations: Vec<Violation> = Vec::new();
            let node_shape_ids: &[u64] = &[#(#supported_node_ids),*];

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
