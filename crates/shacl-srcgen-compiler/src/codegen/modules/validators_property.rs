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

    let mut match_arms: Vec<TokenStream> = Vec::new();
    let mut supported_shapes = 0usize;

    for shape in ir.property_shapes.iter().filter(|shape| shape.supported) {
        let Some(predicate) = shape.path_predicate.as_ref() else {
            continue;
        };
        supported_shapes += 1;

        let shape_id = shape.id;
        let predicate_lit = LitStr::new(predicate, Span::call_site());

        let mut constraint_checks: Vec<TokenStream> = Vec::new();
        for component_id in &shape.constraints {
            let component_id_value = *component_id;
            let Some(kind) = component_by_id.get(component_id) else {
                continue;
            };

            match kind {
                SrcGenComponentKind::MinCount { min_count } => {
                    let min_count = *min_count as usize;
                    constraint_checks.push(quote! {
                        if values.len() < #min_count {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: None,
                                path: Some(ResultPath::Term(predicate_term.clone())),
                            });
                        }
                    });
                }
                SrcGenComponentKind::MaxCount { max_count } => {
                    let max_count = *max_count as usize;
                    constraint_checks.push(quote! {
                        if values.len() > #max_count {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: None,
                                path: Some(ResultPath::Term(predicate_term.clone())),
                            });
                        }
                    });
                }
                SrcGenComponentKind::Datatype { datatype_iri } => {
                    let datatype_lit = LitStr::new(datatype_iri, Span::call_site());
                    constraint_checks.push(quote! {
                        for value in &values {
                            let valid = match value {
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
                                    value: Some(value.clone()),
                                    path: Some(ResultPath::Term(predicate_term.clone())),
                                });
                            }
                        }
                    });
                }
                SrcGenComponentKind::Class { class_iri } => {
                    let class_lit = LitStr::new(class_iri, Span::call_site());
                    constraint_checks.push(quote! {
                        for value in &values {
                            let valid = term_has_rdf_type(store, data_graph, value, #class_lit)?;
                            if !valid {
                                violations.push(Violation {
                                    shape_id: #shape_id,
                                    component_id: #component_id_value,
                                    focus: focus.clone(),
                                    value: Some(value.clone()),
                                    path: Some(ResultPath::Term(predicate_term.clone())),
                                });
                            }
                        }
                    });
                }
                SrcGenComponentKind::PropertyLink => {}
                SrcGenComponentKind::Unsupported { .. } => {}
            }
        }

        match_arms.push(quote! {
            #shape_id => {
                let values = values_for_predicate(store, data_graph, focus, #predicate_lit)?;
                let predicate_term = oxigraph::model::Term::NamedNode(
                    oxigraph::model::NamedNode::new_unchecked(#predicate_lit),
                );
                #(#constraint_checks)*
            }
        });
    }

    let tokens = quote! {
        pub const GENERATED_PROPERTY_VALIDATORS: usize = #supported_shapes;

        fn subject_ref_from_term<'a>(
            term: &'a oxigraph::model::Term,
        ) -> Option<oxigraph::model::NamedOrBlankNodeRef<'a>> {
            match term {
                oxigraph::model::Term::NamedNode(node) => {
                    Some(oxigraph::model::NamedOrBlankNodeRef::NamedNode(node.as_ref()))
                }
                oxigraph::model::Term::BlankNode(node) => {
                    Some(oxigraph::model::NamedOrBlankNodeRef::BlankNode(node.as_ref()))
                }
                _ => None,
            }
        }

        fn values_for_predicate(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            focus: &oxigraph::model::Term,
            predicate_iri: &str,
        ) -> Result<Vec<oxigraph::model::Term>, String> {
            let Some(subject_ref) = subject_ref_from_term(focus) else {
                return Ok(Vec::new());
            };
            let predicate = oxigraph::model::NamedNode::new(predicate_iri)
                .map_err(|err| format!("invalid path predicate IRI {predicate_iri}: {err}"))?;
            let graph_ref = oxigraph::model::GraphNameRef::NamedNode(data_graph.as_ref());
            let mut values = Vec::new();
            for quad in store.quads_for_pattern(
                Some(subject_ref),
                Some(predicate.as_ref()),
                None,
                Some(graph_ref),
            ) {
                let quad = quad.map_err(|err| format!("store query failed: {err}"))?;
                values.push(quad.object);
            }
            values.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
            Ok(values)
        }

        pub fn term_has_rdf_type(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            term: &oxigraph::model::Term,
            class_iri: &str,
        ) -> Result<bool, String> {
            let Some(subject_ref) = subject_ref_from_term(term) else {
                return Ok(false);
            };
            let graph_ref = oxigraph::model::GraphNameRef::NamedNode(data_graph.as_ref());
            for quad in store.quads_for_pattern(
                Some(subject_ref),
                Some(oxigraph::model::vocab::rdf::TYPE),
                None,
                Some(graph_ref),
            ) {
                let quad = quad.map_err(|err| format!("store query failed: {err}"))?;
                if let oxigraph::model::Term::NamedNode(node) = quad.object {
                    if node.as_str() == class_iri {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }

        pub fn validate_supported_property_shape(
            shape_id: u64,
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            focus: &oxigraph::model::Term,
            violations: &mut Vec<Violation>,
        ) -> Result<(), String> {
            match shape_id {
                #(#match_arms,)*
                _ => {}
            }
            Ok(())
        }
    };

    render_tokens_as_module(tokens)
}
