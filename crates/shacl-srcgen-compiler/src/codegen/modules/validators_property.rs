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
                SrcGenComponentKind::NodeKind { node_kind_iri } => {
                    let node_kind_lit = LitStr::new(node_kind_iri, Span::call_site());
                    constraint_checks.push(quote! {
                        for value in &values {
                            let valid = shacl_node_kind_matches(value, #node_kind_lit);
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
                SrcGenComponentKind::MinLength { min_length } => {
                    let min_length = *min_length as usize;
                    constraint_checks.push(quote! {
                        for value in &values {
                            let valid = term_string_for_text_constraints(value)
                                .map(|text| text.chars().count() >= #min_length)
                                .unwrap_or(false);
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
                SrcGenComponentKind::MaxLength { max_length } => {
                    let max_length = *max_length as usize;
                    constraint_checks.push(quote! {
                        for value in &values {
                            let valid = term_string_for_text_constraints(value)
                                .map(|text| text.chars().count() <= #max_length)
                                .unwrap_or(false);
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
                SrcGenComponentKind::Pattern { pattern, flags } => {
                    let pattern_lit = LitStr::new(pattern, Span::call_site());
                    let flags_token = if let Some(flags) = flags {
                        let flags_lit = LitStr::new(flags, Span::call_site());
                        quote! { Some(#flags_lit) }
                    } else {
                        quote! { None }
                    };
                    constraint_checks.push(quote! {
                        let regex = build_pattern_regex(#pattern_lit, #flags_token)?;
                        for value in &values {
                            let valid = term_string_for_text_constraints(value)
                                .map(|text| regex.is_match(&text))
                                .unwrap_or(false);
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

        fn validation_graphs(
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<Vec<oxigraph::model::NamedNode>, String> {
            let mut graphs = vec![data_graph.clone()];
            let shape_graph = oxigraph::model::NamedNode::new(SHAPE_GRAPH)
                .map_err(|err| format!("invalid SHAPE_GRAPH IRI: {err}"))?;
            if shape_graph.as_str() != data_graph.as_str() {
                graphs.push(shape_graph);
            }
            Ok(graphs)
        }

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
            let mut values = Vec::new();
            for graph in validation_graphs(data_graph)? {
                let graph_ref = oxigraph::model::GraphNameRef::NamedNode(graph.as_ref());
                for quad in store.quads_for_pattern(
                    Some(subject_ref),
                    Some(predicate.as_ref()),
                    None,
                    Some(graph_ref),
                ) {
                    let quad = quad.map_err(|err| format!("store query failed: {err}"))?;
                    values.push(quad.object);
                }
            }
            values.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
            values.dedup();
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
            for graph in validation_graphs(data_graph)? {
                let graph_ref = oxigraph::model::GraphNameRef::NamedNode(graph.as_ref());
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
            }
            Ok(false)
        }

        pub fn shacl_node_kind_matches(term: &oxigraph::model::Term, node_kind_iri: &str) -> bool {
            let is_iri = matches!(term, oxigraph::model::Term::NamedNode(_));
            let is_blank = matches!(term, oxigraph::model::Term::BlankNode(_));
            let is_literal = matches!(term, oxigraph::model::Term::Literal(_));
            match node_kind_iri {
                "http://www.w3.org/ns/shacl#IRI" => is_iri,
                "http://www.w3.org/ns/shacl#BlankNode" => is_blank,
                "http://www.w3.org/ns/shacl#Literal" => is_literal,
                "http://www.w3.org/ns/shacl#BlankNodeOrIRI" => is_blank || is_iri,
                "http://www.w3.org/ns/shacl#BlankNodeOrLiteral" => is_blank || is_literal,
                "http://www.w3.org/ns/shacl#IRIOrLiteral" => is_iri || is_literal,
                _ => false,
            }
        }

        pub fn term_string_for_text_constraints(term: &oxigraph::model::Term) -> Option<String> {
            match term {
                oxigraph::model::Term::NamedNode(node) => Some(node.as_str().to_string()),
                oxigraph::model::Term::Literal(literal) => Some(literal.value().to_string()),
                oxigraph::model::Term::BlankNode(_) => None,
            }
        }

        pub fn build_pattern_regex(
            pattern: &str,
            flags: Option<&str>,
        ) -> Result<regex::Regex, String> {
            let mut pattern_builder = regex::RegexBuilder::new(pattern);
            if let Some(flags) = flags {
                if flags.contains('i') {
                    pattern_builder.case_insensitive(true);
                }
            }
            pattern_builder
                .build()
                .map_err(|err| format!("invalid regex pattern {pattern:?}: {err}"))
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
