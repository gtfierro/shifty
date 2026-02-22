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
                SrcGenComponentKind::LanguageIn { languages } => {
                    let language_literals: Vec<LitStr> = languages
                        .iter()
                        .map(|language| LitStr::new(language, Span::call_site()))
                        .collect();
                    constraint_checks.push(quote! {
                        let allowed_languages: &[&str] = &[#(#language_literals),*];
                        for value in &values {
                            let valid = term_matches_language_in(value, allowed_languages);
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
                SrcGenComponentKind::UniqueLang { enabled } => {
                    if *enabled {
                        constraint_checks.push(quote! {
                            let mut seen_language_tags: std::collections::HashSet<String> =
                                std::collections::HashSet::new();
                            let mut duplicated_language_tags: std::collections::HashSet<String> =
                                std::collections::HashSet::new();
                            for value in &values {
                                if let oxigraph::model::Term::Literal(literal) = value {
                                    if let Some(language) = literal.language() {
                                        if !language.is_empty() {
                                            let lowered = language.to_ascii_lowercase();
                                            if !seen_language_tags.insert(lowered.clone()) {
                                                duplicated_language_tags.insert(lowered);
                                            }
                                        }
                                    }
                                }
                            }
                            for _tag in duplicated_language_tags {
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
                }
                SrcGenComponentKind::Equals { property_iri } => {
                    let property_lit = LitStr::new(property_iri, Span::call_site());
                    constraint_checks.push(quote! {
                        let other_values =
                            values_for_predicate(store, data_graph, focus, #property_lit)?;
                        let own_set: std::collections::HashSet<oxigraph::model::Term> =
                            values.iter().cloned().collect();
                        let other_set: std::collections::HashSet<oxigraph::model::Term> =
                            other_values.into_iter().collect();

                        for missing_other in own_set.difference(&other_set) {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: Some(missing_other.clone()),
                                path: Some(ResultPath::Term(predicate_term.clone())),
                            });
                        }

                        for missing_self in other_set.difference(&own_set) {
                            violations.push(Violation {
                                shape_id: #shape_id,
                                component_id: #component_id_value,
                                focus: focus.clone(),
                                value: Some(missing_self.clone()),
                                path: Some(ResultPath::Term(predicate_term.clone())),
                            });
                        }
                    });
                }
                SrcGenComponentKind::Disjoint { property_iri } => {
                    let property_lit = LitStr::new(property_iri, Span::call_site());
                    constraint_checks.push(quote! {
                        let other_values =
                            values_for_predicate(store, data_graph, focus, #property_lit)?;
                        let other_set: std::collections::HashSet<oxigraph::model::Term> =
                            other_values.into_iter().collect();
                        for value in &values {
                            if other_set.contains(value) {
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
                SrcGenComponentKind::LessThan { property_iri } => {
                    let property_lit = LitStr::new(property_iri, Span::call_site());
                    constraint_checks.push(quote! {
                        let other_values =
                            values_for_predicate(store, data_graph, focus, #property_lit)?;
                        for value in &values {
                            for other_value in &other_values {
                                let valid = compare_terms_with_operator(
                                    store,
                                    value,
                                    other_value,
                                    "<",
                                    false,
                                )?;
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
                        }
                    });
                }
                SrcGenComponentKind::LessThanOrEquals { property_iri } => {
                    let property_lit = LitStr::new(property_iri, Span::call_site());
                    constraint_checks.push(quote! {
                        let other_values =
                            values_for_predicate(store, data_graph, focus, #property_lit)?;
                        for value in &values {
                            for other_value in &other_values {
                                let valid = compare_terms_with_operator(
                                    store,
                                    value,
                                    other_value,
                                    "<=",
                                    false,
                                )?;
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

        fn lang_matches(tag: &str, range: &str) -> bool {
            if range == "*" {
                return !tag.is_empty();
            }
            let tag_lower = tag.to_ascii_lowercase();
            let range_lower = range.to_ascii_lowercase();
            if tag_lower == range_lower {
                return true;
            }
            tag_lower.starts_with(&format!("{range_lower}-"))
        }

        fn term_matches_language_in(term: &oxigraph::model::Term, allowed_languages: &[&str]) -> bool {
            let oxigraph::model::Term::Literal(literal) = term else {
                return false;
            };
            let literal_language = literal.language().unwrap_or("");
            if allowed_languages.is_empty() {
                return false;
            }
            allowed_languages
                .iter()
                .any(|allowed| lang_matches(literal_language, allowed))
        }

        fn decimal_from_literal(literal: oxigraph::model::LiteralRef<'_>) -> Option<oxsdatatypes::Decimal> {
            let datatype = literal.datatype();
            if datatype == oxigraph::model::vocab::xsd::INTEGER
                || datatype == oxigraph::model::vocab::xsd::DECIMAL
            {
                use std::str::FromStr;
                oxsdatatypes::Decimal::from_str(literal.value()).ok()
            } else {
                None
            }
        }

        fn float_from_literal(literal: oxigraph::model::LiteralRef<'_>) -> Option<f64> {
            let datatype = literal.datatype();
            if datatype == oxigraph::model::vocab::xsd::FLOAT
                || datatype == oxigraph::model::vocab::xsd::DOUBLE
            {
                literal
                    .value()
                    .parse::<f64>()
                    .ok()
                    .filter(|value| !value.is_nan())
            } else {
                None
            }
        }

        fn compare_terms_fast(
            left: &oxigraph::model::Term,
            right: &oxigraph::model::Term,
        ) -> Option<std::cmp::Ordering> {
            let (oxigraph::model::Term::Literal(left_literal), oxigraph::model::Term::Literal(right_literal)) =
                (left, right)
            else {
                return None;
            };

            if let (Some(left_decimal), Some(right_decimal)) = (
                decimal_from_literal(left_literal.as_ref()),
                decimal_from_literal(right_literal.as_ref()),
            ) {
                return left_decimal.partial_cmp(&right_decimal);
            }

            if let (Some(left_float), Some(right_float)) = (
                float_from_literal(left_literal.as_ref()),
                float_from_literal(right_literal.as_ref()),
            ) {
                return left_float.partial_cmp(&right_float);
            }

            None
        }

        fn escape_sparql_string(value: &str) -> String {
            let mut out = String::with_capacity(value.len());
            for ch in value.chars() {
                match ch {
                    '\\' => out.push_str("\\\\"),
                    '"' => out.push_str("\\\""),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c => out.push(c),
                }
            }
            out
        }

        fn term_to_sparql(term: &oxigraph::model::Term) -> String {
            match term {
                oxigraph::model::Term::NamedNode(node) => format!("<{}>", node.as_str()),
                oxigraph::model::Term::BlankNode(node) => format!("_:{}", node.as_str()),
                oxigraph::model::Term::Literal(literal) => {
                    if let Some(language) = literal.language() {
                        format!("\"{}\"@{}", escape_sparql_string(literal.value()), language)
                    } else {
                        format!(
                            "\"{}\"^^<{}>",
                            escape_sparql_string(literal.value()),
                            literal.datatype().as_str(),
                        )
                    }
                }
            }
        }

        fn compare_terms_with_operator(
            store: &oxigraph::store::Store,
            left: &oxigraph::model::Term,
            right: &oxigraph::model::Term,
            operator: &str,
            incomparable_is_valid: bool,
        ) -> Result<bool, String> {
            if let Some(ordering) = compare_terms_fast(left, right) {
                let result = match operator {
                    "<" => ordering == std::cmp::Ordering::Less,
                    "<=" => {
                        ordering == std::cmp::Ordering::Less || ordering == std::cmp::Ordering::Equal
                    }
                    ">" => ordering == std::cmp::Ordering::Greater,
                    ">=" => {
                        ordering == std::cmp::Ordering::Greater
                            || ordering == std::cmp::Ordering::Equal
                    }
                    _ => false,
                };
                return Ok(result);
            }

            let query = format!(
                "ASK {{ FILTER({} {} {}) }}",
                term_to_sparql(left),
                operator,
                term_to_sparql(right),
            );

            #[allow(deprecated)]
            let result = store.query(query.as_str());
            match result {
                Ok(oxigraph::sparql::QueryResults::Boolean(valid)) => Ok(valid),
                Ok(_) => Ok(false),
                Err(_) => Ok(incomparable_is_valid),
            }
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
