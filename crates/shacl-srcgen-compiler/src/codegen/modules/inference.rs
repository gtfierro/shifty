use crate::codegen::render_tokens_as_module;
use crate::ir::{SrcGenIR, SrcGenRuleKind, SrcGenRuleObject, SrcGenRuleSubject};
use oxigraph::model::Term;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use regex::Regex;
use spargebra::algebra::{Expression, GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNodePattern, TermPattern};
use spargebra::{Query as AlgebraQuery, SparqlParser};
use std::sync::OnceLock;
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

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GeneratedFocusDependency {
    All,
    TargetClass(String),
    TargetSubjectsOf(String),
    TargetObjectsOf(String),
    OutgoingPredicate(String),
    IncomingPredicate(String),
    AnyPredicateParticipant(String),
}

#[derive(Debug, Clone, Default)]
struct GeneratedSparqlPrefilter {
    direct_outgoing_predicates: Vec<String>,
    direct_incoming_predicates: Vec<String>,
    direct_classes: Vec<String>,
}

#[derive(Debug, Clone)]
enum GeneratedNativeSparqlPath {
    NamedNode(String),
    ReverseNamedNode(String),
    Sequence(Vec<GeneratedNativeSparqlPath>),
}

#[derive(Debug, Clone)]
enum GeneratedNativeSparqlRule {
    PathCopy {
        construct_predicate: String,
        source_path: GeneratedNativeSparqlPath,
    },
    EqualityConstant {
        construct_predicate: String,
        left_predicate: String,
        right_predicate: String,
        object: Term,
    },
}

#[derive(Debug, Clone)]
struct GeneratedRuleMetadata {
    dependencies: Vec<GeneratedFocusDependency>,
    sparql_prefilter: Option<GeneratedSparqlPrefilter>,
    native_sparql: Option<GeneratedNativeSparqlRule>,
}

fn dedup_preserve_order<T: Eq + std::hash::Hash + Clone>(items: &mut Vec<T>) {
    let mut seen = std::collections::HashSet::new();
    items.retain(|item| seen.insert(item.clone()));
}

fn generated_dependencies_for_targets(
    rule: &crate::ir::SrcGenRule,
) -> Vec<GeneratedFocusDependency> {
    let mut dependencies = Vec::new();
    dependencies.extend(
        rule.target_classes
            .iter()
            .cloned()
            .map(GeneratedFocusDependency::TargetClass),
    );
    dependencies.extend(
        rule.target_subjects_of
            .iter()
            .cloned()
            .map(GeneratedFocusDependency::TargetSubjectsOf),
    );
    dependencies.extend(
        rule.target_objects_of
            .iter()
            .cloned()
            .map(GeneratedFocusDependency::TargetObjectsOf),
    );
    if !rule.target_advanced_select_queries.is_empty() {
        dependencies.push(GeneratedFocusDependency::All);
    }
    dependencies
}

fn generated_dependencies_for_rule(rule: &crate::ir::SrcGenRule) -> Vec<GeneratedFocusDependency> {
    match &rule.kind {
        SrcGenRuleKind::Triple {
            condition_shape_iris,
            ..
        } => {
            if condition_shape_iris.is_empty() {
                Vec::new()
            } else {
                vec![GeneratedFocusDependency::All]
            }
        }
        SrcGenRuleKind::Sparql {
            query,
            condition_shape_iris,
        } => {
            let where_body = query
                .split_once("WHERE")
                .map(|(_, tail)| tail)
                .unwrap_or(query.as_str());
            let mut dependencies = extract_sparql_dependencies(where_body);
            if !condition_shape_iris.is_empty() {
                dependencies.push(GeneratedFocusDependency::All);
            }
            if dependencies.is_empty() {
                dependencies.push(GeneratedFocusDependency::All);
            }
            dependencies
        }
        SrcGenRuleKind::Unsupported { .. } => vec![GeneratedFocusDependency::All],
    }
}

fn generated_metadata_for_rule(rule: &crate::ir::SrcGenRule) -> GeneratedRuleMetadata {
    let mut dependencies = generated_dependencies_for_targets(rule);
    dependencies.extend(generated_dependencies_for_rule(rule));
    dedup_preserve_order(&mut dependencies);

    let (sparql_prefilter, native_sparql) = match &rule.kind {
        SrcGenRuleKind::Sparql { query, .. } => (
            generated_sparql_prefilter(query),
            generated_native_sparql_rule(query),
        ),
        _ => (None, None),
    };

    GeneratedRuleMetadata {
        dependencies,
        sparql_prefilter,
        native_sparql,
    }
}

fn generated_sparql_prefilter(query: &str) -> Option<GeneratedSparqlPrefilter> {
    let where_body = query
        .split_once("WHERE")
        .map(|(_, tail)| tail)
        .unwrap_or(query);
    let upper = where_body.to_ascii_uppercase();
    if upper.contains("OPTIONAL")
        || upper.contains("UNION")
        || upper.contains("MINUS")
        || upper.contains("NOT EXISTS")
    {
        return None;
    }

    static DIRECT_OUTGOING_RE: OnceLock<Regex> = OnceLock::new();
    static DIRECT_INCOMING_RE: OnceLock<Regex> = OnceLock::new();
    static DIRECT_TYPE_RE: OnceLock<Regex> = OnceLock::new();

    let mut prefilter = GeneratedSparqlPrefilter::default();

    let direct_outgoing_re = DIRECT_OUTGOING_RE.get_or_init(|| {
        Regex::new(
            r#"(?i)\$this\s+(<[^>]+>)\s+(?:\?[A-Za-z_][A-Za-z0-9_]*|\$this|<[^>]+>|\"[^\"]*\")"#,
        )
        .expect("valid direct outgoing regex")
    });
    for capture in direct_outgoing_re.captures_iter(where_body) {
        if let Some(predicate) = capture.get(1).and_then(|m| parse_iri_str(m.as_str())) {
            prefilter
                .direct_outgoing_predicates
                .push(predicate.to_string());
        }
    }

    let direct_incoming_re = DIRECT_INCOMING_RE.get_or_init(|| {
        Regex::new(r#"(?i)(?:\?[A-Za-z_][A-Za-z0-9_]*|<[^>]+>|\"[^\"]*\")\s+(<[^>]+>)\s+\$this"#)
            .expect("valid direct incoming regex")
    });
    for capture in direct_incoming_re.captures_iter(where_body) {
        if let Some(predicate) = capture.get(1).and_then(|m| parse_iri_str(m.as_str())) {
            prefilter
                .direct_incoming_predicates
                .push(predicate.to_string());
        }
    }

    let direct_type_re = DIRECT_TYPE_RE.get_or_init(|| {
        Regex::new(
            r#"(?i)\$this\s+(?:a|<http://www\.w3\.org/1999/02/22-rdf-syntax-ns#type>)\s+(<[^>]+>)"#,
        )
        .expect("valid direct type regex")
    });
    for capture in direct_type_re.captures_iter(where_body) {
        if let Some(class) = capture.get(1).and_then(|m| parse_iri_str(m.as_str())) {
            prefilter.direct_classes.push(class.to_string());
        }
    }

    prefilter.direct_outgoing_predicates.sort();
    prefilter.direct_outgoing_predicates.dedup();
    prefilter.direct_incoming_predicates.sort();
    prefilter.direct_incoming_predicates.dedup();
    dedup_preserve_order(&mut prefilter.direct_classes);

    if prefilter.direct_outgoing_predicates.is_empty()
        && prefilter.direct_incoming_predicates.is_empty()
        && prefilter.direct_classes.is_empty()
    {
        None
    } else {
        Some(prefilter)
    }
}

fn generated_native_sparql_rule(query: &str) -> Option<GeneratedNativeSparqlRule> {
    let algebra = SparqlParser::new().parse_query(query).ok()?;
    generated_native_path_copy_rule(&algebra)
        .or_else(|| generated_native_equality_constant_rule(&algebra))
}

fn generated_native_path_copy_rule(query: &AlgebraQuery) -> Option<GeneratedNativeSparqlRule> {
    let AlgebraQuery::Construct {
        template, pattern, ..
    } = query
    else {
        return None;
    };
    let [triple] = template.as_slice() else {
        return None;
    };
    if !generated_term_pattern_is_this(&triple.subject) {
        return None;
    }
    let construct_predicate = generated_named_node_pattern_named_node(&triple.predicate)?;
    let target_var = generated_term_pattern_variable_name(&triple.object)?;
    let source_path = generated_native_path_from_graph_pattern(pattern, target_var)?;
    Some(GeneratedNativeSparqlRule::PathCopy {
        construct_predicate,
        source_path,
    })
}

fn generated_native_equality_constant_rule(
    query: &AlgebraQuery,
) -> Option<GeneratedNativeSparqlRule> {
    let AlgebraQuery::Construct {
        template, pattern, ..
    } = query
    else {
        return None;
    };
    let [triple] = template.as_slice() else {
        return None;
    };
    if !generated_term_pattern_is_this(&triple.subject) {
        return None;
    }
    let construct_predicate = generated_named_node_pattern_named_node(&triple.predicate)?;
    let object = generated_term_pattern_constant(&triple.object)?;
    let GraphPattern::Filter { inner, expr } = generated_strip_simple_graph_pattern(pattern) else {
        return None;
    };
    let Expression::Equal(left, right) = expr else {
        return None;
    };
    let left_var = generated_expression_variable_name(left.as_ref())?;
    let right_var = generated_expression_variable_name(right.as_ref())?;
    let GraphPattern::Bgp { patterns } = generated_strip_simple_graph_pattern(inner.as_ref())
    else {
        return None;
    };
    let [left_pattern, right_pattern] = patterns.as_slice() else {
        return None;
    };
    let left_predicate = generated_named_node_pattern_named_node(&left_pattern.predicate)?;
    let right_predicate = generated_named_node_pattern_named_node(&right_pattern.predicate)?;
    if !generated_term_pattern_is_this(&left_pattern.subject)
        || !generated_term_pattern_is_this(&right_pattern.subject)
    {
        return None;
    }
    let first_var = generated_term_pattern_variable_name(&left_pattern.object)?;
    let second_var = generated_term_pattern_variable_name(&right_pattern.object)?;
    let filter_matches = (first_var == left_var && second_var == right_var)
        || (first_var == right_var && second_var == left_var);
    if !filter_matches {
        return None;
    }
    Some(GeneratedNativeSparqlRule::EqualityConstant {
        construct_predicate,
        left_predicate,
        right_predicate,
        object,
    })
}

fn generated_native_path_from_graph_pattern(
    pattern: &GraphPattern,
    target_var: &str,
) -> Option<GeneratedNativeSparqlPath> {
    match generated_strip_simple_graph_pattern(pattern) {
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            if !generated_term_pattern_is_this(subject)
                || generated_term_pattern_variable_name(object)? != target_var
            {
                return None;
            }
            generated_native_path_from_property_path(path)
        }
        other => {
            let mut patterns = Vec::new();
            generated_collect_named_triple_patterns(other, &mut patterns)?;
            generated_native_path_from_named_triples(&patterns, target_var)
        }
    }
}

fn generated_native_path_from_property_path(
    path: &PropertyPathExpression,
) -> Option<GeneratedNativeSparqlPath> {
    match path {
        PropertyPathExpression::NamedNode(predicate) => Some(GeneratedNativeSparqlPath::NamedNode(
            predicate.as_str().to_string(),
        )),
        PropertyPathExpression::Reverse(inner) => match inner.as_ref() {
            PropertyPathExpression::NamedNode(predicate) => Some(
                GeneratedNativeSparqlPath::ReverseNamedNode(predicate.as_str().to_string()),
            ),
            _ => None,
        },
        PropertyPathExpression::Sequence(left, right) => {
            let mut segments = Vec::new();
            generated_flatten_native_path_sequence(left.as_ref(), &mut segments)?;
            generated_flatten_native_path_sequence(right.as_ref(), &mut segments)?;
            Some(GeneratedNativeSparqlPath::Sequence(segments))
        }
        _ => None,
    }
}

fn generated_flatten_native_path_sequence(
    path: &PropertyPathExpression,
    segments: &mut Vec<GeneratedNativeSparqlPath>,
) -> Option<()> {
    match path {
        PropertyPathExpression::Sequence(left, right) => {
            generated_flatten_native_path_sequence(left.as_ref(), segments)?;
            generated_flatten_native_path_sequence(right.as_ref(), segments)?;
            Some(())
        }
        other => {
            segments.push(generated_native_path_from_property_path(other)?);
            Some(())
        }
    }
}

fn generated_collect_named_triple_patterns<'a>(
    pattern: &'a GraphPattern,
    patterns: &mut Vec<&'a spargebra::term::TriplePattern>,
) -> Option<()> {
    match generated_strip_simple_graph_pattern(pattern) {
        GraphPattern::Bgp {
            patterns: bgp_patterns,
        } => {
            for triple in bgp_patterns {
                generated_named_node_pattern_named_node(&triple.predicate)?;
                patterns.push(triple);
            }
            Some(())
        }
        GraphPattern::Join { left, right } => {
            generated_collect_named_triple_patterns(left.as_ref(), patterns)?;
            generated_collect_named_triple_patterns(right.as_ref(), patterns)
        }
        _ => None,
    }
}

fn generated_native_path_from_named_triples(
    patterns: &[&spargebra::term::TriplePattern],
    target_var: &str,
) -> Option<GeneratedNativeSparqlPath> {
    let mut used = vec![false; patterns.len()];
    let mut segments = Vec::new();
    let mut current = TermPattern::Variable(spargebra::term::Variable::new_unchecked("this"));

    loop {
        if generated_term_pattern_variable_name(&current) == Some(target_var) {
            break;
        }

        let mut next_match: Option<(usize, GeneratedNativeSparqlPath, TermPattern)> = None;
        for (index, triple) in patterns.iter().enumerate() {
            if used[index] {
                continue;
            }
            let predicate = generated_named_node_pattern_named_node(&triple.predicate)?;
            let candidate = if triple.subject == current {
                Some((
                    index,
                    GeneratedNativeSparqlPath::NamedNode(predicate),
                    triple.object.clone(),
                ))
            } else if triple.object == current {
                Some((
                    index,
                    GeneratedNativeSparqlPath::ReverseNamedNode(predicate),
                    triple.subject.clone(),
                ))
            } else {
                None
            };

            if let Some(candidate) = candidate {
                if next_match.is_some() {
                    return None;
                }
                next_match = Some(candidate);
            }
        }

        let Some((index, segment, next)) = next_match else {
            return None;
        };
        used[index] = true;
        segments.push(segment);
        current = next;
    }

    if used.iter().any(|used| !used) || segments.is_empty() {
        return None;
    }

    Some(if segments.len() == 1 {
        segments.pop().expect("single segment")
    } else {
        GeneratedNativeSparqlPath::Sequence(segments)
    })
}

fn generated_strip_simple_graph_pattern(mut pattern: &GraphPattern) -> &GraphPattern {
    loop {
        pattern = match pattern {
            GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner }
            | GraphPattern::Slice { inner, .. }
            | GraphPattern::OrderBy { inner, .. }
            | GraphPattern::Project { inner, .. }
            | GraphPattern::Group { inner, .. }
            | GraphPattern::Extend { inner, .. } => inner.as_ref(),
            other => return other,
        };
    }
}

fn generated_term_pattern_is_this(term: &TermPattern) -> bool {
    matches!(term, TermPattern::Variable(var) if var.as_str() == "this")
}

fn generated_term_pattern_variable_name<'a>(term: &'a TermPattern) -> Option<&'a str> {
    match term {
        TermPattern::Variable(var) => Some(var.as_str()),
        _ => None,
    }
}

fn generated_term_pattern_constant(term: &TermPattern) -> Option<Term> {
    match term {
        TermPattern::NamedNode(node) => Some(Term::NamedNode(node.clone())),
        TermPattern::Literal(lit) => Some(Term::Literal(lit.clone())),
        _ => None,
    }
}

fn generated_named_node_pattern_named_node(pattern: &NamedNodePattern) -> Option<String> {
    match pattern {
        NamedNodePattern::NamedNode(node) => Some(node.as_str().to_string()),
        NamedNodePattern::Variable(_) => None,
    }
}

fn generated_expression_variable_name(expression: &Expression) -> Option<&str> {
    match expression {
        Expression::Variable(var) => Some(var.as_str()),
        _ => None,
    }
}

fn parse_iri_str(token: &str) -> Option<String> {
    token
        .strip_prefix('<')
        .and_then(|value| value.strip_suffix('>'))
        .map(str::to_string)
}

fn extract_sparql_dependencies(query: &str) -> Vec<GeneratedFocusDependency> {
    static TYPE_CLASS_RE: OnceLock<Regex> = OnceLock::new();
    static PREDICATE_RE: OnceLock<Regex> = OnceLock::new();

    let mut dependencies = Vec::new();
    let type_class_re = TYPE_CLASS_RE.get_or_init(|| {
        Regex::new(
            r#"(?i)(?:\?[A-Za-z_][A-Za-z0-9_]*|\$this)\s+(?:a|<http://www\.w3\.org/1999/02/22-rdf-syntax-ns#type>)\s+(<[^>]+>)"#,
        )
        .expect("valid rdf:type regex")
    });
    for capture in type_class_re.captures_iter(query) {
        if let Some(class) = capture.get(1).and_then(|m| parse_iri_str(m.as_str())) {
            dependencies.push(GeneratedFocusDependency::TargetClass(class.to_string()));
        }
    }

    let predicate_re = PREDICATE_RE.get_or_init(|| {
        Regex::new(r#"(?i)(?:\?[A-Za-z_][A-Za-z0-9_]*|\$this)\s+(<[^>]+>)\s+(?:\?[A-Za-z_][A-Za-z0-9_]*|\$this|<[^>]+>|\"[^\"]*\")"#)
            .expect("valid predicate regex")
    });
    for capture in predicate_re.captures_iter(query) {
        if let Some(predicate) = capture.get(1).and_then(|m| parse_iri_str(m.as_str())) {
            dependencies.push(GeneratedFocusDependency::AnyPredicateParticipant(
                predicate.to_string(),
            ));
        }
    }

    dependencies
}

fn focus_dependency_expr(dependency: &GeneratedFocusDependency) -> TokenStream {
    match dependency {
        GeneratedFocusDependency::All => quote! { FocusDependency::All },
        GeneratedFocusDependency::TargetClass(class) => {
            let class_lit = LitStr::new(class, Span::call_site());
            quote! {
                FocusDependency::TargetClass(
                    oxigraph::model::Term::NamedNode(
                        oxigraph::model::NamedNode::new_unchecked(#class_lit),
                    ),
                )
            }
        }
        GeneratedFocusDependency::TargetSubjectsOf(predicate)
        | GeneratedFocusDependency::OutgoingPredicate(predicate)
        | GeneratedFocusDependency::IncomingPredicate(predicate)
        | GeneratedFocusDependency::AnyPredicateParticipant(predicate) => {
            let predicate_lit = LitStr::new(predicate, Span::call_site());
            match dependency {
                GeneratedFocusDependency::TargetSubjectsOf(_) => quote! {
                    FocusDependency::TargetSubjectsOf(
                        oxigraph::model::NamedNode::new_unchecked(#predicate_lit),
                    )
                },
                GeneratedFocusDependency::OutgoingPredicate(_) => quote! {
                    FocusDependency::OutgoingPredicate(
                        oxigraph::model::NamedNode::new_unchecked(#predicate_lit),
                    )
                },
                GeneratedFocusDependency::IncomingPredicate(_) => quote! {
                    FocusDependency::IncomingPredicate(
                        oxigraph::model::NamedNode::new_unchecked(#predicate_lit),
                    )
                },
                GeneratedFocusDependency::AnyPredicateParticipant(_) => quote! {
                    FocusDependency::AnyPredicateParticipant(
                        oxigraph::model::NamedNode::new_unchecked(#predicate_lit),
                    )
                },
                _ => unreachable!(),
            }
        }
        GeneratedFocusDependency::TargetObjectsOf(predicate) => {
            let predicate_lit = LitStr::new(predicate, Span::call_site());
            quote! {
                FocusDependency::TargetObjectsOf(
                    oxigraph::model::NamedNode::new_unchecked(#predicate_lit),
                )
            }
        }
    }
}

fn prefilter_expr(prefilter: &GeneratedSparqlPrefilter) -> TokenStream {
    let outgoing: Vec<LitStr> = prefilter
        .direct_outgoing_predicates
        .iter()
        .map(|predicate| LitStr::new(predicate, Span::call_site()))
        .collect();
    let incoming: Vec<LitStr> = prefilter
        .direct_incoming_predicates
        .iter()
        .map(|predicate| LitStr::new(predicate, Span::call_site()))
        .collect();
    let classes: Vec<LitStr> = prefilter
        .direct_classes
        .iter()
        .map(|class| LitStr::new(class, Span::call_site()))
        .collect();
    quote! {
        SparqlPrefilter {
            direct_outgoing_predicates: vec![
                #(oxigraph::model::NamedNode::new_unchecked(#outgoing)),*
            ],
            direct_incoming_predicates: vec![
                #(oxigraph::model::NamedNode::new_unchecked(#incoming)),*
            ],
            direct_classes: vec![
                #(
                    oxigraph::model::Term::NamedNode(
                        oxigraph::model::NamedNode::new_unchecked(#classes),
                    )
                ),*
            ],
        }
    }
}

fn native_sparql_path_expr(path: &GeneratedNativeSparqlPath) -> TokenStream {
    match path {
        GeneratedNativeSparqlPath::NamedNode(predicate) => {
            let predicate_lit = LitStr::new(predicate, Span::call_site());
            quote! {
                NativeSparqlPath::NamedNode(
                    oxigraph::model::NamedNode::new_unchecked(#predicate_lit),
                )
            }
        }
        GeneratedNativeSparqlPath::ReverseNamedNode(predicate) => {
            let predicate_lit = LitStr::new(predicate, Span::call_site());
            quote! {
                NativeSparqlPath::ReverseNamedNode(
                    oxigraph::model::NamedNode::new_unchecked(#predicate_lit),
                )
            }
        }
        GeneratedNativeSparqlPath::Sequence(segments) => {
            let segment_exprs: Vec<_> = segments.iter().map(native_sparql_path_expr).collect();
            quote! {
                NativeSparqlPath::Sequence(vec![#(#segment_exprs),*])
            }
        }
    }
}

fn native_sparql_expr(rule: &GeneratedNativeSparqlRule) -> TokenStream {
    match rule {
        GeneratedNativeSparqlRule::PathCopy {
            construct_predicate,
            source_path,
        } => {
            let construct_lit = LitStr::new(construct_predicate, Span::call_site());
            let source_path_expr = native_sparql_path_expr(source_path);
            quote! {
                NativeSparqlRule::PathCopy {
                    construct_predicate: oxigraph::model::NamedNode::new_unchecked(#construct_lit),
                    source_path: #source_path_expr,
                }
            }
        }
        GeneratedNativeSparqlRule::EqualityConstant {
            construct_predicate,
            left_predicate,
            right_predicate,
            object,
        } => {
            let construct_lit = LitStr::new(construct_predicate, Span::call_site());
            let left_lit = LitStr::new(left_predicate, Span::call_site());
            let right_lit = LitStr::new(right_predicate, Span::call_site());
            let object_expr = term_expr(object);
            quote! {
                NativeSparqlRule::EqualityConstant {
                    construct_predicate: oxigraph::model::NamedNode::new_unchecked(#construct_lit),
                    left_predicate: oxigraph::model::NamedNode::new_unchecked(#left_lit),
                    right_predicate: oxigraph::model::NamedNode::new_unchecked(#right_lit),
                    object: #object_expr,
                }
            }
        }
    }
}

pub fn generate(ir: &SrcGenIR) -> Result<String, String> {
    let generated_rule_count = ir.meta.rule_count;
    let fallback_rule_count = ir.rules.iter().filter(|rule| rule.fallback_only).count();
    let specialized_rules: Vec<_> = ir.rules.iter().filter(|rule| !rule.fallback_only).collect();
    let specialized_rule_count = specialized_rules.len();
    let specialized_triple_rule_count = specialized_rules
        .iter()
        .filter(|rule| matches!(rule.kind, SrcGenRuleKind::Triple { .. }))
        .count();
    let specialized_sparql_rule_count = specialized_rules
        .iter()
        .filter(|rule| matches!(rule.kind, SrcGenRuleKind::Sparql { .. }))
        .count();

    let mut metadata_match_arms: Vec<TokenStream> = Vec::new();
    let mut specialized_rule_blocks: Vec<TokenStream> = Vec::new();

    for (rule_index, rule) in specialized_rules.iter().enumerate() {
        let metadata = generated_metadata_for_rule(rule);
        let dependency_exprs: Vec<TokenStream> = metadata
            .dependencies
            .iter()
            .map(focus_dependency_expr)
            .collect();
        let prefilter_expr = metadata
            .sparql_prefilter
            .as_ref()
            .map(prefilter_expr)
            .map(|expr| quote! { Some(#expr) })
            .unwrap_or_else(|| quote! { None });
        let native_expr = metadata
            .native_sparql
            .as_ref()
            .map(native_sparql_expr)
            .map(|expr| quote! { Some(#expr) })
            .unwrap_or_else(|| quote! { None });
        metadata_match_arms.push(quote! {
            #rule_index => RuleMetadata {
                dependencies: vec![#(#dependency_exprs),*],
                sparql_prefilter: #prefilter_expr,
                native_sparql: #native_expr,
            }
        });

        let rule_id = rule.id;
        let target_class_lits: Vec<LitStr> = rule
            .target_classes
            .iter()
            .map(|iri| LitStr::new(iri, Span::call_site()))
            .collect();
        let target_node_exprs: Vec<TokenStream> = rule.target_nodes.iter().map(term_expr).collect();
        let target_subjects_of_lits: Vec<LitStr> = rule
            .target_subjects_of
            .iter()
            .map(|iri| LitStr::new(iri, Span::call_site()))
            .collect();
        let target_objects_of_lits: Vec<LitStr> = rule
            .target_objects_of
            .iter()
            .map(|iri| LitStr::new(iri, Span::call_site()))
            .collect();
        let target_advanced_select_query_lits: Vec<LitStr> = rule
            .target_advanced_select_queries
            .iter()
            .map(|query| LitStr::new(query, Span::call_site()))
            .collect();
        let rule_focus_sources = quote! {
            #(
                focus_nodes.extend(focus_nodes_for_target_class_cached_cloned(
                    &mut target_class_focus_cache,
                    &mut target_class_index,
                    store,
                    data_graph,
                    #target_class_lits,
                )?);
            )*
            #(
                focus_nodes.push(#target_node_exprs);
            )*
            #(
                focus_nodes.extend(focus_nodes_for_target_subjects_of_cached_cloned(
                    &mut target_subjects_of_focus_cache,
                    store,
                    data_graph,
                    #target_subjects_of_lits,
                )?);
            )*
            #(
                focus_nodes.extend(focus_nodes_for_target_objects_of_cached_cloned(
                    &mut target_objects_of_focus_cache,
                    store,
                    data_graph,
                    #target_objects_of_lits,
                )?);
            )*
            #(
                focus_nodes.extend(focus_nodes_for_advanced_target_select_cached_cloned(
                    &mut advanced_target_select_focus_cache,
                    store,
                    data_graph,
                    #target_advanced_select_query_lits,
                )?);
            )*
        };

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
                let object_setup = match object {
                    SrcGenRuleObject::Constant(term) => {
                        let expr = term_expr(term);
                        quote! {
                            let object_term: oxigraph::model::Term = #expr;
                        }
                    }
                    SrcGenRuleObject::This => quote! {
                        let object_term: oxigraph::model::Term = focus.clone();
                    },
                };
                specialized_rule_blocks.push(quote! {
                    if scheduled_rule_indices.contains(&#rule_index) {
                        let rule_metadata = generated_rule_metadata(#rule_index);
                        let trigger = trigger_for_dependencies(&rule_metadata.dependencies, &delta);
                        if !trigger.is_none() {
                            let mut condition_cache: std::collections::HashMap<(String, String), bool> =
                                std::collections::HashMap::new();
                            let mut focus_nodes: Vec<oxigraph::model::Term> = Vec::new();
                            #rule_focus_sources
                            focus_nodes = sort_and_dedup_terms(focus_nodes);
                            focus_nodes = filter_focus_nodes_for_trigger(focus_nodes, &trigger);

                            if !focus_nodes.is_empty() {
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
                                            let conforms = srcgen_shape_conforms(
                                                store,
                                                data_graph,
                                                #condition_shape_lits,
                                                &focus,
                                            )?;
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
                                    #object_setup
                                    let added = insert_inferred_candidates(
                                        store,
                                        &graph_name,
                                        vec![(subject_term, predicate.clone(), object_term)],
                                        &mut round_inserted_quads,
                                    )?;
                                    if added > 0 {
                                        inserted += added;
                                        condition_cache.clear();
                                        reset_srcgen_shape_conformance_cache();
                                    }
                                }
                            }
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
                let has_condition_shapes = !condition_shape_iris.is_empty();
                let condition_shape_lits: Vec<LitStr> = condition_shape_iris
                    .iter()
                    .map(|iri| LitStr::new(iri, Span::call_site()))
                    .collect();
                specialized_rule_blocks.push(quote! {
                    if scheduled_rule_indices.contains(&#rule_index) {
                        let rule_metadata = generated_rule_metadata(#rule_index);
                        let trigger = trigger_for_dependencies(&rule_metadata.dependencies, &delta);
                        if !trigger.is_none() {
                            let mut condition_cache: std::collections::HashMap<(String, String), bool> =
                                std::collections::HashMap::new();
                            let mut focus_nodes: Vec<oxigraph::model::Term> = Vec::new();
                            #rule_focus_sources
                            focus_nodes = sort_and_dedup_terms(focus_nodes);
                            focus_nodes = filter_focus_nodes_for_trigger(focus_nodes, &trigger);

                            if !focus_nodes.is_empty() {
                                let query_uses_this = sparql_query_uses_this(#query_lit);
                                let mut candidate_batches: Vec<Vec<(
                                    oxigraph::model::Term,
                                    oxigraph::model::NamedNode,
                                    oxigraph::model::Term,
                                )>> = Vec::new();

                                if let Some(native_rule) = rule_metadata.native_sparql.as_ref() {
                                    for focus in &focus_nodes {
                                        let mut conditions_met = true;
                                        #(
                                            let condition_key = (#condition_shape_lits.to_string(), focus.to_string());
                                            let condition_conforms = if let Some(cached) = condition_cache.get(&condition_key) {
                                                *cached
                                            } else {
                                                let conforms = srcgen_shape_conforms(
                                                    store,
                                                    data_graph,
                                                    #condition_shape_lits,
                                                    focus,
                                                )?;
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
                                        if let Some(prefilter) = rule_metadata.sparql_prefilter.as_ref() {
                                            if !prefilter.matches_delta(&delta, focus) {
                                                continue;
                                            }
                                        }
                                        candidate_batches.push(
                                            native_sparql_construct_triples(
                                                native_rule,
                                                focus,
                                                &delta,
                                                store,
                                                data_graph,
                                            )
                                            .map_err(|err| {
                                                format!("SPARQLRule {} execution failed: {err}", #rule_id)
                                            })?
                                        );
                                    }
                                } else if !query_uses_this {
                                    candidate_batches.push(
                                        sparql_construct_triples_with_bindings(
                                            #query_lit,
                                            store,
                                            data_graph,
                                            &[],
                                        )
                                        .map_err(|err| {
                                            format!("SPARQLRule {} execution failed: {err}", #rule_id)
                                        })?
                                    );
                                } else if !#has_condition_shapes {
                                    let mut filtered_focus_nodes = Vec::new();
                                    for focus in focus_nodes {
                                        if let Some(prefilter) = rule_metadata.sparql_prefilter.as_ref() {
                                            if !prefilter.matches_delta(&delta, &focus) {
                                                continue;
                                            }
                                        }
                                        filtered_focus_nodes.push(focus);
                                    }

                                    let mut ground_focus_nodes: Vec<oxigraph::model::Term> = Vec::new();
                                    let mut fallback_focus_nodes: Vec<oxigraph::model::Term> = Vec::new();
                                    for focus in filtered_focus_nodes {
                                        if term_to_sparql_ground(&focus).is_some() {
                                            ground_focus_nodes.push(focus);
                                        } else {
                                            fallback_focus_nodes.push(focus);
                                        }
                                    }

                                    const THIS_VALUES_BATCH_SIZE: usize = 1024;
                                    let ground_batches = ground_focus_nodes
                                        .par_chunks(THIS_VALUES_BATCH_SIZE)
                                        .map(|chunk| -> Result<
                                            (
                                                Option<Vec<(
                                                    oxigraph::model::Term,
                                                    oxigraph::model::NamedNode,
                                                    oxigraph::model::Term,
                                                )>>,
                                                Vec<oxigraph::model::Term>,
                                            ),
                                            String,
                                        > {
                                            let Some(batched_query) =
                                                sparql_query_with_this_values(#query_lit, chunk)?
                                            else {
                                                return Ok((None, chunk.to_vec()));
                                            };
                                            let constructed = sparql_construct_triples_with_bindings(
                                                batched_query.as_str(),
                                                store,
                                                data_graph,
                                                &[],
                                            )
                                            .map_err(|err| {
                                                format!("SPARQLRule {} execution failed: {err}", #rule_id)
                                            })?;
                                            Ok((Some(constructed), Vec::new()))
                                        })
                                        .collect::<Result<Vec<_>, _>>()?;

                                    for (constructed, fallback_chunk) in ground_batches {
                                        if let Some(constructed) = constructed {
                                            candidate_batches.push(constructed);
                                        }
                                        fallback_focus_nodes.extend(fallback_chunk);
                                    }

                                    let fallback_batches = fallback_focus_nodes
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
                                    candidate_batches.extend(fallback_batches);
                                } else {
                                    for focus in &focus_nodes {
                                        let mut conditions_met = true;
                                        #(
                                            let condition_key = (#condition_shape_lits.to_string(), focus.to_string());
                                            let condition_conforms = if let Some(cached) = condition_cache.get(&condition_key) {
                                                *cached
                                            } else {
                                                let conforms = srcgen_shape_conforms(
                                                    store,
                                                    data_graph,
                                                    #condition_shape_lits,
                                                    focus,
                                                )?;
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
                                        if let Some(prefilter) = rule_metadata.sparql_prefilter.as_ref() {
                                            if !prefilter.matches_delta(&delta, focus) {
                                                continue;
                                            }
                                        }
                                        let sparql_bindings = [("this", focus.clone())];
                                        candidate_batches.push(
                                            sparql_construct_triples_with_bindings(
                                                #query_lit,
                                                store,
                                                data_graph,
                                                &sparql_bindings,
                                            )
                                            .map_err(|err| {
                                                format!("SPARQLRule {} execution failed: {err}", #rule_id)
                                            })?
                                        );
                                    }
                                }

                                let mut all_candidates: Vec<(
                                    oxigraph::model::Term,
                                    oxigraph::model::NamedNode,
                                    oxigraph::model::Term,
                                )> = Vec::new();
                                for constructed in candidate_batches {
                                    all_candidates.extend(constructed);
                                }
                                let added = insert_inferred_candidates(
                                    store,
                                    &graph_name,
                                    all_candidates,
                                    &mut round_inserted_quads,
                                )?;
                                if added > 0 && #has_condition_shapes {
                                    condition_cache.clear();
                                    reset_srcgen_shape_conformance_cache();
                                }
                                inserted += added;
                            }
                        }
                    }
                });
            }
            SrcGenRuleKind::Unsupported { .. } => {}
        }
    }

    let tokens = quote! {
        use rayon::prelude::*;

        pub const GENERATED_INFERENCE_RULES: usize = #generated_rule_count;
        pub const GENERATED_SPECIALIZED_INFERENCE_RULES: usize = #specialized_rule_count;
        pub const GENERATED_SPECIALIZED_TRIPLE_INFERENCE_RULES: usize = #specialized_triple_rule_count;
        pub const GENERATED_SPECIALIZED_SPARQL_INFERENCE_RULES: usize = #specialized_sparql_rule_count;
        pub const GENERATED_FALLBACK_INFERENCE_RULES: usize = #fallback_rule_count;

        thread_local! {
            static SPARQL_RULE_PREPARED_CACHE: std::cell::RefCell<
                std::collections::HashMap<String, oxigraph::sparql::PreparedSparqlQuery>
            > = std::cell::RefCell::new(std::collections::HashMap::new());
            static SPARQL_RULE_USES_THIS_CACHE: std::cell::RefCell<
                std::collections::HashMap<String, bool>
            > = std::cell::RefCell::new(std::collections::HashMap::new());
        }

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        enum FocusDependency {
            All,
            TargetClass(oxigraph::model::Term),
            TargetSubjectsOf(oxigraph::model::NamedNode),
            TargetObjectsOf(oxigraph::model::NamedNode),
            OutgoingPredicate(oxigraph::model::NamedNode),
            IncomingPredicate(oxigraph::model::NamedNode),
            AnyPredicateParticipant(oxigraph::model::NamedNode),
        }

        #[derive(Debug, Clone, Default)]
        struct SparqlPrefilter {
            direct_outgoing_predicates: Vec<oxigraph::model::NamedNode>,
            direct_incoming_predicates: Vec<oxigraph::model::NamedNode>,
            direct_classes: Vec<oxigraph::model::Term>,
        }

        #[derive(Debug, Clone)]
        enum NativeSparqlPath {
            NamedNode(oxigraph::model::NamedNode),
            ReverseNamedNode(oxigraph::model::NamedNode),
            Sequence(Vec<NativeSparqlPath>),
        }

        #[derive(Debug, Clone)]
        enum NativeSparqlRule {
            PathCopy {
                construct_predicate: oxigraph::model::NamedNode,
                source_path: NativeSparqlPath,
            },
            EqualityConstant {
                construct_predicate: oxigraph::model::NamedNode,
                left_predicate: oxigraph::model::NamedNode,
                right_predicate: oxigraph::model::NamedNode,
                object: oxigraph::model::Term,
            },
        }

        #[derive(Debug, Clone)]
        struct RuleMetadata {
            dependencies: Vec<FocusDependency>,
            sparql_prefilter: Option<SparqlPrefilter>,
            native_sparql: Option<NativeSparqlRule>,
        }

        #[derive(Debug, Clone)]
        enum TriggerSet {
            All,
            Nodes(std::collections::HashSet<oxigraph::model::Term>),
        }

        impl TriggerSet {
            fn is_none(&self) -> bool {
                matches!(self, TriggerSet::Nodes(nodes) if nodes.is_empty())
            }
        }

        #[derive(Debug, Default, Clone)]
        struct DeltaIndex {
            is_initial: bool,
            subjects_by_predicate: std::collections::HashMap<
                oxigraph::model::NamedNode,
                std::collections::HashSet<oxigraph::model::Term>,
            >,
            objects_by_predicate: std::collections::HashMap<
                oxigraph::model::NamedNode,
                std::collections::HashSet<oxigraph::model::Term>,
            >,
            participants_by_predicate: std::collections::HashMap<
                oxigraph::model::NamedNode,
                std::collections::HashSet<oxigraph::model::Term>,
            >,
            typed_subjects_by_class: std::collections::HashMap<
                oxigraph::model::Term,
                std::collections::HashSet<oxigraph::model::Term>,
            >,
            outgoing_values_by_focus: std::collections::HashMap<
                oxigraph::model::Term,
                std::collections::HashMap<
                    oxigraph::model::NamedNode,
                    std::collections::HashSet<oxigraph::model::Term>,
                >,
            >,
            incoming_values_by_focus: std::collections::HashMap<
                oxigraph::model::Term,
                std::collections::HashMap<
                    oxigraph::model::NamedNode,
                    std::collections::HashSet<oxigraph::model::Term>,
                >,
            >,
        }

        impl DeltaIndex {
            fn initial() -> Self {
                Self {
                    is_initial: true,
                    ..Self::default()
                }
            }

            fn from_quads(quads: &[oxigraph::model::Quad]) -> Self {
                let mut index = Self::default();
                for quad in quads {
                    let subject_term = match quad.subject.as_ref() {
                        oxigraph::model::NamedOrBlankNodeRef::NamedNode(node) => {
                            oxigraph::model::Term::NamedNode(node.into_owned())
                        }
                        oxigraph::model::NamedOrBlankNodeRef::BlankNode(node) => {
                            oxigraph::model::Term::BlankNode(node.into_owned())
                        }
                    };
                    let predicate = quad.predicate.clone();
                    index
                        .subjects_by_predicate
                        .entry(predicate.clone())
                        .or_default()
                        .insert(subject_term.clone());
                    index
                        .objects_by_predicate
                        .entry(predicate.clone())
                        .or_default()
                        .insert(quad.object.clone());
                    let participants = index
                        .participants_by_predicate
                        .entry(predicate.clone())
                        .or_default();
                    participants.insert(subject_term.clone());
                    participants.insert(quad.object.clone());
                    index
                        .outgoing_values_by_focus
                        .entry(subject_term.clone())
                        .or_default()
                        .entry(predicate.clone())
                        .or_default()
                        .insert(quad.object.clone());
                    index
                        .incoming_values_by_focus
                        .entry(quad.object.clone())
                        .or_default()
                        .entry(predicate.clone())
                        .or_default()
                        .insert(subject_term.clone());
                    if predicate == oxigraph::model::vocab::rdf::TYPE
                        && matches!(quad.object, oxigraph::model::Term::NamedNode(_))
                    {
                        index
                            .typed_subjects_by_class
                            .entry(quad.object.clone())
                            .or_default()
                            .insert(subject_term);
                    }
                }
                index
            }

            fn dependency_keys(&self) -> Vec<FocusDependency> {
                let mut keys = Vec::new();
                for predicate in self.subjects_by_predicate.keys() {
                    keys.push(FocusDependency::TargetSubjectsOf(predicate.clone()));
                    keys.push(FocusDependency::OutgoingPredicate(predicate.clone()));
                }
                for predicate in self.objects_by_predicate.keys() {
                    keys.push(FocusDependency::TargetObjectsOf(predicate.clone()));
                    keys.push(FocusDependency::IncomingPredicate(predicate.clone()));
                }
                for predicate in self.participants_by_predicate.keys() {
                    keys.push(FocusDependency::AnyPredicateParticipant(predicate.clone()));
                }
                for class in self.typed_subjects_by_class.keys() {
                    keys.push(FocusDependency::TargetClass(class.clone()));
                }
                keys
            }

            fn outgoing_values_for_focus(
                &self,
                focus: &oxigraph::model::Term,
                predicate: &oxigraph::model::NamedNode,
            ) -> Option<Vec<oxigraph::model::Term>> {
                self.outgoing_values_by_focus
                    .get(focus)
                    .and_then(|by_predicate| by_predicate.get(predicate))
                    .map(|values| values.iter().cloned().collect())
            }

            fn incoming_values_for_focus(
                &self,
                focus: &oxigraph::model::Term,
                predicate: &oxigraph::model::NamedNode,
            ) -> Option<Vec<oxigraph::model::Term>> {
                self.incoming_values_by_focus
                    .get(focus)
                    .and_then(|by_predicate| by_predicate.get(predicate))
                    .map(|values| values.iter().cloned().collect())
            }

            fn subject_has_predicate(
                &self,
                focus: &oxigraph::model::Term,
                predicate: &oxigraph::model::NamedNode,
            ) -> bool {
                self.subjects_by_predicate
                    .get(predicate)
                    .is_some_and(|subjects| subjects.contains(focus))
            }

            fn object_has_predicate(
                &self,
                focus: &oxigraph::model::Term,
                predicate: &oxigraph::model::NamedNode,
            ) -> bool {
                self.objects_by_predicate
                    .get(predicate)
                    .is_some_and(|objects| objects.contains(focus))
            }

            fn subject_has_class(
                &self,
                focus: &oxigraph::model::Term,
                class: &oxigraph::model::Term,
            ) -> bool {
                self.typed_subjects_by_class
                    .get(class)
                    .is_some_and(|subjects| subjects.contains(focus))
            }
        }

        impl SparqlPrefilter {
            fn matches_delta(
                &self,
                delta: &DeltaIndex,
                focus: &oxigraph::model::Term,
            ) -> bool {
                if delta.is_initial {
                    return true;
                }

                self.direct_outgoing_predicates
                    .iter()
                    .any(|predicate| delta.subject_has_predicate(focus, predicate))
                    || self
                        .direct_incoming_predicates
                        .iter()
                        .any(|predicate| delta.object_has_predicate(focus, predicate))
                    || self
                        .direct_classes
                        .iter()
                        .any(|class| delta.subject_has_class(focus, class))
            }
        }

        fn generated_rule_metadata(rule_index: usize) -> RuleMetadata {
            match rule_index {
                #(#metadata_match_arms,)*
                _ => unreachable!("unknown generated inference rule index {rule_index}"),
            }
        }

        fn build_dependency_index() -> (
            std::collections::HashMap<FocusDependency, Vec<usize>>,
            Vec<usize>,
        ) {
            let mut index: std::collections::HashMap<FocusDependency, Vec<usize>> =
                std::collections::HashMap::new();
            let mut always_run = Vec::new();
            for rule_index in 0..GENERATED_SPECIALIZED_INFERENCE_RULES {
                let metadata = generated_rule_metadata(rule_index);
                for dependency in metadata.dependencies {
                    if dependency == FocusDependency::All {
                        always_run.push(rule_index);
                    } else {
                        index.entry(dependency).or_default().push(rule_index);
                    }
                }
            }
            (index, always_run)
        }

        fn scheduled_rule_indices(
            delta: &DeltaIndex,
            dependency_index: &std::collections::HashMap<FocusDependency, Vec<usize>>,
            always_run: &[usize],
        ) -> std::collections::HashSet<usize> {
            if delta.is_initial {
                return (0..GENERATED_SPECIALIZED_INFERENCE_RULES).collect();
            }

            let mut scheduled: std::collections::HashSet<usize> =
                always_run.iter().copied().collect();
            for key in delta.dependency_keys() {
                if let Some(rule_indices) = dependency_index.get(&key) {
                    scheduled.extend(rule_indices.iter().copied());
                }
            }
            scheduled
        }

        fn trigger_for_dependencies(
            dependencies: &[FocusDependency],
            delta: &DeltaIndex,
        ) -> TriggerSet {
            if delta.is_initial {
                return TriggerSet::All;
            }

            let mut focus_nodes = std::collections::HashSet::new();
            for dependency in dependencies {
                match dependency {
                    FocusDependency::All => return TriggerSet::All,
                    FocusDependency::TargetClass(class) => {
                        if let Some(nodes) = delta.typed_subjects_by_class.get(class) {
                            focus_nodes.extend(nodes.iter().cloned());
                        }
                    }
                    FocusDependency::TargetSubjectsOf(predicate)
                    | FocusDependency::OutgoingPredicate(predicate) => {
                        if let Some(nodes) = delta.subjects_by_predicate.get(predicate) {
                            focus_nodes.extend(nodes.iter().cloned());
                        }
                    }
                    FocusDependency::TargetObjectsOf(predicate)
                    | FocusDependency::IncomingPredicate(predicate) => {
                        if let Some(nodes) = delta.objects_by_predicate.get(predicate) {
                            focus_nodes.extend(nodes.iter().cloned());
                        }
                    }
                    FocusDependency::AnyPredicateParticipant(predicate) => {
                        if let Some(nodes) = delta.participants_by_predicate.get(predicate) {
                            focus_nodes.extend(nodes.iter().cloned());
                        }
                    }
                }
            }
            TriggerSet::Nodes(focus_nodes)
        }

        fn filter_focus_nodes_for_trigger(
            focus_nodes: Vec<oxigraph::model::Term>,
            trigger: &TriggerSet,
        ) -> Vec<oxigraph::model::Term> {
            match trigger {
                TriggerSet::All => focus_nodes,
                TriggerSet::Nodes(nodes) => focus_nodes
                    .into_iter()
                    .filter(|focus| nodes.contains(focus))
                    .collect(),
            }
        }

        fn sparql_query_uses_this(query: &str) -> bool {
            SPARQL_RULE_USES_THIS_CACHE.with(|cache| {
                let mut cache = cache.borrow_mut();
                if let Some(cached) = cache.get(query) {
                    *cached
                } else {
                    let mentions_this = query_mentions_var(query, "this");
                    cache.insert(query.to_string(), mentions_this);
                    mentions_this
                }
            })
        }

        fn inject_values_clause_into_where(
            query: &str,
            values_clause: &str,
        ) -> Option<String> {
            let query_bytes = query.as_bytes();
            let lower = query.to_ascii_lowercase();
            let lower_bytes = lower.as_bytes();
            let mut where_idx: Option<usize> = None;
            let mut idx = 0usize;
            while idx + 5 <= lower_bytes.len() {
                if &lower_bytes[idx..idx + 5] == b"where" {
                    let boundary_before = idx == 0
                        || !(lower_bytes[idx - 1].is_ascii_alphanumeric()
                            || lower_bytes[idx - 1] == b'_');
                    let boundary_after = idx + 5 == lower_bytes.len()
                        || !(lower_bytes[idx + 5].is_ascii_alphanumeric()
                            || lower_bytes[idx + 5] == b'_');
                    if boundary_before && boundary_after {
                        where_idx = Some(idx);
                        break;
                    }
                }
                idx += 1;
            }
            let where_idx = where_idx?;
            let mut brace_idx: Option<usize> = None;
            for (offset, ch) in query[where_idx + 5..].char_indices() {
                if ch == '{' {
                    brace_idx = Some(where_idx + 5 + offset);
                    break;
                }
            }
            let brace_idx = brace_idx?;

            let mut out = String::with_capacity(query.len() + values_clause.len() + 4);
            out.push_str(&query[..brace_idx + 1]);
            out.push('\n');
            out.push_str(values_clause);
            out.push('\n');
            out.push_str(&query[brace_idx + 1..]);
            if out.as_bytes() == query_bytes {
                None
            } else {
                Some(out)
            }
        }

        fn sparql_query_with_this_values(
            query: &str,
            this_terms: &[oxigraph::model::Term],
        ) -> Result<Option<String>, String> {
            if this_terms.is_empty() || !sparql_query_uses_this(query) {
                return Ok(None);
            }
            let mut values_clause = String::from("VALUES ?this {");
            for term in this_terms {
                let Some(ground) = term_to_sparql_ground(term) else {
                    return Ok(None);
                };
                values_clause.push(' ');
                values_clause.push_str(ground.as_str());
            }
            values_clause.push_str(" }");
            Ok(inject_values_clause_into_where(query, values_clause.as_str()))
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
            let query_uses_this = sparql_query_uses_this(query);
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

        fn focus_nodes_for_target_subjects_of_cached_cloned(
            target_subjects_of_cache: &mut std::collections::HashMap<
                String,
                Vec<oxigraph::model::Term>,
            >,
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            predicate_iri: &str,
        ) -> Result<Vec<oxigraph::model::Term>, String> {
            if let Some(cached) = target_subjects_of_cache.get(predicate_iri) {
                return Ok(cached.clone());
            }
            let predicate = oxigraph::model::NamedNode::new(predicate_iri).map_err(|err| {
                format!("invalid rule targetSubjectsOf predicate IRI {predicate_iri}: {err}")
            })?;
            let mut nodes = Vec::new();
            for graph in validation_graphs(data_graph)? {
                let graph_ref = oxigraph::model::GraphNameRef::NamedNode(graph.as_ref());
                for quad in store.quads_for_pattern(
                    None,
                    Some(predicate.as_ref()),
                    None,
                    Some(graph_ref),
                ) {
                    let quad = quad.map_err(|err| format!("store query failed: {err}"))?;
                    nodes.push(oxigraph::model::Term::from(quad.subject));
                }
            }
            nodes = sort_and_dedup_terms(nodes);
            target_subjects_of_cache.insert(predicate_iri.to_string(), nodes.clone());
            Ok(nodes)
        }

        fn focus_nodes_for_target_objects_of_cached_cloned(
            target_objects_of_cache: &mut std::collections::HashMap<
                String,
                Vec<oxigraph::model::Term>,
            >,
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            predicate_iri: &str,
        ) -> Result<Vec<oxigraph::model::Term>, String> {
            if let Some(cached) = target_objects_of_cache.get(predicate_iri) {
                return Ok(cached.clone());
            }
            let predicate = oxigraph::model::NamedNode::new(predicate_iri).map_err(|err| {
                format!("invalid rule targetObjectsOf predicate IRI {predicate_iri}: {err}")
            })?;
            let mut nodes = Vec::new();
            for graph in validation_graphs(data_graph)? {
                let graph_ref = oxigraph::model::GraphNameRef::NamedNode(graph.as_ref());
                for quad in store.quads_for_pattern(
                    None,
                    Some(predicate.as_ref()),
                    None,
                    Some(graph_ref),
                ) {
                    let quad = quad.map_err(|err| format!("store query failed: {err}"))?;
                    nodes.push(quad.object);
                }
            }
            nodes = sort_and_dedup_terms(nodes);
            target_objects_of_cache.insert(predicate_iri.to_string(), nodes.clone());
            Ok(nodes)
        }

        fn focus_nodes_for_advanced_target_select_cached_cloned(
            advanced_target_cache: &mut std::collections::HashMap<
                String,
                Vec<oxigraph::model::Term>,
            >,
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            select_query: &str,
        ) -> Result<Vec<oxigraph::model::Term>, String> {
            if let Some(cached) = advanced_target_cache.get(select_query) {
                return Ok(cached.clone());
            }

            let mut prepared = SPARQL_RULE_PREPARED_CACHE.with(
                |cache| -> Result<oxigraph::sparql::PreparedSparqlQuery, String> {
                    let mut cache = cache.borrow_mut();
                    if let Some(cached) = cache.get(select_query) {
                        return Ok(cached.clone());
                    }
                    let parsed = oxigraph::sparql::SparqlEvaluator::new()
                        .parse_query(select_query)
                        .map_err(|err| format!("failed to parse advanced target SELECT query: {err}"))?;
                    cache.insert(select_query.to_string(), parsed.clone());
                    Ok(parsed)
                }
            )?;

            let default_graphs: Vec<oxigraph::model::GraphName> = validation_graphs(data_graph)?
                .into_iter()
                .map(oxigraph::model::GraphName::NamedNode)
                .collect();
            prepared.dataset_mut().set_default_graph(default_graphs);

            let mut out: Vec<oxigraph::model::Term> = Vec::new();
            match prepared.on_store(store).execute() {
                Ok(oxigraph::sparql::QueryResults::Solutions(solutions)) => {
                    for solution_result in solutions {
                        let solution = solution_result
                            .map_err(|err| format!("advanced target SELECT solution error: {err}"))?;
                        if let Some(term) = solution.get("this") {
                            out.push(term.to_owned());
                            continue;
                        }
                        if let Some(term) = solution.get("target") {
                            out.push(term.to_owned());
                            continue;
                        }
                        if let Some(first_var) = solution.variables().first() {
                            if let Some(term) = solution.get(first_var.as_str()) {
                                out.push(term.to_owned());
                            }
                        }
                    }
                }
                Ok(_) => {
                    return Err("advanced target SELECT query returned non-solution results".to_string());
                }
                Err(err) => return Err(format!("failed to execute advanced target SELECT query: {err}")),
            }

            out = sort_and_dedup_terms(out);
            advanced_target_cache.insert(select_query.to_string(), out.clone());
            Ok(out)
        }

        fn focus_objects_for_predicate(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            focus: &oxigraph::model::Term,
            predicate: &oxigraph::model::NamedNode,
        ) -> Result<Vec<oxigraph::model::Term>, String> {
            let Some(subject_ref) = subject_ref_from_term(focus) else {
                return Ok(Vec::new());
            };
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
            Ok(sort_and_dedup_terms(values))
        }

        fn focus_subjects_for_inverse_predicate(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            focus: &oxigraph::model::Term,
            predicate: &oxigraph::model::NamedNode,
        ) -> Result<Vec<oxigraph::model::Term>, String> {
            let mut values = Vec::new();
            for graph in validation_graphs(data_graph)? {
                let graph_ref = oxigraph::model::GraphNameRef::NamedNode(graph.as_ref());
                for quad in store.quads_for_pattern(
                    None,
                    Some(predicate.as_ref()),
                    Some(focus.as_ref()),
                    Some(graph_ref),
                ) {
                    let quad = quad.map_err(|err| format!("store query failed: {err}"))?;
                    values.push(oxigraph::model::Term::from(quad.subject));
                }
            }
            Ok(sort_and_dedup_terms(values))
        }

        fn native_direct_values(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            focus: &oxigraph::model::Term,
            predicate: &oxigraph::model::NamedNode,
            delta: &DeltaIndex,
        ) -> Result<Vec<oxigraph::model::Term>, String> {
            if !delta.is_initial {
                if let Some(values) = delta.outgoing_values_for_focus(focus, predicate) {
                    return Ok(values);
                }
            }
            focus_objects_for_predicate(store, data_graph, focus, predicate)
        }

        fn native_path_values(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            focus: &oxigraph::model::Term,
            path: &NativeSparqlPath,
            delta: &DeltaIndex,
        ) -> Result<Vec<oxigraph::model::Term>, String> {
            match path {
                NativeSparqlPath::NamedNode(predicate) => {
                    native_direct_values(store, data_graph, focus, predicate, delta)
                }
                NativeSparqlPath::ReverseNamedNode(predicate) => {
                    if !delta.is_initial {
                        if let Some(values) = delta.incoming_values_for_focus(focus, predicate) {
                            return Ok(values);
                        }
                    }
                    focus_subjects_for_inverse_predicate(store, data_graph, focus, predicate)
                }
                NativeSparqlPath::Sequence(segments) => {
                    let mut frontier = vec![focus.clone()];
                    for (index, segment) in segments.iter().enumerate() {
                        let segment_delta = if index + 1 == segments.len() {
                            delta
                        } else {
                            &DeltaIndex::default()
                        };
                        let mut next = Vec::new();
                        for node in &frontier {
                            next.extend(native_path_values(
                                store,
                                data_graph,
                                node,
                                segment,
                                segment_delta,
                            )?);
                        }
                        let mut unique = std::collections::HashSet::new();
                        next.retain(|term| unique.insert(term.clone()));
                        frontier = next;
                        if frontier.is_empty() {
                            break;
                        }
                    }
                    Ok(frontier)
                }
            }
        }

        fn native_sparql_construct_triples(
            native_rule: &NativeSparqlRule,
            focus: &oxigraph::model::Term,
            delta: &DeltaIndex,
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<Vec<(
            oxigraph::model::Term,
            oxigraph::model::NamedNode,
            oxigraph::model::Term,
        )>, String> {
            match native_rule {
                NativeSparqlRule::PathCopy {
                    construct_predicate,
                    source_path,
                } => {
                    let values = native_path_values(
                        store,
                        data_graph,
                        focus,
                        source_path,
                        delta,
                    )?;
                    Ok(values
                        .into_iter()
                        .map(|value| (focus.clone(), construct_predicate.clone(), value))
                        .collect())
                }
                NativeSparqlRule::EqualityConstant {
                    construct_predicate,
                    left_predicate,
                    right_predicate,
                    object,
                } => {
                    let left_values =
                        focus_objects_for_predicate(store, data_graph, focus, left_predicate)?;
                    let right_values =
                        focus_objects_for_predicate(store, data_graph, focus, right_predicate)?;
                    let right_set: std::collections::HashSet<oxigraph::model::Term> =
                        right_values.into_iter().collect();
                    if left_values.into_iter().any(|value| right_set.contains(&value)) {
                        Ok(vec![(focus.clone(), construct_predicate.clone(), object.clone())])
                    } else {
                        Ok(Vec::new())
                    }
                }
            }
        }

        fn insert_inferred_candidates(
            store: &oxigraph::store::Store,
            graph_name: &oxigraph::model::GraphName,
            candidates: Vec<(
                oxigraph::model::Term,
                oxigraph::model::NamedNode,
                oxigraph::model::Term,
            )>,
            inserted_quads: &mut Vec<oxigraph::model::Quad>,
        ) -> Result<usize, String> {
            if candidates.is_empty() {
                return Ok(0);
            }

            let mut seen: std::collections::HashSet<(
                oxigraph::model::Term,
                oxigraph::model::NamedNode,
                oxigraph::model::Term,
            )> = std::collections::HashSet::new();
            let mut inferred_quads: Vec<oxigraph::model::Quad> = Vec::new();
            for (subject_term, predicate, object_term) in candidates {
                if !seen.insert((subject_term.clone(), predicate.clone(), object_term.clone())) {
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
                inferred_quads.push(inferred);
            }

            if inferred_quads.is_empty() {
                return Ok(0);
            }

            let inserted = inferred_quads.len();
            store
                .extend(inferred_quads.iter().cloned())
                .map_err(|err| format!("failed to insert inferred quads: {err}"))?;
            inserted_quads.extend(inferred_quads);
            Ok(inserted)
        }

        fn run_runtime_inference(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            graph_name: &oxigraph::model::GraphName,
        ) -> Result<usize, String> {
            let validator = build_runtime_validator(store, data_graph)?;
            let config = shifty::InferenceConfig::default();
            let outcome = validator
                .run_inference_with_config(config)
                .map_err(|err| format!("runtime inference failed: {err}"))?;
            let candidates = outcome
                .inferred_quads
                .into_iter()
                .map(|quad| (oxigraph::model::Term::from(quad.subject), quad.predicate, quad.object))
                .collect();
            let mut inserted_quads = Vec::new();
            insert_inferred_candidates(store, graph_name, candidates, &mut inserted_quads)
        }

        pub fn run_generated_inference_with_options(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            allow_runtime_fallback: bool,
        ) -> Result<usize, String> {
            if GENERATED_INFERENCE_RULES == 0 {
                return Ok(0);
            }

            let graph_name = oxigraph::model::GraphName::NamedNode(data_graph.clone());
            let mut inserted = 0usize;
            let (dependency_index, always_run_rule_indices) = build_dependency_index();
            let mut delta = DeltaIndex::initial();

            loop {
                let inserted_before_round = inserted;
                let mut round_inserted_quads = Vec::new();
                let scheduled_rule_indices =
                    scheduled_rule_indices(&delta, &dependency_index, &always_run_rule_indices);
                let mut target_class_index: Option<TargetClassIndex> = None;
                let mut target_class_focus_cache: std::collections::HashMap<
                    String,
                    Vec<oxigraph::model::Term>,
                > = std::collections::HashMap::new();
                let mut target_subjects_of_focus_cache: std::collections::HashMap<
                    String,
                    Vec<oxigraph::model::Term>,
                > = std::collections::HashMap::new();
                let mut target_objects_of_focus_cache: std::collections::HashMap<
                    String,
                    Vec<oxigraph::model::Term>,
                > = std::collections::HashMap::new();
                let mut advanced_target_select_focus_cache: std::collections::HashMap<
                    String,
                    Vec<oxigraph::model::Term>,
                > = std::collections::HashMap::new();
                #(#specialized_rule_blocks)*
                if inserted == inserted_before_round {
                    break;
                }
                delta = DeltaIndex::from_quads(&round_inserted_quads);
            }

            if GENERATED_FALLBACK_INFERENCE_RULES == 0 {
                return Ok(inserted);
            }

            let strict_full_aot = match std::env::var("SHFTY_SRCGEN_FULL_AOT_STRICT") {
                Ok(value) => matches!(
                    value.to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                ),
                Err(_) => false,
            };
            if !allow_runtime_fallback {
                let message = format!(
                    "srcgen full-aot mode: {} unsupported inference rule(s) require runtime fallback",
                    GENERATED_FALLBACK_INFERENCE_RULES,
                );
                if strict_full_aot {
                    return Err(format!("{message} (strict full-aot mode)"));
                }
                eprintln!("{message}; skipping them because runtime fallback is disabled");
                return Ok(inserted);
            }

            inserted += run_runtime_inference(store, data_graph, &graph_name)?;
            Ok(inserted)
        }

        pub fn run_generated_inference(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<usize, String> {
            run_generated_inference_with_options(store, data_graph, true)
        }
    };
    render_tokens_as_module(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxigraph::model::{Literal, Term};

    #[test]
    fn generated_native_sparql_rule_matches_prefixed_path_copy_query() {
        let query = "PREFIX ex: <http://example.com/ns#> CONSTRUCT { $this ex:output ?value . } WHERE { $this ^ex:sourceOf/ex:value ?value . }";

        let native_rule = generated_native_sparql_rule(query).expect("native path-copy rule");

        match native_rule {
            GeneratedNativeSparqlRule::PathCopy {
                construct_predicate,
                source_path: GeneratedNativeSparqlPath::Sequence(segments),
            } => {
                assert_eq!(construct_predicate, "http://example.com/ns#output");
                assert_eq!(segments.len(), 2);
                match &segments[0] {
                    GeneratedNativeSparqlPath::ReverseNamedNode(predicate) => {
                        assert_eq!(predicate, "http://example.com/ns#sourceOf");
                    }
                    other => panic!("unexpected first path segment: {other:?}"),
                }
                match &segments[1] {
                    GeneratedNativeSparqlPath::NamedNode(predicate) => {
                        assert_eq!(predicate, "http://example.com/ns#value");
                    }
                    other => panic!("unexpected second path segment: {other:?}"),
                }
            }
            other => panic!("unexpected native rule: {other:?}"),
        }
    }

    #[test]
    fn generated_native_sparql_rule_matches_prefixed_equality_constant_query() {
        let query = "PREFIX ex: <http://example.com/ns#> CONSTRUCT { $this ex:isSquare true . } WHERE { $this ex:width ?left . $this ex:height ?right . FILTER(?left = ?right) }";

        let native_rule =
            generated_native_sparql_rule(query).expect("native equality-constant rule");

        match native_rule {
            GeneratedNativeSparqlRule::EqualityConstant {
                construct_predicate,
                left_predicate,
                right_predicate,
                object,
            } => {
                assert_eq!(construct_predicate, "http://example.com/ns#isSquare");
                assert_eq!(left_predicate, "http://example.com/ns#width");
                assert_eq!(right_predicate, "http://example.com/ns#height");
                assert_eq!(object, Term::Literal(Literal::from(true)));
            }
            other => panic!("unexpected native rule: {other:?}"),
        }
    }
}
