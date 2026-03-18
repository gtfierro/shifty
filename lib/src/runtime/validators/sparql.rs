#![allow(deprecated)]
use crate::context::{
    format_term_for_label, sanitize_graphviz_string, BatchedSparqlResult, BatchedSparqlViolation,
    Context, ValidationContext,
};
use crate::model::components::sparql::CustomConstraintComponentDefinition;
use crate::named_nodes::SHACL;
use crate::runtime::{
    ComponentValidationResult, GraphvizOutput, ToSubjectRef, ValidateComponent, ValidationFailure,
};
use crate::sparql::{
    ensure_pre_binding_semantics, format_term_with_namespace_aliases, lowered_sparql_query_kind,
    parse_prefix_lines, required_this_predicates, validate_prebound_variable_usage,
    AdjacentPredicateWhitelistPlan, CompatibilitySide, LocalSetCompatibilityMode,
    LocalSetCompatibilityPlan, LoweredPropertyPath, LoweredSparqlQueryKind, MessageTemplater,
    MissingRelatedNodePlan, RequiredPathSupportPlan, SparqlExecutor, ThisPredicateDirection,
};
use crate::types::{ComponentID, Path, Severity, TraceItem};
use log::debug;
use oxigraph::model::vocab::rdfs;
use oxigraph::model::vocab::xsd;
use oxigraph::model::{NamedNode, Term, TermRef};
use oxigraph::sparql::{QueryResults, Variable};
use rayon::prelude::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::time::Instant;

fn query_mentions_var(query: &str, var: &str) -> bool {
    fn contains(query: &str, prefix: char, var: &str) -> bool {
        let mut start = 0;
        let bytes = query.as_bytes();
        let var_bytes = var.as_bytes();
        while let Some(pos) = query[start..].find(prefix) {
            let idx = start + pos + 1; // skip prefix itself
            if bytes.len() >= idx + var_bytes.len()
                && &bytes[idx..idx + var_bytes.len()] == var_bytes
            {
                let after = idx + var_bytes.len();
                if after >= bytes.len() {
                    return true;
                }
                let next = bytes[after] as char;
                if !next.is_ascii_alphanumeric() && next != '_' {
                    return true;
                }
            }
            start += pos + 1;
        }
        false
    }

    contains(query, '?', var) || contains(query, '$', var)
}

fn gather_default_substitutions(
    context: &Context,
    current_shape_term: Option<&Term>,
    value_term: Option<&Term>,
    path_override: Option<&String>,
    message_prefixes: &[(String, String)],
) -> Vec<(String, String)> {
    let mut substitutions = Vec::new();
    substitutions.push((
        "this".to_string(),
        term_to_message_value(context.focus_node(), message_prefixes),
    ));

    if let Some(shape_term) = current_shape_term {
        substitutions.push((
            "currentShape".to_string(),
            term_to_message_value(shape_term, message_prefixes),
        ));
    }

    if let Some(value) = value_term {
        substitutions.push((
            "value".to_string(),
            term_to_message_value(value, message_prefixes),
        ));
    }

    if let Some(path) = path_override {
        substitutions.push(("PATH".to_string(), path.clone()));
    }

    substitutions
}

fn gather_default_substitutions_for_focus(
    focus_node: &Term,
    current_shape_term: Option<&Term>,
    value_term: Option<&Term>,
    path_override: Option<&String>,
    message_prefixes: &[(String, String)],
) -> Vec<(String, String)> {
    let mut substitutions = Vec::new();
    substitutions.push((
        "this".to_string(),
        term_to_message_value(focus_node, message_prefixes),
    ));

    if let Some(shape_term) = current_shape_term {
        substitutions.push((
            "currentShape".to_string(),
            term_to_message_value(shape_term, message_prefixes),
        ));
    }

    if let Some(value) = value_term {
        substitutions.push((
            "value".to_string(),
            term_to_message_value(value, message_prefixes),
        ));
    }

    if let Some(path) = path_override {
        substitutions.push(("PATH".to_string(), path.clone()));
    }

    substitutions
}

const SPARQL_VALUES_BATCH_MIN_FOCI: usize = 16;
const SPARQL_VALUES_BATCH_SIZE_MIN: usize = 8;
const SPARQL_VALUES_BATCH_SIZE_MAX: usize = 32;
const SPARQL_PROBE_BATCH_SIZE: usize = 8;
const SPARQL_PROBE_MAX_MS_PER_FOCUS: f64 = 500.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SparqlExecutionMode {
    PerFocus,
    ProbeMicroBatch,
    BatchSmallChunks,
    BatchLargeChunks,
}

pub(crate) fn should_batch_sparql_focuses(focus_count: usize) -> bool {
    focus_count >= SPARQL_VALUES_BATCH_MIN_FOCI
}

fn plan_sparql_execution(
    original_focus_count: usize,
    candidate_focus_count: usize,
    required_predicate_count: usize,
) -> SparqlExecutionMode {
    if required_predicate_count == 0 || !should_batch_sparql_focuses(candidate_focus_count) {
        return SparqlExecutionMode::PerFocus;
    }

    if original_focus_count == 0 {
        return SparqlExecutionMode::PerFocus;
    }

    let selectivity = candidate_focus_count as f64 / original_focus_count as f64;
    if candidate_focus_count >= 512 {
        return SparqlExecutionMode::BatchLargeChunks;
    }
    if candidate_focus_count >= 128 && selectivity <= 0.40 {
        return SparqlExecutionMode::BatchSmallChunks;
    }
    if candidate_focus_count >= 64 && selectivity <= 0.25 {
        return SparqlExecutionMode::BatchSmallChunks;
    }
    if candidate_focus_count >= 64 {
        return SparqlExecutionMode::ProbeMicroBatch;
    }

    SparqlExecutionMode::PerFocus
}

fn sparql_values_batch_size(focus_count: usize, mode: SparqlExecutionMode) -> usize {
    if matches!(
        mode,
        SparqlExecutionMode::BatchSmallChunks | SparqlExecutionMode::ProbeMicroBatch
    ) {
        return SPARQL_VALUES_BATCH_SIZE_MIN;
    }
    let target_chunks = rayon::current_num_threads().saturating_mul(4).max(1);
    (focus_count / target_chunks).clamp(SPARQL_VALUES_BATCH_SIZE_MIN, SPARQL_VALUES_BATCH_SIZE_MAX)
}

fn term_to_message_value(term: &Term, message_prefixes: &[(String, String)]) -> String {
    match term {
        Term::Literal(lit) => lit.value().to_string(),
        _ => format_term_with_namespace_aliases(term, message_prefixes),
    }
}

fn term_ref_to_message_value(term: TermRef<'_>, message_prefixes: &[(String, String)]) -> String {
    term_to_message_value(&term.into_owned(), message_prefixes)
}

fn hash_query_64(query: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    query.hash(&mut hasher);
    hasher.finish()
}

fn resolve_lowered_property_path(
    context: &ValidationContext,
    focus_node: &Term,
    path: &LoweredPropertyPath,
) -> Result<Vec<Term>, String> {
    match path {
        LoweredPropertyPath::SelfNode => Ok(vec![focus_node.clone()]),
        LoweredPropertyPath::NamedNode(predicate) => {
            context.focus_objects_for_predicate(focus_node, &Term::NamedNode(predicate.clone()))
        }
        LoweredPropertyPath::ReverseNamedNode(predicate) => context
            .focus_subjects_for_inverse_predicate(focus_node, &Term::NamedNode(predicate.clone())),
        LoweredPropertyPath::ZeroOrOne(inner) => {
            let mut results = HashSet::from([focus_node.clone()]);
            results.extend(resolve_lowered_property_path(context, focus_node, inner)?);
            Ok(results.into_iter().collect())
        }
        LoweredPropertyPath::ZeroOrMore(inner) => {
            let mut results = HashSet::new();
            let mut frontier = vec![focus_node.clone()];
            while let Some(node) = frontier.pop() {
                if !results.insert(node.clone()) {
                    continue;
                }
                frontier.extend(resolve_lowered_property_path(context, &node, inner)?);
            }
            Ok(results.into_iter().collect())
        }
        LoweredPropertyPath::OneOrMore(inner) => {
            let mut results = HashSet::new();
            let mut frontier = resolve_lowered_property_path(context, focus_node, inner)?;
            while let Some(node) = frontier.pop() {
                if !results.insert(node.clone()) {
                    continue;
                }
                frontier.extend(resolve_lowered_property_path(context, &node, inner)?);
            }
            Ok(results.into_iter().collect())
        }
        LoweredPropertyPath::Sequence(segments) => {
            let mut current = vec![focus_node.clone()];
            for segment in segments {
                let mut next = HashSet::new();
                for node in &current {
                    next.extend(resolve_lowered_property_path(context, node, segment)?);
                }
                current = next.into_iter().collect();
                if current.is_empty() {
                    break;
                }
            }
            Ok(current)
        }
        LoweredPropertyPath::Alternative(alternatives) => {
            let mut results = HashSet::new();
            for alternative in alternatives {
                results.extend(resolve_lowered_property_path(
                    context,
                    focus_node,
                    alternative,
                )?);
            }
            Ok(results.into_iter().collect())
        }
    }
}

fn resolve_lowered_property_path_in_store(
    context: &ValidationContext,
    focus_node: &Term,
    path: &LoweredPropertyPath,
) -> Result<Vec<Term>, String> {
    match path {
        LoweredPropertyPath::SelfNode => Ok(vec![focus_node.clone()]),
        LoweredPropertyPath::NamedNode(predicate) => {
            let Ok(subject_ref) = focus_node.try_to_subject_ref() else {
                return Ok(Vec::new());
            };
            Ok(context
                .quads_for_pattern(Some(subject_ref), Some(predicate.as_ref()), None, None)?
                .into_iter()
                .map(|quad| quad.object)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect())
        }
        LoweredPropertyPath::ReverseNamedNode(predicate) => Ok(context
            .quads_for_pattern(None, Some(predicate.as_ref()), Some(focus_node), None)?
            .into_iter()
            .map(|quad| quad.subject.into())
            .collect::<HashSet<Term>>()
            .into_iter()
            .collect()),
        LoweredPropertyPath::ZeroOrOne(inner) => {
            let mut results = HashSet::from([focus_node.clone()]);
            results.extend(resolve_lowered_property_path_in_store(
                context, focus_node, inner,
            )?);
            Ok(results.into_iter().collect())
        }
        LoweredPropertyPath::ZeroOrMore(inner) => {
            let mut results = HashSet::new();
            let mut frontier = vec![focus_node.clone()];
            while let Some(node) = frontier.pop() {
                if !results.insert(node.clone()) {
                    continue;
                }
                frontier.extend(resolve_lowered_property_path_in_store(
                    context, &node, inner,
                )?);
            }
            Ok(results.into_iter().collect())
        }
        LoweredPropertyPath::OneOrMore(inner) => {
            let mut results = HashSet::new();
            let mut frontier = resolve_lowered_property_path_in_store(context, focus_node, inner)?;
            while let Some(node) = frontier.pop() {
                if !results.insert(node.clone()) {
                    continue;
                }
                frontier.extend(resolve_lowered_property_path_in_store(
                    context, &node, inner,
                )?);
            }
            Ok(results.into_iter().collect())
        }
        LoweredPropertyPath::Sequence(segments) => {
            let mut current = vec![focus_node.clone()];
            for segment in segments {
                let mut next = HashSet::new();
                for node in &current {
                    next.extend(resolve_lowered_property_path_in_store(
                        context, node, segment,
                    )?);
                }
                current = next.into_iter().collect();
                if current.is_empty() {
                    break;
                }
            }
            Ok(current)
        }
        LoweredPropertyPath::Alternative(alternatives) => {
            let mut results = HashSet::new();
            for alternative in alternatives {
                results.extend(resolve_lowered_property_path_in_store(
                    context,
                    focus_node,
                    alternative,
                )?);
            }
            Ok(results.into_iter().collect())
        }
    }
}

fn lowered_path_reaches_target(
    context: &ValidationContext,
    focus_node: &Term,
    path: &LoweredPropertyPath,
    target: &Term,
) -> Result<bool, String> {
    Ok(resolve_lowered_property_path(context, focus_node, path)?
        .iter()
        .any(|candidate| candidate == target))
}

#[allow(clippy::too_many_arguments)]
fn try_run_lowered_adjacent_predicate_whitelist(
    component_id: ComponentID,
    c: &Context,
    context: &ValidationContext,
    current_shape_term: Option<&Term>,
    constraint_node: &Term,
    plan: &AdjacentPredicateWhitelistPlan,
    path_substitution_value: Option<&String>,
    message_prefixes: &[(String, String)],
    messages: &[Term],
    severity: Option<Severity>,
) -> Result<Option<Vec<ComponentValidationResult>>, String> {
    let anchors = resolve_lowered_property_path(context, c.focus_node(), &plan.anchor_path)?;
    if anchors.is_empty() {
        return Ok(Some(vec![]));
    }

    let allowed: HashSet<Term> = plan
        .allowed_predicates
        .iter()
        .cloned()
        .map(Term::NamedNode)
        .collect();

    let has_disallowed = anchors.iter().any(|anchor| {
        let outgoing = context
            .focus_outgoing_predicates(anchor)
            .unwrap_or_default()
            .into_iter();
        let incoming = context
            .focus_incoming_predicates(anchor)
            .unwrap_or_default()
            .into_iter();
        outgoing
            .chain(incoming)
            .any(|predicate| !allowed.contains(&predicate))
    });

    if !has_disallowed {
        return Ok(Some(vec![]));
    }

    let substitutions_for_messages = gather_default_substitutions(
        c,
        current_shape_term,
        Some(c.focus_node()),
        path_substitution_value,
        message_prefixes,
    );
    let (message_opt, message_terms) = context
        .sparql_services()
        .instantiate_messages(messages, &substitutions_for_messages);
    let message =
        message_opt.unwrap_or_else(|| "Node does not conform to SPARQL constraint".to_string());
    let failure = ValidationFailure::new(
        component_id,
        Some(c.focus_node().clone()),
        message,
        None,
        Some(constraint_node.clone()),
    )
    .with_severity(severity)
    .with_message_terms(message_terms);
    Ok(Some(vec![ComponentValidationResult::Fail(
        c.clone(),
        failure,
    )]))
}

#[allow(clippy::too_many_arguments)]
fn try_run_lowered_required_path_support(
    component_id: ComponentID,
    c: &Context,
    context: &ValidationContext,
    current_shape_term: Option<&Term>,
    constraint_node: &Term,
    plan: &RequiredPathSupportPlan,
    path_substitution_value: Option<&String>,
    message_prefixes: &[(String, String)],
    messages: &[Term],
    severity: Option<Severity>,
) -> Result<Option<Vec<ComponentValidationResult>>, String> {
    let related_nodes =
        resolve_lowered_property_path(context, c.focus_node(), &plan.antecedent_path)?;
    if related_nodes.is_empty() {
        return Ok(Some(vec![]));
    }

    let mut results = Vec::new();
    for related_node in related_nodes {
        if lowered_path_reaches_target(context, c.focus_node(), &plan.support_path, &related_node)?
        {
            continue;
        }

        let mut substitutions_for_messages = gather_default_substitutions(
            c,
            current_shape_term,
            if c.source_shape().as_node_id().is_some() {
                Some(c.focus_node())
            } else {
                None
            },
            path_substitution_value,
            message_prefixes,
        );
        substitutions_for_messages.push((
            plan.target_variable.clone(),
            term_to_message_value(&related_node, message_prefixes),
        ));
        let (message_opt, message_terms) = context
            .sparql_services()
            .instantiate_messages(messages, &substitutions_for_messages);
        let message =
            message_opt.unwrap_or_else(|| "Node does not conform to SPARQL constraint".to_string());
        let failure = ValidationFailure::new(
            component_id,
            if c.source_shape().as_node_id().is_some() {
                Some(c.focus_node().clone())
            } else {
                None
            },
            message,
            None,
            Some(constraint_node.clone()),
        )
        .with_severity(severity.clone())
        .with_message_terms(message_terms);
        results.push(ComponentValidationResult::Fail(c.clone(), failure));
    }

    Ok(Some(results))
}

#[allow(clippy::too_many_arguments)]
fn try_run_lowered_missing_related_node(
    component_id: ComponentID,
    c: &Context,
    context: &ValidationContext,
    current_shape_term: Option<&Term>,
    constraint_node: &Term,
    plan: &MissingRelatedNodePlan,
    path_substitution_value: Option<&String>,
    message_prefixes: &[(String, String)],
    messages: &[Term],
    severity: Option<Severity>,
) -> Result<Option<Vec<ComponentValidationResult>>, String> {
    let mut related_nodes =
        resolve_lowered_property_path_in_store(context, c.focus_node(), &plan.related_path)?;
    if let Some(class_term) = &plan.related_class {
        related_nodes.retain(|related| {
            term_satisfies_lowered_class(context, related, class_term).unwrap_or(false)
        });
    }
    if related_nodes.is_empty() {
        return Ok(Some(vec![]));
    }

    let required_nodes =
        resolve_lowered_property_path_in_store(context, c.focus_node(), &plan.required_path)?;
    if !required_nodes.is_empty() {
        return Ok(Some(vec![]));
    }

    let mut results = Vec::new();
    for related_node in related_nodes {
        let mut substitutions_for_messages = gather_default_substitutions(
            c,
            current_shape_term,
            if c.source_shape().as_node_id().is_some() {
                Some(c.focus_node())
            } else {
                None
            },
            path_substitution_value,
            message_prefixes,
        );
        substitutions_for_messages.push((
            plan.related_variable.clone(),
            term_to_message_value(&related_node, message_prefixes),
        ));
        let (message_opt, message_terms) = context
            .sparql_services()
            .instantiate_messages(messages, &substitutions_for_messages);
        let message =
            message_opt.unwrap_or_else(|| "Node does not conform to SPARQL constraint".to_string());
        let failure = ValidationFailure::new(
            component_id,
            if c.source_shape().as_node_id().is_some() {
                Some(c.focus_node().clone())
            } else {
                None
            },
            message,
            None,
            Some(constraint_node.clone()),
        )
        .with_severity(severity.clone())
        .with_message_terms(message_terms);
        results.push(ComponentValidationResult::Fail(c.clone(), failure));
    }

    Ok(Some(results))
}

fn term_satisfies_lowered_class(
    context: &ValidationContext,
    node: &Term,
    class_term: &Term,
) -> Result<bool, String> {
    Ok(context
        .class_constraint_matches_fast(node, class_term)?
        .unwrap_or(false))
}

fn term_has_direct_predicate_in_store(
    context: &ValidationContext,
    node: &Term,
    predicate: &NamedNode,
) -> Result<bool, String> {
    let Ok(subject_ref) = node.try_to_subject_ref() else {
        return Ok(false);
    };
    Ok(!context
        .quads_for_pattern(Some(subject_ref), Some(predicate.as_ref()), None, None)?
        .is_empty())
}

fn lowered_subclass_related(
    context: &ValidationContext,
    left: &Term,
    right: &Term,
) -> Result<bool, String> {
    if let Some(result) = context.subclass_of_or_same_fast(left, right)? {
        return Ok(result);
    }
    let subclass_star = LoweredPropertyPath::ZeroOrMore(Box::new(LoweredPropertyPath::NamedNode(
        rdfs::SUB_CLASS_OF.into_owned(),
    )));
    Ok(
        resolve_lowered_property_path_in_store(context, left, &subclass_star)?
            .into_iter()
            .any(|candidate| candidate == *right),
    )
}

fn are_terms_compatible_via_subclass(
    context: &ValidationContext,
    left: &Term,
    right: &Term,
) -> Result<bool, String> {
    Ok(lowered_subclass_related(context, left, right)?
        || lowered_subclass_related(context, right, left)?)
}

fn collect_constituents(
    context: &ValidationContext,
    value: &Term,
    constituent_path: Option<&LoweredPropertyPath>,
) -> Result<Vec<Term>, String> {
    let Some(constituent_path) = constituent_path else {
        return Ok(Vec::new());
    };
    resolve_lowered_property_path_in_store(context, value, constituent_path)
}

fn value_pair_is_incompatible(
    context: &ValidationContext,
    left_value: &Term,
    right_value: &Term,
    plan: &LocalSetCompatibilityPlan,
) -> Result<bool, String> {
    match plan.mode {
        LocalSetCompatibilityMode::PurePure => {
            let left_is_pure = !term_has_direct_predicate_in_store(
                context,
                left_value,
                &plan.composed_of_predicate,
            )?;
            let right_is_pure = !term_has_direct_predicate_in_store(
                context,
                right_value,
                &plan.composed_of_predicate,
            )?;
            if !left_is_pure || !right_is_pure {
                return Ok(false);
            }
            Ok(!are_terms_compatible_via_subclass(
                context,
                left_value,
                right_value,
            )?)
        }
        LocalSetCompatibilityMode::CompositeVsPure { composite_side } => {
            let (composite_value, pure_value) = match composite_side {
                CompatibilitySide::Left => (left_value, right_value),
                CompatibilitySide::Right => (right_value, left_value),
            };
            let pure_is_pure = !term_has_direct_predicate_in_store(
                context,
                pure_value,
                &plan.composed_of_predicate,
            )?;
            if !pure_is_pure {
                return Ok(false);
            }
            let constituents =
                collect_constituents(context, composite_value, plan.constituent_path.as_ref())?;
            if constituents.is_empty() {
                return Ok(false);
            }
            Ok(!constituents.iter().any(|constituent| {
                are_terms_compatible_via_subclass(context, constituent, pure_value).unwrap_or(false)
            }))
        }
        LocalSetCompatibilityMode::CompositeVsComposite => {
            let left_constituents =
                collect_constituents(context, left_value, plan.constituent_path.as_ref())?;
            let right_constituents =
                collect_constituents(context, right_value, plan.constituent_path.as_ref())?;
            if left_constituents.is_empty() || right_constituents.is_empty() {
                return Ok(false);
            }
            Ok(!left_constituents.iter().any(|left_constituent| {
                right_constituents.iter().any(|right_constituent| {
                    are_terms_compatible_via_subclass(context, left_constituent, right_constituent)
                        .unwrap_or(false)
                })
            }))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn try_run_lowered_local_set_compatibility(
    component_id: ComponentID,
    c: &Context,
    context: &ValidationContext,
    current_shape_term: Option<&Term>,
    constraint_node: &Term,
    plan: &LocalSetCompatibilityPlan,
    path_substitution_value: Option<&String>,
    message_prefixes: &[(String, String)],
    messages: &[Term],
    severity: Option<Severity>,
) -> Result<Option<Vec<ComponentValidationResult>>, String> {
    let left_anchors =
        resolve_lowered_property_path_in_store(context, c.focus_node(), &plan.left_anchor_path)?;
    let right_anchors =
        resolve_lowered_property_path_in_store(context, c.focus_node(), &plan.right_anchor_path)?;
    if left_anchors.is_empty() || right_anchors.is_empty() {
        return Ok(Some(vec![]));
    }

    let mut results = Vec::new();
    for left_anchor in &left_anchors {
        if let Some(class_term) = &plan.left_class {
            if !term_satisfies_lowered_class(context, left_anchor, class_term)? {
                continue;
            }
        }
        let left_values =
            resolve_lowered_property_path_in_store(context, left_anchor, &plan.left_value_path)?;
        if left_values.is_empty() {
            continue;
        }

        for right_anchor in &right_anchors {
            if plan.distinct_anchors && left_anchor == right_anchor {
                continue;
            }
            if let Some(class_term) = &plan.right_class {
                if !term_satisfies_lowered_class(context, right_anchor, class_term)? {
                    continue;
                }
            }
            let right_values = resolve_lowered_property_path_in_store(
                context,
                right_anchor,
                &plan.right_value_path,
            )?;
            if right_values.is_empty() {
                continue;
            }

            for left_value in &left_values {
                for right_value in &right_values {
                    if !value_pair_is_incompatible(context, left_value, right_value, plan)? {
                        continue;
                    }

                    let mut substitutions_for_messages = gather_default_substitutions(
                        c,
                        current_shape_term,
                        if c.source_shape().as_node_id().is_some() {
                            Some(c.focus_node())
                        } else {
                            None
                        },
                        path_substitution_value,
                        message_prefixes,
                    );
                    substitutions_for_messages.push((
                        plan.left_anchor_var.clone(),
                        term_to_message_value(left_anchor, message_prefixes),
                    ));
                    substitutions_for_messages.push((
                        plan.right_anchor_var.clone(),
                        term_to_message_value(right_anchor, message_prefixes),
                    ));
                    substitutions_for_messages.push((
                        plan.left_value_var.clone(),
                        term_to_message_value(left_value, message_prefixes),
                    ));
                    substitutions_for_messages.push((
                        plan.right_value_var.clone(),
                        term_to_message_value(right_value, message_prefixes),
                    ));
                    let (message_opt, message_terms) = context
                        .sparql_services()
                        .instantiate_messages(messages, &substitutions_for_messages);
                    let message = message_opt.unwrap_or_else(|| {
                        "Node does not conform to SPARQL constraint".to_string()
                    });
                    let failure = ValidationFailure::new(
                        component_id,
                        if c.source_shape().as_node_id().is_some() {
                            Some(c.focus_node().clone())
                        } else {
                            None
                        },
                        message,
                        None,
                        Some(constraint_node.clone()),
                    )
                    .with_severity(severity.clone())
                    .with_message_terms(message_terms);
                    results.push(ComponentValidationResult::Fail(c.clone(), failure));
                }
            }
        }
    }

    Ok(Some(results))
}

#[derive(Debug)]
struct ChunkBatchOutcome {
    elapsed: std::time::Duration,
    rows_returned: u64,
    violations: Vec<(Term, BatchedSparqlViolation)>,
}

#[allow(clippy::too_many_arguments)]
fn execute_batched_sparql_chunk(
    context: &ValidationContext,
    full_query_str: &str,
    prepared_query: &oxigraph::sparql::PreparedSparqlQuery,
    this_var: &Variable,
    chunk: &[Term],
    substitutions: &[(Variable, Term)],
    current_shape_term: Option<&Term>,
    path_substitution_value: Option<&String>,
    message_prefixes: &[(String, String)],
    messages: &[Term],
) -> Result<ChunkBatchOutcome, String> {
    let query_started = Instant::now();
    let query_outcome = context.execute_with_value_rows(
        full_query_str,
        prepared_query,
        this_var,
        chunk,
        substitutions,
    );
    match query_outcome {
        Ok(QueryResults::Solutions(solutions)) => {
            let mut rows_returned = 0u64;
            let mut chunk_violations: Vec<(Term, BatchedSparqlViolation)> = Vec::new();
            let mut seen_solutions: HashSet<Vec<(String, Term)>> = HashSet::new();
            for solution_res in solutions {
                rows_returned = rows_returned.saturating_add(1);
                let solution = solution_res.map_err(|e| e.to_string())?;
                if let Some(Term::Literal(failure)) = solution.get("failure") {
                    if failure.datatype() == xsd::BOOLEAN && failure.value() == "true" {
                        return Err("SPARQL query reported a failure.".to_string());
                    }
                }

                let Some(focus_node) = solution.get("this").cloned() else {
                    return Err(
                        "Batched SPARQL constraint query result is missing ?this.".to_string()
                    );
                };

                let mut solution_key: Vec<(String, Term)> = solution
                    .variables()
                    .iter()
                    .filter_map(|var| {
                        solution
                            .get(var)
                            .map(|term| (var.as_str().to_string(), term.clone()))
                    })
                    .collect();
                solution_key.sort_by(|a, b| a.0.cmp(&b.0));
                if !seen_solutions.insert(solution_key) {
                    continue;
                }

                let failed_value_node = if let Some(val) = solution.get("value") {
                    Some(val.clone())
                } else {
                    Some(focus_node.clone())
                };

                let mut message_templates = Vec::new();
                if let Some(term) = solution.get("message") {
                    message_templates.push(term.clone());
                }
                if message_templates.is_empty() && !messages.is_empty() {
                    message_templates.extend(messages.iter().cloned());
                }

                let mut substitutions_for_messages = gather_default_substitutions_for_focus(
                    &focus_node,
                    current_shape_term,
                    failed_value_node.as_ref(),
                    path_substitution_value,
                    message_prefixes,
                );
                for var in solution.variables() {
                    if let Some(term) = solution.get(var) {
                        substitutions_for_messages.push((
                            var.as_str().to_string(),
                            term_ref_to_message_value(term.into(), message_prefixes),
                        ));
                    }
                }

                let (message_opt, message_terms) = context
                    .sparql_services()
                    .instantiate_messages(&message_templates, &substitutions_for_messages);
                let message = message_opt
                    .unwrap_or_else(|| "Node does not conform to SPARQL constraint".to_string());
                let result_path = if let Some(Term::NamedNode(path_iri)) = solution.get("path") {
                    Some(Path::Simple(Term::NamedNode(path_iri.clone())))
                } else {
                    None
                };

                chunk_violations.push((
                    focus_node,
                    BatchedSparqlViolation {
                        failed_value_node,
                        message,
                        result_path,
                        message_terms,
                    },
                ));
            }

            Ok(ChunkBatchOutcome {
                elapsed: query_started.elapsed(),
                rows_returned,
                violations: chunk_violations,
            })
        }
        Err(err) => Err(format!("SPARQL query failed: {}", err)),
        _ => Err("SPARQL constraint query did not return solutions.".to_string()),
    }
}

#[allow(clippy::too_many_arguments)]
fn try_run_batched_sparql_constraint(
    context: &ValidationContext,
    component_id: ComponentID,
    source_shape: crate::context::SourceShape,
    explicit_focus_nodes: Option<&[Term]>,
    current_shape_term: Option<&Term>,
    constraint_node: &Term,
    full_query_str: &str,
    prepared_query: &oxigraph::sparql::PreparedSparqlQuery,
    required_predicates: &HashSet<crate::sparql::ThisPredicateRequirement>,
    path_substitution_value: Option<&String>,
    message_prefixes: &[(String, String)],
    messages: &[Term],
    query_hash: u64,
) -> Result<Option<BatchedSparqlResult>, String> {
    if !query_mentions_var(full_query_str, "this") {
        return Ok(None);
    }
    let owned_focus_nodes;
    let focus_nodes: &[Term] = if let Some(explicit) = explicit_focus_nodes {
        explicit
    } else {
        let cached = if let Some(node_shape_id) = source_shape.as_node_id() {
            context.cached_node_targets(node_shape_id)
        } else if let Some(prop_shape_id) = source_shape.as_prop_id() {
            context.cached_prop_targets(prop_shape_id)
        } else {
            None
        };
        let Some(cached) = cached else {
            return Ok(None);
        };
        owned_focus_nodes = cached;
        owned_focus_nodes.as_ref()
    };

    let candidate_focuses: Vec<Term> = focus_nodes
        .iter()
        .filter(|focus| {
            !(context.skip_sparql_blank_targets() && matches!(focus, Term::BlankNode(_)))
        })
        .filter(|focus| {
            required_predicates.iter().all(|requirement| {
                let predicate_term = Term::NamedNode(requirement.predicate.clone());
                let count = match requirement.direction {
                    ThisPredicateDirection::Outgoing => context
                        .focus_outgoing_predicate_count(focus, &predicate_term)
                        .unwrap_or(0),
                    ThisPredicateDirection::Incoming => context
                        .focus_incoming_predicate_count(focus, &predicate_term)
                        .unwrap_or(0),
                };
                count > 0
            })
        })
        .cloned()
        .collect();

    let execution_mode = plan_sparql_execution(
        focus_nodes.len(),
        candidate_focuses.len(),
        required_predicates.len(),
    );
    if execution_mode == SparqlExecutionMode::PerFocus {
        return Ok(None);
    }

    let mut substitutions = Vec::new();
    if let Some(shape_term) = current_shape_term {
        if query_mentions_var(full_query_str, "currentShape") {
            substitutions.push((Variable::new_unchecked("currentShape"), shape_term.clone()));
        }
    }
    if query_mentions_var(full_query_str, "shapesGraph") {
        substitutions.push((
            Variable::new_unchecked("shapesGraph"),
            context.model.shape_graph_iri.clone().into(),
        ));
    }

    let this_var = Variable::new_unchecked("this");
    let mut remaining_focuses = candidate_focuses;
    let mut completed_chunks: Vec<ChunkBatchOutcome> = Vec::new();
    if execution_mode == SparqlExecutionMode::ProbeMicroBatch {
        let probe_len = remaining_focuses.len().min(SPARQL_PROBE_BATCH_SIZE);
        let probe_chunk = remaining_focuses[..probe_len].to_vec();
        let probe = execute_batched_sparql_chunk(
            context,
            full_query_str,
            prepared_query,
            &this_var,
            &probe_chunk,
            &substitutions,
            current_shape_term,
            path_substitution_value,
            message_prefixes,
            messages,
        )?;
        let probe_ms_per_focus = probe.elapsed.as_secs_f64() * 1000.0 / probe_chunk.len() as f64;
        if probe.rows_returned == 0 && probe_ms_per_focus > SPARQL_PROBE_MAX_MS_PER_FOCUS {
            return Ok(None);
        }
        completed_chunks.push(probe);
        remaining_focuses.drain(..probe_len);
    }

    let batch_size = sparql_values_batch_size(remaining_focuses.len().max(1), execution_mode);
    let chunked_focuses: Vec<Vec<Term>> = remaining_focuses
        .chunks(batch_size)
        .map(|chunk| chunk.to_vec())
        .collect();
    let chunk_outcomes: Vec<Result<ChunkBatchOutcome, String>> = chunked_focuses
        .par_iter()
        .map(|chunk| {
            execute_batched_sparql_chunk(
                context,
                full_query_str,
                prepared_query,
                &this_var,
                chunk,
                &substitutions,
                current_shape_term,
                path_substitution_value,
                message_prefixes,
                messages,
            )
        })
        .collect();

    let mut violations_by_focus: HashMap<Term, Vec<BatchedSparqlViolation>> = HashMap::new();
    completed_chunks.extend(chunk_outcomes.into_iter().collect::<Result<Vec<_>, _>>()?);
    for chunk in completed_chunks {
        context.record_sparql_query_call(
            source_shape.clone(),
            component_id,
            constraint_node,
            query_hash,
            chunk.rows_returned,
            chunk.elapsed,
        );
        for (focus_node, violation) in chunk.violations {
            violations_by_focus
                .entry(focus_node)
                .or_default()
                .push(violation);
        }
    }
    Ok(Some(BatchedSparqlResult {
        violations_by_focus,
    }))
}

#[derive(Debug, Clone)]
pub struct SPARQLConstraintComponent {
    pub constraint_node: Term,
}

impl SPARQLConstraintComponent {
    pub fn new(constraint_node: Term) -> Self {
        SPARQLConstraintComponent { constraint_node }
    }

    pub(crate) fn validate_batch_for_focuses(
        &self,
        component_id: ComponentID,
        source_shape: crate::context::SourceShape,
        focus_nodes: &[Term],
        path_substitution_value: Option<&String>,
        context: &ValidationContext,
    ) -> Result<Option<BatchedSparqlResult>, String> {
        let shacl = SHACL::new();
        let sparql_services = context.sparql_services();
        let constraint_subject = self.constraint_node.to_subject_ref();
        let current_shape_term = source_shape.get_term(context);

        let prefixes = sparql_services.prefixes_for_node(
            &self.constraint_node,
            &context.model.store,
            &context.model.env,
            context.shape_graph_iri_ref(),
        )?;
        let message_prefixes = parse_prefix_lines(&prefixes);

        let select_query_term = context
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.select),
                None,
                Some(context.shape_graph_iri_ref()),
            )?
            .into_iter()
            .map(|q| q.object)
            .next()
            .ok_or_else(|| "sh:sparql constraint does not have a sh:select query".to_string())?;

        let mut select_query = match &select_query_term {
            Term::Literal(lit) => lit.value().to_string(),
            _ => {
                return Err(format!(
                    "sh:select value must be a literal, found {:?}",
                    select_query_term
                ));
            }
        };

        if let Some(path_str) = path_substitution_value {
            if select_query.contains("$PATH") {
                select_query = select_query.replace("$PATH", path_str);
            }
        }

        let full_query_str = if !prefixes.is_empty() {
            format!("{}\n{}", prefixes, select_query)
        } else {
            select_query
        };

        let algebra_query = sparql_services
            .algebra(&full_query_str)
            .map_err(|e| format!("Failed to parse SPARQL constraint query: {}", e))?;

        let mut prebound_vars: HashSet<Variable> = HashSet::new();
        let mut optional_prebound_vars: HashSet<Variable> = HashSet::new();
        if query_mentions_var(&full_query_str, "this") {
            prebound_vars.insert(Variable::new_unchecked("this"));
        }
        if query_mentions_var(&full_query_str, "currentShape") {
            let var = Variable::new_unchecked("currentShape");
            optional_prebound_vars.insert(var.clone());
            prebound_vars.insert(var);
        }
        if query_mentions_var(&full_query_str, "shapesGraph") {
            let var = Variable::new_unchecked("shapesGraph");
            optional_prebound_vars.insert(var.clone());
            prebound_vars.insert(var);
        }

        ensure_pre_binding_semantics(
            &algebra_query,
            "SPARQL constraint query",
            &prebound_vars,
            &optional_prebound_vars,
        )?;

        let required_predicates = required_this_predicates(&algebra_query);
        let prepared_query = context
            .prepare_query(&full_query_str)
            .map_err(|e| format!("Failed to prepare SPARQL constraint query: {}", e))?;

        let messages: Vec<Term> = context
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.message),
                None,
                Some(context.shape_graph_iri_ref()),
            )?
            .into_iter()
            .map(|q| q.object)
            .collect();

        let query_hash = hash_query_64(&full_query_str);

        try_run_batched_sparql_constraint(
            context,
            component_id,
            source_shape,
            Some(focus_nodes),
            current_shape_term.as_ref(),
            &self.constraint_node,
            &full_query_str,
            &prepared_query,
            &required_predicates,
            path_substitution_value,
            &message_prefixes,
            &messages,
            query_hash,
        )
    }
}

impl GraphvizOutput for SPARQLConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#SPARQLConstraintComponent")
    }

    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String {
        let shacl = SHACL::new();
        let subject = self.constraint_node.to_subject_ref();
        let select_query_opt = context
            .quads_for_pattern(
                Some(subject),
                Some(shacl.select),
                None,
                Some(context.shape_graph_iri_ref()),
            )
            .unwrap_or_default()
            .into_iter()
            .map(|q| q.object)
            .find_map(|object| match object {
                Term::Literal(lit) => Some(lit.value().to_string()),
                _ => None,
            });

        let label_str = match select_query_opt {
            Some(query) => format!("SPARQL constraint\n{}", query.replace('\n', " ")),
            None => format!(
                "SPARQL constraint\n{}",
                format_term_for_label(&self.constraint_node)
            ),
        };

        format!(
            "{} [label=\"{}\"];",
            component_id.to_graphviz_id(),
            sanitize_graphviz_string(&label_str)
        )
    }
}

impl ValidateComponent for SPARQLConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let shacl = SHACL::new();
        let sparql_services = context.sparql_services();
        let constraint_subject = self.constraint_node.to_subject_ref();

        if context.skip_sparql_blank_targets() && matches!(c.focus_node(), Term::BlankNode(_)) {
            debug!(
                "Skipping SPARQL constraint {} for blank focus node {}",
                format_term_for_label(&self.constraint_node),
                format_term_for_label(c.focus_node())
            );
            return Ok(vec![]);
        }

        // 1. Check if deactivated
        if let Some(deactivated_quad) = context
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.deactivated),
                None,
                Some(context.shape_graph_iri_ref()),
            )?
            .into_iter()
            .next()
        {
            if let Term::Literal(lit) = &deactivated_quad.object {
                if lit.datatype() == xsd::BOOLEAN && lit.value() == "true" {
                    return Ok(vec![]);
                }
            }
        }

        // 2. Get SELECT query
        let mut select_query = if let Some(quad) = context
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.select),
                None,
                Some(context.shape_graph_iri_ref()),
            )?
            .into_iter()
            .next()
        {
            if let Term::Literal(lit) = &quad.object {
                lit.value().to_string()
            } else {
                return Err("sh:select value must be a literal string".to_string());
            }
        } else {
            return Err("SPARQL constraint is missing sh:select".to_string());
        };

        validate_prebound_variable_usage(
            &select_query,
            &format!(
                "SPARQL constraint {}",
                format_term_for_label(&self.constraint_node)
            ),
            true,
            false,
        )?;

        // Collect prefixes using the shared SPARQL services
        let prefixes = context.prefixes_for_node(&self.constraint_node)?;
        let message_prefixes = parse_prefix_lines(&prefixes);

        // Handle $PATH substitution for property shapes
        let mut path_substitution_value: Option<String> = None;
        if c.source_shape().as_prop_id().is_some() {
            if let Some(prop_id) = c.source_shape().as_prop_id() {
                if let Some(prop_shape) = context.model.get_prop_shape_by_id(prop_id) {
                    let path_str = prop_shape.sparql_path();
                    path_substitution_value = Some(path_str.clone());
                    select_query = select_query.replace("$PATH", &path_str);
                }
            }
        }

        let full_query_str = if !prefixes.is_empty() {
            format!("{}\n{}", prefixes, select_query)
        } else {
            select_query
        };

        let algebra_query = sparql_services
            .algebra(&full_query_str)
            .map_err(|e| format!("Failed to parse SPARQL constraint query: {}", e))?;

        let mut prebound_vars: HashSet<Variable> = HashSet::new();
        let mut optional_prebound_vars: HashSet<Variable> = HashSet::new();

        if query_mentions_var(&full_query_str, "this") {
            prebound_vars.insert(Variable::new_unchecked("this"));
        }

        if query_mentions_var(&full_query_str, "currentShape") {
            let var = Variable::new_unchecked("currentShape");
            optional_prebound_vars.insert(var.clone());
            prebound_vars.insert(var);
        }

        if query_mentions_var(&full_query_str, "shapesGraph") {
            let var = Variable::new_unchecked("shapesGraph");
            optional_prebound_vars.insert(var.clone());
            prebound_vars.insert(var);
        }

        ensure_pre_binding_semantics(
            &algebra_query,
            "SPARQL constraint query",
            &prebound_vars,
            &optional_prebound_vars,
        )?;

        let lowered_query_kind = lowered_sparql_query_kind(&algebra_query);

        let required_predicates = required_this_predicates(&algebra_query);
        if !required_predicates.is_empty() {
            let mut can_match = true;
            for requirement in &required_predicates {
                let predicate_term = Term::NamedNode(requirement.predicate.clone());
                let count =
                    match requirement.direction {
                        ThisPredicateDirection::Outgoing => context
                            .focus_outgoing_predicate_count(c.focus_node(), &predicate_term)?,
                        ThisPredicateDirection::Incoming => context
                            .focus_incoming_predicate_count(c.focus_node(), &predicate_term)?,
                    };
                if count == 0 {
                    can_match = false;
                    break;
                }
            }

            if !can_match {
                return Ok(vec![]);
            }
        }

        let prepared_query = context
            .prepare_query(&full_query_str)
            .map_err(|e| format!("Failed to prepare SPARQL constraint query: {}", e))?;

        // Prepare pre-bound variables
        let mut substitutions = vec![];

        let current_shape_term = c.source_shape().get_term(context);

        if query_mentions_var(&full_query_str, "this") {
            // Only add if the query uses it
            substitutions.push((Variable::new_unchecked("this"), c.focus_node().clone()));
        }

        if let Some(shape_term) = current_shape_term.clone() {
            if query_mentions_var(&full_query_str, "currentShape") {
                // Only add if the query uses it
                substitutions.push((Variable::new_unchecked("currentShape"), shape_term));
            }
        }
        if query_mentions_var(&full_query_str, "shapesGraph") {
            // Only add if the query uses it
            substitutions.push((
                Variable::new_unchecked("shapesGraph"),
                context.model.shape_graph_iri.clone().into(),
            ));
        }

        // Get messages
        let messages: Vec<Term> = context
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.message),
                None,
                Some(context.shape_graph_iri_ref()),
            )?
            .into_iter()
            .map(|q| q.object)
            .collect();

        let severity = context
            .quads_for_pattern(
                Some(constraint_subject),
                Some(shacl.severity),
                None,
                Some(context.shape_graph_iri_ref()),
            )?
            .into_iter()
            .map(|q| q.object)
            .find_map(|term| <Severity as crate::types::SeverityExt>::from_term(&term));

        if let Some(LoweredSparqlQueryKind::AdjacentPredicateWhitelist(plan)) =
            lowered_query_kind.as_ref()
        {
            if let Some(results) = try_run_lowered_adjacent_predicate_whitelist(
                component_id,
                c,
                context,
                current_shape_term.as_ref(),
                &self.constraint_node,
                plan,
                path_substitution_value.as_ref(),
                &message_prefixes,
                &messages,
                severity.clone(),
            )? {
                return Ok(results);
            }
        }
        if let Some(LoweredSparqlQueryKind::RequiredPathSupport(plan)) = lowered_query_kind.as_ref()
        {
            if let Some(results) = try_run_lowered_required_path_support(
                component_id,
                c,
                context,
                current_shape_term.as_ref(),
                &self.constraint_node,
                plan,
                path_substitution_value.as_ref(),
                &message_prefixes,
                &messages,
                severity.clone(),
            )? {
                return Ok(results);
            }
        }
        if let Some(LoweredSparqlQueryKind::MissingRelatedNode(plan)) = lowered_query_kind.as_ref()
        {
            if let Some(results) = try_run_lowered_missing_related_node(
                component_id,
                c,
                context,
                current_shape_term.as_ref(),
                &self.constraint_node,
                plan,
                path_substitution_value.as_ref(),
                &message_prefixes,
                &messages,
                severity.clone(),
            )? {
                return Ok(results);
            }
        }
        if let Some(LoweredSparqlQueryKind::LocalSetCompatibility(plan)) =
            lowered_query_kind.as_ref()
        {
            if let Some(results) = try_run_lowered_local_set_compatibility(
                component_id,
                c,
                context,
                current_shape_term.as_ref(),
                &self.constraint_node,
                plan,
                path_substitution_value.as_ref(),
                &message_prefixes,
                &messages,
                severity.clone(),
            )? {
                return Ok(results);
            }
        }

        let source_shape = c.source_shape();
        let query_hash = hash_query_64(&full_query_str);

        let batched_result = context.get_or_compute_sparql_batch(
            source_shape.clone(),
            component_id,
            query_hash,
            || {
                try_run_batched_sparql_constraint(
                    context,
                    component_id,
                    source_shape.clone(),
                    None,
                    current_shape_term.as_ref(),
                    &self.constraint_node,
                    &full_query_str,
                    &prepared_query,
                    &required_predicates,
                    path_substitution_value.as_ref(),
                    &message_prefixes,
                    &messages,
                    query_hash,
                )
            },
        );
        match batched_result.as_ref() {
            Ok(Some(batch)) => {
                let mut results = Vec::new();
                for violation in batch
                    .violations_by_focus
                    .get(c.focus_node())
                    .cloned()
                    .unwrap_or_default()
                {
                    let failure = ValidationFailure::new(
                        component_id,
                        violation.failed_value_node,
                        violation.message,
                        violation.result_path,
                        Some(self.constraint_node.clone()),
                    )
                    .with_severity(severity.clone())
                    .with_message_terms(violation.message_terms);
                    results.push(ComponentValidationResult::Fail(c.clone(), failure));
                }
                return Ok(results);
            }
            Ok(None) => {}
            Err(err) => return Err(err.clone()),
        }

        // Execute query
        let query_started = Instant::now();
        let query_outcome =
            context.execute_prepared(&full_query_str, &prepared_query, &substitutions, true);
        match query_outcome {
            Ok(QueryResults::Solutions(solutions)) => {
                let mut results = vec![];
                let mut seen_solutions: HashSet<Vec<(String, Term)>> = HashSet::new();
                let mut rows_returned = 0u64;
                #[cfg(debug_assertions)]
                let debug_prebinding = std::env::var("SHACL_DEBUG_PRE_BINDING").is_ok();
                #[cfg(debug_assertions)]
                let mut solution_count = 0usize;
                for solution_res in solutions {
                    rows_returned = rows_returned.saturating_add(1);
                    let solution = match solution_res {
                        Ok(solution) => solution,
                        Err(e) => {
                            context.record_sparql_query_call(
                                source_shape.clone(),
                                component_id,
                                &self.constraint_node,
                                query_hash,
                                rows_returned,
                                query_started.elapsed(),
                            );
                            return Err(e.to_string());
                        }
                    };
                    if let Some(Term::Literal(failure)) = solution.get("failure") {
                        if failure.datatype() == xsd::BOOLEAN && failure.value() == "true" {
                            context.record_sparql_query_call(
                                source_shape.clone(),
                                component_id,
                                &self.constraint_node,
                                query_hash,
                                rows_returned,
                                query_started.elapsed(),
                            );
                            return Err("SPARQL query reported a failure.".to_string());
                        }
                    }

                    let failed_value_node = if let Some(val) = solution.get("value") {
                        Some(val.clone())
                    } else if c.source_shape().as_node_id().is_some() {
                        Some(c.focus_node().clone())
                    } else {
                        None
                    };
                    let mut solution_key: Vec<(String, Term)> = solution
                        .variables()
                        .iter()
                        .filter_map(|var| {
                            solution
                                .get(var)
                                .map(|term| (var.as_str().to_string(), term.clone()))
                        })
                        .collect();
                    solution_key.sort_by(|a, b| a.0.cmp(&b.0));
                    if !seen_solutions.insert(solution_key) {
                        // Skip duplicate solutions
                        continue;
                    }

                    let mut message_templates = Vec::new();
                    if let Some(term) = solution.get("message") {
                        message_templates.push(term.clone());
                    }
                    if message_templates.is_empty() && !messages.is_empty() {
                        message_templates.extend(messages.clone());
                    }

                    let mut substitutions_for_messages = gather_default_substitutions(
                        c,
                        current_shape_term.as_ref(),
                        failed_value_node.as_ref(),
                        path_substitution_value.as_ref(),
                        &message_prefixes,
                    );
                    for var in solution.variables() {
                        if let Some(term) = solution.get(var) {
                            substitutions_for_messages.push((
                                var.as_str().to_string(),
                                term_ref_to_message_value(term.into(), &message_prefixes),
                            ));
                        }
                    }

                    let (message_opt, message_terms) = sparql_services
                        .instantiate_messages(&message_templates, &substitutions_for_messages);
                    let message = message_opt.unwrap_or_else(|| {
                        "Node does not conform to SPARQL constraint".to_string()
                    });

                    // The path for the validation result is taken from the ?path variable if bound,
                    // otherwise it's taken from the context `c`.
                    let result_path_override =
                        if let Some(Term::NamedNode(path_iri)) = solution.get("path") {
                            Some(Path::Simple(Term::NamedNode(path_iri.clone())))
                        } else {
                            None
                        };

                    let failure = ValidationFailure::new(
                        component_id,
                        failed_value_node.clone(),
                        message,
                        result_path_override,
                        Some(self.constraint_node.clone()),
                    )
                    .with_severity(severity.clone())
                    .with_message_terms(message_terms);

                    results.push(ComponentValidationResult::Fail(c.clone(), failure));
                    #[cfg(debug_assertions)]
                    {
                        solution_count += 1;
                    }
                }
                #[cfg(debug_assertions)]
                if debug_prebinding {
                    let debug_label = format_term_for_label(&self.constraint_node);
                    log::debug!(
                        "SPARQL constraint {} produced {} solutions",
                        debug_label,
                        solution_count
                    );
                }
                context.record_sparql_query_call(
                    source_shape,
                    component_id,
                    &self.constraint_node,
                    query_hash,
                    rows_returned,
                    query_started.elapsed(),
                );
                Ok(results)
            }
            Err(e) => {
                context.record_sparql_query_call(
                    source_shape,
                    component_id,
                    &self.constraint_node,
                    query_hash,
                    0,
                    query_started.elapsed(),
                );
                Err(format!("SPARQL query failed: {}", e))
            }
            _ => {
                context.record_sparql_query_call(
                    source_shape,
                    component_id,
                    &self.constraint_node,
                    query_hash,
                    0,
                    query_started.elapsed(),
                );
                Ok(vec![]) // Other query result types are ignored
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CustomConstraintComponent {
    pub definition: CustomConstraintComponentDefinition,
    pub parameter_values: HashMap<NamedNode, Vec<Term>>,
}

impl CustomConstraintComponent {
    pub(crate) fn local_name(&self) -> String {
        local_name(&self.definition.iri)
    }
}

fn local_name(iri: &NamedNode) -> String {
    let iri_str = iri.as_str();
    if let Some(hash_idx) = iri_str.rfind('#') {
        iri_str[hash_idx + 1..].to_string()
    } else if let Some(slash_idx) = iri_str.rfind('/') {
        if slash_idx < iri_str.len() - 1 {
            iri_str[slash_idx + 1..].to_string()
        } else {
            // trailing slash
            let end = slash_idx;
            let mut start = slash_idx;
            if let Some(prev_slash) = iri_str[..end].rfind('/') {
                start = prev_slash + 1;
            }
            iri_str[start..end].to_string()
        }
    } else {
        iri_str.to_string()
    }
}

impl GraphvizOutput for CustomConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let label = format!(
            "Custom: {}\\n{}",
            local_name(&self.definition.iri),
            self.parameter_values
                .iter()
                .map(|(p, vs)| format!(
                    "{}: {}",
                    local_name(p),
                    vs.iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .collect::<Vec<_>>()
                .join("\\n")
        );
        format!(
            "  {} [label=\"{}\", shape=box];",
            component_id.to_graphviz_id(),
            sanitize_graphviz_string(&label)
        )
    }

    fn component_type(&self) -> NamedNode {
        self.definition.iri.clone()
    }
}

impl ValidateComponent for CustomConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let sparql_services = context.sparql_services();
        let is_prop_shape = c.source_shape().as_prop_id().is_some();

        if context.skip_sparql_blank_targets() && matches!(c.focus_node(), Term::BlankNode(_)) {
            debug!(
                "Skipping custom SPARQL constraint {} for blank focus node {}",
                self.definition.iri,
                format_term_for_label(c.focus_node())
            );
            return Ok(vec![]);
        }

        enum ValidatorScope {
            Node,
            Property,
            General,
        }

        let (validator, _scope) = if is_prop_shape {
            if let Some(v) = self.definition.property_validator.as_ref() {
                (Some(v), ValidatorScope::Property)
            } else {
                (self.definition.validator.as_ref(), ValidatorScope::General)
            }
        } else if let Some(v) = self.definition.node_validator.as_ref() {
            (Some(v), ValidatorScope::Node)
        } else {
            (self.definition.validator.as_ref(), ValidatorScope::General)
        };

        let validator = match validator {
            Some(v) => v,
            None => return Ok(vec![]),
        };

        let require_path = validator.require_path;
        let require_this = validator.require_this;

        validate_prebound_variable_usage(
            &validator.query,
            &format!("Custom constraint {}", self.definition.iri),
            require_this,
            require_path,
        )?;

        let mut query_body = validator.query.clone();
        let mut path_substitution_value: Option<String> = None;

        if is_prop_shape {
            if let Some(prop_id) = c.source_shape().as_prop_id() {
                if let Some(prop_shape) = context.model.get_prop_shape_by_id(prop_id) {
                    let path_str = prop_shape.sparql_path();
                    path_substitution_value = Some(path_str.clone());
                    query_body = query_body.replace("$PATH", &path_str);
                }
            }
        }

        let current_shape_term = c.source_shape().get_term(context);

        let mut substitutions: Vec<(Variable, Term)> = Vec::new();
        let mut prebound_vars: HashSet<Variable> = HashSet::new();
        let mut optional_prebound_vars: HashSet<Variable> = HashSet::new();

        if query_mentions_var(&query_body, "this") {
            let var = Variable::new_unchecked("this");
            substitutions.push((var.clone(), c.focus_node().clone()));
            if !require_this {
                optional_prebound_vars.insert(var.clone());
            }
            prebound_vars.insert(var);
        }

        if let Some(term) = current_shape_term.clone() {
            if query_mentions_var(&query_body, "currentShape") {
                let var = Variable::new_unchecked("currentShape");
                substitutions.push((var.clone(), term));
                optional_prebound_vars.insert(var.clone());
                prebound_vars.insert(var);
            }
        }

        if query_mentions_var(&query_body, "shapesGraph") {
            let var = Variable::new_unchecked("shapesGraph");
            substitutions.push((var.clone(), context.model.shape_graph_iri.clone().into()));
            optional_prebound_vars.insert(var.clone());
            prebound_vars.insert(var);
        }

        for (param_path, values) in &self.parameter_values {
            let param_meta = self
                .definition
                .parameters
                .iter()
                .find(|p| p.path == *param_path);
            let var_name = match param_meta.and_then(|p| p.var_name.clone()) {
                Some(name) => name,
                None => local_name(param_path),
            };

            if !query_mentions_var(&query_body, &var_name) {
                // Skip optional parameters that are unused in the query.
                if let Some(param) = param_meta {
                    if !param.optional {
                        return Err(format!(
                            "Custom constraint {} expects query variable ?{} for parameter {}, but it was not referenced.",
                            self.definition.iri,
                            var_name,
                            param.path
                        ));
                    }
                }
                continue;
            }

            let value = values.first().ok_or_else(|| {
                format!(
                    "Custom constraint {} is missing a value for parameter {} needed by its SPARQL query.",
                    self.definition.iri,
                    var_name
                )
            })?;
            let var = Variable::new_unchecked(&var_name);
            substitutions.push((var.clone(), value.clone()));
            prebound_vars.insert(var.clone());
            if param_meta.map(|p| p.optional).unwrap_or(false) {
                optional_prebound_vars.insert(var);
            }
        }

        for param in &self.definition.parameters {
            let var_name = param
                .var_name
                .clone()
                .unwrap_or_else(|| local_name(&param.path));
            if !query_mentions_var(&query_body, &var_name) {
                continue;
            }
            if self.parameter_values.contains_key(&param.path) {
                continue;
            }
            let var = Variable::new_unchecked(&var_name);
            if param.optional {
                optional_prebound_vars.insert(var);
                continue;
            }
            return Err(format!(
                "Custom constraint {} is missing required parameter {} for query variable ?{}.",
                self.definition.iri, param.path, var_name
            ));
        }

        let include_value = validator.is_ask && query_mentions_var(&query_body, "value");
        if include_value {
            prebound_vars.insert(Variable::new_unchecked("value"));
        }

        let query_with_prefixes = if validator.prefixes.is_empty() {
            query_body.clone()
        } else {
            format!(
                "{}
{}",
                validator.prefixes, query_body
            )
        };
        let message_prefixes = parse_prefix_lines(&validator.prefixes);

        let context_label = if validator.is_ask {
            format!("SPARQL ASK validator {}", self.definition.iri)
        } else {
            format!("SPARQL SELECT validator {}", self.definition.iri)
        };

        let algebra_query = sparql_services
            .algebra(&query_with_prefixes)
            .map_err(|e| format!("Failed to parse SPARQL validator query: {}", e))?;

        ensure_pre_binding_semantics(
            &algebra_query,
            &context_label,
            &prebound_vars,
            &optional_prebound_vars,
        )?;

        let prepared_query = context
            .prepare_query(&query_with_prefixes)
            .map_err(|e| format!("Failed to prepare SPARQL validator query: {}", e))?;

        let mut results = Vec::new();

        if validator.is_ask {
            if let Some(value_nodes) = c.value_nodes() {
                for value_node in value_nodes {
                    let mut ask_substitutions = substitutions.clone();
                    if include_value {
                        ask_substitutions
                            .push((Variable::new_unchecked("value"), value_node.clone()));
                    }

                    match context.execute_prepared(
                        &query_with_prefixes,
                        &prepared_query,
                        &ask_substitutions,
                        true,
                    ) {
                        Ok(QueryResults::Boolean(conforms)) => {
                            if !conforms {
                                let message_templates = if !validator.messages.is_empty() {
                                    validator.messages.clone()
                                } else {
                                    self.definition.messages.clone()
                                };
                                let mut substitutions_for_messages = gather_default_substitutions(
                                    c,
                                    current_shape_term.as_ref(),
                                    Some(value_node),
                                    path_substitution_value.as_ref(),
                                    &message_prefixes,
                                );
                                for (param_path, values) in &self.parameter_values {
                                    if let Some(val) = values.first() {
                                        substitutions_for_messages.push((
                                            local_name(param_path),
                                            term_to_message_value(val, &message_prefixes),
                                        ));
                                    }
                                }
                                let (message_opt, message_terms) = sparql_services
                                    .instantiate_messages(
                                        &message_templates,
                                        &substitutions_for_messages,
                                    );
                                let message = message_opt.unwrap_or_else(|| {
                                    format!(
                                        "Value does not conform to custom constraint {}",
                                        self.definition.iri
                                    )
                                });
                                let severity_override = validator
                                    .severity
                                    .clone()
                                    .or_else(|| self.definition.severity.clone());
                                let failure = ValidationFailure::new(
                                    component_id,
                                    Some(value_node.clone()),
                                    message,
                                    None,
                                    None,
                                )
                                .with_severity(severity_override)
                                .with_message_terms(message_terms);

                                results.push(ComponentValidationResult::Fail(c.clone(), failure));
                            }
                        }
                        Ok(_) => {}
                        Err(e) => return Err(format!("SPARQL query failed: {}", e)),
                    }
                }
            }
        } else {
            match context.execute_prepared(
                &query_with_prefixes,
                &prepared_query,
                &substitutions,
                true,
            ) {
                Ok(QueryResults::Solutions(solutions)) => {
                    let mut seen_solutions = HashSet::new();
                    for solution_res in solutions {
                        let solution = solution_res.map_err(|e| e.to_string())?;

                        if let Some(Term::Literal(failure)) = solution.get("failure") {
                            if failure.datatype() == xsd::BOOLEAN && failure.value() == "true" {
                                return Err("SPARQL validator reported a failure.".to_string());
                            }
                        }

                        let failed_value_node = if let Some(val) = solution.get("value") {
                            Some(val.clone())
                        } else if c.source_shape().as_node_id().is_some() {
                            Some(c.focus_node().clone())
                        } else {
                            None
                        };

                        if !seen_solutions.insert(failed_value_node.clone()) {
                            continue;
                        }

                        let mut message_templates = Vec::new();
                        if let Some(term) = solution.get("message") {
                            message_templates.push(term.clone());
                        }
                        if message_templates.is_empty() && !validator.messages.is_empty() {
                            message_templates.extend(validator.messages.clone());
                        }
                        if message_templates.is_empty() && !self.definition.messages.is_empty() {
                            message_templates.extend(self.definition.messages.clone());
                        }

                        let mut substitutions_for_messages = gather_default_substitutions(
                            c,
                            current_shape_term.as_ref(),
                            failed_value_node.as_ref(),
                            path_substitution_value.as_ref(),
                            &message_prefixes,
                        );
                        for (param_path, values) in &self.parameter_values {
                            if let Some(val) = values.first() {
                                substitutions_for_messages.push((
                                    local_name(param_path),
                                    term_ref_to_message_value(val.as_ref(), &message_prefixes),
                                ));
                            }
                        }
                        for var in solution.variables() {
                            if let Some(term) = solution.get(var) {
                                substitutions_for_messages.push((
                                    var.as_str().to_string(),
                                    term_ref_to_message_value(term.into(), &message_prefixes),
                                ));
                            }
                        }

                        let (message_opt, message_terms) = sparql_services
                            .instantiate_messages(&message_templates, &substitutions_for_messages);
                        let message = message_opt.unwrap_or_else(|| {
                            format!(
                                "Node does not conform to custom constraint {}",
                                self.definition.iri
                            )
                        });

                        let severity_override = validator
                            .severity
                            .clone()
                            .or_else(|| self.definition.severity.clone());
                        let failure = ValidationFailure::new(
                            component_id,
                            failed_value_node.clone(),
                            message,
                            None,
                            None,
                        )
                        .with_severity(severity_override)
                        .with_message_terms(message_terms);

                        results.push(ComponentValidationResult::Fail(c.clone(), failure));
                    }
                }
                Ok(_) => {}
                Err(e) => return Err(format!("SPARQL query failed: {}", e)),
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::{plan_sparql_execution, SparqlExecutionMode};

    #[test]
    fn sparql_execution_plan_stays_per_focus_for_unselective_medium_batches() {
        assert_eq!(
            plan_sparql_execution(32, 32, 1),
            SparqlExecutionMode::PerFocus
        );
        assert_eq!(
            plan_sparql_execution(48, 48, 1),
            SparqlExecutionMode::PerFocus
        );
    }

    #[test]
    fn sparql_execution_plan_batches_large_or_selective_candidate_sets() {
        assert_eq!(
            plan_sparql_execution(4274, 1057, 1),
            SparqlExecutionMode::BatchLargeChunks
        );
        assert_eq!(
            plan_sparql_execution(300, 64, 1),
            SparqlExecutionMode::BatchSmallChunks
        );
    }

    #[test]
    fn sparql_execution_plan_probes_broad_medium_batches() {
        assert_eq!(
            plan_sparql_execution(238, 238, 1),
            SparqlExecutionMode::ProbeMicroBatch
        );
        assert_eq!(
            plan_sparql_execution(431, 238, 1),
            SparqlExecutionMode::ProbeMicroBatch
        );
    }
}
