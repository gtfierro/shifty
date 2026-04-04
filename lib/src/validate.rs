//! Validation pipeline orchestration.
//!
//! This module drives the runtime evaluation of shapes against a data graph. It builds a
//! validation plan from the ShapeIR, executes node/property shapes in parallel, and collects
//! failures into a `ValidationReportBuilder`.

use crate::context::model::OriginalValueIndex;
use crate::context::{Context, ShapeTimingPhase, SourceShape, ValidationContext};
use crate::model::components::ComponentDescriptor;
use crate::planning::{ShapeRef, build_validation_plan};
use crate::report::ValidationReportBuilder;
use crate::runtime::{Component, ComponentValidationResult, ToSubjectRef, ValidationFailure};
use crate::shape::{NodeShape, PropertyShape, ValidateShape};
use crate::target_hash::hash_target;
use crate::trace::TraceEvent;
use crate::types::{Path, PropShapeID, TargetEvalExt, TraceItem};
use log::{debug, info};
use oxigraph::model::vocab::xsd;
use oxigraph::model::{Literal, NamedNode, Term};

fn format_term_for_sparql(term: &Term) -> String {
    match term {
        Term::NamedNode(nn) => format!("<{}>", nn.as_str()),
        Term::BlankNode(bn) => format!("_:{}", bn.as_str()),
        Term::Literal(lit) => {
            let value = lit.value().replace('\\', "\\\\").replace('"', "\\\"");
            if let Some(lang) = lit.language() {
                format!("\"{}\"@{}", value, lang)
            } else if lit.datatype() != xsd::STRING {
                format!("\"{}\"^^<{}>", value, lit.datatype().as_str())
            } else {
                format!("\"{}\"", value)
            }
        }
    }
}

pub(crate) fn is_path_summary_able(path: &Path) -> bool {
    match path {
        Path::Simple(Term::NamedNode(_)) => true,
        Path::Inverse(inner) => matches!(inner.as_ref(), Path::Simple(Term::NamedNode(_))),
        _ => false,
    }
}
use oxigraph::sparql::{QueryResults, Variable};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

pub(crate) fn validate(context: &ValidationContext) -> Result<ValidationReportBuilder, String> {
    let debug_parallel = std::env::var("SHACL_DEBUG_PARALLEL").is_ok();
    if debug_parallel {
        debug!("rayon threads available: {}", rayon::current_num_threads());
    }

    let mut report_builder = ValidationReportBuilder::with_capacity(128);

    let plan = build_validation_plan(context.shape_ir());
    let tree_reports: Result<Vec<ValidationReportBuilder>, String> = plan
        .trees
        .par_iter()
        .map(|tree| {
            let mut local_report = ValidationReportBuilder::with_capacity(8);
            for shape_ref in &tree.shapes {
                run_plan_shape(*shape_ref, context, &mut local_report)?;
            }
            Ok(local_report)
        })
        .collect();

    for builder in tree_reports? {
        report_builder.merge(builder);
    }

    Ok(report_builder)
}

fn run_plan_shape(
    shape_ref: ShapeRef,
    context: &ValidationContext,
    report_builder: &mut ValidationReportBuilder,
) -> Result<(), String> {
    let source = match shape_ref {
        ShapeRef::Node(id) => SourceShape::NodeShape(id),
        ShapeRef::Property(id) => SourceShape::PropertyShape(PropShapeID(id.0)),
    };
    context.trace_sink.record(TraceEvent::EnterShapeExecution(
        source.clone(),
        Instant::now(),
    ));

    let res = match shape_ref {
        ShapeRef::Node(id) => {
            if let Some(shape) = context.model.get_node_shape_by_id(&id) {
                shape.process_targets(context, report_builder)
            } else {
                Err(format!("Planned node shape {:?} not found in model", id))
            }
        }
        ShapeRef::Property(id) => {
            if let Some(shape) = context.model.get_prop_shape_by_id(&id) {
                shape.process_targets(context, report_builder)
            } else {
                Err(format!(
                    "Planned property shape {:?} not found in model",
                    id
                ))
            }
        }
    };

    context
        .trace_sink
        .record(TraceEvent::ExitShapeExecution(source, Instant::now()));

    res
}

fn canonicalize_value_nodes(
    validation_context: &ValidationContext,
    shape: &PropertyShape,
    focus_node: &Term,
    mut nodes: Vec<Term>,
) -> Vec<Term> {
    if nodes.is_empty() {
        return nodes;
    }

    let predicate = match shape.path_term() {
        Term::NamedNode(nn) => nn,
        _ => return nodes,
    };

    let subject = match focus_node.try_to_subject_ref() {
        Ok(subject) => subject,
        Err(_) => return nodes,
    };

    let raw_objects: Vec<Term> = validation_context
        .validation_objects_for_predicate(subject, predicate.as_ref())
        .unwrap_or_default();

    if raw_objects.is_empty() && validation_context.model.original_values.is_none() {
        return nodes;
    }

    let mut exact_matches: HashSet<Term> = HashSet::with_capacity(raw_objects.len());
    let mut literals_by_signature: HashMap<(String, Option<String>), VecDeque<Term>> =
        HashMap::new();

    for term in raw_objects {
        if let Term::Literal(lit) = &term {
            let key = literal_signature(lit);
            literals_by_signature
                .entry(key)
                .or_default()
                .push_back(term.clone());
        }
        exact_matches.insert(term);
    }

    let original_index = validation_context.model.original_values.as_ref();

    for node in &mut nodes {
        let current = node.clone();

        if let Some(original) =
            resolve_original_literal(original_index, focus_node, predicate, &current)
        {
            exact_matches.remove(&original);
            *node = original;
            continue;
        }

        if exact_matches.remove(&current) {
            continue;
        }

        if let Term::Literal(ref lit) = current
            && let Some(term) =
                lookup_by_signature(&mut literals_by_signature, &mut exact_matches, lit)
        {
            *node = term;
        }
    }

    nodes
}

fn literal_signature(lit: &Literal) -> (String, Option<String>) {
    (
        lit.value().to_string(),
        lit.language().map(|lang| lang.to_ascii_lowercase()),
    )
}

fn lookup_by_signature(
    buckets: &mut HashMap<(String, Option<String>), VecDeque<Term>>,
    exact_matches: &mut HashSet<Term>,
    lit: &Literal,
) -> Option<Term> {
    let key = literal_signature(lit);
    let queue = buckets.get_mut(&key)?;
    let term = queue.pop_front()?;
    exact_matches.remove(&term);
    Some(term)
}

fn resolve_original_literal(
    original_index: Option<&OriginalValueIndex>,
    focus_node: &Term,
    predicate: &NamedNode,
    current: &Term,
) -> Option<Term> {
    let Term::Literal(lit) = current else {
        return None;
    };
    let index = original_index?;
    let original = index.resolve_literal(focus_node, predicate, lit)?;

    if original == *current {
        return None;
    }

    Some(original)
}

fn path_is_definitely_absent_for_focus(
    context: &ValidationContext,
    shape: &PropertyShape,
    focus_node: &Term,
) -> Result<bool, String> {
    match shape.path() {
        Path::Simple(Term::NamedNode(predicate)) => Ok(context
            .focus_outgoing_predicate_count(focus_node, &Term::NamedNode(predicate.clone()))?
            == 0),
        Path::Inverse(inner) => match inner.as_ref() {
            Path::Simple(Term::NamedNode(predicate)) => Ok(!context
                .focus_has_incoming_predicate(focus_node, &Term::NamedNode(predicate.clone()))?),
            _ => Ok(false),
        },
        _ => Ok(false),
    }
}

fn summary_value_nodes_for_focus(
    context: &ValidationContext,
    shape: &PropertyShape,
    focus_node: &Term,
) -> Result<Option<Vec<Term>>, String> {
    match shape.path() {
        Path::Simple(Term::NamedNode(predicate)) => context
            .focus_objects_for_predicate(focus_node, &Term::NamedNode(predicate.clone()))
            .map(Some),
        Path::Inverse(inner) => match inner.as_ref() {
            Path::Simple(Term::NamedNode(predicate)) => context
                .focus_subjects_for_inverse_predicate(
                    focus_node,
                    &Term::NamedNode(predicate.clone()),
                )
                .map(Some),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

fn summary_value_count_for_focus(
    context: &ValidationContext,
    shape: &PropertyShape,
    focus_node: &Term,
) -> Result<Option<usize>, String> {
    match shape.path() {
        Path::Simple(Term::NamedNode(predicate)) => context
            .focus_outgoing_predicate_count(focus_node, &Term::NamedNode(predicate.clone()))
            .map(Some),
        Path::Inverse(inner) => match inner.as_ref() {
            Path::Simple(Term::NamedNode(predicate)) => context
                .focus_incoming_predicate_count(focus_node, &Term::NamedNode(predicate.clone()))
                .map(Some),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

fn constraints_are_cardinality_only(
    context: &ValidationContext,
    constraints: &[crate::types::ComponentID],
) -> bool {
    constraints.iter().all(|id| {
        matches!(
            context
                .shape_ir()
                .components
                .get(id)
                .or_else(|| context.model.get_component_descriptor(id)),
            Some(ComponentDescriptor::MinCount { .. } | ComponentDescriptor::MaxCount { .. })
        )
    })
}

fn constraint_can_use_count_without_values(
    context: &ValidationContext,
    constraint_id: crate::types::ComponentID,
) -> bool {
    matches!(
        context
            .shape_ir()
            .components
            .get(&constraint_id)
            .or_else(|| context.model.get_component_descriptor(&constraint_id)),
        Some(ComponentDescriptor::MinCount { .. } | ComponentDescriptor::MaxCount { .. })
    )
}

fn constraint_is_vacuous_when_value_count_is_zero(
    context: &ValidationContext,
    constraint_id: crate::types::ComponentID,
) -> bool {
    match context
        .shape_ir()
        .components
        .get(&constraint_id)
        .or_else(|| context.model.get_component_descriptor(&constraint_id))
    {
        Some(
            ComponentDescriptor::Class { .. }
            | ComponentDescriptor::Datatype { .. }
            | ComponentDescriptor::NodeKind { .. }
            | ComponentDescriptor::In { .. }
            | ComponentDescriptor::LanguageIn { .. }
            | ComponentDescriptor::UniqueLang { .. }
            | ComponentDescriptor::MinLength { .. }
            | ComponentDescriptor::MaxLength { .. }
            | ComponentDescriptor::Pattern { .. }
            | ComponentDescriptor::Node { .. }
            | ComponentDescriptor::Not { .. }
            | ComponentDescriptor::And { .. }
            | ComponentDescriptor::Or { .. }
            | ComponentDescriptor::Xone { .. },
        ) => true,
        Some(ComponentDescriptor::QualifiedValueShape { min_count, .. }) => {
            min_count.is_none_or(|min| min == 0)
        }
        _ => false,
    }
}

fn batched_property_sparql_failures_by_constraint(
    shape: &PropertyShape,
    focus_nodes: &[Term],
    constraints: &[crate::types::ComponentID],
    context: &ValidationContext,
) -> Result<HashMap<crate::types::ComponentID, crate::context::BatchedSparqlResult>, String> {
    if focus_nodes.len() < 2 {
        return Ok(HashMap::new());
    }

    let mut batched = HashMap::new();
    let path_substitution_value = shape.sparql_path();
    let source_shape = SourceShape::PropertyShape(*shape.identifier());
    for &constraint_id in constraints {
        let Some(Component::SPARQLConstraint(component)) = context.get_component(&constraint_id)
        else {
            continue;
        };
        if let Some(result) = component.validate_batch_for_focuses(
            constraint_id,
            source_shape.clone(),
            focus_nodes,
            Some(&path_substitution_value),
            context,
        )? {
            batched.insert(constraint_id, result);
        }
    }
    Ok(batched)
}

impl ValidateShape for NodeShape {
    fn process_targets(
        &self,
        context: &ValidationContext,
        report_builder: &mut ValidationReportBuilder,
    ) -> Result<(), String> {
        if self.is_deactivated() {
            return Ok(());
        }
        // first gather all of the targets (cached per shape)
        let target_selection_started = Instant::now();
        context.trace_sink.record(TraceEvent::TargetCollectionStart(
            SourceShape::NodeShape(*self.identifier()),
            target_selection_started,
        ));

        let focus_nodes = if let Some(cached) = context.cached_node_targets(self.identifier()) {
            context.trace_sink.record(TraceEvent::TargetCacheHit(
                SourceShape::NodeShape(*self.identifier()),
                cached.len(),
            ));
            cached
        } else {
            let mut all_targets = HashSet::new();

            for target in self.targets.iter() {
                let target_hash = hash_target(target);

                // Check global cache for this specific target expression
                let targets = if let Some(cached) = context.global_target_cache.read().unwrap().get(&target_hash) {
                    Arc::clone(cached)
                } else {
                    // Cache miss - evaluate target and store in global cache
                    info!(
                        "get targets from target: {:?} on shape {}",
                        target,
                        self.identifier()
                    );
                    let nodes: Vec<Term> = target
                        .get_target_nodes(context, SourceShape::NodeShape(*self.identifier()))?
                        .into_iter()
                        .map(|ctx| ctx.focus_node().clone())
                        .collect();

                    let arc_nodes: Arc<[Term]> = nodes.into();
                    context.global_target_cache.write().unwrap().insert(target_hash, Arc::clone(&arc_nodes));
                    arc_nodes
                };

                all_targets.extend(targets.iter().cloned());
            }

            let vec: Vec<Term> = all_targets.into_iter().collect();
            let cached: Arc<[Term]> = vec.into();
            context.store_node_targets(*self.identifier(), Arc::clone(&cached));
            cached
        };

        context.trace_sink.record(TraceEvent::TargetCollectionEnd(
            SourceShape::NodeShape(*self.identifier()),
            focus_nodes.len(),
            Instant::now(),
        ));

        context.record_shape_phase_duration(
            SourceShape::NodeShape(*self.identifier()),
            ShapeTimingPhase::NodeTarget,
            target_selection_started.elapsed(),
        );

        if focus_nodes.is_empty() {
            return Ok(());
        }

        info!(
            "Node shape {} has {} focus nodes",
            self.identifier(),
            focus_nodes.len()
        );

        let constraints = context.order_constraints(self.constraints());

        let focus_reports: Result<Vec<ValidationReportBuilder>, String> = focus_nodes
            .as_ref()
            .par_iter()
            .map(|focus_node| {
                let mut local_report = ValidationReportBuilder::with_capacity(8);
                let node_value_selection_started = Instant::now();
                let mut target_context = Context::new(
                    focus_node.clone(),
                    None,
                    Some(vec![focus_node.clone()]),
                    SourceShape::NodeShape(*self.identifier()),
                    context.new_trace(),
                );
                context.record_shape_phase_duration(
                    SourceShape::NodeShape(*self.identifier()),
                    ShapeTimingPhase::NodeValue,
                    node_value_selection_started.elapsed(),
                );
                let trace_index = target_context.trace_index();
                let now = Instant::now();
                let mut local_events = Vec::new();
                local_events.push(TraceEvent::EnterNodeShape(*self.identifier(), now));

                let shape_label = context
                    .model
                    .nodeshape_id_lookup()
                    .read()
                    .unwrap()
                    .get_term(*self.identifier())
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| format!("nodeshape:{}", self.identifier().0));

                let describe_component = |id: &crate::types::ComponentID| -> String {
                    let descriptor = context
                        .shape_ir()
                        .components
                        .get(id)
                        .or_else(|| context.model.get_component_descriptor(id));
                    match descriptor {
                        Some(ComponentDescriptor::Class { .. }) => "class".into(),
                        Some(ComponentDescriptor::Datatype { .. }) => "datatype".into(),
                        Some(ComponentDescriptor::NodeKind { .. }) => "nodeKind".into(),
                        Some(ComponentDescriptor::MinCount { .. }) => "minCount".into(),
                        Some(ComponentDescriptor::MaxCount { .. }) => "maxCount".into(),
                        Some(ComponentDescriptor::MinExclusive { .. }) => "minExclusive".into(),
                        Some(ComponentDescriptor::MinInclusive { .. }) => "minInclusive".into(),
                        Some(ComponentDescriptor::MaxExclusive { .. }) => "maxExclusive".into(),
                        Some(ComponentDescriptor::MaxInclusive { .. }) => "maxInclusive".into(),
                        Some(ComponentDescriptor::MinLength { .. }) => "minLength".into(),
                        Some(ComponentDescriptor::MaxLength { .. }) => "maxLength".into(),
                        Some(ComponentDescriptor::Pattern { .. }) => "pattern".into(),
                        Some(ComponentDescriptor::LanguageIn { .. }) => "languageIn".into(),
                        Some(ComponentDescriptor::UniqueLang { .. }) => "uniqueLang".into(),
                        Some(ComponentDescriptor::Equals { .. }) => "equals".into(),
                        Some(ComponentDescriptor::Disjoint { .. }) => "disjoint".into(),
                        Some(ComponentDescriptor::LessThan { .. }) => "lessThan".into(),
                        Some(ComponentDescriptor::LessThanOrEquals { .. }) => {
                            "lessThanOrEquals".into()
                        }
                        Some(ComponentDescriptor::Not { .. }) => "not".into(),
                        Some(ComponentDescriptor::And { .. }) => "and".into(),
                        Some(ComponentDescriptor::Or { .. }) => "or".into(),
                        Some(ComponentDescriptor::Xone { .. }) => "xone".into(),
                        Some(ComponentDescriptor::Closed { .. }) => "closed".into(),
                        Some(ComponentDescriptor::HasValue { .. }) => "hasValue".into(),
                        Some(ComponentDescriptor::In { .. }) => "in".into(),
                        Some(ComponentDescriptor::Sparql { .. }) => "sparql".into(),
                        Some(ComponentDescriptor::Custom { definition, .. }) => {
                            format!("custom({})", definition.iri.as_str())
                        }
                        Some(ComponentDescriptor::Node { .. }) => "node".into(),
                        Some(ComponentDescriptor::Property { .. }) => "property".into(),
                        Some(ComponentDescriptor::QualifiedValueShape { .. }) => {
                            "qualifiedValueShape".into()
                        }
                        None => format!("component_id:{}", id.0),
                    }
                };

                let mut local_trace: Vec<TraceItem> = Vec::new();
                local_trace.push(TraceItem::NodeShape(*self.identifier()));

                debug!(
                    "Node shape {} has {} constraints",
                    shape_label,
                    constraints.len()
                );

                for constraint_id in &constraints {
                    debug!(
                        "Evaluating constraint {} ({}) for node shape {}",
                        constraint_id,
                        describe_component(constraint_id),
                        shape_label
                    );

                    let comp = context
                        .get_component(constraint_id)
                        .ok_or_else(|| format!("Component not found: {}", constraint_id))?;

                    let component_start = Instant::now();
                    local_events.push(TraceEvent::ComponentExecutionStart(
                        *constraint_id,
                        SourceShape::NodeShape(*self.identifier()),
                        component_start,
                    ));

                    match comp.validate(
                        *constraint_id,
                        &mut target_context,
                        context,
                        &mut local_trace,
                        &mut local_events,
                        None,
                    ) {
                        Ok(validation_results) => {
                            for result in validation_results {
                                match result {
                                    ComponentValidationResult::Fail(ctx, failure) => {
                                        local_events.push(TraceEvent::ComponentFailed {
                                            component: *constraint_id,
                                            focus: ctx.focus_node().clone(),
                                            value: failure
                                                .failed_value_node
                                                .clone()
                                                .or_else(|| ctx.value().cloned()),
                                            message: Some(failure.message.clone()),
                                            ts: Instant::now(),
                                        });
                                        local_report.add_failure(&ctx, failure);
                                    }
                                    ComponentValidationResult::Pass(ctx) => {
                                        local_events.push(TraceEvent::ComponentPassed {
                                            component: *constraint_id,
                                            focus: ctx.focus_node().clone(),
                                            value: ctx.value().cloned(),
                                            ts: Instant::now(),
                                        });
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }

                    local_events.push(TraceEvent::ComponentExecutionEnd(
                        *constraint_id,
                        SourceShape::NodeShape(*self.identifier()),
                        Instant::now(),
                    ));
                }

                // append the trace once per focus node to avoid long-held locks
                {
                    let mut traces = context.execution_traces.lock().unwrap();
                    if let Some(slot) = traces.get_mut(trace_index) {
                        *slot = local_trace;
                    } else {
                        traces.push(local_trace);
                    }
                }

                local_events.push(TraceEvent::ExitNodeShape(
                    *self.identifier(),
                    Instant::now(),
                ));
                context.trace_sink.record_batch(local_events);

                Ok(local_report)
            })
            .collect();

        for builder in focus_reports? {
            report_builder.merge(builder);
        }
        Ok(())
    }
}

impl ValidateShape for PropertyShape {
    fn process_targets(
        &self,
        context: &ValidationContext,
        report_builder: &mut ValidationReportBuilder,
    ) -> Result<(), String> {
        if self.is_deactivated() {
            return Ok(());
        }
        // first gather all of the targets (cached per shape)
        let target_selection_started = Instant::now();
        context.trace_sink.record(TraceEvent::TargetCollectionStart(
            SourceShape::PropertyShape(*self.identifier()),
            target_selection_started,
        ));

        let focus_nodes = if let Some(cached) = context.cached_prop_targets(self.identifier()) {
            context.trace_sink.record(TraceEvent::TargetCacheHit(
                SourceShape::PropertyShape(*self.identifier()),
                cached.len(),
            ));
            cached
        } else {
            let mut all_targets = HashSet::new();

            for target in self.targets.iter() {
                let target_hash = hash_target(target);

                // Check global cache for this specific target expression
                let targets = if let Some(cached) = context.global_target_cache.read().unwrap().get(&target_hash) {
                    Arc::clone(cached)
                } else {
                    // Cache miss - evaluate target and store in global cache
                    info!(
                        "get targets from target: {:?} on shape {}",
                        target,
                        self.identifier()
                    );
                    let nodes: Vec<Term> = target
                        .get_target_nodes(context, SourceShape::PropertyShape(*self.identifier()))?
                        .into_iter()
                        .map(|ctx| ctx.focus_node().clone())
                        .collect();

                    let arc_nodes: Arc<[Term]> = nodes.into();
                    context.global_target_cache.write().unwrap().insert(target_hash, Arc::clone(&arc_nodes));
                    arc_nodes
                };

                all_targets.extend(targets.iter().cloned());
            }

            let vec: Vec<Term> = all_targets.into_iter().collect();
            let cached: Arc<[Term]> = vec.into();
            context.store_prop_targets(*self.identifier(), Arc::clone(&cached));
            cached
        };

        context.trace_sink.record(TraceEvent::TargetCollectionEnd(
            SourceShape::PropertyShape(*self.identifier()),
            focus_nodes.len(),
            Instant::now(),
        ));

        context.record_shape_phase_duration(
            SourceShape::PropertyShape(*self.identifier()),
            ShapeTimingPhase::PropertyTarget,
            target_selection_started.elapsed(),
        );

        if focus_nodes.is_empty() {
            return Ok(());
        }

        let mut prefetched_values: HashMap<Term, Vec<Term>> = HashMap::new();
        if !is_path_summary_able(self.path())
            && focus_nodes.len() > 1
            && let Ok(path_str) = self.path().to_sparql_path()
        {
            prefetched_values = self.pre_fetch_value_nodes(&focus_nodes, &path_str, context)?;
        }

        let focus_reports: Result<Vec<ValidationReportBuilder>, String> = focus_nodes
            .as_ref()
            .par_iter()
            .map(|focus_node| {
                let mut local_report = ValidationReportBuilder::with_capacity(8);
                let values_for_this_focus = prefetched_values.get(focus_node);

                let mut target_context = Context::new(
                    focus_node.clone(),
                    None,
                    None,
                    SourceShape::PropertyShape(*self.identifier()),
                    context.new_trace(),
                );
                let trace_index = target_context.trace_index();
                let now = Instant::now();
                let mut local_events = Vec::new();
                local_events.push(TraceEvent::EnterPropertyShape(*self.identifier(), now));

                let mut local_trace: Vec<TraceItem> = Vec::new();

                match self.validate(
                    &mut target_context,
                    context,
                    &mut local_trace,
                    &mut local_events,
                    values_for_this_focus.cloned(),
                ) {
                    Ok(validation_results) => {
                        for result in validation_results {
                            if let ComponentValidationResult::Fail(ctx, failure) = result {
                                local_report.add_failure(&ctx, failure);
                            }
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }

                {
                    let mut traces = context.execution_traces.lock().unwrap();
                    if let Some(slot) = traces.get_mut(trace_index) {
                        *slot = local_trace;
                    } else {
                        traces.push(local_trace);
                    }
                }

                local_events.push(TraceEvent::ExitPropertyShape(
                    *self.identifier(),
                    Instant::now(),
                ));
                context.trace_sink.record_batch(local_events);

                Ok(local_report)
            })
            .collect();

        for builder in focus_reports? {
            report_builder.merge(builder);
        }
        Ok(())
    }
}

impl PropertyShape {
    pub(crate) fn pre_fetch_value_nodes(
        &self,
        focus_nodes: &[Term],
        sparql_path: &str,
        context: &ValidationContext,
    ) -> Result<HashMap<Term, Vec<Term>>, String> {
        if focus_nodes.is_empty() {
            return Ok(HashMap::new());
        }

        // Check if we already have all results in the path batch cache
        let cache_key = sparql_path.to_string();
        let mut missing_focus_nodes = Vec::new();
        let mut all_results = HashMap::new();

        if let Ok(cache) = context.path_batch_cache.read() {
            if let Some(cached_batch) = cache.get(&cache_key) {
                for focus_node in focus_nodes {
                    if let Some(cached_values) = cached_batch.get(focus_node) {
                        all_results.insert(focus_node.clone(), cached_values.clone());
                    } else {
                        missing_focus_nodes.push(focus_node.clone());
                    }
                }
            } else {
                missing_focus_nodes.extend(focus_nodes.iter().cloned());
            }
        } else {
            missing_focus_nodes.extend(focus_nodes.iter().cloned());
        }

        // If we have all results from cache, return early
        if missing_focus_nodes.is_empty() {
            return Ok(all_results);
        }

        // Execute query for missing focus nodes
        let focus_var = Variable::new_unchecked("focus");
        let value_node_var = Variable::new_unchecked("valueNode");
        let mut new_results = HashMap::new();

        for chunk in missing_focus_nodes.chunks(500) {
            let mut values_clause = String::from("VALUES ?focus { ");
            for node in chunk {
                values_clause.push_str(&format!("{} ", format_term_for_sparql(node)));
            }
            values_clause.push_str(" }");

            let query_str = format!(
                "SELECT ?focus ?valueNode WHERE {{ {} ?focus {} ?valueNode . }}",
                values_clause, sparql_path
            );

            let prepared = context.prepare_query(&query_str)?;
            let results = context.execute_prepared(&query_str, &prepared, &[], false)?;

            if let QueryResults::Solutions(solutions) = results {
                for solution_res in solutions {
                    let solution = solution_res.map_err(|e| e.to_string())?;
                    if let Some(focus) = solution.get(&focus_var).cloned()
                        && let Some(value) = solution.get(&value_node_var).cloned()
                    {
                        new_results
                            .entry(focus)
                            .or_insert_with(Vec::new)
                            .push(value);
                    }
                }
            }
        }

        // Populate cache with new results
        if !new_results.is_empty() {
            if let Ok(mut cache) = context.path_batch_cache.write() {
                let batch = cache.entry(cache_key).or_insert_with(|| Arc::new(HashMap::new()));
                let mut updated_batch = (**batch).clone();
                for (focus, values) in &new_results {
                    updated_batch.insert(focus.clone(), values.clone());
                }
                *batch = Arc::new(updated_batch);
            }
        }

        // Merge new results with cached results
        all_results.extend(new_results);

        Ok(all_results)
    }

    pub(crate) fn collect_batched_sparql_failures(
        &self,
        focus_nodes: &[Term],
        context: &ValidationContext,
    ) -> Result<HashMap<crate::types::ComponentID, crate::context::BatchedSparqlResult>, String>
    {
        let constraints = context.order_constraints(self.constraints());
        batched_property_sparql_failures_by_constraint(self, focus_nodes, &constraints, context)
    }

    pub(crate) fn validate_with_batched_sparql(
        &self,
        focus_context: &mut Context,
        context: &ValidationContext,
        trace: &mut Vec<TraceItem>,
        prebatched_sparql_failures: Option<
            &HashMap<crate::types::ComponentID, crate::context::BatchedSparqlResult>,
        >,
        events: &mut Vec<TraceEvent>,
        prefetched_values: Option<Vec<Term>>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        self.validate_internal(
            focus_context,
            context,
            trace,
            prebatched_sparql_failures,
            events,
            prefetched_values,
        )
    }

    /// Validates a context against this property shape.
    ///
    /// This involves finding the value nodes for the property shape's path from the
    /// focus node in the `focus_context`, and then validating those value nodes
    /// against all the constraints of this property shape.
    pub(crate) fn validate(
        &self,
        focus_context: &mut Context,
        context: &ValidationContext,
        trace: &mut Vec<TraceItem>,
        events: &mut Vec<TraceEvent>,
        prefetched_values: Option<Vec<Term>>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        self.validate_internal(
            focus_context,
            context,
            trace,
            None,
            events,
            prefetched_values,
        )
    }

    fn validate_internal(
        &self,
        focus_context: &mut Context,
        context: &ValidationContext,
        trace: &mut Vec<TraceItem>,
        prebatched_sparql_failures: Option<
            &HashMap<crate::types::ComponentID, crate::context::BatchedSparqlResult>,
        >,
        events: &mut Vec<TraceEvent>,
        prefetched_values: Option<Vec<Term>>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        if self.is_deactivated() {
            return Ok(vec![]);
        }
        let source = SourceShape::PropertyShape(PropShapeID(self.identifier().0));
        events.push(TraceEvent::EnterShapeExecution(
            source.clone(),
            Instant::now(),
        ));

        trace.push(TraceItem::PropertyShape(*self.identifier()));

        let shape_label = context
            .model
            .propshape_id_lookup()
            .read()
            .unwrap()
            .get_term(*self.identifier())
            .map(|t| t.to_string())
            .unwrap_or_else(|| format!("propertyshape:{}", self.identifier().0));

        let mut all_results: Vec<ComponentValidationResult> = Vec::new();

        // If the incoming context has value nodes, those are our focus nodes (for nested property shapes).
        // Otherwise, the focus node of the incoming context is our single focus node (for top-level property shapes).
        let focus_nodes_for_this_shape = if let Some(value_nodes) = focus_context.value_nodes() {
            value_nodes.clone()
        } else {
            vec![focus_context.focus_node().clone()]
        };

        let mut value_node_map: HashMap<Term, Vec<Term>> = HashMap::new();
        let constraints = context.order_constraints(self.constraints());
        let local_batched_sparql_failures;
        let batched_sparql_failures = if let Some(prebatched) = prebatched_sparql_failures {
            prebatched
        } else {
            local_batched_sparql_failures = batched_property_sparql_failures_by_constraint(
                self,
                &focus_nodes_for_this_shape,
                &constraints,
                context,
            )?;
            &local_batched_sparql_failures
        };
        let cardinality_only = constraints_are_cardinality_only(context, &constraints);
        let value_node_var = Variable::new("valueNode")
            .map_err(|e| format!("Internal error creating SPARQL variable: {}", e))?;
        let focus_var = Variable::new("focus")
            .map_err(|e| format!("Internal error creating SPARQL variable: {}", e))?;
        let sparql_path = self.sparql_path();
        let fallback_query_str = format!(
            "SELECT DISTINCT ?focus ?valueNode WHERE {{ ?focus {} ?valueNode . }}",
            sparql_path
        );
        let fallback_prepared = context.prepare_query(&fallback_query_str).map_err(|e| {
            format!(
                "Failed to prepare query template for PropertyShape {}: {}",
                self.identifier(),
                e
            )
        })?;

        let describe_component = |id: &crate::types::ComponentID| -> String {
            let descriptor = context
                .shape_ir()
                .components
                .get(id)
                .or_else(|| context.model.get_component_descriptor(id));
            match descriptor {
                Some(ComponentDescriptor::Class { .. }) => "class".into(),
                Some(ComponentDescriptor::Datatype { .. }) => "datatype".into(),
                Some(ComponentDescriptor::NodeKind { .. }) => "nodeKind".into(),
                Some(ComponentDescriptor::MinCount { .. }) => "minCount".into(),
                Some(ComponentDescriptor::MaxCount { .. }) => "maxCount".into(),
                Some(ComponentDescriptor::MinExclusive { .. }) => "minExclusive".into(),
                Some(ComponentDescriptor::MinInclusive { .. }) => "minInclusive".into(),
                Some(ComponentDescriptor::MaxExclusive { .. }) => "maxExclusive".into(),
                Some(ComponentDescriptor::MaxInclusive { .. }) => "maxInclusive".into(),
                Some(ComponentDescriptor::MinLength { .. }) => "minLength".into(),
                Some(ComponentDescriptor::MaxLength { .. }) => "maxLength".into(),
                Some(ComponentDescriptor::Pattern { .. }) => "pattern".into(),
                Some(ComponentDescriptor::LanguageIn { .. }) => "languageIn".into(),
                Some(ComponentDescriptor::UniqueLang { .. }) => "uniqueLang".into(),
                Some(ComponentDescriptor::Equals { .. }) => "equals".into(),
                Some(ComponentDescriptor::Disjoint { .. }) => "disjoint".into(),
                Some(ComponentDescriptor::LessThan { .. }) => "lessThan".into(),
                Some(ComponentDescriptor::LessThanOrEquals { .. }) => "lessThanOrEquals".into(),
                Some(ComponentDescriptor::Not { .. }) => "not".into(),
                Some(ComponentDescriptor::And { .. }) => "and".into(),
                Some(ComponentDescriptor::Or { .. }) => "or".into(),
                Some(ComponentDescriptor::Xone { .. }) => "xone".into(),
                Some(ComponentDescriptor::Closed { .. }) => "closed".into(),
                Some(ComponentDescriptor::HasValue { .. }) => "hasValue".into(),
                Some(ComponentDescriptor::In { .. }) => "in".into(),
                Some(ComponentDescriptor::Sparql { .. }) => "sparql".into(),
                Some(ComponentDescriptor::Custom { definition, .. }) => {
                    format!("custom({})", definition.iri.as_str())
                }
                Some(ComponentDescriptor::Node { .. }) => "node".into(),
                Some(ComponentDescriptor::Property { .. }) => "property".into(),
                Some(ComponentDescriptor::QualifiedValueShape { .. }) => {
                    "qualifiedValueShape".into()
                }
                None => format!("component_id:{}", id.0),
            }
        };

        // Fast path: resolve simple and inverse-simple paths directly from the cached
        // focus-predicate summary instead of issuing SPARQL queries.
        // Also check global path batch cache for shared path results.
        for focus_node in &focus_nodes_for_this_shape {
            if let Some(values) = &prefetched_values {
                value_node_map.insert(focus_node.clone(), values.clone());
            } else {
                // Check global path batch cache first
                let sparql_path = self.sparql_path();
                let cache_hit = if let Ok(cache) = context.path_batch_cache.read() {
                    cache.get(&sparql_path).and_then(|batch| batch.get(focus_node).cloned())
                } else {
                    None
                };

                if let Some(cached_values) = cache_hit {
                    value_node_map.insert(focus_node.clone(), cached_values);
                } else if let Some(value_nodes) =
                    summary_value_nodes_for_focus(context, self, focus_node)?
                {
                    value_node_map.insert(focus_node.clone(), value_nodes);
                }
            }
        }

        for focus_node in focus_nodes_for_this_shape {
            let value_selection_started = Instant::now();
            let path_is_definitely_absent =
                path_is_definitely_absent_for_focus(context, self, &focus_node)?;
            let summary_value_count = summary_value_count_for_focus(context, self, &focus_node)?;
            context.record_shape_phase_duration(
                SourceShape::PropertyShape(*self.identifier()),
                ShapeTimingPhase::PropertyValue,
                value_selection_started.elapsed(),
            );

            let mut constraint_validation_context = Context::new(
                focus_node.clone(),
                Some(self.path().clone()),
                None,
                SourceShape::PropertyShape(PropShapeID(self.identifier().0)),
                focus_context.trace_index(),
            );
            if let Some(value_count) = summary_value_count {
                constraint_validation_context.set_value_count(value_count);
            }

            let mut materialized_value_nodes: Option<Option<Vec<Term>>> =
                if path_is_definitely_absent || (cardinality_only && summary_value_count.is_some())
                {
                    Some(None)
                } else {
                    None
                };

            debug!(
                "Property shape {} has {} constraints",
                shape_label,
                constraints.len()
            );
            for &constraint_id in &constraints {
                debug!(
                    "Evaluating constraint {} ({}) for property shape {}",
                    constraint_id,
                    describe_component(&constraint_id),
                    shape_label
                );
                let component = context
                    .get_component(&constraint_id)
                    .ok_or_else(|| format!("Component not found: {}", constraint_id))?;

                if let Some(batch) = batched_sparql_failures.get(&constraint_id) {
                    for violation in batch
                        .violations_by_focus
                        .get(&focus_node)
                        .cloned()
                        .unwrap_or_default()
                    {
                        let failure = ValidationFailure::new(
                            constraint_id,
                            violation.failed_value_node,
                            violation.message,
                            violation.result_path,
                            Some(match component {
                                Component::SPARQLConstraint(comp) => comp.constraint_node.clone(),
                                _ => unreachable!(),
                            }),
                        )
                        .with_message_terms(violation.message_terms);
                        events.push(TraceEvent::ComponentFailed {
                            component: constraint_id,
                            focus: focus_node.clone(),
                            value: failure.failed_value_node.clone(),
                            message: Some(failure.message.clone()),
                            ts: Instant::now(),
                        });
                        all_results.push(ComponentValidationResult::Fail(
                            constraint_validation_context.clone(),
                            failure,
                        ));
                    }
                    continue;
                }

                if summary_value_count == Some(0)
                    && constraint_is_vacuous_when_value_count_is_zero(context, constraint_id)
                {
                    continue;
                }

                if materialized_value_nodes.is_none() {
                    let can_use_summary_count =
                        constraint_can_use_count_without_values(context, constraint_id)
                            && summary_value_count.is_some();
                    let can_skip_for_empty = summary_value_count == Some(0);

                    if !can_use_summary_count && !can_skip_for_empty {
                        let raw_values = if let Some(values) = value_node_map.remove(&focus_node) {
                            values
                        } else {
                            let substitutions = [(focus_var.clone(), focus_node.clone())];

                            let results = context
                            .execute_prepared(
                                &fallback_query_str,
                                &fallback_prepared,
                                &substitutions,
                                false,
                            )
                            .map_err(|e| {
                                format!(
                                    "Failed to execute parameterized query for PropertyShape {}: {}",
                                    self.identifier(),
                                    e
                                )
                            })?;

                            match results {
                                QueryResults::Solutions(solutions) => {
                                    let mut nodes = Vec::new();
                                    for solution_res in solutions {
                                        let solution = solution_res.map_err(|e| e.to_string())?;
                                        if let Some(term) = solution.get(&value_node_var) {
                                            nodes.push(term.clone());
                                        } else {
                                            return Err(format!(
                                                "Missing valueNode in solution for PropertyShape {}",
                                                self.identifier()
                                            ));
                                        }
                                    }
                                    nodes
                                }
                                QueryResults::Boolean(_) => {
                                    return Err(format!(
                                        "Unexpected boolean result for PropertyShape {} query",
                                        self.identifier()
                                    ));
                                }
                                QueryResults::Graph(_) => {
                                    return Err(format!(
                                        "Unexpected graph result for PropertyShape {} query",
                                        self.identifier()
                                    ));
                                }
                            }
                        };

                        let value_nodes_vec =
                            canonicalize_value_nodes(context, self, &focus_node, raw_values);
                        materialized_value_nodes = Some(if value_nodes_vec.is_empty() {
                            None
                        } else {
                            Some(value_nodes_vec)
                        });
                    }
                }

                if let Some(value_nodes) = &materialized_value_nodes {
                    constraint_validation_context.set_value_nodes(value_nodes.clone());
                }

                let component_start = Instant::now();
                events.push(TraceEvent::ComponentExecutionStart(
                    constraint_id,
                    SourceShape::PropertyShape(*self.identifier()),
                    component_start,
                ));

                match component.validate(
                    constraint_id,
                    &mut constraint_validation_context,
                    context,
                    trace,
                    events,
                    None,
                ) {
                    Ok(results) => {
                        for result in results {
                            match result {
                                ComponentValidationResult::Fail(ctx, failure) => {
                                    events.push(TraceEvent::ComponentFailed {
                                        component: constraint_id,
                                        focus: ctx.focus_node().clone(),
                                        value: failure
                                            .failed_value_node
                                            .clone()
                                            .or_else(|| ctx.value().cloned()),
                                        message: Some(failure.message.clone()),
                                        ts: Instant::now(),
                                    });
                                    all_results.push(ComponentValidationResult::Fail(ctx, failure));
                                }
                                ComponentValidationResult::Pass(ctx) => {
                                    events.push(TraceEvent::ComponentPassed {
                                        component: constraint_id,
                                        focus: ctx.focus_node().clone(),
                                        value: ctx.value().cloned(),
                                        ts: Instant::now(),
                                    });
                                    all_results.push(ComponentValidationResult::Pass(ctx));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }

                events.push(TraceEvent::ComponentExecutionEnd(
                    constraint_id,
                    SourceShape::PropertyShape(*self.identifier()),
                    Instant::now(),
                ));
            }
        }

        events.push(TraceEvent::ExitShapeExecution(source, Instant::now()));

        Ok(all_results)
    }
}
