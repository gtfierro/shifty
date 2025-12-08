use crate::context::{Context, SourceShape, ValidationContext};
use crate::model::components::ComponentDescriptor;
use crate::planning::{build_validation_plan, ShapeRef};
use crate::report::ValidationReportBuilder;
use crate::runtime::{ComponentValidationResult, ToSubjectRef};
use crate::shape::{NodeShape, PropertyShape, ValidateShape};
use crate::trace::TraceEvent;
use crate::types::{PropShapeID, TargetEvalExt, TraceItem};
use log::{debug, info};
use oxigraph::model::{Literal, Term};
use oxigraph::sparql::{QueryResults, Variable};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet, VecDeque};

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
    match shape_ref {
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
    }
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
        .objects_for_predicate(
            subject,
            predicate.as_ref(),
            validation_context.data_graph_iri_ref(),
        )
        .unwrap_or_default();

    if raw_objects.is_empty() && validation_context.model.original_values.is_none() {
        return nodes;
    }

    let mut exact_matches: HashSet<Term> = raw_objects.iter().cloned().collect();
    let mut literals_by_signature: HashMap<(String, Option<String>), VecDeque<Term>> =
        HashMap::new();

    for term in &raw_objects {
        if let Term::Literal(lit) = term {
            let key = literal_signature(lit);
            literals_by_signature
                .entry(key)
                .or_default()
                .push_back(term.clone());
        }
    }

    let original_index = validation_context.model.original_values.as_ref();

    for node in &mut nodes {
        let current = node.clone();

        if let Term::Literal(ref lit) = current {
            if let Some(index) = original_index {
                if let Some(original) = index.resolve_literal(focus_node, predicate, lit) {
                    if original != current {
                        exact_matches.remove(&original);
                        *node = original;
                        continue;
                    }
                }
            }
        }

        if exact_matches.remove(&current) {
            continue;
        }

        if let Term::Literal(ref lit) = current {
            if let Some(term) =
                lookup_by_signature(&mut literals_by_signature, &mut exact_matches, lit)
            {
                *node = term;
            }
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
    if let Some(queue) = buckets.get_mut(&key) {
        if let Some(term) = queue.pop_front() {
            exact_matches.remove(&term);
            return Some(term);
        }
    }
    None
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
        let focus_nodes = if let Some(cached) = context.cached_node_targets(self.identifier()) {
            cached
        } else {
            let mut nodes = HashSet::new();
            for target in self.targets.iter() {
                info!(
                    "get targets from target: {:?} on shape {}",
                    target,
                    self.identifier()
                );
                for ctx in
                    target.get_target_nodes(context, SourceShape::NodeShape(*self.identifier()))?
                {
                    nodes.insert(ctx.focus_node().clone());
                }
            }
            let vec: Vec<Term> = nodes.into_iter().collect();
            context.store_node_targets(*self.identifier(), vec.clone());
            vec
        };

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
            .par_iter()
            .map(|focus_node| {
                let mut local_report = ValidationReportBuilder::with_capacity(8);
                let mut target_context = Context::new(
                    focus_node.clone(),
                    None,
                    Some(vec![focus_node.clone()]),
                    SourceShape::NodeShape(*self.identifier()),
                    context.new_trace(),
                );
                let trace_index = target_context.trace_index();
                context
                    .trace_sink
                    .record(TraceEvent::EnterNodeShape(*self.identifier()));

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

                    match comp.validate(
                        *constraint_id,
                        &mut target_context,
                        context,
                        &mut local_trace,
                    ) {
                        Ok(validation_results) => {
                            for result in validation_results {
                                match result {
                                    ComponentValidationResult::Fail(ctx, failure) => {
                                        context.trace_sink.record(TraceEvent::ComponentFailed {
                                            component: *constraint_id,
                                            focus: ctx.focus_node().clone(),
                                            value: failure
                                                .failed_value_node
                                                .clone()
                                                .or_else(|| ctx.value().cloned()),
                                            message: Some(failure.message.clone()),
                                        });
                                        local_report.add_failure(&ctx, failure);
                                    }
                                    ComponentValidationResult::Pass(ctx) => {
                                        context.trace_sink.record(TraceEvent::ComponentPassed {
                                            component: *constraint_id,
                                            focus: ctx.focus_node().clone(),
                                            value: ctx.value().cloned(),
                                        });
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
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
        let focus_nodes = if let Some(cached) = context.cached_prop_targets(self.identifier()) {
            cached
        } else {
            let mut nodes = HashSet::new();
            for target in self.targets.iter() {
                info!(
                    "get targets from target: {:?} on shape {}",
                    target,
                    self.identifier()
                );
                for ctx in target
                    .get_target_nodes(context, SourceShape::PropertyShape(*self.identifier()))?
                {
                    nodes.insert(ctx.focus_node().clone());
                }
            }
            let vec: Vec<Term> = nodes.into_iter().collect();
            context.store_prop_targets(*self.identifier(), vec.clone());
            vec
        };

        if focus_nodes.is_empty() {
            return Ok(());
        }

        let focus_reports: Result<Vec<ValidationReportBuilder>, String> = focus_nodes
            .par_iter()
            .map(|focus_node| {
                let mut local_report = ValidationReportBuilder::with_capacity(8);
                let mut target_context = Context::new(
                    focus_node.clone(),
                    None,
                    Some(vec![focus_node.clone()]),
                    SourceShape::PropertyShape(*self.identifier()),
                    context.new_trace(),
                );
                let trace_index = target_context.trace_index();
                context
                    .trace_sink
                    .record(TraceEvent::EnterPropertyShape(*self.identifier()));

                let mut local_trace: Vec<TraceItem> = Vec::new();

                match self.validate(&mut target_context, context, &mut local_trace) {
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
    ) -> Result<Vec<ComponentValidationResult>, String> {
        if self.is_deactivated() {
            return Ok(vec![]);
        }
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

        // Fast path: batch query when all focus nodes are IRIs and the path is simple.
        if focus_nodes_for_this_shape.len() > 1 && self.path().is_simple_predicate() {
            let predicate_term = self.path_term();
            if let Term::NamedNode(pred) = predicate_term {
                // Only IRIs allowed in VALUES; if any focus node is blank, skip batching.
                if focus_nodes_for_this_shape
                    .iter()
                    .all(|f| matches!(f, Term::NamedNode(_)))
                {
                    let values_clause = focus_nodes_for_this_shape
                        .iter()
                        .map(|t| t.to_string())
                        .collect::<Vec<_>>()
                        .join(" ");
                    let query_str = format!(
                        "SELECT DISTINCT ?focus ?valueNode WHERE {{ VALUES ?focus {{ {} }} ?focus <{}> ?valueNode . }}",
                        values_clause,
                        pred.as_str()
                    );

                    let prepared = context.prepare_query(&query_str).map_err(|e| {
                        format!(
                            "Failed to prepare batch query for PropertyShape {}: {}",
                            self.identifier(),
                            e
                        )
                    })?;
                    let results = context
                        .execute_prepared(&query_str, &prepared, &[], false)
                        .map_err(|e| {
                            format!(
                                "Failed to execute batch query for PropertyShape {}: {}",
                                self.identifier(),
                                e
                            )
                        })?;
                    let focus_var = Variable::new("focus")
                        .map_err(|e| format!("Internal error creating SPARQL variable: {}", e))?;
                    let value_var = Variable::new("valueNode")
                        .map_err(|e| format!("Internal error creating SPARQL variable: {}", e))?;

                    match results {
                        QueryResults::Solutions(solutions) => {
                            for solution_res in solutions {
                                let solution = solution_res.map_err(|e| e.to_string())?;
                                let Some(focus_term) = solution.get(&focus_var) else {
                                    continue;
                                };
                                let Some(val_term) = solution.get(&value_var) else {
                                    continue;
                                };
                                value_node_map
                                    .entry(focus_term.clone())
                                    .or_default()
                                    .push(val_term.clone());
                            }
                        }
                        QueryResults::Boolean(_) => {
                            return Err(format!(
                                "Unexpected boolean result for PropertyShape {} batch query",
                                self.identifier()
                            ));
                        }
                        QueryResults::Graph(_) => {
                            return Err(format!(
                                "Unexpected graph result for PropertyShape {} batch query",
                                self.identifier()
                            ));
                        }
                    }
                }
            }
        }

        for focus_node in focus_nodes_for_this_shape {
            let raw_values = if let Some(values) = value_node_map.remove(&focus_node) {
                values
            } else {
                let sparql_path = self.sparql_path();
                let query_str = format!(
                    "SELECT DISTINCT ?valueNode WHERE {{ {} {} ?valueNode . }}",
                    focus_node, sparql_path
                );

                let prepared = context.prepare_query(&query_str).map_err(|e| {
                    format!(
                        "Failed to prepare query for PropertyShape {}: {}",
                        self.identifier(),
                        e
                    )
                })?;

                let results = context
                    .execute_prepared(&query_str, &prepared, &[], false)
                    .map_err(|e| {
                        format!(
                            "Failed to execute query for PropertyShape {}: {}",
                            self.identifier(),
                            e
                        )
                    })?;

                match results {
                    QueryResults::Solutions(solutions) => {
                        let value_node_var = Variable::new("valueNode").map_err(|e| {
                            format!("Internal error creating SPARQL variable: {}", e)
                        })?;

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
                canonicalize_value_nodes(context, self, &focus_node, raw_values.clone());

            let value_nodes_opt = if value_nodes_vec.is_empty() {
                None
            } else {
                Some(value_nodes_vec)
            };

            let mut constraint_validation_context = Context::new(
                focus_node.clone(),
                Some(self.path().clone()),
                value_nodes_opt,
                SourceShape::PropertyShape(PropShapeID(self.identifier().0)),
                focus_context.trace_index(),
            );

            let constraints = context.order_constraints(self.constraints());
            debug!(
                "Property shape {} has {} constraints",
                shape_label,
                constraints.len()
            );
            for constraint_id in constraints {
                debug!(
                    "Evaluating constraint {} ({}) for property shape {}",
                    constraint_id,
                    describe_component(&constraint_id),
                    shape_label
                );
                let component = context
                    .get_component(&constraint_id)
                    .ok_or_else(|| format!("Component not found: {}", constraint_id))?;

                match component.validate(
                    constraint_id,
                    &mut constraint_validation_context,
                    context,
                    trace,
                ) {
                    Ok(results) => {
                        for result in results {
                            match result {
                                ComponentValidationResult::Fail(ctx, failure) => {
                                    context.trace_sink.record(TraceEvent::ComponentFailed {
                                        component: constraint_id,
                                        focus: ctx.focus_node().clone(),
                                        value: failure
                                            .failed_value_node
                                            .clone()
                                            .or_else(|| ctx.value().cloned()),
                                        message: Some(failure.message.clone()),
                                    });
                                    all_results.push(ComponentValidationResult::Fail(ctx, failure));
                                }
                                ComponentValidationResult::Pass(ctx) => {
                                    context.trace_sink.record(TraceEvent::ComponentPassed {
                                        component: constraint_id,
                                        focus: ctx.focus_node().clone(),
                                        value: ctx.value().cloned(),
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
            }
        }

        Ok(all_results)
    }
}
