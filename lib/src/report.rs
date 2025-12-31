use crate::context::{Context, SourceShape, ValidationContext};
use crate::named_nodes::SHACL;
use crate::runtime::ValidationFailure;
use crate::types::{Path, Severity};
use oxigraph::io::{RdfFormat, RdfSerializer};
use oxigraph::model::vocab::rdf;
use oxigraph::model::{
    BlankNode, Graph, Literal, NamedOrBlankNode, NamedOrBlankNode as Subject,
    NamedOrBlankNodeRef as SubjectRef, Quad, Term, TermRef, Triple,
};
use std::collections::{HashMap, HashSet}; // For using Term as a HashMap key
use std::error::Error;

/// Represents the result of a SHACL validation.
///
/// This struct provides methods to inspect the validation outcome and
/// serialize the report into various formats. The report is tied to the
/// lifetime of the `Validator` or `ValidationContext` that created it.
pub struct ValidationReport<'a> {
    builder: ValidationReportBuilder,
    context: &'a ValidationContext,
}

/// Options for assembling validation reports.
#[derive(Debug, Clone, Copy, Default)]
pub struct ValidationReportOptions {
    /// Follow blank nodes referenced by the report and include their CBD.
    pub follow_bnodes: bool,
}

impl<'a> ValidationReport<'a> {
    /// Creates a new ValidationReport.
    /// This is intended for internal use by the library.
    pub(crate) fn new(builder: ValidationReportBuilder, context: &'a ValidationContext) -> Self {
        ValidationReport { builder, context }
    }

    /// Checks if the validation conformed.
    ///
    /// Returns `true` if there were no validation failures, `false` otherwise.
    pub fn conforms(&self) -> bool {
        self.builder.conforms(self.context)
    }

    /// Returns the validation report as an `oxigraph::model::Graph`.
    pub fn to_graph(&self) -> Graph {
        self.builder.to_graph(self.context)
    }

    /// Returns the validation report as an `oxigraph::model::Graph`, applying the given options.
    pub fn to_graph_with_options(&self, options: ValidationReportOptions) -> Graph {
        self.builder.to_graph_with_options(self.context, options)
    }

    /// Serializes the validation report to a string in the specified RDF format.
    pub fn to_rdf(&self, format: RdfFormat) -> Result<String, Box<dyn Error>> {
        self.builder.to_rdf(self.context, format)
    }

    /// Serializes the validation report to a string in the specified RDF format, applying options.
    pub fn to_rdf_with_options(
        &self,
        format: RdfFormat,
        options: ValidationReportOptions,
    ) -> Result<String, Box<dyn Error>> {
        self.builder
            .to_rdf_with_options(self.context, format, options)
    }

    /// Serializes the validation report to a string in Turtle format.
    pub fn to_turtle(&self) -> Result<String, Box<dyn Error>> {
        self.builder.to_turtle(self.context)
    }

    /// Serializes the validation report to a string in Turtle format, applying options.
    pub fn to_turtle_with_options(
        &self,
        options: ValidationReportOptions,
    ) -> Result<String, Box<dyn Error>> {
        self.builder.to_turtle_with_options(self.context, options)
    }

    /// Dumps a summary of the validation report to the console for debugging.
    ///
    /// If the validation did not conform, this method prints each validation failure,
    /// grouped by the focus node that was being validated. For each failure, it includes
    /// the error message, the source shape that triggered the validation, and the
    /// execution trace that led to the failure.
    ///
    /// The execution trace is a sequential log of the validation steps (`NodeShape`,
    /// `PropertyShape`, and `Component` visitations) that occurred before the failure.
    /// This is invaluable for debugging complex shapes.
    pub fn dump(&self) {
        self.builder.dump(self.context)
    }

    /// Prints all execution traces to the console for debugging.
    ///
    /// An execution trace is a sequential log of the validation steps performed. Each time
    /// the validator starts checking a node against a shape, it creates a new trace. This
    /// trace then records every `NodeShape`, `PropertyShape`, and `Component` that is
    /// visited during that specific validation path.
    ///
    /// When a validation constraint is violated, the resulting failure report is linked to
    /// the specific execution trace that led to it. This allows for precise debugging of
    /// *why* a failure occurred.
    ///
    /// This method prints *all* traces that were generated during the validation process,
    /// regardless of whether they resulted in a failure. This can be useful for
    /// understanding the overall flow of the validation logic. To see only the traces
    /// for failures, use the `dump()` method.
    pub fn print_traces(&self) {
        self.builder.print_traces(self.context);
    }

    /// Calculates the frequency of each component, node shape, and property shape invocation
    /// across all validation failures.
    ///
    /// Returns a HashMap where the key is a tuple of (ID, Label, Type) and the value is the count.
    pub fn get_component_frequencies(&self) -> HashMap<(String, String, String), usize> {
        self.builder.get_component_frequencies(self.context)
    }
}

/// A builder for creating a `ValidationReport`.
///
/// It collects validation results and can then be used to generate
/// the final report in various formats.
pub struct ValidationReportBuilder {
    results: Vec<(Context, ValidationFailure)>,
}

impl ValidationReportBuilder {
    fn effective_severity(
        context: &Context,
        failure: &ValidationFailure,
        validation_context: &ValidationContext,
    ) -> Severity {
        if let Some(severity) = &failure.severity {
            return severity.clone();
        }

        match context.source_shape() {
            SourceShape::NodeShape(id) => validation_context
                .model
                .get_node_shape_by_id(&id)
                .map(|s| s.severity().clone())
                .unwrap_or(Severity::Violation),
            SourceShape::PropertyShape(id) => validation_context
                .model
                .get_prop_shape_by_id(&id)
                .map(|s| s.severity().clone())
                .unwrap_or(Severity::Violation),
        }
    }

    pub(crate) fn conforms(&self, validation_context: &ValidationContext) -> bool {
        self.results.iter().all(|(ctx, failure)| {
            let sev = Self::effective_severity(ctx, failure, validation_context);
            match sev {
                Severity::Violation => false,
                Severity::Warning => !validation_context.warnings_are_errors(),
                Severity::Info => true,
                Severity::Custom(_) => false,
            }
        })
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ValidationReportBuilder {
            results: Vec::with_capacity(capacity),
        }
    }

    pub fn merge(&mut self, other: ValidationReportBuilder) {
        self.results.extend(other.results);
    }

    /// Adds a validation failure to the report.
    ///
    /// # Arguments
    ///
    /// * `context` - The validation `Context` at the time of the failure.
    /// * `failure` - A `ValidationFailure` struct with details about the error.
    pub(crate) fn add_failure(&mut self, context: &Context, failure: ValidationFailure) {
        self.results.push((context.clone(), failure));
    }

    /// Returns a slice of the validation results collected so far.
    /// Each item is a tuple containing the `Context` of the failure and the `ValidationFailure` details.
    #[allow(dead_code)]
    pub fn results(&self) -> &[(Context, ValidationFailure)] {
        &self.results
    }

    /// Calculates the frequency of each component, node shape, and property shape invocation
    /// across all validation failures.
    ///
    /// This is useful for debugging and identifying which constraints are triggered most often.
    ///
    /// # Arguments
    ///
    /// * `validation_context` - The `ValidationContext` needed to resolve IDs to labels.
    ///
    /// # Returns
    ///
    /// A `HashMap` where the key is a tuple of (ID String, Label, Type) and the value is the count.
    pub(crate) fn get_component_frequencies(
        &self,
        validation_context: &ValidationContext,
    ) -> HashMap<(String, String, String), usize> {
        let mut frequencies: HashMap<(String, String, String), usize> = HashMap::new();
        let traces = validation_context.execution_traces.lock().unwrap();
        for (context, _) in &self.results {
            if let Some(trace) = traces.get(context.trace_index()) {
                for item in trace {
                    let (label, item_type) = validation_context.get_trace_item_label_and_type(item);
                    let id = item.to_string();
                    *frequencies.entry((id, label, item_type)).or_insert(0) += 1;
                }
            }
        }
        frequencies
    }

    fn severity_term_for_result(
        context: &Context,
        failure: &ValidationFailure,
        vc: &ValidationContext,
    ) -> Term {
        let sh = SHACL::new();
        let default_violation = Term::from(sh.violation);

        if let Some(severity) = &failure.severity {
            return severity_to_term(severity, &sh);
        }

        match context.source_shape() {
            SourceShape::PropertyShape(prop_id) => vc
                .model
                .get_prop_shape_by_id(&prop_id)
                .map(|ps| severity_to_term(ps.severity(), &sh))
                .unwrap_or(default_violation),
            SourceShape::NodeShape(node_id) => vc
                .model
                .get_node_shape_by_id(&node_id)
                .map(|ns| severity_to_term(ns.severity(), &sh))
                .unwrap_or(default_violation),
        }
    }

    /// Constructs an `oxigraph::model::Graph` representing the validation report.
    pub fn to_graph(&self, validation_context: &ValidationContext) -> Graph {
        self.to_graph_with_options(validation_context, ValidationReportOptions::default())
    }

    /// Constructs an `oxigraph::model::Graph` representing the validation report, applying options.
    pub fn to_graph_with_options(
        &self,
        validation_context: &ValidationContext,
        options: ValidationReportOptions,
    ) -> Graph {
        let mut graph = Graph::new();
        let report_node: Subject = BlankNode::default().into();
        let sh = SHACL::new();

        graph.insert(&Triple::new(
            report_node.clone(),
            rdf::TYPE,
            Term::from(sh.validation_report),
        ));

        let conforms = self.conforms(validation_context);
        graph.insert(&Triple::new(
            report_node.clone(),
            sh.conforms,
            Term::from(Literal::from(conforms)),
        ));

        if !conforms {
            for (context, failure) in &self.results {
                let result_node: Subject = BlankNode::default().into();
                graph.insert(&Triple::new(
                    report_node.clone(),
                    sh.result,
                    Term::from(result_node.clone()),
                ));

                graph.insert(&Triple::new(
                    result_node.clone(),
                    rdf::TYPE,
                    Term::from(sh.validation_result),
                ));

                // sh:focusNode
                graph.insert(&Triple::new(
                    result_node.clone(),
                    sh.focus_node,
                    context.focus_node().clone(),
                ));

                // sh:resultMessage
                let mut message_terms = Vec::new();

                if let Some(shape_term) = context.source_shape().get_term(validation_context) {
                    message_terms.extend(fetch_shape_messages(validation_context, &shape_term));
                }

                if message_terms.is_empty() {
                    if let Some(constraint_term) = &failure.source_constraint {
                        message_terms
                            .extend(fetch_shape_messages(validation_context, constraint_term));
                    }
                }

                for term in &failure.message_terms {
                    if !message_terms.contains(term) {
                        message_terms.push(term.clone());
                    }
                }

                if !message_terms.is_empty() {
                    for message_term in message_terms {
                        graph.insert(&Triple::new(
                            result_node.clone(),
                            sh.result_message,
                            message_term,
                        ));
                    }
                }

                // sh:resultPath
                let result_path_term = if let Some(path_override) = &failure.result_path {
                    // If the override is a blank node head from the shapes graph, deep-clone its subgraph.
                    // Otherwise, build structurally.
                    Some(match path_override {
                        Path::Simple(t) if matches!(t, Term::BlankNode(_)) => {
                            clone_path_term_from_shapes_graph(t, validation_context, &mut graph)
                        }
                        _ => path_to_rdf(path_override, &mut graph),
                    })
                } else if let Some(_p) = context.result_path() {
                    context.result_path().map(|p| match p {
                        Path::Simple(t) if matches!(t, Term::BlankNode(_)) => {
                            clone_path_term_from_shapes_graph(t, validation_context, &mut graph)
                        }
                        _ => path_to_rdf(p, &mut graph),
                    })
                    // Prefer the original shapes-graph term when the source is a PropertyShape.
                    //match context.source_shape() {
                    //    SourceShape::PropertyShape(prop_id) => validation_context
                    //        .model
                    //        .get_prop_shape_by_id(&prop_id)
                    //        .map(|ps| clone_path_term_from_shapes_graph(ps.path_term(), validation_context, &mut graph)),
                    //    // For NodeShape-derived paths (rare), if it's a blank node head from the shapes graph, clone it.
                    //    // Otherwise, fall back to structural build.
                    //    _ => context.result_path().map(|p| match p {
                    //        Path::Simple(t) if matches!(t, Term::BlankNode(_)) => {
                    //            clone_path_term_from_shapes_graph(t, validation_context, &mut graph)
                    //        }
                    //        _ => path_to_rdf(p, &mut graph),
                    //    }),
                    //}
                } else {
                    // No runtime path set; if the source is a PropertyShape, clone from shapes graph.
                    match context.source_shape() {
                        SourceShape::PropertyShape(prop_id) => validation_context
                            .model
                            .get_prop_shape_by_id(&prop_id)
                            .map(|ps| {
                                clone_path_term_from_shapes_graph(
                                    ps.path_term(),
                                    validation_context,
                                    &mut graph,
                                )
                            }),
                        _ => None,
                    }
                };

                let source_shape_term = context.source_shape().get_term(validation_context);

                let source_constraint_component_term = validation_context
                    .get_component(&failure.component_id)
                    .map(|component| component.component_type());

                if let Some(v) = &failure.failed_value_node {
                    graph.insert(&Triple::new(result_node.clone(), sh.value, v.clone()));
                }

                if let Some(term) = source_shape_term {
                    graph.insert(&Triple::new(result_node.clone(), sh.source_shape, term));
                }

                if let Some(term) = result_path_term {
                    graph.insert(&Triple::new(result_node.clone(), sh.result_path, term));
                }

                let severity_term = ValidationReportBuilder::severity_term_for_result(
                    context,
                    failure,
                    validation_context,
                );
                graph.insert(&Triple::new(
                    result_node.clone(),
                    sh.result_severity,
                    severity_term,
                ));

                if let Some(term) = source_constraint_component_term {
                    graph.insert(&Triple::new(
                        result_node.clone(),
                        sh.source_constraint_component,
                        term,
                    ));
                }

                if let Some(term) = &failure.source_constraint {
                    graph.insert(&Triple::new(
                        result_node.clone(),
                        sh.source_constraint,
                        term.clone(),
                    ));
                }
            }
        }

        if options.follow_bnodes {
            follow_bnodes_in_report(&mut graph, validation_context);
        }

        graph
    }

    /// Serializes the validation report to a string in the specified RDF format.
    pub(crate) fn to_rdf(
        &self,
        validation_context: &ValidationContext,
        format: RdfFormat,
    ) -> Result<String, Box<dyn Error>> {
        self.to_rdf_with_options(
            validation_context,
            format,
            ValidationReportOptions::default(),
        )
    }

    /// Serializes the validation report to a string in the specified RDF format, applying options.
    pub(crate) fn to_rdf_with_options(
        &self,
        validation_context: &ValidationContext,
        format: RdfFormat,
        options: ValidationReportOptions,
    ) -> Result<String, Box<dyn Error>> {
        let graph = self.to_graph_with_options(validation_context, options);
        let mut writer = Vec::new();
        let mut serializer = RdfSerializer::from_format(format)
            .with_prefix("sh", "http://www.w3.org/ns/shacl#")?
            .with_prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")?
            .with_prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#")?
            .for_writer(&mut writer);

        for triple in graph.iter() {
            serializer.serialize_triple(triple)?;
        }
        serializer.finish()?;
        Ok(String::from_utf8(writer)?)
    }

    /// Serializes the validation report to a string in Turtle format.
    pub(crate) fn to_turtle(
        &self,
        validation_context: &ValidationContext,
    ) -> Result<String, Box<dyn Error>> {
        self.to_rdf_with_options(
            validation_context,
            RdfFormat::Turtle,
            ValidationReportOptions::default(),
        )
    }

    /// Serializes the validation report to a string in Turtle format, applying options.
    pub(crate) fn to_turtle_with_options(
        &self,
        validation_context: &ValidationContext,
        options: ValidationReportOptions,
    ) -> Result<String, Box<dyn Error>> {
        self.to_rdf_with_options(validation_context, RdfFormat::Turtle, options)
    }

    /// Dumps a summary of the validation report to the console for debugging.
    ///
    /// If the validation did not conform, this method prints each validation failure,
    /// grouped by the focus node that was being validated. For each failure, it includes
    /// the error message, the source shape that triggered the validation, and the
    /// execution trace that led to the failure.
    ///
    /// The execution trace is a sequential log of the validation steps (`NodeShape`,
    /// `PropertyShape`, and `Component` visitations) that occurred before the failure.
    /// This is invaluable for debugging complex shapes.
    pub(crate) fn dump(&self, validation_context: &ValidationContext) {
        if self.results.is_empty() {
            println!("Validation report: No errors found.");
            return;
        }

        println!("Validation Report:");
        println!("------------------");

        let mut grouped_errors: HashMap<Term, Vec<(&Context, &ValidationFailure)>> = HashMap::new();

        for (context, failure) in &self.results {
            grouped_errors
                .entry(context.focus_node().clone())
                .or_default()
                .push((context, failure));
        }

        let traces = validation_context.execution_traces.lock().unwrap();
        for (focus_node, context_failure_pairs) in grouped_errors {
            println!("\nFocus Node: {}", focus_node);
            for (context, failure) in context_failure_pairs {
                println!("  - Error: {}", failure.message);
                if let Some(source_shape_term) = context.source_shape().get_term(validation_context)
                {
                    println!("    From shape: {}", source_shape_term);
                } else {
                    println!("    From shape: {}", context.source_shape());
                }

                println!("    Trace:");
                if let Some(trace) = traces.get(context.trace_index()) {
                    for item in trace {
                        let (label, item_type) =
                            validation_context.get_trace_item_label_and_type(item);
                        println!("      - {} ({}) - {}", item, item_type, label);
                    }
                }
            }
        }
        println!("\n------------------");
    }

    /// Prints all execution traces to the console for debugging.
    ///
    /// An execution trace is a sequential log of the validation steps performed. Each time
    /// the validator starts checking a node against a shape, it creates a new trace. This
    /// trace then records every `NodeShape`, `PropertyShape`, and `Component` that is
    /// visited during that specific validation path.
    ///
    /// When a validation constraint is violated, the resulting failure report is linked to
    /// the specific execution trace that led to it. This allows for precise debugging of
    /// *why* a failure occurred.
    ///
    /// This method prints *all* traces that were generated during the validation process,
    /// regardless of whether they resulted in a failure. This can be useful for
    /// understanding the overall flow of the validation logic. To see only the traces
    /// for failures, use the `dump()` method.
    pub(crate) fn print_traces(&self, validation_context: &ValidationContext) {
        println!("\nExecution Traces:");
        println!("-----------------");
        let traces = validation_context.execution_traces.lock().unwrap();
        if traces.is_empty() {
            println!("No execution traces recorded.");
            return;
        }

        for (i, trace) in traces.iter().enumerate() {
            println!("\nTrace {}:", i);
            if trace.is_empty() {
                println!("  (empty trace)");
                continue;
            }
            for item in trace {
                let (label, item_type) = validation_context.get_trace_item_label_and_type(item);
                println!("  - {} ({}) - {}", item, item_type, label);
            }
        }
    }
}

fn severity_to_term(severity: &Severity, sh: &SHACL) -> Term {
    match severity {
        Severity::Info => Term::from(sh.info),
        Severity::Warning => Term::from(sh.warning),
        Severity::Violation => Term::from(sh.violation),
        Severity::Custom(nn) => Term::from(nn.clone()),
    }
}

#[allow(dead_code)]
fn result_path_term_for_property_shape(path: &Path, graph: &mut Graph) -> Term {
    match path {
        Path::Sequence(elements) => build_list_minimal(elements, graph),
        Path::Alternative(options) => {
            let sh = SHACL::new();
            let head = BlankNode::default();
            let head_subject: Subject = head.clone().into();
            let list_head = build_list_minimal(options, graph);
            graph.insert(&Triple::new(head_subject, sh.alternative_path, list_head));
            head.into()
        }
        _ => path_to_rdf(path, graph),
    }
}

#[allow(dead_code)]
fn build_list_minimal(elements: &[Path], graph: &mut Graph) -> Term {
    let head_bnode = BlankNode::default();
    let head_subject: Subject = head_bnode.clone().into();

    if let Some(first) = elements.first() {
        let first_term = path_to_rdf(first, graph);
        graph.insert(&Triple::new(head_subject.clone(), rdf::FIRST, first_term));
    }

    let rest_term: Term = if elements.is_empty() {
        rdf::NIL.into()
    } else {
        Term::from(BlankNode::default())
    };

    graph.insert(&Triple::new(head_subject, rdf::REST, rest_term.clone()));
    head_bnode.into()
}

fn path_to_rdf(path: &Path, graph: &mut Graph) -> Term {
    let sh = SHACL::new();
    match path {
        Path::Simple(term) => term.clone(),
        Path::Inverse(inner) => {
            let bn: Subject = BlankNode::default().into();
            let inner_term = path_to_rdf(inner, graph);
            graph.insert(&Triple::new(bn.clone(), sh.inverse_path, inner_term));
            bn.into()
        }
        Path::Sequence(paths) => {
            let items: Vec<Term> = paths.iter().map(|p| path_to_rdf(p, graph)).collect();
            build_rdf_list(items, graph)
        }
        Path::Alternative(paths) => {
            let bn: Subject = BlankNode::default().into();
            let items: Vec<Term> = paths.iter().map(|p| path_to_rdf(p, graph)).collect();
            let list_head = build_rdf_list(items, graph);
            graph.insert(&Triple::new(bn.clone(), sh.alternative_path, list_head));
            bn.into()
        }
        Path::ZeroOrMore(inner) => {
            let bn: Subject = BlankNode::default().into();
            let inner_term = path_to_rdf(inner, graph);
            graph.insert(&Triple::new(bn.clone(), sh.zero_or_more_path, inner_term));
            bn.into()
        }
        Path::OneOrMore(inner) => {
            let bn: Subject = BlankNode::default().into();
            let inner_term = path_to_rdf(inner, graph);
            graph.insert(&Triple::new(bn.clone(), sh.one_or_more_path, inner_term));
            bn.into()
        }
        Path::ZeroOrOne(inner) => {
            let bn: Subject = BlankNode::default().into();
            let inner_term = path_to_rdf(inner, graph);
            graph.insert(&Triple::new(bn.clone(), sh.zero_or_one_path, inner_term));
            bn.into()
        }
    }
}

fn build_rdf_list(items: impl IntoIterator<Item = Term>, graph: &mut Graph) -> Term {
    let head: Subject = rdf::NIL.into();

    let items: Vec<Term> = items.into_iter().collect();
    if items.is_empty() {
        return head.into();
    }

    let bnodes: Vec<NamedOrBlankNode> = (0..items.len())
        .map(|_| BlankNode::default().into())
        .collect();
    let head: Subject = bnodes[0].clone();

    for (i, item) in items.iter().enumerate() {
        let subject: Subject = bnodes[i].clone();
        graph.insert(&Triple::new(subject.clone(), rdf::FIRST, item.clone()));
        let rest: Term = if i == items.len() - 1 {
            rdf::NIL.into()
        } else {
            bnodes[i + 1].clone().into()
        };
        graph.insert(&Triple::new(subject, rdf::REST, rest));
    }
    head.into()
}

// Deeply clones a SHACL path term from the shapes graph into the report graph,
// preserving the original blank-node and RDF list structure.
fn clone_path_term_from_shapes_graph(
    term: &Term,
    validation_context: &ValidationContext,
    out_graph: &mut Graph,
) -> Term {
    let mut memo: HashMap<Term, Term> = HashMap::new();
    clone_path_term_from_shapes_graph_inner(term, validation_context, out_graph, &mut memo)
}

fn fetch_shape_messages(validation_context: &ValidationContext, term: &Term) -> Vec<Term> {
    let shacl = SHACL::new();
    if let Some(subject_ref) = term_to_subject_ref(term) {
        validation_context
            .quads_for_pattern(
                Some(subject_ref),
                Some(shacl.message),
                None,
                Some(validation_context.shape_graph_iri_ref()),
            )
            .unwrap_or_default()
            .into_iter()
            .map(|q| q.object)
            .collect()
    } else {
        Vec::new()
    }
}

fn term_to_subject_ref(term: &Term) -> Option<SubjectRef<'_>> {
    match term {
        Term::NamedNode(nn) => Some(SubjectRef::NamedNode(nn.as_ref())),
        Term::BlankNode(bn) => Some(SubjectRef::BlankNode(bn.as_ref())),
        _ => None,
    }
}

fn clone_path_term_from_shapes_graph_inner(
    term: &Term,
    validation_context: &ValidationContext,
    out_graph: &mut Graph,
    memo: &mut HashMap<Term, Term>,
) -> Term {
    if !matches!(term, Term::BlankNode(_)) {
        return term.clone();
    }

    if let Some(mapped) = memo.get(term) {
        return mapped.clone();
    }

    let new_bn_term: Term = BlankNode::default().into();
    memo.insert(term.clone(), new_bn_term.clone());

    let sh = SHACL::new();

    let subject_ref = match term {
        Term::BlankNode(b) => SubjectRef::BlankNode(b.as_ref()),
        Term::NamedNode(n) => SubjectRef::NamedNode(n.as_ref()),
        _ => unreachable!(),
    };

    for q in validation_context
        .quads_for_pattern(Some(subject_ref), None, None, None)
        .unwrap_or_default()
    {
        let pred = q.predicate;

        // Copy only the path-defining predicates.
        if pred == rdf::FIRST
            || pred == rdf::REST
            || pred == sh.alternative_path
            || pred == sh.inverse_path
            || pred == sh.zero_or_more_path
            || pred == sh.one_or_more_path
            || pred == sh.zero_or_one_path
        {
            let obj_owned: Term = q.object.to_owned();
            let cloned_obj = clone_path_term_from_shapes_graph_inner(
                &obj_owned,
                validation_context,
                out_graph,
                memo,
            );

            let new_subject: Subject = match &new_bn_term {
                Term::BlankNode(b) => b.clone().into(),
                Term::NamedNode(n) => n.clone().into(),
                _ => unreachable!(),
            };

            out_graph.insert(&Triple::new(new_subject, pred, cloned_obj));
        }
    }

    new_bn_term
}

fn follow_bnodes_in_report(graph: &mut Graph, validation_context: &ValidationContext) {
    let mut queue: Vec<Term> = Vec::new();
    let mut seen: HashSet<Term> = HashSet::new();

    for t in graph.iter() {
        match t.subject {
            SubjectRef::BlankNode(bn) => {
                let term = Term::BlankNode(bn.into_owned());
                if seen.insert(term.clone()) {
                    queue.push(term);
                }
            }
            SubjectRef::NamedNode(nn) => {
                if validation_context.is_data_skolem_iri(nn)
                    || validation_context.is_shape_skolem_iri(nn)
                {
                    let term = Term::NamedNode(nn.into_owned());
                    if seen.insert(term.clone()) {
                        queue.push(term);
                    }
                }
            }
        }
        match t.object {
            TermRef::BlankNode(bn) => {
                let term = Term::BlankNode(bn.into_owned());
                if seen.insert(term.clone()) {
                    queue.push(term);
                }
            }
            TermRef::NamedNode(nn) => {
                if validation_context.is_data_skolem_iri(nn)
                    || validation_context.is_shape_skolem_iri(nn)
                {
                    let term = Term::NamedNode(nn.into_owned());
                    if seen.insert(term.clone()) {
                        queue.push(term);
                    }
                }
            }
            _ => {}
        }
    }

    while let Some(term) = queue.pop() {
        let subject_ref = match &term {
            Term::BlankNode(bn) => SubjectRef::BlankNode(bn.as_ref()),
            Term::NamedNode(nn) => SubjectRef::NamedNode(nn.as_ref()),
            _ => continue,
        };
        for graph_ref in [
            validation_context.data_graph_iri_ref(),
            validation_context.shape_graph_iri_ref(),
        ] {
            let quads = validation_context
                .quads_for_pattern(Some(subject_ref), None, None, Some(graph_ref))
                .unwrap_or_default();
            for quad in quads {
                let Quad {
                    subject,
                    predicate,
                    object,
                    ..
                } = quad;
                graph.insert(&Triple::new(subject, predicate, object.clone()));
                match object {
                    Term::BlankNode(obj_bn) => {
                        let term = Term::BlankNode(obj_bn);
                        if seen.insert(term.clone()) {
                            queue.push(term);
                        }
                    }
                    Term::NamedNode(obj_nn) => {
                        if validation_context.is_data_skolem_iri(obj_nn.as_ref())
                            || validation_context.is_shape_skolem_iri(obj_nn.as_ref())
                        {
                            let term = Term::NamedNode(obj_nn);
                            if seen.insert(term.clone()) {
                                queue.push(term);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}
