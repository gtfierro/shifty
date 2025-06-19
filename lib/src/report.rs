use crate::context::{Context, ValidationContext};
use crate::named_nodes::SHACL;
use crate::types::{Path, TraceItem};
use oxigraph::io::{RdfFormat, RdfSerializer};
use oxigraph::model::vocab::rdf;
use oxigraph::model::{BlankNode, Graph, Literal, NamedOrBlankNode, Subject, Term, Triple};
use std::collections::HashMap; // For using Term as a HashMap key
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
        self.builder.results.is_empty()
    }

    /// Returns the validation report as an `oxigraph::model::Graph`.
    pub fn to_graph(&self) -> Graph {
        self.builder.to_graph(self.context)
    }

    /// Serializes the validation report to a string in the specified RDF format.
    pub fn to_rdf(&self, format: RdfFormat) -> Result<String, Box<dyn Error>> {
        self.builder.to_rdf(self.context, format)
    }

    /// Serializes the validation report to a string in Turtle format.
    pub fn to_turtle(&self) -> Result<String, Box<dyn Error>> {
        self.builder.to_turtle(self.context)
    }

    /// Dumps a summary of the validation report to the console for debugging.
    pub fn dump(&self) {
        self.builder.dump(self.context)
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
    results: Vec<(Context, String)>,
}

impl ValidationReportBuilder {
    /// Creates a new, empty `ValidationReportBuilder`.
    pub fn new() -> Self {
        ValidationReportBuilder {
            results: Vec::new(),
        }
    }

    /// Adds a validation failure to the report.
    ///
    /// # Arguments
    ///
    /// * `context` - The validation `Context` at the time of the failure.
    /// * `error` - A string message describing the validation error.
    pub(crate) fn add_error(&mut self, context: &Context, error: String) {
        // Store the context by cloning it, as the original context might have a shorter lifetime.
        // The error string is moved.
        self.results.push((context.clone(), error));
        // The println! macro is removed as per the request to track errors instead of printing.
    }

    /// Returns a slice of the validation results collected so far.
    /// Each item is a tuple containing the `Context` of the failure and the error message.
    pub fn results(&self) -> &[(Context, String)] {
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
        let traces = validation_context.execution_traces.borrow();
        for (context, _) in &self.results {
            if let Some(trace) = traces.get(context.trace_index()) {
                for item in trace {
                    let (label, item_type) =
                        validation_context.get_trace_item_label_and_type(item);
                    let id = item.to_string();
                    *frequencies.entry((id, label, item_type)).or_insert(0) += 1;
                }
            }
        }
        frequencies
    }

    /// Constructs an `oxigraph::model::Graph` representing the validation report.
    pub fn to_graph(&self, validation_context: &ValidationContext) -> Graph {
        let mut graph = Graph::new();
        let report_node: Subject = BlankNode::default().into();
        let sh = SHACL::new();

        graph.insert(&Triple::new(
            report_node.clone(),
            rdf::TYPE,
            Term::from(sh.validation_report),
        ));

        let conforms = self.results.is_empty();
        graph.insert(&Triple::new(
            report_node.clone(),
            sh.conforms,
            Term::from(Literal::from(conforms)),
        ));

        if !conforms {
            for (context, error_message) in &self.results {
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
                // TODO: temporarily remove
                // graph.insert(&Triple::new(
                //     result_node.clone(),
                //     sh.result_message,
                //     Term::from(Literal::new_simple_literal(error_message)),
                // ));

                // Extract info from trace
                let result_path_term = context.result_path().map(|p| path_to_rdf(p, &mut graph));
                let mut source_constraint_component_term = None;

                // TODO: this could be property shape *OR* node shape
                let source_shape_term = context.source_shape().get_term(validation_context);

                let traces = validation_context.execution_traces.borrow();
                if let Some(trace) = traces.get(context.trace_index()) {
                    println!("\n\n\nTrace for context: {:?}", context);
                    for item in trace.iter().rev() {
                        println!("trace item: {:?}", item.label(validation_context));
                    }
                    for item in trace.iter().rev() {
                        println!("trace item: {:?}", item);
                        match item {
                            TraceItem::Component(id) => {
                                if source_constraint_component_term.is_none() {
                                    source_constraint_component_term = Some(
                                        validation_context
                                            .components
                                            .get(id)
                                            .unwrap()
                                            .component_type(),
                                    );
                                }
                                break;
                            }
                            _ => {}
                        }
                    }
                }

                if let Some(v) = context.value() {
                    graph.insert(&Triple::new(result_node.clone(), sh.value, v.clone()));
                }

                if let Some(term) = source_shape_term {
                    graph.insert(&Triple::new(result_node.clone(), sh.source_shape, term));
                }

                if let Some(term) = result_path_term {
                    graph.insert(&Triple::new(result_node.clone(), sh.result_path, term));
                }

                graph.insert(&Triple::new(
                    result_node.clone(),
                    sh.result_severity,
                    Term::from(sh.violation),
                ));

                if let Some(term) = source_constraint_component_term {
                    graph.insert(&Triple::new(
                        result_node.clone(),
                        sh.source_constraint_component,
                        term,
                    ));
                }
            }
        }

        graph
    }

    /// Serializes the validation report to a string in the specified RDF format.
    pub(crate) fn to_rdf(
        &self,
        validation_context: &ValidationContext,
        format: RdfFormat,
    ) -> Result<String, Box<dyn Error>> {
        let graph = self.to_graph(validation_context);
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
        self.to_rdf(validation_context, RdfFormat::Turtle)
    }

    /// Dumps a summary of the validation report to the console for debugging.
    pub(crate) fn dump(&self, validation_context: &ValidationContext) {
        if self.results.is_empty() {
            println!("Validation report: No errors found.");
            return;
        }

        println!("Validation Report:");
        println!("------------------");

        let mut grouped_errors: HashMap<Term, Vec<(&Context, &String)>> = HashMap::new();

        for (context, error_message) in &self.results {
            grouped_errors
                .entry(context.focus_node().clone())
                .or_default()
                .push((context, error_message));
        }

        let traces = validation_context.execution_traces.borrow();
        for (focus_node, context_error_pairs) in grouped_errors {
            println!("\nFocus Node: {}", focus_node);
            for (context, error) in context_error_pairs {
                println!("  - Error: {}", error);
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
                        println!("      - {} ({}) - {}", item.to_string(), item_type, label);
                    }
                }
            }
        }
        println!("\n------------------");
    }

    /// Merges results from another `ValidationReportBuilder` into this one.
    pub(crate) fn merge(&mut self, other: ValidationReportBuilder) {
        self.results.extend(other.results);
    }
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
    let head: Subject = bnodes[0].clone().into();

    for (i, item) in items.iter().enumerate() {
        let subject: Subject = bnodes[i].clone().into();
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
