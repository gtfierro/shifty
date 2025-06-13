use crate::context::{Context, TraceItem, ValidationContext};
use crate::named_nodes::SHACL;
use crate::types::Path;
use oxigraph::io::{RdfFormat, RdfSerializer};
use oxigraph::model::vocab::rdf;
use oxigraph::model::{BlankNode, Graph, Literal, NamedOrBlankNode, Subject, Term, Triple};
use std::collections::HashMap; // For using Term as a HashMap key
use std::error::Error;

pub struct ValidationReportBuilder {
    pub(crate) results: Vec<(Context, String)>, // Made pub(crate)
}

impl ValidationReportBuilder {
    pub fn new() -> Self {
        ValidationReportBuilder {
            results: Vec::new(),
        }
    }

    pub fn add_error(&mut self, context: &Context, error: String) {
        // Store the context by cloning it, as the original context might have a shorter lifetime.
        // The error string is moved.
        self.results.push((context.clone(), error));
        // The println! macro is removed as per the request to track errors instead of printing.
    }

    pub fn results(&self) -> &[(Context, String)] {
        &self.results
    }

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
                graph.insert(&Triple::new(
                    result_node.clone(),
                    sh.result_message,
                    Term::from(Literal::new_simple_literal(error_message)),
                ));

                // Extract info from trace
                let mut source_shape_term = None;
                let mut result_path_term = None;
                let mut source_constraint_component_term = None;

                for item in context.execution_trace().iter().rev() {
                    match item {
                        TraceItem::NodeShape(id) => {
                            if source_shape_term.is_none() {
                                source_shape_term = validation_context
                                    .nodeshape_id_lookup
                                    .borrow()
                                    .get_term(*id)
                                    .cloned();
                            }
                        }
                        TraceItem::PropertyShape(id) => {
                            if source_shape_term.is_none() {
                                source_shape_term = validation_context
                                    .propshape_id_lookup
                                    .borrow()
                                    .get_term(*id)
                                    .cloned();
                                if let Some(shape) = validation_context.get_prop_shape_by_id(id) {
                                    if result_path_term.is_none() {
                                        result_path_term =
                                            Some(path_to_rdf(shape.path(), &mut graph));
                                    }
                                }
                            }
                        }
                        TraceItem::Component(id) => {
                            if source_constraint_component_term.is_none() {
                                source_constraint_component_term = Some(validation_context
                                    .components.get(id)
                                    .unwrap()
                                    .component_type());
                            }
                        }
                    }
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

    pub fn to_rdf(
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

    pub fn to_turtle(
        &self,
        validation_context: &ValidationContext,
    ) -> Result<String, Box<dyn Error>> {
        self.to_rdf(validation_context, RdfFormat::Turtle)
    }

    pub fn dump(&self) {
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

        for (focus_node, context_error_pairs) in grouped_errors {
            println!("\nFocus Node: {}", focus_node);
            for (context, error) in context_error_pairs {
                println!("  - Error: {}", error);
                println!("    From shape: {}", context.source_shape());
                println!("    Trace: {:?}", context.execution_trace());
            }
        }
        println!("\n------------------");
    }

    pub fn merge(&mut self, other: ValidationReportBuilder) {
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
