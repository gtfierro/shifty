use crate::codegen::render_tokens_as_module;
use crate::ir::SrcGenIR;
use quote::quote;

pub fn generate(_ir: &SrcGenIR) -> Result<String, String> {
    let tokens = quote! {
        const SH_RESULT: &str = "http://www.w3.org/ns/shacl#result";
        const SH_FOCUS_NODE: &str = "http://www.w3.org/ns/shacl#focusNode";
        const SH_SOURCE_SHAPE: &str = "http://www.w3.org/ns/shacl#sourceShape";
        const SH_SOURCE_COMPONENT: &str = "http://www.w3.org/ns/shacl#sourceConstraintComponent";
        const SH_VALUE: &str = "http://www.w3.org/ns/shacl#value";
        const SH_RESULT_PATH: &str = "http://www.w3.org/ns/shacl#resultPath";

        pub fn build_violations(report_graph: &oxigraph::model::Graph) -> Vec<Violation> {
            let mut result_nodes: Vec<oxigraph::model::Term> = Vec::new();
            for triple in report_graph.iter() {
                if triple.predicate.as_str() == SH_RESULT {
                    result_nodes.push(triple.object.into());
                }
            }

            let mut violations = Vec::with_capacity(result_nodes.len());
            for result_node in result_nodes {
                let mut focus: Option<oxigraph::model::Term> = None;
                let mut value: Option<oxigraph::model::Term> = None;
                let mut path: Option<ResultPath> = None;
                let mut shape_id = 0_u64;
                let mut component_id = 0_u64;

                for triple in report_graph.iter() {
                    let subject_term = oxigraph::model::Term::from(triple.subject.clone());
                    if subject_term != result_node {
                        continue;
                    }

                    match triple.predicate.as_str() {
                        SH_FOCUS_NODE => {
                            focus = Some(triple.object.into());
                        }
                        SH_VALUE => {
                            value = Some(triple.object.into());
                        }
                        SH_RESULT_PATH => {
                            path = Some(ResultPath::Term(triple.object.into()));
                        }
                        SH_SOURCE_SHAPE => {
                            if let oxigraph::model::TermRef::NamedNode(node) = triple.object {
                                if let Some(id) = shape_id_for_iri(node.as_str()) {
                                    shape_id = id;
                                }
                            }
                        }
                        SH_SOURCE_COMPONENT => {
                            if let oxigraph::model::TermRef::NamedNode(node) = triple.object {
                                if let Some(id) = component_id_for_iri(node.as_str()) {
                                    component_id = id;
                                }
                            }
                        }
                        _ => {}
                    }
                }

                if let Some(focus) = focus {
                    violations.push(Violation {
                        shape_id,
                        component_id,
                        focus,
                        value,
                        path,
                    });
                }
            }

            violations
        }

        pub fn render_violations_to_turtle(violations: &[Violation]) -> Result<String, String> {
            let mut graph = oxigraph::model::Graph::new();
            let report_node: oxigraph::model::NamedOrBlankNode =
                oxigraph::model::BlankNode::default().into();

            graph.insert(&oxigraph::model::Triple::new(
                report_node.clone(),
                oxigraph::model::vocab::rdf::TYPE,
                oxigraph::model::Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "http://www.w3.org/ns/shacl#ValidationReport",
                )),
            ));

            graph.insert(&oxigraph::model::Triple::new(
                report_node.clone(),
                oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#conforms"),
                oxigraph::model::Term::Literal(oxigraph::model::Literal::from(violations.is_empty())),
            ));

            for violation in violations {
                let result_node: oxigraph::model::NamedOrBlankNode =
                    oxigraph::model::BlankNode::default().into();
                graph.insert(&oxigraph::model::Triple::new(
                    report_node.clone(),
                    oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#result"),
                    oxigraph::model::Term::from(result_node.clone()),
                ));
                graph.insert(&oxigraph::model::Triple::new(
                    result_node.clone(),
                    oxigraph::model::vocab::rdf::TYPE,
                    oxigraph::model::Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                        "http://www.w3.org/ns/shacl#ValidationResult",
                    )),
                ));
                graph.insert(&oxigraph::model::Triple::new(
                    result_node.clone(),
                    oxigraph::model::NamedNode::new_unchecked(
                        "http://www.w3.org/ns/shacl#resultSeverity",
                    ),
                    oxigraph::model::Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                        "http://www.w3.org/ns/shacl#Violation",
                    )),
                ));
                graph.insert(&oxigraph::model::Triple::new(
                    result_node.clone(),
                    oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#focusNode"),
                    violation.focus.clone(),
                ));

                if let Some(value) = &violation.value {
                    graph.insert(&oxigraph::model::Triple::new(
                        result_node.clone(),
                        oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#value"),
                        value.clone(),
                    ));
                }

                if let Some(path) = &violation.path {
                    if let ResultPath::Term(path_term) = path {
                        graph.insert(&oxigraph::model::Triple::new(
                            result_node.clone(),
                            oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#resultPath"),
                            path_term.clone(),
                        ));
                    }
                }

                let source_shape = shape_iri(violation.shape_id);
                if !source_shape.is_empty() {
                    if let Ok(shape_node) = oxigraph::model::NamedNode::new(source_shape) {
                        graph.insert(&oxigraph::model::Triple::new(
                            result_node.clone(),
                            oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#sourceShape"),
                            oxigraph::model::Term::NamedNode(shape_node),
                        ));
                    }
                }

                let source_component = component_iri(violation.component_id);
                if let Ok(component_node) = oxigraph::model::NamedNode::new(source_component) {
                    graph.insert(&oxigraph::model::Triple::new(
                        result_node,
                        oxigraph::model::NamedNode::new_unchecked(
                            "http://www.w3.org/ns/shacl#sourceConstraintComponent",
                        ),
                        oxigraph::model::Term::NamedNode(component_node),
                    ));
                }
            }

            let mut writer = Vec::new();
            let mut serializer = oxigraph::io::RdfSerializer::from_format(oxigraph::io::RdfFormat::Turtle)
                .with_prefix("sh", "http://www.w3.org/ns/shacl#")
                .map_err(|err| format!("failed to set sh prefix: {err}"))?
                .with_prefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
                .map_err(|err| format!("failed to set rdf prefix: {err}"))?
                .for_writer(&mut writer);
            for triple in graph.iter() {
                serializer
                    .serialize_triple(triple)
                    .map_err(|err| format!("failed to serialize report triple: {err}"))?;
            }
            serializer
                .finish()
                .map_err(|err| format!("failed to finish report serialization: {err}"))?;
            String::from_utf8(writer).map_err(|err| format!("report is not valid utf8: {err}"))
        }

        pub fn build_report_from_specialized_violations(
            violations: Vec<Violation>,
        ) -> Result<Report, String> {
            record_violation_metrics(&violations);
            let report_turtle = render_violations_to_turtle(&violations)?;
            Ok(Report {
                violations,
                report_turtle: report_turtle.clone(),
                report_turtle_follow_bnodes: report_turtle,
            })
        }

        pub fn build_report_from_runtime_validation_report(
            report: &shifty::ValidationReport,
        ) -> Report {
            let report_graph = report.to_graph_with_options(shifty::ValidationReportOptions {
                follow_bnodes: false,
            });
            let violations = build_violations(&report_graph);
            record_violation_metrics(&violations);

            let report_turtle = report
                .to_turtle_with_options(shifty::ValidationReportOptions {
                    follow_bnodes: false,
                })
                .unwrap_or_else(|err| {
                    eprintln!("srcgen runtime error: failed to serialize report: {err}");
                    "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:ValidationReport ; sh:conforms true .\n".to_string()
                });
            let report_turtle_follow_bnodes = report
                .to_turtle_with_options(shifty::ValidationReportOptions {
                    follow_bnodes: true,
                })
                .unwrap_or_else(|_| report_turtle.clone());

            Report {
                violations,
                report_turtle,
                report_turtle_follow_bnodes,
            }
        }

        fn record_violation_metrics(violations: &[Violation]) {
            for violation in violations {
                record_component_violation(violation.component_id);
            }
        }

        pub fn empty_conforming_report() -> Report {
            Report {
                violations: Vec::new(),
                report_turtle: String::from(
                    "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:ValidationReport ; sh:conforms true .\n",
                ),
                report_turtle_follow_bnodes: String::from(
                    "@prefix sh: <http://www.w3.org/ns/shacl#> .\n[] a sh:ValidationReport ; sh:conforms true .\n",
                ),
            }
        }

        pub fn render_report(
            report: &Report,
            _store: &oxigraph::store::Store,
            follow_bnodes: bool,
        ) -> String {
            if follow_bnodes {
                return report.report_turtle_follow_bnodes.clone();
            }
            report.report_turtle.clone()
        }
    };
    render_tokens_as_module(tokens)
}
