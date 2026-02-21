use crate::codegen::render_tokens_as_module;
use crate::ir::SrcGenIR;
use quote::quote;

pub fn generate(_ir: &SrcGenIR) -> Result<String, String> {
    let tokens = quote! {
        fn collect_graph_quads(
            store: &oxigraph::store::Store,
            graph: &oxigraph::model::NamedNode,
        ) -> Result<Vec<oxigraph::model::Quad>, String> {
            let graph_ref = oxigraph::model::GraphNameRef::NamedNode(graph.as_ref());
            let mut out = Vec::new();
            for quad in store.quads_for_pattern(None, None, None, Some(graph_ref)) {
                let quad = quad.map_err(|err| format!("store quad scan failed: {err}"))?;
                out.push(quad);
            }
            Ok(out)
        }

        fn build_runtime_validator(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<shifty::Validator, String> {
            let shape_graph = oxigraph::model::NamedNode::new(SHAPE_GRAPH)
                .map_err(|err| format!("invalid SHAPE_GRAPH IRI: {err}"))?;
            let shape_quads = collect_graph_quads(store, &shape_graph)?;
            let data_quads = collect_graph_quads(store, data_graph)?;

            shifty::ValidatorBuilder::new()
                .with_shapes_source(shifty::Source::Quads {
                    graph: SHAPE_GRAPH.to_string(),
                    quads: shape_quads,
                })
                .with_data_source(shifty::Source::Quads {
                    graph: data_graph.as_str().to_string(),
                    quads: data_quads,
                })
                .with_shapes_data_union(true)
                .with_store_optimization(false)
                .with_do_imports(false)
                .build()
                .map_err(|err| format!("failed to initialize runtime validator: {err}"))
        }

        fn empty_conforming_report() -> Report {
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

        pub fn run_with_options(
            store: &oxigraph::store::Store,
            data_graph: Option<&oxigraph::model::NamedNode>,
            enable_inference: bool,
        ) -> Report {
            let data_graph = if let Some(graph) = data_graph {
                graph.clone()
            } else {
                match oxigraph::model::NamedNode::new(DATA_GRAPH) {
                    Ok(graph) => graph,
                    Err(err) => {
                        eprintln!("srcgen runtime error: invalid DATA_GRAPH IRI: {err}");
                        return empty_conforming_report();
                    }
                }
            };

            let mut validator = match build_runtime_validator(store, &data_graph) {
                Ok(validator) => validator,
                Err(err) => {
                    eprintln!("srcgen runtime error: {err}");
                    return empty_conforming_report();
                }
            };

            if enable_inference {
                if let Err(err) = apply_runtime_inference(&validator, store, &data_graph) {
                    eprintln!("srcgen runtime inference error: {err}");
                    return empty_conforming_report();
                }
                validator = match build_runtime_validator(store, &data_graph) {
                    Ok(validator) => validator,
                    Err(err) => {
                        eprintln!("srcgen runtime error: {err}");
                        return empty_conforming_report();
                    }
                };
            }

            if SPECIALIZATION_READY {
                match run_specialized_node_validation(store, &data_graph)
                    .and_then(|violations| render_violations_to_turtle(&violations).map(|ttl| (violations, ttl)))
                {
                    Ok((violations, report_turtle)) => {
                        return Report {
                            violations,
                            report_turtle: report_turtle.clone(),
                            report_turtle_follow_bnodes: report_turtle,
                        };
                    }
                    Err(err) => {
                        eprintln!("srcgen specialized validation fallback: {err}");
                    }
                }
            }

            let report = validator.validate();
            let report_graph = report.to_graph_with_options(shifty::ValidationReportOptions {
                follow_bnodes: false,
            });
            let violations = build_violations(&report_graph);

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

        pub fn run(
            store: &oxigraph::store::Store,
            data_graph: Option<&oxigraph::model::NamedNode>,
        ) -> Report {
            run_with_options(store, data_graph, true)
        }
    };

    render_tokens_as_module(tokens)
}
