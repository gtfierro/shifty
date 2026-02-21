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

            if enable_inference {
                if let Err(err) = run_generated_inference(store, &data_graph) {
                    eprintln!("srcgen runtime inference error: {err}");
                    return empty_conforming_report();
                }
            }

            if SPECIALIZATION_READY {
                match run_specialized_node_validation(store, &data_graph).and_then(
                    build_report_from_specialized_violations,
                ) {
                    Ok(report) => return report,
                    Err(err) => {
                        eprintln!("srcgen specialized validation fallback: {err}");
                    }
                }
            }

            let validator = match build_runtime_validator(store, &data_graph) {
                Ok(validator) => validator,
                Err(err) => {
                    eprintln!("srcgen runtime error: {err}");
                    return empty_conforming_report();
                }
            };
            let report = validator.validate();
            build_report_from_runtime_validation_report(&report)
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
