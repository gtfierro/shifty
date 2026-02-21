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
                    result_nodes.push(triple.object.clone());
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
                            focus = Some(triple.object.clone());
                        }
                        SH_VALUE => {
                            value = Some(triple.object.clone());
                        }
                        SH_RESULT_PATH => {
                            path = Some(ResultPath::Term(triple.object.clone()));
                        }
                        SH_SOURCE_SHAPE => {
                            if let oxigraph::model::Term::NamedNode(node) = &triple.object {
                                if let Some(id) = shape_id_for_iri(node.as_str()) {
                                    shape_id = id;
                                }
                            }
                        }
                        SH_SOURCE_COMPONENT => {
                            if let oxigraph::model::Term::NamedNode(node) = &triple.object {
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
