use crate::codegen::render_tokens_as_module;
use crate::ir::SrcGenIR;
use quote::quote;

pub fn generate(_ir: &SrcGenIR) -> Result<String, String> {
    let tokens = quote! {
        pub fn apply_runtime_inference(
            validator: &shifty::Validator,
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<(), String> {
            let config = shifty::InferenceConfig::default();
            let outcome = validator
                .run_inference_with_config(config)
                .map_err(|err| format!("runtime inference failed: {err}"))?;
            let graph_name = oxigraph::model::GraphName::NamedNode(data_graph.clone());
            for quad in outcome.inferred_quads {
                let inferred = oxigraph::model::Quad::new(
                    quad.subject,
                    quad.predicate,
                    quad.object,
                    graph_name.clone(),
                );
                store
                    .insert(inferred.as_ref())
                    .map_err(|err| format!("failed to insert inferred quad: {err}"))?;
            }
            Ok(())
        }
    };
    render_tokens_as_module(tokens)
}
