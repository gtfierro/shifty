use crate::codegen::render_tokens_as_module;
use crate::ir::SrcGenIR;
use quote::quote;

pub fn generate(ir: &SrcGenIR) -> Result<String, String> {
    let generated_rule_count = ir.meta.rule_count;
    let tokens = quote! {
        pub const GENERATED_INFERENCE_RULES: usize = #generated_rule_count;

        pub fn run_generated_inference(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<usize, String> {
            if GENERATED_INFERENCE_RULES == 0 {
                return Ok(0);
            }

            let validator = build_runtime_validator(store, data_graph)?;
            let config = shifty::InferenceConfig::default();
            let outcome = validator
                .run_inference_with_config(config)
                .map_err(|err| format!("runtime inference failed: {err}"))?;
            let graph_name = oxigraph::model::GraphName::NamedNode(data_graph.clone());
            let mut inserted = 0usize;
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
                inserted += 1;
            }
            Ok(inserted)
        }
    };
    render_tokens_as_module(tokens)
}
