use crate::codegen::render_tokens_as_module;
use crate::ir::SrcGenIR;
use quote::quote;

pub fn generate(ir: &SrcGenIR) -> Result<String, String> {
    let node_shapes = ir
        .shapes
        .iter()
        .filter(|shape| matches!(shape.kind, crate::ir::ShapeKind::Node))
        .count();
    let tokens = quote! {
        pub const GENERATED_NODE_VALIDATORS: usize = #node_shapes;

        pub fn validate_node_shapes_fast_path() -> usize {
            // Phase-0 keeps runtime fallback as the validation engine.
            0
        }
    };
    render_tokens_as_module(tokens)
}
