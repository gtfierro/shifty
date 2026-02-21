use crate::codegen::render_tokens_as_module;
use crate::ir::SrcGenIR;
use quote::quote;

pub fn generate(ir: &SrcGenIR) -> Result<String, String> {
    let target_shape_count = ir.shapes.len();
    let tokens = quote! {
        pub const GENERATED_TARGET_SHAPES: usize = #target_shape_count;

        pub fn evaluate_all_targets() -> usize {
            GENERATED_TARGET_SHAPES
        }
    };
    render_tokens_as_module(tokens)
}
