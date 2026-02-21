use crate::codegen::render_tokens_as_module;
use crate::ir::SrcGenIR;
use quote::quote;

pub fn generate(ir: &SrcGenIR) -> Result<String, String> {
    let property_shapes = ir
        .shapes
        .iter()
        .filter(|shape| matches!(shape.kind, crate::ir::ShapeKind::Property))
        .count();
    let tokens = quote! {
        pub const GENERATED_PROPERTY_VALIDATORS: usize = #property_shapes;

        pub fn validate_property_shapes_fast_path() -> usize {
            // Phase-0 keeps runtime fallback as the validation engine.
            0
        }
    };
    render_tokens_as_module(tokens)
}
