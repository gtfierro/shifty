use crate::codegen::render_tokens_as_module;
use crate::ir::SrcGenIR;
use quote::quote;

pub fn generate(_ir: &SrcGenIR) -> Result<String, String> {
    let tokens = quote! {
        pub fn path_from_term(term: oxigraph::model::Term) -> ResultPath {
            ResultPath::Term(term)
        }
    };
    render_tokens_as_module(tokens)
}
