use crate::codegen::render_tokens_as_module;
use crate::codegen::templates::FALLBACK_REASON_TEMPLATE;
use crate::ir::SrcGenIR;
use crate::SrcGenBackend;
use proc_macro2::TokenStream;
use quote::quote;
use syn::LitStr;

pub fn generate(ir: &SrcGenIR, backend: SrcGenBackend) -> Result<String, String> {
    let shape_graph = LitStr::new(&ir.meta.shape_graph_iri, proc_macro2::Span::call_site());
    let data_graph = LitStr::new(&ir.meta.data_graph_iri, proc_macro2::Span::call_site());
    let compiler_version = LitStr::new(env!("CARGO_PKG_VERSION"), proc_macro2::Span::call_site());
    let backend_mode = LitStr::new(backend.as_str(), proc_macro2::Span::call_site());
    let fallback_reason = LitStr::new(FALLBACK_REASON_TEMPLATE, proc_macro2::Span::call_site());
    let fallback_count = ir.fallback_annotations.len();

    let shape_arms: Vec<TokenStream> = ir
        .shapes
        .iter()
        .map(|shape| {
            let id = shape.id;
            let iri = LitStr::new(&shape.iri, proc_macro2::Span::call_site());
            quote! { #id => #iri, }
        })
        .collect();

    let shape_reverse_arms: Vec<TokenStream> = ir
        .shapes
        .iter()
        .map(|shape| {
            let id = shape.id;
            let iri = LitStr::new(&shape.iri, proc_macro2::Span::call_site());
            quote! { #iri => Some(#id), }
        })
        .collect();

    let component_arms: Vec<TokenStream> = ir
        .components
        .iter()
        .map(|component| {
            let id = component.id;
            let iri = LitStr::new(&component.iri, proc_macro2::Span::call_site());
            quote! { #id => #iri, }
        })
        .collect();

    // `component_id_for_iri` uses first-match semantics so duplicate component IRIs
    // remain deterministic across rebuilds.
    let mut component_reverse_arms = Vec::new();
    let mut seen_component_iris = std::collections::BTreeSet::new();
    for component in &ir.components {
        if seen_component_iris.insert(component.iri.clone()) {
            let id = component.id;
            let iri = LitStr::new(&component.iri, proc_macro2::Span::call_site());
            component_reverse_arms.push(quote! { #iri => Some(#id), });
        }
    }

    let tokens = quote! {
        use oxigraph::model::Term;

        pub const SHAPE_GRAPH: &str = #shape_graph;
        pub const DATA_GRAPH: &str = #data_graph;
        pub const COMPILER_TRACK: &str = "srcgen";
        pub const BACKEND_MODE: &str = #backend_mode;
        pub const COMPILER_VERSION: &str = #compiler_version;
        pub const SRCGEN_FALLBACK_COMPONENTS: usize = #fallback_count;
        pub const SRCGEN_FALLBACK_REASON: &str = #fallback_reason;

        #[derive(Debug, Clone)]
        pub enum ResultPath {
            Term(Term),
            PathId(u64),
        }

        #[derive(Debug, Clone)]
        pub struct Violation {
            pub shape_id: u64,
            pub component_id: u64,
            pub focus: Term,
            pub value: Option<Term>,
            pub path: Option<ResultPath>,
        }

        #[derive(Debug, Default)]
        pub struct Report {
            pub violations: Vec<Violation>,
            report_turtle: String,
            report_turtle_follow_bnodes: String,
        }

        impl Report {
            pub fn to_turtle(&self, _store: &oxigraph::store::Store) -> String {
                self.report_turtle.clone()
            }
        }

        pub fn shape_iri(shape_id: u64) -> &'static str {
            match shape_id {
                #(#shape_arms)*
                _ => "",
            }
        }

        pub fn shape_id_for_iri(iri: &str) -> Option<u64> {
            match iri {
                #(#shape_reverse_arms)*
                _ => None,
            }
        }

        pub fn component_iri(component_id: u64) -> &'static str {
            match component_id {
                #(#component_arms)*
                _ => "http://www.w3.org/ns/shacl#ConstraintComponent",
            }
        }

        pub fn component_id_for_iri(iri: &str) -> Option<u64> {
            match iri {
                #(#component_reverse_arms)*
                _ => None,
            }
        }

        pub fn generated_backend_is_tables() -> bool {
            BACKEND_MODE == "tables"
        }
    };

    render_tokens_as_module(tokens)
}
