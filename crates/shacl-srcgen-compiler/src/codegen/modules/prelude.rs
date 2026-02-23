use crate::codegen::render_tokens_as_module;
use crate::codegen::templates::FALLBACK_REASON_TEMPLATE;
use crate::ir::{SrcGenComponentKind, SrcGenIR};
use crate::SrcGenBackend;
use proc_macro2::TokenStream;
use quote::quote;
use std::collections::HashMap;
use syn::LitStr;

pub fn generate(ir: &SrcGenIR, backend: SrcGenBackend) -> Result<String, String> {
    let shape_graph = LitStr::new(&ir.meta.shape_graph_iri, proc_macro2::Span::call_site());
    let data_graph = LitStr::new(&ir.meta.data_graph_iri, proc_macro2::Span::call_site());
    let compiler_version = LitStr::new(env!("CARGO_PKG_VERSION"), proc_macro2::Span::call_site());
    let backend_mode = LitStr::new(backend.as_str(), proc_macro2::Span::call_site());
    let fallback_reason = LitStr::new(FALLBACK_REASON_TEMPLATE, proc_macro2::Span::call_site());
    let fallback_count = ir.fallback_annotations.len();
    let specialization_ready = ir.meta.specialization_ready;

    let mut shape_rows: Vec<(u64, &str)> = Vec::new();
    for shape in &ir.node_shapes {
        shape_rows.push((shape.id, shape.iri.as_str()));
    }
    for shape in &ir.property_shapes {
        shape_rows.push((shape.id, shape.iri.as_str()));
    }
    shape_rows.sort_by(|a, b| a.0.cmp(&b.0));

    let shape_arms: Vec<TokenStream> = shape_rows
        .iter()
        .map(|(id, iri)| {
            let iri = LitStr::new(iri, proc_macro2::Span::call_site());
            quote! { #id => #iri, }
        })
        .collect();

    let mut shape_reverse_arms = Vec::new();
    let mut seen_shape_iris = std::collections::BTreeSet::new();
    for (id, iri) in &shape_rows {
        if seen_shape_iris.insert((*iri).to_string()) {
            let iri = LitStr::new(iri, proc_macro2::Span::call_site());
            shape_reverse_arms.push(quote! { #iri => Some(#id), });
        }
    }

    let component_arms: Vec<TokenStream> = ir
        .components
        .iter()
        .map(|component| {
            let id = component.id;
            let iri = LitStr::new(&component.iri, proc_macro2::Span::call_site());
            quote! { #id => #iri, }
        })
        .collect();
    let component_iri_by_id: HashMap<u64, String> = ir
        .components
        .iter()
        .map(|component| (component.id, component.iri.clone()))
        .collect();

    let mut component_reverse_arms = Vec::new();
    let mut seen_component_iris = std::collections::BTreeSet::new();
    for component in &ir.components {
        if seen_component_iris.insert(component.iri.clone()) {
            let id = component.id;
            let iri = LitStr::new(&component.iri, proc_macro2::Span::call_site());
            component_reverse_arms.push(quote! { #iri => Some(#id), });
        }
    }

    let sparql_constraint_reverse_arms: Vec<TokenStream> = ir
        .components
        .iter()
        .filter_map(|component| match &component.kind {
            SrcGenComponentKind::Sparql {
                constraint_term, ..
            } => {
                let id = component.id;
                let term_lit = LitStr::new(constraint_term, proc_macro2::Span::call_site());
                Some(quote! { #term_lit => Some(#id), })
            }
            _ => None,
        })
        .collect();

    let mut shape_component_arms: Vec<TokenStream> = Vec::new();
    for shape in &ir.node_shapes {
        let mut by_iri: HashMap<&str, Vec<u64>> = HashMap::new();
        for component_id in &shape.constraints {
            if let Some(iri) = component_iri_by_id.get(component_id) {
                by_iri.entry(iri.as_str()).or_default().push(*component_id);
            }
        }
        for (iri, ids) in by_iri {
            if ids.len() == 1 {
                let iri_lit = LitStr::new(iri, proc_macro2::Span::call_site());
                let id = ids[0];
                let shape_id = shape.id;
                shape_component_arms.push(quote! { (#shape_id, #iri_lit) => Some(#id), });
            }
        }
    }
    for shape in &ir.property_shapes {
        let mut by_iri: HashMap<&str, Vec<u64>> = HashMap::new();
        for component_id in &shape.constraints {
            if let Some(iri) = component_iri_by_id.get(component_id) {
                by_iri.entry(iri.as_str()).or_default().push(*component_id);
            }
        }
        for (iri, ids) in by_iri {
            if ids.len() == 1 {
                let iri_lit = LitStr::new(iri, proc_macro2::Span::call_site());
                let id = ids[0];
                let shape_id = shape.id;
                shape_component_arms.push(quote! { (#shape_id, #iri_lit) => Some(#id), });
            }
        }
    }

    let tokens = quote! {
        use oxigraph::model::Term;
        use std::collections::BTreeMap;
        use std::sync::{Mutex, OnceLock};

        pub const SHAPE_GRAPH: &str = #shape_graph;
        pub const DATA_GRAPH: &str = #data_graph;
        pub const COMPILER_TRACK: &str = "srcgen";
        pub const BACKEND_MODE: &str = #backend_mode;
        pub const COMPILER_VERSION: &str = #compiler_version;
        pub const SPECIALIZATION_READY: bool = #specialization_ready;
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

        #[derive(Debug, Clone, Default)]
        pub struct RuntimeMetricsSnapshot {
            pub fast_path_hits: u64,
            pub fallback_dispatches: u64,
            pub per_component_violations: BTreeMap<u64, u64>,
        }

        #[derive(Debug, Default)]
        struct RuntimeMetrics {
            fast_path_hits: u64,
            fallback_dispatches: u64,
            per_component_violations: BTreeMap<u64, u64>,
        }

        fn runtime_metrics() -> &'static Mutex<RuntimeMetrics> {
            static METRICS: OnceLock<Mutex<RuntimeMetrics>> = OnceLock::new();
            METRICS.get_or_init(|| Mutex::new(RuntimeMetrics::default()))
        }

        pub fn reset_runtime_metrics() {
            if let Ok(mut metrics) = runtime_metrics().lock() {
                *metrics = RuntimeMetrics::default();
            }
        }

        pub fn record_fast_path_hit() {
            if let Ok(mut metrics) = runtime_metrics().lock() {
                metrics.fast_path_hits = metrics.fast_path_hits.saturating_add(1);
            }
        }

        pub fn record_fallback_dispatch() {
            if let Ok(mut metrics) = runtime_metrics().lock() {
                metrics.fallback_dispatches = metrics.fallback_dispatches.saturating_add(1);
            }
        }

        pub fn record_component_violation(component_id: u64) {
            if let Ok(mut metrics) = runtime_metrics().lock() {
                let counter = metrics
                    .per_component_violations
                    .entry(component_id)
                    .or_insert(0);
                *counter = counter.saturating_add(1);
            }
        }

        pub fn runtime_metrics_snapshot() -> RuntimeMetricsSnapshot {
            if let Ok(metrics) = runtime_metrics().lock() {
                RuntimeMetricsSnapshot {
                    fast_path_hits: metrics.fast_path_hits,
                    fallback_dispatches: metrics.fallback_dispatches,
                    per_component_violations: metrics.per_component_violations.clone(),
                }
            } else {
                RuntimeMetricsSnapshot::default()
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

        pub fn component_id_for_source_constraint(term: &oxigraph::model::Term) -> Option<u64> {
            let key = term.to_string();
            match key.as_str() {
                #(#sparql_constraint_reverse_arms)*
                _ => None,
            }
        }

        pub fn component_id_for_shape_and_iri(shape_id: u64, component_iri: &str) -> Option<u64> {
            match (shape_id, component_iri) {
                #(#shape_component_arms)*
                _ => None,
            }
        }

        pub fn generated_backend_is_tables() -> bool {
            BACKEND_MODE == "tables"
        }
    };

    render_tokens_as_module(tokens)
}
