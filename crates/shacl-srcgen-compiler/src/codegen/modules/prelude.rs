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
            SrcGenComponentKind::ExpressionThis => {
                let id = component.id;
                Some(quote! { "http://www.w3.org/ns/shacl#this" => Some(#id), })
            }
            _ => None,
        })
        .collect();
    let source_constraint_arms: Vec<TokenStream> = ir
        .components
        .iter()
        .filter_map(|component| match &component.kind {
            SrcGenComponentKind::Sparql {
                constraint_term, ..
            } => {
                let id = component.id;
                let term_lit = LitStr::new(constraint_term, proc_macro2::Span::call_site());
                Some(quote! { #id => parse_term_key(#term_lit), })
            }
            SrcGenComponentKind::ExpressionThis => {
                let id = component.id;
                Some(quote! { #id => parse_term_key("http://www.w3.org/ns/shacl#this"), })
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
        use std::collections::{BTreeMap, HashMap, VecDeque};
        use std::path::Path;
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
        enum LoweredPropertyPathRuntime {
            NamedNode(&'static str),
            ReverseNamedNode(&'static str),
            ZeroOrOne(Box<LoweredPropertyPathRuntime>),
            Sequence(Vec<LoweredPropertyPathRuntime>),
            Alternative(Vec<LoweredPropertyPathRuntime>),
        }

        #[derive(Debug, Clone)]
        pub struct Violation {
            pub shape_id: u64,
            pub component_id: u64,
            pub focus: Term,
            pub value: Option<Term>,
            pub path: Option<ResultPath>,
        }

        pub fn sparql_row_signature(
            row: &std::collections::HashMap<String, oxigraph::model::Term>,
        ) -> Vec<(String, oxigraph::model::Term)> {
            let mut signature: Vec<(String, oxigraph::model::Term)> =
                row.iter().map(|(key, value)| (key.clone(), value.clone())).collect();
            signature.sort_by(|a, b| a.0.cmp(&b.0));
            signature
        }

        fn resolve_lowered_property_path_runtime(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            focus: &oxigraph::model::Term,
            path: &LoweredPropertyPathRuntime,
        ) -> Result<Vec<oxigraph::model::Term>, String> {
            match path {
                LoweredPropertyPathRuntime::NamedNode(predicate_iri) => {
                    let predicate = oxigraph::model::NamedNode::new(predicate_iri.to_string())
                        .map_err(|err| format!("invalid lowered predicate IRI: {err}"))?;
                    let subject = match focus {
                        oxigraph::model::Term::NamedNode(node) => node.as_ref().into(),
                        oxigraph::model::Term::BlankNode(node) => node.as_ref().into(),
                        _ => return Ok(Vec::new()),
                    };
                    let mut values = Vec::new();
                    for graph in validation_graphs(data_graph)? {
                        for quad in store.quads_for_pattern(
                            Some(subject),
                            Some(predicate.as_ref()),
                            None,
                            Some(oxigraph::model::GraphNameRef::NamedNode(graph.as_ref())),
                        ) {
                            let quad = quad.map_err(|err| {
                                format!("failed to scan lowered outgoing path: {err}")
                            })?;
                            values.push(quad.object);
                        }
                    }
                    Ok(sort_and_dedup_terms(values))
                }
                LoweredPropertyPathRuntime::ReverseNamedNode(predicate_iri) => {
                    let predicate = oxigraph::model::NamedNode::new(predicate_iri.to_string())
                        .map_err(|err| format!("invalid lowered predicate IRI: {err}"))?;
                    let mut values = Vec::new();
                    for graph in validation_graphs(data_graph)? {
                        for quad in store.quads_for_pattern(
                            None,
                            Some(predicate.as_ref()),
                            Some(focus.as_ref()),
                            Some(oxigraph::model::GraphNameRef::NamedNode(graph.as_ref())),
                        ) {
                            let quad = quad.map_err(|err| {
                                format!("failed to scan lowered incoming path: {err}")
                            })?;
                            let subject = match quad.subject {
                                oxigraph::model::NamedOrBlankNodeRef::NamedNode(node) => {
                                    oxigraph::model::Term::NamedNode(node.into_owned())
                                }
                                oxigraph::model::NamedOrBlankNodeRef::BlankNode(node) => {
                                    oxigraph::model::Term::BlankNode(node.into_owned())
                                }
                            };
                            values.push(subject);
                        }
                    }
                    Ok(sort_and_dedup_terms(values))
                }
                LoweredPropertyPathRuntime::ZeroOrOne(inner) => {
                    let mut values = vec![focus.clone()];
                    values.extend(resolve_lowered_property_path_runtime(
                        store,
                        data_graph,
                        focus,
                        inner.as_ref(),
                    )?);
                    Ok(sort_and_dedup_terms(values))
                }
                LoweredPropertyPathRuntime::Sequence(items) => {
                    let mut current = vec![focus.clone()];
                    for item in items {
                        let mut next = Vec::new();
                        for node in &current {
                            next.extend(resolve_lowered_property_path_runtime(
                                store,
                                data_graph,
                                node,
                                item,
                            )?);
                        }
                        current = sort_and_dedup_terms(next);
                        if current.is_empty() {
                            break;
                        }
                    }
                    Ok(current)
                }
                LoweredPropertyPathRuntime::Alternative(items) => {
                    let mut values = Vec::new();
                    for item in items {
                        values.extend(resolve_lowered_property_path_runtime(
                            store,
                            data_graph,
                            focus,
                            item,
                        )?);
                    }
                    Ok(sort_and_dedup_terms(values))
                }
            }
        }

        fn adjacent_predicates_for_anchor(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            anchor: &oxigraph::model::Term,
        ) -> Result<Vec<String>, String> {
            let mut predicates = Vec::new();
            let subject = match anchor {
                oxigraph::model::Term::NamedNode(node) => Some(node.as_ref().into()),
                oxigraph::model::Term::BlankNode(node) => Some(node.as_ref().into()),
                _ => None,
            };
            for graph in validation_graphs(data_graph)? {
                if let Some(subject) = subject {
                    for quad in store.quads_for_pattern(
                        Some(subject),
                        None,
                        None,
                        Some(oxigraph::model::GraphNameRef::NamedNode(graph.as_ref())),
                    ) {
                        let quad = quad.map_err(|err| {
                            format!("failed to scan lowered outgoing adjacency: {err}")
                        })?;
                        predicates.push(quad.predicate.as_str().to_string());
                    }
                }
                for quad in store.quads_for_pattern(
                    None,
                    None,
                    Some(anchor.as_ref()),
                    Some(oxigraph::model::GraphNameRef::NamedNode(graph.as_ref())),
                ) {
                    let quad = quad.map_err(|err| {
                        format!("failed to scan lowered incoming adjacency: {err}")
                    })?;
                    predicates.push(quad.predicate.as_str().to_string());
                }
            }
            predicates.sort();
            predicates.dedup();
            Ok(predicates)
        }

        fn lowered_adjacent_predicate_whitelist_violation(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            focus: &oxigraph::model::Term,
            anchor_path: &LoweredPropertyPathRuntime,
            allowed_predicates: &[&str],
        ) -> Result<bool, String> {
            let anchors = resolve_lowered_property_path_runtime(store, data_graph, focus, anchor_path)?;
            if anchors.is_empty() {
                return Ok(false);
            }
            let allowed: std::collections::HashSet<&str> =
                allowed_predicates.iter().copied().collect();
            for anchor in anchors {
                for predicate in adjacent_predicates_for_anchor(store, data_graph, &anchor)? {
                    if !allowed.contains(predicate.as_str()) {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
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

        fn sort_and_dedup_terms(
            mut terms: Vec<oxigraph::model::Term>,
        ) -> Vec<oxigraph::model::Term> {
            terms.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
            terms.dedup();
            terms
        }

        #[derive(Hash, Eq, PartialEq, Clone)]
        struct LiteralKey {
            lexical: String,
            language: Option<String>,
        }

        impl LiteralKey {
            fn from_literal(lit: &oxigraph::model::Literal) -> Self {
                let lexical = lit.value().to_string();
                let language = lit.language().map(|l| l.to_ascii_lowercase());
                LiteralKey { lexical, language }
            }
        }

        #[derive(Default, Clone)]
        pub struct OriginalValueIndex {
            literals:
                HashMap<Term, HashMap<oxigraph::model::NamedNode, HashMap<LiteralKey, VecDeque<Term>>>>,
        }

        impl OriginalValueIndex {
            pub fn new() -> Self {
                Self::default()
            }

            pub fn from_path(path: &Path) -> Result<Self, String> {
                let mut index = Self::new();
                let extension = path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.to_ascii_lowercase());

                let format = match extension.as_deref() {
                    Some("ttl") | Some("turtle") => Some(oxigraph::io::RdfFormat::Turtle),
                    Some("nt") => Some(oxigraph::io::RdfFormat::NTriples),
                    _ => None,
                };

                let format = match format {
                    Some(f) => f,
                    None => return Ok(index),
                };

                let file = std::fs::File::open(path)
                    .map_err(|err| format!("failed to open {}: {err}", path.display()))?;
                let reader = std::io::BufReader::new(file);
                let parser = oxigraph::io::RdfParser::from_format(format).without_named_graphs();
                for quad in parser.for_reader(reader) {
                    let triple = quad.map_err(|err| {
                        format!("failed to parse RDF from {}: {err}", path.display())
                    })?;
                    index.record_triple(triple.subject, triple.predicate.clone(), triple.object);
                }
                Ok(index)
            }

            fn record_triple(
                &mut self,
                subject: oxigraph::model::NamedOrBlankNode,
                predicate: oxigraph::model::NamedNode,
                object: Term,
            ) {
                if let Term::Literal(lit) = object {
                    let subject_term = Term::from(subject);
                    let object_term = Term::Literal(lit.clone());
                    let entry = self
                        .literals
                        .entry(subject_term)
                        .or_default()
                        .entry(predicate)
                        .or_default()
                        .entry(LiteralKey::from_literal(&lit))
                        .or_default();
                    entry.push_back(object_term);
                }
            }

            pub fn resolve_literal(
                &self,
                subject: &Term,
                predicate: &oxigraph::model::NamedNode,
                candidate: &oxigraph::model::Literal,
            ) -> Option<Term> {
                let key = LiteralKey::from_literal(candidate);
                let candidates = self.literals.get(subject)?.get(predicate)?.get(&key)?;

                if candidates.is_empty() {
                    return None;
                }

                let candidate_term = Term::Literal(candidate.clone());
                if candidates.iter().any(|term| term == &candidate_term) {
                    Some(candidate_term)
                } else {
                    candidates.front().cloned()
                }
            }
        }

        fn original_value_index_storage() -> &'static Mutex<Option<OriginalValueIndex>> {
            static ORIGINAL_INDEX: OnceLock<Mutex<Option<OriginalValueIndex>>> = OnceLock::new();
            ORIGINAL_INDEX.get_or_init(|| Mutex::new(None))
        }

        pub fn load_original_value_index(path: &Path) -> Result<OriginalValueIndex, String> {
            OriginalValueIndex::from_path(path)
        }

        pub fn set_original_value_index(index: Option<OriginalValueIndex>) {
            if let Ok(mut storage) = original_value_index_storage().lock() {
                *storage = index;
            }
        }

        pub fn resolve_original_literal_term(
            subject: &Term,
            predicate: &oxigraph::model::NamedNode,
            candidate: &oxigraph::model::Literal,
        ) -> Option<Term> {
            if let Ok(storage) = original_value_index_storage().lock() {
                storage
                    .as_ref()
                    .and_then(|index| index.resolve_literal(subject, predicate, candidate))
            } else {
                None
            }
        }

        fn parse_term_key(key: &str) -> Option<oxigraph::model::Term> {
            if let Some(blank_id) = key.strip_prefix("_:") {
                return oxigraph::model::BlankNode::new(blank_id)
                    .ok()
                    .map(oxigraph::model::Term::BlankNode);
            }
            if key.starts_with('<') && key.ends_with('>') && key.len() > 2 {
                let iri = &key[1..(key.len() - 1)];
                return oxigraph::model::NamedNode::new(iri)
                    .ok()
                    .map(oxigraph::model::Term::NamedNode);
            }
            oxigraph::model::NamedNode::new(key)
                .ok()
                .map(oxigraph::model::Term::NamedNode)
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

        pub fn source_constraint_term_for_component(component_id: u64) -> Option<oxigraph::model::Term> {
            match component_id {
                #(#source_constraint_arms)*
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
