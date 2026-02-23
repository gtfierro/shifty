use crate::codegen::render_tokens_as_module;
use crate::ir::SrcGenIR;
use proc_macro2::TokenStream;
use quote::quote;
use std::collections::BTreeSet;

pub fn generate(ir: &SrcGenIR) -> Result<String, String> {
    let full_fallback_node_shape_ids: BTreeSet<u64> = ir
        .node_shapes
        .iter()
        .filter(|shape| !shape.supported)
        .map(|shape| shape.id)
        .collect();

    let unsupported_parent_property_shape_ids: BTreeSet<u64> = ir
        .node_shapes
        .iter()
        .filter(|shape| !shape.supported)
        .flat_map(|shape| shape.property_shapes.iter().copied())
        .collect();

    let mut full_fallback_property_shape_ids: BTreeSet<u64> = ir
        .property_shapes
        .iter()
        .filter(|shape| !shape.supported)
        .map(|shape| shape.id)
        .collect();
    full_fallback_property_shape_ids.extend(unsupported_parent_property_shape_ids);

    let mut full_fallback_shape_ids: BTreeSet<u64> = full_fallback_node_shape_ids;
    full_fallback_shape_ids.extend(full_fallback_property_shape_ids.iter().copied());

    let full_fallback_arms: Vec<TokenStream> = full_fallback_shape_ids
        .iter()
        .map(|shape_id| quote! { #shape_id => true, })
        .collect();

    let fully_fallback_property_shape_ids: BTreeSet<u64> =
        full_fallback_property_shape_ids.iter().copied().collect();
    let mut fallback_component_pairs: BTreeSet<(u64, u64)> = BTreeSet::new();

    for shape in ir.node_shapes.iter().filter(|shape| shape.supported) {
        for component_id in &shape.fallback_constraints {
            fallback_component_pairs.insert((shape.id, *component_id));
        }
    }

    for shape in ir.property_shapes.iter().filter(|shape| shape.supported) {
        if fully_fallback_property_shape_ids.contains(&shape.id) {
            continue;
        }
        for component_id in &shape.fallback_constraints {
            fallback_component_pairs.insert((shape.id, *component_id));
        }
    }

    let fallback_component_arms: Vec<TokenStream> = fallback_component_pairs
        .iter()
        .map(|(shape_id, component_id)| quote! { (#shape_id, #component_id) => true, })
        .collect();

    let has_planned_runtime_fallback =
        !full_fallback_shape_ids.is_empty() || !fallback_component_pairs.is_empty();

    let tokens = quote! {
        const SRCGEN_HAS_PLANNED_RUNTIME_FALLBACK: bool = #has_planned_runtime_fallback;

        fn collect_graph_quads(
            store: &oxigraph::store::Store,
            graph: &oxigraph::model::NamedNode,
        ) -> Result<Vec<oxigraph::model::Quad>, String> {
            let graph_ref = oxigraph::model::GraphNameRef::NamedNode(graph.as_ref());
            let mut out = Vec::new();
            for quad in store.quads_for_pattern(None, None, None, Some(graph_ref)) {
                let quad = quad.map_err(|err| format!("store quad scan failed: {err}"))?;
                out.push(quad);
            }
            Ok(out)
        }

        fn build_runtime_validator(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<shifty::Validator, String> {
            let shape_graph = oxigraph::model::NamedNode::new(SHAPE_GRAPH)
                .map_err(|err| format!("invalid SHAPE_GRAPH IRI: {err}"))?;
            let shape_quads = collect_graph_quads(store, &shape_graph)?;
            let data_quads = collect_graph_quads(store, data_graph)?;

            shifty::ValidatorBuilder::new()
                .with_shapes_source(shifty::Source::Quads {
                    graph: SHAPE_GRAPH.to_string(),
                    quads: shape_quads,
                })
                .with_data_source(shifty::Source::Quads {
                    graph: data_graph.as_str().to_string(),
                    quads: data_quads,
                })
                .with_shapes_data_union(true)
                .with_store_optimization(false)
                .with_do_imports(false)
                .build()
                .map_err(|err| format!("failed to initialize runtime validator: {err}"))
        }

        fn violation_path_key(path: &Option<ResultPath>) -> String {
            match path {
                Some(ResultPath::Term(term)) => format!("term:{}", term),
                Some(ResultPath::PathId(path_id)) => format!("path-id:{}", path_id),
                None => String::new(),
            }
        }

        fn violation_key(violation: &Violation) -> (u64, u64, String, String, String) {
            (
                violation.shape_id,
                violation.component_id,
                violation.focus.to_string(),
                violation
                    .value
                    .as_ref()
                    .map(|term| term.to_string())
                    .unwrap_or_default(),
                violation_path_key(&violation.path),
            )
        }

        fn sort_violations(violations: &mut Vec<Violation>) {
            violations.sort_by(|left, right| violation_key(left).cmp(&violation_key(right)));
        }

        fn is_full_fallback_shape(shape_id: u64) -> bool {
            match shape_id {
                #(#full_fallback_arms)*
                _ => false,
            }
        }

        fn is_fallback_component_for_shape(shape_id: u64, component_id: u64) -> bool {
            match (shape_id, component_id) {
                #(#fallback_component_arms)*
                _ => false,
            }
        }

        fn should_dispatch_runtime_fallback(violation: &Violation) -> bool {
            if violation.shape_id == 0 || violation.component_id == 0 {
                return true;
            }
            if is_full_fallback_shape(violation.shape_id) {
                return true;
            }
            is_fallback_component_for_shape(violation.shape_id, violation.component_id)
        }

        fn merge_specialized_with_runtime(
            specialized_violations: Vec<Violation>,
            runtime_violations: Vec<Violation>,
        ) -> (Vec<Violation>, usize, usize, Vec<String>) {
            let mut specialized_counts: std::collections::BTreeMap<
                (u64, u64, String, String, String),
                usize,
            > = std::collections::BTreeMap::new();
            for violation in &specialized_violations {
                let key = violation_key(violation);
                let count = specialized_counts.entry(key).or_insert(0);
                *count = count.saturating_add(1);
            }

            let mut matched_specialized_counts: std::collections::BTreeMap<
                (u64, u64, String, String, String),
                usize,
            > = std::collections::BTreeMap::new();
            let mut unmatched_runtime: Vec<Violation> = Vec::new();
            for violation in runtime_violations {
                let key = violation_key(&violation);
                let matched = if let Some(count) = specialized_counts.get_mut(&key) {
                    if *count > 0 {
                        *count -= 1;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                if matched {
                    let count = matched_specialized_counts.entry(key).or_insert(0);
                    *count = count.saturating_add(1);
                } else {
                    unmatched_runtime.push(violation);
                }
            }

            let mut specialized_filtered: Vec<Violation> =
                Vec::with_capacity(specialized_violations.len());
            for violation in specialized_violations {
                let key = violation_key(&violation);
                let include = if let Some(count) = matched_specialized_counts.get_mut(&key) {
                    if *count > 0 {
                        *count -= 1;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                if include {
                    specialized_filtered.push(violation);
                }
            }

            let mut fallback_violations: Vec<Violation> = Vec::new();
            let mut parity_guard_violations: Vec<Violation> = Vec::new();
            for violation in unmatched_runtime {
                if should_dispatch_runtime_fallback(&violation) {
                    fallback_violations.push(violation);
                } else {
                    parity_guard_violations.push(violation);
                }
            }

            let fallback_len = fallback_violations.len();
            let parity_guard_len = parity_guard_violations.len();
            let parity_guard_samples: Vec<String> = parity_guard_violations
                .iter()
                .take(3)
                .map(|violation| {
                    let path = violation_path_key(&violation.path);
                    let value = violation
                        .value
                        .as_ref()
                        .map(|term| term.to_string())
                        .unwrap_or_default();
                    format!(
                        "shape={} component={} focus={} value={} path={}",
                        violation.shape_id,
                        violation.component_id,
                        violation.focus,
                        value,
                        path
                    )
                })
                .collect();
            let mut merged = Vec::with_capacity(
                specialized_filtered.len() + fallback_violations.len() + parity_guard_violations.len(),
            );
            merged.extend(specialized_filtered);
            merged.extend(fallback_violations);
            merged.extend(parity_guard_violations);
            sort_violations(&mut merged);

            (merged, fallback_len, parity_guard_len, parity_guard_samples)
        }

        fn run_hybrid_validation(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Option<Report> {
            if generated_backend_is_tables() {
                record_fallback_dispatch();
                return None;
            }

            let specialized_violations = match run_specialized_node_validation(store, data_graph) {
                Ok(violations) => {
                    if GENERATED_NODE_VALIDATORS > 0 {
                        record_fast_path_hit();
                    }
                    violations
                }
                Err(err) => {
                    eprintln!("srcgen specialized validation fallback: {err}");
                    record_fallback_dispatch();
                    return None;
                }
            };

            if !SRCGEN_HAS_PLANNED_RUNTIME_FALLBACK {
                return match build_report_from_specialized_violations(specialized_violations) {
                    Ok(report) => Some(report),
                    Err(err) => {
                        eprintln!("srcgen specialized report build fallback: {err}");
                        record_fallback_dispatch();
                        None
                    }
                };
            }

            let validator = match build_runtime_validator(store, data_graph) {
                Ok(validator) => validator,
                Err(err) => {
                    eprintln!("srcgen runtime fallback error: {err}");
                    record_fallback_dispatch();
                    return None;
                }
            };
            let runtime_report = validator.validate();
            let runtime_graph = runtime_report.to_graph_with_options(shifty::ValidationReportOptions {
                follow_bnodes: false,
            });
            let runtime_violations = build_violations(&runtime_graph);
            let (merged_violations, fallback_count, parity_guard_count, parity_guard_samples) =
                merge_specialized_with_runtime(specialized_violations, runtime_violations);
            record_fallback_dispatch();

            if parity_guard_count > 0 {
                eprintln!(
                    "srcgen hybrid parity safeguard retained {parity_guard_count} runtime violation(s) outside planned fallback dispatch",
                );
                if std::env::var("SHFTY_SRCGEN_DEBUG_PARITY").is_ok() {
                    for sample in parity_guard_samples {
                        eprintln!("srcgen hybrid parity sample: {sample}");
                    }
                }
            }
            if fallback_count == 0 && SRCGEN_HAS_PLANNED_RUNTIME_FALLBACK {
                eprintln!("srcgen hybrid runtime dispatch executed with zero fallback violations");
            }

            match build_report_from_specialized_violations(merged_violations) {
                Ok(report) => Some(report),
                Err(err) => {
                    eprintln!("srcgen hybrid report build fallback: {err}");
                    Some(build_report_from_runtime_validation_report(&runtime_report))
                }
            }
        }

        pub fn run_with_options(
            store: &oxigraph::store::Store,
            data_graph: Option<&oxigraph::model::NamedNode>,
            enable_inference: bool,
        ) -> Report {
            reset_runtime_metrics();
            reset_runtime_shape_conformance_cache();
            let data_graph = if let Some(graph) = data_graph {
                graph.clone()
            } else {
                match oxigraph::model::NamedNode::new(DATA_GRAPH) {
                    Ok(graph) => graph,
                    Err(err) => {
                        eprintln!("srcgen runtime error: invalid DATA_GRAPH IRI: {err}");
                        return empty_conforming_report();
                    }
                }
            };

            if enable_inference {
                if let Err(err) = run_generated_inference(store, &data_graph) {
                    eprintln!("srcgen runtime inference error: {err}");
                    return empty_conforming_report();
                }
            }

            if let Some(report) = run_hybrid_validation(store, &data_graph) {
                return report;
            }

            let validator = match build_runtime_validator(store, &data_graph) {
                Ok(validator) => validator,
                Err(err) => {
                    eprintln!("srcgen runtime error: {err}");
                    return empty_conforming_report();
                }
            };
            let report = validator.validate();
            build_report_from_runtime_validation_report(&report)
        }

        pub fn run(
            store: &oxigraph::store::Store,
            data_graph: Option<&oxigraph::model::NamedNode>,
        ) -> Report {
            run_with_options(store, data_graph, true)
        }
    };

    render_tokens_as_module(tokens)
}
