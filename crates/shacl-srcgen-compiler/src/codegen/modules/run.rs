use crate::codegen::render_tokens_as_module;
use crate::ir::SrcGenIR;
use proc_macro2::{Literal, TokenStream};
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
    let embedded_shape_ir_bytes = Literal::byte_string(&ir.embedded_shape_ir_bincode);

    let tokens = quote! {
        const SRCGEN_HAS_PLANNED_RUNTIME_FALLBACK: bool = #has_planned_runtime_fallback;
        const SRCGEN_EMBEDDED_SHAPE_IR_BIN: &[u8] = #embedded_shape_ir_bytes;

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

        fn deserialize_embedded_shape_ir() -> Result<shifty::shacl_ir::ShapeIR, String> {
            if SRCGEN_EMBEDDED_SHAPE_IR_BIN.is_empty() {
                return Err("embedded ShapeIR payload is empty".to_string());
            }
            bincode::serde::decode_from_slice::<shifty::shacl_ir::ShapeIR, _>(
                SRCGEN_EMBEDDED_SHAPE_IR_BIN,
                bincode::config::standard(),
            )
            .map(|(shape_ir, _)| shape_ir)
            .map_err(|err| format!("failed to deserialize embedded ShapeIR payload: {err}"))
        }

        fn embedded_shape_ir() -> Result<&'static shifty::shacl_ir::ShapeIR, String> {
            static SHAPE_IR: std::sync::OnceLock<Result<shifty::shacl_ir::ShapeIR, String>> =
                std::sync::OnceLock::new();
            match SHAPE_IR.get_or_init(deserialize_embedded_shape_ir) {
                Ok(shape_ir) => Ok(shape_ir),
                Err(err) => Err(err.clone()),
            }
        }

        fn build_targeted_fallback_shape_ir() -> Result<Option<shifty::shacl_ir::ShapeIR>, String> {
            use shifty::shacl_ir::{ComponentDescriptor, ComponentID, ID, PropShapeID};

            if !SRCGEN_HAS_PLANNED_RUNTIME_FALLBACK {
                return Ok(None);
            }

            let full_shape_ir = embedded_shape_ir()?.clone();

            let mut lowered_node_shape_by_ir: std::collections::HashMap<ID, u64> =
                std::collections::HashMap::new();
            for (shape_id, term) in &full_shape_ir.node_shape_terms {
                let key = term.to_string();
                if let Some(lowered_shape_id) = shape_id_for_iri(&key) {
                    lowered_node_shape_by_ir.insert(*shape_id, lowered_shape_id);
                }
            }

            let mut lowered_property_shape_by_ir: std::collections::HashMap<PropShapeID, u64> =
                std::collections::HashMap::new();
            for (shape_id, term) in &full_shape_ir.property_shape_terms {
                let key = term.to_string();
                if let Some(lowered_shape_id) = shape_id_for_iri(&key) {
                    lowered_property_shape_by_ir.insert(*shape_id, lowered_shape_id);
                }
            }

            let mut full_property_shape_ids: std::collections::HashSet<PropShapeID> =
                std::collections::HashSet::new();
            let mut partial_property_shape_constraints: std::collections::HashMap<
                PropShapeID,
                Vec<ComponentID>,
            > = std::collections::HashMap::new();

            for property_shape in &full_shape_ir.property_shapes {
                let Some(lowered_shape_id) = lowered_property_shape_by_ir.get(&property_shape.id) else {
                    continue;
                };
                if is_full_fallback_shape(*lowered_shape_id) {
                    full_property_shape_ids.insert(property_shape.id);
                    continue;
                }
                let mut fallback_constraints: Vec<ComponentID> = property_shape
                    .constraints
                    .iter()
                    .copied()
                    .filter(|component_id| {
                        is_fallback_component_for_shape(*lowered_shape_id, component_id.0)
                    })
                    .collect();
                fallback_constraints.sort_by_key(|component_id| component_id.0);
                fallback_constraints.dedup();
                if !fallback_constraints.is_empty() {
                    partial_property_shape_constraints.insert(property_shape.id, fallback_constraints);
                }
            }

            let mut full_node_shape_ids: std::collections::HashSet<ID> =
                std::collections::HashSet::new();
            for node_shape in &full_shape_ir.node_shapes {
                let Some(lowered_shape_id) = lowered_node_shape_by_ir.get(&node_shape.id) else {
                    continue;
                };
                if !is_full_fallback_shape(*lowered_shape_id) {
                    continue;
                }
                full_node_shape_ids.insert(node_shape.id);
                for property_shape_id in &node_shape.property_shapes {
                    full_property_shape_ids.insert(*property_shape_id);
                    partial_property_shape_constraints.remove(property_shape_id);
                }
            }

            let mut filtered_property_shapes: Vec<shifty::shacl_ir::PropertyShapeIR> = Vec::new();
            let mut filtered_property_shape_terms: std::collections::HashMap<
                PropShapeID,
                oxigraph::model::Term,
            > = std::collections::HashMap::new();

            for property_shape in &full_shape_ir.property_shapes {
                if full_property_shape_ids.contains(&property_shape.id) {
                    filtered_property_shapes.push(property_shape.clone());
                    if let Some(term) = full_shape_ir.property_shape_terms.get(&property_shape.id) {
                        filtered_property_shape_terms.insert(property_shape.id, term.clone());
                    }
                    continue;
                }
                let Some(constraints) = partial_property_shape_constraints.get(&property_shape.id) else {
                    continue;
                };
                let mut filtered_shape = property_shape.clone();
                filtered_shape.constraints = constraints.clone();
                filtered_property_shapes.push(filtered_shape);
                if let Some(term) = full_shape_ir.property_shape_terms.get(&property_shape.id) {
                    filtered_property_shape_terms.insert(property_shape.id, term.clone());
                }
            }

            let filtered_property_shape_ids: std::collections::HashSet<PropShapeID> = filtered_property_shapes
                .iter()
                .map(|shape| shape.id)
                .collect();

            let mut filtered_node_shapes: Vec<shifty::shacl_ir::NodeShapeIR> = Vec::new();
            let mut filtered_node_shape_terms: std::collections::HashMap<ID, oxigraph::model::Term> =
                std::collections::HashMap::new();
            for node_shape in &full_shape_ir.node_shapes {
                let Some(lowered_shape_id) = lowered_node_shape_by_ir.get(&node_shape.id) else {
                    continue;
                };

                if full_node_shape_ids.contains(&node_shape.id) {
                    filtered_node_shapes.push(node_shape.clone());
                    if let Some(term) = full_shape_ir.node_shape_terms.get(&node_shape.id) {
                        filtered_node_shape_terms.insert(node_shape.id, term.clone());
                    }
                    continue;
                }

                let mut constraints: Vec<ComponentID> = node_shape
                    .constraints
                    .iter()
                    .copied()
                    .filter(|component_id| {
                        is_fallback_component_for_shape(*lowered_shape_id, component_id.0)
                    })
                    .collect();
                for component_id in &node_shape.constraints {
                    let Some(descriptor) = full_shape_ir.components.get(component_id) else {
                        continue;
                    };
                    if let ComponentDescriptor::Property { shape } = descriptor {
                        if filtered_property_shape_ids.contains(shape) {
                            constraints.push(*component_id);
                        }
                    }
                }
                constraints.sort_by_key(|component_id| component_id.0);
                constraints.dedup();

                let mut property_shapes: Vec<PropShapeID> = node_shape
                    .property_shapes
                    .iter()
                    .copied()
                    .filter(|property_shape_id| filtered_property_shape_ids.contains(property_shape_id))
                    .collect();
                property_shapes.sort_by_key(|id| id.0);
                property_shapes.dedup();

                if constraints.is_empty() && property_shapes.is_empty() {
                    continue;
                }

                let mut filtered_shape = node_shape.clone();
                filtered_shape.constraints = constraints;
                filtered_shape.property_shapes = property_shapes;
                filtered_node_shapes.push(filtered_shape);
                if let Some(term) = full_shape_ir.node_shape_terms.get(&node_shape.id) {
                    filtered_node_shape_terms.insert(node_shape.id, term.clone());
                }
            }

            let mut needed_component_ids: std::collections::HashSet<ComponentID> =
                std::collections::HashSet::new();
            for node_shape in &filtered_node_shapes {
                for component_id in &node_shape.constraints {
                    needed_component_ids.insert(*component_id);
                }
            }
            for property_shape in &filtered_property_shapes {
                for component_id in &property_shape.constraints {
                    needed_component_ids.insert(*component_id);
                }
            }

            for component_id in &needed_component_ids {
                let Some(descriptor) = full_shape_ir.components.get(component_id) else {
                    continue;
                };
                if matches!(
                    descriptor,
                    ComponentDescriptor::Node { .. }
                        | ComponentDescriptor::Not { .. }
                        | ComponentDescriptor::And { .. }
                        | ComponentDescriptor::Or { .. }
                        | ComponentDescriptor::Xone { .. }
                        | ComponentDescriptor::QualifiedValueShape { .. }
                ) {
                    return Err(format!(
                        "targeted runtime fallback shape-IR filtering is not yet enabled for shape-dependent component {}",
                        component_id.0
                    ));
                }
            }

            if filtered_node_shapes.is_empty() && filtered_property_shapes.is_empty() {
                return Ok(None);
            }

            let mut filtered_components: std::collections::HashMap<
                ComponentID,
                shifty::shacl_ir::ComponentDescriptor,
            > = std::collections::HashMap::new();
            for component_id in needed_component_ids {
                if let Some(descriptor) = full_shape_ir.components.get(&component_id) {
                    filtered_components.insert(component_id, descriptor.clone());
                }
            }

            let mut filtered = full_shape_ir.clone();
            filtered.node_shapes = filtered_node_shapes;
            filtered.property_shapes = filtered_property_shapes;
            filtered.components = filtered_components;
            filtered.node_shape_terms = filtered_node_shape_terms;
            filtered.property_shape_terms = filtered_property_shape_terms;
            filtered.rules = std::collections::HashMap::new();
            filtered.node_shape_rules = std::collections::HashMap::new();
            filtered.prop_shape_rules = std::collections::HashMap::new();
            Ok(Some(filtered))
        }

        fn cached_targeted_fallback_shape_ir(
        ) -> Result<Option<shifty::shacl_ir::ShapeIR>, String> {
            static CACHED: std::sync::OnceLock<Result<Option<shifty::shacl_ir::ShapeIR>, String>> =
                std::sync::OnceLock::new();
            match CACHED.get_or_init(build_targeted_fallback_shape_ir) {
                Ok(Some(shape_ir)) => Ok(Some(shape_ir.clone())),
                Ok(None) => Ok(None),
                Err(err) => Err(err.clone()),
            }
        }

        fn build_runtime_validator_with_shape_ir(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
            shape_ir_override: Option<&shifty::shacl_ir::ShapeIR>,
        ) -> Result<shifty::Validator, String> {
            let data_quads = collect_graph_quads(store, data_graph)?;

            let mut builder = shifty::ValidatorBuilder::new()
                .with_data_source(shifty::Source::Quads {
                    graph: data_graph.as_str().to_string(),
                    quads: data_quads,
                })
                .with_shapes_data_union(true)
                .with_store_optimization(false)
                .with_do_imports(false);

            if let Some(shape_ir) = shape_ir_override {
                builder = builder.with_shape_ir(shape_ir.clone());
            } else {
                let shape_graph = oxigraph::model::NamedNode::new(SHAPE_GRAPH)
                    .map_err(|err| format!("invalid SHAPE_GRAPH IRI: {err}"))?;
                let shape_quads = collect_graph_quads(store, &shape_graph)?;
                builder = builder.with_shapes_source(shifty::Source::Quads {
                    graph: SHAPE_GRAPH.to_string(),
                    quads: shape_quads,
                });
            }

            builder
                .build()
                .map_err(|err| format!("failed to initialize runtime validator: {err}"))
        }

        fn build_runtime_validator_full(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<shifty::Validator, String> {
            build_runtime_validator_with_shape_ir(store, data_graph, None)
        }

        fn build_runtime_validator(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<shifty::Validator, String> {
            build_runtime_validator_full(store, data_graph)
        }

        fn build_runtime_validator_targeted_fallback(
            store: &oxigraph::store::Store,
            data_graph: &oxigraph::model::NamedNode,
        ) -> Result<shifty::Validator, String> {
            if !SRCGEN_HAS_PLANNED_RUNTIME_FALLBACK {
                return build_runtime_validator_full(store, data_graph);
            }
            let fallback_shape_ir = cached_targeted_fallback_shape_ir()?;
            if let Some(shape_ir) = fallback_shape_ir.as_ref() {
                return build_runtime_validator_with_shape_ir(store, data_graph, Some(shape_ir));
            }
            build_runtime_validator_full(store, data_graph)
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
            strict_full_aot: bool,
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

            if strict_full_aot {
                if SRCGEN_HAS_PLANNED_RUNTIME_FALLBACK {
                    eprintln!(
                        "srcgen full-aot mode enabled: skipping planned runtime fallback dispatch"
                    );
                }
                return match build_report_from_specialized_violations(specialized_violations) {
                    Ok(report) => Some(report),
                    Err(err) => {
                        eprintln!("srcgen specialized report build fallback: {err}");
                        None
                    }
                };
            }

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

            let validator = match build_runtime_validator_targeted_fallback(store, data_graph) {
                Ok(validator) => validator,
                Err(err) => {
                    eprintln!(
                        "srcgen targeted runtime fallback unavailable ({err}); using full runtime fallback"
                    );
                    match build_runtime_validator_full(store, data_graph) {
                        Ok(validator) => validator,
                        Err(full_err) => {
                            eprintln!("srcgen runtime fallback error: {full_err}");
                            record_fallback_dispatch();
                            return None;
                        }
                    }
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

        fn env_full_aot_enabled() -> bool {
            match std::env::var("SHFTY_SRCGEN_FULL_AOT") {
                Ok(value) => match value.to_ascii_lowercase().as_str() {
                    "1" | "true" | "yes" | "on" => true,
                    _ => false,
                },
                Err(_) => false,
            }
        }

        fn env_full_aot_strict_enabled() -> bool {
            match std::env::var("SHFTY_SRCGEN_FULL_AOT_STRICT") {
                Ok(value) => match value.to_ascii_lowercase().as_str() {
                    "1" | "true" | "yes" | "on" => true,
                    _ => false,
                },
                Err(_) => false,
            }
        }

        pub fn run_with_extended_options(
            store: &oxigraph::store::Store,
            data_graph: Option<&oxigraph::model::NamedNode>,
            enable_inference: bool,
            full_aot: bool,
        ) -> Report {
            let full_aot = full_aot || env_full_aot_enabled();
            let strict_full_aot = full_aot && env_full_aot_strict_enabled();
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
                if let Err(err) =
                    run_generated_inference_with_options(store, &data_graph, !full_aot)
                {
                    eprintln!("srcgen runtime inference error: {err}");
                    return empty_conforming_report();
                }
            }

            if full_aot && !strict_full_aot && SRCGEN_HAS_PLANNED_RUNTIME_FALLBACK {
                eprintln!(
                    "srcgen full-aot mode: runtime fallback remains enabled for unsupported validators"
                );
            }

            if let Some(report) = run_hybrid_validation(store, &data_graph, strict_full_aot) {
                return report;
            }

            if strict_full_aot {
                eprintln!(
                    "srcgen full-aot mode could not complete specialized validation; runtime fallback disabled"
                );
                return empty_conforming_report();
            }

            let validator = match build_runtime_validator_full(store, &data_graph) {
                Ok(validator) => validator,
                Err(err) => {
                    eprintln!("srcgen runtime error: {err}");
                    return empty_conforming_report();
                }
            };
            let report = validator.validate();
            build_report_from_runtime_validation_report(&report)
        }

        pub fn run_with_options(
            store: &oxigraph::store::Store,
            data_graph: Option<&oxigraph::model::NamedNode>,
            enable_inference: bool,
        ) -> Report {
            run_with_extended_options(store, data_graph, enable_inference, false)
        }

        pub fn run_with_full_aot(
            store: &oxigraph::store::Store,
            data_graph: Option<&oxigraph::model::NamedNode>,
            enable_inference: bool,
        ) -> Report {
            run_with_extended_options(store, data_graph, enable_inference, true)
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
