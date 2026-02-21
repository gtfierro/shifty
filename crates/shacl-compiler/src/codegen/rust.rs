use crate::{GeneratedRust, PlanIR};
use oxigraph::model::Term;
use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use syn::{parse2, parse_file, File, Item, LitStr};

const MODULE_ITEM_CHUNK_SIZE: usize = 256;
const MODULE_CHUNK_TRIGGER_ITEMS: usize = MODULE_ITEM_CHUNK_SIZE * 2;
const SHAPE_GRAPH_NT_CHUNK_BYTES: usize = 1_000_000;

pub fn generate(plan: &PlanIR) -> Result<String, String> {
    let modules = generate_modules_impl(plan, false)?;
    Ok(modules.to_single_file())
}

pub fn generate_modules(plan: &PlanIR) -> Result<GeneratedRust, String> {
    generate_modules_impl(plan, true)
}

fn generate_modules_impl(
    plan: &PlanIR,
    chunk_large_modules: bool,
) -> Result<GeneratedRust, String> {
    let plan_view = deserialize_plan_view(plan)?;
    let ported = build_ported_validators(&plan_view)?;
    ensure_ported_validator_coverage(&plan_view, &ported)?;

    let (helper_items, remove_fn_names, remove_const_names) =
        generate_helper_overrides(&plan_view)?;
    let helpers = merge_helpers_module(
        "helpers.rs",
        helpers_template_content(),
        helper_items,
        &remove_fn_names,
        &remove_const_names,
    )?;

    let mut files: Vec<(String, String)> = Vec::new();
    let mut root_modules: Vec<String> = Vec::new();

    push_root_module(
        &mut files,
        &mut root_modules,
        "prelude.rs",
        generate_prelude_module(&plan_view)?,
    );
    push_root_module(&mut files, &mut root_modules, "helpers.rs", helpers);
    push_root_module(
        &mut files,
        &mut root_modules,
        "paths.rs",
        generate_paths_module(&plan_view)?,
    );

    let shape_graph_files = generate_shape_graph_nt_files(plan, chunk_large_modules)?;
    let shape_graph_names: Vec<String> = shape_graph_files
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    push_root_module(
        &mut files,
        &mut root_modules,
        "shape_triples.rs",
        generate_shape_triples_module(&plan_view, &shape_graph_names)?,
    );
    files.extend(shape_graph_files);

    append_items_module(
        &mut files,
        &mut root_modules,
        "targets.rs",
        generate_targets_items(&plan_view)?,
        chunk_large_modules,
    )?;
    append_items_module(
        &mut files,
        &mut root_modules,
        "allowed_predicates.rs",
        generate_allowed_predicates_items(&plan_view)?,
        chunk_large_modules,
    )?;
    append_items_module(
        &mut files,
        &mut root_modules,
        "validators_property.rs",
        ported.property_items,
        chunk_large_modules,
    )?;
    append_items_module(
        &mut files,
        &mut root_modules,
        "validators_node.rs",
        ported.node_items,
        chunk_large_modules,
    )?;
    append_items_module(
        &mut files,
        &mut root_modules,
        "inference.rs",
        generate_inference_items(&plan_view)?,
        chunk_large_modules,
    )?;

    push_root_module(
        &mut files,
        &mut root_modules,
        "run.rs",
        generate_run_module(&plan_view)?,
    );

    let root = build_root_module(&root_modules)?;
    Ok(GeneratedRust { root, files })
}

fn push_root_module(
    files: &mut Vec<(String, String)>,
    root_modules: &mut Vec<String>,
    name: &str,
    content: String,
) {
    root_modules.push(name.to_string());
    files.push((name.to_string(), content));
}

fn append_items_module(
    files: &mut Vec<(String, String)>,
    root_modules: &mut Vec<String>,
    module_name: &str,
    items: Vec<Item>,
    chunk_large_modules: bool,
) -> Result<(), String> {
    let module_files = if chunk_large_modules {
        render_items_module_chunked(
            module_name,
            items,
            MODULE_CHUNK_TRIGGER_ITEMS,
            MODULE_ITEM_CHUNK_SIZE,
        )?
    } else {
        vec![(
            module_name.to_string(),
            render_items_module(module_name, items)?,
        )]
    };
    root_modules.push(module_name.to_string());
    files.extend(module_files);
    Ok(())
}

fn generate_shape_graph_nt_files(
    plan: &PlanIR,
    chunk_large_modules: bool,
) -> Result<Vec<(String, String)>, String> {
    let nt = generate_shape_graph_nt(plan)?;
    if !chunk_large_modules {
        return Ok(vec![("shape_graph.nt".to_string(), nt)]);
    }

    let chunks = split_by_line_boundaries(&nt, SHAPE_GRAPH_NT_CHUNK_BYTES);
    if chunks.len() <= 1 {
        return Ok(vec![("shape_graph.nt".to_string(), nt)]);
    }

    let files = chunks
        .into_iter()
        .enumerate()
        .map(|(idx, chunk)| (format!("shape_graph/chunk_{idx:04}.nt"), chunk))
        .collect();
    Ok(files)
}

fn split_by_line_boundaries(input: &str, max_chunk_bytes: usize) -> Vec<String> {
    if input.is_empty() {
        return vec![String::new()];
    }
    if input.len() <= max_chunk_bytes {
        return vec![input.to_string()];
    }

    let mut out: Vec<String> = Vec::new();
    let mut current = String::new();
    for line in input.split_inclusive('\n') {
        if current.is_empty() {
            if line.len() > max_chunk_bytes {
                out.push(line.to_string());
            } else {
                current.push_str(line);
            }
            continue;
        }

        if current.len() + line.len() > max_chunk_bytes {
            out.push(current);
            current = String::new();
            if line.len() > max_chunk_bytes {
                out.push(line.to_string());
            } else {
                current.push_str(line);
            }
        } else {
            current.push_str(line);
        }
    }

    if !current.is_empty() {
        out.push(current);
    }
    if out.is_empty() {
        out.push(String::new());
    }
    out
}

#[derive(Debug, Deserialize)]
struct PlanView {
    terms: Vec<Term>,
    paths: Vec<Value>,
    components: Vec<PlanComponentView>,
    shapes: Vec<PlanShapeView>,
    #[serde(default)]
    shape_triples: Vec<PlanTripleView>,
    shape_graph: u64,
    order: PlanOrderView,
    #[serde(default)]
    rules: Vec<PlanRuleView>,
    #[serde(default)]
    node_shape_rules: HashMap<u64, Vec<u64>>,
    #[serde(default)]
    property_shape_rules: HashMap<u64, Vec<u64>>,
}

#[derive(Debug, Deserialize)]
struct PlanComponentView {
    id: u64,
    kind: String,
    params: Value,
}

#[derive(Debug, Deserialize)]
struct PlanShapeView {
    id: u64,
    kind: String,
    #[serde(default)]
    targets: Vec<Value>,
    constraints: Vec<u64>,
    path: Option<u64>,
    term: u64,
    #[serde(default)]
    severity: Value,
    #[serde(default)]
    deactivated: bool,
}

#[derive(Debug, Deserialize, Default)]
struct PlanOrderView {
    #[serde(default)]
    node_shapes: Vec<u64>,
    #[serde(default)]
    property_shapes: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct PlanRuleView {
    id: u64,
    kind: Value,
    #[serde(default)]
    conditions: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct PlanTripleView {
    subject: u64,
    predicate: u64,
    object: u64,
}

#[derive(Default)]
struct PortedValidators {
    property_items: Vec<Item>,
    node_items: Vec<Item>,
    remove_property_fn_names: HashSet<String>,
    remove_node_fn_names: HashSet<String>,
}

type HelperOverrides = (Vec<Item>, HashSet<String>, HashSet<String>);

fn deserialize_plan_view(plan: &PlanIR) -> Result<PlanView, String> {
    let json = plan
        .to_json()
        .map_err(|err| format!("failed to serialize plan for migration view: {err}"))?;
    serde_json::from_str::<PlanView>(&json)
        .map_err(|err| format!("failed to deserialize migration view: {err}"))
}

fn build_ported_validators(plan: &PlanView) -> Result<PortedValidators, String> {
    let mut out = PortedValidators::default();
    let component_lookup: HashMap<u64, &PlanComponentView> =
        plan.components.iter().map(|c| (c.id, c)).collect();
    let qualified_siblings = build_qualified_sibling_map(plan, &component_lookup);

    for shape in plan.shapes.iter().filter(|shape| !shape.deactivated) {
        match shape.kind.as_str() {
            "Property" => {
                if !shape.constraints.iter().all(|id| {
                    component_lookup
                        .get(id)
                        .is_some_and(|c| property_component_supported(&c.kind))
                }) {
                    continue;
                }

                let path_id = shape.path.ok_or_else(|| {
                    format!(
                        "property shape {} missing path in migration backend",
                        shape.id
                    )
                })?;
                let path_sparql = path_to_sparql(plan, path_id)?;
                let path_fn_ident = format_ident!("path_{}", path_id);
                let focus_fn_ident =
                    format_ident!("validate_property_shape_{}_for_focus", shape.id);

                let mut pre_lines: Vec<TokenStream> = Vec::new();
                let mut per_value_lines: Vec<TokenStream> = Vec::new();
                let mut post_lines: Vec<TokenStream> = Vec::new();
                let mut needs_count = false;

                for component_id in &shape.constraints {
                    let component = component_lookup.get(component_id).ok_or_else(|| {
                        format!(
                            "property shape {} missing component {} in migration backend",
                            shape.id, component_id
                        )
                    })?;

                    match component.kind.as_str() {
                        "Class" => {
                            let class_id =
                                parse_class_term_id(&component.params).ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected Class params",
                                        shape.id, component.id
                                    )
                                })?;
                            let class_iri = term_iri(plan, class_id)?;
                            let class_lit = LitStr::new(&class_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            per_value_lines.push(quote! {
                                if !has_rdf_type(store, graph, &value, #class_lit) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "Datatype" => {
                            let datatype_id = parse_datatype_term_id(&component.params)
                                .ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected Datatype params",
                                        shape.id, component.id
                                    )
                                })?;
                            let datatype_iri = term_iri(plan, datatype_id)?;
                            let datatype_lit = LitStr::new(&datatype_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            per_value_lines.push(quote! {
                                if !is_literal_with_datatype(&value, #datatype_lit) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "NodeKind" => {
                            let node_kind_id = parse_node_kind_term_id(&component.params)
                                .ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected NodeKind params",
                                        shape.id, component.id
                                    )
                                })?;
                            let node_kind_iri = term_iri(plan, node_kind_id)?;
                            let node_kind_lit = LitStr::new(&node_kind_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            per_value_lines.push(quote! {
                                if !matches_node_kind(&value, #node_kind_lit) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "MinCount" => {
                            let min_count =
                                parse_min_count(&component.params).ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected MinCount params",
                                        shape.id, component.id
                                    )
                                })?;
                            let shape_id = shape.id;
                            let component_id = component.id;
                            needs_count = true;
                            post_lines.push(quote! {
                                if count < #min_count {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        None,
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "MaxCount" => {
                            let max_count =
                                parse_max_count(&component.params).ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected MaxCount params",
                                        shape.id, component.id
                                    )
                                })?;
                            let shape_id = shape.id;
                            let component_id = component.id;
                            needs_count = true;
                            post_lines.push(quote! {
                                if count > #max_count {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        None,
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "MinLength" => {
                            let min = parse_min_length(&component.params).ok_or_else(|| {
                                format!(
                                    "property shape {} component {} expected MinLength params",
                                    shape.id, component.id
                                )
                            })?;
                            let shape_id = shape.id;
                            let component_id = component.id;
                            per_value_lines.push(quote! {
                                if !literal_length_at_least(&value, #min) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "MaxLength" => {
                            let max = parse_max_length(&component.params).ok_or_else(|| {
                                format!(
                                    "property shape {} component {} expected MaxLength params",
                                    shape.id, component.id
                                )
                            })?;
                            let shape_id = shape.id;
                            let component_id = component.id;
                            per_value_lines.push(quote! {
                                if !literal_length_at_most(&value, #max) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "Pattern" => {
                            let (pattern, flags) = parse_pattern_params(&component.params)
                                .ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected Pattern params",
                                        shape.id, component.id
                                    )
                                })?;
                            let regex_ident = format_ident!("regex_{}", component.id);
                            let regex_pattern = LitStr::new(
                                &pattern_with_flags(&pattern, flags.as_deref()),
                                Span::call_site(),
                            );
                            let shape_id = shape.id;
                            let component_id = component.id;
                            pre_lines.push(quote! {
                                let #regex_ident = regex::Regex::new(#regex_pattern).unwrap();
                            });
                            per_value_lines.push(quote! {
                                if !literal_matches_regex(&value, &#regex_ident) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "LanguageIn" => {
                            let langs = parse_language_in(&component.params).ok_or_else(|| {
                                format!(
                                    "property shape {} component {} expected LanguageIn params",
                                    shape.id, component.id
                                )
                            })?;
                            let allowed_ident = format_ident!("allowed_langs_{}", component.id);
                            let lang_lits: Vec<LitStr> = langs
                                .iter()
                                .map(|lang| LitStr::new(lang, Span::call_site()))
                                .collect();
                            let shape_id = shape.id;
                            let component_id = component.id;
                            pre_lines.push(quote! {
                                let #allowed_ident = [#(#lang_lits),*];
                            });
                            per_value_lines.push(quote! {
                                if !language_in_allowed(&value, &#allowed_ident) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "UniqueLang" => {
                            let enabled =
                                parse_unique_lang(&component.params).ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected UniqueLang params",
                                        shape.id, component.id
                                    )
                                })?;
                            if !enabled {
                                continue;
                            }
                            let seen_ident = format_ident!("langs_seen_{}", component.id);
                            let dup_ident = format_ident!("langs_dup_{}", component.id);
                            let shape_id = shape.id;
                            let component_id = component.id;
                            pre_lines.push(quote! {
                                let mut #seen_ident: std::collections::HashSet<String> =
                                    std::collections::HashSet::new();
                            });
                            pre_lines.push(quote! {
                                let mut #dup_ident: std::collections::HashSet<String> =
                                    std::collections::HashSet::new();
                            });
                            per_value_lines.push(quote! {
                                if let Term::Literal(lit) = &value {
                                    if let Some(lang) = lit.language() {
                                        if !lang.is_empty() {
                                            let lower = lang.to_lowercase();
                                            if !#seen_ident.insert(lower.clone()) {
                                                #dup_ident.insert(lower);
                                            }
                                        }
                                    }
                                }
                            });
                            post_lines.push(quote! {
                                for _dup in &#dup_ident {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        None,
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "Equals" => {
                            let prop = parse_property_term_id(&component.params, "Equals")
                                .ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected Equals params",
                                        shape.id, component.id
                                    )
                                })?;
                            let prop_iri = term_iri(plan, prop)?;
                            let prop_lit = LitStr::new(&prop_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            post_lines.push(quote! {
                                let other_values = values_for_predicate(store, graph, focus, #prop_lit);
                                let values_set: std::collections::HashSet<Term> =
                                    values.iter().cloned().collect();
                                let other_set: std::collections::HashSet<Term> =
                                    other_values.into_iter().collect();
                                for value in values_set.difference(&other_set) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                                for value in other_set.difference(&values_set) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "Disjoint" => {
                            let prop = parse_property_term_id(&component.params, "Disjoint")
                                .ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected Disjoint params",
                                        shape.id, component.id
                                    )
                                })?;
                            let prop_iri = term_iri(plan, prop)?;
                            let prop_lit = LitStr::new(&prop_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            post_lines.push(quote! {
                                let other_values = values_for_predicate(store, graph, focus, #prop_lit);
                                let other_set: std::collections::HashSet<Term> =
                                    other_values.into_iter().collect();
                                for value in &values {
                                    if other_set.contains(value) {
                                        report.record(
                                            #shape_id,
                                            #component_id,
                                            focus,
                                            Some(value),
                                            Some(ResultPath::PathId(#path_id)),
                                        );
                                    }
                                }
                            });
                        }
                        "LessThan" => {
                            let prop = parse_property_term_id(&component.params, "LessThan")
                                .ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected LessThan params",
                                        shape.id, component.id
                                    )
                                })?;
                            let prop_iri = term_iri(plan, prop)?;
                            let prop_lit = LitStr::new(&prop_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            post_lines.push(quote! {
                                let other_values = values_for_predicate(store, graph, focus, #prop_lit);
                                for value in &values {
                                    for other in &other_values {
                                        let query =
                                            format!("ASK {{ FILTER(?value < {}) }}", term_to_sparql(other));
                                        if !sparql_any_solution(&query, store, graph, None, None, Some(value)) {
                                            report.record(
                                                #shape_id,
                                                #component_id,
                                                focus,
                                                Some(value),
                                                Some(ResultPath::PathId(#path_id)),
                                            );
                                        }
                                    }
                                }
                            });
                        }
                        "LessThanOrEquals" => {
                            let prop =
                                parse_property_term_id(&component.params, "LessThanOrEquals")
                                    .ok_or_else(|| {
                                        format!(
                                            "property shape {} component {} expected LessThanOrEquals params",
                                            shape.id, component.id
                                        )
                                    })?;
                            let prop_iri = term_iri(plan, prop)?;
                            let prop_lit = LitStr::new(&prop_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            post_lines.push(quote! {
                                let other_values = values_for_predicate(store, graph, focus, #prop_lit);
                                for value in &values {
                                    for other in &other_values {
                                        let query =
                                            format!("ASK {{ FILTER(?value <= {}) }}", term_to_sparql(other));
                                        if !sparql_any_solution(&query, store, graph, None, None, Some(value)) {
                                            report.record(
                                                #shape_id,
                                                #component_id,
                                                focus,
                                                Some(value),
                                                Some(ResultPath::PathId(#path_id)),
                                            );
                                        }
                                    }
                                }
                            });
                        }
                        "HasValue" => {
                            let value_id =
                                parse_has_value_term_id(&component.params).ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected HasValue params",
                                        shape.id, component.id
                                    )
                                })?;
                            let value_expr = term_expr(plan, value_id)?;
                            let shape_id = shape.id;
                            let component_id = component.id;
                            post_lines.push(quote! {
                                if !values.iter().any(|v| v == &#value_expr) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        None,
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "In" => {
                            let values = parse_in_values(&component.params).ok_or_else(|| {
                                format!(
                                    "property shape {} component {} expected In params",
                                    shape.id, component.id
                                )
                            })?;
                            let allowed_ident = format_ident!("allowed_{}", component.id);
                            let value_terms: Vec<TokenStream> = values
                                .iter()
                                .map(|term_id| term_expr(plan, *term_id))
                                .collect::<Result<Vec<_>, _>>()?;
                            let shape_id = shape.id;
                            let component_id = component.id;
                            pre_lines.push(quote! {
                                let #allowed_ident: Vec<Term> = vec![#(#value_terms),*];
                            });
                            per_value_lines.push(quote! {
                                if !#allowed_ident.iter().any(|v| v == &value) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "Node" => {
                            let node_shape = parse_shape_ref(&component.params, "Node")
                                .ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected Node params",
                                        shape.id, component.id
                                    )
                                })?;
                            let node_conforms_ident =
                                format_ident!("node_shape_conforms_{}", node_shape);
                            let shape_id = shape.id;
                            let component_id = component.id;
                            per_value_lines.push(quote! {
                                if !#node_conforms_ident(store, graph, &value) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "Property" => {
                            let prop_shape = parse_shape_ref(&component.params, "Property")
                                .ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected Property params",
                                        shape.id, component.id
                                    )
                                })?;
                            let prop_validate_ident =
                                format_ident!("validate_property_shape_{}_for_focus", prop_shape);
                            per_value_lines.push(quote! {
                                #prop_validate_ident(store, graph, &value, report);
                            });
                        }
                        "Not" => {
                            let not_shape =
                                parse_shape_ref(&component.params, "Not").ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected Not params",
                                        shape.id, component.id
                                    )
                                })?;
                            let not_conforms_ident =
                                format_ident!("node_shape_conforms_{}", not_shape);
                            let shape_id = shape.id;
                            let component_id = component.id;
                            per_value_lines.push(quote! {
                                if #not_conforms_ident(store, graph, &value) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "And" => {
                            let and_shapes =
                                parse_shapes(&component.params, "And").ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected And params",
                                        shape.id, component.id
                                    )
                                })?;
                            let checks: Vec<TokenStream> = and_shapes
                                .iter()
                                .map(|sid| {
                                    let ident = format_ident!("node_shape_conforms_{}", sid);
                                    quote! { #ident(store, graph, &value) }
                                })
                                .collect();
                            let and_expr = if checks.is_empty() {
                                quote! { true }
                            } else {
                                quote! { #(#checks)&&* }
                            };
                            let shape_id = shape.id;
                            let component_id = component.id;
                            per_value_lines.push(quote! {
                                if !(#and_expr) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "Or" => {
                            let or_shapes =
                                parse_shapes(&component.params, "Or").ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected Or params",
                                        shape.id, component.id
                                    )
                                })?;
                            let checks: Vec<TokenStream> = or_shapes
                                .iter()
                                .map(|sid| {
                                    let ident = format_ident!("node_shape_conforms_{}", sid);
                                    quote! { #ident(store, graph, &value) }
                                })
                                .collect();
                            let or_expr = if checks.is_empty() {
                                quote! { false }
                            } else {
                                quote! { #(#checks)||* }
                            };
                            let shape_id = shape.id;
                            let component_id = component.id;
                            per_value_lines.push(quote! {
                                if !(#or_expr) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "Xone" => {
                            let xone_shapes =
                                parse_shapes(&component.params, "Xone").ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected Xone params",
                                        shape.id, component.id
                                    )
                                })?;
                            let sum_terms: Vec<TokenStream> = xone_shapes
                                .iter()
                                .map(|sid| {
                                    let ident = format_ident!("node_shape_conforms_{}", sid);
                                    quote! { (#ident(store, graph, &value) as i32) }
                                })
                                .collect();
                            let sum_expr = if sum_terms.is_empty() {
                                quote! { 0i32 }
                            } else {
                                quote! { #(#sum_terms)+* }
                            };
                            let shape_id = shape.id;
                            let component_id = component.id;
                            per_value_lines.push(quote! {
                                if (#sum_expr) != 1 {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "QualifiedValueShape" => {
                            let qualified = parse_qualified_params(&component.params).ok_or_else(|| {
                                format!(
                                    "property shape {} component {} expected QualifiedValueShape params",
                                    shape.id, component.id
                                )
                            })?;
                            let qualified_ident = format_ident!("qualified_count_{}", component.id);
                            let target_ident =
                                format_ident!("node_shape_conforms_{}", qualified.shape);
                            let shape_id = shape.id;
                            let component_id = component.id;
                            pre_lines.push(quote! {
                                let mut #qualified_ident: u64 = 0;
                            });
                            if qualified.disjoint.unwrap_or(false) {
                                let sibling_shape_ids = qualified_siblings
                                    .get(&component.id)
                                    .cloned()
                                    .unwrap_or_default();
                                let sibling_checks: Vec<TokenStream> = sibling_shape_ids
                                    .iter()
                                    .map(|sid| {
                                        let ident = format_ident!("node_shape_conforms_{}", sid);
                                        quote! { #ident(store, graph, &value) }
                                    })
                                    .collect();
                                let sibling_expr = if sibling_checks.is_empty() {
                                    quote! { false }
                                } else {
                                    quote! { #(#sibling_checks)||* }
                                };
                                per_value_lines.push(quote! {
                                    let conforms_target = #target_ident(store, graph, &value);
                                    let conforms_sibling = #sibling_expr;
                                    if conforms_target && !conforms_sibling {
                                        #qualified_ident += 1;
                                    }
                                });
                            } else {
                                per_value_lines.push(quote! {
                                    let conforms_target = #target_ident(store, graph, &value);
                                    if conforms_target {
                                        #qualified_ident += 1;
                                    }
                                });
                            }
                            if let Some(min) = qualified.min_count {
                                post_lines.push(quote! {
                                    if #qualified_ident < #min {
                                        report.record(
                                            #shape_id,
                                            #component_id,
                                            focus,
                                            None,
                                            Some(ResultPath::PathId(#path_id)),
                                        );
                                    }
                                });
                            }
                            if let Some(max) = qualified.max_count {
                                post_lines.push(quote! {
                                    if #qualified_ident > #max {
                                        report.record(
                                            #shape_id,
                                            #component_id,
                                            focus,
                                            None,
                                            Some(ResultPath::PathId(#path_id)),
                                        );
                                    }
                                });
                            }
                        }
                        "MinExclusive" | "MinInclusive" | "MaxExclusive" | "MaxInclusive" => {
                            let term_id = parse_range_value(&component.params, &component.kind)
                                .ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected range params for {}",
                                        shape.id, component.id, component.kind
                                    )
                                })?;
                            let op = range_operator(&component.kind).ok_or_else(|| {
                                format!(
                                    "property shape {} component {} has unsupported range kind {}",
                                    shape.id, component.id, component.kind
                                )
                            })?;
                            let threshold_expr = term_expr(plan, term_id)?;
                            let shape_id = shape.id;
                            let component_id = component.id;
                            let op_lit = LitStr::new(op, Span::call_site());
                            per_value_lines.push(quote! {
                                let query = format!(
                                    "ASK {{ FILTER(?value {} {}) }}",
                                    #op_lit,
                                    term_to_sparql(&#threshold_expr)
                                );
                                if !sparql_any_solution_lenient(&query, store, graph, None, None, Some(&value)) {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::PathId(#path_id)),
                                    );
                                }
                            });
                        }
                        "Custom" => {
                            let custom =
                                parse_custom_params(&component.params).ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected Custom params",
                                        shape.id, component.id
                                    )
                                })?;
                            let chosen = custom
                                .property_validator
                                .as_ref()
                                .or(custom.validator.as_ref());
                            let Some(chosen) = chosen else {
                                continue;
                            };
                            let bindings_ident = format_ident!("param_bindings_{}", component.id);
                            let query_ident = format_ident!("query_{}", component.id);
                            let query_lit = LitStr::new(&chosen.query, Span::call_site());
                            let prefixes_lit = LitStr::new(&chosen.prefixes, Span::call_site());
                            let path_sparql_lit = LitStr::new(&path_sparql, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            let binding_pushes: Vec<TokenStream> = custom
                                .bindings
                                .iter()
                                .map(|binding| {
                                    let var_name =
                                        LitStr::new(&binding.var_name, Span::call_site());
                                    let value_expr = term_expr(plan, binding.value)?;
                                    Ok(quote! {
                                        #bindings_ident.push((#var_name, #value_expr));
                                    })
                                })
                                .collect::<Result<Vec<_>, String>>()?;

                            pre_lines.push(quote! {
                                let mut #bindings_ident: Vec<(&str, Term)> = Vec::new();
                                #(#binding_pushes)*
                            });
                            pre_lines.push(quote! {
                                let mut #query_ident = String::from(#query_lit);
                                if #query_ident.contains("$PATH") {
                                    #query_ident = #query_ident.replace("$PATH", #path_sparql_lit);
                                }
                            });

                            if chosen.is_ask {
                                per_value_lines.push(quote! {
                                    let conforms = sparql_any_solution_with_bindings(
                                        &#query_ident,
                                        #prefixes_lit,
                                        store,
                                        graph,
                                        Some(focus),
                                        Some(&value),
                                        &#bindings_ident,
                                    );
                                    if !conforms {
                                        report.record(
                                            #shape_id,
                                            #component_id,
                                            focus,
                                            Some(&value),
                                            Some(ResultPath::PathId(#path_id)),
                                        );
                                    }
                                });
                            } else {
                                post_lines.push(quote! {
                                    let solutions = sparql_select_solutions_with_bindings(
                                        &#query_ident,
                                        #prefixes_lit,
                                        store,
                                        graph,
                                        Some(focus),
                                        None,
                                        &#bindings_ident,
                                    );
                                    let mut seen: HashSet<Term> = HashSet::new();
                                    let mut recorded_without_value = false;
                                    for row in solutions {
                                        if let Some(value) = row.get("value").cloned() {
                                            if !seen.insert(value.clone()) {
                                                continue;
                                            }
                                            report.record(
                                                #shape_id,
                                                #component_id,
                                                focus,
                                                Some(&value),
                                                Some(ResultPath::PathId(#path_id)),
                                            );
                                        } else if !recorded_without_value {
                                            recorded_without_value = true;
                                            report.record(
                                                #shape_id,
                                                #component_id,
                                                focus,
                                                None,
                                                Some(ResultPath::PathId(#path_id)),
                                            );
                                        }
                                    }
                                });
                            }
                        }
                        "Sparql" => {
                            let sparql =
                                parse_sparql_params(&component.params).ok_or_else(|| {
                                    format!(
                                        "property shape {} component {} expected Sparql params",
                                        shape.id, component.id
                                    )
                                })?;
                            let query_lit = LitStr::new(&sparql.query, Span::call_site());
                            let path_sparql_lit = LitStr::new(&path_sparql, Span::call_site());
                            let constraint_expr = term_expr(plan, sparql.constraint_node)?;
                            let shape_id = shape.id;
                            let component_id = component.id;
                            per_value_lines.push(quote! {
                                let mut query = String::from(#query_lit);
                                if query.contains("$PATH") {
                                    query = query.replace("$PATH", #path_sparql_lit);
                                }
                                let constraint = #constraint_expr;
                                let prefixes = prefixes_for_selector(store, &constraint);
                                let solutions = sparql_select_solutions_with_bindings(
                                    &query,
                                    &prefixes,
                                    store,
                                    graph,
                                    Some(focus),
                                    None,
                                    &[],
                                );
                                let default_path: Option<ResultPath> = Some(ResultPath::PathId(#path_id));
                                let mut seen: HashSet<(Option<Term>, Option<Term>)> = HashSet::new();
                                for row in solutions {
                                    let value = row.get("value").cloned();
                                    let path_term = row.get("path").cloned();
                                    let key = (value.clone(), path_term.clone());
                                    if !seen.insert(key) {
                                        continue;
                                    }
                                    let path = if let Some(path_term) = path_term {
                                        Some(ResultPath::Term(path_term))
                                    } else {
                                        default_path.clone()
                                    };
                                    report.record(#shape_id, #component_id, focus, value.as_ref(), path);
                                }
                            });
                        }
                        other => {
                            return Err(format!(
                                "unsupported token-native property component kind in migrated shape {}: {}",
                                shape.id, other
                            ));
                        }
                    }
                }

                let canonicalize_stmt =
                    if let Some(predicate_iri) = simple_predicate_iri(plan, path_id)? {
                        let predicate_lit = LitStr::new(&predicate_iri, Span::call_site());
                        Some(quote! {
                            let values = canonicalize_values_for_predicate(
                                store,
                                graph,
                                focus,
                                #predicate_lit,
                                values,
                            );
                        })
                    } else {
                        None
                    };

                let count_stmt = if needs_count {
                    Some(quote! {
                        let count: u64 = values.len() as u64;
                    })
                } else {
                    None
                };

                let per_value_loop = if per_value_lines.is_empty() {
                    None
                } else {
                    Some(quote! {
                        for value in values.iter().cloned() {
                            #(#per_value_lines)*
                        }
                    })
                };

                let noop_stmt = if pre_lines.is_empty()
                    && per_value_lines.is_empty()
                    && post_lines.is_empty()
                    && !needs_count
                {
                    Some(quote! {
                        let _ = values;
                    })
                } else {
                    None
                };

                out.property_items.push(
                    parse2::<Item>(quote! {
                        fn #focus_fn_ident(
                            store: &Store,
                            graph: Option<GraphNameRef<'_>>,
                            focus: &Term,
                            report: &mut Report,
                        ) {
                            let values = #path_fn_ident(store, graph, focus);
                            #canonicalize_stmt
                            #(#pre_lines)*
                            #count_stmt
                            #per_value_loop
                            #(#post_lines)*
                            #noop_stmt
                        }
                    })
                    .map_err(|err| {
                        format!(
                            "failed to build token validator for property shape {}: {}",
                            shape.id, err
                        )
                    })?,
                );

                out.remove_property_fn_names
                    .insert(format!("validate_property_shape_{}_for_focus", shape.id));

                if !shape.targets.is_empty() {
                    let top_fn_ident = format_ident!("validate_property_shape_{}", shape.id);
                    let collect_ident = format_ident!("collect_targets_prop_{}", shape.id);

                    out.property_items.push(
                        parse2::<Item>(quote! {
                            pub fn #top_fn_ident(
                                store: &Store,
                                graph: Option<GraphNameRef<'_>>,
                                report: &mut Report,
                            ) {
                                let targets = #collect_ident(store, graph);
                                for focus in targets {
                                    #focus_fn_ident(store, graph, &focus, report);
                                }
                            }
                        })
                        .map_err(|err| {
                            format!(
                                "failed to build token entry validator for property shape {}: {}",
                                shape.id, err
                            )
                        })?,
                    );
                    out.remove_property_fn_names
                        .insert(format!("validate_property_shape_{}", shape.id));
                }
            }
            "Node" => {
                if !shape.constraints.iter().all(|id| {
                    component_lookup
                        .get(id)
                        .is_some_and(|c| node_component_supported(&c.kind))
                }) {
                    continue;
                }

                let focus_fn_ident = format_ident!("validate_node_shape_{}_for_focus", shape.id);
                let mut body: Vec<TokenStream> = Vec::new();

                for component_id in &shape.constraints {
                    let component = component_lookup.get(component_id).ok_or_else(|| {
                        format!(
                            "node shape {} missing component {} in migration backend",
                            shape.id, component_id
                        )
                    })?;

                    match component.kind.as_str() {
                        "Class" => {
                            let class_id =
                                parse_class_term_id(&component.params).ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected Class params",
                                        shape.id, component.id
                                    )
                                })?;
                            let class_iri = term_iri(plan, class_id)?;
                            let class_lit = LitStr::new(&class_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                if !has_rdf_type(store, graph, focus, #class_lit) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "Datatype" => {
                            let datatype_id = parse_datatype_term_id(&component.params)
                                .ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected Datatype params",
                                        shape.id, component.id
                                    )
                                })?;
                            let datatype_iri = term_iri(plan, datatype_id)?;
                            let datatype_lit = LitStr::new(&datatype_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                if !is_literal_with_datatype(focus, #datatype_lit) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "NodeKind" => {
                            let node_kind_id = parse_node_kind_term_id(&component.params)
                                .ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected NodeKind params",
                                        shape.id, component.id
                                    )
                                })?;
                            let node_kind_iri = term_iri(plan, node_kind_id)?;
                            let node_kind_lit = LitStr::new(&node_kind_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                if !matches_node_kind(focus, #node_kind_lit) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "MinLength" => {
                            let min = parse_min_length(&component.params).ok_or_else(|| {
                                format!(
                                    "node shape {} component {} expected MinLength params",
                                    shape.id, component.id
                                )
                            })?;
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                if !literal_length_at_least(focus, #min) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "MaxLength" => {
                            let max = parse_max_length(&component.params).ok_or_else(|| {
                                format!(
                                    "node shape {} component {} expected MaxLength params",
                                    shape.id, component.id
                                )
                            })?;
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                if !literal_length_at_most(focus, #max) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "Pattern" => {
                            let (pattern, flags) = parse_pattern_params(&component.params)
                                .ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected Pattern params",
                                        shape.id, component.id
                                    )
                                })?;
                            let regex_ident = format_ident!("regex_{}", component.id);
                            let regex_pattern = LitStr::new(
                                &pattern_with_flags(&pattern, flags.as_deref()),
                                Span::call_site(),
                            );
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                let #regex_ident = regex::Regex::new(#regex_pattern).unwrap();
                                if !literal_matches_regex(focus, &#regex_ident) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "LanguageIn" => {
                            let langs = parse_language_in(&component.params).ok_or_else(|| {
                                format!(
                                    "node shape {} component {} expected LanguageIn params",
                                    shape.id, component.id
                                )
                            })?;
                            let allowed_ident = format_ident!("allowed_langs_{}", component.id);
                            let lang_lits: Vec<LitStr> = langs
                                .iter()
                                .map(|lang| LitStr::new(lang, Span::call_site()))
                                .collect();
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                let #allowed_ident = [#(#lang_lits),*];
                                if !language_in_allowed(focus, &#allowed_ident) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "Equals" => {
                            let prop = parse_property_term_id(&component.params, "Equals")
                                .ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected Equals params",
                                        shape.id, component.id
                                    )
                                })?;
                            let prop_iri = term_iri(plan, prop)?;
                            let prop_lit = LitStr::new(&prop_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                let values: Vec<Term> = vec![focus.clone()];
                                let other_values = values_for_predicate(store, graph, focus, #prop_lit);
                                let values_set: std::collections::HashSet<Term> =
                                    values.into_iter().collect();
                                let other_set: std::collections::HashSet<Term> =
                                    other_values.into_iter().collect();
                                for value in values_set.difference(&other_set) {
                                    report.record(#shape_id, #component_id, focus, Some(value), None);
                                }
                                for value in other_set.difference(&values_set) {
                                    report.record(#shape_id, #component_id, focus, Some(value), None);
                                }
                            });
                        }
                        "Disjoint" => {
                            let prop = parse_property_term_id(&component.params, "Disjoint")
                                .ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected Disjoint params",
                                        shape.id, component.id
                                    )
                                })?;
                            let prop_iri = term_iri(plan, prop)?;
                            let prop_lit = LitStr::new(&prop_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                let other_values = values_for_predicate(store, graph, focus, #prop_lit);
                                let other_set: std::collections::HashSet<Term> =
                                    other_values.into_iter().collect();
                                if other_set.contains(focus) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "LessThan" => {
                            let prop = parse_property_term_id(&component.params, "LessThan")
                                .ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected LessThan params",
                                        shape.id, component.id
                                    )
                                })?;
                            let prop_iri = term_iri(plan, prop)?;
                            let prop_lit = LitStr::new(&prop_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                let other_values = values_for_predicate(store, graph, focus, #prop_lit);
                                for other in &other_values {
                                    let query =
                                        format!("ASK {{ FILTER(?value < {}) }}", term_to_sparql(other));
                                    if !sparql_any_solution(&query, store, graph, None, None, Some(focus)) {
                                        report.record(#shape_id, #component_id, focus, Some(focus), None);
                                    }
                                }
                            });
                        }
                        "LessThanOrEquals" => {
                            let prop =
                                parse_property_term_id(&component.params, "LessThanOrEquals")
                                    .ok_or_else(|| {
                                        format!(
                                    "node shape {} component {} expected LessThanOrEquals params",
                                    shape.id, component.id
                                )
                                    })?;
                            let prop_iri = term_iri(plan, prop)?;
                            let prop_lit = LitStr::new(&prop_iri, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                let other_values = values_for_predicate(store, graph, focus, #prop_lit);
                                for other in &other_values {
                                    let query =
                                        format!("ASK {{ FILTER(?value <= {}) }}", term_to_sparql(other));
                                    if !sparql_any_solution(&query, store, graph, None, None, Some(focus)) {
                                        report.record(#shape_id, #component_id, focus, Some(focus), None);
                                    }
                                }
                            });
                        }
                        "Node" => {
                            let node_shape = parse_shape_ref(&component.params, "Node")
                                .ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected Node params",
                                        shape.id, component.id
                                    )
                                })?;
                            let conforms_ident =
                                format_ident!("node_shape_conforms_{}", node_shape);
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                if !#conforms_ident(store, graph, focus) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "Property" => {
                            let prop_shape = parse_shape_ref(&component.params, "Property")
                                .ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected Property params",
                                        shape.id, component.id
                                    )
                                })?;
                            let prop_validate_ident =
                                format_ident!("validate_property_shape_{}_for_focus", prop_shape);
                            body.push(quote! {
                                #prop_validate_ident(store, graph, focus, report);
                            });
                        }
                        "Not" => {
                            let not_shape =
                                parse_shape_ref(&component.params, "Not").ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected Not params",
                                        shape.id, component.id
                                    )
                                })?;
                            let conforms_ident = format_ident!("node_shape_conforms_{}", not_shape);
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                if #conforms_ident(store, graph, focus) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "And" => {
                            let and_shapes =
                                parse_shapes(&component.params, "And").ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected And params",
                                        shape.id, component.id
                                    )
                                })?;
                            let checks: Vec<TokenStream> = and_shapes
                                .iter()
                                .map(|sid| {
                                    let ident = format_ident!("node_shape_conforms_{}", sid);
                                    quote! { #ident(store, graph, focus) }
                                })
                                .collect();
                            let and_expr = if checks.is_empty() {
                                quote! { true }
                            } else {
                                quote! { #(#checks)&&* }
                            };
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                if !(#and_expr) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "Or" => {
                            let or_shapes =
                                parse_shapes(&component.params, "Or").ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected Or params",
                                        shape.id, component.id
                                    )
                                })?;
                            let checks: Vec<TokenStream> = or_shapes
                                .iter()
                                .map(|sid| {
                                    let ident = format_ident!("node_shape_conforms_{}", sid);
                                    quote! { #ident(store, graph, focus) }
                                })
                                .collect();
                            let or_expr = if checks.is_empty() {
                                quote! { false }
                            } else {
                                quote! { #(#checks)||* }
                            };
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                if !(#or_expr) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "Xone" => {
                            let xone_shapes =
                                parse_shapes(&component.params, "Xone").ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected Xone params",
                                        shape.id, component.id
                                    )
                                })?;
                            let sum_terms: Vec<TokenStream> = xone_shapes
                                .iter()
                                .map(|sid| {
                                    let ident = format_ident!("node_shape_conforms_{}", sid);
                                    quote! { (#ident(store, graph, focus) as i32) }
                                })
                                .collect();
                            let sum_expr = if sum_terms.is_empty() {
                                quote! { 0i32 }
                            } else {
                                quote! { #(#sum_terms)+* }
                            };
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                if (#sum_expr) != 1 {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "HasValue" => {
                            let value_id =
                                parse_has_value_term_id(&component.params).ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected HasValue params",
                                        shape.id, component.id
                                    )
                                })?;
                            let value_expr = term_expr(plan, value_id)?;
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                if focus != &#value_expr {
                                    report.record(#shape_id, #component_id, focus, None, None);
                                }
                            });
                        }
                        "In" => {
                            let values = parse_in_values(&component.params).ok_or_else(|| {
                                format!(
                                    "node shape {} component {} expected In params",
                                    shape.id, component.id
                                )
                            })?;
                            let allowed_ident = format_ident!("allowed_{}", component.id);
                            let value_terms: Vec<TokenStream> = values
                                .iter()
                                .map(|term_id| term_expr(plan, *term_id))
                                .collect::<Result<Vec<_>, _>>()?;
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                let #allowed_ident: Vec<Term> = vec![#(#value_terms),*];
                                if !#allowed_ident.iter().any(|v| v == focus) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "MinExclusive" | "MinInclusive" | "MaxExclusive" | "MaxInclusive" => {
                            let term_id = parse_range_value(&component.params, &component.kind)
                                .ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected range params for {}",
                                        shape.id, component.id, component.kind
                                    )
                                })?;
                            let op = range_operator(&component.kind).ok_or_else(|| {
                                format!(
                                    "node shape {} component {} has unsupported range kind {}",
                                    shape.id, component.id, component.kind
                                )
                            })?;
                            let threshold_expr = term_expr(plan, term_id)?;
                            let op_lit = LitStr::new(op, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                let query = format!(
                                    "ASK {{ FILTER(?value {} {}) }}",
                                    #op_lit,
                                    term_to_sparql(&#threshold_expr)
                                );
                                if !sparql_any_solution_lenient(&query, store, graph, None, None, Some(focus)) {
                                    report.record(#shape_id, #component_id, focus, Some(focus), None);
                                }
                            });
                        }
                        "Closed" => {
                            let (closed, ignored) =
                                parse_closed_params_with_plan(plan, &component.params).ok_or_else(
                                    || {
                                        format!(
                                            "node shape {} component {} expected Closed params",
                                            shape.id, component.id
                                        )
                                    },
                                )?;
                            if !closed {
                                continue;
                            }
                            let ignored_lits: Vec<LitStr> = ignored
                                .iter()
                                .map(|iri| LitStr::new(iri, Span::call_site()))
                                .collect();
                            let allowed_ident = format_ident!("allowed_predicates_{}", shape.id);
                            let collect_ident = format_ident!("collect_targets_node_{}", shape.id);
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                let mut allowed: std::collections::HashSet<String> = #allowed_ident();
                                let ignored: Vec<&str> = vec![#(#ignored_lits),*];
                                for ig in ignored {
                                    allowed.insert(ig.to_string());
                                }
                                let cache_key = (#shape_id, graph_cache_key(graph));
                                let violations_for_focus: Vec<(NamedNode, Term)> =
                                    CLOSED_WORLD_VIOLATION_CACHE.with(|cell| {
                                        let mut cache = cell.borrow_mut();
                                        let per_focus = cache.entry(cache_key).or_insert_with(|| {
                                            let targets = #collect_ident(store, graph);
                                            collect_closed_world_violations_for_targets(
                                                store,
                                                graph,
                                                &targets,
                                                &allowed,
                                            )
                                        });
                                        per_focus.get(&focus).cloned().unwrap_or_default()
                                    });
                                for (predicate, value) in violations_for_focus {
                                    report.record(
                                        #shape_id,
                                        #component_id,
                                        focus,
                                        Some(&value),
                                        Some(ResultPath::Term(Term::NamedNode(predicate.clone()))),
                                    );
                                }
                            });
                        }
                        "Custom" => {
                            let custom =
                                parse_custom_params(&component.params).ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected Custom params",
                                        shape.id, component.id
                                    )
                                })?;
                            let chosen =
                                custom.node_validator.as_ref().or(custom.validator.as_ref());
                            let Some(chosen) = chosen else {
                                continue;
                            };
                            let bindings_ident = format_ident!("param_bindings_{}", component.id);
                            let query_ident = format_ident!("query_{}", component.id);
                            let query_lit = LitStr::new(&chosen.query, Span::call_site());
                            let prefixes_lit = LitStr::new(&chosen.prefixes, Span::call_site());
                            let shape_id = shape.id;
                            let component_id = component.id;
                            let binding_pushes: Vec<TokenStream> = custom
                                .bindings
                                .iter()
                                .map(|binding| {
                                    let var_name =
                                        LitStr::new(&binding.var_name, Span::call_site());
                                    let value_expr = term_expr(plan, binding.value)?;
                                    Ok(quote! {
                                        #bindings_ident.push((#var_name, #value_expr));
                                    })
                                })
                                .collect::<Result<Vec<_>, String>>()?;

                            body.push(quote! {
                                let mut #bindings_ident: Vec<(&str, Term)> = Vec::new();
                                #(#binding_pushes)*
                                let mut #query_ident = String::from(#query_lit);
                            });

                            if chosen.is_ask {
                                body.push(quote! {
                                    let conforms = sparql_any_solution_with_bindings(
                                        &#query_ident,
                                        #prefixes_lit,
                                        store,
                                        graph,
                                        Some(focus),
                                        Some(focus),
                                        &#bindings_ident,
                                    );
                                    if !conforms {
                                        report.record(#shape_id, #component_id, focus, Some(focus), None);
                                    }
                                });
                            } else {
                                body.push(quote! {
                                    let solutions = sparql_select_solutions_with_bindings(
                                        &#query_ident,
                                        #prefixes_lit,
                                        store,
                                        graph,
                                        Some(focus),
                                        None,
                                        &#bindings_ident,
                                    );
                                    let mut seen: HashSet<Term> = HashSet::new();
                                    for row in solutions {
                                        let value = row
                                            .get("value")
                                            .cloned()
                                            .unwrap_or_else(|| focus.clone());
                                        if !seen.insert(value.clone()) {
                                            continue;
                                        }
                                        report.record(#shape_id, #component_id, focus, Some(&value), None);
                                    }
                                });
                            }
                        }
                        "Sparql" => {
                            let sparql =
                                parse_sparql_params(&component.params).ok_or_else(|| {
                                    format!(
                                        "node shape {} component {} expected Sparql params",
                                        shape.id, component.id
                                    )
                                })?;
                            let query_lit = LitStr::new(&sparql.query, Span::call_site());
                            let constraint_expr = term_expr(plan, sparql.constraint_node)?;
                            let shape_id = shape.id;
                            let component_id = component.id;
                            body.push(quote! {
                                let mut query = String::from(#query_lit);
                                let constraint = #constraint_expr;
                                let prefixes = prefixes_for_selector(store, &constraint);
                                let solutions = sparql_select_solutions_with_bindings(
                                    &query,
                                    &prefixes,
                                    store,
                                    graph,
                                    Some(focus),
                                    None,
                                    &[],
                                );
                                let default_path: Option<ResultPath> = None;
                                let mut seen: HashSet<(Option<Term>, Option<Term>)> = HashSet::new();
                                for row in solutions {
                                    let value = row
                                        .get("value")
                                        .cloned()
                                        .unwrap_or_else(|| focus.clone());
                                    let path_term = row.get("path").cloned();
                                    let key = (Some(value.clone()), path_term.clone());
                                    if !seen.insert(key) {
                                        continue;
                                    }
                                    let path = if let Some(path_term) = path_term {
                                        Some(ResultPath::Term(path_term))
                                    } else {
                                        default_path.clone()
                                    };
                                    report.record(#shape_id, #component_id, focus, Some(&value), path);
                                }
                            });
                        }
                        other => {
                            return Err(format!(
                                "unsupported token-native node component kind in migrated shape {}: {}",
                                shape.id, other
                            ));
                        }
                    }
                }

                if body.is_empty() {
                    body.push(quote! {
                        let _ = (store, graph, focus, report);
                    });
                }

                out.node_items.push(
                    parse2::<Item>(quote! {
                        fn #focus_fn_ident(
                            store: &Store,
                            graph: Option<GraphNameRef<'_>>,
                            focus: &Term,
                            report: &mut Report,
                        ) {
                            #(#body)*
                        }
                    })
                    .map_err(|err| {
                        format!(
                            "failed to build token validator for node shape {}: {}",
                            shape.id, err
                        )
                    })?,
                );

                out.remove_node_fn_names
                    .insert(format!("validate_node_shape_{}_for_focus", shape.id));

                let conforms_ident = format_ident!("node_shape_conforms_{}", shape.id);
                out.node_items.push(
                    parse2::<Item>(quote! {
                        fn #conforms_ident(store: &Store, graph: Option<GraphNameRef<'_>>, focus: &Term) -> bool {
                            let mut report = Report::default();
                            #focus_fn_ident(store, graph, focus, &mut report);
                            report.violations.is_empty()
                        }
                    })
                    .map_err(|err| {
                        format!(
                            "failed to build token conforms helper for node shape {}: {}",
                            shape.id, err
                        )
                    })?,
                );
                out.remove_node_fn_names
                    .insert(format!("node_shape_conforms_{}", shape.id));

                let top_fn_ident = format_ident!("validate_node_shape_{}", shape.id);
                let collect_ident = format_ident!("collect_targets_node_{}", shape.id);
                out.node_items.push(
                    parse2::<Item>(quote! {
                        pub fn #top_fn_ident(
                            store: &Store,
                            graph: Option<GraphNameRef<'_>>,
                            report: &mut Report,
                        ) {
                            let targets = #collect_ident(store, graph);
                            for focus in targets {
                                #focus_fn_ident(store, graph, &focus, report);
                            }
                        }
                    })
                    .map_err(|err| {
                        format!(
                            "failed to build token entry validator for node shape {}: {}",
                            shape.id, err
                        )
                    })?,
                );
                out.remove_node_fn_names
                    .insert(format!("validate_node_shape_{}", shape.id));
            }
            _ => {}
        }
    }

    Ok(out)
}

fn item_function_names(items: &[Item]) -> HashSet<String> {
    items
        .iter()
        .filter_map(|item| match item {
            Item::Fn(item_fn) => Some(item_fn.sig.ident.to_string()),
            _ => None,
        })
        .collect()
}

fn ensure_ported_validator_coverage(
    plan: &PlanView,
    ported: &PortedValidators,
) -> Result<(), String> {
    let property_names = item_function_names(&ported.property_items);
    let node_names = item_function_names(&ported.node_items);

    let mut missing: Vec<String> = Vec::new();

    for shape in plan.shapes.iter().filter(|shape| !shape.deactivated) {
        match shape.kind.as_str() {
            "Property" => {
                let focus_name = format!("validate_property_shape_{}_for_focus", shape.id);
                if !property_names.contains(&focus_name) {
                    missing.push(focus_name);
                }
                if !shape.targets.is_empty() {
                    let top_name = format!("validate_property_shape_{}", shape.id);
                    if !property_names.contains(&top_name) {
                        missing.push(top_name);
                    }
                }
            }
            "Node" => {
                for name in [
                    format!("validate_node_shape_{}", shape.id),
                    format!("validate_node_shape_{}_for_focus", shape.id),
                    format!("node_shape_conforms_{}", shape.id),
                ] {
                    if !node_names.contains(&name) {
                        missing.push(name);
                    }
                }
            }
            _ => {}
        }
    }

    if missing.is_empty() {
        return Ok(());
    }

    missing.sort();
    missing.dedup();
    let preview = missing
        .iter()
        .take(20)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let suffix = if missing.len() > 20 {
        format!(", ... ({} total)", missing.len())
    } else {
        String::new()
    };

    Err(format!(
        "ported validator coverage incomplete; missing generated functions: {}{}",
        preview, suffix
    ))
}

fn property_component_supported(kind: &str) -> bool {
    matches!(
        kind,
        "Node"
            | "Property"
            | "QualifiedValueShape"
            | "Class"
            | "Datatype"
            | "NodeKind"
            | "MinCount"
            | "MaxCount"
            | "MinExclusive"
            | "MinInclusive"
            | "MaxExclusive"
            | "MaxInclusive"
            | "MinLength"
            | "MaxLength"
            | "Pattern"
            | "LanguageIn"
            | "UniqueLang"
            | "Equals"
            | "Disjoint"
            | "LessThan"
            | "LessThanOrEquals"
            | "Not"
            | "And"
            | "Or"
            | "Xone"
            | "HasValue"
            | "In"
            | "Sparql"
            | "Custom"
    )
}

fn node_component_supported(kind: &str) -> bool {
    matches!(
        kind,
        "Node"
            | "Property"
            | "Class"
            | "Datatype"
            | "NodeKind"
            | "MinExclusive"
            | "MinInclusive"
            | "MaxExclusive"
            | "MaxInclusive"
            | "MinLength"
            | "MaxLength"
            | "Pattern"
            | "LanguageIn"
            | "Equals"
            | "Disjoint"
            | "LessThan"
            | "LessThanOrEquals"
            | "Not"
            | "And"
            | "Or"
            | "Xone"
            | "Closed"
            | "HasValue"
            | "In"
            | "Sparql"
            | "Custom"
    )
}

fn build_qualified_sibling_map(
    plan: &PlanView,
    component_lookup: &HashMap<u64, &PlanComponentView>,
) -> HashMap<u64, Vec<u64>> {
    let mut prop_shapes_by_node: HashMap<u64, Vec<u64>> = HashMap::new();
    for shape in plan.shapes.iter().filter(|s| s.kind == "Node") {
        let mut props = Vec::new();
        for comp_id in &shape.constraints {
            if let Some(component) = component_lookup.get(comp_id) {
                if component.kind == "Property" {
                    if let Some(prop_shape) = parse_shape_ref(&component.params, "Property") {
                        props.push(prop_shape);
                    }
                }
            }
        }
        prop_shapes_by_node.insert(shape.id, props);
    }

    let mut qualified_by_prop: HashMap<u64, Vec<(u64, u64)>> = HashMap::new();
    for shape in plan.shapes.iter().filter(|s| s.kind == "Property") {
        let mut comps = Vec::new();
        for comp_id in &shape.constraints {
            if let Some(component) = component_lookup.get(comp_id) {
                if component.kind == "QualifiedValueShape" {
                    if let Some(qualified) = parse_qualified_params(&component.params) {
                        comps.push((component.id, qualified.shape));
                    }
                }
            }
        }
        if !comps.is_empty() {
            qualified_by_prop.insert(shape.id, comps);
        }
    }

    let mut siblings: HashMap<u64, Vec<u64>> = HashMap::new();
    for (_, props) in prop_shapes_by_node {
        let mut all: Vec<(u64, u64)> = Vec::new();
        for prop_id in props {
            if let Some(list) = qualified_by_prop.get(&prop_id) {
                all.extend(list.iter().cloned());
            }
        }
        for (comp_id, _) in &all {
            let mut sibs = Vec::new();
            for (other_id, shape_id) in &all {
                if other_id == comp_id {
                    continue;
                }
                sibs.push(*shape_id);
            }
            siblings.insert(*comp_id, sibs);
        }
    }
    siblings
}

#[derive(Debug, Clone)]
struct QualifiedParamsView {
    shape: u64,
    min_count: Option<u64>,
    max_count: Option<u64>,
    disjoint: Option<bool>,
}

#[derive(Debug, Clone)]
struct SparqlParamsView {
    query: String,
    constraint_node: u64,
}

#[derive(Debug, Clone)]
struct CustomBindingView {
    var_name: String,
    value: u64,
}

#[derive(Debug, Clone)]
struct CustomValidatorView {
    query: String,
    is_ask: bool,
    prefixes: String,
}

#[derive(Debug, Clone)]
struct CustomParamsView {
    iri: Option<u64>,
    bindings: Vec<CustomBindingView>,
    validator: Option<CustomValidatorView>,
    node_validator: Option<CustomValidatorView>,
    property_validator: Option<CustomValidatorView>,
}

#[derive(Debug, Clone)]
enum PathView {
    Simple(u64),
    Inverse(u64),
    Sequence(Vec<u64>),
    Alternative(Vec<u64>),
    ZeroOrMore(u64),
    OneOrMore(u64),
    ZeroOrOne(u64),
}

fn parse_path_view(value: &Value) -> Result<PathView, String> {
    if let Some(term_id) = value.get("Simple").and_then(Value::as_u64) {
        return Ok(PathView::Simple(term_id));
    }
    if let Some(inner) = value.get("Inverse").and_then(Value::as_u64) {
        return Ok(PathView::Inverse(inner));
    }
    if let Some(items) = value.get("Sequence").and_then(Value::as_array) {
        let mut ids = Vec::with_capacity(items.len());
        for item in items {
            ids.push(
                item.as_u64()
                    .ok_or_else(|| "invalid sequence path id".to_string())?,
            );
        }
        return Ok(PathView::Sequence(ids));
    }
    if let Some(items) = value.get("Alternative").and_then(Value::as_array) {
        let mut ids = Vec::with_capacity(items.len());
        for item in items {
            ids.push(
                item.as_u64()
                    .ok_or_else(|| "invalid alternative path id".to_string())?,
            );
        }
        return Ok(PathView::Alternative(ids));
    }
    if let Some(inner) = value.get("ZeroOrMore").and_then(Value::as_u64) {
        return Ok(PathView::ZeroOrMore(inner));
    }
    if let Some(inner) = value.get("OneOrMore").and_then(Value::as_u64) {
        return Ok(PathView::OneOrMore(inner));
    }
    if let Some(inner) = value.get("ZeroOrOne").and_then(Value::as_u64) {
        return Ok(PathView::ZeroOrOne(inner));
    }
    Err(format!("unsupported path encoding: {}", value))
}

fn path_view_for_id(plan: &PlanView, path_id: u64) -> Result<PathView, String> {
    let value = plan
        .paths
        .get(path_id as usize)
        .ok_or_else(|| format!("path id {} out of bounds", path_id))?;
    parse_path_view(value)
}

fn parse_class_term_id(params: &Value) -> Option<u64> {
    params
        .get("Class")
        .and_then(|v| v.get("class"))
        .and_then(Value::as_u64)
}

fn parse_datatype_term_id(params: &Value) -> Option<u64> {
    params
        .get("Datatype")
        .and_then(|v| v.get("datatype"))
        .and_then(Value::as_u64)
}

fn parse_node_kind_term_id(params: &Value) -> Option<u64> {
    params
        .get("NodeKind")
        .and_then(|v| v.get("node_kind"))
        .and_then(Value::as_u64)
}

fn parse_min_count(params: &Value) -> Option<u64> {
    params
        .get("MinCount")
        .and_then(|v| v.get("min_count"))
        .and_then(Value::as_u64)
}

fn parse_max_count(params: &Value) -> Option<u64> {
    params
        .get("MaxCount")
        .and_then(|v| v.get("max_count"))
        .and_then(Value::as_u64)
}

fn parse_min_length(params: &Value) -> Option<u64> {
    params
        .get("MinLength")
        .and_then(|v| v.get("length"))
        .and_then(Value::as_u64)
}

fn parse_max_length(params: &Value) -> Option<u64> {
    params
        .get("MaxLength")
        .and_then(|v| v.get("length"))
        .and_then(Value::as_u64)
}

fn parse_pattern_params(params: &Value) -> Option<(String, Option<String>)> {
    let pattern = params.get("Pattern")?;
    let value = pattern.get("pattern")?.as_str()?.to_string();
    let flags = pattern
        .get("flags")
        .and_then(Value::as_str)
        .map(str::to_string);
    Some((value, flags))
}

fn pattern_with_flags(pattern: &str, flags: Option<&str>) -> String {
    let mut flag_prefix = String::new();
    if let Some(flags) = flags {
        let mut opts = String::new();
        for ch in flags.chars() {
            match ch {
                'i' | 'm' | 's' | 'x' | 'U' => opts.push(ch),
                _ => {}
            }
        }
        if !opts.is_empty() {
            flag_prefix = format!("(?{})", opts);
        }
    }
    format!("{}{}", flag_prefix, pattern)
}

fn parse_language_in(params: &Value) -> Option<Vec<String>> {
    let langs = params.get("LanguageIn")?.get("languages")?.as_array()?;
    Some(
        langs
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
    )
}

fn parse_unique_lang(params: &Value) -> Option<bool> {
    params
        .get("UniqueLang")
        .and_then(|v| v.get("enabled"))
        .and_then(Value::as_bool)
}

fn parse_property_term_id(params: &Value, variant: &str) -> Option<u64> {
    params
        .get(variant)
        .and_then(|v| v.get("property"))
        .and_then(Value::as_u64)
}

fn parse_shape_ref(params: &Value, variant: &str) -> Option<u64> {
    params
        .get(variant)
        .and_then(|v| v.get("shape"))
        .and_then(Value::as_u64)
}

fn parse_shapes(params: &Value, variant: &str) -> Option<Vec<u64>> {
    let arr = params.get(variant)?.get("shapes")?.as_array()?;
    Some(arr.iter().filter_map(Value::as_u64).collect())
}

fn parse_has_value_term_id(params: &Value) -> Option<u64> {
    params
        .get("HasValue")
        .and_then(|v| v.get("value"))
        .and_then(Value::as_u64)
}

fn parse_in_values(params: &Value) -> Option<Vec<u64>> {
    let arr = params.get("In")?.get("values")?.as_array()?;
    Some(arr.iter().filter_map(Value::as_u64).collect())
}

fn parse_qualified_params(params: &Value) -> Option<QualifiedParamsView> {
    let qualified = params.get("QualifiedValueShape")?;
    Some(QualifiedParamsView {
        shape: qualified.get("shape")?.as_u64()?,
        min_count: qualified.get("min_count").and_then(Value::as_u64),
        max_count: qualified.get("max_count").and_then(Value::as_u64),
        disjoint: qualified.get("disjoint").and_then(Value::as_bool),
    })
}

fn parse_sparql_params(params: &Value) -> Option<SparqlParamsView> {
    let sparql = params.get("Sparql")?;
    Some(SparqlParamsView {
        query: sparql.get("query")?.as_str()?.to_string(),
        constraint_node: sparql.get("constraint_node")?.as_u64()?,
    })
}

fn parse_custom_validator(value: Option<&Value>) -> Option<CustomValidatorView> {
    let value = value?;
    if value.is_null() {
        return None;
    }
    Some(CustomValidatorView {
        query: value.get("query")?.as_str()?.to_string(),
        is_ask: value.get("is_ask")?.as_bool()?,
        prefixes: value
            .get("prefixes")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
    })
}

fn parse_custom_params(params: &Value) -> Option<CustomParamsView> {
    let custom = params.get("Custom")?;
    let bindings = custom
        .get("bindings")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    Some(CustomBindingView {
                        var_name: item.get("var_name")?.as_str()?.to_string(),
                        value: item.get("value")?.as_u64()?,
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Some(CustomParamsView {
        iri: custom.get("iri").and_then(Value::as_u64),
        bindings,
        validator: parse_custom_validator(custom.get("validator")),
        node_validator: parse_custom_validator(custom.get("node_validator")),
        property_validator: parse_custom_validator(custom.get("property_validator")),
    })
}

fn parse_range_value(params: &Value, variant: &str) -> Option<u64> {
    params
        .get(variant)
        .and_then(|v| v.get("value"))
        .and_then(Value::as_u64)
}

fn range_operator(kind: &str) -> Option<&'static str> {
    match kind {
        "MinExclusive" => Some(">"),
        "MinInclusive" => Some(">="),
        "MaxExclusive" => Some("<"),
        "MaxInclusive" => Some("<="),
        _ => None,
    }
}

fn term_expr(plan: &PlanView, term_id: u64) -> Result<TokenStream, String> {
    let term = plan
        .terms
        .get(term_id as usize)
        .ok_or_else(|| format!("term id {} out of bounds", term_id))?;
    match term {
        Term::NamedNode(node) => {
            let iri = LitStr::new(node.as_str(), Span::call_site());
            Ok(quote! {
                Term::NamedNode(NamedNode::new(#iri).unwrap())
            })
        }
        Term::BlankNode(node) => {
            let id = LitStr::new(node.as_str(), Span::call_site());
            Ok(quote! {
                Term::BlankNode(BlankNode::new_unchecked(#id))
            })
        }
        Term::Literal(lit) => {
            let value = LitStr::new(lit.value(), Span::call_site());
            if let Some(lang) = lit.language() {
                let lang_lit = LitStr::new(lang, Span::call_site());
                Ok(quote! {
                    Term::Literal(
                        Literal::new_language_tagged_literal(#value, #lang_lit).unwrap(),
                    )
                })
            } else if lit.datatype().as_str() == "http://www.w3.org/2001/XMLSchema#string" {
                Ok(quote! {
                    Term::Literal(Literal::new_simple_literal(#value))
                })
            } else {
                let dt = LitStr::new(lit.datatype().as_str(), Span::call_site());
                Ok(quote! {
                    Term::Literal(Literal::new_typed_literal(#value, NamedNode::new(#dt).unwrap()))
                })
            }
        }
    }
}

fn path_to_sparql(plan: &PlanView, path_id: u64) -> Result<String, String> {
    path_to_sparql_inner(plan, path_id, false)
}

fn path_to_sparql_inner(plan: &PlanView, path_id: u64, wrap: bool) -> Result<String, String> {
    let path = path_view_for_id(plan, path_id)?;

    let rendered = match &path {
        PathView::Simple(term_id) => term_sparql(plan, *term_id)?,
        PathView::Inverse(inner) => {
            let inner_str = path_to_sparql_inner(plan, *inner, true)?;
            format!("^{}", inner_str)
        }
        PathView::Sequence(paths) => {
            let mut parts = Vec::with_capacity(paths.len());
            for id in paths {
                parts.push(path_to_sparql_inner(plan, *id, true)?);
            }
            parts.join("/")
        }
        PathView::Alternative(paths) => {
            let mut parts = Vec::with_capacity(paths.len());
            for id in paths {
                parts.push(path_to_sparql_inner(plan, *id, true)?);
            }
            parts.join("|")
        }
        PathView::ZeroOrMore(inner) => {
            let inner_str = path_to_sparql_inner(plan, *inner, true)?;
            format!("{}*", inner_str)
        }
        PathView::OneOrMore(inner) => {
            let inner_str = path_to_sparql_inner(plan, *inner, true)?;
            format!("{}+", inner_str)
        }
        PathView::ZeroOrOne(inner) => {
            let inner_str = path_to_sparql_inner(plan, *inner, true)?;
            format!("{}?", inner_str)
        }
    };

    let is_simple = matches!(path, PathView::Simple(_));
    if wrap && !is_simple {
        Ok(format!("({})", rendered))
    } else {
        Ok(rendered)
    }
}

fn term_sparql(plan: &PlanView, term_id: u64) -> Result<String, String> {
    let term = plan
        .terms
        .get(term_id as usize)
        .ok_or_else(|| format!("term id {} out of bounds", term_id))?;
    Ok(term.to_string())
}

fn parse_closed_params_with_plan(plan: &PlanView, params: &Value) -> Option<(bool, Vec<String>)> {
    let closed = params.get("Closed")?;
    let enabled = closed.get("closed")?.as_bool()?;
    let ignored = closed
        .get("ignored_properties")
        .and_then(Value::as_array)
        .map(|arr| arr.iter().filter_map(Value::as_u64).collect::<Vec<_>>())
        .unwrap_or_default();
    let mut out = Vec::with_capacity(ignored.len());
    for term_id in ignored {
        out.push(term_iri(plan, term_id).ok()?);
    }
    Some((enabled, out))
}

fn term_iri(plan: &PlanView, term_id: u64) -> Result<String, String> {
    let term = plan
        .terms
        .get(term_id as usize)
        .ok_or_else(|| format!("term id {} out of bounds", term_id))?;
    match term {
        Term::NamedNode(node) => Ok(node.as_str().to_string()),
        other => Err(format!(
            "expected named node for term id {}, found {:?}",
            term_id, other
        )),
    }
}

fn simple_predicate_iri(plan: &PlanView, path_id: u64) -> Result<Option<String>, String> {
    match path_view_for_id(plan, path_id)? {
        PathView::Simple(term_id) => Ok(Some(term_iri(plan, term_id)?)),
        _ => Ok(None),
    }
}

fn generate_helper_overrides(plan: &PlanView) -> Result<HelperOverrides, String> {
    let items = vec![
        generate_shape_iri_item(plan)?,
        generate_component_iri_item(plan)?,
        generate_component_source_constraint_item(plan)?,
        generate_severity_term_item(plan)?,
        generate_shape_subclass_edges_const_item(plan)?,
    ];

    let remove_fn_names: HashSet<String> = [
        "shape_iri",
        "component_iri",
        "component_source_constraint",
        "severity_term",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();

    let remove_const_names: HashSet<String> = ["SHAPE_SUBCLASS_EDGES"]
        .into_iter()
        .map(str::to_string)
        .collect();

    Ok((items, remove_fn_names, remove_const_names))
}

fn generate_shape_iri_item(plan: &PlanView) -> Result<Item, String> {
    let mut arms: Vec<TokenStream> = Vec::new();
    for shape in &plan.shapes {
        let shape_id = shape.id;
        let iri = term_iri(plan, shape.term)?;
        let iri_lit = LitStr::new(&iri, Span::call_site());
        arms.push(quote! { #shape_id => #iri_lit, });
    }
    parse2::<Item>(quote! {
        pub fn shape_iri(shape_id: u64) -> &'static str {
            match shape_id {
                #(#arms)*
                _ => "",
            }
        }
    })
    .map_err(|err| format!("failed to generate shape_iri helper: {err}"))
}

fn component_constraint_component_iri(kind: &str, params: &Value) -> &'static str {
    match kind {
        "Node" => "http://www.w3.org/ns/shacl#NodeConstraintComponent",
        "Property" => "http://www.w3.org/ns/shacl#PropertyShapeComponent",
        "QualifiedValueShape" => {
            if let Some(qualified) = parse_qualified_params(params) {
                if qualified.min_count.is_some() {
                    "http://www.w3.org/ns/shacl#QualifiedMinCountConstraintComponent"
                } else {
                    "http://www.w3.org/ns/shacl#QualifiedMaxCountConstraintComponent"
                }
            } else {
                "http://www.w3.org/ns/shacl#QualifiedMinCountConstraintComponent"
            }
        }
        "Class" => "http://www.w3.org/ns/shacl#ClassConstraintComponent",
        "Datatype" => "http://www.w3.org/ns/shacl#DatatypeConstraintComponent",
        "NodeKind" => "http://www.w3.org/ns/shacl#NodeKindConstraintComponent",
        "MinCount" => "http://www.w3.org/ns/shacl#MinCountConstraintComponent",
        "MaxCount" => "http://www.w3.org/ns/shacl#MaxCountConstraintComponent",
        "MinExclusive" => "http://www.w3.org/ns/shacl#MinExclusiveConstraintComponent",
        "MinInclusive" => "http://www.w3.org/ns/shacl#MinInclusiveConstraintComponent",
        "MaxExclusive" => "http://www.w3.org/ns/shacl#MaxExclusiveConstraintComponent",
        "MaxInclusive" => "http://www.w3.org/ns/shacl#MaxInclusiveConstraintComponent",
        "MinLength" => "http://www.w3.org/ns/shacl#MinLengthConstraintComponent",
        "MaxLength" => "http://www.w3.org/ns/shacl#MaxLengthConstraintComponent",
        "Pattern" => "http://www.w3.org/ns/shacl#PatternConstraintComponent",
        "LanguageIn" => "http://www.w3.org/ns/shacl#LanguageInConstraintComponent",
        "UniqueLang" => "http://www.w3.org/ns/shacl#UniqueLangConstraintComponent",
        "Equals" => "http://www.w3.org/ns/shacl#EqualsConstraintComponent",
        "Disjoint" => "http://www.w3.org/ns/shacl#DisjointConstraintComponent",
        "LessThan" => "http://www.w3.org/ns/shacl#LessThanConstraintComponent",
        "LessThanOrEquals" => "http://www.w3.org/ns/shacl#LessThanOrEqualsConstraintComponent",
        "Not" => "http://www.w3.org/ns/shacl#NotConstraintComponent",
        "And" => "http://www.w3.org/ns/shacl#AndConstraintComponent",
        "Or" => "http://www.w3.org/ns/shacl#OrConstraintComponent",
        "Xone" => "http://www.w3.org/ns/shacl#XoneConstraintComponent",
        "Closed" => "http://www.w3.org/ns/shacl#ClosedConstraintComponent",
        "HasValue" => "http://www.w3.org/ns/shacl#HasValueConstraintComponent",
        "In" => "http://www.w3.org/ns/shacl#InConstraintComponent",
        "Sparql" => "http://www.w3.org/ns/shacl#SPARQLConstraintComponent",
        "Custom" => "http://www.w3.org/ns/shacl#ConstraintComponent",
        _ => "http://www.w3.org/ns/shacl#ConstraintComponent",
    }
}

fn generate_component_iri_item(plan: &PlanView) -> Result<Item, String> {
    let mut arms: Vec<TokenStream> = Vec::new();
    for component in &plan.components {
        let component_id = component.id;
        let component_iri = if component.kind == "Custom" {
            if let Some(custom) = parse_custom_params(&component.params) {
                if let Some(iri_term_id) = custom.iri {
                    term_iri(plan, iri_term_id).unwrap_or_else(|_| {
                        "http://www.w3.org/ns/shacl#ConstraintComponent".to_string()
                    })
                } else {
                    "http://www.w3.org/ns/shacl#ConstraintComponent".to_string()
                }
            } else {
                "http://www.w3.org/ns/shacl#ConstraintComponent".to_string()
            }
        } else {
            component_constraint_component_iri(&component.kind, &component.params).to_string()
        };
        let iri_lit = LitStr::new(&component_iri, Span::call_site());
        arms.push(quote! { #component_id => #iri_lit, });
    }

    parse2::<Item>(quote! {
        pub fn component_iri(component_id: u64) -> &'static str {
            match component_id {
                #(#arms)*
                _ => "http://www.w3.org/ns/shacl#ConstraintComponent",
            }
        }
    })
    .map_err(|err| format!("failed to generate component_iri helper: {err}"))
}

fn generate_component_source_constraint_item(plan: &PlanView) -> Result<Item, String> {
    let mut arms: Vec<TokenStream> = Vec::new();
    for component in &plan.components {
        if let Some(sparql) = parse_sparql_params(&component.params) {
            let component_id = component.id;
            let constraint_expr = term_expr(plan, sparql.constraint_node)?;
            arms.push(quote! { #component_id => Some(#constraint_expr), });
        }
    }

    parse2::<Item>(quote! {
        fn component_source_constraint(component_id: u64) -> Option<Term> {
            match component_id {
                #(#arms)*
                _ => None,
            }
        }
    })
    .map_err(|err| format!("failed to generate component_source_constraint helper: {err}"))
}

fn severity_iri_from_value(severity: &Value) -> String {
    if let Some(label) = severity.as_str() {
        return match label {
            "Info" => "http://www.w3.org/ns/shacl#Info".to_string(),
            "Warning" => "http://www.w3.org/ns/shacl#Warning".to_string(),
            "Violation" => "http://www.w3.org/ns/shacl#Violation".to_string(),
            _ => "http://www.w3.org/ns/shacl#Violation".to_string(),
        };
    }

    if let Some(custom) = severity.get("Custom") {
        if let Some(custom_iri) = custom.as_str() {
            return custom_iri.to_string();
        }
        if let Some(custom_iri) = custom.get("value").and_then(Value::as_str) {
            return custom_iri.to_string();
        }
    }

    "http://www.w3.org/ns/shacl#Violation".to_string()
}

fn generate_severity_term_item(plan: &PlanView) -> Result<Item, String> {
    let mut arms: Vec<TokenStream> = Vec::new();
    for shape in &plan.shapes {
        let shape_id = shape.id;
        let severity_iri = severity_iri_from_value(&shape.severity);
        let severity_lit = LitStr::new(&severity_iri, Span::call_site());
        arms.push(quote! { #shape_id => #severity_lit, });
    }

    parse2::<Item>(quote! {
        fn severity_term(shape_id: u64) -> &'static str {
            match shape_id {
                #(#arms)*
                _ => "http://www.w3.org/ns/shacl#Violation",
            }
        }
    })
    .map_err(|err| format!("failed to generate severity_term helper: {err}"))
}

fn collect_subclass_edges_from_plan(plan: &PlanView) -> Result<Vec<(String, String)>, String> {
    let mut edges: Vec<(String, String)> = Vec::new();
    for triple in &plan.shape_triples {
        let predicate = plan
            .terms
            .get(triple.predicate as usize)
            .ok_or_else(|| format!("Unknown term id {}", triple.predicate))?;
        let is_subclass = matches!(
            predicate,
            Term::NamedNode(node) if node.as_str() == "http://www.w3.org/2000/01/rdf-schema#subClassOf"
        );
        if !is_subclass {
            continue;
        }

        let subject = match term_iri(plan, triple.subject) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let object = match term_iri(plan, triple.object) {
            Ok(value) => value,
            Err(_) => continue,
        };
        edges.push((subject, object));
    }
    Ok(edges)
}

fn generate_shape_subclass_edges_const_item(plan: &PlanView) -> Result<Item, String> {
    let edges = collect_subclass_edges_from_plan(plan)?;
    let edge_terms: Vec<TokenStream> = edges
        .iter()
        .map(|(sub, sup)| {
            let sub_lit = LitStr::new(sub, Span::call_site());
            let sup_lit = LitStr::new(sup, Span::call_site());
            quote! { (#sub_lit, #sup_lit) }
        })
        .collect();

    parse2::<Item>(quote! {
        const SHAPE_SUBCLASS_EDGES: &[(&str, &str)] = &[
            #(#edge_terms,)*
        ];
    })
    .map_err(|err| format!("failed to generate SHAPE_SUBCLASS_EDGES const: {err}"))
}

fn generate_prelude_module(plan: &PlanView) -> Result<String, String> {
    let shape_graph_iri = term_iri(plan, plan.shape_graph)?;
    let data_graph_iri = format!("{}-compiled-data", shape_graph_iri);
    let shape_graph_lit = LitStr::new(&shape_graph_iri, Span::call_site());
    let data_graph_lit = LitStr::new(&data_graph_iri, Span::call_site());

    let file = parse2::<File>(quote! {
        use oxigraph::model::{
            BlankNode, Graph, GraphName, GraphNameRef, Literal, NamedNode, NamedNodeRef,
            NamedOrBlankNode, NamedOrBlankNodeRef, Quad, Term, TermRef, Triple,
        };
        use oxigraph::model::vocab::{rdf, xsd};
        use oxigraph::io::{RdfFormat, RdfParser, RdfSerializer};
        use oxigraph::store::Store;
        use oxigraph::sparql::{PreparedSparqlQuery, QueryResults, SparqlEvaluator, Variable};
        use std::collections::{HashMap, HashSet, VecDeque};
        use std::sync::{Arc, OnceLock};
        use std::time::{Instant, SystemTime, UNIX_EPOCH};
        use fixedbitset::FixedBitSet;
        use dashmap::DashMap;
        use rayon::prelude::*;
        use regex::Regex;
        use log::info;
        use oxsdatatypes::*;
        use std::str::FromStr;
        use std::fs::File;
        use std::io::BufReader;
        use std::path::Path;

        const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
        const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
        pub const SHAPE_GRAPH: &str = #shape_graph_lit;
        pub const DATA_GRAPH: &str = #data_graph_lit;
        const SHACL_SELECT: &str = "http://www.w3.org/ns/shacl#select";
        const SHACL_PREFIXES: &str = "http://www.w3.org/ns/shacl#prefixes";
        const SHACL_DECLARE: &str = "http://www.w3.org/ns/shacl#declare";
        const SHACL_PREFIX: &str = "http://www.w3.org/ns/shacl#prefix";
        const SHACL_NAMESPACE: &str = "http://www.w3.org/ns/shacl#namespace";

        fn compiled_stage(stage: &str) {
            static START: OnceLock<Instant> = OnceLock::new();
            let start = START.get_or_init(Instant::now);
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            let elapsed = start.elapsed().as_millis();
            eprintln!("[compiled-stage ts_ms={} elapsed_ms={}] {}", now, elapsed, stage);
        }

        #[derive(Debug, Default)]
        pub struct Report {
            pub violations: Vec<Violation>,
        }

        #[derive(Debug)]
        pub struct Violation {
            pub shape_id: u64,
            pub component_id: u64,
            pub focus: Term,
            pub value: Option<Term>,
            pub path: Option<ResultPath>,
        }

        #[derive(Debug, Clone)]
        pub enum ResultPath {
            Term(Term),
            PathId(u64),
        }

        impl Report {
            pub fn record(
                &mut self,
                shape_id: u64,
                component_id: u64,
                focus: &Term,
                value: Option<&Term>,
                path: Option<ResultPath>,
            ) {
                self.violations.push(Violation {
                    shape_id,
                    component_id,
                    focus: focus.clone(),
                    value: value.cloned(),
                    path,
                });
            }

            pub fn merge(&mut self, other: Report) {
                self.violations.extend(other.violations);
            }

            pub fn to_turtle(&self, store: &Store) -> String {
                format_validation_report(self, store)
            }
        }
    })
    .map_err(|err| format!("failed to generate prelude module AST: {err}"))?;

    Ok(prettyplease::unparse(&file))
}

fn generate_paths_module(plan: &PlanView) -> Result<String, String> {
    let mut items: Vec<Item> = Vec::new();

    if plan.paths.is_empty() {
        let item = parse2::<Item>(quote! {
            fn path_term(_path_id: u64, _graph: &mut Graph) -> Term {
                Term::BlankNode(BlankNode::default())
            }
        })
        .map_err(|err| format!("failed to generate empty path_term function: {err}"))?;
        items.push(item);
        let file = File {
            shebang: None,
            attrs: vec![],
            items,
        };
        return Ok(prettyplease::unparse(&file));
    }

    for path_id in 0..plan.paths.len() as u64 {
        items.push(generate_path_eval_function(plan, path_id, false)?);
        items.push(generate_path_eval_function(plan, path_id, true)?);
    }

    let path_term_match_arms: Vec<TokenStream> = (0..plan.paths.len() as u64)
        .map(|path_id| {
            let path_term_ident = format_ident!("path_term_{}", path_id);
            quote! { #path_id => #path_term_ident(graph), }
        })
        .collect();
    let path_term_dispatch = parse2::<Item>(quote! {
        fn path_term(path_id: u64, graph: &mut Graph) -> Term {
            match path_id {
                #(#path_term_match_arms)*
                _ => rdf::NIL.into(),
            }
        }
    })
    .map_err(|err| format!("failed to generate path_term dispatch: {err}"))?;
    items.push(path_term_dispatch);

    let build_rdf_list = parse2::<Item>(quote! {
        fn build_rdf_list(items: Vec<Term>, graph: &mut Graph) -> Term {
            if items.is_empty() {
                return rdf::NIL.into();
            }
            let bnodes: Vec<NamedOrBlankNode> = (0..items.len())
                .map(|_| BlankNode::default().into())
                .collect();
            let head: NamedOrBlankNode = bnodes[0].clone();
            for (idx, item) in items.iter().enumerate() {
                let subject: NamedOrBlankNode = bnodes[idx].clone();
                graph.insert(Triple::new(subject.clone(), rdf::FIRST, item.clone()).as_ref());
                let rest: Term = if idx + 1 == items.len() {
                    rdf::NIL.into()
                } else {
                    bnodes[idx + 1].clone().into()
                };
                graph.insert(Triple::new(subject, rdf::REST, rest).as_ref());
            }
            head.into()
        }
    })
    .map_err(|err| format!("failed to generate build_rdf_list helper: {err}"))?;
    items.push(build_rdf_list);

    for path_id in 0..plan.paths.len() as u64 {
        items.push(generate_path_term_function(plan, path_id)?);
    }

    let file = File {
        shebang: None,
        attrs: vec![],
        items,
    };
    Ok(prettyplease::unparse(&file))
}

fn generate_path_eval_function(
    plan: &PlanView,
    path_id: u64,
    reverse: bool,
) -> Result<Item, String> {
    let fn_ident = if reverse {
        format_ident!("path_{}_rev", path_id)
    } else {
        format_ident!("path_{}", path_id)
    };
    let body = generate_path_eval_body(plan, path_id, reverse)?;
    parse2::<Item>(quote! {
        fn #fn_ident(store: &Store, graph: Option<GraphNameRef<'_>>, start: &Term) -> Vec<Term> {
            #body
        }
    })
    .map_err(|err| format!("failed to generate path evaluator {}: {}", fn_ident, err))
}

fn generate_path_eval_body(
    plan: &PlanView,
    path_id: u64,
    reverse: bool,
) -> Result<TokenStream, String> {
    let path = path_view_for_id(plan, path_id)?;
    match path {
        PathView::Simple(term_id) => {
            let pred_iri = term_iri(plan, term_id)?;
            let pred_lit = LitStr::new(&pred_iri, Span::call_site());
            if reverse {
                Ok(quote! {
                    let predicate = NamedNodeRef::new(#pred_lit).unwrap();
                    let object = term_ref(start);
                    store
                        .quads_for_pattern(None, Some(predicate), Some(object), graph)
                        .filter_map(Result::ok)
                        .map(|q| q.subject.into())
                        .collect()
                })
            } else {
                Ok(quote! {
                    let subject = match subject_ref(start) {
                        Some(subject) => subject,
                        None => return Vec::new(),
                    };
                    let predicate = NamedNodeRef::new(#pred_lit).unwrap();
                    store
                        .quads_for_pattern(Some(subject), Some(predicate), None, graph)
                        .filter_map(Result::ok)
                        .map(|q| q.object)
                        .collect()
                })
            }
        }
        PathView::Inverse(inner) => {
            let fn_ident = if reverse {
                format_ident!("path_{}", inner)
            } else {
                format_ident!("path_{}_rev", inner)
            };
            Ok(quote! { #fn_ident(store, graph, start) })
        }
        PathView::Sequence(paths) => {
            let ordered: Vec<u64> = if reverse {
                paths.into_iter().rev().collect()
            } else {
                paths
            };
            let mut steps: Vec<TokenStream> = Vec::with_capacity(ordered.len());
            for step_path in ordered {
                let step_ident = if reverse {
                    format_ident!("path_{}_rev", step_path)
                } else {
                    format_ident!("path_{}", step_path)
                };
                steps.push(quote! {
                    let mut next: HashSet<Term> = HashSet::new();
                    for node in current {
                        for value in #step_ident(store, graph, &node) {
                            next.insert(value);
                        }
                    }
                    current = next;
                    if current.is_empty() {
                        return Vec::new();
                    }
                });
            }
            Ok(quote! {
                let mut current: HashSet<Term> = HashSet::new();
                current.insert(start.clone());
                #(#steps)*
                current.into_iter().collect()
            })
        }
        PathView::Alternative(paths) => {
            let mut loops: Vec<TokenStream> = Vec::with_capacity(paths.len());
            for alt in paths {
                let alt_ident = if reverse {
                    format_ident!("path_{}_rev", alt)
                } else {
                    format_ident!("path_{}", alt)
                };
                loops.push(quote! {
                    for value in #alt_ident(store, graph, start) {
                        out_set.insert(value);
                    }
                });
            }
            Ok(quote! {
                let mut out_set: HashSet<Term> = HashSet::new();
                #(#loops)*
                out_set.into_iter().collect()
            })
        }
        PathView::ZeroOrMore(inner) => {
            let inner_ident = if reverse {
                format_ident!("path_{}_rev", inner)
            } else {
                format_ident!("path_{}", inner)
            };
            Ok(quote! {
                let mut seen: HashSet<Term> = HashSet::new();
                let mut queue: VecDeque<Term> = VecDeque::new();
                seen.insert(start.clone());
                queue.push_back(start.clone());
                while let Some(node) = queue.pop_front() {
                    for value in #inner_ident(store, graph, &node) {
                        if seen.insert(value.clone()) {
                            queue.push_back(value);
                        }
                    }
                }
                seen.into_iter().collect()
            })
        }
        PathView::OneOrMore(inner) => {
            let inner_ident = if reverse {
                format_ident!("path_{}_rev", inner)
            } else {
                format_ident!("path_{}", inner)
            };
            Ok(quote! {
                let mut seen: HashSet<Term> = HashSet::new();
                let mut queue: VecDeque<Term> = VecDeque::new();
                for value in #inner_ident(store, graph, start) {
                    if seen.insert(value.clone()) {
                        queue.push_back(value);
                    }
                }
                while let Some(node) = queue.pop_front() {
                    for value in #inner_ident(store, graph, &node) {
                        if seen.insert(value.clone()) {
                            queue.push_back(value);
                        }
                    }
                }
                seen.into_iter().collect()
            })
        }
        PathView::ZeroOrOne(inner) => {
            let inner_ident = if reverse {
                format_ident!("path_{}_rev", inner)
            } else {
                format_ident!("path_{}", inner)
            };
            Ok(quote! {
                let mut out_set: HashSet<Term> = HashSet::new();
                out_set.insert(start.clone());
                for value in #inner_ident(store, graph, start) {
                    out_set.insert(value);
                }
                out_set.into_iter().collect()
            })
        }
    }
}

fn generate_path_term_function(plan: &PlanView, path_id: u64) -> Result<Item, String> {
    let fn_ident = format_ident!("path_term_{}", path_id);
    let body = generate_path_term_body(plan, path_id)?;
    parse2::<Item>(quote! {
        fn #fn_ident(graph: &mut Graph) -> Term {
            #body
        }
    })
    .map_err(|err| {
        format!(
            "failed to generate path term function {}: {}",
            fn_ident, err
        )
    })
}

fn generate_path_term_body(plan: &PlanView, path_id: u64) -> Result<TokenStream, String> {
    let path = path_view_for_id(plan, path_id)?;
    match path {
        PathView::Simple(term_id) => term_expr(plan, term_id),
        PathView::Inverse(inner) => {
            let inner_ident = format_ident!("path_term_{}", inner);
            let pred_lit = LitStr::new("http://www.w3.org/ns/shacl#inversePath", Span::call_site());
            Ok(quote! {
                let bn = BlankNode::default();
                let subject: NamedOrBlankNode = bn.clone().into();
                let inner = #inner_ident(graph);
                let pred = NamedNode::new_unchecked(#pred_lit);
                graph.insert(Triple::new(subject.clone(), pred, inner).as_ref());
                bn.into()
            })
        }
        PathView::Sequence(paths) => {
            let path_terms: Vec<TokenStream> = paths
                .iter()
                .map(|inner| {
                    let inner_ident = format_ident!("path_term_{}", inner);
                    quote! { #inner_ident(graph) }
                })
                .collect();
            Ok(quote! {
                let items: Vec<Term> = vec![#(#path_terms),*];
                build_rdf_list(items, graph)
            })
        }
        PathView::Alternative(paths) => {
            let path_terms: Vec<TokenStream> = paths
                .iter()
                .map(|inner| {
                    let inner_ident = format_ident!("path_term_{}", inner);
                    quote! { #inner_ident(graph) }
                })
                .collect();
            let pred_lit = LitStr::new(
                "http://www.w3.org/ns/shacl#alternativePath",
                Span::call_site(),
            );
            Ok(quote! {
                let bn = BlankNode::default();
                let subject: NamedOrBlankNode = bn.clone().into();
                let items: Vec<Term> = vec![#(#path_terms),*];
                let list_head = build_rdf_list(items, graph);
                let pred = NamedNode::new_unchecked(#pred_lit);
                graph.insert(Triple::new(subject.clone(), pred, list_head).as_ref());
                bn.into()
            })
        }
        PathView::ZeroOrMore(inner) => {
            let inner_ident = format_ident!("path_term_{}", inner);
            let pred_lit = LitStr::new(
                "http://www.w3.org/ns/shacl#zeroOrMorePath",
                Span::call_site(),
            );
            Ok(quote! {
                let bn = BlankNode::default();
                let subject: NamedOrBlankNode = bn.clone().into();
                let inner = #inner_ident(graph);
                let pred = NamedNode::new_unchecked(#pred_lit);
                graph.insert(Triple::new(subject.clone(), pred, inner).as_ref());
                bn.into()
            })
        }
        PathView::OneOrMore(inner) => {
            let inner_ident = format_ident!("path_term_{}", inner);
            let pred_lit = LitStr::new(
                "http://www.w3.org/ns/shacl#oneOrMorePath",
                Span::call_site(),
            );
            Ok(quote! {
                let bn = BlankNode::default();
                let subject: NamedOrBlankNode = bn.clone().into();
                let inner = #inner_ident(graph);
                let pred = NamedNode::new_unchecked(#pred_lit);
                graph.insert(Triple::new(subject.clone(), pred, inner).as_ref());
                bn.into()
            })
        }
        PathView::ZeroOrOne(inner) => {
            let inner_ident = format_ident!("path_term_{}", inner);
            let pred_lit = LitStr::new(
                "http://www.w3.org/ns/shacl#zeroOrOnePath",
                Span::call_site(),
            );
            Ok(quote! {
                let bn = BlankNode::default();
                let subject: NamedOrBlankNode = bn.clone().into();
                let inner = #inner_ident(graph);
                let pred = NamedNode::new_unchecked(#pred_lit);
                graph.insert(Triple::new(subject.clone(), pred, inner).as_ref());
                bn.into()
            })
        }
    }
}

fn generate_inference_items(plan: &PlanView) -> Result<Vec<Item>, String> {
    let mut property_shape_targets: HashSet<u64> = HashSet::new();
    for shape in plan.shapes.iter().filter(|shape| shape.kind == "Property") {
        if !shape.targets.is_empty() {
            property_shape_targets.insert(shape.id);
        }
    }

    let mut rule_targets: HashMap<u64, (Vec<u64>, Vec<u64>)> = HashMap::new();
    for (shape_id, rule_ids) in &plan.node_shape_rules {
        for rule_id in rule_ids {
            let entry = rule_targets
                .entry(*rule_id)
                .or_insert_with(|| (Vec::new(), Vec::new()));
            entry.0.push(*shape_id);
        }
    }
    for (shape_id, rule_ids) in &plan.property_shape_rules {
        if !property_shape_targets.contains(shape_id) {
            continue;
        }
        for rule_id in rule_ids {
            let entry = rule_targets
                .entry(*rule_id)
                .or_insert_with(|| (Vec::new(), Vec::new()));
            entry.1.push(*shape_id);
        }
    }
    for (_, targets) in rule_targets.iter_mut() {
        targets.0.sort_unstable();
        targets.1.sort_unstable();
    }

    let rule_ids: Vec<u64> = plan.rules.iter().map(|rule| rule.id).collect();
    let rule_ids_len = rule_ids.len();

    let mut items: Vec<Item> = Vec::new();

    items.push(
        parse2::<Item>(quote! { const INFERENCE_MAX_ITERATIONS: usize = 32; })
            .map_err(|err| format!("failed to generate INFERENCE_MAX_ITERATIONS: {err}"))?,
    );
    items.push(
        parse2::<Item>(quote! {
            const INFERENCE_RULE_IDS: [u64; #rule_ids_len] = [#(#rule_ids),*];
        })
        .map_err(|err| format!("failed to generate INFERENCE_RULE_IDS: {err}"))?,
    );

    items.push(
        parse2::<Item>(quote! {
            fn run_inference(
                store: &Store,
                graph: Option<GraphNameRef<'_>>,
                graph_node: &NamedNode,
            ) -> Result<(), String> {
                let mut iterations: usize = 0;
                loop {
                    info!("Starting inference iteration {}", iterations);
                    compiled_stage(&format!("inference iteration {} start", iterations));
                    let mut seen_new: HashSet<(Term, NamedNode, Term)> = HashSet::new();
                    let added = run_inference_iteration(store, graph, graph_node, &mut seen_new)?;
                    info!("Iteration {} added {} triples", iterations, added);
                    compiled_stage(&format!(
                        "inference iteration {} finish added={}",
                        iterations,
                        added
                    ));
                    if added == 0 || iterations >= INFERENCE_MAX_ITERATIONS {
                        break;
                    }
                    iterations = iterations.saturating_add(1);
                }
                compiled_stage(&format!("inference loop finished iterations={}", iterations));
                Ok(())
            }
        })
        .map_err(|err| format!("failed to generate run_inference: {err}"))?,
    );

    items.push(
        parse2::<Item>(quote! {
            fn run_inference_iteration(
                store: &Store,
                graph: Option<GraphNameRef<'_>>,
                graph_node: &NamedNode,
                seen_new: &mut HashSet<(Term, NamedNode, Term)>,
            ) -> Result<usize, String> {
                let mut added: usize = 0;
                let mut graph_changed = false;
                let mut node_target_cache: HashMap<u64, Vec<Term>> = HashMap::new();
                let mut prop_target_cache: HashMap<u64, Vec<Term>> = HashMap::new();
                for rule_id in INFERENCE_RULE_IDS {
                    let focus_nodes = focus_nodes_for_rule(
                        rule_id,
                        store,
                        graph,
                        &mut node_target_cache,
                        &mut prop_target_cache,
                    );
                    if focus_nodes.is_empty() {
                        continue;
                    }
                    let candidates = collect_inference_rule_candidates(
                        rule_id,
                        store,
                        graph,
                        &focus_nodes,
                    )?;
                    let mut delta: usize = 0;
                    for (subject_term, predicate, object_term) in candidates {
                        if record_inferred_quad(
                            store,
                            graph_node,
                            seen_new,
                            subject_term,
                            predicate,
                            object_term,
                        )? {
                            delta += 1;
                        }
                    }
                    added += delta;
                    if delta > 0 {
                        graph_changed = true;
                    }
                }
                if graph_changed {
                    clear_target_class_caches();
                }
                Ok(added)
            }
        })
        .map_err(|err| format!("failed to generate run_inference_iteration: {err}"))?,
    );

    let mut focus_rule_arms: Vec<TokenStream> = Vec::new();
    for rule in &plan.rules {
        let mut node_target_blocks: Vec<TokenStream> = Vec::new();
        let mut prop_target_blocks: Vec<TokenStream> = Vec::new();
        if let Some((node_shapes, prop_shapes)) = rule_targets.get(&rule.id) {
            for node_shape in node_shapes {
                let node_shape_id = *node_shape;
                let collect_ident = format_ident!("collect_targets_node_{}", node_shape_id);
                node_target_blocks.push(quote! {
                    if !node_target_cache.contains_key(&#node_shape_id) {
                        let targets = #collect_ident(store, graph);
                        node_target_cache.insert(#node_shape_id, targets);
                    }
                    if let Some(targets) = node_target_cache.get(&#node_shape_id) {
                        for focus in targets {
                            if seen.insert(focus.clone()) {
                                nodes.push(focus.clone());
                            }
                        }
                    }
                });
            }
            for prop_shape in prop_shapes {
                let prop_shape_id = *prop_shape;
                let collect_ident = format_ident!("collect_targets_prop_{}", prop_shape_id);
                prop_target_blocks.push(quote! {
                    if !prop_target_cache.contains_key(&#prop_shape_id) {
                        let targets = #collect_ident(store, graph);
                        prop_target_cache.insert(#prop_shape_id, targets);
                    }
                    if let Some(targets) = prop_target_cache.get(&#prop_shape_id) {
                        for focus in targets {
                            if seen.insert(focus.clone()) {
                                nodes.push(focus.clone());
                            }
                        }
                    }
                });
            }
        }
        let rule_id = rule.id;
        focus_rule_arms.push(quote! {
            #rule_id => {
                let mut seen: HashSet<Term> = HashSet::new();
                let mut nodes: Vec<Term> = Vec::new();
                #(#node_target_blocks)*
                #(#prop_target_blocks)*
                nodes
            }
        });
    }

    items.push(
        parse2::<Item>(quote! {
            fn focus_nodes_for_rule(
                rule_id: u64,
                store: &Store,
                graph: Option<GraphNameRef<'_>>,
                node_target_cache: &mut HashMap<u64, Vec<Term>>,
                prop_target_cache: &mut HashMap<u64, Vec<Term>>,
            ) -> Vec<Term> {
                match rule_id {
                    #(#focus_rule_arms,)*
                    _ => Vec::new(),
                }
            }
        })
        .map_err(|err| format!("failed to generate focus_nodes_for_rule: {err}"))?,
    );

    let mut condition_arms: Vec<TokenStream> = Vec::new();
    for rule in &plan.rules {
        if rule.conditions.is_empty() {
            continue;
        }
        let rule_id = rule.id;
        let checks: Vec<TokenStream> = rule
            .conditions
            .iter()
            .map(|shape_id| {
                let conforms_ident = format_ident!("node_shape_conforms_{}", shape_id);
                quote! {
                    if !#conforms_ident(store, graph, focus) {
                        return false;
                    }
                }
            })
            .collect();
        condition_arms.push(quote! {
            #rule_id => {
                #(#checks)*
                true
            }
        });
    }

    items.push(
        parse2::<Item>(quote! {
            fn rule_conditions_satisfied(
                rule_id: u64,
                store: &Store,
                graph: Option<GraphNameRef<'_>>,
                focus: &Term,
            ) -> bool {
                match rule_id {
                    #(#condition_arms,)*
                    _ => true,
                }
            }
        })
        .map_err(|err| format!("failed to generate rule_conditions_satisfied: {err}"))?,
    );

    let apply_rule_arms: Vec<TokenStream> = plan
        .rules
        .iter()
        .map(|rule| {
            let rule_id = rule.id;
            let fn_ident = format_ident!("inference_rule_{}", rule_id);
            quote! { #rule_id => #fn_ident(store, graph, focus_nodes) }
        })
        .collect();

    items.push(
        parse2::<Item>(quote! {
            fn collect_inference_rule_candidates(
                rule_id: u64,
                store: &Store,
                graph: Option<GraphNameRef<'_>>,
                focus_nodes: &[Term],
            ) -> Result<Vec<(Term, NamedNode, Term)>, String> {
                match rule_id {
                    #(#apply_rule_arms,)*
                    _ => Ok(Vec::new()),
                }
            }
        })
        .map_err(|err| format!("failed to generate collect_inference_rule_candidates: {err}"))?,
    );

    for rule in &plan.rules {
        items.push(generate_inference_rule_function(plan, rule)?);
    }

    items.push(
        parse2::<Item>(quote! {
            fn record_inferred_quad(
                store: &Store,
                graph_node: &NamedNode,
                seen_new: &mut HashSet<(Term, NamedNode, Term)>,
                subject_term: Term,
                predicate: NamedNode,
                object_term: Term,
            ) -> Result<bool, String> {
                let key = (subject_term.clone(), predicate.clone(), object_term.clone());
                if seen_new.contains(&key) {
                    return Ok(false);
                }
                let subject = term_to_named_or_blank_inference(&subject_term)?;
                let graph = GraphName::NamedNode(graph_node.clone());
                let quad = Quad::new(
                    subject.clone(),
                    predicate.clone(),
                    object_term.clone(),
                    graph.clone(),
                );
                if store.contains(&quad).map_err(|e| e.to_string())? {
                    return Ok(false);
                }
                store.insert(quad.as_ref()).map_err(|e| e.to_string())?;
                seen_new.insert(key);
                Ok(true)
            }
        })
        .map_err(|err| format!("failed to generate record_inferred_quad: {err}"))?,
    );

    items.push(
        parse2::<Item>(quote! {
            fn term_to_named_or_blank_inference(term: &Term) -> Result<NamedOrBlankNode, String> {
                match term {
                    Term::NamedNode(node) => Ok(node.clone().into()),
                    Term::BlankNode(bn) => Ok(bn.clone().into()),
                    other => Err(format!(
                        "Inference subject must be IRI or blank node, found {:?}",
                        other
                    )),
                }
            }
        })
        .map_err(|err| format!("failed to generate term_to_named_or_blank_inference: {err}"))?,
    );

    items.push(
        parse2::<Item>(quote! {
            fn named_or_blank_to_term(value: &NamedOrBlankNode) -> Term {
                match value {
                    NamedOrBlankNode::NamedNode(nn) => Term::NamedNode(nn.clone()),
                    NamedOrBlankNode::BlankNode(bn) => Term::BlankNode(bn.clone()),
                }
            }
        })
        .map_err(|err| format!("failed to generate named_or_blank_to_term: {err}"))?,
    );

    Ok(items)
}

fn rule_term_values_expr(plan: &PlanView, value: &Value) -> Result<TokenStream, String> {
    if value.as_str() == Some("This") {
        return Ok(quote! {
            {
                let mut values = Vec::new();
                values.push(focus.clone());
                values
            }
        });
    }
    if let Some(term_id) = value.get("Constant").and_then(Value::as_u64) {
        let constant_expr = term_expr(plan, term_id)?;
        return Ok(quote! {
            {
                let mut values = Vec::new();
                values.push(#constant_expr);
                values
            }
        });
    }
    if let Some(path_id) = value.get("Path").and_then(Value::as_u64) {
        let path_ident = format_ident!("path_{}", path_id);
        return Ok(quote! {
            #path_ident(store, graph, focus)
        });
    }
    Err(format!("unsupported rule term encoding: {}", value))
}

fn generate_inference_rule_function(plan: &PlanView, rule: &PlanRuleView) -> Result<Item, String> {
    let fn_ident = format_ident!("inference_rule_{}", rule.id);

    if let Some(triple) = rule.kind.get("Triple") {
        let subject = triple
            .get("subject")
            .ok_or_else(|| format!("inference triple rule {} missing subject", rule.id))?;
        let predicate_id = triple
            .get("predicate")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("inference triple rule {} missing predicate", rule.id))?;
        let object = triple
            .get("object")
            .ok_or_else(|| format!("inference triple rule {} missing object", rule.id))?;

        let predicate_iri = term_iri(plan, predicate_id)?;
        let predicate_lit = LitStr::new(&predicate_iri, Span::call_site());
        let subject_values_expr = rule_term_values_expr(plan, subject)?;
        let object_values_expr = rule_term_values_expr(plan, object)?;
        let rule_id = rule.id;

        return parse2::<Item>(quote! {
            fn #fn_ident(
                store: &Store,
                graph: Option<GraphNameRef<'_>>,
                focus_nodes: &[Term],
            ) -> Result<Vec<(Term, NamedNode, Term)>, String> {
                let predicate = NamedNode::new(#predicate_lit).unwrap();
                let candidates: Result<Vec<Vec<(Term, NamedNode, Term)>>, String> = focus_nodes
                    .par_iter()
                    .map(|focus| {
                        if !rule_conditions_satisfied(#rule_id, store, graph, focus) {
                            return Ok(Vec::new());
                        }

                        let subjects = #subject_values_expr;
                        let objects = #object_values_expr;
                        let mut batch = Vec::with_capacity(subjects.len() * objects.len());
                        for subject_term in &subjects {
                            for object_term in &objects {
                                batch.push((
                                    subject_term.clone(),
                                    predicate.clone(),
                                    object_term.clone(),
                                ));
                            }
                        }
                        Ok(batch)
                    })
                    .collect();

                let mut out = Vec::new();
                for batch in candidates? {
                    out.extend(batch);
                }
                Ok(out)
            }
        })
        .map_err(|err| {
            format!(
                "failed to generate triple inference rule function {}: {}",
                rule.id, err
            )
        });
    }

    if let Some(sparql) = rule.kind.get("Sparql") {
        let query = sparql
            .get("query")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("inference sparql rule {} missing query", rule.id))?;
        let query_lit = LitStr::new(query, Span::call_site());
        let rule_id = rule.id;

        return parse2::<Item>(quote! {
            fn #fn_ident(
                store: &Store,
                graph: Option<GraphNameRef<'_>>,
                focus_nodes: &[Term],
            ) -> Result<Vec<(Term, NamedNode, Term)>, String> {
                let base_query = #query_lit;
                let query_has_this = query_mentions_var(base_query, "this");
                let mut prepared_base = SparqlEvaluator::new().parse_query(base_query).map_err(|e| {
                    format!("Failed to parse inference query {}: {}", #rule_id, e)
                })?;
                if let Some(graph) = graph {
                    prepared_base
                        .dataset_mut()
                        .set_default_graph(vec![graph.into_owned()]);
                } else {
                    prepared_base.dataset_mut().set_default_graph_as_union();
                }

                let candidates: Result<Vec<Vec<(Term, NamedNode, Term)>>, String> = focus_nodes
                    .par_iter()
                    .map(|focus| {
                        if !rule_conditions_satisfied(#rule_id, store, graph, focus) {
                            return Ok(Vec::new());
                        }

                        let var_this = Variable::new("this").map_err(|e| {
                            format!("Inference rule {} failed to build variable: {}", #rule_id, e)
                        })?;

                        let mut prepared_query = if query_has_this {
                            if let Some(ground) = term_to_sparql_ground(focus) {
                                let bindings = format!("BIND({} AS ?this)", ground);
                                let bound_query = inject_bindings_in_where(base_query, &bindings);
                                let mut parsed = SparqlEvaluator::new().parse_query(&bound_query).map_err(|e| {
                                    format!("Failed to parse inference query {}: {}", #rule_id, e)
                                })?;
                                if let Some(graph) = graph {
                                    parsed
                                        .dataset_mut()
                                        .set_default_graph(vec![graph.into_owned()]);
                                } else {
                                    parsed.dataset_mut().set_default_graph_as_union();
                                }
                                parsed
                            } else {
                                prepared_base.clone()
                            }
                        } else {
                            prepared_base.clone()
                        };

                        let mut bound = prepared_query.on_store(store);
                        if query_has_this && term_to_sparql_ground(focus).is_none() {
                            bound = bound.substitute_variable(var_this, focus.clone());
                        }

                        let mut batch = Vec::new();
                        match bound.execute() {
                            Ok(QueryResults::Graph(mut triples)) => {
                                for triple_res in &mut triples {
                                    let triple = triple_res.map_err(|e| {
                                        format!("Inference rule {} failed: {}", #rule_id, e)
                                    })?;
                                    let subject_term = named_or_blank_to_term(&triple.subject);
                                    batch.push((subject_term, triple.predicate, triple.object));
                                }
                            }
                            Ok(_) => {
                                return Err(format!(
                                    "Inference rule {} returned non-graph result",
                                    #rule_id
                                ));
                            }
                            Err(e) => {
                                return Err(e.to_string());
                            }
                        }

                        Ok(batch)
                    })
                    .collect();

                let mut out = Vec::new();
                for batch in candidates? {
                    out.extend(batch);
                }
                Ok(out)
            }
        })
        .map_err(|err| {
            format!(
                "failed to generate sparql inference rule function {}: {}",
                rule.id, err
            )
        });
    }

    Err(format!(
        "inference rule {} has unsupported kind encoding: {}",
        rule.id, rule.kind
    ))
}

fn generate_run_module(plan: &PlanView) -> Result<String, String> {
    let node_shape_lookup: HashMap<u64, &PlanShapeView> = plan
        .shapes
        .iter()
        .filter(|shape| shape.kind == "Node")
        .map(|shape| (shape.id, shape))
        .collect();
    let property_shape_lookup: HashMap<u64, &PlanShapeView> = plan
        .shapes
        .iter()
        .filter(|shape| shape.kind == "Property")
        .map(|shape| (shape.id, shape))
        .collect();

    let mut node_validators = Vec::new();
    let mut node_target_specs: Vec<(bool, Vec<String>)> = Vec::new();
    for shape_id in &plan.order.node_shapes {
        let shape = node_shape_lookup
            .get(shape_id)
            .ok_or_else(|| format!("Missing node shape {}", shape_id))?;
        if shape.deactivated {
            continue;
        }
        node_validators.push(format_ident!("validate_node_shape_{}", shape.id));

        let mut has_non_class_targets = false;
        let mut class_targets: Vec<String> = Vec::new();
        for target in &shape.targets {
            if let Some(term_id) = target.get("Class").and_then(Value::as_u64) {
                class_targets.push(term_iri(plan, term_id)?);
            } else {
                has_non_class_targets = true;
            }
        }
        class_targets.sort();
        class_targets.dedup();
        node_target_specs.push((has_non_class_targets, class_targets));
    }

    let mut property_validators = Vec::new();
    let mut property_target_specs: Vec<(bool, Vec<String>)> = Vec::new();
    for shape_id in &plan.order.property_shapes {
        let shape = property_shape_lookup
            .get(shape_id)
            .ok_or_else(|| format!("Missing property shape {}", shape_id))?;
        if shape.deactivated || shape.targets.is_empty() {
            continue;
        }
        property_validators.push(format_ident!("validate_property_shape_{}", shape.id));

        let mut has_non_class_targets = false;
        let mut class_targets: Vec<String> = Vec::new();
        for target in &shape.targets {
            if let Some(term_id) = target.get("Class").and_then(Value::as_u64) {
                class_targets.push(term_iri(plan, term_id)?);
            } else {
                has_non_class_targets = true;
            }
        }
        class_targets.sort();
        class_targets.dedup();
        property_target_specs.push((has_non_class_targets, class_targets));
    }

    let node_target_spec_items: Vec<TokenStream> = node_target_specs
        .iter()
        .map(|(has_non_class_targets, class_targets)| {
            let has_non_class_targets = *has_non_class_targets;
            let class_lits: Vec<LitStr> = class_targets
                .iter()
                .map(|class_iri| LitStr::new(class_iri, Span::call_site()))
                .collect();
            quote! {
                TargetOptimizationSpec {
                    has_non_class_targets: #has_non_class_targets,
                    class_targets: &[#(#class_lits),*],
                }
            }
        })
        .collect();

    let property_target_spec_items: Vec<TokenStream> = property_target_specs
        .iter()
        .map(|(has_non_class_targets, class_targets)| {
            let has_non_class_targets = *has_non_class_targets;
            let class_lits: Vec<LitStr> = class_targets
                .iter()
                .map(|class_iri| LitStr::new(class_iri, Span::call_site()))
                .collect();
            quote! {
                TargetOptimizationSpec {
                    has_non_class_targets: #has_non_class_targets,
                    class_targets: &[#(#class_lits),*],
                }
            }
        })
        .collect();

    let file = parse2::<File>(quote! {
        type ValidateFn = for<'a> fn(&Store, Option<GraphNameRef<'a>>, &mut Report);

        struct ThreadState {
            closure: Option<SubclassClosure>,
            original_index: Option<OriginalValueIndex>,
        }

        #[derive(Clone, Copy)]
        struct TargetOptimizationSpec {
            has_non_class_targets: bool,
            class_targets: &'static [&'static str],
        }

        const NODE_SHAPE_VALIDATORS: &[ValidateFn] = &[#(#node_validators),*];
        const PROPERTY_SHAPE_VALIDATORS: &[ValidateFn] = &[#(#property_validators),*];
        const NODE_SHAPE_TARGET_OPT_SPECS: &[TargetOptimizationSpec] = &[#(#node_target_spec_items),*];
        const PROPERTY_SHAPE_TARGET_OPT_SPECS: &[TargetOptimizationSpec] = &[#(#property_target_spec_items),*];

        fn active_validators_for_runtime_data(
            store: &Store,
            graph: Option<GraphNameRef<'_>>,
            validators: &[ValidateFn],
            specs: &[TargetOptimizationSpec],
            kind: &str,
        ) -> Vec<ValidateFn> {
            if validators.len() != specs.len() {
                info!(
                    "Skipping data-dependent shape optimization for {} validators due to metadata mismatch (validators={}, specs={})",
                    kind,
                    validators.len(),
                    specs.len()
                );
                return validators.to_vec();
            }

            let mut class_has_instances: HashMap<&'static str, bool> = HashMap::new();
            let mut active: Vec<ValidateFn> = Vec::with_capacity(validators.len());
            let mut pruned = 0usize;

            for (validator, spec) in validators.iter().zip(specs.iter()) {
                let mut keep = spec.has_non_class_targets;
                if !keep {
                    for class_iri in spec.class_targets {
                        let has_instances = *class_has_instances
                            .entry(*class_iri)
                            .or_insert_with(|| !targets_for_class(store, graph, class_iri).is_empty());
                        if has_instances {
                            keep = true;
                            break;
                        }
                    }
                }

                if keep {
                    active.push(*validator);
                } else {
                    pruned += 1;
                }
            }

            info!(
                "Data-dependent shape optimization [{}]: active={} pruned={}",
                kind,
                active.len(),
                pruned
            );
            active
        }

        pub fn run_with_options(
            store: &Store,
            data_graph: Option<&NamedNode>,
            enable_inference: bool,
        ) -> Report {
            let default_data_graph = data_graph_named();
            let graph_node = data_graph.unwrap_or(&default_data_graph);
            let graph = graph_ref(Some(graph_node));
            let mut report = Report::default();
            compiled_stage("run start");

            info!("Starting shape graph load");
            compiled_stage("shape graph load start");
            insert_shape_triples(store);
            info!("Finished shape graph load");
            compiled_stage("shape graph load finish");

            info!("Merging shape graph into data graph for union execution");
            compiled_stage("union setup start");
            if let Err(err) = union_shape_graph_into_data_graph(store, graph_node) {
                eprintln!("Union graph setup failed: {}", err);
                info!("Union graph setup failed");
                compiled_stage("union setup failed");
            } else {
                info!("Finished union graph setup");
                compiled_stage("union setup finish");
            }

            set_current_subclass_closure(subclass_closure_from_shape_edges());
            extend_current_subclass_closure_from_store(store, Some(shape_graph_ref()));
            extend_current_subclass_closure_from_store(
                store,
                Some(GraphNameRef::NamedNode(graph_node.as_ref())),
            );
            clear_target_class_caches();

            if enable_inference {
                info!("Starting inference");
                compiled_stage("inference start");
                match run_inference(store, graph, graph_node) {
                    Ok(_) => {
                        info!("Finished inference");
                        compiled_stage("inference finish");
                    },
                    Err(err) => {
                        eprintln!("Inference failed: {}", err);
                        info!("Inference failed");
                        compiled_stage("inference failed");
                    }
                }
            } else {
                info!("Skipping inference");
                compiled_stage("inference skipped");
            }
            extend_current_subclass_closure_from_store(
                store,
                Some(GraphNameRef::NamedNode(graph_node.as_ref())),
            );

            info!("Starting data-dependent shape optimization");
            compiled_stage("shape optimization start");
            let active_node_validators = active_validators_for_runtime_data(
                store,
                graph,
                NODE_SHAPE_VALIDATORS,
                NODE_SHAPE_TARGET_OPT_SPECS,
                "node",
            );
            let active_property_validators = active_validators_for_runtime_data(
                store,
                graph,
                PROPERTY_SHAPE_VALIDATORS,
                PROPERTY_SHAPE_TARGET_OPT_SPECS,
                "property",
            );
            let node_pruned = NODE_SHAPE_VALIDATORS
                .len()
                .saturating_sub(active_node_validators.len());
            let property_pruned = PROPERTY_SHAPE_VALIDATORS
                .len()
                .saturating_sub(active_property_validators.len());
            info!(
                "Finished data-dependent shape optimization (node active={}, node pruned={}, property active={}, property pruned={})",
                active_node_validators.len(),
                node_pruned,
                active_property_validators.len(),
                property_pruned
            );
            compiled_stage(&format!(
                "shape optimization finish node_active={} node_pruned={} property_active={} property_pruned={}",
                active_node_validators.len(),
                node_pruned,
                active_property_validators.len(),
                property_pruned
            ));

            info!("Starting validation");
            compiled_stage("validation start");
            let validation_closure = with_subclass_closure(|closure| closure.clone());
            let original_index = with_original_value_index(|idx| idx.cloned());

            let node_reports: Vec<Report> = active_node_validators
                .par_iter()
                .map_init(
                    || ThreadState {
                        closure: Some(validation_closure.clone()),
                        original_index: original_index.clone(),
                    },
                    |state, validator| {
                        if let Some(closure) = state.closure.take() {
                            init_thread_state(closure);
                        }
                        if let Some(index) = state.original_index.take() {
                            set_original_value_index(Some(index));
                        }
                        let mut local = Report::default();
                        validator(store, graph, &mut local);
                        local
                    },
                )
                .collect();
            for local in node_reports {
                report.merge(local);
            }

            let prop_reports: Vec<Report> = active_property_validators
                .par_iter()
                .map_init(
                    || ThreadState {
                        closure: Some(validation_closure.clone()),
                        original_index: original_index.clone(),
                    },
                    |state, validator| {
                        if let Some(closure) = state.closure.take() {
                            init_thread_state(closure);
                        }
                        if let Some(index) = state.original_index.take() {
                            set_original_value_index(Some(index));
                        }
                        let mut local = Report::default();
                        validator(store, graph, &mut local);
                        local
                    },
                )
                .collect();
            for local in prop_reports {
                report.merge(local);
            }

            info!("Finished validation");
            compiled_stage(&format!("validation finish violations={}", report.violations.len()));
            report
        }

        pub fn run(store: &Store, data_graph: Option<&NamedNode>) -> Report {
            run_with_options(store, data_graph, true)
        }
    })
    .map_err(|err| format!("failed to generate run module AST: {err}"))?;

    Ok(prettyplease::unparse(&file))
}
fn generate_shape_triples_module(
    plan: &PlanView,
    shape_graph_files: &[String],
) -> Result<String, String> {
    let shape_graph_iri = term_iri(plan, plan.shape_graph)?;
    let shape_graph_lit = LitStr::new(&shape_graph_iri, Span::call_site());
    let include_files: Vec<String> = if shape_graph_files.is_empty() {
        vec!["shape_graph.nt".to_string()]
    } else {
        shape_graph_files.to_vec()
    };

    let chunk_const_defs: Vec<TokenStream> = include_files
        .iter()
        .enumerate()
        .map(|(idx, file)| {
            let ident = format_ident!("SHAPE_GRAPH_NT_CHUNK_{idx:04}");
            let lit = LitStr::new(file, Span::call_site());
            quote! { const #ident: &str = include_str!(#lit); }
        })
        .collect();
    let chunk_const_idents: Vec<_> = (0..include_files.len())
        .map(|idx| format_ident!("SHAPE_GRAPH_NT_CHUNK_{idx:04}"))
        .collect();
    let chunk_count = chunk_const_idents.len();

    let file = parse2::<File>(quote! {
        #(#chunk_const_defs)*
        const SHAPE_GRAPH_NT_CHUNKS: [&str; #chunk_count] = [#(#chunk_const_idents),*];

        fn insert_shape_triples(store: &Store) {
            let shape_graph = GraphName::NamedNode(NamedNode::new(#shape_graph_lit).unwrap());
            for nt_chunk in SHAPE_GRAPH_NT_CHUNKS {
                let parser = RdfParser::from_format(RdfFormat::NTriples).for_slice(nt_chunk.as_bytes());
                for triple in parser {
                    let triple = match triple {
                        Ok(t) => t,
                        Err(_) => continue,
                    };
                    let quad = Quad::new(triple.subject, triple.predicate, triple.object, shape_graph.clone());
                    let _ = store.insert(quad.as_ref());
                }
            }
        }
    })
    .map_err(|err| format!("failed to generate shape_triples module AST: {err}"))?;

    Ok(prettyplease::unparse(&file))
}

fn generate_shape_graph_nt(plan: &PlanIR) -> Result<String, String> {
    let mut out = String::new();
    for triple in &plan.shape_triples {
        let subject_term = plan
            .terms
            .get(triple.subject as usize)
            .ok_or_else(|| format!("Unknown term id {}", triple.subject))?;
        let predicate_term = plan
            .terms
            .get(triple.predicate as usize)
            .ok_or_else(|| format!("Unknown term id {}", triple.predicate))?;
        let object_term = plan
            .terms
            .get(triple.object as usize)
            .ok_or_else(|| format!("Unknown term id {}", triple.object))?;
        let subj_nt = term_to_nt(subject_term)?;
        let pred_nt = term_to_nt(predicate_term)?;
        let obj_nt = term_to_nt(object_term)?;
        out.push_str(&subj_nt);
        out.push(' ');
        out.push_str(&pred_nt);
        out.push(' ');
        out.push_str(&obj_nt);
        out.push_str(" .\n");
    }
    Ok(out)
}

fn term_to_nt(term: &Term) -> Result<String, String> {
    match term {
        Term::NamedNode(node) => Ok(format!("<{}>", escape_nt_iri(node.as_str()))),
        Term::BlankNode(node) => Ok(format!("_:{}", node.as_str())),
        Term::Literal(lit) => {
            let mut out = String::new();
            out.push('"');
            out.push_str(&escape_nt_string(lit.value()));
            out.push('"');
            if let Some(lang) = lit.language() {
                out.push('@');
                out.push_str(lang);
            } else {
                out.push_str("^^<");
                out.push_str(&escape_nt_iri(lit.datatype().as_str()));
                out.push('>');
            }
            Ok(out)
        }
    }
}

fn escape_nt_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch <= '\u{1F}' || ch == '\u{7F}' => {
                out.push_str(&format!("\\u{:04X}", ch as u32))
            }
            ch => out.push(ch),
        }
    }
    out
}

fn escape_nt_iri(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '<' | '>' | '"' | '{' | '}' | '|' | '^' | '`' | '\\' => {
                out.push_str(&format!("\\u{:04X}", ch as u32))
            }
            ch if ch <= '\u{1F}' || ch == '\u{7F}' => {
                out.push_str(&format!("\\u{:04X}", ch as u32))
            }
            ch => out.push(ch),
        }
    }
    out
}

fn generate_targets_items(plan: &PlanView) -> Result<Vec<Item>, String> {
    let mut items: Vec<Item> = Vec::new();

    for shape in plan.shapes.iter().filter(|shape| !shape.deactivated) {
        let prefix = match shape.kind.as_str() {
            "Node" => "node",
            "Property" => "prop",
            _ => continue,
        };
        let fn_ident = format_ident!("collect_targets_{}_{}", prefix, shape.id);
        let shape_term_expr = term_expr(plan, shape.term)?;
        let mut target_blocks: Vec<TokenStream> = Vec::new();

        for target in &shape.targets {
            if let Some(term_id) = target.get("Class").and_then(Value::as_u64) {
                let class_iri = term_iri(plan, term_id)?;
                let class_lit = LitStr::new(&class_iri, Span::call_site());
                target_blocks.push(quote! {
                    for focus in targets_for_class(store, graph, #class_lit) {
                        if seen.insert(focus.clone()) {
                            out.push(focus);
                        }
                    }
                });
                continue;
            }

            if let Some(term_id) = target.get("Node").and_then(Value::as_u64) {
                let node_expr = term_expr(plan, term_id)?;
                let shape_term_expr = shape_term_expr.clone();
                target_blocks.push(quote! {
                    {
                        let focus: Term = #node_expr;
                        let shape_term: Term = deskolemize_term(#shape_term_expr);
                        let nodes = vec![focus];
                        let nodes = canonicalize_values_for_predicate(
                            store,
                            Some(shape_graph_ref()),
                            &shape_term,
                            "http://www.w3.org/ns/shacl#targetNode",
                            nodes,
                        );
                        for node in nodes {
                            if seen.insert(node.clone()) {
                                out.push(node);
                            }
                        }
                    }
                });
                continue;
            }

            if let Some(term_id) = target.get("SubjectsOf").and_then(Value::as_u64) {
                let pred_iri = term_iri(plan, term_id)?;
                let pred_lit = LitStr::new(&pred_iri, Span::call_site());
                target_blocks.push(quote! {
                    let pred_ref = NamedNodeRef::new(#pred_lit).unwrap();
                    for quad in store.quads_for_pattern(None, Some(pred_ref), None, graph) {
                        let quad = match quad {
                            Ok(quad) => quad,
                            Err(_) => continue,
                        };
                        let focus: Term = quad.subject.into();
                        if seen.insert(focus.clone()) {
                            out.push(focus);
                        }
                    }
                });
                continue;
            }

            if let Some(term_id) = target.get("ObjectsOf").and_then(Value::as_u64) {
                let pred_iri = term_iri(plan, term_id)?;
                let pred_lit = LitStr::new(&pred_iri, Span::call_site());
                target_blocks.push(quote! {
                    let pred_ref = NamedNodeRef::new(#pred_lit).unwrap();
                    for quad in store.quads_for_pattern(None, Some(pred_ref), None, graph) {
                        let quad = match quad {
                            Ok(quad) => quad,
                            Err(_) => continue,
                        };
                        let focus: Term = quad.object;
                        if seen.insert(focus.clone()) {
                            out.push(focus);
                        }
                    }
                });
                continue;
            }

            if let Some(term_id) = target.get("Advanced").and_then(Value::as_u64) {
                let selector_expr = term_expr(plan, term_id)?;
                target_blocks.push(quote! {
                    {
                        let selector: Term = #selector_expr;
                        for focus in advanced_targets_for(store, graph, &selector) {
                            if seen.insert(focus.clone()) {
                                out.push(focus);
                            }
                        }
                    }
                });
                continue;
            }
        }

        let item = parse2::<Item>(quote! {
            fn #fn_ident(store: &Store, graph: Option<GraphNameRef<'_>>) -> Vec<Term> {
                let mut seen: HashSet<Term> = HashSet::new();
                let mut out: Vec<Term> = Vec::new();
                #(#target_blocks)*
                out
            }
        })
        .map_err(|err| {
            format!(
                "failed to generate targets collector for shape {}: {}",
                shape.id, err
            )
        })?;

        items.push(item);
    }

    Ok(items)
}

fn generate_allowed_predicates_items(plan: &PlanView) -> Result<Vec<Item>, String> {
    let component_lookup: HashMap<u64, &PlanComponentView> =
        plan.components.iter().map(|c| (c.id, c)).collect();
    let property_shape_lookup: HashMap<u64, &PlanShapeView> = plan
        .shapes
        .iter()
        .filter(|shape| shape.kind == "Property")
        .map(|shape| (shape.id, shape))
        .collect();

    let mut items: Vec<Item> = Vec::new();
    for shape in plan.shapes.iter().filter(|shape| shape.kind == "Node") {
        let has_closed = shape.constraints.iter().any(|comp_id| {
            component_lookup
                .get(comp_id)
                .map(|component| component.kind == "Closed")
                .unwrap_or(false)
        });
        if !has_closed {
            continue;
        }

        let mut predicates: Vec<String> = Vec::new();
        for comp_id in &shape.constraints {
            let Some(component) = component_lookup.get(comp_id) else {
                continue;
            };
            if component.kind != "Property" {
                continue;
            }
            let Some(prop_id) = parse_shape_ref(&component.params, "Property") else {
                continue;
            };
            let Some(prop_shape) = property_shape_lookup.get(&prop_id) else {
                continue;
            };
            let Some(path_id) = prop_shape.path else {
                continue;
            };
            if let Some(iri) = simple_predicate_iri(plan, path_id)? {
                predicates.push(iri);
            }
        }

        let fn_ident = format_ident!("allowed_predicates_{}", shape.id);
        let pred_lits: Vec<LitStr> = predicates
            .iter()
            .map(|pred| LitStr::new(pred, Span::call_site()))
            .collect();
        let item = parse2::<Item>(quote! {
            fn #fn_ident() -> HashSet<String> {
                let mut set: HashSet<String> = HashSet::new();
                #(set.insert(#pred_lits.to_string());)*
                set
            }
        })
        .map_err(|err| {
            format!(
                "failed to generate allowed_predicates for shape {}: {}",
                shape.id, err
            )
        })?;
        items.push(item);
    }

    Ok(items)
}

fn render_items_module(module_name: &str, items: Vec<Item>) -> Result<String, String> {
    let file = File {
        shebang: None,
        attrs: vec![],
        items,
    };
    parse_file(&prettyplease::unparse(&file)).map_err(|err| {
        format!(
            "generated module {} failed to parse after rendering: {}",
            module_name, err
        )
    })?;
    Ok(prettyplease::unparse(&file))
}

fn render_items_module_chunked(
    module_name: &str,
    items: Vec<Item>,
    chunk_trigger_items: usize,
    chunk_size: usize,
) -> Result<Vec<(String, String)>, String> {
    if items.len() <= chunk_trigger_items {
        return Ok(vec![(
            module_name.to_string(),
            render_items_module(module_name, items)?,
        )]);
    }

    let module_stem = module_name
        .strip_suffix(".rs")
        .ok_or_else(|| format!("module {module_name} missing .rs suffix"))?;
    let mut chunk_files: Vec<(String, String)> = Vec::new();
    let mut chunk_names: Vec<String> = Vec::new();
    for (idx, chunk_items) in items.chunks(chunk_size).enumerate() {
        let chunk_name = format!("{module_stem}/chunk_{idx:04}.rs");
        let chunk_content = render_items_module(&chunk_name, chunk_items.to_vec())?;
        chunk_names.push(chunk_name.clone());
        chunk_files.push((chunk_name, chunk_content));
    }

    let wrapper = render_chunk_wrapper_module(module_name, &chunk_names)?;
    let mut files = vec![(module_name.to_string(), wrapper)];
    files.extend(chunk_files);
    Ok(files)
}

fn render_chunk_wrapper_module(
    module_name: &str,
    chunk_names: &[String],
) -> Result<String, String> {
    let include_items: Vec<Item> = chunk_names
        .iter()
        .map(|name| {
            let lit = LitStr::new(name, Span::call_site());
            parse2::<Item>(quote! { include!(#lit); }).map_err(|err| {
                format!("failed to build chunk include for {module_name}::{name}: {err}")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let file = File {
        shebang: None,
        attrs: vec![],
        items: include_items,
    };
    Ok(prettyplease::unparse(&file))
}

fn normalize_rust_module(name: &str, content: &str) -> Result<String, String> {
    let parsed = parse_file(content)
        .map_err(|err| format!("generated module {name} failed to parse: {err}"))?;
    Ok(prettyplease::unparse(&parsed))
}

fn helpers_template_content() -> &'static str {
    include_str!("templates/helpers_template.rs")
}

fn merge_helpers_module(
    module_name: &str,
    content: &str,
    new_items: Vec<Item>,
    remove_fn_names: &HashSet<String>,
    remove_const_names: &HashSet<String>,
) -> Result<String, String> {
    if new_items.is_empty() {
        return normalize_rust_module(module_name, content);
    }

    let mut parsed = parse_file(content)
        .map_err(|err| format!("generated module {module_name} failed to parse: {err}"))?;
    parsed.items.retain(|item| match item {
        Item::Fn(item_fn) => !remove_fn_names.contains(&item_fn.sig.ident.to_string()),
        Item::Const(item_const) => !remove_const_names.contains(&item_const.ident.to_string()),
        _ => true,
    });
    parsed.items.extend(new_items);
    Ok(prettyplease::unparse(&parsed))
}

fn build_root_module(module_files: &[String]) -> Result<String, String> {
    let include_items: Vec<Item> = module_files
        .iter()
        .map(|name| {
            let lit = LitStr::new(name, Span::call_site());
            parse2::<Item>(quote! { include!(#lit); })
                .map_err(|err| format!("failed to build include for {name}: {err}"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut file = parse2::<File>(quote! {
        #![allow(unused_variables)]
        #![allow(dead_code)]
        #![allow(unused_mut)]
        #![allow(unused_imports)]
        #![allow(unused_comparisons)]
    })
    .map_err(|err| format!("failed to initialize root module AST: {err}"))?;

    file.items.extend(include_items);

    let mut root = String::from("// Code generated by shacl-compiler. DO NOT EDIT.\n");
    root.push_str(&prettyplease::unparse(&file));
    Ok(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_module_contains_each_include_once() {
        let root = build_root_module(&[
            "prelude.rs".to_string(),
            "helpers.rs".to_string(),
            "run.rs".to_string(),
        ])
        .expect("root module should build");
        assert!(root.contains("include!(\"prelude.rs\");"));
        assert!(root.contains("include!(\"helpers.rs\");"));
        assert!(root.contains("include!(\"run.rs\");"));
    }

    #[test]
    fn helpers_template_keeps_lenient_query_helper() {
        let helpers = helpers_template_content();
        assert!(helpers.contains("fn sparql_any_solution_lenient("));
    }

    #[test]
    fn render_items_module_chunked_emits_wrapper_and_chunks() {
        let items: Vec<Item> = (0..5usize)
            .map(|idx| {
                let fn_ident = format_ident!("f_{idx}");
                parse2::<Item>(quote! { fn #fn_ident() {} }).expect("test item should parse")
            })
            .collect();

        let files = render_items_module_chunked("inference.rs", items, 2, 2)
            .expect("chunked render should succeed");
        assert_eq!(files[0].0, "inference.rs");
        assert!(files[0]
            .1
            .contains("include!(\"inference/chunk_0000.rs\");"));
        assert!(files[0]
            .1
            .contains("include!(\"inference/chunk_0001.rs\");"));
        assert!(files[0]
            .1
            .contains("include!(\"inference/chunk_0002.rs\");"));
        assert_eq!(files.len(), 4);
    }

    #[test]
    fn split_by_line_boundaries_preserves_lines() {
        let input = "a\nbb\nccc\ndddd\n";
        let chunks = split_by_line_boundaries(input, 5);
        assert_eq!(chunks.join(""), input);
        assert!(chunks.iter().all(|chunk| !chunk.is_empty()));
    }
}
