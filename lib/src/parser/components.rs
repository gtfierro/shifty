use std::collections::{HashMap, HashSet};

use oxigraph::model::{Literal, NamedNode, NamedOrBlankNodeRef as SubjectRef, Term, TermRef};

use super::{component_registry::COMPONENT_REGISTRY, ParsingContext};
use crate::model::components::sparql::CustomConstraintComponentDefinition;
use crate::model::components::ComponentDescriptor;
use crate::named_nodes::SHACL;
use crate::types::ComponentID;

/// Parses all constraint components attached to a given shape subject (`start`) from the shapes graph.
///
/// Returns data-only `ComponentDescriptor`s keyed by `ComponentID` for later runtime instantiation.
pub(crate) fn parse_components(
    shape_term: &Term,
    context: &mut ParsingContext,
    unique_lang_lexicals: &HashMap<Term, String>,
    is_property_shape: bool,
) -> Result<HashMap<ComponentID, ComponentDescriptor>, String> {
    let mut descriptors = HashMap::new();
    let shacl = SHACL::new();
    let shape_ref = shape_term.as_ref();

    let pred_obj_pairs: HashMap<NamedNode, Vec<Term>> = context
        .store
        .quads_for_pattern(
            Some(to_subject_ref(shape_ref.clone()).expect("invalid subject term")),
            None,
            None,
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok)
        .fold(HashMap::new(), |mut acc, quad| {
            acc.entry(quad.predicate).or_default().push(quad.object);
            acc
        });

    let mut processed_predicates = HashSet::new();

    for entry in COMPONENT_REGISTRY {
        (entry.apply)(
            &shacl,
            shape_term,
            context,
            unique_lang_lexicals,
            &pred_obj_pairs,
            &mut processed_predicates,
            &mut descriptors,
            is_property_shape,
        )?;
    }

    if let Some(sparql_terms) = pred_obj_pairs.get(&shacl.sparql.into_owned()) {
        processed_predicates.insert(shacl.sparql.into_owned());
        for sparql_term in sparql_terms {
            crate::runtime::validators::validate_constraint_prebound_usage(
                context,
                sparql_term,
                is_property_shape,
            )?;
            let component_id = context.get_or_create_component_id(sparql_term.clone());
            descriptors.insert(
                component_id,
                ComponentDescriptor::Sparql {
                    constraint_node: sparql_term.clone(),
                },
            );
        }
    }

    let (custom_component_defs, param_to_component) = parse_custom_constraint_components(context)?;

    let mut shape_predicates: HashSet<NamedNode> = pred_obj_pairs.keys().cloned().collect();
    for p in processed_predicates {
        shape_predicates.remove(&p);
    }

    let mut component_candidates: HashSet<NamedNode> = HashSet::new();
    for p in &shape_predicates {
        if let Some(ccs) = param_to_component.get(p) {
            component_candidates.extend(ccs.iter().cloned());
        }
    }

    for cc_iri in component_candidates {
        if let Some(cc_def) = custom_component_defs.get(&cc_iri) {
            let mut has_all_mandatory = true;
            let mut parameter_values = HashMap::new();

            for param in &cc_def.parameters {
                if let Some(values) = pred_obj_pairs.get(&param.path) {
                    parameter_values.insert(param.path.clone(), values.clone());
                } else if !param.optional {
                    has_all_mandatory = false;
                    break;
                }
            }

            if has_all_mandatory {
                let mut param_entries: Vec<String> = cc_def
                    .parameters
                    .iter()
                    .map(|param| {
                        let mut values: Vec<String> = parameter_values
                            .get(&param.path)
                            .map(|vals| vals.iter().map(|v| v.to_string()).collect())
                            .unwrap_or_else(|| vec!["<none>".to_string()]);
                        values.sort();
                        format!("{}={}", param.path.as_str(), values.join(","))
                    })
                    .collect();
                param_entries.sort();
                let component_key = format!(
                    "CustomConstraint:{}|{}",
                    cc_iri.as_str(),
                    param_entries.join("|")
                );
                let component_id = context.get_or_create_component_id(Term::Literal(
                    Literal::new_simple_literal(component_key),
                ));
                descriptors.insert(
                    component_id,
                    ComponentDescriptor::Custom {
                        definition: cc_def.clone(),
                        parameter_values,
                    },
                );
            }
        }
    }

    Ok(descriptors)
}

fn to_subject_ref(term: TermRef<'_>) -> Result<SubjectRef<'_>, String> {
    match term {
        TermRef::NamedNode(n) => Ok(n.into()),
        TermRef::BlankNode(b) => Ok(b.into()),
        _ => Err(format!("Invalid subject term {:?}", term)),
    }
}

fn parse_custom_constraint_components(
    context: &ParsingContext,
) -> Result<
    (
        HashMap<NamedNode, CustomConstraintComponentDefinition>,
        HashMap<NamedNode, Vec<NamedNode>>,
    ),
    String,
> {
    crate::runtime::parse_custom_constraint_components(context)
        .map_err(|e| format!("Error parsing custom constraint components: {}", e))
}
