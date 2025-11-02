use super::{parse_rdf_list, ParsingContext};
use crate::model::components::ComponentDescriptor;
use crate::named_nodes::SHACL;
use crate::types::{ComponentID, ID};
use oxigraph::model::{Literal, NamedNode, NamedNodeRef, Term};
use std::collections::{HashMap, HashSet};

pub(crate) type RegistryFn = fn(
    &SHACL,
    &Term,
    &mut ParsingContext,
    &HashMap<Term, String>,
    &HashMap<NamedNode, Vec<Term>>,
    &mut HashSet<NamedNode>,
    &mut HashMap<ComponentID, ComponentDescriptor>,
    bool,
) -> Result<(), String>;

pub(crate) struct ComponentRegistryEntry {
    pub apply: RegistryFn,
}

pub(crate) static COMPONENT_REGISTRY: &[ComponentRegistryEntry] = &[
    ComponentRegistryEntry {
        apply: handle_class_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_datatype_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_node_kind_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_node_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_property_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_min_count_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_max_count_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_min_exclusive_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_min_inclusive_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_max_exclusive_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_max_inclusive_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_min_length_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_max_length_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_pattern_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_language_in_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_unique_lang_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_equals_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_disjoint_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_less_than_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_less_than_or_equals_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_not_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_and_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_or_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_xone_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_qualified_value_shape_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_closed_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_has_value_constraint,
    },
    ComponentRegistryEntry {
        apply: handle_in_constraint,
    },
];

fn insert_descriptor(
    context: &mut ParsingContext,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    key: Term,
    descriptor: ComponentDescriptor,
) {
    let component_id = context.get_or_create_component_id(key);
    descriptors.insert(component_id, descriptor);
}

fn owned_predicate(node: NamedNodeRef<'_>) -> NamedNode {
    node.into_owned()
}

fn handle_class_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.class);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            let key = Term::Literal(Literal::new_simple_literal(format!(
                "ClassConstraint:{}",
                term
            )));
            insert_descriptor(
                context,
                descriptors,
                key,
                ComponentDescriptor::Class {
                    class: term.clone(),
                },
            );
        }
    }
    Ok(())
}

fn handle_datatype_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.datatype);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            let key = Term::Literal(Literal::new_simple_literal(format!(
                "DatatypeConstraint:{}",
                term
            )));
            insert_descriptor(
                context,
                descriptors,
                key,
                ComponentDescriptor::Datatype {
                    datatype: term.clone(),
                },
            );
        }
    }
    Ok(())
}

fn handle_node_kind_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.node_kind);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            let key = Term::Literal(Literal::new_simple_literal(format!(
                "NodeKindConstraint:{}",
                term
            )));
            insert_descriptor(
                context,
                descriptors,
                key,
                ComponentDescriptor::NodeKind {
                    node_kind: term.clone(),
                },
            );
        }
    }
    Ok(())
}

fn handle_node_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.node);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            let target_shape_id = context.get_or_create_node_id(term.clone());
            let key = Term::Literal(Literal::new_simple_literal(format!(
                "NodeConstraint:{}",
                term
            )));
            insert_descriptor(
                context,
                descriptors,
                key,
                ComponentDescriptor::Node {
                    shape: target_shape_id,
                },
            );
        }
    }
    Ok(())
}

fn handle_property_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.property);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            let target_shape_id = context.get_or_create_prop_id(term.clone());
            let key = Term::Literal(Literal::new_simple_literal(format!(
                "PropertyConstraint:{}",
                term
            )));
            insert_descriptor(
                context,
                descriptors,
                key,
                ComponentDescriptor::Property {
                    shape: target_shape_id,
                },
            );
        }
    }
    Ok(())
}

fn handle_min_count_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.min_count);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            if let Term::Literal(lit) = term {
                if let Ok(value) = lit.value().parse::<u64>() {
                    let key = Term::Literal(Literal::new_simple_literal(format!(
                        "MinCountConstraint:{}",
                        term
                    )));
                    insert_descriptor(
                        context,
                        descriptors,
                        key,
                        ComponentDescriptor::MinCount { min_count: value },
                    );
                }
            }
        }
    }
    Ok(())
}

fn handle_max_count_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.max_count);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            if let Term::Literal(lit) = term {
                if let Ok(value) = lit.value().parse::<u64>() {
                    let key = Term::Literal(Literal::new_simple_literal(format!(
                        "MaxCountConstraint:{}",
                        term
                    )));
                    insert_descriptor(
                        context,
                        descriptors,
                        key,
                        ComponentDescriptor::MaxCount { max_count: value },
                    );
                }
            }
        }
    }
    Ok(())
}

fn handle_min_exclusive_constraint(
    shacl: &SHACL,
    shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.min_exclusive);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            let key = Term::Literal(Literal::new_simple_literal(format!(
                "MinExclusiveConstraint:{}:{}",
                shape_term, term
            )));
            insert_descriptor(
                context,
                descriptors,
                key,
                ComponentDescriptor::MinExclusive {
                    value: term.clone(),
                },
            );
        }
    }
    Ok(())
}

fn handle_min_inclusive_constraint(
    shacl: &SHACL,
    shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.min_inclusive);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            let key = Term::Literal(Literal::new_simple_literal(format!(
                "MinInclusiveConstraint:{}:{}",
                shape_term, term
            )));
            insert_descriptor(
                context,
                descriptors,
                key,
                ComponentDescriptor::MinInclusive {
                    value: term.clone(),
                },
            );
        }
    }
    Ok(())
}

fn handle_max_exclusive_constraint(
    shacl: &SHACL,
    shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.max_exclusive);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            let key = Term::Literal(Literal::new_simple_literal(format!(
                "MaxExclusiveConstraint:{}:{}",
                shape_term, term
            )));
            insert_descriptor(
                context,
                descriptors,
                key,
                ComponentDescriptor::MaxExclusive {
                    value: term.clone(),
                },
            );
        }
    }
    Ok(())
}

fn handle_max_inclusive_constraint(
    shacl: &SHACL,
    shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.max_inclusive);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            let key = Term::Literal(Literal::new_simple_literal(format!(
                "MaxInclusiveConstraint:{}:{}",
                shape_term, term
            )));
            insert_descriptor(
                context,
                descriptors,
                key,
                ComponentDescriptor::MaxInclusive {
                    value: term.clone(),
                },
            );
        }
    }
    Ok(())
}

fn handle_min_length_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.min_length);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            if let Term::Literal(lit) = term {
                if let Ok(value) = lit.value().parse::<u64>() {
                    let key = Term::Literal(Literal::new_simple_literal(format!(
                        "MinLengthConstraint:{}",
                        term
                    )));
                    insert_descriptor(
                        context,
                        descriptors,
                        key,
                        ComponentDescriptor::MinLength { length: value },
                    );
                }
            }
        }
    }
    Ok(())
}

fn handle_max_length_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.max_length);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            if let Term::Literal(lit) = term {
                if let Ok(value) = lit.value().parse::<u64>() {
                    let key = Term::Literal(Literal::new_simple_literal(format!(
                        "MaxLengthConstraint:{}",
                        term
                    )));
                    insert_descriptor(
                        context,
                        descriptors,
                        key,
                        ComponentDescriptor::MaxLength { length: value },
                    );
                }
            }
        }
    }
    Ok(())
}

fn handle_pattern_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.pattern);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        if let Some(Term::Literal(pattern_lit)) = terms.first() {
            let pattern_str = pattern_lit.value().to_string();
            let flags_predicate = owned_predicate(shacl.flags);
            let flags = pred_obj_pairs
                .get(&flags_predicate)
                .and_then(|terms| terms.first())
                .and_then(|flag| match flag {
                    Term::Literal(lit) => Some(lit.value().to_string()),
                    _ => None,
                });
            if flags.is_some() {
                processed.insert(flags_predicate);
            }
            let key = Term::Literal(Literal::new_simple_literal(format!(
                "PatternConstraint:{}:{}",
                pattern_str,
                flags.as_deref().unwrap_or("")
            )));
            insert_descriptor(
                context,
                descriptors,
                key,
                ComponentDescriptor::Pattern {
                    pattern: pattern_str,
                    flags,
                },
            );
        }
    }
    Ok(())
}

fn handle_language_in_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.language_in);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        if let Some(list_head) = terms.first() {
            let list_items = parse_rdf_list(context, list_head.clone());
            let languages: Vec<String> = list_items
                .into_iter()
                .filter_map(|term| match term {
                    Term::Literal(lit) => Some(lit.value().to_string()),
                    _ => None,
                })
                .collect();
            let component_id = context.get_or_create_component_id(list_head.clone());
            descriptors.insert(component_id, ComponentDescriptor::LanguageIn { languages });
        }
    }
    Ok(())
}

fn handle_unique_lang_constraint(
    shacl: &SHACL,
    shape_term: &Term,
    context: &mut ParsingContext,
    unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.unique_lang);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        let original_lexical = unique_lang.get(shape_term);
        for term in terms {
            if let Term::Literal(lit) = term {
                let lexical = original_lexical
                    .map(|value| value.as_str())
                    .unwrap_or_else(|| lit.value());
                if lexical.eq_ignore_ascii_case("true") {
                    let key = Term::Literal(Literal::new_simple_literal(format!(
                        "UniqueLangConstraint:{}",
                        term
                    )));
                    insert_descriptor(
                        context,
                        descriptors,
                        key,
                        ComponentDescriptor::UniqueLang { enabled: true },
                    );
                }
            }
        }
    }
    Ok(())
}

fn handle_equals_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.equals);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            if matches!(term, Term::NamedNode(_)) {
                let key = Term::Literal(Literal::new_simple_literal(format!(
                    "EqualsConstraint:{}",
                    term
                )));
                insert_descriptor(
                    context,
                    descriptors,
                    key,
                    ComponentDescriptor::Equals {
                        property: term.clone(),
                    },
                );
            }
        }
    }
    Ok(())
}

fn handle_disjoint_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.disjoint);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            if matches!(term, Term::NamedNode(_)) {
                let key = Term::Literal(Literal::new_simple_literal(format!(
                    "DisjointConstraint:{}",
                    term
                )));
                insert_descriptor(
                    context,
                    descriptors,
                    key,
                    ComponentDescriptor::Disjoint {
                        property: term.clone(),
                    },
                );
            }
        }
    }
    Ok(())
}

fn handle_less_than_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.less_than);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            if matches!(term, Term::NamedNode(_)) {
                let key = Term::Literal(Literal::new_simple_literal(format!(
                    "LessThanConstraint:{}",
                    term
                )));
                insert_descriptor(
                    context,
                    descriptors,
                    key,
                    ComponentDescriptor::LessThan {
                        property: term.clone(),
                    },
                );
            }
        }
    }
    Ok(())
}

fn handle_less_than_or_equals_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.less_than_or_equals);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            if matches!(term, Term::NamedNode(_)) {
                let key = Term::Literal(Literal::new_simple_literal(format!(
                    "LessThanOrEqualsConstraint:{}",
                    term
                )));
                insert_descriptor(
                    context,
                    descriptors,
                    key,
                    ComponentDescriptor::LessThanOrEquals {
                        property: term.clone(),
                    },
                );
            }
        }
    }
    Ok(())
}

fn handle_not_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.not);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            let negated_shape_id = context.get_or_create_node_id(term.clone());
            let key = Term::Literal(Literal::new_simple_literal(format!(
                "NotConstraint:{}",
                term
            )));
            insert_descriptor(
                context,
                descriptors,
                key,
                ComponentDescriptor::Not {
                    shape: negated_shape_id,
                },
            );
        }
    }
    Ok(())
}

fn handle_and_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.and_);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        if let Some(list_head) = terms.first() {
            let shape_terms = parse_rdf_list(context, list_head.clone());
            let shape_ids: Vec<ID> = shape_terms
                .into_iter()
                .map(|term| context.get_or_create_node_id(term))
                .collect();
            let component_id = context.get_or_create_component_id(list_head.clone());
            descriptors.insert(component_id, ComponentDescriptor::And { shapes: shape_ids });
        }
    }
    Ok(())
}

fn handle_or_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.or_);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        if let Some(list_head) = terms.first() {
            let shape_terms = parse_rdf_list(context, list_head.clone());
            let shape_ids: Vec<ID> = shape_terms
                .into_iter()
                .map(|term| context.get_or_create_node_id(term))
                .collect();
            let component_id = context.get_or_create_component_id(list_head.clone());
            descriptors.insert(component_id, ComponentDescriptor::Or { shapes: shape_ids });
        }
    }
    Ok(())
}

fn handle_xone_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.xone);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        if let Some(list_head) = terms.first() {
            let shape_terms = parse_rdf_list(context, list_head.clone());
            let shape_ids: Vec<ID> = shape_terms
                .into_iter()
                .map(|term| context.get_or_create_node_id(term))
                .collect();
            let component_id = context.get_or_create_component_id(list_head.clone());
            descriptors.insert(
                component_id,
                ComponentDescriptor::Xone { shapes: shape_ids },
            );
        }
    }
    Ok(())
}

fn handle_qualified_value_shape_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.qualified_value_shape);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        if let Some(qvs_term) = terms.first() {
            let shape_id = context.get_or_create_node_id(qvs_term.clone());
            let min_pred = owned_predicate(shacl.qualified_min_count);
            let max_pred = owned_predicate(shacl.qualified_max_count);
            let disjoint_pred = owned_predicate(shacl.qualified_value_shapes_disjoint);

            let min_count = pred_obj_pairs
                .get(&min_pred)
                .and_then(|terms| terms.first())
                .and_then(|term| match term {
                    Term::Literal(lit) => lit.value().parse::<u64>().ok(),
                    _ => None,
                });
            if pred_obj_pairs.contains_key(&min_pred) {
                processed.insert(min_pred.clone());
            }

            let max_count = pred_obj_pairs
                .get(&max_pred)
                .and_then(|terms| terms.first())
                .and_then(|term| match term {
                    Term::Literal(lit) => lit.value().parse::<u64>().ok(),
                    _ => None,
                });
            if pred_obj_pairs.contains_key(&max_pred) {
                processed.insert(max_pred.clone());
            }

            let disjoint = pred_obj_pairs
                .get(&disjoint_pred)
                .and_then(|terms| terms.first())
                .and_then(|term| match term {
                    Term::Literal(lit) => lit.value().parse::<bool>().ok(),
                    _ => None,
                });
            if pred_obj_pairs.contains_key(&disjoint_pred) {
                processed.insert(disjoint_pred.clone());
            }

            let key = Term::Literal(Literal::new_simple_literal(format!(
                "QualifiedValueShape:{}:{}:{}:{}",
                qvs_term,
                min_count.map(|v| v.to_string()).unwrap_or_default(),
                max_count.map(|v| v.to_string()).unwrap_or_default(),
                disjoint.map(|v| v.to_string()).unwrap_or_default()
            )));
            insert_descriptor(
                context,
                descriptors,
                key,
                ComponentDescriptor::QualifiedValueShape {
                    shape: shape_id,
                    min_count,
                    max_count,
                    disjoint,
                },
            );
        }
    }
    Ok(())
}

fn handle_closed_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.closed);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            if let Term::Literal(lit) = term {
                if let Ok(closed_val) = lit.value().parse::<bool>() {
                    let ignored_pred = owned_predicate(shacl.ignored_properties);
                    let ignored_terms = pred_obj_pairs
                        .get(&ignored_pred)
                        .and_then(|terms| terms.first().cloned());
                    if pred_obj_pairs.contains_key(&ignored_pred) {
                        processed.insert(ignored_pred.clone());
                    }
                    let ignored_values: Vec<Term> = if let Some(list_head) = ignored_terms.clone() {
                        parse_rdf_list(context, list_head)
                    } else {
                        Vec::new()
                    };
                    let key = Term::Literal(Literal::new_simple_literal(format!(
                        "ClosedConstraint:{}:{:?}",
                        closed_val, ignored_values
                    )));
                    insert_descriptor(
                        context,
                        descriptors,
                        key,
                        ComponentDescriptor::Closed {
                            closed: closed_val,
                            ignored_properties: ignored_values,
                        },
                    );
                }
            }
        }
    }
    Ok(())
}

fn handle_has_value_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.has_value);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        for term in terms {
            let key = Term::Literal(Literal::new_simple_literal(format!(
                "HasValueConstraint:{}",
                term
            )));
            insert_descriptor(
                context,
                descriptors,
                key,
                ComponentDescriptor::HasValue {
                    value: term.clone(),
                },
            );
        }
    }
    Ok(())
}

fn handle_in_constraint(
    shacl: &SHACL,
    _shape_term: &Term,
    context: &mut ParsingContext,
    _unique_lang: &HashMap<Term, String>,
    pred_obj_pairs: &HashMap<NamedNode, Vec<Term>>,
    processed: &mut HashSet<NamedNode>,
    descriptors: &mut HashMap<ComponentID, ComponentDescriptor>,
    _is_property_shape: bool,
) -> Result<(), String> {
    let predicate = owned_predicate(shacl.in_);
    if let Some(terms) = pred_obj_pairs.get(&predicate) {
        processed.insert(predicate.clone());
        if let Some(list_head) = terms.first() {
            let values = parse_rdf_list(context, list_head.clone());
            let component_id = context.get_or_create_component_id(list_head.clone());
            descriptors.insert(component_id, ComponentDescriptor::In { values });
        }
    }
    Ok(())
}
