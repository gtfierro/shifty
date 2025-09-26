use std::collections::{HashMap, HashSet};

use oxigraph::model::{Literal, NamedNode, SubjectRef, Term, TermRef};

use crate::model::components::sparql::CustomConstraintComponentDefinition;
use crate::model::components::ComponentDescriptor;
use crate::named_nodes::SHACL;
use crate::types::{ComponentID, ID};

use super::{parse_rdf_list, ParsingContext};

/// Parses all constraint components attached to a given shape subject (`start`) from the shapes graph.
///
/// Returns data-only `ComponentDescriptor`s keyed by `ComponentID` for later runtime instantiation.
pub(crate) fn parse_components(
    shape_term: &Term,
    context: &mut ParsingContext,
    unique_lang_lexicals: &HashMap<Term, String>,
) -> HashMap<ComponentID, ComponentDescriptor> {
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

    // value type
    if let Some(class_terms) = pred_obj_pairs.get(&shacl.class.into_owned()) {
        processed_predicates.insert(shacl.class.into_owned());
        for class_term in class_terms {
            let component_id = context.get_or_create_component_id(Term::Literal(
                Literal::new_simple_literal(format!("ClassConstraint:{}", class_term)),
            ));
            descriptors.insert(
                component_id,
                ComponentDescriptor::Class {
                    class: class_term.clone(),
                },
            );
        }
    }

    if let Some(datatype_terms) = pred_obj_pairs.get(&shacl.datatype.into_owned()) {
        processed_predicates.insert(shacl.datatype.into_owned());
        for datatype_term in datatype_terms {
            let component_id = context.get_or_create_component_id(Term::Literal(
                Literal::new_simple_literal(format!("DatatypeConstraint:{}", datatype_term)),
            ));
            descriptors.insert(
                component_id,
                ComponentDescriptor::Datatype {
                    datatype: datatype_term.clone(),
                },
            );
        }
    }

    if let Some(node_kind_terms) = pred_obj_pairs.get(&shacl.node_kind.into_owned()) {
        processed_predicates.insert(shacl.node_kind.into_owned());
        for node_kind_term in node_kind_terms {
            let component_id = context.get_or_create_component_id(Term::Literal(
                Literal::new_simple_literal(format!("NodeKindConstraint:{}", node_kind_term)),
            ));
            descriptors.insert(
                component_id,
                ComponentDescriptor::NodeKind {
                    node_kind: node_kind_term.clone(),
                },
            );
        }
    }

    // node constraint component
    if let Some(node_terms) = pred_obj_pairs.get(&shacl.node.into_owned()) {
        processed_predicates.insert(shacl.node.into_owned());
        for node_term in node_terms {
            let target_shape_id = context.get_or_create_node_id(node_term.clone());
            let component_id = context.get_or_create_component_id(Term::Literal(
                Literal::new_simple_literal(format!("NodeConstraint:{}", node_term)),
            ));
            descriptors.insert(
                component_id,
                ComponentDescriptor::Node {
                    shape: target_shape_id,
                },
            );
        }
    }

    // property constraints
    if let Some(property_terms) = pred_obj_pairs.get(&shacl.property.into_owned()) {
        processed_predicates.insert(shacl.property.into_owned());
        for property_term in property_terms {
            let target_shape_id = context.get_or_create_prop_id(property_term.clone());
            let component_id = context.get_or_create_component_id(Term::Literal(
                Literal::new_simple_literal(format!("PropertyConstraint:{}", property_term)),
            ));
            descriptors.insert(
                component_id,
                ComponentDescriptor::Property {
                    shape: target_shape_id,
                },
            );
        }
    }

    // cardinality
    if let Some(min_count_terms) = pred_obj_pairs.get(&shacl.min_count.into_owned()) {
        processed_predicates.insert(shacl.min_count.into_owned());
        for min_count_term in min_count_terms {
            if let Term::Literal(lit) = min_count_term {
                if let Ok(min_count_val) = lit.value().parse::<u64>() {
                    let component_id = context.get_or_create_component_id(Term::Literal(
                        Literal::new_simple_literal(format!(
                            "MinCountConstraint:{}",
                            min_count_term
                        )),
                    ));
                    descriptors.insert(
                        component_id,
                        ComponentDescriptor::MinCount {
                            min_count: min_count_val,
                        },
                    );
                }
            }
        }
    }

    if let Some(max_count_terms) = pred_obj_pairs.get(&shacl.max_count.into_owned()) {
        processed_predicates.insert(shacl.max_count.into_owned());
        for max_count_term in max_count_terms {
            if let Term::Literal(lit) = max_count_term {
                if let Ok(max_count_val) = lit.value().parse::<u64>() {
                    let component_id = context.get_or_create_component_id(Term::Literal(
                        Literal::new_simple_literal(format!(
                            "MaxCountConstraint:{}",
                            max_count_term
                        )),
                    ));
                    descriptors.insert(
                        component_id,
                        ComponentDescriptor::MaxCount {
                            max_count: max_count_val,
                        },
                    );
                }
            }
        }
    }

    // value range
    if let Some(min_exclusive_terms) = pred_obj_pairs.get(&shacl.min_exclusive.into_owned()) {
        processed_predicates.insert(shacl.min_exclusive.into_owned());
        for min_exclusive_term in min_exclusive_terms {
            let component_id = context.get_or_create_component_id(Term::Literal(
                Literal::new_simple_literal(format!(
                    "MinExclusiveConstraint:{}:{}",
                    shape_term, min_exclusive_term
                )),
            ));
            descriptors.insert(
                component_id,
                ComponentDescriptor::MinExclusive {
                    value: min_exclusive_term.clone(),
                },
            );
        }
    }

    if let Some(min_inclusive_terms) = pred_obj_pairs.get(&shacl.min_inclusive.into_owned()) {
        processed_predicates.insert(shacl.min_inclusive.into_owned());
        for min_inclusive_term in min_inclusive_terms {
            let component_id = context.get_or_create_component_id(Term::Literal(
                Literal::new_simple_literal(format!(
                    "MinInclusiveConstraint:{}:{}",
                    shape_term, min_inclusive_term
                )),
            ));
            descriptors.insert(
                component_id,
                ComponentDescriptor::MinInclusive {
                    value: min_inclusive_term.clone(),
                },
            );
        }
    }

    if let Some(max_exclusive_terms) = pred_obj_pairs.get(&shacl.max_exclusive.into_owned()) {
        processed_predicates.insert(shacl.max_exclusive.into_owned());
        for max_exclusive_term in max_exclusive_terms {
            let component_id = context.get_or_create_component_id(Term::Literal(
                Literal::new_simple_literal(format!(
                    "MaxExclusiveConstraint:{}:{}",
                    shape_term, max_exclusive_term
                )),
            ));
            descriptors.insert(
                component_id,
                ComponentDescriptor::MaxExclusive {
                    value: max_exclusive_term.clone(),
                },
            );
        }
    }

    if let Some(max_inclusive_terms) = pred_obj_pairs.get(&shacl.max_inclusive.into_owned()) {
        processed_predicates.insert(shacl.max_inclusive.into_owned());
        for max_inclusive_term in max_inclusive_terms {
            let component_id = context.get_or_create_component_id(Term::Literal(
                Literal::new_simple_literal(format!(
                    "MaxInclusiveConstraint:{}:{}",
                    shape_term, max_inclusive_term
                )),
            ));
            descriptors.insert(
                component_id,
                ComponentDescriptor::MaxInclusive {
                    value: max_inclusive_term.clone(),
                },
            );
        }
    }

    // string-based constraints
    if let Some(min_length_terms) = pred_obj_pairs.get(&shacl.min_length.into_owned()) {
        processed_predicates.insert(shacl.min_length.into_owned());
        for min_length_term in min_length_terms {
            if let Term::Literal(lit) = min_length_term {
                if let Ok(min_length_val) = lit.value().parse::<u64>() {
                    let component_id = context.get_or_create_component_id(Term::Literal(
                        Literal::new_simple_literal(format!(
                            "MinLengthConstraint:{}",
                            min_length_term
                        )),
                    ));
                    descriptors.insert(
                        component_id,
                        ComponentDescriptor::MinLength {
                            length: min_length_val,
                        },
                    );
                }
            }
        }
    }

    if let Some(max_length_terms) = pred_obj_pairs.get(&shacl.max_length.into_owned()) {
        processed_predicates.insert(shacl.max_length.into_owned());
        for max_length_term in max_length_terms {
            if let Term::Literal(lit) = max_length_term {
                if let Ok(max_length_val) = lit.value().parse::<u64>() {
                    let component_id = context.get_or_create_component_id(Term::Literal(
                        Literal::new_simple_literal(format!(
                            "MaxLengthConstraint:{}",
                            max_length_term
                        )),
                    ));
                    descriptors.insert(
                        component_id,
                        ComponentDescriptor::MaxLength {
                            length: max_length_val,
                        },
                    );
                }
            }
        }
    }

    if let Some(pattern_terms) = pred_obj_pairs.get(&shacl.pattern.into_owned()) {
        processed_predicates.insert(shacl.pattern.into_owned());
        if let Some(Term::Literal(pattern_lit)) = pattern_terms.first() {
            let pattern_str = pattern_lit.value().to_string();
            let flags_str = pred_obj_pairs
                .get(&shacl.flags.into_owned())
                .and_then(|flags_terms| flags_terms.first())
                .and_then(|flag_term| match flag_term {
                    Term::Literal(flag_lit) => Some(flag_lit.value().to_string()),
                    _ => None,
                });
            let component_id = context.get_or_create_component_id(Term::Literal(
                Literal::new_simple_literal(format!(
                    "PatternConstraint:{}:{}",
                    pattern_str,
                    flags_str.as_deref().unwrap_or("")
                )),
            ));
            let flags_clone = flags_str.clone();
            descriptors.insert(
                component_id,
                ComponentDescriptor::Pattern {
                    pattern: pattern_str,
                    flags: flags_clone,
                },
            );
            if flags_str.is_some() {
                processed_predicates.insert(shacl.flags.into_owned());
            }
        }
    }

    if let Some(language_in_terms) = pred_obj_pairs.get(&shacl.language_in.into_owned()) {
        processed_predicates.insert(shacl.language_in.into_owned());
        if let Some(list_head_term) = language_in_terms.first() {
            let list_items = parse_rdf_list(context, list_head_term.clone());
            let languages: Vec<String> = list_items
                .into_iter()
                .filter_map(|term| match term {
                    Term::Literal(lit) => Some(lit.value().to_string()),
                    _ => None,
                })
                .collect();
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            descriptors.insert(component_id, ComponentDescriptor::LanguageIn { languages });
        }
    }

    if let Some(unique_lang_terms) = pred_obj_pairs.get(&shacl.unique_lang.into_owned()) {
        processed_predicates.insert(shacl.unique_lang.into_owned());
        let original_lexical = unique_lang_lexicals.get(shape_term);
        println!(
            "uniqueLang parsing for shape {:?}, map entry: {:?}",
            shape_term, original_lexical
        );
        for unique_lang_term in unique_lang_terms {
            if let Term::Literal(lit) = unique_lang_term {
                let lexical = original_lexical
                    .map(|value| value.as_str())
                    .unwrap_or_else(|| lit.value());
                if lexical.eq_ignore_ascii_case("true") {
                    let component_id = context.get_or_create_component_id(Term::Literal(
                        Literal::new_simple_literal(format!(
                            "UniqueLangConstraint:{}",
                            unique_lang_term
                        )),
                    ));
                    descriptors.insert(
                        component_id,
                        ComponentDescriptor::UniqueLang { enabled: true },
                    );
                }
            }
        }
    }

    // property pair constraints
    if let Some(equals_terms) = pred_obj_pairs.get(&shacl.equals.into_owned()) {
        processed_predicates.insert(shacl.equals.into_owned());
        for equals_term in equals_terms {
            if let Term::NamedNode(_) = equals_term {
                let component_id = context.get_or_create_component_id(Term::Literal(
                    Literal::new_simple_literal(format!("EqualsConstraint:{}", equals_term)),
                ));
                descriptors.insert(
                    component_id,
                    ComponentDescriptor::Equals {
                        property: equals_term.clone(),
                    },
                );
            }
        }
    }

    if let Some(disjoint_terms) = pred_obj_pairs.get(&shacl.disjoint.into_owned()) {
        processed_predicates.insert(shacl.disjoint.into_owned());
        for disjoint_term in disjoint_terms {
            if let Term::NamedNode(_) = disjoint_term {
                let component_id = context.get_or_create_component_id(Term::Literal(
                    Literal::new_simple_literal(format!("DisjointConstraint:{}", disjoint_term)),
                ));
                descriptors.insert(
                    component_id,
                    ComponentDescriptor::Disjoint {
                        property: disjoint_term.clone(),
                    },
                );
            }
        }
    }

    if let Some(less_than_terms) = pred_obj_pairs.get(&shacl.less_than.into_owned()) {
        processed_predicates.insert(shacl.less_than.into_owned());
        for less_than_term in less_than_terms {
            if let Term::NamedNode(_) = less_than_term {
                let component_id = context.get_or_create_component_id(Term::Literal(
                    Literal::new_simple_literal(format!("LessThanConstraint:{}", less_than_term)),
                ));
                descriptors.insert(
                    component_id,
                    ComponentDescriptor::LessThan {
                        property: less_than_term.clone(),
                    },
                );
            }
        }
    }

    if let Some(less_than_or_equals_terms) =
        pred_obj_pairs.get(&shacl.less_than_or_equals.into_owned())
    {
        processed_predicates.insert(shacl.less_than_or_equals.into_owned());
        for less_than_or_equals_term in less_than_or_equals_terms {
            if let Term::NamedNode(_) = less_than_or_equals_term {
                let component_id =
                    context.get_or_create_component_id(Term::Literal(Literal::new_simple_literal(
                        format!("LessThanOrEqualsConstraint:{}", less_than_or_equals_term),
                    )));
                descriptors.insert(
                    component_id,
                    ComponentDescriptor::LessThanOrEquals {
                        property: less_than_or_equals_term.clone(),
                    },
                );
            }
        }
    }

    // logical constraints
    if let Some(not_terms) = pred_obj_pairs.get(&shacl.not.into_owned()) {
        processed_predicates.insert(shacl.not.into_owned());
        for not_term in not_terms {
            let negated_shape_id = context.get_or_create_node_id(not_term.clone());
            let component_id = context.get_or_create_component_id(Term::Literal(
                Literal::new_simple_literal(format!("NotConstraint:{}", not_term)),
            ));
            descriptors.insert(
                component_id,
                ComponentDescriptor::Not {
                    shape: negated_shape_id,
                },
            );
        }
    }

    if let Some(and_terms) = pred_obj_pairs.get(&shacl.and_.into_owned()) {
        processed_predicates.insert(shacl.and_.into_owned());
        if let Some(list_head_term) = and_terms.first() {
            let shape_list_terms = parse_rdf_list(context, list_head_term.clone());
            let shape_ids: Vec<ID> = shape_list_terms
                .into_iter()
                .filter_map(|term| Some(context.get_or_create_node_id(term)))
                .collect();
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            descriptors.insert(component_id, ComponentDescriptor::And { shapes: shape_ids });
        }
    }

    if let Some(or_terms) = pred_obj_pairs.get(&shacl.or_.into_owned()) {
        processed_predicates.insert(shacl.or_.into_owned());
        if let Some(list_head_term) = or_terms.first() {
            let shape_list_terms = parse_rdf_list(context, list_head_term.clone());
            let shape_ids: Vec<ID> = shape_list_terms
                .into_iter()
                .filter_map(|term| Some(context.get_or_create_node_id(term)))
                .collect();
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            descriptors.insert(component_id, ComponentDescriptor::Or { shapes: shape_ids });
        }
    }

    if let Some(xone_terms) = pred_obj_pairs.get(&shacl.xone.into_owned()) {
        processed_predicates.insert(shacl.xone.into_owned());
        if let Some(list_head_term) = xone_terms.first() {
            let shape_list_terms = parse_rdf_list(context, list_head_term.clone());
            let shape_ids: Vec<ID> = shape_list_terms
                .into_iter()
                .filter_map(|term| Some(context.get_or_create_node_id(term)))
                .collect();
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            descriptors.insert(
                component_id,
                ComponentDescriptor::Xone { shapes: shape_ids },
            );
        }
    }

    // Qualified Value Shape
    if let Some(qvs_terms) = pred_obj_pairs.get(&shacl.qualified_value_shape.into_owned()) {
        processed_predicates.insert(shacl.qualified_value_shape.into_owned());
        if let Some(qvs_term) = qvs_terms.first() {
            let q_min_count_opt = pred_obj_pairs
                .get(&shacl.qualified_min_count.into_owned())
                .and_then(|terms| terms.first())
                .and_then(|term_ref_val| match term_ref_val {
                    Term::Literal(lit) => lit.value().parse::<u64>().ok(),
                    _ => None,
                });
            if q_min_count_opt.is_some() {
                processed_predicates.insert(shacl.qualified_min_count.into_owned());
            }
            let q_max_count_opt = pred_obj_pairs
                .get(&shacl.qualified_max_count.into_owned())
                .and_then(|terms| terms.first())
                .and_then(|term_ref_val| match term_ref_val {
                    Term::Literal(lit) => lit.value().parse::<u64>().ok(),
                    _ => None,
                });
            if q_max_count_opt.is_some() {
                processed_predicates.insert(shacl.qualified_max_count.into_owned());
            }
            let q_disjoint_opt = pred_obj_pairs
                .get(&shacl.qualified_value_shapes_disjoint.into_owned())
                .and_then(|terms| terms.first())
                .and_then(|term_ref_val| match term_ref_val {
                    Term::Literal(lit) => lit.value().parse::<bool>().ok(),
                    _ => None,
                });
            if q_disjoint_opt.is_some() {
                processed_predicates.insert(shacl.qualified_value_shapes_disjoint.into_owned());
            }
            let shape_id = context.get_or_create_node_id(qvs_term.clone());
            let component_id = context.get_or_create_component_id(Term::Literal(
                Literal::new_simple_literal(format!(
                    "QualifiedValueShape:{}:{}:{}:{}",
                    qvs_term,
                    q_min_count_opt.map(|v| v.to_string()).unwrap_or_default(),
                    q_max_count_opt.map(|v| v.to_string()).unwrap_or_default(),
                    q_disjoint_opt.map(|v| v.to_string()).unwrap_or_default()
                )),
            ));
            descriptors.insert(
                component_id,
                ComponentDescriptor::QualifiedValueShape {
                    shape: shape_id,
                    min_count: q_min_count_opt,
                    max_count: q_max_count_opt,
                    disjoint: q_disjoint_opt,
                },
            );
        }
    }

    // Other Constraint Components
    if let Some(closed_terms) = pred_obj_pairs.get(&shacl.closed.into_owned()) {
        processed_predicates.insert(shacl.closed.into_owned());
        for closed_term in closed_terms {
            if let Term::Literal(lit) = closed_term {
                if let Ok(closed_val) = lit.value().parse::<bool>() {
                    let ignored_properties_list_opt = pred_obj_pairs
                        .get(&shacl.ignored_properties.into_owned())
                        .and_then(|terms| terms.first().cloned());
                    if ignored_properties_list_opt.is_some() {
                        processed_predicates.insert(shacl.ignored_properties.into_owned());
                    }
                    let ignored_properties_terms: Vec<Term> =
                        if let Some(list_head) = ignored_properties_list_opt {
                            parse_rdf_list(context, list_head)
                        } else {
                            Vec::new()
                        };
                    let component_id = context.get_or_create_component_id(Term::Literal(
                        Literal::new_simple_literal(format!(
                            "ClosedConstraint:{}:{:?}",
                            closed_val, ignored_properties_terms
                        )),
                    ));
                    descriptors.insert(
                        component_id,
                        ComponentDescriptor::Closed {
                            closed: closed_val,
                            ignored_properties: ignored_properties_terms,
                        },
                    );
                }
            }
        }
    }

    if let Some(has_value_terms) = pred_obj_pairs.get(&shacl.has_value.into_owned()) {
        processed_predicates.insert(shacl.has_value.into_owned());
        for has_value_term in has_value_terms {
            let component_id = context.get_or_create_component_id(Term::Literal(
                Literal::new_simple_literal(format!("HasValueConstraint:{}", has_value_term)),
            ));
            descriptors.insert(
                component_id,
                ComponentDescriptor::HasValue {
                    value: has_value_term.clone(),
                },
            );
        }
    }

    if let Some(in_terms) = pred_obj_pairs.get(&shacl.in_.into_owned()) {
        processed_predicates.insert(shacl.in_.into_owned());
        if let Some(list_head_term) = in_terms.first() {
            let list_items = parse_rdf_list(context, list_head_term.clone());
            let values: Vec<Term> = list_items;
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            descriptors.insert(component_id, ComponentDescriptor::In { values });
        }
    }

    if let Some(sparql_terms) = pred_obj_pairs.get(&shacl.sparql.into_owned()) {
        processed_predicates.insert(shacl.sparql.into_owned());
        for sparql_term in sparql_terms {
            let component_id = context.get_or_create_component_id(sparql_term.clone());
            descriptors.insert(
                component_id,
                ComponentDescriptor::Sparql {
                    constraint_node: sparql_term.clone(),
                },
            );
        }
    }

    let (custom_component_defs, param_to_component) = parse_custom_constraint_components(context);

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

    descriptors
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
) -> (
    HashMap<NamedNode, CustomConstraintComponentDefinition>,
    HashMap<NamedNode, Vec<NamedNode>>,
) {
    crate::runtime::parse_custom_constraint_components(context)
}
