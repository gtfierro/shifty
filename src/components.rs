use oxigraph::model::{Term, TermRef, SubjectRef, TripleRef};
use std::collections::HashMap;
use crate::context::ValidationContext;
use crate::types::ID;
use crate::named_nodes::SHACL;

pub trait ToSubjectRef {
    fn to_subject_ref(&self) -> SubjectRef;
}

impl ToSubjectRef for Term {
    fn to_subject_ref(&self) -> SubjectRef {
        match self {
            Term::NamedNode(n) => n.into(),
            Term::BlankNode(b) => b.into(),
            _ => panic!("Invalid subject term: {:?}", self),
        }
    }
}

impl<'a> ToSubjectRef for TermRef<'a> {
    fn to_subject_ref(&self) -> SubjectRef<'a> {
        match self {
            TermRef::NamedNode(nr) => nr.clone().into(),
            TermRef::BlankNode(br) => br.clone().into(),
            _ => panic!("Invalid subject term ref: {:?}", self),
        }
    }
}

fn parse_rdf_list(list_head: TermRef, context: &ValidationContext) -> Vec<Term> {
    let mut items = Vec::new();
    let mut current_node = list_head;
    let shacl = SHACL::new();

    while current_node != shacl.rdf_nil.into() {
        let subject_ref = match current_node {
            TermRef::NamedNode(n) => SubjectRef::NamedNode(n),
            TermRef::BlankNode(b) => SubjectRef::BlankNode(b),
            _ => return items, // Not a valid list node or rdf:nil
        };

        let first_term = context.shape_graph().object_for_subject_predicate(subject_ref, shacl.rdf_first);
        if let Some(item) = first_term {
            items.push(item.into_owned());
        } else {
            // Malformed list: node has no rdf:first
            return items; 
        }

        let rest_term = context.shape_graph().object_for_subject_predicate(subject_ref, shacl.rdf_rest);
        if let Some(rest) = rest_term {
            current_node = rest;
        } else {
            // Malformed list: node has no rdf:rest
            return items;
        }
    }
    items
}


pub fn parse_components(start: Term, context: &mut ValidationContext) -> Vec<Component> {
    let mut components = Vec::new();
    let shacl = SHACL::new();

    // make a list of all the predicate-object pairs for the given start term, as a dictionary
    let pred_obj_pairs: HashMap<TermRef, Vec<TermRef>> = context.shape_graph()
        .triples_for_subject(start.to_subject_ref())
        .fold(
            HashMap::new(),
            |mut acc, TripleRef{subject, predicate, object}| {
                let predicate: TermRef = predicate.into();
                let object: TermRef = object.into();
                if let Some(objects) = acc.get_mut(&predicate) {
                    objects.push(object);
                } else {
                    acc.insert(predicate.clone(), vec![object]);
                }
                acc
            },
        );

    // value type
    if let Some(class_terms) = pred_obj_pairs.get(&shacl.class.into()) {
        for class_term in class_terms {
            components.push(Component::ClassConstraint(ClassConstraintComponent {
                class: class_term.clone().into(),
            }));
        }
    }

    if let Some(datatype_terms) = pred_obj_pairs.get(&shacl.datatype.into()) {
        for datatype_term in datatype_terms {
            components.push(Component::DatatypeConstraint(DatatypeConstraintComponent {
                datatype: datatype_term.clone().into(),
            }));
        }
    }

    if let Some(node_kind_terms) = pred_obj_pairs.get(&shacl.node_kind.into()) {
        for node_kind_term in node_kind_terms {
            components.push(Component::NodeKindConstraint(NodeKindConstraintComponent {
                node_kind: node_kind_term.clone().into(),
            }));
        }
    }

    // node constraint component
    if let Some(node_terms) = pred_obj_pairs.get(&shacl.node.into()) {
        for node_term in node_terms {
            components.push(Component::NodeConstraint(NodeConstraintComponent {
                shape: context.get_or_create_id(node_term.clone().into()),
            }));
        }
    }

    // property constraints
    if let Some(property_terms) = pred_obj_pairs.get(&shacl.property.into()) {
        for property_term in property_terms {
            components.push(Component::PropertyConstraint(PropertyConstraintComponent {
                shape: context.get_or_create_id(property_term.clone().into()),
            }));
        }
    }


    // cardinality
    if let Some(min_count_terms) = pred_obj_pairs.get(&shacl.min_count.into()) {
        for min_count_term in min_count_terms {
            if let TermRef::Literal(lit) = min_count_term {
                if let Ok(min_count) = lit.value().parse::<u64>() {
                    components.push(Component::MinCount(MinCountConstraintComponent {
                        min_count,
                    }));
                }
            }
        }
    }

    if let Some(max_count_terms) = pred_obj_pairs.get(&shacl.max_count.into()) {
        for max_count_term in max_count_terms {
            if let TermRef::Literal(lit) = max_count_term {
                if let Ok(max_count) = lit.value().parse::<u64>() {
                    components.push(Component::MaxCount(MaxCountConstraintComponent {
                        max_count,
                    }));
                }
            }
        }
    }

    // value range
    if let Some(min_exclusive_terms) = pred_obj_pairs.get(&shacl.min_exclusive.into()) {
        for min_exclusive_term in min_exclusive_terms {
            if let TermRef::Literal(_lit) = min_exclusive_term { // Ensure it's a literal
                components.push(Component::MinExclusiveConstraint(
                    MinExclusiveConstraintComponent {
                        min_exclusive: min_exclusive_term.clone().into(),
                    },
                ));
            }
        }
    }

    if let Some(min_inclusive_terms) = pred_obj_pairs.get(&shacl.min_inclusive.into()) {
        for min_inclusive_term in min_inclusive_terms {
            if let TermRef::Literal(_lit) = min_inclusive_term {
                components.push(Component::MinInclusiveConstraint(
                    MinInclusiveConstraintComponent {
                        min_inclusive: min_inclusive_term.clone().into(),
                    },
                ));
            }
        }
    }

    if let Some(max_exclusive_terms) = pred_obj_pairs.get(&shacl.max_exclusive.into()) {
        for max_exclusive_term in max_exclusive_terms {
            if let TermRef::Literal(_lit) = max_exclusive_term {
                components.push(Component::MaxExclusiveConstraint(
                    MaxExclusiveConstraintComponent {
                        max_exclusive: max_exclusive_term.clone().into(),
                    },
                ));
            }
        }
    }

    if let Some(max_inclusive_terms) = pred_obj_pairs.get(&shacl.max_inclusive.into()) {
        for max_inclusive_term in max_inclusive_terms {
            if let TermRef::Literal(_lit) = max_inclusive_term {
                components.push(Component::MaxInclusiveConstraint(
                    MaxInclusiveConstraintComponent {
                        max_inclusive: max_inclusive_term.clone().into(),
                    },
                ));
            }
        }
    }

    // string-based constraints
    if let Some(min_length_terms) = pred_obj_pairs.get(&shacl.min_length.into()) {
        for min_length_term in min_length_terms {
            if let TermRef::Literal(lit) = min_length_term {
                if let Ok(min_length) = lit.value().parse::<u64>() {
                    components.push(Component::MinLengthConstraint(
                        MinLengthConstraintComponent { min_length },
                    ));
                }
            }
        }
    }

    if let Some(max_length_terms) = pred_obj_pairs.get(&shacl.max_length.into()) {
        for max_length_term in max_length_terms {
            if let TermRef::Literal(lit) = max_length_term {
                if let Ok(max_length) = lit.value().parse::<u64>() {
                    components.push(Component::MaxLengthConstraint(
                        MaxLengthConstraintComponent { max_length },
                    ));
                }
            }
        }
    }

    if let Some(pattern_terms) = pred_obj_pairs.get(&shacl.pattern.into()) {
        if let Some(TermRef::Literal(pattern_lit)) = pattern_terms.first() { // sh:pattern maxCount 1
            let pattern_str = pattern_lit.value().to_string();
            let flags_str = pred_obj_pairs.get(&shacl.flags.into())
                .and_then(|flags_terms| flags_terms.first())
                .and_then(|flag_term| if let TermRef::Literal(flag_lit) = flag_term { Some(flag_lit.value().to_string()) } else { None });
            components.push(Component::PatternConstraint(PatternConstraintComponent {
                pattern: pattern_str,
                flags: flags_str,
            }));
        }
    }

    if let Some(language_in_terms) = pred_obj_pairs.get(&shacl.language_in.into()) {
        if let Some(list_head_term) = language_in_terms.first() { // sh:languageIn maxCount 1
            let list_items = parse_rdf_list(list_head_term.clone(), context);
            let languages: Vec<String> = list_items.into_iter().filter_map(|term| {
                if let Term::Literal(lit) = term {
                    // TODO: Validate that datatype is xsd:string as per spec?
                    // For now, just extract the string value.
                    Some(lit.value().to_string())
                } else {
                    // Non-literal in languageIn list, should ideally be a validation error for the shapes graph.
                    None 
                }
            }).collect();

            components.push(Component::LanguageInConstraint(
                LanguageInConstraintComponent { languages },
            ));
        }
    }

    if let Some(unique_lang_terms) = pred_obj_pairs.get(&shacl.unique_lang.into()) {
        for unique_lang_term in unique_lang_terms { // sh:uniqueLang maxCount 1, but loop for safety
            if let TermRef::Literal(lit) = unique_lang_term {
                if let Ok(unique_lang) = lit.value().parse::<bool>() {
                    components.push(Component::UniqueLangConstraint(
                        UniqueLangConstraintComponent { unique_lang },
                    ));
                }
            }
        }
    }


    //// Qualified value shape constraints
    //for qvs in context.shape_graph().objects_for_subject_predicate(
    //    start.to_subject_ref(),
    //    shacl.qualified_value_shape,
    //) {
    //    let min_count = context.shape_graph()
    //        .object_for_subject_predicate(
    //            qvs.to_subject_ref(),
    //            shacl.min_count,
    //        )
    //        .and_then(|t| if let TermRef::Literal(lit) = t { lit.value().parse().ok() } else { None });
    //    let max_count = context.shape_graph()
    //        .object_for_subject_predicate(
    //            qvs.to_subject_ref(),
    //            shacl.max_count,
    //        )
    //        .and_then(|t| if let TermRef::Literal(lit) = t { lit.value().parse().ok() } else { None });
    //    let disjoint = context.shape_graph()
    //        .object_for_subject_predicate(
    //            qvs.to_subject_ref(),
    //            shacl.qualified_value_shapes_disjoint,
    //        )
    //        .and_then(|t| if let TermRef::Literal(lit) = t { lit.value().parse().ok() } else { None });
    //    components.push(Component::QualifiedValueShape(
    //        QualifiedValueShapeComponent {
    //            shape: context.get_or_create_id(qvs.clone().into()),
    //            min_count,
    //            max_count,
    //            disjoint,
    //        },
    //    ));
    //}
    components
}

pub enum Component {
    NodeConstraint(NodeConstraintComponent),
    PropertyConstraint(PropertyConstraintComponent),
    QualifiedValueShape(QualifiedValueShapeComponent),

    // value type
    ClassConstraint(ClassConstraintComponent),
    DatatypeConstraint(DatatypeConstraintComponent),
    NodeKindConstraint(NodeKindConstraintComponent),

    // cardinality constraints
    MinCount(MinCountConstraintComponent),
    MaxCount(MaxCountConstraintComponent),

    // value range constraints
    MinExclusiveConstraint(MinExclusiveConstraintComponent),
    MinInclusiveConstraint(MinInclusiveConstraintComponent),
    MaxExclusiveConstraint(MaxExclusiveConstraintComponent),
    MaxInclusiveConstraint(MaxInclusiveConstraintComponent),

    // string-based constraints
    MinLengthConstraint(MinLengthConstraintComponent),
    MaxLengthConstraint(MaxLengthConstraintComponent),
    PatternConstraint(PatternConstraintComponent),
    LanguageInConstraint(LanguageInConstraintComponent),
    UniqueLangConstraint(UniqueLangConstraintComponent),
}

// value type
pub struct ClassConstraintComponent {
    class: Term,
}

pub struct DatatypeConstraintComponent {
    datatype: Term,
}

pub struct NodeKindConstraintComponent {
    node_kind: Term,
}

pub struct NodeConstraintComponent {
    shape: ID,
}

pub struct PropertyConstraintComponent {
    shape: ID,
}

pub struct QualifiedValueShapeComponent {
    shape: ID,
    min_count: Option<u64>,
    max_count: Option<u64>,
    disjoint: Option<bool>,
}

pub struct MinCountConstraintComponent {
    min_count: u64,
}

pub struct MaxCountConstraintComponent {
    max_count: u64,
}

// value range constraints
pub struct MinExclusiveConstraintComponent {
    min_exclusive: Term,
}

pub struct MinInclusiveConstraintComponent {
    min_inclusive: Term,
}

pub struct MaxExclusiveConstraintComponent {
    max_exclusive: Term,
}

pub struct MaxInclusiveConstraintComponent {
    max_inclusive: Term,
}

// string-based constraints
pub struct MinLengthConstraintComponent {
    min_length: u64,
}

pub struct MaxLengthConstraintComponent {
    max_length: u64,
}

pub struct PatternConstraintComponent {
    pattern: String,
    flags: Option<String>,
}

pub struct LanguageInConstraintComponent {
    languages: Vec<String>,
}

pub struct UniqueLangConstraintComponent {
    unique_lang: bool,
}
