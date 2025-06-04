use crate::context::ValidationContext;
use crate::named_nodes::SHACL;
use crate::types::{ComponentID, ID}; // Added ComponentID
use oxigraph::model::{SubjectRef, Term, TermRef, TripleRef};
use std::collections::HashMap; // Removed RDF

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

pub fn parse_components(
    start: TermRef,
    context: &mut ValidationContext,
) -> HashMap<ComponentID, Component> {
    let mut new_components = HashMap::new();
    let shacl = SHACL::new();

    // make a list of all the predicate-object pairs for the given start term, as a dictionary
    let pred_obj_pairs: HashMap<TermRef, Vec<TermRef>> = context
        .shape_graph()
        .triples_for_subject(start.to_subject_ref())
        .fold(
            HashMap::new(),
            |mut acc,
             TripleRef {
                 subject,
                 predicate,
                 object,
             }| {
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
        for class_term_ref in class_terms {
            let component = Component::ClassConstraint(ClassConstraintComponent {
                class: class_term_ref.clone().into(),
            });
            let component_id = context.get_or_create_component_id(class_term_ref.into_owned());
            new_components.insert(component_id, component);
        }
    }

    if let Some(datatype_terms) = pred_obj_pairs.get(&shacl.datatype.into()) {
        for datatype_term_ref in datatype_terms {
            let component = Component::DatatypeConstraint(DatatypeConstraintComponent {
                datatype: datatype_term_ref.clone().into(),
            });
            let component_id = context.get_or_create_component_id(datatype_term_ref.into_owned());
            new_components.insert(component_id, component);
        }
    }

    if let Some(node_kind_terms) = pred_obj_pairs.get(&shacl.node_kind.into()) {
        for node_kind_term_ref in node_kind_terms {
            let component = Component::NodeKindConstraint(NodeKindConstraintComponent {
                node_kind: node_kind_term_ref.clone().into(),
            });
            let component_id = context.get_or_create_component_id(node_kind_term_ref.into_owned());
            new_components.insert(component_id, component);
        }
    }

    // node constraint component
    if let Some(node_terms) = pred_obj_pairs.get(&shacl.node.into()) {
        for node_term_ref in node_terms {
            let target_shape_id = context.get_or_create_node_id(node_term_ref.clone().into());
            let component = Component::NodeConstraint(NodeConstraintComponent {
                shape: target_shape_id,
            });
            let component_id = context.get_or_create_component_id(node_term_ref.into_owned());
            new_components.insert(component_id, component);
        }
    }

    // property constraints
    if let Some(property_terms) = pred_obj_pairs.get(&shacl.property.into()) {
        for property_term_ref in property_terms {
            let target_shape_id = context.get_or_create_node_id(property_term_ref.clone().into());
            let component = Component::PropertyConstraint(PropertyConstraintComponent {
                shape: target_shape_id,
            });
            let component_id = context.get_or_create_component_id(property_term_ref.into_owned());
            new_components.insert(component_id, component);
        }
    }

    // cardinality
    if let Some(min_count_terms) = pred_obj_pairs.get(&shacl.min_count.into()) {
        for min_count_term_ref in min_count_terms {
            if let TermRef::Literal(lit) = min_count_term_ref {
                if let Ok(min_count_val) = lit.value().parse::<u64>() {
                    let component = Component::MinCount(MinCountConstraintComponent {
                        min_count: min_count_val,
                    });
                    let component_id =
                        context.get_or_create_component_id(min_count_term_ref.into_owned());
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    if let Some(max_count_terms) = pred_obj_pairs.get(&shacl.max_count.into()) {
        for max_count_term_ref in max_count_terms {
            if let TermRef::Literal(lit) = max_count_term_ref {
                if let Ok(max_count_val) = lit.value().parse::<u64>() {
                    let component = Component::MaxCount(MaxCountConstraintComponent {
                        max_count: max_count_val,
                    });
                    let component_id =
                        context.get_or_create_component_id(max_count_term_ref.into_owned());
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    // value range
    if let Some(min_exclusive_terms) = pred_obj_pairs.get(&shacl.min_exclusive.into()) {
        for min_exclusive_term_ref in min_exclusive_terms {
            if let TermRef::Literal(_lit) = min_exclusive_term_ref {
                // Ensure it's a literal
                let component =
                    Component::MinExclusiveConstraint(MinExclusiveConstraintComponent {
                        min_exclusive: min_exclusive_term_ref.clone().into(),
                    });
                let component_id =
                    context.get_or_create_component_id(min_exclusive_term_ref.into_owned());
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(min_inclusive_terms) = pred_obj_pairs.get(&shacl.min_inclusive.into()) {
        for min_inclusive_term_ref in min_inclusive_terms {
            if let TermRef::Literal(_lit) = min_inclusive_term_ref {
                let component =
                    Component::MinInclusiveConstraint(MinInclusiveConstraintComponent {
                        min_inclusive: min_inclusive_term_ref.clone().into(),
                    });
                let component_id =
                    context.get_or_create_component_id(min_inclusive_term_ref.into_owned());
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(max_exclusive_terms) = pred_obj_pairs.get(&shacl.max_exclusive.into()) {
        for max_exclusive_term_ref in max_exclusive_terms {
            if let TermRef::Literal(_lit) = max_exclusive_term_ref {
                let component =
                    Component::MaxExclusiveConstraint(MaxExclusiveConstraintComponent {
                        max_exclusive: max_exclusive_term_ref.clone().into(),
                    });
                let component_id =
                    context.get_or_create_component_id(max_exclusive_term_ref.into_owned());
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(max_inclusive_terms) = pred_obj_pairs.get(&shacl.max_inclusive.into()) {
        for max_inclusive_term_ref in max_inclusive_terms {
            if let TermRef::Literal(_lit) = max_inclusive_term_ref {
                let component =
                    Component::MaxInclusiveConstraint(MaxInclusiveConstraintComponent {
                        max_inclusive: max_inclusive_term_ref.clone().into(),
                    });
                let component_id =
                    context.get_or_create_component_id(max_inclusive_term_ref.into_owned());
                new_components.insert(component_id, component);
            }
        }
    }

    // string-based constraints
    if let Some(min_length_terms) = pred_obj_pairs.get(&shacl.min_length.into()) {
        for min_length_term_ref in min_length_terms {
            if let TermRef::Literal(lit) = min_length_term_ref {
                if let Ok(min_length_val) = lit.value().parse::<u64>() {
                    let component = Component::MinLengthConstraint(MinLengthConstraintComponent {
                        min_length: min_length_val,
                    });
                    let component_id =
                        context.get_or_create_component_id(min_length_term_ref.into_owned());
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    if let Some(max_length_terms) = pred_obj_pairs.get(&shacl.max_length.into()) {
        for max_length_term_ref in max_length_terms {
            if let TermRef::Literal(lit) = max_length_term_ref {
                if let Ok(max_length_val) = lit.value().parse::<u64>() {
                    let component = Component::MaxLengthConstraint(MaxLengthConstraintComponent {
                        max_length: max_length_val,
                    });
                    let component_id =
                        context.get_or_create_component_id(max_length_term_ref.into_owned());
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    if let Some(pattern_terms) = pred_obj_pairs.get(&shacl.pattern.into()) {
        if let Some(pattern_term_ref @ TermRef::Literal(pattern_lit)) = pattern_terms.first() {
            // sh:pattern maxCount 1
            let pattern_str = pattern_lit.value().to_string();
            let flags_str = pred_obj_pairs
                .get(&shacl.flags.into())
                .and_then(|flags_terms| flags_terms.first())
                .and_then(|flag_term| {
                    if let TermRef::Literal(flag_lit) = flag_term {
                        Some(flag_lit.value().to_string())
                    } else {
                        None
                    }
                });
            let component = Component::PatternConstraint(PatternConstraintComponent {
                pattern: pattern_str,
                flags: flags_str,
            });
            let component_id = context.get_or_create_component_id(pattern_term_ref.into_owned());
            new_components.insert(component_id, component);
        }
    }

    if let Some(language_in_terms) = pred_obj_pairs.get(&shacl.language_in.into()) {
        if let Some(list_head_term_ref) = language_in_terms.first() {
            // sh:languageIn maxCount 1
            let list_items = context.parse_rdf_list(list_head_term_ref.clone());
            let languages: Vec<String> = list_items
                .into_iter()
                .filter_map(|term| {
                    let term_owned = term.into_owned(); // Use into_owned() to avoid lifetime issues if term is used later
                    if let Term::Literal(lit) = term_owned {
                        // TODO: Validate that datatype is xsd:string as per spec?
                        // For now, just extract the string value.
                        Some(lit.value().to_string())
                    } else {
                        // Non-literal in languageIn list, should ideally be a validation error for the shapes graph.
                        None
                    }
                })
                .collect();

            let component =
                Component::LanguageInConstraint(LanguageInConstraintComponent { languages });
            let component_id = context.get_or_create_component_id(list_head_term_ref.into_owned());
            new_components.insert(component_id, component);
        }
    }

    if let Some(unique_lang_terms) = pred_obj_pairs.get(&shacl.unique_lang.into()) {
        for unique_lang_term_ref in unique_lang_terms {
            // sh:uniqueLang maxCount 1, but loop for safety
            if let TermRef::Literal(lit) = unique_lang_term_ref {
                if let Ok(unique_lang_val) = lit.value().parse::<bool>() {
                    let component =
                        Component::UniqueLangConstraint(UniqueLangConstraintComponent {
                            unique_lang: unique_lang_val,
                        });
                    let component_id =
                        context.get_or_create_component_id(unique_lang_term_ref.into_owned());
                    new_components.insert(component_id, component);
                }
            }
        }
    }
    new_components
}

#[derive(Debug)]
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

impl Component {
    pub fn label(&self) -> String {
        match self {
            Component::NodeConstraint(_) => "NodeConstraint".to_string(),
            Component::PropertyConstraint(_) => "PropertyConstraint".to_string(),
            Component::QualifiedValueShape(_) => "QualifiedValueShape".to_string(),

            Component::ClassConstraint(_) => "ClassConstraint".to_string(),
            Component::DatatypeConstraint(_) => "DatatypeConstraint".to_string(),
            Component::NodeKindConstraint(_) => "NodeKindConstraint".to_string(),

            Component::MinCount(_) => "MinCount".to_string(),
            Component::MaxCount(_) => "MaxCount".to_string(),

            Component::MinExclusiveConstraint(_) => "MinExclusiveConstraint".to_string(),
            Component::MinInclusiveConstraint(_) => "MinInclusiveConstraint".to_string(),
            Component::MaxExclusiveConstraint(_) => "MaxExclusiveConstraint".to_string(),
            Component::MaxInclusiveConstraint(_) => "MaxInclusiveConstraint".to_string(),

            Component::MinLengthConstraint(_) => "MinLengthConstraint".to_string(),
            Component::MaxLengthConstraint(_) => "MaxLengthConstraint".to_string(),
            Component::PatternConstraint(_) => "PatternConstraint".to_string(),
            Component::LanguageInConstraint(_) => "LanguageInConstraint".to_string(),
            Component::UniqueLangConstraint(_) => "UniqueLangConstraint".to_string(),
        }
    }
}

// value type
#[derive(Debug)]
pub struct ClassConstraintComponent {
    class: Term,
}

#[derive(Debug)]
pub struct DatatypeConstraintComponent {
    datatype: Term,
}

#[derive(Debug)]
pub struct NodeKindConstraintComponent {
    node_kind: Term,
}

#[derive(Debug)]
pub struct NodeConstraintComponent {
    shape: ID,
}

#[derive(Debug)]
pub struct PropertyConstraintComponent {
    shape: ID,
}

#[derive(Debug)]
pub struct QualifiedValueShapeComponent {
    shape: ID,
    min_count: Option<u64>,
    max_count: Option<u64>,
    disjoint: Option<bool>,
}

#[derive(Debug)]
pub struct MinCountConstraintComponent {
    min_count: u64,
}

#[derive(Debug)]
pub struct MaxCountConstraintComponent {
    max_count: u64,
}

// value range constraints
#[derive(Debug)]
pub struct MinExclusiveConstraintComponent {
    min_exclusive: Term,
}

#[derive(Debug)]
pub struct MinInclusiveConstraintComponent {
    min_inclusive: Term,
}

#[derive(Debug)]
pub struct MaxExclusiveConstraintComponent {
    max_exclusive: Term,
}

#[derive(Debug)]
pub struct MaxInclusiveConstraintComponent {
    max_inclusive: Term,
}

// string-based constraints
#[derive(Debug)]
pub struct MinLengthConstraintComponent {
    min_length: u64,
}

#[derive(Debug)]
pub struct MaxLengthConstraintComponent {
    max_length: u64,
}

#[derive(Debug)]
pub struct PatternConstraintComponent {
    pattern: String,
    flags: Option<String>,
}

#[derive(Debug)]
pub struct LanguageInConstraintComponent {
    languages: Vec<String>,
}

#[derive(Debug)]
pub struct UniqueLangConstraintComponent {
    unique_lang: bool,
}
