use crate::context::{format_term_for_label, sanitize_graphviz_string, ValidationContext};
use crate::named_nodes::SHACL;
use crate::types::{ComponentID, ID, PropShapeID};
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
            let target_shape_id = context.get_or_create_prop_id(property_term_ref.clone().into());
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

    // property pair constraints
    if let Some(equals_terms) = pred_obj_pairs.get(&shacl.equals.into()) {
        for equals_term_ref in equals_terms {
            if let TermRef::NamedNode(_nn) = equals_term_ref {
                let component = Component::EqualsConstraint(EqualsConstraintComponent {
                    property: equals_term_ref.clone().into(),
                });
                let component_id =
                    context.get_or_create_component_id(equals_term_ref.into_owned());
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(disjoint_terms) = pred_obj_pairs.get(&shacl.disjoint.into()) {
        for disjoint_term_ref in disjoint_terms {
            if let TermRef::NamedNode(_nn) = disjoint_term_ref {
                let component = Component::DisjointConstraint(DisjointConstraintComponent {
                    property: disjoint_term_ref.clone().into(),
                });
                let component_id =
                    context.get_or_create_component_id(disjoint_term_ref.into_owned());
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(less_than_terms) = pred_obj_pairs.get(&shacl.less_than.into()) {
        for less_than_term_ref in less_than_terms {
            if let TermRef::NamedNode(_nn) = less_than_term_ref {
                let component = Component::LessThanConstraint(LessThanConstraintComponent {
                    property: less_than_term_ref.clone().into(),
                });
                let component_id =
                    context.get_or_create_component_id(less_than_term_ref.into_owned());
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(less_than_or_equals_terms) =
        pred_obj_pairs.get(&shacl.less_than_or_equals.into())
    {
        for less_than_or_equals_term_ref in less_than_or_equals_terms {
            if let TermRef::NamedNode(_nn) = less_than_or_equals_term_ref {
                let component =
                    Component::LessThanOrEqualsConstraint(LessThanOrEqualsConstraintComponent {
                        property: less_than_or_equals_term_ref.clone().into(),
                    });
                let component_id =
                    context.get_or_create_component_id(less_than_or_equals_term_ref.into_owned());
                new_components.insert(component_id, component);
            }
        }
    }

    // logical constraints
    if let Some(not_terms) = pred_obj_pairs.get(&shacl.not.into()) {
        for not_term_ref in not_terms {
            // Assuming sh:not takes a single shape
            let negated_shape_id = context.get_or_create_node_id(not_term_ref.clone().into());
            let component = Component::NotConstraint(NotConstraintComponent {
                shape: negated_shape_id,
            });
            let component_id = context.get_or_create_component_id(not_term_ref.into_owned());
            new_components.insert(component_id, component);
        }
    }

    if let Some(and_terms) = pred_obj_pairs.get(&shacl.and_.into()) {
        // sh:and expects a list of shapes
        if let Some(list_head_ref) = and_terms.first() {
            let shape_list = context.parse_rdf_list(list_head_ref.clone());
            let shape_ids: Vec<ID> = shape_list
                .iter()
                .map(|term_ref| context.get_or_create_node_id(term_ref.clone().into()))
                .collect();
            let component = Component::AndConstraint(AndConstraintComponent { shapes: shape_ids });
            let component_id = context.get_or_create_component_id(list_head_ref.into_owned());
            new_components.insert(component_id, component);
        }
    }

    if let Some(or_terms) = pred_obj_pairs.get(&shacl.or_.into()) {
        if let Some(list_head_ref) = or_terms.first() {
            let shape_list = context.parse_rdf_list(list_head_ref.clone());
            let shape_ids: Vec<ID> = shape_list
                .iter()
                .map(|term_ref| context.get_or_create_node_id(term_ref.clone().into()))
                .collect();
            let component = Component::OrConstraint(OrConstraintComponent { shapes: shape_ids });
            let component_id = context.get_or_create_component_id(list_head_ref.into_owned());
            new_components.insert(component_id, component);
        }
    }

    if let Some(xone_terms) = pred_obj_pairs.get(&shacl.xone.into()) {
        if let Some(list_head_ref) = xone_terms.first() {
            let shape_list = context.parse_rdf_list(list_head_ref.clone());
            let shape_ids: Vec<ID> = shape_list
                .iter()
                .map(|term_ref| context.get_or_create_node_id(term_ref.clone().into()))
                .collect();
            let component = Component::XoneConstraint(XoneConstraintComponent { shapes: shape_ids });
            let component_id = context.get_or_create_component_id(list_head_ref.into_owned());
            new_components.insert(component_id, component);
        }
    }

    // Qualified Value Shape (must be parsed together as they belong to the same component instance)
    let qvs_term_opt = pred_obj_pairs
        .get(&shacl.qualified_value_shape.into())
        .and_then(|terms| terms.first().cloned()); // sh:qualifiedValueShape has maxCount 1 on its subject in SHACL spec for this component context

    if let Some(qvs_term_ref) = qvs_term_opt {
        let q_min_count_opt = pred_obj_pairs
            .get(&shacl.qualified_min_count.into())
            .and_then(|terms| terms.first())
            .and_then(|term_ref| {
                if let TermRef::Literal(lit) = term_ref {
                    lit.value().parse::<u64>().ok()
                } else {
                    None
                }
            });

        let q_max_count_opt = pred_obj_pairs
            .get(&shacl.qualified_max_count.into())
            .and_then(|terms| terms.first())
            .and_then(|term_ref| {
                if let TermRef::Literal(lit) = term_ref {
                    lit.value().parse::<u64>().ok()
                } else {
                    None
                }
            });

        let q_disjoint_opt = pred_obj_pairs
            .get(&shacl.qualified_value_shapes_disjoint.into())
            .and_then(|terms| terms.first())
            .and_then(|term_ref| {
                if let TermRef::Literal(lit) = term_ref {
                    lit.value().parse::<bool>().ok()
                } else {
                    None
                }
            });

        // A QualifiedValueShapeComponent is formed if sh:qualifiedValueShape is present
        // AND (sh:qualifiedMinCount or sh:qualifiedMaxCount is present)
        let shape_id = context.get_or_create_node_id(qvs_term_ref.clone().into());
        let component = Component::QualifiedValueShape(QualifiedValueShapeComponent {
            shape: shape_id,
            min_count: q_min_count_opt,
            max_count: q_max_count_opt,
            disjoint: q_disjoint_opt,
        });
        // The component ID is based on the 'start' node, which is the subject
        // of sh:qualifiedValueShape, sh:qualifiedMinCount, etc.
        let component_id = context.get_or_create_component_id(qvs_term_ref.into_owned());
        new_components.insert(component_id, component);
    }

    // Other Constraint Components

    // sh:closed / sh:ignoredProperties
    if let Some(closed_terms) = pred_obj_pairs.get(&shacl.closed.into()) {
        for closed_term_ref in closed_terms {
            // sh:closed maxCount 1, but loop for safety
            if let TermRef::Literal(lit) = closed_term_ref {
                if let Ok(closed_val) = lit.value().parse::<bool>() {
                    let ignored_properties_list_opt = pred_obj_pairs
                        .get(&shacl.ignored_properties.into())
                        .and_then(|terms| terms.first().cloned()); // sh:ignoredProperties maxCount 1

                    let ignored_properties_terms: Vec<Term> =
                        if let Some(list_head) = ignored_properties_list_opt {
                            context
                                .parse_rdf_list(list_head)
                                .into_iter()
                                .map(|t| t.into_owned())
                                .collect()
                        } else {
                            Vec::new()
                        };

                    let component =
                        Component::ClosedConstraint(ClosedConstraintComponent {
                            closed: closed_val,
                            ignored_properties: if ignored_properties_terms.is_empty() {
                                None
                            } else {
                                Some(ignored_properties_terms)
                            },
                        });
                    let component_id =
                        context.get_or_create_component_id(closed_term_ref.into_owned());
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    // sh:hasValue
    if let Some(has_value_terms) = pred_obj_pairs.get(&shacl.has_value.into()) {
        for has_value_term_ref in has_value_terms {
            let component = Component::HasValueConstraint(HasValueConstraintComponent {
                value: has_value_term_ref.clone().into(),
            });
            let component_id =
                context.get_or_create_component_id(has_value_term_ref.into_owned());
            new_components.insert(component_id, component);
        }
    }

    // sh:in
    if let Some(in_terms) = pred_obj_pairs.get(&shacl.in_.into()) {
        if let Some(list_head_term_ref) = in_terms.first() {
            // sh:in maxCount 1
            let list_items = context.parse_rdf_list(list_head_term_ref.clone());
            let values: Vec<Term> = list_items.into_iter().map(|t| t.into_owned()).collect();

            let component = Component::InConstraint(InConstraintComponent { values });
            let component_id =
                context.get_or_create_component_id(list_head_term_ref.into_owned());
            new_components.insert(component_id, component);
        }
    }

    new_components
}

pub trait GraphvizOutput {
    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String;
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

    // property pair constraints
    EqualsConstraint(EqualsConstraintComponent),
    DisjointConstraint(DisjointConstraintComponent),
    LessThanConstraint(LessThanConstraintComponent),
    LessThanOrEqualsConstraint(LessThanOrEqualsConstraintComponent),

    // logical constraints
    NotConstraint(NotConstraintComponent),
    AndConstraint(AndConstraintComponent),
    OrConstraint(OrConstraintComponent),
    XoneConstraint(XoneConstraintComponent),

    // other constraint components
    ClosedConstraint(ClosedConstraintComponent),
    HasValueConstraint(HasValueConstraintComponent),
    InConstraint(InConstraintComponent),
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

            Component::EqualsConstraint(_) => "EqualsConstraint".to_string(),
            Component::DisjointConstraint(_) => "DisjointConstraint".to_string(),
            Component::LessThanConstraint(_) => "LessThanConstraint".to_string(),
            Component::LessThanOrEqualsConstraint(_) => "LessThanOrEqualsConstraint".to_string(),

            Component::NotConstraint(_) => "NotConstraint".to_string(),
            Component::AndConstraint(_) => "AndConstraint".to_string(),
            Component::OrConstraint(_) => "OrConstraint".to_string(),
            Component::XoneConstraint(_) => "XoneConstraint".to_string(),

            Component::ClosedConstraint(_) => "ClosedConstraint".to_string(),
            Component::HasValueConstraint(_) => "HasValueConstraint".to_string(),
            Component::InConstraint(_) => "InConstraint".to_string(),
        }
    }

    pub fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String {
        match self {
            Component::NodeConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::PropertyConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::QualifiedValueShape(c) => c.to_graphviz_string(component_id, context),
            Component::ClassConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::DatatypeConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::NodeKindConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::MinCount(c) => c.to_graphviz_string(component_id, context),
            Component::MaxCount(c) => c.to_graphviz_string(component_id, context),
            Component::MinExclusiveConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::MinInclusiveConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::MaxExclusiveConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::MaxInclusiveConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::MinLengthConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::MaxLengthConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::PatternConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::LanguageInConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::UniqueLangConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::EqualsConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::DisjointConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::LessThanConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::LessThanOrEqualsConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::NotConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::AndConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::OrConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::XoneConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::ClosedConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::HasValueConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::InConstraint(c) => c.to_graphviz_string(component_id, context),
        }
    }
}

// value type
#[derive(Debug)]
pub struct ClassConstraintComponent {
    class: Term,
}

impl GraphvizOutput for ClassConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        let class_name = format_term_for_label(&self.class);
        format!("{} [label=\"Class: {}\"];", component_id.to_graphviz_id(), class_name)
    }
}

#[derive(Debug)]
pub struct DatatypeConstraintComponent {
    datatype: Term,
}

impl GraphvizOutput for DatatypeConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        let datatype_name = format_term_for_label(&self.datatype);
        format!("{} [label=\"Datatype: {}\"];", component_id.to_graphviz_id(), datatype_name)
    }
}

#[derive(Debug)]
pub struct NodeKindConstraintComponent {
    node_kind: Term,
}

impl GraphvizOutput for NodeKindConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        let node_kind_name = format_term_for_label(&self.node_kind);
        format!("{} [label=\"NodeKind: {}\"];", component_id.to_graphviz_id(), node_kind_name)
    }
}

#[derive(Debug)]
pub struct NodeConstraintComponent {
    shape: ID,
}

impl GraphvizOutput for NodeConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String {
        let shape_term_str = context
            .nodeshape_id_lookup()
            .borrow()
            .get_term(self.shape)
            .map_or_else(
                || format!("MissingNodeShape:{}", self.shape),
                |term| format_term_for_label(term),
            );
        let label = format!("NodeConstraint\\n({})", shape_term_str);
        format!(
            "{0} [label=\"{1}\"];\n    {0} -> {2} [style=dashed, label=\"validates\"];",
            component_id.to_graphviz_id(), label, self.shape.to_graphviz_id()
        )
    }
}

#[derive(Debug)]
pub struct PropertyConstraintComponent {
    shape: PropShapeID,
}

impl GraphvizOutput for PropertyConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String {
        let shape_term_str = context
            .propshape_id_lookup()
            .borrow()
            .get_term(self.shape)
            .map_or_else(
                || format!("MissingPropShape:{}", self.shape),
                |term| format_term_for_label(term),
            );
        let label = format!("PropertyConstraint\\n({})", shape_term_str);
        format!(
            "{0} [label=\"{1}\"];\n    {0} -> {2} [style=dashed, label=\"validates\"];",
            component_id.to_graphviz_id(), label, self.shape.to_graphviz_id()
        )
    }
}

#[derive(Debug)]
pub struct QualifiedValueShapeComponent {
    shape: ID, // This is a NodeShape ID
    min_count: Option<u64>,
    max_count: Option<u64>,
    disjoint: Option<bool>,
}

impl GraphvizOutput for QualifiedValueShapeComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String {
        let shape_term_str = context
            .nodeshape_id_lookup()
            .borrow()
            .get_term(self.shape)
            .map_or_else(
                || format!("MissingNodeShape:{}", self.shape),
                |term| format_term_for_label(term),
            );
        let mut label_parts = vec![format!("QualifiedValueShape\\nShape: {}", shape_term_str)];
        if let Some(min) = self.min_count {
            label_parts.push(format!("MinCount: {}", min));
        }
        if let Some(max) = self.max_count {
            label_parts.push(format!("MaxCount: {}", max));
        }
        if let Some(disjoint) = self.disjoint {
            label_parts.push(format!("Disjoint: {}", disjoint));
        }
        let label = label_parts.join("\\n");
        format!(
            "{0} [label=\"{1}\"];\n    {0} -> {2} [style=dashed, label=\"qualifies\"];",
            component_id.to_graphviz_id(), label, self.shape.to_graphviz_id()
        )
    }
}

#[derive(Debug)]
pub struct MinCountConstraintComponent {
    min_count: u64,
}

impl GraphvizOutput for MinCountConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        format!("{} [label=\"MinCount: {}\"];", component_id.to_graphviz_id(), self.min_count)
    }
}

#[derive(Debug)]
pub struct MaxCountConstraintComponent {
    max_count: u64,
}

impl GraphvizOutput for MaxCountConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        format!("{} [label=\"MaxCount: {}\"];", component_id.to_graphviz_id(), self.max_count)
    }
}

// value range constraints
#[derive(Debug)]
pub struct MinExclusiveConstraintComponent {
    min_exclusive: Term,
}

impl GraphvizOutput for MinExclusiveConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        format!(
            "{} [label=\"MinExclusive: {}\"];",
            component_id.to_graphviz_id(),
            format_term_for_label(&self.min_exclusive)
        )
    }
}

#[derive(Debug)]
pub struct MinInclusiveConstraintComponent {
    min_inclusive: Term,
}

impl GraphvizOutput for MinInclusiveConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        format!(
            "{} [label=\"MinInclusive: {}\"];",
            component_id.to_graphviz_id(),
            format_term_for_label(&self.min_inclusive)
        )
    }
}

#[derive(Debug)]
pub struct MaxExclusiveConstraintComponent {
    max_exclusive: Term,
}

impl GraphvizOutput for MaxExclusiveConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        format!(
            "{} [label=\"MaxExclusive: {}\"];",
            component_id.to_graphviz_id(),
            format_term_for_label(&self.max_exclusive)
        )
    }
}

#[derive(Debug)]
pub struct MaxInclusiveConstraintComponent {
    max_inclusive: Term,
}

impl GraphvizOutput for MaxInclusiveConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        format!(
            "{} [label=\"MaxInclusive: {}\"];",
            component_id.to_graphviz_id(),
            format_term_for_label(&self.max_inclusive)
        )
    }
}

// string-based constraints
#[derive(Debug)]
pub struct MinLengthConstraintComponent {
    min_length: u64,
}

impl GraphvizOutput for MinLengthConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        format!("{} [label=\"MinLength: {}\"];", component_id.to_graphviz_id(), self.min_length)
    }
}

#[derive(Debug)]
pub struct MaxLengthConstraintComponent {
    max_length: u64,
}

impl GraphvizOutput for MaxLengthConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        format!("{} [label=\"MaxLength: {}\"];", component_id.to_graphviz_id(), self.max_length)
    }
}

#[derive(Debug)]
pub struct PatternConstraintComponent {
    pattern: String,
    flags: Option<String>,
}

impl GraphvizOutput for PatternConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        let flags_str = self.flags.as_deref().unwrap_or("");
        format!(
            "{} [label=\"Pattern: {}\\nFlags: {}\"];",
            component_id.to_graphviz_id(),
            sanitize_graphviz_string(&self.pattern), // Pattern is a String, not a Term
            flags_str
        )
    }
}

#[derive(Debug)]
pub struct LanguageInConstraintComponent {
    languages: Vec<String>,
}

impl GraphvizOutput for LanguageInConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        format!(
            "{} [label=\"LanguageIn: [{}]\"];",
            component_id.to_graphviz_id(),
            self.languages.join(", ")
        )
    }
}

#[derive(Debug)]
pub struct UniqueLangConstraintComponent {
    unique_lang: bool,
}

impl GraphvizOutput for UniqueLangConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        format!("{} [label=\"UniqueLang: {}\"];", component_id.to_graphviz_id(), self.unique_lang)
    }
}

// property pair constraints
#[derive(Debug)]
pub struct EqualsConstraintComponent {
    property: Term, // Should be an IRI
}

impl GraphvizOutput for EqualsConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        let property_name = format_term_for_label(&self.property);
        format!(
            "{} [label=\"Equals: {}\"];",
            component_id.to_graphviz_id(), property_name
        )
    }
}

#[derive(Debug)]
pub struct DisjointConstraintComponent {
    property: Term, // Should be an IRI
}

impl GraphvizOutput for DisjointConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        let property_name = format_term_for_label(&self.property);
        format!(
            "{} [label=\"Disjoint: {}\"];",
            component_id.to_graphviz_id(), property_name
        )
    }
}

#[derive(Debug)]
pub struct LessThanConstraintComponent {
    property: Term, // Should be an IRI
}

impl GraphvizOutput for LessThanConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        let property_name = format_term_for_label(&self.property);
        format!(
            "{} [label=\"LessThan: {}\"];",
            component_id.to_graphviz_id(), property_name
        )
    }
}

#[derive(Debug)]
pub struct LessThanOrEqualsConstraintComponent {
    property: Term, // Should be an IRI
}

impl GraphvizOutput for LessThanOrEqualsConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        let property_name = format_term_for_label(&self.property);
        format!(
            "{} [label=\"LessThanOrEquals: {}\"];",
            component_id.to_graphviz_id(), property_name
        )
    }
}

// logical constraints
#[derive(Debug)]
pub struct NotConstraintComponent {
    shape: ID, // NodeShape ID
}

impl GraphvizOutput for NotConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String {
        let shape_term_str = context
            .nodeshape_id_lookup()
            .borrow()
            .get_term(self.shape)
            .map_or_else(
                || format!("MissingNodeShape:{}", self.shape),
                |term| format_term_for_label(term),
            );
        let label = format!("Not\\n({})", shape_term_str);
        format!(
            "{0} [label=\"{1}\"];\n    {0} -> {2} [style=dashed, label=\"negates\"];",
            component_id.to_graphviz_id(), label, self.shape.to_graphviz_id()
        )
    }
}

#[derive(Debug)]
pub struct AndConstraintComponent {
    shapes: Vec<ID>, // List of NodeShape IDs
}

impl GraphvizOutput for AndConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String {
        let mut edges = String::new();
        for shape_id in &self.shapes {
            edges.push_str(&format!(
                "    {} -> {} [style=dashed, label=\"conjunct\"];\n",
                component_id.to_graphviz_id(), shape_id.to_graphviz_id()
            ));
        }
        format!("{} [label=\"And\"];\n{}", component_id.to_graphviz_id(), edges.trim_end())
    }
}

#[derive(Debug)]
pub struct OrConstraintComponent {
    shapes: Vec<ID>, // List of NodeShape IDs
}

impl GraphvizOutput for OrConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String {
        let mut edges = String::new();
        for shape_id in &self.shapes {
            edges.push_str(&format!(
                "    {} -> {} [style=dashed, label=\"disjunct\"];\n",
                component_id.to_graphviz_id(), shape_id.to_graphviz_id()
            ));
        }
        format!("{} [label=\"Or\"];\n{}", component_id.to_graphviz_id(), edges.trim_end())
    }
}

#[derive(Debug)]
pub struct XoneConstraintComponent {
    shapes: Vec<ID>, // List of NodeShape IDs
}

impl GraphvizOutput for XoneConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String {
        let mut edges = String::new();
        for shape_id in &self.shapes {
            edges.push_str(&format!(
                "    {} -> {} [style=dashed, label=\"xone_option\"];\n",
                component_id.to_graphviz_id(), shape_id.to_graphviz_id()
            ));
        }
        format!("{} [label=\"Xone\"];\n{}", component_id.to_graphviz_id(), edges.trim_end())
    }
}

// Other Constraint Components
#[derive(Debug)]
pub struct ClosedConstraintComponent {
    closed: bool,
    ignored_properties: Option<Vec<Term>>,
}

impl GraphvizOutput for ClosedConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        let mut label_parts = vec![format!("Closed: {}", self.closed)];
        if let Some(ignored) = &self.ignored_properties {
            if !ignored.is_empty() {
                let ignored_str = ignored
                    .iter()
                    .map(format_term_for_label)
                    .collect::<Vec<String>>()
                    .join(", ");
                label_parts.push(format!("Ignored: [{}]", ignored_str));
            }
        }
        format!("{} [label=\"{}\"];", component_id.to_graphviz_id(), label_parts.join("\\n"))
    }
}

#[derive(Debug)]
pub struct HasValueConstraintComponent {
    value: Term,
}

impl GraphvizOutput for HasValueConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        format!(
            "{} [label=\"HasValue: {}\"];",
            component_id.to_graphviz_id(),
            format_term_for_label(&self.value)
        )
    }
}

#[derive(Debug)]
pub struct InConstraintComponent {
    values: Vec<Term>,
}

impl GraphvizOutput for InConstraintComponent {
    fn to_graphviz_string(&self, component_id: ComponentID, _context: &ValidationContext) -> String {
        let values_str = self
            .values
            .iter()
            .map(format_term_for_label)
            .collect::<Vec<String>>()
            .join(", ");
        format!("{} [label=\"In: [{}]\"];", component_id.to_graphviz_id(), values_str)
    }
}
