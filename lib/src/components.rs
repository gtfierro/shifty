use crate::context::{format_term_for_label, sanitize_graphviz_string, Context, ValidationContext};
use crate::named_nodes::SHACL;
use crate::report::ValidationReportBuilder;
use crate::shape::{NodeShape, PropertyShape}; // Added NodeShape, PropertyShape
use crate::types::{ComponentID, PropShapeID, ID};
use oxigraph::model::{NamedNode, SubjectRef, Term, TermRef}; // Removed TripleRef
use oxigraph::sparql::{Query, QueryOptions, QueryResults, Variable}; // Added Query
use std::collections::HashMap;

pub enum ComponentValidationResult {
    Pass(ComponentID),
    Fail(ComponentID, ValidationFailReason),
}

pub enum ValidationFailReason {
}

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

impl ValidateComponent for InConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &Context,
        _validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        if self.values.is_empty() {
            // According to SHACL spec, if sh:in has an empty list, no value nodes can conform.
            // "The constraint sh:in specifies the condition that each value node is a member of a provided SHACL list."
            // "If the SHACL list is empty, then no value nodes can satisfy the constraint."
            return if c.value_nodes().map_or(true, |vns| vns.is_empty()) {
                // If there are no value nodes, or the list of value nodes is empty, it passes.
                Ok(ComponentValidationResult::Pass(component_id))
            } else {
                Err(format!(
                    "sh:in constraint has an empty list, but value nodes {:?} exist.",
                    c.value_nodes().unwrap_or(&Vec::new()) // Provide empty vec for formatting if None
                ))
            };
        }

        match c.value_nodes() {
            Some(value_nodes) => {
                for vn in value_nodes {
                    if !self.values.contains(vn) {
                        return Err(format!(
                            "Value {:?} is not in the allowed list {:?}.",
                            vn, self.values
                        ));
                    }
                }
                // All value nodes are in self.values
                Ok(ComponentValidationResult::Pass(component_id))
            }
            None => {
                // No value nodes to check, so the constraint is satisfied.
                Ok(ComponentValidationResult::Pass(component_id))
            }
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
    // The key is NamedNode (predicate), value is Vec<Term> (objects)
    // These pairs are read from the shape graph.
    let pred_obj_pairs: HashMap<NamedNode, Vec<Term>> = context
        .store()
        .quads_for_pattern(
            Some(start.to_subject_ref()),
            None,
            None,
            Some(context.shape_graph_iri_ref()),
        )
        .filter_map(Result::ok) // Filter out StorageError
        .fold(HashMap::new(), |mut acc, quad| {
            // quad.predicate is NamedNode, quad.object is Term
            acc.entry(quad.predicate).or_default().push(quad.object);
            acc
        });

    // value type
    if let Some(class_terms) = pred_obj_pairs.get(&shacl.class.into_owned()) {
        for class_term in class_terms {
            // class_term is &Term
            let component = Component::ClassConstraint(ClassConstraintComponent::new(class_term.clone()));
            let component_id = context.get_or_create_component_id(class_term.clone());
            new_components.insert(component_id, component);
        }
    }

    if let Some(datatype_terms) = pred_obj_pairs.get(&shacl.datatype.into_owned()) {
        for datatype_term in datatype_terms {
            // datatype_term is &Term
            let component = Component::DatatypeConstraint(DatatypeConstraintComponent {
                datatype: datatype_term.clone(),
            });
            let component_id = context.get_or_create_component_id(datatype_term.clone());
            new_components.insert(component_id, component);
        }
    }

    if let Some(node_kind_terms) = pred_obj_pairs.get(&shacl.node_kind.into_owned()) {
        for node_kind_term in node_kind_terms {
            // node_kind_term is &Term
            let component = Component::NodeKindConstraint(NodeKindConstraintComponent {
                node_kind: node_kind_term.clone(),
            });
            let component_id = context.get_or_create_component_id(node_kind_term.clone());
            new_components.insert(component_id, component);
        }
    }

    // node constraint component
    if let Some(node_terms) = pred_obj_pairs.get(&shacl.node.into_owned()) {
        for node_term in node_terms {
            // node_term is &Term
            let target_shape_id = context.get_or_create_node_id(node_term.clone());
            let component = Component::NodeConstraint(NodeConstraintComponent {
                shape: target_shape_id,
            });
            let component_id = context.get_or_create_component_id(node_term.clone());
            new_components.insert(component_id, component);
        }
    }

    // property constraints
    if let Some(property_terms) = pred_obj_pairs.get(&shacl.property.into_owned()) {
        for property_term in property_terms {
            // property_term is &Term
            let target_shape_id = context.get_or_create_prop_id(property_term.clone());
            let component = Component::PropertyConstraint(PropertyConstraintComponent {
                shape: target_shape_id,
            });
            let component_id = context.get_or_create_component_id(property_term.clone());
            new_components.insert(component_id, component);
        }
    }

    // cardinality
    if let Some(min_count_terms) = pred_obj_pairs.get(&shacl.min_count.into_owned()) {
        for min_count_term in min_count_terms {
            // min_count_term is &Term
            if let Term::Literal(lit) = min_count_term {
                if let Ok(min_count_val) = lit.value().parse::<u64>() {
                    let component = Component::MinCount(MinCountConstraintComponent {
                        min_count: min_count_val,
                    });
                    let component_id = context.get_or_create_component_id(min_count_term.clone());
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    if let Some(max_count_terms) = pred_obj_pairs.get(&shacl.max_count.into_owned()) {
        for max_count_term in max_count_terms {
            // max_count_term is &Term
            if let Term::Literal(lit) = max_count_term {
                if let Ok(max_count_val) = lit.value().parse::<u64>() {
                    let component = Component::MaxCount(MaxCountConstraintComponent {
                        max_count: max_count_val,
                    });
                    let component_id = context.get_or_create_component_id(max_count_term.clone());
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    // value range
    if let Some(min_exclusive_terms) = pred_obj_pairs.get(&shacl.min_exclusive.into_owned()) {
        for min_exclusive_term in min_exclusive_terms {
            // min_exclusive_term is &Term
            if let Term::Literal(_lit) = min_exclusive_term {
                let component =
                    Component::MinExclusiveConstraint(MinExclusiveConstraintComponent {
                        min_exclusive: min_exclusive_term.clone(),
                    });
                let component_id = context.get_or_create_component_id(min_exclusive_term.clone());
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(min_inclusive_terms) = pred_obj_pairs.get(&shacl.min_inclusive.into_owned()) {
        for min_inclusive_term in min_inclusive_terms {
            // min_inclusive_term is &Term
            if let Term::Literal(_lit) = min_inclusive_term {
                let component =
                    Component::MinInclusiveConstraint(MinInclusiveConstraintComponent {
                        min_inclusive: min_inclusive_term.clone(),
                    });
                let component_id = context.get_or_create_component_id(min_inclusive_term.clone());
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(max_exclusive_terms) = pred_obj_pairs.get(&shacl.max_exclusive.into_owned()) {
        for max_exclusive_term in max_exclusive_terms {
            // max_exclusive_term is &Term
            if let Term::Literal(_lit) = max_exclusive_term {
                let component =
                    Component::MaxExclusiveConstraint(MaxExclusiveConstraintComponent {
                        max_exclusive: max_exclusive_term.clone(),
                    });
                let component_id = context.get_or_create_component_id(max_exclusive_term.clone());
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(max_inclusive_terms) = pred_obj_pairs.get(&shacl.max_inclusive.into_owned()) {
        for max_inclusive_term in max_inclusive_terms {
            // max_inclusive_term is &Term
            if let Term::Literal(_lit) = max_inclusive_term {
                let component =
                    Component::MaxInclusiveConstraint(MaxInclusiveConstraintComponent {
                        max_inclusive: max_inclusive_term.clone(),
                    });
                let component_id = context.get_or_create_component_id(max_inclusive_term.clone());
                new_components.insert(component_id, component);
            }
        }
    }

    // string-based constraints
    if let Some(min_length_terms) = pred_obj_pairs.get(&shacl.min_length.into_owned()) {
        for min_length_term in min_length_terms {
            // min_length_term is &Term
            if let Term::Literal(lit) = min_length_term {
                if let Ok(min_length_val) = lit.value().parse::<u64>() {
                    let component = Component::MinLengthConstraint(MinLengthConstraintComponent {
                        min_length: min_length_val,
                    });
                    let component_id = context.get_or_create_component_id(min_length_term.clone());
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    if let Some(max_length_terms) = pred_obj_pairs.get(&shacl.max_length.into_owned()) {
        for max_length_term in max_length_terms {
            // max_length_term is &Term
            if let Term::Literal(lit) = max_length_term {
                if let Ok(max_length_val) = lit.value().parse::<u64>() {
                    let component = Component::MaxLengthConstraint(MaxLengthConstraintComponent {
                        max_length: max_length_val,
                    });
                    let component_id = context.get_or_create_component_id(max_length_term.clone());
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    if let Some(pattern_terms) = pred_obj_pairs.get(&shacl.pattern.into_owned()) {
        if let Some(pattern_term @ Term::Literal(pattern_lit)) = pattern_terms.first() {
            // pattern_term is &Term
            let pattern_str = pattern_lit.value().to_string();
            let flags_str = pred_obj_pairs
                .get(&shacl.flags.into_owned())
                .and_then(|flags_terms| flags_terms.first())
                .and_then(|flag_term| {
                    // flag_term is &Term
                    if let Term::Literal(flag_lit) = flag_term {
                        Some(flag_lit.value().to_string())
                    } else {
                        None
                    }
                });
            let component = Component::PatternConstraint(PatternConstraintComponent {
                pattern: pattern_str,
                flags: flags_str,
            });
            let component_id = context.get_or_create_component_id(pattern_term.clone());
            new_components.insert(component_id, component);
        }
    }

    if let Some(language_in_terms) = pred_obj_pairs.get(&shacl.language_in.into_owned()) {
        if let Some(list_head_term) = language_in_terms.first() {
            // list_head_term is &Term
            let list_items = context.parse_rdf_list(list_head_term.clone()); // parse_rdf_list takes Term, returns Vec<Term>
            let languages: Vec<String> = list_items
                .into_iter() // Iterates over Term
                .filter_map(|term| {
                    // term is Term
                    if let Term::Literal(lit) = term {
                        Some(lit.value().to_string())
                    } else {
                        None
                    }
                })
                .collect();

            let component =
                Component::LanguageInConstraint(LanguageInConstraintComponent { languages });
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            new_components.insert(component_id, component);
        }
    }

    if let Some(unique_lang_terms) = pred_obj_pairs.get(&shacl.unique_lang.into_owned()) {
        for unique_lang_term in unique_lang_terms {
            // unique_lang_term is &Term
            if let Term::Literal(lit) = unique_lang_term {
                if let Ok(unique_lang_val) = lit.value().parse::<bool>() {
                    let component =
                        Component::UniqueLangConstraint(UniqueLangConstraintComponent {
                            unique_lang: unique_lang_val,
                        });
                    let component_id = context.get_or_create_component_id(unique_lang_term.clone());
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    // property pair constraints
    if let Some(equals_terms) = pred_obj_pairs.get(&shacl.equals.into_owned()) {
        for equals_term in equals_terms {
            // equals_term is &Term
            if let Term::NamedNode(_nn) = equals_term {
                let component = Component::EqualsConstraint(EqualsConstraintComponent {
                    property: equals_term.clone(),
                });
                let component_id = context.get_or_create_component_id(equals_term.clone());
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(disjoint_terms) = pred_obj_pairs.get(&shacl.disjoint.into_owned()) {
        for disjoint_term in disjoint_terms {
            // disjoint_term is &Term
            if let Term::NamedNode(_nn) = disjoint_term {
                let component = Component::DisjointConstraint(DisjointConstraintComponent {
                    property: disjoint_term.clone(),
                });
                let component_id = context.get_or_create_component_id(disjoint_term.clone());
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(less_than_terms) = pred_obj_pairs.get(&shacl.less_than.into_owned()) {
        for less_than_term in less_than_terms {
            // less_than_term is &Term
            if let Term::NamedNode(_nn) = less_than_term {
                let component = Component::LessThanConstraint(LessThanConstraintComponent {
                    property: less_than_term.clone(),
                });
                let component_id = context.get_or_create_component_id(less_than_term.clone());
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(less_than_or_equals_terms) =
        pred_obj_pairs.get(&shacl.less_than_or_equals.into_owned())
    {
        for less_than_or_equals_term in less_than_or_equals_terms {
            // less_than_or_equals_term is &Term
            if let Term::NamedNode(_nn) = less_than_or_equals_term {
                let component =
                    Component::LessThanOrEqualsConstraint(LessThanOrEqualsConstraintComponent {
                        property: less_than_or_equals_term.clone(),
                    });
                let component_id =
                    context.get_or_create_component_id(less_than_or_equals_term.clone());
                new_components.insert(component_id, component);
            }
        }
    }

    // logical constraints
    if let Some(not_terms) = pred_obj_pairs.get(&shacl.not.into_owned()) {
        for not_term in not_terms {
            // not_term is &Term
            let negated_shape_id = context.get_or_create_node_id(not_term.clone());
            let component = Component::NotConstraint(NotConstraintComponent {
                shape: negated_shape_id,
            });
            let component_id = context.get_or_create_component_id(not_term.clone());
            new_components.insert(component_id, component);
        }
    }

    if let Some(and_terms) = pred_obj_pairs.get(&shacl.and_.into_owned()) {
        if let Some(list_head_term) = and_terms.first() {
            // list_head_term is &Term
            let shape_list_terms = context.parse_rdf_list(list_head_term.clone()); // Vec<Term>
            let shape_ids: Vec<ID> = shape_list_terms
                .iter() // Iterates over &Term
                .map(|term| context.get_or_create_node_id(term.clone()))
                .collect();
            let component = Component::AndConstraint(AndConstraintComponent { shapes: shape_ids });
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            new_components.insert(component_id, component);
        }
    }

    if let Some(or_terms) = pred_obj_pairs.get(&shacl.or_.into_owned()) {
        if let Some(list_head_term) = or_terms.first() {
            // list_head_term is &Term
            let shape_list_terms = context.parse_rdf_list(list_head_term.clone()); // Vec<Term>
            let shape_ids: Vec<ID> = shape_list_terms
                .iter()
                .map(|term| context.get_or_create_node_id(term.clone()))
                .collect();
            let component = Component::OrConstraint(OrConstraintComponent { shapes: shape_ids });
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            new_components.insert(component_id, component);
        }
    }

    if let Some(xone_terms) = pred_obj_pairs.get(&shacl.xone.into_owned()) {
        if let Some(list_head_term) = xone_terms.first() {
            // list_head_term is &Term
            let shape_list_terms = context.parse_rdf_list(list_head_term.clone()); // Vec<Term>
            let shape_ids: Vec<ID> = shape_list_terms
                .iter()
                .map(|term| context.get_or_create_node_id(term.clone()))
                .collect();
            let component =
                Component::XoneConstraint(XoneConstraintComponent { shapes: shape_ids });
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            new_components.insert(component_id, component);
        }
    }

    // Qualified Value Shape
    let qvs_term_opt = pred_obj_pairs
        .get(&shacl.qualified_value_shape.into_owned())
        .and_then(|terms| terms.first().cloned()); // qvs_term_opt is Option<Term>

    if let Some(qvs_term) = qvs_term_opt {
        // qvs_term is Term
        let q_min_count_opt = pred_obj_pairs
            .get(&shacl.qualified_min_count.into_owned())
            .and_then(|terms| terms.first()) // &Term
            .and_then(|term_ref_val| {
                // term_ref_val is &Term
                if let Term::Literal(lit) = term_ref_val {
                    lit.value().parse::<u64>().ok()
                } else {
                    None
                }
            });

        let q_max_count_opt = pred_obj_pairs
            .get(&shacl.qualified_max_count.into_owned())
            .and_then(|terms| terms.first()) // &Term
            .and_then(|term_ref_val| {
                // term_ref_val is &Term
                if let Term::Literal(lit) = term_ref_val {
                    lit.value().parse::<u64>().ok()
                } else {
                    None
                }
            });

        let q_disjoint_opt = pred_obj_pairs
            .get(&shacl.qualified_value_shapes_disjoint.into_owned())
            .and_then(|terms| terms.first()) // &Term
            .and_then(|term_ref_val| {
                // term_ref_val is &Term
                if let Term::Literal(lit) = term_ref_val {
                    lit.value().parse::<bool>().ok()
                } else {
                    None
                }
            });

        let shape_id = context.get_or_create_node_id(qvs_term.clone());
        let component = Component::QualifiedValueShape(QualifiedValueShapeComponent {
            shape: shape_id,
            min_count: q_min_count_opt,
            max_count: q_max_count_opt,
            disjoint: q_disjoint_opt,
        });
        let component_id = context.get_or_create_component_id(qvs_term); // qvs_term is Term
        new_components.insert(component_id, component);
    }

    // Other Constraint Components

    // sh:closed / sh:ignoredProperties
    if let Some(closed_terms) = pred_obj_pairs.get(&shacl.closed.into_owned()) {
        for closed_term in closed_terms {
            // closed_term is &Term
            if let Term::Literal(lit) = closed_term {
                if let Ok(closed_val) = lit.value().parse::<bool>() {
                    let ignored_properties_list_opt = pred_obj_pairs
                        .get(&shacl.ignored_properties.into_owned())
                        .and_then(|terms| terms.first().cloned()); // Option<Term>

                    let ignored_properties_terms: Vec<Term> =
                        if let Some(list_head) = ignored_properties_list_opt {
                            // list_head is Term
                            context
                                .parse_rdf_list(list_head) // parse_rdf_list takes Term, returns Vec<Term>
                                .into_iter() // Iterates Term
                                // .map(|t| t) // No .into_owned() needed as it's already Term
                                .collect()
                        } else {
                            Vec::new()
                        };

                    let component = Component::ClosedConstraint(ClosedConstraintComponent {
                        closed: closed_val,
                        ignored_properties: if ignored_properties_terms.is_empty() {
                            None
                        } else {
                            Some(ignored_properties_terms)
                        },
                    });
                    let component_id = context.get_or_create_component_id(closed_term.clone());
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    // sh:hasValue
    if let Some(has_value_terms) = pred_obj_pairs.get(&shacl.has_value.into_owned()) {
        for has_value_term in has_value_terms {
            // has_value_term is &Term
            let component = Component::HasValueConstraint(HasValueConstraintComponent {
                value: has_value_term.clone(),
            });
            let component_id = context.get_or_create_component_id(has_value_term.clone());
            new_components.insert(component_id, component);
        }
    }

    // sh:in
    if let Some(in_terms) = pred_obj_pairs.get(&shacl.in_.into_owned()) {
        if let Some(list_head_term) = in_terms.first() {
            // list_head_term is &Term
            let list_items = context.parse_rdf_list(list_head_term.clone()); // Vec<Term>
            let values: Vec<Term> = list_items.into_iter().collect(); // Already Vec<Term>

            let component = Component::InConstraint(InConstraintComponent { values });
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            new_components.insert(component_id, component);
        }
    }

    new_components
}

pub trait GraphvizOutput {
    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String;
}

pub trait ValidateComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &Context,
        context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String>;
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

    pub fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        context: &ValidationContext,
    ) -> String {
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

    pub fn validate(
        &self,
        component_id: ComponentID,
        c: &Context, 
        context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        match self {
            Component::ClassConstraint(comp) => comp.validate(component_id, c, context),
            Component::NodeConstraint(comp) => comp.validate(component_id, c, context),
            Component::PropertyConstraint(comp) => comp.validate(component_id, c, context),
            //Component::QualifiedValueShape(c) => c.validate(component_id, c, context),
            Component::DatatypeConstraint(comp) => comp.validate(component_id, c, context),
            Component::NodeKindConstraint(comp) => comp.validate(component_id, c, context),
            Component::MinCount(comp) => comp.validate(component_id, c, context),
            Component::MaxCount(comp) => comp.validate(component_id, c, context),
            //Component::MinExclusiveConstraint(c) => c.validate(component_id, c, context),
            //Component::MinInclusiveConstraint(c) => c.validate(component_id, c, context),
            //Component::MaxExclusiveConstraint(c) => c.validate(component_id, c, context),
            //Component::MaxInclusiveConstraint(c) => c.validate(component_id, c, context),
            //Component::MinLengthConstraint(c) => c.validate(component_id, c, context),
            //Component::MaxLengthConstraint(c) => c.validate(component_id, c, context),
            //Component::PatternConstraint(c) => c.validate(component_id, c, context),
            //Component::LanguageInConstraint(c) => c.validate(component_id, c, context),
            //Component::UniqueLangConstraint(c) => c.validate(component_id, c, context),
            //Component::EqualsConstraint(c) => c.validate(component_id, c, context),
            //Component::DisjointConstraint(c) => c.validate(component_id, c, context),
            //Component::LessThanConstraint(c) => c.validate(component_id, c, context),
            //Component::LessThanOrEqualsConstraint(c) => c.validate(component_id, c, context),
            Component::NotConstraint(comp) => comp.validate(component_id, c, context),
            Component::AndConstraint(comp) => comp.validate(component_id, c, context),
            Component::OrConstraint(comp) => comp.validate(component_id, c, context),
            Component::XoneConstraint(comp) => comp.validate(component_id, c, context),
            Component::HasValueConstraint(comp) => comp.validate(component_id, c, context),
            Component::InConstraint(comp) => comp.validate(component_id, c, context),
            //Component::ClosedConstraint(_) |
                // Other components that do not have validate method
                // For components without specific validation logic, or structural ones, consider them as passing.
                _ => Ok(ComponentValidationResult::Pass(component_id)),
        }
    }
}

/// Checks if a given node (represented by `node_as_context`) conforms to the `shape_to_check_against`.
/// Returns `Ok(true)` if it conforms, `Ok(false)` if it does not, or `Err(String)` for an internal error.
fn check_conformance_for_node(
    node_as_context: &Context,
    shape_to_check_against: &NodeShape,
    main_validation_context: &ValidationContext,
) -> Result<bool, String> {
    for constraint_id in shape_to_check_against.constraints() {
        let component = main_validation_context
            .get_component_by_id(constraint_id)
            .ok_or_else(|| format!("Logical check: Component not found: {}", constraint_id))?;

        match component.validate(*constraint_id, node_as_context, main_validation_context) {
            Ok(_validation_result) => {
                // If the component is a PropertyConstraint, we need to further validate the property shape.
                if let Component::PropertyConstraint(pc_comp) = component {
                    let prop_shape = main_validation_context
                        .get_prop_shape_by_id(pc_comp.shape())
                        .ok_or_else(|| {
                            format!(
                                "Logical check: Property shape not found for ID: {}",
                                pc_comp.shape()
                            )
                        })?;
                    
                    // PropertyShape::validate is an inherent method in lib/src/validate.rs
                    // It takes &Context, &ValidationContext, &mut ValidationReportBuilder
                    let mut temp_rb = ValidationReportBuilder::new();
                    if let Err(e) = prop_shape.validate(node_as_context, main_validation_context, &mut temp_rb) {
                        // Error during property shape validation itself (e.g., query parse error)
                        return Err(format!("Logical check: Error validating property shape {}: {}", pc_comp.shape(), e));
                    }
                    if !temp_rb.results.is_empty() {
                        // The property shape validation produced errors for the node_as_context.
                        return Ok(false); // Does not conform
                    }
                }
                // Other component types passed their own validation.
            }
            Err(_e) => {
                // The component's validate method returned an Err, meaning a constraint violation.
                return Ok(false); // Does not conform
            }
        }
    }
    Ok(true) // All constraints passed for the node_as_context against shape_to_check_against
}


// value type
#[derive(Debug)]
pub struct ClassConstraintComponent {
    class: Term,
    query: Query, 
}

impl ClassConstraintComponent {
    pub fn new(class: Term) -> Self {
        let class_term = class.to_subject_ref();
        let query_str = format!("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        ASK {{
            ?value_node rdf:type/rdfs:subClassOf* {} .
        }}",
            class_term.to_string()
        );
        match Query::parse(&query_str, None) {
            Ok(mut query) => {
                query.dataset_mut().set_default_graph_as_union();
                ClassConstraintComponent {
                    class,
                    query,
                }
            }
            Err(e) => panic!("Failed to parse SPARQL query: {}", e),
        }
    }
}

impl GraphvizOutput for ClassConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let class_name = format_term_for_label(&self.class);
        format!(
            "{} [label=\"Class: {}\"];",
            component_id.to_graphviz_id(),
            class_name
        )
    }
}

impl ValidateComponent for ClassConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &Context,
        context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let cc_var = Variable::new("value_node").unwrap();
        if c.value_nodes().is_none() {
            return Ok(ComponentValidationResult::Pass(component_id)); // No value nodes to validate
        }
        let vns = c.value_nodes().unwrap();
        for vn in vns.iter() {
            match context.store().query_opt_with_substituted_variables(
                self.query.clone(),
                QueryOptions::default(),
                [(cc_var.clone(), vn.clone())],
            ) {
                Ok(QueryResults::Boolean(result)) => {
                    if !result {
                        return Err(format!(
                            "Value {:?} does not conform to class constraint: {}",
                            vn, self.class
                        ));
                    }
                },
                Ok(_) => {
                    return Err("Expected a boolean result for class constraint query".to_string());
                },
                Err(e) => {
                    return Err(format!(
                        "Failed to execute class constraint query: {}",
                        e
                    ));
                },
            }
        }
        Ok(ComponentValidationResult::Pass(component_id))
    }
}

#[derive(Debug)]
pub struct DatatypeConstraintComponent {
    datatype: Term,
}

impl ValidateComponent for DatatypeConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &Context,
        _context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let target_datatype_iri = match self.datatype.as_ref() {
            TermRef::NamedNode(nn) => nn,
            _ => return Err("sh:datatype must be an IRI".to_string()),
        };

        if let Some(value_nodes) = c.value_nodes() {
            for value_node in value_nodes {
                match value_node.as_ref() {
                    TermRef::Literal(lit) => {
                        if lit.datatype() != target_datatype_iri {
                            // TODO: Consider ill-typed literals if required by spec for specific datatypes
                            return Err(format!(
                                "Value {:?} does not have datatype {}",
                                value_node, self.datatype
                            ));
                        }
                    }
                    _ => {
                        // Not a literal, so it cannot conform to a datatype constraint
                        return Err(format!(
                            "Value {:?} is not a literal, expected datatype {}",
                            value_node, self.datatype
                        ));
                    }
                }
            }
        }
        Ok(ComponentValidationResult::Pass(component_id))
    }
}

impl GraphvizOutput for DatatypeConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let datatype_name = format_term_for_label(&self.datatype);
        format!(
            "{} [label=\"Datatype: {}\"];",
            component_id.to_graphviz_id(),
            datatype_name
        )
    }
}

#[derive(Debug)]
pub struct NodeKindConstraintComponent {
    node_kind: Term,
}

impl ValidateComponent for NodeKindConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &Context,
        _context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let sh = SHACL::new();
        let expected_node_kind_term = self.node_kind.as_ref();

        if let Some(value_nodes) = c.value_nodes() {
            for value_node in value_nodes {
                let matches = match value_node.as_ref() {
                    TermRef::NamedNode(_) => {
                        expected_node_kind_term == sh.IRI.into()
                            || expected_node_kind_term == sh.BlankNodeOrIRI.into()
                            || expected_node_kind_term == sh.IRIOrLiteral.into()
                    }
                    TermRef::BlankNode(_) => {
                        expected_node_kind_term == sh.BlankNode.into()
                            || expected_node_kind_term == sh.BlankNodeOrIRI.into()
                            || expected_node_kind_term == sh.BlankNodeOrLiteral.into()
                    }
                    TermRef::Literal(_) => {
                        expected_node_kind_term == sh.Literal.into()
                            || expected_node_kind_term == sh.BlankNodeOrLiteral.into()
                            || expected_node_kind_term == sh.IRIOrLiteral.into()
                    }
                    _ => false, // Triple, GraphName - should not occur as value nodes
                };

                if !matches {
                    return Err(format!(
                        "Value {:?} does not match nodeKind {}",
                        value_node, self.node_kind
                    ));
                }
            }
        }
        Ok(ComponentValidationResult::Pass(component_id))
    }
}

impl GraphvizOutput for NodeKindConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let node_kind_name = format_term_for_label(&self.node_kind);
        format!(
            "{} [label=\"NodeKind: {}\"];",
            component_id.to_graphviz_id(),
            node_kind_name
        )
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
            component_id.to_graphviz_id(),
            label,
            self.shape.to_graphviz_id()
        )
    }
}

impl ValidateComponent for NodeConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &Context, // Context of the shape that has the sh:node constraint
        validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let Some(value_nodes) = c.value_nodes() else {
            // No value nodes to check against the node constraint.
            return Ok(ComponentValidationResult::Pass(component_id));
        };

        let Some(target_node_shape) = validation_context.get_node_shape_by_id(&self.shape) else {
            return Err(format!(
                "sh:node referenced shape {:?} not found",
                self.shape
            ));
        };

        for value_node_to_check in value_nodes {
            // Create a new context where the current value_node is the focus node.
            // The path and other aspects of the original context 'c' are not directly relevant
            // for this specific conformance check of the value_node against target_node_shape.
            let value_node_as_context = Context::new(
                value_node_to_check.clone(),
                None, // Path is not directly relevant for this sub-check's context
                Some(vec![value_node_to_check.clone()]), // Value nodes for the sub-check
            );

            match check_conformance_for_node(
                &value_node_as_context,
                target_node_shape,
                validation_context,
            ) {
                Ok(true) => {
                    // value_node_to_check CONFORMS to the target_node_shape.
                    // This is the desired outcome for sh:node, so continue to the next value_node.
                }
                Ok(false) => {
                    // value_node_to_check DOES NOT CONFORM to the target_node_shape.
                    // This means the sh:node constraint FAILS for this value_node.
                    return Err(format!(
                        "Value {:?} does not conform to sh:node shape {:?}",
                        value_node_to_check, self.shape
                    ));
                }
                Err(e) => {
                    // An error occurred during the conformance check itself.
                    return Err(format!(
                        "Error checking conformance for sh:node shape {:?}: {}",
                        self.shape, e
                    ));
                }
            }
        }

        // All value_nodes successfully conformed to the target_node_shape.
        Ok(ComponentValidationResult::Pass(component_id))
    }
}

#[derive(Debug)]
pub struct PropertyConstraintComponent {
    shape: PropShapeID,
}

impl PropertyConstraintComponent {
    pub fn shape(&self) -> &PropShapeID {
        &self.shape
    }
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
            component_id.to_graphviz_id(),
            label,
            self.shape.to_graphviz_id()
        )
    }
}

impl ValidateComponent for PropertyConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        _c: &Context, // Context may not be directly used here if PSS::validate is called elsewhere
        context: &ValidationContext, // May be used to check existence of self.shape
    ) -> Result<ComponentValidationResult, String> {
        // Ensure the referenced property shape exists.
        // The actual validation via PropertyShape::validate (which uses an RB)
        // is assumed to be handled by the caller of Component::validate (e.g. NodeShape::validate).
        // This component's validation, under the new trait, primarily confirms its own structural validity
        // or delegates checks that don't involve the RB directly.
        if context.get_prop_shape_by_id(&self.shape).is_none() {
            return Err(format!(
                "Referenced property shape not found for ID: {}",
                self.shape
            ));
        }
        Ok(ComponentValidationResult::Pass(component_id))
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
            component_id.to_graphviz_id(),
            label,
            self.shape.to_graphviz_id()
        )
    }
}

#[derive(Debug)]
pub struct MinCountConstraintComponent {
    min_count: u64,
}

impl GraphvizOutput for MinCountConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        format!(
            "{} [label=\"MinCount: {}\"];",
            component_id.to_graphviz_id(),
            self.min_count
        )
    }
}

impl ValidateComponent for MinCountConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &Context,
        _context: &ValidationContext, // context is not used
    ) -> Result<ComponentValidationResult, String> {
        if c.value_nodes().map_or(0, |v| v.len()) < self.min_count as usize {
            return Err(format!(
                "Value count ({}) does not meet minimum requirement: {}",
                c.value_nodes().map_or(0, |v| v.len()),
                self.min_count
            ));
        }
        Ok(ComponentValidationResult::Pass(component_id))
    }
}

#[derive(Debug)]
pub struct MaxCountConstraintComponent {
    max_count: u64,
}

impl GraphvizOutput for MaxCountConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        format!(
            "{} [label=\"MaxCount: {}\"];",
            component_id.to_graphviz_id(),
            self.max_count
        )
    }
}

impl ValidateComponent for MaxCountConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &Context,
        _context: &ValidationContext, // context is not used
    ) -> Result<ComponentValidationResult, String> {
        if c.value_nodes().map_or(0, |v| v.len()) > self.max_count as usize {
            return Err(format!(
                "Value count ({}) does not meet maximum requirement: {}",
                c.value_nodes().map_or(0, |v| v.len()),
                self.max_count
            ));
        }
        Ok(ComponentValidationResult::Pass(component_id))
    }
}

// value range constraints
#[derive(Debug)]
pub struct MinExclusiveConstraintComponent {
    min_exclusive: Term,
}

impl GraphvizOutput for MinExclusiveConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
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
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
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
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
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
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
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
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        format!(
            "{} [label=\"MinLength: {}\"];",
            component_id.to_graphviz_id(),
            self.min_length
        )
    }
}

#[derive(Debug)]
pub struct MaxLengthConstraintComponent {
    max_length: u64,
}

impl GraphvizOutput for MaxLengthConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        format!(
            "{} [label=\"MaxLength: {}\"];",
            component_id.to_graphviz_id(),
            self.max_length
        )
    }
}

#[derive(Debug)]
pub struct PatternConstraintComponent {
    pattern: String,
    flags: Option<String>,
}

impl GraphvizOutput for PatternConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
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
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
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
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        format!(
            "{} [label=\"UniqueLang: {}\"];",
            component_id.to_graphviz_id(),
            self.unique_lang
        )
    }
}

// property pair constraints
#[derive(Debug)]
pub struct EqualsConstraintComponent {
    property: Term, // Should be an IRI
}

impl GraphvizOutput for EqualsConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let property_name = format_term_for_label(&self.property);
        format!(
            "{} [label=\"Equals: {}\"];",
            component_id.to_graphviz_id(),
            property_name
        )
    }
}

#[derive(Debug)]
pub struct DisjointConstraintComponent {
    property: Term, // Should be an IRI
}

impl GraphvizOutput for DisjointConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let property_name = format_term_for_label(&self.property);
        format!(
            "{} [label=\"Disjoint: {}\"];",
            component_id.to_graphviz_id(),
            property_name
        )
    }
}

#[derive(Debug)]
pub struct LessThanConstraintComponent {
    property: Term, // Should be an IRI
}

impl GraphvizOutput for LessThanConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let property_name = format_term_for_label(&self.property);
        format!(
            "{} [label=\"LessThan: {}\"];",
            component_id.to_graphviz_id(),
            property_name
        )
    }
}

#[derive(Debug)]
pub struct LessThanOrEqualsConstraintComponent {
    property: Term, // Should be an IRI
}

impl GraphvizOutput for LessThanOrEqualsConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let property_name = format_term_for_label(&self.property);
        format!(
            "{} [label=\"LessThanOrEquals: {}\"];",
            component_id.to_graphviz_id(),
            property_name
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
            component_id.to_graphviz_id(),
            label,
            self.shape.to_graphviz_id()
        )
    }
}

impl ValidateComponent for NotConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &Context, // This is the context of the shape that has the sh:not constraint
        validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let Some(value_nodes) = c.value_nodes() else {
            return Ok(ComponentValidationResult::Pass(component_id)); // No value nodes to check
        };

        let Some(negated_node_shape) = validation_context.get_node_shape_by_id(&self.shape) else {
            return Err(format!(
                "sh:not referenced shape {:?} not found",
                self.shape
            ));
        };

        for value_node_to_check in value_nodes {
            // Create a new context where the current value_node is the focus node.
            let value_node_as_context = Context::new(
                value_node_to_check.clone(),
                None, // Path is not directly relevant for this sub-check's context
                Some(vec![value_node_to_check.clone()]) // Value nodes for the sub-check
            );

            match check_conformance_for_node(
                &value_node_as_context,
                negated_node_shape,
                validation_context,
            ) {
                Ok(true) => {
                    // value_node_to_check CONFORMS to the negated_node_shape.
                    // This means the sh:not constraint FAILS for this value_node.
                    return Err(format!(
                        "Value {:?} conforms to sh:not shape {:?}, but should not.",
                        value_node_to_check, self.shape
                    ));
                }
                Ok(false) => {
                    // value_node_to_check DOES NOT CONFORM to the negated_node_shape.
                    // This means the sh:not constraint PASSES for this value_node. Continue.
                }
                Err(e) => {
                    // An error occurred during the conformance check itself.
                    return Err(format!(
                        "Error checking conformance for sh:not shape {:?}: {}",
                        self.shape, e
                    ));
                }
            }
        }
        // All value_nodes correctly did not conform to the negated_node_shape.
        Ok(ComponentValidationResult::Pass(component_id))
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
                component_id.to_graphviz_id(),
                shape_id.to_graphviz_id()
            ));
        }
        format!(
            "{} [label=\"And\"];\n{}",
            component_id.to_graphviz_id(),
            edges.trim_end()
        )
    }
}

impl ValidateComponent for AndConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &Context,
        validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let Some(value_nodes) = c.value_nodes() else {
            return Ok(ComponentValidationResult::Pass(component_id)); // No value nodes
        };

        for value_node_to_check in value_nodes {
            let value_node_as_context = Context::new(
                value_node_to_check.clone(), None, Some(vec![value_node_to_check.clone()])
            );

            for conjunct_shape_id in &self.shapes {
                let Some(conjunct_node_shape) = validation_context.get_node_shape_by_id(conjunct_shape_id) else {
                    return Err(format!(
                        "sh:and referenced shape {:?} not found",
                        conjunct_shape_id
                    ));
                };

                match check_conformance_for_node(
                    &value_node_as_context,
                    conjunct_node_shape,
                    validation_context,
                ) {
                    Ok(true) => {
                        // value_node_to_check CONFORMS to this conjunct_node_shape. Continue to next conjunct.
                    }
                    Ok(false) => {
                        // value_node_to_check DOES NOT CONFORM to this conjunct_node_shape.
                        // For sh:and, all shapes must conform. So, this is a failure for this value_node.
                        return Err(format!(
                            "Value {:?} does not conform to sh:and shape {:?}",
                            value_node_to_check, conjunct_shape_id
                        ));
                    }
                    Err(e) => {
                        return Err(format!(
                            "Error checking conformance for sh:and shape {:?}: {}",
                            conjunct_shape_id, e
                        ));
                    }
                }
            }
            // If loop completes, value_node_to_check conformed to all conjunct_node_shapes.
        }
        // All value_nodes conformed to all conjunct_node_shapes.
        Ok(ComponentValidationResult::Pass(component_id))
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
                component_id.to_graphviz_id(),
                shape_id.to_graphviz_id()
            ));
        }
        format!(
            "{} [label=\"Or\"];\n{}",
            component_id.to_graphviz_id(),
            edges.trim_end()
        )
    }
}

impl ValidateComponent for OrConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &Context,
        validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let Some(value_nodes) = c.value_nodes() else {
            return Ok(ComponentValidationResult::Pass(component_id)); // No value nodes
        };

        if self.shapes.is_empty() {
            // If sh:or list is empty, no value node can conform unless there are no value nodes.
            return if value_nodes.is_empty() {
                Ok(ComponentValidationResult::Pass(component_id))
            } else {
                Err("sh:or with an empty list of shapes cannot be satisfied by any value node.".to_string())
            };
        }

        for value_node_to_check in value_nodes {
            let value_node_as_context = Context::new(
                value_node_to_check.clone(), None, Some(vec![value_node_to_check.clone()])
            );
            let mut passed_at_least_one_disjunct = false;

            for disjunct_shape_id in &self.shapes {
                let Some(disjunct_node_shape) = validation_context.get_node_shape_by_id(disjunct_shape_id) else {
                    return Err(format!(
                        "sh:or referenced shape {:?} not found",
                        disjunct_shape_id
                    ));
                };

                match check_conformance_for_node(
                    &value_node_as_context,
                    disjunct_node_shape,
                    validation_context,
                ) {
                    Ok(true) => {
                        // value_node_to_check CONFORMS to this disjunct_node_shape.
                        // For sh:or, this is enough for this value_node.
                        passed_at_least_one_disjunct = true;
                        break; // Move to the next value_node_to_check
                    }
                    Ok(false) => {
                        // value_node_to_check DOES NOT CONFORM. Try next disjunct shape.
                    }
                    Err(e) => {
                        return Err(format!(
                            "Error checking conformance for sh:or shape {:?}: {}",
                            disjunct_shape_id, e
                        ));
                    }
                }
            }
            if !passed_at_least_one_disjunct {
                // This value_node_to_check did not conform to any of the sh:or shapes.
                return Err(format!(
                    "Value {:?} does not conform to any sh:or shapes.",
                    value_node_to_check
                ));
            }
            // If loop completes, value_node_to_check conformed to at least one disjunct.
        }
        // All value_nodes conformed to at least one of the disjunct_node_shapes.
        Ok(ComponentValidationResult::Pass(component_id))
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
                component_id.to_graphviz_id(),
                shape_id.to_graphviz_id()
            ));
        }
        format!(
            "{} [label=\"Xone\"];\n{}",
            component_id.to_graphviz_id(),
            edges.trim_end()
        )
    }
}

impl ValidateComponent for XoneConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &Context,
        validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let Some(value_nodes) = c.value_nodes() else {
            return Ok(ComponentValidationResult::Pass(component_id)); // No value nodes
        };

        if self.shapes.is_empty() {
            // If sh:xone list is empty, no value node can conform unless there are no value nodes.
            return if value_nodes.is_empty() {
                Ok(ComponentValidationResult::Pass(component_id))
            } else {
                Err("sh:xone with an empty list of shapes cannot be satisfied by any value node.".to_string())
            };
        }

        for value_node_to_check in value_nodes {
            let value_node_as_context = Context::new(
                value_node_to_check.clone(), None, Some(vec![value_node_to_check.clone()])
            );
            let mut conforming_shapes_count = 0;

            for xone_shape_id in &self.shapes {
                let Some(xone_node_shape) = validation_context.get_node_shape_by_id(xone_shape_id) else {
                    return Err(format!(
                        "sh:xone referenced shape {:?} not found",
                        xone_shape_id
                    ));
                };

                match check_conformance_for_node(
                    &value_node_as_context,
                    xone_node_shape,
                    validation_context,
                ) {
                    Ok(true) => {
                        // value_node_to_check CONFORMS to this xone_node_shape.
                        conforming_shapes_count += 1;
                    }
                    Ok(false) => {
                        // value_node_to_check DOES NOT CONFORM. Continue.
                    }
                    Err(e) => {
                        return Err(format!(
                            "Error checking conformance for sh:xone shape {:?}: {}",
                            xone_shape_id, e
                        ));
                    }
                }
            }

            if conforming_shapes_count != 1 {
                // This value_node_to_check did not conform to exactly one of the sh:xone shapes.
                return Err(format!(
                    "Value {:?} conformed to {} sh:xone shapes, but expected exactly 1.",
                    value_node_to_check, conforming_shapes_count
                ));
            }
            // If loop completes, value_node_to_check conformed to exactly one xone_shape.
        }
        // All value_nodes conformed to exactly one of the xone_node_shapes.
        Ok(ComponentValidationResult::Pass(component_id))
    }
}

// Other Constraint Components
#[derive(Debug)]
pub struct ClosedConstraintComponent {
    closed: bool,
    ignored_properties: Option<Vec<Term>>,
}

impl GraphvizOutput for ClosedConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
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
        format!(
            "{} [label=\"{}\"];",
            component_id.to_graphviz_id(),
            label_parts.join("\\n")
        )
    }
}

#[derive(Debug)]
pub struct HasValueConstraintComponent {
    value: Term,
}

impl GraphvizOutput for HasValueConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        format!(
            "{} [label=\"HasValue: {}\"];",
            component_id.to_graphviz_id(),
            format_term_for_label(&self.value)
        )
    }
}

impl ValidateComponent for HasValueConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &Context,
        _validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        match c.value_nodes() {
            Some(value_nodes) => {
                if value_nodes.iter().any(|vn| vn == &self.value) {
                    // At least one value node is equal to self.value
                    Ok(ComponentValidationResult::Pass(component_id))
                } else {
                    // No value node is equal to self.value
                    Err(format!(
                        "None of the value nodes {:?} are equal to the required value {:?}",
                        value_nodes, self.value
                    ))
                }
            }
            None => {
                // No value nodes present, so self.value cannot be among them.
                Err(format!(
                    "No value nodes found to check against required value {:?}",
                    self.value
                ))
            }
        }
    }
}

#[derive(Debug)]
pub struct InConstraintComponent {
    values: Vec<Term>,
}

impl GraphvizOutput for InConstraintComponent {
    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let values_str = self
            .values
            .iter()
            .map(format_term_for_label)
            .collect::<Vec<String>>()
            .join(", ");
        format!(
            "{} [label=\"In: [{}]\"];",
            component_id.to_graphviz_id(),
            values_str
        )
    }
}
