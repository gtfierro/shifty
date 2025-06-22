use crate::context::{Context, ValidationContext};
use crate::named_nodes::SHACL;
use crate::shape::NodeShape;
use crate::types::{ComponentID, ID, Path, TraceItem};
use oxigraph::model::{Literal, NamedNode, SubjectRef, Term, TermRef};
use std::collections::{HashMap, HashSet};

mod cardinality;
mod logical;
mod other;
mod property_pair;
mod shape_based;
mod sparql;
mod string_based;
mod value_range;
mod value_type;

pub use cardinality::*;
pub use logical::*;
pub use other::*;
pub use property_pair::*;
pub use shape_based::*;
pub use sparql::*;
pub use string_based::*;
pub use value_range::*;
pub use value_type::*;

/// The result of validating a single value node against a constraint component.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum ComponentValidationResult {
    /// Indicates that the validation passed. Contains the context of the validation.
    Pass(Context),
    /// Indicates that the validation failed. Contains the context and details of the failure.
    Fail(Context, ValidationFailure),
}

/// The result of a conformance check for a node against a shape.
/// Used by logical constraints like `sh:not`, `sh:and`, etc.
#[derive(Debug, Clone)]
pub(crate) enum ConformanceReport {
    /// The node conforms to the shape.
    Conforms,
    /// The node does not conform to the shape, with details of the first failure.
    NonConforms(ValidationFailure),
}

/// Details about a single validation failure.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ValidationFailure {
    /// The ID of the component that was violated.
    pub component_id: ComponentID,
    /// The specific value node that failed validation, if applicable.
    pub failed_value_node: Option<Term>,
    /// A human-readable message describing the failure.
    pub message: String,
    /// The path of the validation result, which can be overridden by SPARQL-based constraints.
    pub result_path: Option<Path>,
    /// The constraint that was violated, for `sh:sparql` constraints.
    pub source_constraint: Option<Term>,
}

/// A trait for converting `Term` or `TermRef` into `SubjectRef`.
///
/// This is a utility trait to handle cases where a `Term` that is expected
/// to be a subject (IRI or Blank Node) needs to be used in a context that
/// requires a `SubjectRef`.
pub(crate) trait ToSubjectRef {
    /// Converts to `SubjectRef`, panicking if the term is a `Literal`.
    fn to_subject_ref(&self) -> SubjectRef<'_>;
    /// Tries to convert to `SubjectRef`, returning a `Result`.
    fn try_to_subject_ref(&self) -> Result<SubjectRef<'_>, String>;
}

impl ToSubjectRef for Term {
    fn to_subject_ref(&self) -> SubjectRef<'_> {
        self.try_to_subject_ref().expect("Invalid subject term")
    }
    fn try_to_subject_ref(&self) -> Result<SubjectRef<'_>, String> {
        match self {
            Term::NamedNode(n) => Ok(n.into()),
            Term::BlankNode(b) => Ok(b.into()),
            _ => Err(format!("Invalid subject term {:?}", self)),
        }
    }
}

impl<'a> ToSubjectRef for TermRef<'a> {
    fn to_subject_ref(&self) -> SubjectRef<'a> {
        match self {
            TermRef::NamedNode(n) => n.clone().into(),
            TermRef::BlankNode(b) => b.clone().into(),
            _ => panic!("Invalid subject term {:?}", self),
        }
    }
    fn try_to_subject_ref(&self) -> Result<SubjectRef<'a>, String> {
        match self {
            TermRef::NamedNode(n) => Ok(n.clone().into()),
            TermRef::BlankNode(b) => Ok(b.clone().into()),
            _ => Err(format!("Invalid subject term {:?}", self)),
        }
    }
}

/// Parses all constraint components attached to a given shape subject (`start`) from the shapes graph.
///
/// This function iterates through all known SHACL constraint properties (e.g., `sh:class`, `sh:minCount`)
/// and creates the corresponding `Component` structs if they are present on the `start` term.
/// The created components are returned in a `HashMap` keyed by their `ComponentID`.
pub(crate) fn parse_components(
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

    let mut processed_predicates = HashSet::new();

    // value type
    if let Some(class_terms) = pred_obj_pairs.get(&shacl.class.into_owned()) {
        processed_predicates.insert(shacl.class.into_owned());
        for class_term in class_terms {
            // class_term is &Term
            let component =
                Component::ClassConstraint(ClassConstraintComponent::new(class_term.clone()));
            let id_term = Term::Literal(Literal::new_simple_literal(format!(
                "ClassConstraint:{}",
                class_term
            )));
            let component_id = context.get_or_create_component_id(id_term);
            new_components.insert(component_id, component);
        }
    }

    if let Some(datatype_terms) = pred_obj_pairs.get(&shacl.datatype.into_owned()) {
        processed_predicates.insert(shacl.datatype.into_owned());
        for datatype_term in datatype_terms {
            // datatype_term is &Term
            let component = Component::DatatypeConstraint(DatatypeConstraintComponent::new(
                datatype_term.clone(),
            ));
            let id_term = Term::Literal(Literal::new_simple_literal(format!(
                "DatatypeConstraint:{}",
                datatype_term
            )));
            let component_id = context.get_or_create_component_id(id_term);
            new_components.insert(component_id, component);
        }
    }

    if let Some(node_kind_terms) = pred_obj_pairs.get(&shacl.node_kind.into_owned()) {
        processed_predicates.insert(shacl.node_kind.into_owned());
        for node_kind_term in node_kind_terms {
            // node_kind_term is &Term
            let component = Component::NodeKindConstraint(NodeKindConstraintComponent::new(
                node_kind_term.clone(),
            ));
            let id_term = Term::Literal(Literal::new_simple_literal(format!(
                "NodeKindConstraint:{}",
                node_kind_term
            )));
            let component_id = context.get_or_create_component_id(id_term);
            new_components.insert(component_id, component);
        }
    }

    // node constraint component
    if let Some(node_terms) = pred_obj_pairs.get(&shacl.node.into_owned()) {
        processed_predicates.insert(shacl.node.into_owned());
        for node_term in node_terms {
            // node_term is &Term
            let target_shape_id = context.get_or_create_node_id(node_term.clone());
            let component =
                Component::NodeConstraint(NodeConstraintComponent::new(target_shape_id));
            let id_term = Term::Literal(Literal::new_simple_literal(format!(
                "NodeConstraint:{}",
                node_term
            )));
            let component_id = context.get_or_create_component_id(id_term);
            new_components.insert(component_id, component);
        }
    }

    // property constraints
    if let Some(property_terms) = pred_obj_pairs.get(&shacl.property.into_owned()) {
        processed_predicates.insert(shacl.property.into_owned());
        for property_term in property_terms {
            // property_term is &Term
            let target_shape_id = context.get_or_create_prop_id(property_term.clone());
            let component =
                Component::PropertyConstraint(PropertyConstraintComponent::new(target_shape_id));
            let id_term = Term::Literal(Literal::new_simple_literal(format!(
                "PropertyConstraint:{}",
                property_term
            )));
            let component_id = context.get_or_create_component_id(id_term);
            new_components.insert(component_id, component);
        }
    }

    // cardinality
    if let Some(min_count_terms) = pred_obj_pairs.get(&shacl.min_count.into_owned()) {
        processed_predicates.insert(shacl.min_count.into_owned());
        for min_count_term in min_count_terms {
            // min_count_term is &Term
            if let Term::Literal(lit) = min_count_term {
                if let Ok(min_count_val) = lit.value().parse::<u64>() {
                    let component =
                        Component::MinCount(MinCountConstraintComponent::new(min_count_val));
                    let id_term = Term::Literal(Literal::new_simple_literal(format!(
                        "MinCountConstraint:{}",
                        min_count_term
                    )));
                    let component_id = context.get_or_create_component_id(id_term);
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    if let Some(max_count_terms) = pred_obj_pairs.get(&shacl.max_count.into_owned()) {
        processed_predicates.insert(shacl.max_count.into_owned());
        for max_count_term in max_count_terms {
            // max_count_term is &Term
            if let Term::Literal(lit) = max_count_term {
                if let Ok(max_count_val) = lit.value().parse::<u64>() {
                    let component =
                        Component::MaxCount(MaxCountConstraintComponent::new(max_count_val));
                    let id_term = Term::Literal(Literal::new_simple_literal(format!(
                        "MaxCountConstraint:{}",
                        max_count_term
                    )));
                    let component_id = context.get_or_create_component_id(id_term);
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    // value range
    if let Some(min_exclusive_terms) = pred_obj_pairs.get(&shacl.min_exclusive.into_owned()) {
        processed_predicates.insert(shacl.min_exclusive.into_owned());
        for min_exclusive_term in min_exclusive_terms {
            // min_exclusive_term is &Term
            if let Term::Literal(_lit) = min_exclusive_term {
                let component = Component::MinExclusiveConstraint(
                    MinExclusiveConstraintComponent::new(min_exclusive_term.clone()),
                );
                let id_term = Term::Literal(Literal::new_simple_literal(format!(
                    "MinExclusiveConstraint:{}:{}",
                    start, min_exclusive_term
                )));
                let component_id = context.get_or_create_component_id(id_term);
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(min_inclusive_terms) = pred_obj_pairs.get(&shacl.min_inclusive.into_owned()) {
        processed_predicates.insert(shacl.min_inclusive.into_owned());
        for min_inclusive_term in min_inclusive_terms {
            // min_inclusive_term is &Term
            if let Term::Literal(_lit) = min_inclusive_term {
                let component = Component::MinInclusiveConstraint(
                    MinInclusiveConstraintComponent::new(min_inclusive_term.clone()),
                );
                let id_term = Term::Literal(Literal::new_simple_literal(format!(
                    "MinInclusiveConstraint:{}:{}",
                    start, min_inclusive_term
                )));
                let component_id = context.get_or_create_component_id(id_term);
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(max_exclusive_terms) = pred_obj_pairs.get(&shacl.max_exclusive.into_owned()) {
        processed_predicates.insert(shacl.max_exclusive.into_owned());
        for max_exclusive_term in max_exclusive_terms {
            // max_exclusive_term is &Term
            if let Term::Literal(_lit) = max_exclusive_term {
                let component = Component::MaxExclusiveConstraint(
                    MaxExclusiveConstraintComponent::new(max_exclusive_term.clone()),
                );
                let id_term = Term::Literal(Literal::new_simple_literal(format!(
                    "MaxExclusiveConstraint:{}:{}",
                    start, max_exclusive_term
                )));
                let component_id = context.get_or_create_component_id(id_term);
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(max_inclusive_terms) = pred_obj_pairs.get(&shacl.max_inclusive.into_owned()) {
        processed_predicates.insert(shacl.max_inclusive.into_owned());
        for max_inclusive_term in max_inclusive_terms {
            // max_inclusive_term is &Term
            if let Term::Literal(_lit) = max_inclusive_term {
                let component = Component::MaxInclusiveConstraint(
                    MaxInclusiveConstraintComponent::new(max_inclusive_term.clone()),
                );
                let id_term = Term::Literal(Literal::new_simple_literal(format!(
                    "MaxInclusiveConstraint:{}:{}",
                    start, max_inclusive_term
                )));
                let component_id = context.get_or_create_component_id(id_term);
                new_components.insert(component_id, component);
            }
        }
    }

    // string-based constraints
    if let Some(min_length_terms) = pred_obj_pairs.get(&shacl.min_length.into_owned()) {
        processed_predicates.insert(shacl.min_length.into_owned());
        for min_length_term in min_length_terms {
            // min_length_term is &Term
            if let Term::Literal(lit) = min_length_term {
                if let Ok(min_length_val) = lit.value().parse::<u64>() {
                    let component = Component::MinLengthConstraint(
                        MinLengthConstraintComponent::new(min_length_val),
                    );
                    let id_term = Term::Literal(Literal::new_simple_literal(format!(
                        "MinLengthConstraint:{}",
                        min_length_term
                    )));
                    let component_id = context.get_or_create_component_id(id_term);
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    if let Some(max_length_terms) = pred_obj_pairs.get(&shacl.max_length.into_owned()) {
        processed_predicates.insert(shacl.max_length.into_owned());
        for max_length_term in max_length_terms {
            // max_length_term is &Term
            if let Term::Literal(lit) = max_length_term {
                if let Ok(max_length_val) = lit.value().parse::<u64>() {
                    let component = Component::MaxLengthConstraint(
                        MaxLengthConstraintComponent::new(max_length_val),
                    );
                    let id_term = Term::Literal(Literal::new_simple_literal(format!(
                        "MaxLengthConstraint:{}",
                        max_length_term
                    )));
                    let component_id = context.get_or_create_component_id(id_term);
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    if let Some(pattern_terms) = pred_obj_pairs.get(&shacl.pattern.into_owned()) {
        processed_predicates.insert(shacl.pattern.into_owned());
        if let Some(Term::Literal(pattern_lit)) = pattern_terms.first() {
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
            let component = Component::PatternConstraint(PatternConstraintComponent::new(
                pattern_str.clone(),
                flags_str.clone(),
            ));
            let id_term = Term::Literal(Literal::new_simple_literal(format!(
                "PatternConstraint:{}:{}",
                pattern_str,
                flags_str.as_deref().unwrap_or("")
            )));
            let component_id = context.get_or_create_component_id(id_term);
            new_components.insert(component_id, component);
            if flags_str.is_some() {
                processed_predicates.insert(shacl.flags.into_owned());
            }
        }
    }

    if let Some(language_in_terms) = pred_obj_pairs.get(&shacl.language_in.into_owned()) {
        processed_predicates.insert(shacl.language_in.into_owned());
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
                Component::LanguageInConstraint(LanguageInConstraintComponent::new(languages));
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            new_components.insert(component_id, component);
        }
    }

    if let Some(unique_lang_terms) = pred_obj_pairs.get(&shacl.unique_lang.into_owned()) {
        processed_predicates.insert(shacl.unique_lang.into_owned());
        for unique_lang_term in unique_lang_terms {
            // unique_lang_term is &Term
            if let Term::Literal(lit) = unique_lang_term {
                if let Ok(unique_lang_val) = lit.value().parse::<bool>() {
                    let component = Component::UniqueLangConstraint(
                        UniqueLangConstraintComponent::new(unique_lang_val),
                    );
                    let id_term = Term::Literal(Literal::new_simple_literal(format!(
                        "UniqueLangConstraint:{}",
                        unique_lang_term
                    )));
                    let component_id = context.get_or_create_component_id(id_term);
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    // property pair constraints
    if let Some(equals_terms) = pred_obj_pairs.get(&shacl.equals.into_owned()) {
        processed_predicates.insert(shacl.equals.into_owned());
        for equals_term in equals_terms {
            // equals_term is &Term
            if let Term::NamedNode(_nn) = equals_term {
                let component = Component::EqualsConstraint(EqualsConstraintComponent::new(
                    equals_term.clone(),
                ));
                let id_term = Term::Literal(Literal::new_simple_literal(format!(
                    "EqualsConstraint:{}",
                    equals_term
                )));
                let component_id = context.get_or_create_component_id(id_term);
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(disjoint_terms) = pred_obj_pairs.get(&shacl.disjoint.into_owned()) {
        processed_predicates.insert(shacl.disjoint.into_owned());
        for disjoint_term in disjoint_terms {
            // disjoint_term is &Term
            if let Term::NamedNode(_nn) = disjoint_term {
                let component = Component::DisjointConstraint(DisjointConstraintComponent::new(
                    disjoint_term.clone(),
                ));
                let id_term = Term::Literal(Literal::new_simple_literal(format!(
                    "DisjointConstraint:{}",
                    disjoint_term
                )));
                let component_id = context.get_or_create_component_id(id_term);
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(less_than_terms) = pred_obj_pairs.get(&shacl.less_than.into_owned()) {
        processed_predicates.insert(shacl.less_than.into_owned());
        for less_than_term in less_than_terms {
            // less_than_term is &Term
            if let Term::NamedNode(_nn) = less_than_term {
                let component = Component::LessThanConstraint(LessThanConstraintComponent::new(
                    less_than_term.clone(),
                ));
                let id_term = Term::Literal(Literal::new_simple_literal(format!(
                    "LessThanConstraint:{}",
                    less_than_term
                )));
                let component_id = context.get_or_create_component_id(id_term);
                new_components.insert(component_id, component);
            }
        }
    }

    if let Some(less_than_or_equals_terms) =
        pred_obj_pairs.get(&shacl.less_than_or_equals.into_owned())
    {
        processed_predicates.insert(shacl.less_than_or_equals.into_owned());
        for less_than_or_equals_term in less_than_or_equals_terms {
            // less_than_or_equals_term is &Term
            if let Term::NamedNode(_nn) = less_than_or_equals_term {
                let component = Component::LessThanOrEqualsConstraint(
                    LessThanOrEqualsConstraintComponent::new(less_than_or_equals_term.clone()),
                );
                let id_term = Term::Literal(Literal::new_simple_literal(format!(
                    "LessThanOrEqualsConstraint:{}",
                    less_than_or_equals_term
                )));
                let component_id = context.get_or_create_component_id(id_term);
                new_components.insert(component_id, component);
            }
        }
    }

    // logical constraints
    if let Some(not_terms) = pred_obj_pairs.get(&shacl.not.into_owned()) {
        processed_predicates.insert(shacl.not.into_owned());
        for not_term in not_terms {
            // not_term is &Term
            let negated_shape_id = context.get_or_create_node_id(not_term.clone());
            let component = Component::NotConstraint(NotConstraintComponent::new(negated_shape_id));
            let id_term = Term::Literal(Literal::new_simple_literal(format!(
                "NotConstraint:{}",
                not_term
            )));
            let component_id = context.get_or_create_component_id(id_term);
            new_components.insert(component_id, component);
        }
    }

    if let Some(and_terms) = pred_obj_pairs.get(&shacl.and_.into_owned()) {
        processed_predicates.insert(shacl.and_.into_owned());
        if let Some(list_head_term) = and_terms.first() {
            // list_head_term is &Term
            let shape_list_terms = context.parse_rdf_list(list_head_term.clone()); // Vec<Term>
            let shape_ids: Vec<ID> = shape_list_terms
                .iter() // Iterates over &Term
                .map(|term| context.get_or_create_node_id(term.clone()))
                .collect();
            let component = Component::AndConstraint(AndConstraintComponent::new(shape_ids));
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            new_components.insert(component_id, component);
        }
    }

    if let Some(or_terms) = pred_obj_pairs.get(&shacl.or_.into_owned()) {
        processed_predicates.insert(shacl.or_.into_owned());
        if let Some(list_head_term) = or_terms.first() {
            // list_head_term is &Term
            let shape_list_terms = context.parse_rdf_list(list_head_term.clone()); // Vec<Term>
            let shape_ids: Vec<ID> = shape_list_terms
                .iter()
                .map(|term| context.get_or_create_node_id(term.clone()))
                .collect();
            let component = Component::OrConstraint(OrConstraintComponent::new(shape_ids));
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            new_components.insert(component_id, component);
        }
    }

    if let Some(xone_terms) = pred_obj_pairs.get(&shacl.xone.into_owned()) {
        processed_predicates.insert(shacl.xone.into_owned());
        if let Some(list_head_term) = xone_terms.first() {
            // list_head_term is &Term
            let shape_list_terms = context.parse_rdf_list(list_head_term.clone()); // Vec<Term>
            let shape_ids: Vec<ID> = shape_list_terms
                .iter()
                .map(|term| context.get_or_create_node_id(term.clone()))
                .collect();
            let component = Component::XoneConstraint(XoneConstraintComponent::new(shape_ids));
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            new_components.insert(component_id, component);
        }
    }

    // Qualified Value Shape
    if let Some(qvs_terms) = pred_obj_pairs.get(&shacl.qualified_value_shape.into_owned()) {
        processed_predicates.insert(shacl.qualified_value_shape.into_owned());
        let qvs_term_opt = qvs_terms.first().cloned();

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

            if q_min_count_opt.is_some() {
                processed_predicates.insert(shacl.qualified_min_count.into_owned());
            }

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

            if q_max_count_opt.is_some() {
                processed_predicates.insert(shacl.qualified_max_count.into_owned());
            }

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

            if q_disjoint_opt.is_some() {
                processed_predicates.insert(shacl.qualified_value_shapes_disjoint.into_owned());
            }

            let shape_id = context.get_or_create_node_id(qvs_term.clone());
            let component = Component::QualifiedValueShape(QualifiedValueShapeComponent::new(
                shape_id,
                q_min_count_opt,
                q_max_count_opt,
                q_disjoint_opt,
            ));
            let id_term = Term::Literal(Literal::new_simple_literal(format!(
                "QualifiedValueShape:{}:{}:{}:{}",
                qvs_term,
                q_min_count_opt.map(|v| v.to_string()).unwrap_or_default(),
                q_max_count_opt.map(|v| v.to_string()).unwrap_or_default(),
                q_disjoint_opt.map(|v| v.to_string()).unwrap_or_default()
            )));
            let component_id = context.get_or_create_component_id(id_term);
            new_components.insert(component_id, component);
        }
    }

    // Other Constraint Components

    // sh:closed / sh:ignoredProperties
    if let Some(closed_terms) = pred_obj_pairs.get(&shacl.closed.into_owned()) {
        processed_predicates.insert(shacl.closed.into_owned());
        for closed_term in closed_terms {
            // closed_term is &Term
            if let Term::Literal(lit) = closed_term {
                if let Ok(closed_val) = lit.value().parse::<bool>() {
                    let ignored_properties_list_opt = pred_obj_pairs
                        .get(&shacl.ignored_properties.into_owned())
                        .and_then(|terms| terms.first().cloned()); // Option<Term>

                    if ignored_properties_list_opt.is_some() {
                        processed_predicates.insert(shacl.ignored_properties.into_owned());
                    }

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

                    let component = Component::ClosedConstraint(ClosedConstraintComponent::new(
                        closed_val,
                        if ignored_properties_terms.is_empty() {
                            None
                        } else {
                            Some(ignored_properties_terms.clone())
                        },
                    ));
                    let id_term = Term::Literal(Literal::new_simple_literal(format!(
                        "ClosedConstraint:{}:{:?}",
                        closed_val, ignored_properties_terms
                    )));
                    let component_id = context.get_or_create_component_id(id_term);
                    new_components.insert(component_id, component);
                }
            }
        }
    }

    // sh:hasValue
    if let Some(has_value_terms) = pred_obj_pairs.get(&shacl.has_value.into_owned()) {
        processed_predicates.insert(shacl.has_value.into_owned());
        for has_value_term in has_value_terms {
            // has_value_term is &Term
            let component = Component::HasValueConstraint(HasValueConstraintComponent::new(
                has_value_term.clone(),
            ));
            let id_term = Term::Literal(Literal::new_simple_literal(format!(
                "HasValueConstraint:{}",
                has_value_term
            )));
            let component_id = context.get_or_create_component_id(id_term);
            new_components.insert(component_id, component);
        }
    }

    // sh:in
    if let Some(in_terms) = pred_obj_pairs.get(&shacl.in_.into_owned()) {
        processed_predicates.insert(shacl.in_.into_owned());
        if let Some(list_head_term) = in_terms.first() {
            // list_head_term is &Term
            let list_items = context.parse_rdf_list(list_head_term.clone()); // Vec<Term>
            let values: Vec<Term> = list_items.into_iter().collect(); // Already Vec<Term>

            let component = Component::InConstraint(InConstraintComponent::new(values));
            let component_id = context.get_or_create_component_id(list_head_term.clone());
            new_components.insert(component_id, component);
        }
    }

    // sh:sparql
    if let Some(sparql_terms) = pred_obj_pairs.get(&shacl.sparql.into_owned()) {
        processed_predicates.insert(shacl.sparql.into_owned());
        for sparql_term in sparql_terms {
            // sparql_term is &Term, which is the constraint details node.
            let component =
                Component::SPARQLConstraint(SPARQLConstraintComponent::new(sparql_term.clone()));
            let component_id = context.get_or_create_component_id(sparql_term.clone());
            new_components.insert(component_id, component);
        }
    }

    // SPARQL-based Constraint Components
    let (custom_component_defs, param_to_component) =
        sparql::parse_custom_constraint_components(context);

    let mut shape_predicates: HashSet<NamedNode> = pred_obj_pairs.keys().cloned().collect();
    for p in processed_predicates {
        shape_predicates.remove(&p);
    }

    let mut component_candidates: HashSet<NamedNode> = HashSet::new();
    for p in &shape_predicates {
        if let Some(ccs) = param_to_component.get(p) {
            for cc in ccs {
                component_candidates.insert(cc.clone());
            }
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
                let component = Component::CustomConstraint(CustomConstraintComponent {
                    definition: cc_def.clone(),
                    parameter_values,
                });
                let id_term = Term::Literal(Literal::new_simple_literal(format!(
                    "CustomConstraint:{}",
                    cc_iri.as_str()
                )));
                let component_id = context.get_or_create_component_id(id_term);
                new_components.insert(component_id, component);
            }
        }
    }

    new_components
}

/// A trait for components that can be represented in Graphviz DOT format.
pub(crate) trait GraphvizOutput {
    /// Generates a Graphviz DOT string representation for the component.
    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String;
    /// Returns the SHACL IRI for the component type (e.g., `sh:MinCountConstraintComponent`).
    fn component_type(&self) -> NamedNode;
}

/// A trait for constraint components that can perform validation.
pub(crate) trait ValidateComponent {
    /// Validates the given context against the component's logic.
    ///
    /// # Arguments
    ///
    /// * `component_id` - The ID of this component instance.
    /// * `c` - The mutable `Context` for the current validation.
    /// * `context` - The overall `ValidationContext`.
    /// * `trace` - A mutable reference to the execution trace.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of `ComponentValidationResult`s on success,
    /// or an error string on failure.
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context, // Changed to &mut Context
        context: &ValidationContext,
        trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String>;
}

/// An enum representing any of the SHACL constraint components.
#[derive(Debug)]
pub(crate) enum Component {
    /// `sh:node`
    NodeConstraint(NodeConstraintComponent),
    /// `sh:property`
    PropertyConstraint(PropertyConstraintComponent),
    /// `sh:qualifiedValueShape`
    QualifiedValueShape(QualifiedValueShapeComponent),

    // value type
    /// `sh:class`
    ClassConstraint(ClassConstraintComponent),
    /// `sh:datatype`
    DatatypeConstraint(DatatypeConstraintComponent),
    /// `sh:nodeKind`
    NodeKindConstraint(NodeKindConstraintComponent),

    // cardinality constraints
    /// `sh:minCount`
    MinCount(MinCountConstraintComponent),
    /// `sh:maxCount`
    MaxCount(MaxCountConstraintComponent),

    // value range constraints
    /// `sh:minExclusive`
    MinExclusiveConstraint(MinExclusiveConstraintComponent),
    /// `sh:minInclusive`
    MinInclusiveConstraint(MinInclusiveConstraintComponent),
    /// `sh:maxExclusive`
    MaxExclusiveConstraint(MaxExclusiveConstraintComponent),
    /// `sh:maxInclusive`
    MaxInclusiveConstraint(MaxInclusiveConstraintComponent),

    // string-based constraints
    /// `sh:minLength`
    MinLengthConstraint(MinLengthConstraintComponent),
    /// `sh:maxLength`
    MaxLengthConstraint(MaxLengthConstraintComponent),
    /// `sh:pattern`
    PatternConstraint(PatternConstraintComponent),
    /// `sh:languageIn`
    LanguageInConstraint(LanguageInConstraintComponent),
    /// `sh:uniqueLang`
    UniqueLangConstraint(UniqueLangConstraintComponent),

    // property pair constraints
    /// `sh:equals`
    EqualsConstraint(EqualsConstraintComponent),
    /// `sh:disjoint`
    DisjointConstraint(DisjointConstraintComponent),
    /// `sh:lessThan`
    LessThanConstraint(LessThanConstraintComponent),
    /// `sh:lessThanOrEquals`
    LessThanOrEqualsConstraint(LessThanOrEqualsConstraintComponent),

    // logical constraints
    /// `sh:not`
    NotConstraint(NotConstraintComponent),
    /// `sh:and`
    AndConstraint(AndConstraintComponent),
    /// `sh:or`
    OrConstraint(OrConstraintComponent),
    /// `sh:xone`
    XoneConstraint(XoneConstraintComponent),

    // other constraint components
    /// `sh:closed`
    ClosedConstraint(ClosedConstraintComponent),
    /// `sh:hasValue`
    HasValueConstraint(HasValueConstraintComponent),
    /// `sh:in`
    InConstraint(InConstraintComponent),
    /// `sh:sparql`
    SPARQLConstraint(SPARQLConstraintComponent),
    /// A constraint from a SPARQL-based constraint component
    CustomConstraint(CustomConstraintComponent),
}

impl Component {
    /// Returns a human-readable label for the component type.
    pub(crate) fn label(&self) -> String {
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
            Component::SPARQLConstraint(_) => "SPARQLConstraint".to_string(),
            Component::CustomConstraint(c) => c.local_name(),
        }
    }

    /// Delegates to the inner component to get its SHACL IRI type.
    pub(crate) fn component_type(&self) -> NamedNode {
        match self {
            Component::NodeConstraint(c) => c.component_type(),
            Component::PropertyConstraint(c) => c.component_type(),
            Component::QualifiedValueShape(c) => c.component_type(),
            Component::ClassConstraint(c) => c.component_type(),
            Component::DatatypeConstraint(c) => c.component_type(),
            Component::NodeKindConstraint(c) => c.component_type(),
            Component::MinCount(c) => c.component_type(),
            Component::MaxCount(c) => c.component_type(),
            Component::MinExclusiveConstraint(c) => c.component_type(),
            Component::MinInclusiveConstraint(c) => c.component_type(),
            Component::MaxExclusiveConstraint(c) => c.component_type(),
            Component::MaxInclusiveConstraint(c) => c.component_type(),
            Component::MinLengthConstraint(c) => c.component_type(),
            Component::MaxLengthConstraint(c) => c.component_type(),
            Component::PatternConstraint(c) => c.component_type(),
            Component::LanguageInConstraint(c) => c.component_type(),
            Component::UniqueLangConstraint(c) => c.component_type(),
            Component::EqualsConstraint(c) => c.component_type(),
            Component::DisjointConstraint(c) => c.component_type(),
            Component::LessThanConstraint(c) => c.component_type(),
            Component::LessThanOrEqualsConstraint(c) => c.component_type(),
            Component::NotConstraint(c) => c.component_type(),
            Component::AndConstraint(c) => c.component_type(),
            Component::OrConstraint(c) => c.component_type(),
            Component::XoneConstraint(c) => c.component_type(),
            Component::ClosedConstraint(c) => c.component_type(),
            Component::HasValueConstraint(c) => c.component_type(),
            Component::InConstraint(c) => c.component_type(),
            Component::SPARQLConstraint(c) => c.component_type(),
            Component::CustomConstraint(c) => c.component_type(),
        }
    }

    /// Delegates to the inner component to generate its Graphviz representation.
    pub(crate) fn to_graphviz_string(
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
            Component::SPARQLConstraint(c) => c.to_graphviz_string(component_id, context),
            Component::CustomConstraint(c) => c.to_graphviz_string(component_id, context),
        }
    }

    /// Delegates validation to the specific inner component.
    pub(crate) fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
        trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        trace.push(TraceItem::Component(component_id));
        match self {
            Component::ClassConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::NodeConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::PropertyConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::QualifiedValueShape(comp) => comp.validate(component_id, c, context, trace),
            Component::DatatypeConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::NodeKindConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::MinCount(comp) => comp.validate(component_id, c, context, trace),
            Component::MaxCount(comp) => comp.validate(component_id, c, context, trace),
            Component::MinLengthConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::MaxLengthConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::PatternConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::LanguageInConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::UniqueLangConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::NotConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::AndConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::OrConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::XoneConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::HasValueConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::InConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::SPARQLConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::DisjointConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::EqualsConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::LessThanConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::LessThanOrEqualsConstraint(comp) => {
                comp.validate(component_id, c, context, trace)
            }
            Component::MinExclusiveConstraint(comp) => {
                comp.validate(component_id, c, context, trace)
            }
            Component::MinInclusiveConstraint(comp) => {
                comp.validate(component_id, c, context, trace)
            }
            Component::MaxExclusiveConstraint(comp) => {
                comp.validate(component_id, c, context, trace)
            }
            Component::MaxInclusiveConstraint(comp) => {
                comp.validate(component_id, c, context, trace)
            }
            Component::ClosedConstraint(comp) => comp.validate(component_id, c, context, trace),
            Component::CustomConstraint(comp) => comp.validate(component_id, c, context, trace),
        }
    }
}

/// Checks if a given node (represented by `node_as_context`) conforms to the `shape_to_check_against`.
/// Returns a `ConformanceReport` indicating success or detailing the first validation failure.
/// Returns `Err(String)` for an internal processing error.
pub(crate) fn check_conformance_for_node(
    node_as_context: &mut Context,
    shape_to_check_against: &NodeShape,
    main_validation_context: &ValidationContext,
    trace: &mut Vec<TraceItem>,
) -> Result<ConformanceReport, String> {
    trace.push(TraceItem::NodeShape(*shape_to_check_against.identifier()));

    for constraint_id in shape_to_check_against.constraints() {
        let component = main_validation_context
            .get_component_by_id(constraint_id)
            .ok_or_else(|| format!("Logical check: Component not found: {}", constraint_id))?;

        match component.validate(
            *constraint_id,
            node_as_context,
            main_validation_context,
            trace,
        ) {
            Ok(validation_results) => {
                // Find the first failure, if any.
                if let Some(ComponentValidationResult::Fail(_ctx, failure)) = validation_results
                    .into_iter()
                    .find(|r| matches!(r, ComponentValidationResult::Fail(_, _)))
                {
                    // A failure was found. The node does not conform.
                    return Ok(ConformanceReport::NonConforms(failure));
                }
                // All results were Pass or the vec was empty, so continue to the next constraint.
            }
            Err(e) => {
                // The component's validate method returned an Err, meaning a processing error.
                return Err(e);
            }
        }
    }
    Ok(ConformanceReport::Conforms) // All results passed for the node_as_context against shape_to_check_against
}
