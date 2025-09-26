//! Builds executable validators from structural descriptors.

use crate::model::components::ComponentDescriptor;
use crate::runtime::validators;
use crate::runtime::{Component, CustomConstraintComponent};

/// Responsible for building runtime evaluators from high-level component descriptors.
pub(crate) struct RuntimeEngine;

impl RuntimeEngine {
    /// Builds a runtime component from its structural descriptor.
    pub(crate) fn build_component(descriptor: &ComponentDescriptor) -> Component {
        build_component_from_descriptor(descriptor)
    }
}

/// Creates a concrete runtime component from a structural descriptor.
pub(crate) fn build_component_from_descriptor(descriptor: &ComponentDescriptor) -> Component {
    match descriptor {
        ComponentDescriptor::Node { shape } => {
            Component::NodeConstraint(validators::NodeConstraintComponent::new(*shape))
        }
        ComponentDescriptor::Property { shape } => {
            Component::PropertyConstraint(validators::PropertyConstraintComponent::new(*shape))
        }
        ComponentDescriptor::QualifiedValueShape {
            shape,
            min_count,
            max_count,
            disjoint,
        } => Component::QualifiedValueShape(validators::QualifiedValueShapeComponent::new(
            *shape, *min_count, *max_count, *disjoint,
        )),
        ComponentDescriptor::Class { class } => {
            Component::ClassConstraint(validators::ClassConstraintComponent::new(class.clone()))
        }
        ComponentDescriptor::Datatype { datatype } => Component::DatatypeConstraint(
            validators::DatatypeConstraintComponent::new(datatype.clone()),
        ),
        ComponentDescriptor::NodeKind { node_kind } => Component::NodeKindConstraint(
            validators::NodeKindConstraintComponent::new(node_kind.clone()),
        ),
        ComponentDescriptor::MinCount { min_count } => {
            Component::MinCount(validators::MinCountConstraintComponent::new(*min_count))
        }
        ComponentDescriptor::MaxCount { max_count } => {
            Component::MaxCount(validators::MaxCountConstraintComponent::new(*max_count))
        }
        ComponentDescriptor::MinExclusive { value } => Component::MinExclusiveConstraint(
            validators::MinExclusiveConstraintComponent::new(value.clone()),
        ),
        ComponentDescriptor::MinInclusive { value } => Component::MinInclusiveConstraint(
            validators::MinInclusiveConstraintComponent::new(value.clone()),
        ),
        ComponentDescriptor::MaxExclusive { value } => Component::MaxExclusiveConstraint(
            validators::MaxExclusiveConstraintComponent::new(value.clone()),
        ),
        ComponentDescriptor::MaxInclusive { value } => Component::MaxInclusiveConstraint(
            validators::MaxInclusiveConstraintComponent::new(value.clone()),
        ),
        ComponentDescriptor::MinLength { length } => {
            Component::MinLengthConstraint(validators::MinLengthConstraintComponent::new(*length))
        }
        ComponentDescriptor::MaxLength { length } => {
            Component::MaxLengthConstraint(validators::MaxLengthConstraintComponent::new(*length))
        }
        ComponentDescriptor::Pattern { pattern, flags } => Component::PatternConstraint(
            validators::PatternConstraintComponent::new(pattern.clone(), flags.clone()),
        ),
        ComponentDescriptor::LanguageIn { languages } => Component::LanguageInConstraint(
            validators::LanguageInConstraintComponent::new(languages.clone()),
        ),
        ComponentDescriptor::UniqueLang { enabled } => Component::UniqueLangConstraint(
            validators::UniqueLangConstraintComponent::new(*enabled),
        ),
        ComponentDescriptor::Equals { property } => Component::EqualsConstraint(
            validators::EqualsConstraintComponent::new(property.clone()),
        ),
        ComponentDescriptor::Disjoint { property } => Component::DisjointConstraint(
            validators::DisjointConstraintComponent::new(property.clone()),
        ),
        ComponentDescriptor::LessThan { property } => Component::LessThanConstraint(
            validators::LessThanConstraintComponent::new(property.clone()),
        ),
        ComponentDescriptor::LessThanOrEquals { property } => {
            Component::LessThanOrEqualsConstraint(
                validators::LessThanOrEqualsConstraintComponent::new(property.clone()),
            )
        }
        ComponentDescriptor::Not { shape } => {
            Component::NotConstraint(validators::NotConstraintComponent::new(*shape))
        }
        ComponentDescriptor::And { shapes } => {
            Component::AndConstraint(validators::AndConstraintComponent::new(shapes.clone()))
        }
        ComponentDescriptor::Or { shapes } => {
            Component::OrConstraint(validators::OrConstraintComponent::new(shapes.clone()))
        }
        ComponentDescriptor::Xone { shapes } => {
            Component::XoneConstraint(validators::XoneConstraintComponent::new(shapes.clone()))
        }
        ComponentDescriptor::Closed {
            closed,
            ignored_properties,
        } => Component::ClosedConstraint(validators::ClosedConstraintComponent::new(
            *closed,
            if ignored_properties.is_empty() {
                None
            } else {
                Some(ignored_properties.clone())
            },
        )),
        ComponentDescriptor::HasValue { value } => Component::HasValueConstraint(
            validators::HasValueConstraintComponent::new(value.clone()),
        ),
        ComponentDescriptor::In { values } => {
            Component::InConstraint(validators::InConstraintComponent::new(values.clone()))
        }
        ComponentDescriptor::Sparql { constraint_node } => Component::SPARQLConstraint(
            validators::SPARQLConstraintComponent::new(constraint_node.clone()),
        ),
        ComponentDescriptor::Custom {
            definition,
            parameter_values,
        } => Component::CustomConstraint(CustomConstraintComponent {
            definition: definition.clone(),
            parameter_values: parameter_values.clone(),
        }),
    }
}
