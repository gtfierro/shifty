use crate::context::{format_term_for_label, Context, SourceShape, ValidationContext};
use crate::types::{ComponentID, PropShapeID, ID};
use oxigraph::model::NamedNode;
// Removed: use oxigraph::model::Term;

use super::{
    check_conformance_for_node, ComponentValidationResult, ConformanceReport, GraphvizOutput,
    ValidateComponent, ValidationFailure,
};

#[derive(Debug)]
pub struct NodeConstraintComponent {
    shape: ID,
}

impl NodeConstraintComponent {
    pub fn new(shape: ID) -> Self {
        NodeConstraintComponent { shape }
    }
}

impl GraphvizOutput for NodeConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#NodeConstraintComponent")
    }

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

impl ValidateComponent for QualifiedValueShapeComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        validation_context: &ValidationContext,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let value_nodes = c.value_nodes().cloned().unwrap_or_default();

        let Some(target_node_shape) = validation_context.get_node_shape_by_id(&self.shape) else {
            return Err(format!("sh:qualifiedValueShape referenced shape {:?} not found", self.shape));
        };

        // Get sibling QualifiedValueShape shapes
        let mut sibling_shapes = Vec::new();
        if let Some(source_prop_id) = c.source_shape().as_prop_id() {
            if let Some(source_prop_shape) =
                validation_context.get_prop_shape_by_id(source_prop_id)
            {
                for sibling_component_id in source_prop_shape.constraints() {
                    if sibling_component_id == &component_id {
                        continue;
                    }

                    if let Some(super::Component::QualifiedValueShape(sibling_qvs)) =
                        validation_context.get_component_by_id(sibling_component_id)
                    {
                        if let Some(sibling_target_shape) =
                            validation_context.get_node_shape_by_id(&sibling_qvs.shape)
                        {
                            sibling_shapes.push(sibling_target_shape);
                        }
                    }
                }
            }
        }

        let mut qualified_nodes_count = 0;
        let mut validation_results = Vec::new();

        for value_node in &value_nodes {
            // Check conformance to target_node_shape
            let mut value_node_as_context = Context::new(
                value_node.clone(),
                None,
                Some(vec![value_node.clone()]),
                SourceShape::NodeShape(*target_node_shape.identifier()),
            );

            let conforms_to_target = match check_conformance_for_node(
                &mut value_node_as_context,
                target_node_shape,
                validation_context,
            )? {
                ConformanceReport::Conforms => true,
                ConformanceReport::NonConforms(_) => false,
            };

            if !conforms_to_target {
                continue;
            }

            // At this point, value_node conforms to the target shape.

            // Check if it conforms to any sibling shape
            let mut conforms_to_sibling = false;
            for sibling_shape in &sibling_shapes {
                let mut sibling_check_context = Context::new(
                    value_node.clone(),
                    None,
                    Some(vec![value_node.clone()]),
                    SourceShape::NodeShape(*sibling_shape.identifier()),
                );
                match check_conformance_for_node(
                    &mut sibling_check_context,
                    sibling_shape,
                    validation_context,
                )? {
                    ConformanceReport::Conforms => {
                        conforms_to_sibling = true;

                        // If disjoint is true, this is a failure.
                        if self.disjoint.unwrap_or(false) {
                            let failure = ValidationFailure {
                                component_id,
                                failed_value_node: Some(value_node.clone()),
                                message: format!("Value {:?} conforms to both this sh:qualifiedValueShape and a sibling, but sh:qualifiedValueShapesDisjoint is true.", value_node),
                            };
                            validation_results.push(ComponentValidationResult::Fail(c.clone(), failure));
                        }

                        break; // Found a conforming sibling, no need to check others for this value_node
                    }
                    ConformanceReport::NonConforms(_) => {}
                }
            }

            if !conforms_to_sibling {
                qualified_nodes_count += 1;
            }
        }

        // Check min/max counts
        if let Some(min) = self.min_count {
            if qualified_nodes_count < min {
                let failure = ValidationFailure {
                    component_id,
                    failed_value_node: None,
                    message: format!(
                        "Found {} values that conform to the qualified value shape and not to any sibling shapes, but at least {} were required.",
                        qualified_nodes_count, min
                    ),
                };
                validation_results.push(ComponentValidationResult::Fail(c.clone(), failure));
            }
        }

        if let Some(max) = self.max_count {
            if qualified_nodes_count > max {
                let failure = ValidationFailure {
                    component_id,
                    failed_value_node: None,
                    message: format!(
                        "Found {} values that conform to the qualified value shape and not to any sibling shapes, but at most {} were allowed.",
                        qualified_nodes_count, max
                    ),
                };
                validation_results.push(ComponentValidationResult::Fail(c.clone(), failure));
            }
        }

        Ok(validation_results)
    }
}

impl ValidateComponent for NodeConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        validation_context: &ValidationContext,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let Some(value_nodes) = c.value_nodes() else {
            return Ok(vec![]);
        };

        let Some(target_node_shape) = validation_context.get_node_shape_by_id(&self.shape) else {
            return Err(format!("sh:node referenced shape {:?} not found", self.shape));
        };

        let mut results = Vec::new();

        for value_node_to_check in value_nodes {
            let mut value_node_as_context = Context::new(
                value_node_to_check.clone(),
                None,
                Some(vec![value_node_to_check.clone()]),
                SourceShape::NodeShape(*target_node_shape.identifier()),
            );

            match check_conformance_for_node(
                &mut value_node_as_context,
                target_node_shape,
                validation_context,
            )? {
                ConformanceReport::Conforms => {
                    // Conforms, so this value node passes. Continue to the next.
                }
                ConformanceReport::NonConforms(inner_failure) => {
                    // Does not conform. This is a failure for the NodeConstraintComponent.
                    let mut error_context = c.clone();
                    println!(
                        "NodeConstraintComponent: Value {:?} does not conform to shape {:?}: {}",
                        value_node_to_check, self.shape, inner_failure.message
                    );
                    error_context.with_value(value_node_to_check.clone());
                    let failure = ValidationFailure {
                        component_id,
                        failed_value_node: Some(value_node_to_check.clone()),
                        message: inner_failure.message,
                    };
                    results.push(ComponentValidationResult::Fail(error_context, failure));
                }
            }
        }

        Ok(results)
    }
}

#[derive(Debug)]
pub struct PropertyConstraintComponent {
    shape: PropShapeID,
}

impl PropertyConstraintComponent {
    pub fn new(shape: PropShapeID) -> Self {
        PropertyConstraintComponent { shape }
    }

    pub fn shape(&self) -> &PropShapeID {
        &self.shape
    }
}

// Finish implementing the PropertyConstraintComponent by delegating
// validation to the referenced property shape.
impl ValidateComponent for PropertyConstraintComponent {
    fn validate(
        &self,
        _component_id: ComponentID,
        c: &mut Context,
        validation_context: &ValidationContext,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        if let Some(property_shape) = validation_context.get_prop_shape_by_id(&self.shape) {
            // Per SHACL spec for sh:property, the validation results from the property shape
            // are the results of this constraint.
            property_shape.validate(c, validation_context)
        } else {
            Err(format!(
                "Referenced property shape not found for ID: {:?}",
                self.shape
            ))
        }
    }
}
impl GraphvizOutput for PropertyConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#PropertyShapeComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        validation_context: &ValidationContext,
    ) -> String {
        let shape_term_str = validation_context
            .propshape_id_lookup()
            .borrow()
            .get_term(*self.shape())
            .map_or_else(
                || format!("MissingPropertyShape:{}", self.shape().0),
                |term| format!("{}", term),
            );
        format!(
            "{} [label=\"PropertyConstraint: {}\"];\n{} -> {};",
            component_id.to_graphviz_id(),
            shape_term_str,
            component_id.to_graphviz_id(),
            self.shape.to_graphviz_id(),
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

impl QualifiedValueShapeComponent {
    pub fn new(
        shape: ID,
        min_count: Option<u64>,
        max_count: Option<u64>,
        disjoint: Option<bool>,
    ) -> Self {
        QualifiedValueShapeComponent {
            shape,
            min_count,
            max_count,
            disjoint,
        }
    }
}

impl GraphvizOutput for QualifiedValueShapeComponent {
    fn component_type(&self) -> NamedNode {
        if self.min_count.is_some() {
            NamedNode::new_unchecked(
                "http://www.w3.org/ns/shacl#QualifiedMinCountConstraintComponent",
            )
        } else {
            NamedNode::new_unchecked(
                "http://www.w3.org/ns/shacl#QualifiedMaxCountConstraintComponent",
            )
        }
    }

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
