use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::types::{ComponentID, ID};

use super::{GraphvizOutput, ValidateComponent, ComponentValidationResult, check_conformance_for_node};

// logical constraints
#[derive(Debug)]
pub struct NotConstraintComponent {
    shape: ID, // NodeShape ID
}

impl NotConstraintComponent {
    pub fn new(shape: ID) -> Self {
        NotConstraintComponent { shape }
    }
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
        c: &mut Context, // Changed to &mut Context
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
            let mut value_node_as_context = Context::new( // Made mutable
                value_node_to_check.clone(),
                None, // Path is not directly relevant for this sub-check's context
                Some(vec![value_node_to_check.clone()]), // Value nodes for the sub-check
                *negated_node_shape.identifier() // Source shape is the one being checked against
            );

            match check_conformance_for_node(
                &mut value_node_as_context, // Pass mutably
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

impl AndConstraintComponent {
    pub fn new(shapes: Vec<ID>) -> Self {
        AndConstraintComponent { shapes }
    }
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
        c: &mut Context, // Changed to &mut Context
        validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let Some(value_nodes) = c.value_nodes() else {
            return Ok(ComponentValidationResult::Pass(component_id)); // No value nodes
        };

        for value_node_to_check in value_nodes {
            // The source_shape for the context used in check_conformance_for_node
            // will be set to the specific conjunct_node_shape's ID.
            for conjunct_shape_id in &self.shapes {
                let mut value_node_as_context = Context::new( // Made mutable
                    value_node_to_check.clone(),
                    None,
                    Some(vec![value_node_to_check.clone()]),
                    *conjunct_shape_id // Source shape is the conjunct being checked
                );
                let Some(conjunct_node_shape) = validation_context.get_node_shape_by_id(conjunct_shape_id) else {
                    return Err(format!(
                        "sh:and referenced shape {:?} not found",
                        conjunct_shape_id
                    ));
                };

                match check_conformance_for_node(
                    &mut value_node_as_context, // Pass mutably
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

impl OrConstraintComponent {
    pub fn new(shapes: Vec<ID>) -> Self {
        OrConstraintComponent { shapes }
    }
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
        c: &mut Context, // Changed to &mut Context
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
            let mut passed_at_least_one_disjunct = false;
            // The source_shape for the context used in check_conformance_for_node
            // will be set to the specific disjunct_node_shape's ID.
            for disjunct_shape_id in &self.shapes {
                let mut value_node_as_context = Context::new( // Made mutable
                    value_node_to_check.clone(),
                    None,
                    Some(vec![value_node_to_check.clone()]),
                    *disjunct_shape_id // Source shape is the disjunct being checked
                );
                let Some(disjunct_node_shape) = validation_context.get_node_shape_by_id(disjunct_shape_id) else {
                    return Err(format!(
                        "sh:or referenced shape {:?} not found",
                        disjunct_shape_id
                    ));
                };

                match check_conformance_for_node(
                    &mut value_node_as_context, // Pass mutably
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

impl XoneConstraintComponent {
    pub fn new(shapes: Vec<ID>) -> Self {
        XoneConstraintComponent { shapes }
    }
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
        c: &mut Context, // Changed to &mut Context
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
            let mut conforming_shapes_count = 0;
            // The source_shape for the context used in check_conformance_for_node
            // will be set to the specific xone_node_shape's ID.
            for xone_shape_id in &self.shapes {
                let mut value_node_as_context = Context::new( // Made mutable
                    value_node_to_check.clone(),
                    None,
                    Some(vec![value_node_to_check.clone()]),
                    *xone_shape_id // Source shape is the xone option being checked
                );
                let Some(xone_node_shape) = validation_context.get_node_shape_by_id(xone_shape_id) else {
                    return Err(format!(
                        "sh:xone referenced shape {:?} not found",
                        xone_shape_id
                    ));
                };

                match check_conformance_for_node(
                    &mut value_node_as_context, // Pass mutably
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
