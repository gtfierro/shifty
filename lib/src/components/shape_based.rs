use crate::context::{format_term_for_label, Context, ValidationContext, SourceShape};
use crate::report::ValidationReportBuilder;
use crate::types::{ComponentID, PropShapeID, ID};
use oxigraph::model::NamedNode;
// Removed: use oxigraph::model::Term;

use super::{
    check_conformance_for_node, Component, ComponentValidationResult, GraphvizOutput,
    ValidateComponent,
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
        c: &mut Context, // Changed to &mut Context
        validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let value_nodes = match c.value_nodes() {
            Some(vns) => vns,
            None => {
                // No value nodes. Check if min_count > 0.
                if let Some(min_c) = self.min_count {
                    if min_c > 0 {
                        return Err(format!(
                            "QualifiedValueShape: No value nodes, but sh:qualifiedMinCount is {} for shape {:?}",
                            min_c, self.shape
                        ));
                    }
                }
                // If min_count is 0 or None, this is a pass. Max_count is also satisfied.
                return Ok(ComponentValidationResult::Pass(component_id));
            }
        };

        let Some(target_node_shape_for_self) = validation_context.get_node_shape_by_id(&self.shape)
        else {
            return Err(format!(
                "QualifiedValueShape: Referenced node shape {:?} for sh:qualifiedValueShape not found",
                self.shape
            ));
        };

        let mut sibling_target_node_shape_ids: std::collections::HashSet<ID> =
            std::collections::HashSet::new();
        if self.disjoint.unwrap_or(false) {
            // c.source_shape() now returns ID, which is the ID of the containing PropertyShape.
            let source_property_shape_id = c.source_shape().as_prop_id().unwrap().clone();

            let mut all_qvs_targets_from_relevant_parents: std::collections::HashSet<ID> =
                std::collections::HashSet::new();

            for (_parent_node_shape_id, parent_node_shape) in validation_context.node_shapes() {
                let mut is_parent_of_current_prop_shape = false;
                for constraint_id_on_parent in parent_node_shape.constraints() {
                    if let Some(Component::PropertyConstraint(pc)) =
                        validation_context.get_component_by_id(constraint_id_on_parent)
                    {
                        if pc.shape() == &source_property_shape_id {
                            is_parent_of_current_prop_shape = true;
                            break;
                        }
                    }
                }

                if is_parent_of_current_prop_shape {
                    // This parent_node_shape is one of the shapes `ps` from the spec.
                    // Collect all sh:property / sh:qualifiedValueShape target IDs from this parent.
                    for constraint_id_on_parent in parent_node_shape.constraints() {
                        if let Some(Component::PropertyConstraint(any_pc_on_parent)) =
                            validation_context.get_component_by_id(constraint_id_on_parent)
                        {
                            if let Some(any_prop_shape_on_parent) =
                                validation_context.get_prop_shape_by_id(any_pc_on_parent.shape())
                            {
                                for qvs_constraint_id_on_any_prop in
                                    any_prop_shape_on_parent.constraints()
                                {
                                    if let Some(Component::QualifiedValueShape(
                                        qvs_comp_on_any_prop,
                                    )) = validation_context
                                        .get_component_by_id(qvs_constraint_id_on_any_prop)
                                    {
                                        all_qvs_targets_from_relevant_parents
                                            .insert(qvs_comp_on_any_prop.shape);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Sibling shapes are this set minus self.shape (the QVS target of the current PropertyShape).
            sibling_target_node_shape_ids = all_qvs_targets_from_relevant_parents;
            sibling_target_node_shape_ids.remove(&self.shape);
        }

        let mut conforming_value_node_count = 0;
        for value_node_to_check in value_nodes {
            let mut value_node_as_context = Context::new(
                // Made mutable
                value_node_to_check.clone(),
                None, // Path is not directly relevant for this sub-check's context
                Some(vec![value_node_to_check.clone()]), // Value nodes for the sub-check
                SourceShape::NodeShape(self.shape), // Source shape is the target_node_shape_for_self (self.shape)
            );

            match check_conformance_for_node(
                &mut value_node_as_context, // Pass mutably
                target_node_shape_for_self,
                validation_context,
            ) {
                Ok(true) => {
                    // Conforms to self.shape
                    let mut conforms_to_a_sibling = false;
                    if self.disjoint.unwrap_or(false) && !sibling_target_node_shape_ids.is_empty() {
                        for sibling_shape_id in &sibling_target_node_shape_ids {
                            if let Some(sibling_node_shape) =
                                validation_context.get_node_shape_by_id(sibling_shape_id)
                            {
                                // Create a new context for checking against the sibling, with sibling's ID as source_shape
                                let mut sibling_check_context = Context::new(
                                    // Made mutable
                                    value_node_to_check.clone(),
                                    None,
                                    Some(vec![value_node_to_check.clone()]),
                                    SourceShape::NodeShape(*sibling_shape_id), // Source shape is the sibling shape
                                );
                                match check_conformance_for_node(
                                    &mut sibling_check_context, // Pass mutably
                                    sibling_node_shape,
                                    validation_context,
                                ) {
                                    Ok(true) => {
                                        // Conforms to this sibling shape
                                        conforms_to_a_sibling = true;
                                        break;
                                    }
                                    Ok(false) => {} // Does not conform to this sibling, continue
                                    Err(e) => {
                                        return Err(format!(
                                        "Error checking conformance against sibling shape {:?}: {}",
                                        sibling_shape_id, e
                                    ))
                                    }
                                }
                            } else {
                                return Err(format!("QualifiedValueShape: Sibling node shape {:?} for disjoint check not found", sibling_shape_id));
                            }
                        }
                    }

                    if !conforms_to_a_sibling {
                        conforming_value_node_count += 1;
                    }
                }
                Ok(false) => {} // Does not conform to self.shape, so don't count.
                Err(e) => {
                    return Err(format!(
                    "Error checking conformance for sh:qualifiedValueShape target shape {:?}: {}",
                    self.shape, e
                ))
                }
            }
        }

        if let Some(min_c) = self.min_count {
            if conforming_value_node_count < min_c {
                return Err(format!(
                    "Value count {} is less than sh:qualifiedMinCount {} for shape {:?}",
                    conforming_value_node_count, min_c, self.shape
                ));
            }
        }

        if let Some(max_c) = self.max_count {
            if conforming_value_node_count > max_c {
                return Err(format!(
                    "Value count {} is greater than sh:qualifiedMaxCount {} for shape {:?}",
                    conforming_value_node_count, max_c, self.shape
                ));
            }
        }

        Ok(ComponentValidationResult::Pass(component_id))
    }
}

impl ValidateComponent for NodeConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context, // Changed to &mut Context
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
            let mut value_node_as_context = Context::new(
                // Made mutable
                value_node_to_check.clone(),
                None, // Path is not directly relevant for this sub-check's context
                Some(vec![value_node_to_check.clone()]), // Value nodes for the sub-check
                SourceShape::NodeShape(*target_node_shape.identifier()), // Source shape is the one being checked against
            );

            match check_conformance_for_node(
                &mut value_node_as_context, // Pass mutably
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
        component_id: ComponentID,
        c: &mut Context,
        validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let mut builder = ValidationReportBuilder::new();
        if let Some(property_shape) = validation_context.get_prop_shape_by_id(&self.shape) {
            property_shape.validate(c, validation_context, &mut builder)?;

            if builder.results.is_empty() {
                Ok(ComponentValidationResult::Pass(component_id))
            } else {
                Ok(ComponentValidationResult::SubShape(builder.results))
            }
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

    fn to_graphviz_string(&self, component_id: ComponentID, validation_context: &ValidationContext) -> String {
        let shape_term_str = validation_context
            .propshape_id_lookup()
            .borrow()
            .get_term(*self.shape())
            .map_or_else(|| format!("MissingPropertyShape:{}", self.shape().0), |term| format!("{}", term));
        format!(
            "{} [label=\"PropertyConstraint: {}\"];",
            component_id.to_graphviz_id(),
            shape_term_str
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
