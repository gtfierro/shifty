use crate::context::{format_term_for_label, Context, SourceShape, ValidationContext};
use crate::types::{ComponentID, TraceItem, ID};
use oxigraph::model::NamedNode;

use crate::runtime::{
    check_conformance_for_node, ComponentValidationResult, ConformanceReport, GraphvizOutput,
    ValidateComponent, ValidationFailure,
};

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
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#NotConstraintComponent")
    }

    fn to_graphviz_string(&self, component_id: ComponentID, context: &ValidationContext) -> String {
        let shape_term_str = context
            .model
            .nodeshape_id_lookup()
            .read()
            .unwrap()
            .get_term(self.shape)
            .map_or_else(
                || format!("MissingNodeShape:{}", self.shape),
                format_term_for_label,
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
        c: &mut Context,
        validation_context: &ValidationContext,
        trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let Some(value_nodes) = c.value_nodes() else {
            return Ok(vec![]); // No value nodes to check
        };

        let Some(negated_node_shape) = validation_context.model.get_node_shape_by_id(&self.shape)
        else {
            return Err(format!(
                "sh:not referenced shape {:?} not found",
                self.shape
            ));
        };

        let mut results = Vec::new();

        for value_node_to_check in value_nodes {
            // Create a new context where the current value_node is the focus node.
            let mut value_node_as_context = Context::new(
                value_node_to_check.clone(),
                None, // Path is not directly relevant for this sub-check's context
                Some(vec![value_node_to_check.clone()]), // Value nodes for the sub-check
                SourceShape::NodeShape(*negated_node_shape.identifier()), // Source shape is the one being checked against
                c.trace_index(),
            );
            let result = check_conformance_for_node(
                &mut value_node_as_context,
                negated_node_shape,
                validation_context,
                trace,
            )?;

            match result {
                ConformanceReport::Conforms => {
                    // value_node_to_check CONFORMS to the negated_node_shape.
                    // This means the sh:not constraint FAILS for this value_node.
                    let mut error_context = c.clone();
                    error_context.with_value(value_node_to_check.clone());
                    let message = format!(
                        "Value {:?} conforms to sh:not shape {:?}, but should not.",
                        value_node_to_check, self.shape
                    );
                    let failure = ValidationFailure {
                        component_id,
                        failed_value_node: Some(value_node_to_check.clone()),
                        message,
                        result_path: None,
                        source_constraint: None,

                        severity: None,

                        message_terms: Vec::new(),
                    };
                    results.push(ComponentValidationResult::Fail(error_context, failure));
                }
                ConformanceReport::NonConforms(_) => {
                    // value_node_to_check DOES NOT CONFORM to the negated_node_shape.
                    // This means the sh:not constraint PASSES for this value_node. Continue.
                }
            }
        }

        Ok(results)
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
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#AndConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
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
        c: &mut Context,
        validation_context: &ValidationContext,
        trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let Some(value_nodes) = c.value_nodes() else {
            return Ok(vec![]); // No value nodes
        };
        let value_nodes = value_nodes.clone();
        let mut results = Vec::new();

        for value_node_to_check in value_nodes {
            // The source_shape for the context used in check_conformance_for_node
            // will be set to the specific conjunct_node_shape's ID.
            'conjunct_loop: for conjunct_shape_id in &self.shapes {
                let mut value_node_as_context = Context::new(
                    value_node_to_check.clone(),
                    None,
                    Some(vec![value_node_to_check.clone()]),
                    SourceShape::NodeShape(*conjunct_shape_id), // Source shape is the conjunct being checked
                    c.trace_index(),
                );
                let Some(conjunct_node_shape) = validation_context
                    .model
                    .get_node_shape_by_id(conjunct_shape_id)
                else {
                    return Err(format!(
                        "sh:and referenced shape {:?} not found",
                        conjunct_shape_id
                    ));
                };

                let result = check_conformance_for_node(
                    &mut value_node_as_context,
                    conjunct_node_shape,
                    validation_context,
                    trace,
                )?;

                match result {
                    ConformanceReport::Conforms => {
                        // value_node_to_check CONFORMS to this conjunct_node_shape. Continue to next conjunct.
                    }
                    ConformanceReport::NonConforms(failure) => {
                        // value_node_to_check DOES NOT CONFORM to this conjunct_node_shape.
                        // For sh:and, all shapes must conform. So, this is a failure for this value_node.
                        let mut error_context = c.clone();
                        error_context.with_value(value_node_to_check.clone());
                        let message = format!(
                            "Value {:?} does not conform to sh:and shape {:?}: {}",
                            value_node_to_check, conjunct_shape_id, failure.message
                        );
                        let failure = ValidationFailure {
                            component_id,
                            failed_value_node: Some(value_node_to_check.clone()),
                            message,
                            result_path: None,
                            source_constraint: None,

                            severity: None,

                            message_terms: Vec::new(),
                        };
                        results.push(ComponentValidationResult::Fail(error_context, failure));
                        break 'conjunct_loop; // Fails one, fails all for this value node.
                    }
                }
            }
            // If loop completes, value_node_to_check conformed to all conjunct_node_shapes.
        }

        Ok(results)
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
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#OrConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
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
        c: &mut Context,
        validation_context: &ValidationContext,
        trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let Some(value_nodes) = c.value_nodes() else {
            return Ok(vec![]); // No value nodes
        };

        if self.shapes.is_empty() {
            // If sh:or list is empty, no value node can conform unless there are no value nodes.
            return if value_nodes.is_empty() {
                Ok(vec![])
            } else {
                let failure = ValidationFailure {
                    component_id,
                    failed_value_node: value_nodes.first().cloned(),
                    message:
                        "sh:or with an empty list of shapes cannot be satisfied by any value node."
                            .to_string(),
                    result_path: None,
                    source_constraint: None,

                    severity: None,

                    message_terms: Vec::new(),
                };
                Ok(vec![ComponentValidationResult::Fail(c.clone(), failure)])
            };
        }
        let value_nodes = value_nodes.clone();
        let mut results = Vec::new();

        for value_node_to_check in value_nodes {
            let mut passed_at_least_one_disjunct = false;
            // The source_shape for the context used in check_conformance_for_node
            // will be set to the specific disjunct_node_shape's ID.
            for disjunct_shape_id in &self.shapes {
                let mut value_node_as_context = Context::new(
                    value_node_to_check.clone(),
                    None,
                    Some(vec![value_node_to_check.clone()]),
                    SourceShape::NodeShape(*disjunct_shape_id), // Source shape is the disjunct being checked
                    c.trace_index(),
                );
                let Some(disjunct_node_shape) = validation_context
                    .model
                    .get_node_shape_by_id(disjunct_shape_id)
                else {
                    return Err(format!(
                        "sh:or referenced shape {:?} not found",
                        disjunct_shape_id
                    ));
                };

                let result = check_conformance_for_node(
                    &mut value_node_as_context,
                    disjunct_node_shape,
                    validation_context,
                    trace,
                )?;

                match result {
                    ConformanceReport::Conforms => {
                        // value_node_to_check CONFORMS to this disjunct_node_shape.
                        // For sh:or, this is enough for this value_node.
                        passed_at_least_one_disjunct = true;
                        break; // Move to the next value_node_to_check
                    }
                    ConformanceReport::NonConforms(_) => {
                        // value_node_to_check DOES NOT CONFORM. Try next disjunct shape.
                    }
                }
            }
            if !passed_at_least_one_disjunct {
                // This value_node_to_check did not conform to any of the sh:or shapes.
                let mut error_context = c.clone();
                error_context.with_value(value_node_to_check.clone());
                let message = format!(
                    "Value {:?} does not conform to any sh:or shapes.",
                    value_node_to_check
                );
                let failure = ValidationFailure {
                    component_id,
                    failed_value_node: Some(value_node_to_check.clone()),
                    message,
                    result_path: None,
                    source_constraint: None,

                    severity: None,

                    message_terms: Vec::new(),
                };
                results.push(ComponentValidationResult::Fail(error_context, failure));
            }
            // If loop completes, value_node_to_check conformed to at least one disjunct.
        }

        Ok(results)
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
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#XoneConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
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
        c: &mut Context,
        validation_context: &ValidationContext,
        trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let Some(value_nodes) = c.value_nodes() else {
            return Ok(vec![]); // No value nodes
        };

        if self.shapes.is_empty() {
            // If sh:xone list is empty, no value node can conform unless there are no value nodes.
            return if value_nodes.is_empty() {
                Ok(vec![])
            } else {
                let failure = ValidationFailure {
                    component_id,
                    failed_value_node: value_nodes.first().cloned(),
                    message: "sh:xone with an empty list of shapes cannot be satisfied by any value node.".to_string(),
                    result_path: None,
                    source_constraint: None,

                    severity: None,

                    message_terms: Vec::new(),
                };
                Ok(vec![ComponentValidationResult::Fail(c.clone(), failure)])
            };
        }

        let mut results = Vec::new();
        let value_nodes = value_nodes.clone();

        for value_node_to_check in value_nodes {
            let mut conforming_shapes_count = 0;
            // The source_shape for the context used in check_conformance_for_node
            // will be set to the specific xone_node_shape's ID.
            for xone_shape_id in &self.shapes {
                let mut value_node_as_context = Context::new(
                    value_node_to_check.clone(),
                    None,
                    Some(vec![value_node_to_check.clone()]),
                    SourceShape::NodeShape(*xone_shape_id), // Source shape is the xone option being checked
                    c.trace_index(),
                );
                let Some(xone_node_shape) =
                    validation_context.model.get_node_shape_by_id(xone_shape_id)
                else {
                    return Err(format!(
                        "sh:xone referenced shape {:?} not found",
                        xone_shape_id
                    ));
                };

                let result = check_conformance_for_node(
                    &mut value_node_as_context,
                    xone_node_shape,
                    validation_context,
                    trace,
                )?;

                match result {
                    ConformanceReport::Conforms => {
                        // value_node_to_check CONFORMS to this xone_node_shape.
                        conforming_shapes_count += 1;
                    }
                    ConformanceReport::NonConforms(_) => {
                        // value_node_to_check DOES NOT CONFORM. Continue.
                    }
                }
            }

            if conforming_shapes_count != 1 {
                // This value_node_to_check did not conform to exactly one of the sh:xone shapes.
                let mut error_context = c.clone();
                error_context.with_value(value_node_to_check.clone());
                let message = format!(
                    "Value {:?} conformed to {} sh:xone shapes, but expected exactly 1.",
                    value_node_to_check, conforming_shapes_count
                );
                let failure = ValidationFailure {
                    component_id,
                    failed_value_node: Some(value_node_to_check.clone()),
                    message,
                    result_path: None,
                    source_constraint: None,

                    severity: None,

                    message_terms: Vec::new(),
                };
                results.push(ComponentValidationResult::Fail(error_context, failure));
            }
            // If loop completes, value_node_to_check conformed to exactly one xone_shape.
        }

        Ok(results)
    }
}
