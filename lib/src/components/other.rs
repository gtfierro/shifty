use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::types::ComponentID;
use oxigraph::model::{NamedNode, Term};
use std::vec::Vec;

use super::{ComponentValidationResult, GraphvizOutput, ValidateComponent, ValidationFailure};

impl ValidateComponent for InConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context, // Changed to &mut Context
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
                Ok(ComponentValidationResult::Fail(ValidationFailure {
                    component_id,
                    failed_value_node: None,
                    message: format!(
                        "sh:in constraint has an empty list, but value nodes {:?} exist.",
                        c.value_nodes().unwrap_or(&Vec::new()) // Provide empty vec for formatting if None
                    ),
                }))
            };
        }

        match c.value_nodes().cloned() {
            Some(value_nodes) => {
                for vn in value_nodes {
                    if !self.values.contains(&vn) {
                        c.with_value(vn.clone());
                        return Ok(ComponentValidationResult::Fail(ValidationFailure {
                            component_id,
                            failed_value_node: Some(vn.clone()),
                            message: format!(
                                "Value {:?} is not in the allowed list {:?}.",
                                vn, self.values
                            ),
                        }));
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

// Other Constraint Components
#[derive(Debug)]
pub struct ClosedConstraintComponent {
    closed: bool,
    ignored_properties: Option<Vec<Term>>,
}

impl ClosedConstraintComponent {
    pub fn new(closed: bool, ignored_properties: Option<Vec<Term>>) -> Self {
        ClosedConstraintComponent {
            closed,
            ignored_properties,
        }
    }
}

impl GraphvizOutput for ClosedConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#ClosedConstraintComponent")
    }

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

impl HasValueConstraintComponent {
    pub fn new(value: Term) -> Self {
        HasValueConstraintComponent { value }
    }
}

impl GraphvizOutput for HasValueConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#HasValueConstraintComponent")
    }

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
        c: &mut Context, // Changed to &mut Context
        _validation_context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        match c.value_nodes() {
            Some(value_nodes) => {
                if value_nodes.iter().any(|vn| vn == &self.value) {
                    // At least one value node is equal to self.value
                    Ok(ComponentValidationResult::Pass(component_id))
                } else {
                    // No value node is equal to self.value
                    Ok(ComponentValidationResult::Fail(ValidationFailure {
                        component_id,
                        failed_value_node: None,
                        message: format!(
                            "None of the value nodes {:?} are equal to the required value {:?}",
                            value_nodes, self.value
                        ),
                    }))
                }
            }
            None => {
                // No value nodes present, so self.value cannot be among them.
                Ok(ComponentValidationResult::Fail(ValidationFailure {
                    component_id,
                    failed_value_node: None,
                    message: format!(
                        "No value nodes found to check against required value {:?}",
                        self.value
                    ),
                }))
            }
        }
    }
}

#[derive(Debug)]
pub struct InConstraintComponent {
    values: Vec<Term>,
}

impl InConstraintComponent {
    pub fn new(values: Vec<Term>) -> Self {
        InConstraintComponent { values }
    }
}

impl GraphvizOutput for InConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#InConstraintComponent")
    }

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
