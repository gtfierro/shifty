use crate::context::{Context, ValidationContext};
use crate::types::ComponentID;
use oxigraph::model::NamedNode;

use super::{ComponentValidationResult, GraphvizOutput, ValidateComponent, ValidationFailure};

#[derive(Debug)]
pub struct MinCountConstraintComponent {
    min_count: u64,
}

impl MinCountConstraintComponent {
    pub fn new(min_count: u64) -> Self {
        MinCountConstraintComponent { min_count }
    }
}

impl GraphvizOutput for MinCountConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#MinCountConstraintComponent")
    }

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
        c: &mut Context,
        _context: &ValidationContext,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let count = c.value_nodes().map_or(0, |v| v.len());
        if count < self.min_count as usize {
            let failure = ValidationFailure {
                component_id,
                failed_value_node: None,
                message: format!(
                    "Value count ({}) does not meet minimum requirement: {}",
                    count, self.min_count
                ),
            };
            Ok(vec![ComponentValidationResult::Fail(c.clone(), failure)])
        } else {
            Ok(vec![])
        }
    }
}

#[derive(Debug)]
pub struct MaxCountConstraintComponent {
    max_count: u64,
}

impl MaxCountConstraintComponent {
    pub fn new(max_count: u64) -> Self {
        MaxCountConstraintComponent { max_count }
    }
}

impl GraphvizOutput for MaxCountConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#MaxCountConstraintComponent")
    }

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
        c: &mut Context,
        _context: &ValidationContext,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let count = c.value_nodes().map_or(0, |v| v.len());
        if count > self.max_count as usize {
            let failure = ValidationFailure {
                component_id,
                failed_value_node: None,
                message: format!(
                    "Value count ({}) exceeds maximum requirement: {}",
                    count, self.max_count
                ),
            };
            Ok(vec![ComponentValidationResult::Fail(c.clone(), failure)])
        } else {
            Ok(vec![])
        }
    }
}
