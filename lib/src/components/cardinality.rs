use crate::context::{Context, ValidationContext};
use crate::types::ComponentID;

use super::{ComponentValidationResult, GraphvizOutput, ValidateComponent};

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
        c: &mut Context,              // Changed to &mut Context
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

impl MaxCountConstraintComponent {
    pub fn new(max_count: u64) -> Self {
        MaxCountConstraintComponent { max_count }
    }
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
        c: &mut Context,              // Changed to &mut Context
        _context: &ValidationContext, // context is not used
    ) -> Result<ComponentValidationResult, String> {
        println!(
            "Validating MaxCountConstraintComponent with ID: {} value nodes: {:?}",
            component_id.to_graphviz_id(), c.value_nodes()
        );
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
