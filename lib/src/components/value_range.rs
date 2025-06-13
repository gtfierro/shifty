use crate::context::{format_term_for_label, ValidationContext};
use crate::named_nodes::SHACL;
use crate::types::ComponentID;
use oxigraph::model::{NamedNode, Term};

use super::GraphvizOutput;

// value range constraints
#[derive(Debug)]
pub struct MinExclusiveConstraintComponent {
    min_exclusive: Term,
}

impl MinExclusiveConstraintComponent {
    pub fn new(min_exclusive: Term) -> Self {
        MinExclusiveConstraintComponent { min_exclusive }
    }
}

impl GraphvizOutput for MinExclusiveConstraintComponent {
    fn component_type(&self) -> NamedNode {
        SHACL::new().min_exclusive_constraint_component
    }

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

impl MinInclusiveConstraintComponent {
    pub fn new(min_inclusive: Term) -> Self {
        MinInclusiveConstraintComponent { min_inclusive }
    }
}

impl GraphvizOutput for MinInclusiveConstraintComponent {
    fn component_type(&self) -> NamedNode {
        SHACL::new().min_inclusive_constraint_component
    }

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

impl MaxExclusiveConstraintComponent {
    pub fn new(max_exclusive: Term) -> Self {
        MaxExclusiveConstraintComponent { max_exclusive }
    }
}

impl GraphvizOutput for MaxExclusiveConstraintComponent {
    fn component_type(&self) -> NamedNode {
        SHACL::new().max_exclusive_constraint_component
    }

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

impl MaxInclusiveConstraintComponent {
    pub fn new(max_inclusive: Term) -> Self {
        MaxInclusiveConstraintComponent { max_inclusive }
    }
}

impl GraphvizOutput for MaxInclusiveConstraintComponent {
    fn component_type(&self) -> NamedNode {
        SHACL::new().max_inclusive_constraint_component
    }

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
