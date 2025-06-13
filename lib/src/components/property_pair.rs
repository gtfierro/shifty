use crate::context::{format_term_for_label, ValidationContext};
use crate::types::ComponentID;
use oxigraph::model::{NamedNode, Term};

use super::GraphvizOutput;

// property pair constraints
#[derive(Debug)]
pub struct EqualsConstraintComponent {
    property: Term, // Should be an IRI
}

impl EqualsConstraintComponent {
    pub fn new(property: Term) -> Self {
        EqualsConstraintComponent { property }
    }
}

impl GraphvizOutput for EqualsConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#EqualsConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let property_name = format_term_for_label(&self.property);
        format!(
            "{} [label=\"Equals: {}\"];",
            component_id.to_graphviz_id(),
            property_name
        )
    }
}

#[derive(Debug)]
pub struct DisjointConstraintComponent {
    property: Term, // Should be an IRI
}

impl DisjointConstraintComponent {
    pub fn new(property: Term) -> Self {
        DisjointConstraintComponent { property }
    }
}

impl GraphvizOutput for DisjointConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#DisjointConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let property_name = format_term_for_label(&self.property);
        format!(
            "{} [label=\"Disjoint: {}\"];",
            component_id.to_graphviz_id(),
            property_name
        )
    }
}

#[derive(Debug)]
pub struct LessThanConstraintComponent {
    property: Term, // Should be an IRI
}

impl LessThanConstraintComponent {
    pub fn new(property: Term) -> Self {
        LessThanConstraintComponent { property }
    }
}

impl GraphvizOutput for LessThanConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#LessThanConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let property_name = format_term_for_label(&self.property);
        format!(
            "{} [label=\"LessThan: {}\"];",
            component_id.to_graphviz_id(),
            property_name
        )
    }
}

#[derive(Debug)]
pub struct LessThanOrEqualsConstraintComponent {
    property: Term, // Should be an IRI
}

impl LessThanOrEqualsConstraintComponent {
    pub fn new(property: Term) -> Self {
        LessThanOrEqualsConstraintComponent { property }
    }
}

impl GraphvizOutput for LessThanOrEqualsConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#LessThanOrEqualsConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let property_name = format_term_for_label(&self.property);
        format!(
            "{} [label=\"LessThanOrEquals: {}\"];",
            component_id.to_graphviz_id(),
            property_name
        )
    }
}
