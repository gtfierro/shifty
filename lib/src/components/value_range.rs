use super::{ComponentValidationResult, GraphvizOutput, ValidateComponent, ValidationFailure};
use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::types::ComponentID;
use oxigraph::model::{NamedNode, Term};
use oxigraph::sparql::QueryResults;

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
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#MinExclusiveConstraintComponent")
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

impl ValidateComponent for MinExclusiveConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let value_nodes: Vec<Term> = match c.value_nodes() {
            Some(nodes) => nodes.clone(),
            None => vec![c.focus_node().clone()],
        };

        if value_nodes.is_empty() {
            return Ok(vec![]);
        }

        let mut results = Vec::new();

        for value_node in &value_nodes {
            // For each value node v where the SPARQL expression $minExclusive < v does not return true, there is a validation result.
            let query_str = format!("ASK {{ FILTER({} < {}) }}", self.min_exclusive, value_node);

            let is_valid = match context.store().query(&query_str) {
                Ok(QueryResults::Boolean(b)) => b,
                Ok(_) => false, // Should not happen for ASK
                Err(_) => false, // Incomparable values
            };

            if !is_valid {
                let mut fail_context = c.clone();
                fail_context.with_value(value_node.clone());
                results.push(ComponentValidationResult::Fail(
                    fail_context,
                    ValidationFailure {
                        component_id,
                        failed_value_node: Some(value_node.clone()),
                        message: format!(
                            "Value {} is not exclusively greater than {}",
                            format_term_for_label(value_node),
                            format_term_for_label(&self.min_exclusive),
                        ),
                    },
                ));
            }
        }

        Ok(results)
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
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#MinInclusiveConstraintComponent")
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

impl ValidateComponent for MinInclusiveConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let value_nodes: Vec<Term> = match c.value_nodes() {
            Some(nodes) => nodes.clone(),
            None => vec![c.focus_node().clone()],
        };

        if value_nodes.is_empty() {
            return Ok(vec![]);
        }

        let mut results = Vec::new();

        for value_node in &value_nodes {
            // For each value node v where the SPARQL expression $minInclusive <= v does not return true, there is a validation result.
            let query_str = format!("ASK {{ FILTER({} <= {}) }}", self.min_inclusive, value_node);

            let is_valid = match context.store().query(&query_str) {
                Ok(QueryResults::Boolean(b)) => b,
                Ok(_) => false, // Should not happen for ASK
                Err(_) => false, // Incomparable values
            };

            if !is_valid {
                let mut fail_context = c.clone();
                fail_context.with_value(value_node.clone());
                results.push(ComponentValidationResult::Fail(
                    fail_context,
                    ValidationFailure {
                        component_id,
                        failed_value_node: Some(value_node.clone()),
                        message: format!(
                            "Value {} is not inclusively greater than or equal to {}",
                            format_term_for_label(value_node),
                            format_term_for_label(&self.min_inclusive),
                        ),
                    },
                ));
            }
        }

        Ok(results)
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
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#MaxExclusiveConstraintComponent")
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

impl ValidateComponent for MaxExclusiveConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let value_nodes: Vec<Term> = match c.value_nodes() {
            Some(nodes) => nodes.clone(),
            None => vec![c.focus_node().clone()],
        };

        if value_nodes.is_empty() {
            return Ok(vec![]);
        }

        let mut results = Vec::new();

        for value_node in &value_nodes {
            // For each value node v where the SPARQL expression $maxExclusive > v does not return true, there is a validation result.
            let query_str = format!("ASK {{ FILTER({} > {}) }}", self.max_exclusive, value_node);

            let is_valid = match context.store().query(&query_str) {
                Ok(QueryResults::Boolean(b)) => b,
                Ok(_) => false, // Should not happen for ASK
                Err(_) => false, // Incomparable values
            };

            if !is_valid {
                let mut fail_context = c.clone();
                fail_context.with_value(value_node.clone());
                results.push(ComponentValidationResult::Fail(
                    fail_context,
                    ValidationFailure {
                        component_id,
                        failed_value_node: Some(value_node.clone()),
                        message: format!(
                            "Value {} is not exclusively less than {}",
                            format_term_for_label(value_node),
                            format_term_for_label(&self.max_exclusive),
                        ),
                    },
                ));
            }
        }

        Ok(results)
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
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#MaxInclusiveConstraintComponent")
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

impl ValidateComponent for MaxInclusiveConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let value_nodes: Vec<Term> = match c.value_nodes() {
            Some(nodes) => nodes.clone(),
            None => vec![c.focus_node().clone()],
        };

        if value_nodes.is_empty() {
            return Ok(vec![]);
        }

        let mut results = Vec::new();

        for value_node in &value_nodes {
            // For each value node v where the SPARQL expression $maxInclusive >= v does not return true, there is a validation result.
            let query_str = format!("ASK {{ FILTER({} >= {}) }}", self.max_inclusive, value_node);

            let is_valid = match context.store().query(&query_str) {
                Ok(QueryResults::Boolean(b)) => b,
                Ok(_) => false, // Should not happen for ASK
                Err(_) => false, // Incomparable values
            };

            if !is_valid {
                let mut fail_context = c.clone();
                fail_context.with_value(value_node.clone());
                results.push(ComponentValidationResult::Fail(
                    fail_context,
                    ValidationFailure {
                        component_id,
                        failed_value_node: Some(value_node.clone()),
                        message: format!(
                            "Value {} is not inclusively less than or equal to {}",
                            format_term_for_label(value_node),
                            format_term_for_label(&self.max_inclusive),
                        ),
                    },
                ));
            }
        }

        Ok(results)
    }
}
