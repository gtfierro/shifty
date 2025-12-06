#![allow(deprecated)]
use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::runtime::{
    ComponentValidationResult, GraphvizOutput, ToSubjectRef, ValidateComponent, ValidationFailure,
};
use crate::types::{ComponentID, TraceItem};
use oxigraph::model::{NamedNode, Term};
use oxigraph::sparql::QueryResults;
use std::collections::HashSet;

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

impl ValidateComponent for EqualsConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let value_nodes: Vec<Term> = match c.value_nodes() {
            Some(nodes) => nodes.clone(),
            None => vec![],
        };
        let value_nodes_set: HashSet<Term> = value_nodes.into_iter().collect();

        let focus_node = c.focus_node();
        let equals_property = match &self.property {
            Term::NamedNode(nn) => nn,
            _ => {
                return Err(format!(
                    "sh:equals property must be an IRI, but got {:?}",
                    self.property
                ))
            }
        };

        let other_values_set: HashSet<Term> = context
            .quads_for_pattern(
                Some(focus_node.try_to_subject_ref()?),
                Some(equals_property.as_ref()),
                None,
                Some(context.data_graph_iri_ref()),
            )?
            .into_iter()
            .map(|q| q.object)
            .collect();

        let mut results = Vec::new();

        // For each value node that does not exist as a value of the property $equals at the focus node...
        for value_node in value_nodes_set.difference(&other_values_set) {
            let mut fail_context = c.clone();
            fail_context.with_value(value_node.clone());
            results.push(ComponentValidationResult::Fail(
                fail_context,
                ValidationFailure {
                    component_id,
                    failed_value_node: Some(value_node.clone()),
                    message: format!(
                        "Value node {} not found in values of property <{}>",
                        format_term_for_label(value_node),
                        equals_property.as_str()
                    ),
                    result_path: None,
                    source_constraint: None,

                    severity: None,

                    message_terms: Vec::new(),
                },
            ));
        }

        // For each value of the property $equals at the focus node that is not one of the value nodes...
        for other_value in other_values_set.difference(&value_nodes_set) {
            let mut fail_context = c.clone();
            fail_context.with_value(other_value.clone());
            results.push(ComponentValidationResult::Fail(
                fail_context,
                ValidationFailure {
                    component_id,
                    failed_value_node: Some(other_value.clone()),
                    message: format!(
                        "Value {} of property <{}> not found in value nodes",
                        format_term_for_label(other_value),
                        equals_property.as_str()
                    ),
                    result_path: None,
                    source_constraint: None,

                    severity: None,

                    message_terms: Vec::new(),
                },
            ));
        }

        Ok(results)
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

impl ValidateComponent for DisjointConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let value_nodes: Vec<Term> = match c.value_nodes() {
            Some(nodes) => nodes.clone(),
            None => vec![c.focus_node().clone()],
        };

        if value_nodes.is_empty() {
            return Ok(vec![]);
        }

        let focus_node = c.focus_node();
        let disjoint_property = match &self.property {
            Term::NamedNode(nn) => nn,
            _ => {
                return Err(format!(
                    "sh:disjoint property must be an IRI, but got {:?}",
                    self.property
                ))
            }
        };

        let other_values: HashSet<Term> = context
            .quads_for_pattern(
                Some(focus_node.try_to_subject_ref()?),
                Some(disjoint_property.as_ref()),
                None,
                Some(context.data_graph_iri_ref()),
            )?
            .into_iter()
            .map(|q| q.object)
            .collect();

        if other_values.is_empty() {
            return Ok(vec![]);
        }

        let mut results = Vec::new();
        for value_node in &value_nodes {
            if other_values.contains(value_node) {
                let mut fail_context = c.clone();
                fail_context.with_value(value_node.clone());
                results.push(ComponentValidationResult::Fail(
                    fail_context,
                    ValidationFailure {
                        component_id,
                        failed_value_node: Some(value_node.clone()),
                        message: format!(
                            "Value {} is not disjoint with values of property <{}>",
                            format_term_for_label(value_node),
                            disjoint_property.as_str()
                        ),
                        result_path: None,
                        source_constraint: None,

                        severity: None,

                        message_terms: Vec::new(),
                    },
                ));
            }
        }

        Ok(results)
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

impl ValidateComponent for LessThanConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let value_nodes: Vec<Term> = match c.value_nodes() {
            Some(nodes) => nodes.clone(),
            None => vec![c.focus_node().clone()],
        };

        if value_nodes.is_empty() {
            return Ok(vec![]);
        }

        let focus_node = c.focus_node();
        let less_than_property = match &self.property {
            Term::NamedNode(nn) => nn,
            _ => {
                return Err(format!(
                    "sh:lessThan property must be an IRI, but got {:?}",
                    self.property
                ))
            }
        };

        let other_values: Vec<Term> = context.objects_for_predicate(
            focus_node.try_to_subject_ref()?,
            less_than_property.as_ref(),
            context.data_graph_iri_ref(),
        )?;

        if other_values.is_empty() {
            return Ok(vec![]);
        }

        let mut results = Vec::new();

        for value_node in &value_nodes {
            for other_value in &other_values {
                let query_str = format!("ASK {{ FILTER({} < {}) }}", value_node, other_value);
                let prepared = context.prepare_query(&query_str)?;
                let is_less_than = match context.execute_prepared(&query_str, &prepared, &[], false)
                {
                    Ok(QueryResults::Boolean(b)) => b,
                    Ok(_) => false,  // Should not happen for ASK
                    Err(_) => false, // Incomparable values
                };

                if !is_less_than {
                    let mut fail_context = c.clone();
                    fail_context.with_value(value_node.clone());
                    results.push(ComponentValidationResult::Fail(
                        fail_context,
                        ValidationFailure {
                            component_id,
                            failed_value_node: Some(value_node.clone()),
                            message: format!(
                                "Value {} is not less than {} from property <{}>",
                                format_term_for_label(value_node),
                                format_term_for_label(other_value),
                                less_than_property.as_str()
                            ),
                            result_path: None,
                            source_constraint: None,

                            severity: None,

                            message_terms: Vec::new(),
                        },
                    ));
                }
            }
        }

        Ok(results)
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

impl ValidateComponent for LessThanOrEqualsConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let value_nodes: Vec<Term> = match c.value_nodes() {
            Some(nodes) => nodes.clone(),
            None => vec![c.focus_node().clone()],
        };

        if value_nodes.is_empty() {
            return Ok(vec![]);
        }

        let focus_node = c.focus_node();
        let lte_property = match &self.property {
            Term::NamedNode(nn) => nn,
            _ => {
                return Err(format!(
                    "sh:lessThanOrEquals property must be an IRI, but got {:?}",
                    self.property
                ))
            }
        };

        let other_values: Vec<Term> = context.objects_for_predicate(
            focus_node.try_to_subject_ref()?,
            lte_property.as_ref(),
            context.data_graph_iri_ref(),
        )?;

        if other_values.is_empty() {
            return Ok(vec![]);
        }

        let mut results = Vec::new();

        for value_node in &value_nodes {
            for other_value in &other_values {
                let query_str = format!("ASK {{ FILTER({} <= {}) }}", value_node, other_value);
                let prepared = context.prepare_query(&query_str)?;
                let is_less_than_or_equal =
                    match context.execute_prepared(&query_str, &prepared, &[], false) {
                        Ok(QueryResults::Boolean(b)) => b,
                        Ok(_) => false,  // Should not happen for ASK
                        Err(_) => false, // Incomparable values
                    };

                if !is_less_than_or_equal {
                    let mut fail_context = c.clone();
                    fail_context.with_value(value_node.clone());
                    results.push(ComponentValidationResult::Fail(
                        fail_context,
                        ValidationFailure {
                            component_id,
                            failed_value_node: Some(value_node.clone()),
                            message: format!(
                                "Value {} is not less than or equal to {} from property <{}>",
                                format_term_for_label(value_node),
                                format_term_for_label(other_value),
                                lte_property.as_str()
                            ),
                            result_path: None,
                            source_constraint: None,

                            severity: None,

                            message_terms: Vec::new(),
                        },
                    ));
                }
            }
        }

        Ok(results)
    }
}
