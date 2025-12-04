#![allow(deprecated)]
use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::runtime::{
    ComponentValidationResult, GraphvizOutput, ValidateComponent, ValidationFailure,
};
use crate::types::{ComponentID, TraceItem};
use oxigraph::model::{Literal, NamedNode, Subject, Term};
use oxigraph::sparql::QueryResults;

fn escape_sparql_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
}

#[allow(dead_code)]
fn subject_to_sparql(subject: &Subject) -> String {
    match subject {
        Subject::NamedNode(nn) => format!("<{}>", nn.as_str()),
        Subject::BlankNode(bn) => format!("_:{}", bn.as_str()),
    }
}

fn term_to_sparql(term: &Term) -> String {
    match term {
        Term::NamedNode(nn) => format!("<{}>", nn.as_str()),
        Term::BlankNode(bn) => format!("_:{}", bn.as_str()),
        Term::Literal(lit) => {
            if let Some(lang) = lit.language() {
                format!("\"{}\"@{}", escape_sparql_string(lit.value()), lang)
            } else {
                format!(
                    "\"{}\"^^<{}>",
                    escape_sparql_string(lit.value()),
                    lit.datatype().as_str()
                )
            }
        }
    }
}

fn preserve_numeric_lexical(term: &Term) -> Term {
    if let Term::Literal(lit) = term {
        let dt = lit.datatype().as_str();
        let is_decimal = dt == "http://www.w3.org/2001/XMLSchema#decimal";
        let is_double = dt == "http://www.w3.org/2001/XMLSchema#double";
        let is_float = dt == "http://www.w3.org/2001/XMLSchema#float";
        if is_decimal || is_double || is_float {
            let lex = lit.value();
            if !lex.contains('.') && !lex.contains('e') && !lex.contains('E') {
                return Term::Literal(Literal::new_typed_literal(
                    format!("{}.0", lex),
                    NamedNode::new_unchecked(dt),
                ));
            }
        }
    }
    term.clone()
}

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
        _trace: &mut Vec<TraceItem>,
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
            let query_str = format!(
                "ASK {{ FILTER({} < {}) }}",
                term_to_sparql(&self.min_exclusive),
                term_to_sparql(value_node)
            );

            let prepared = context.prepare_query(&query_str)?;
            let is_valid = match context.execute_prepared(&query_str, &prepared, &[], false) {
                Ok(QueryResults::Boolean(b)) => b,
                Ok(_) => false, // Should not happen for ASK
                Err(_) => true, // Incomparable values are ignored (treated as valid)
            };

            if !is_valid {
                let reported_term = preserve_numeric_lexical(value_node);
                let mut fail_context = c.clone();
                if fail_context.focus_node() == value_node {
                    fail_context.set_focus_node(reported_term.clone());
                }
                if let Some(nodes) = fail_context.value_nodes_mut() {
                    for node in nodes.iter_mut() {
                        if *node == *value_node {
                            *node = reported_term.clone();
                        }
                    }
                }
                fail_context.with_value(reported_term.clone());
                results.push(ComponentValidationResult::Fail(
                    fail_context,
                    ValidationFailure {
                        component_id,
                        failed_value_node: Some(reported_term),
                        message: format!(
                            "Value {} is not exclusively greater than {}",
                            format_term_for_label(value_node),
                            format_term_for_label(&self.min_exclusive),
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
        _trace: &mut Vec<TraceItem>,
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
            let query_str = format!(
                "ASK {{ FILTER({} <= {}) }}",
                term_to_sparql(&self.min_inclusive),
                term_to_sparql(value_node)
            );

            let prepared = context.prepare_query(&query_str)?;
            let is_valid = match context.execute_prepared(&query_str, &prepared, &[], false) {
                Ok(QueryResults::Boolean(b)) => b,
                Ok(_) => false, // Should not happen for ASK
                Err(_) => true, // Incomparable values are ignored (treated as valid)
            };

            if !is_valid {
                let reported_term = preserve_numeric_lexical(value_node);
                let mut fail_context = c.clone();
                if fail_context.focus_node() == value_node {
                    fail_context.set_focus_node(reported_term.clone());
                }
                if let Some(nodes) = fail_context.value_nodes_mut() {
                    for node in nodes.iter_mut() {
                        if *node == *value_node {
                            *node = reported_term.clone();
                        }
                    }
                }
                fail_context.with_value(reported_term.clone());
                results.push(ComponentValidationResult::Fail(
                    fail_context,
                    ValidationFailure {
                        component_id,
                        failed_value_node: Some(reported_term),
                        message: format!(
                            "Value {} is not inclusively greater than or equal to {}",
                            format_term_for_label(value_node),
                            format_term_for_label(&self.min_inclusive),
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
        _trace: &mut Vec<TraceItem>,
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
            let query_str = format!(
                "ASK {{ FILTER({} > {}) }}",
                term_to_sparql(&self.max_exclusive),
                term_to_sparql(value_node)
            );

            let prepared = context.prepare_query(&query_str)?;
            let is_valid = match context.execute_prepared(&query_str, &prepared, &[], false) {
                Ok(QueryResults::Boolean(b)) => b,
                Ok(_) => false, // Should not happen for ASK
                Err(_) => true, // Incomparable values are ignored (treated as valid)
            };

            if !is_valid {
                let reported_term = preserve_numeric_lexical(value_node);

                let mut fail_context = c.clone();
                if fail_context.focus_node() == value_node {
                    fail_context.set_focus_node(reported_term.clone());
                }
                if let Some(nodes) = fail_context.value_nodes_mut() {
                    for node in nodes.iter_mut() {
                        if *node == *value_node {
                            *node = reported_term.clone();
                        }
                    }
                }
                fail_context.with_value(reported_term.clone());
                results.push(ComponentValidationResult::Fail(
                    fail_context,
                    ValidationFailure {
                        component_id,
                        failed_value_node: Some(reported_term),
                        message: format!(
                            "Value {} is not exclusively less than {}",
                            format_term_for_label(value_node),
                            format_term_for_label(&self.max_exclusive),
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
        _trace: &mut Vec<TraceItem>,
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
            let query_str = format!(
                "ASK {{ FILTER({} >= {}) }}",
                term_to_sparql(&self.max_inclusive),
                term_to_sparql(value_node)
            );

            let prepared = context.prepare_query(&query_str)?;
            let is_valid = match context.execute_prepared(&query_str, &prepared, &[], false) {
                Ok(QueryResults::Boolean(b)) => b,
                Ok(_) => false, // Should not happen for ASK
                Err(_) => true, // Incomparable values are ignored (treated as valid)
            };

            if !is_valid {
                let reported_term = preserve_numeric_lexical(value_node);
                let mut fail_context = c.clone();
                if fail_context.focus_node() == value_node {
                    fail_context.set_focus_node(reported_term.clone());
                }
                if let Some(nodes) = fail_context.value_nodes_mut() {
                    for node in nodes.iter_mut() {
                        if *node == *value_node {
                            *node = reported_term.clone();
                        }
                    }
                }
                fail_context.with_value(reported_term.clone());
                results.push(ComponentValidationResult::Fail(
                    fail_context,
                    ValidationFailure {
                        component_id,
                        failed_value_node: Some(reported_term),
                        message: format!(
                            "Value {} is not inclusively less than or equal to {}",
                            format_term_for_label(value_node),
                            format_term_for_label(&self.max_inclusive),
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
