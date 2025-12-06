use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::runtime::Component;
use crate::types::Path;
use crate::types::{ComponentID, TraceItem};
use log::debug;
use oxigraph::model::{NamedNode, Term};
use std::collections::HashSet;
use std::vec::Vec;

use crate::runtime::{
    ComponentValidationResult, GraphvizOutput, ToSubjectRef, ValidateComponent, ValidationFailure,
};

impl ValidateComponent for InConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        _validation_context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        if self.values.is_empty() {
            // According to SHACL spec, if sh:in has an empty list, no value nodes can conform.
            // "The constraint sh:in specifies the condition that each value node is a member of a provided SHACL list."
            // "If the SHACL list is empty, then no value nodes can satisfy the constraint."
            return if c.value_nodes().is_none_or(|vns| vns.is_empty()) {
                // If there are no value nodes, or the list of value nodes is empty, it passes.
                Ok(vec![])
            } else {
                let failure = ValidationFailure {
                    component_id,
                    failed_value_node: None,
                    message: format!(
                        "sh:in constraint has an empty list, but value nodes {:?} exist.",
                        c.value_nodes().unwrap_or(&Vec::new()) // Provide empty vec for formatting if None
                    ),
                    result_path: None,
                    source_constraint: None,

                    severity: None,

                    message_terms: Vec::new(),
                };
                Ok(vec![ComponentValidationResult::Fail(c.clone(), failure)])
            };
        }

        let mut results = Vec::new();
        if let Some(value_nodes) = c.value_nodes().cloned() {
            for vn in value_nodes {
                if !self.values.contains(&vn) {
                    let mut error_context = c.clone();
                    error_context.with_value(vn.clone());
                    let message = format!(
                        "Value {:?} is not in the allowed list {:?}.",
                        vn, self.values
                    );
                    let failure = ValidationFailure {
                        component_id,
                        failed_value_node: Some(vn.clone()),
                        message,
                        result_path: None,
                        source_constraint: None,

                        severity: None,

                        message_terms: Vec::new(),
                    };
                    results.push(ComponentValidationResult::Fail(error_context, failure));
                }
            }
        }

        Ok(results)
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

impl ValidateComponent for ClosedConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        validation_context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        if !self.closed {
            return Ok(vec![]);
        }

        let mut allowed_properties = HashSet::new();

        if let Some(ignored) = &self.ignored_properties {
            for term in ignored {
                if let Term::NamedNode(nn) = term {
                    allowed_properties.insert(nn.clone());
                }
            }
        }

        let source_shape_binding = c.source_shape();
        let source_shape_id = if let Some(id) = source_shape_binding.as_node_id() {
            id
        } else {
            return Err("sh:closed can only be used on a node shape".to_string());
        };

        if let Some(node_shape) = validation_context.model.node_shapes.get(source_shape_id) {
            for constraint_com_id in node_shape.constraints() {
                if let Some(Component::PropertyConstraint(pc)) =
                    validation_context.get_component(constraint_com_id)
                {
                    if let Some(prop_shape) =
                        validation_context.model.get_prop_shape_by_id(pc.shape())
                    {
                        if let Path::Simple(Term::NamedNode(p)) = prop_shape.path() {
                            allowed_properties.insert(p.clone());
                        }
                    }
                }
            }
        }

        debug!(
            "ClosedConstraintComponent: allowed_properties: {:?}",
            allowed_properties
        );
        let mut results = Vec::new();
        let value_nodes = c
            .value_nodes()
            .map_or_else(|| vec![c.focus_node().clone()], |vns| vns.to_vec());

        for vn in value_nodes {
            let subject_ref = match vn.try_to_subject_ref() {
                Ok(s) => s,
                Err(_) => continue, // Literals cannot be subjects of triples.
            };

            let data_graph_ref = oxigraph::model::GraphNameRef::NamedNode(
                validation_context.data_graph_iri.as_ref(),
            );

            for quad in validation_context.quads_for_pattern(
                Some(subject_ref),
                None,
                None,
                Some(data_graph_ref),
            )? {
                let predicate = quad.predicate;

                if !allowed_properties.contains(&predicate) {
                    let mut error_context = c.clone();

                    let object = quad.object.to_owned();
                    error_context.with_value(object.clone());
                    error_context
                        .with_result_path(oxigraph::model::Term::NamedNode(predicate.clone()));

                    let message = format!(
                        "Focus node {:?} has value for property {:?} which is not allowed by sh:closed",
                        vn, predicate
                    );

                    let failure = ValidationFailure {
                        component_id,
                        failed_value_node: Some(object),
                        message,
                        result_path: Some(Path::Simple(Term::NamedNode(predicate))),
                        source_constraint: None,

                        severity: None,

                        message_terms: Vec::new(),
                    };
                    results.push(ComponentValidationResult::Fail(error_context, failure));
                }
            }
        }

        Ok(results)
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
        c: &mut Context,
        _validation_context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        match c.value_nodes() {
            Some(value_nodes) => {
                if value_nodes.iter().any(|vn| vn == &self.value) {
                    // At least one value node is equal to self.value
                    Ok(vec![])
                } else {
                    // No value node is equal to self.value
                    let failure = ValidationFailure {
                        component_id,
                        failed_value_node: None,
                        message: format!(
                            "None of the value nodes {:?} are equal to the required value {:?}",
                            value_nodes, self.value
                        ),
                        result_path: None,
                        source_constraint: None,

                        severity: None,

                        message_terms: Vec::new(),
                    };
                    Ok(vec![ComponentValidationResult::Fail(c.clone(), failure)])
                }
            }
            None => {
                // No value nodes present, so self.value cannot be among them.
                let failure = ValidationFailure {
                    component_id,
                    failed_value_node: None,
                    message: format!(
                        "No value nodes found to check against required value {:?}",
                        self.value
                    ),
                    result_path: None,
                    source_constraint: None,

                    severity: None,

                    message_terms: Vec::new(),
                };
                Ok(vec![ComponentValidationResult::Fail(c.clone(), failure)])
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
