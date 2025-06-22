use crate::components::ToSubjectRef;
use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::named_nodes::SHACL;
use crate::types::{ComponentID, TraceItem};
use oxigraph::model::vocab::{rdf, xsd};
use oxigraph::model::{NamedNode, Term, TermRef};
use oxigraph::sparql::{Query, QueryOptions, QueryResults, Variable};
use oxsdatatypes::*;
use std::str::FromStr;

use super::{ComponentValidationResult, GraphvizOutput, ValidateComponent, ValidationFailure};

// value type
#[derive(Debug)]
pub struct ClassConstraintComponent {
    class: Term,
    query: Query,
}

impl ClassConstraintComponent {
    pub fn new(class: Term) -> Self {
        let class_term = class.to_subject_ref();
        let query_str = format!(
            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        ASK {{
            ?value_node rdf:type/rdfs:subClassOf* {} .
        }}",
            class_term.to_string()
        );
        match Query::parse(&query_str, None) {
            Ok(mut query) => {
                query.dataset_mut().set_default_graph_as_union();
                ClassConstraintComponent { class, query }
            }
            Err(e) => panic!("Failed to parse SPARQL query: {}", e),
        }
    }
}

impl GraphvizOutput for ClassConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#ClassConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let class_name = format_term_for_label(&self.class);
        format!(
            "{} [label=\"Class: {}\"];",
            component_id.to_graphviz_id(),
            class_name
        )
    }
}

impl ValidateComponent for ClassConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let cc_var = Variable::new("value_node").unwrap();
        if c.value_nodes().is_none() {
            return Ok(vec![]); // No value nodes to validate
        }

        let mut results = Vec::new();
        let vns = c.value_nodes().cloned().unwrap();

        for vn in vns.iter() {
            match context.store().query_opt_with_substituted_variables(
                self.query.clone(),
                QueryOptions::default(),
                [(cc_var.clone(), vn.clone())],
            ) {
                Ok(QueryResults::Boolean(result)) => {
                    if !result {
                        let mut error_context = c.clone();
                        error_context.with_value(vn.clone());
                        let message = format!(
                            "Value {:?} does not conform to class constraint: {}",
                            vn, self.class
                        );
                        let failure = ValidationFailure {
                            component_id,
                            failed_value_node: Some(vn.clone()),
                            message,
                            result_path: None,
                        };
                        results.push(ComponentValidationResult::Fail(error_context, failure));
                    }
                }
                Ok(_) => {
                    return Err("Expected a boolean result for class constraint query".to_string());
                }
                Err(e) => {
                    return Err(format!("Failed to execute class constraint query: {}", e));
                }
            }
        }

        Ok(results)
    }
}

#[derive(Debug)]
pub struct DatatypeConstraintComponent {
    datatype: Term,
}

impl DatatypeConstraintComponent {
    pub fn new(datatype: Term) -> Self {
        DatatypeConstraintComponent { datatype }
    }
}

impl ValidateComponent for DatatypeConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        _context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let target_datatype_iri = match self.datatype.as_ref() {
            TermRef::NamedNode(nn) => nn,
            _ => return Err("sh:datatype must be an IRI".to_string()),
        };

        let mut results = Vec::new();

        if let Some(value_nodes) = c.value_nodes().cloned() {
            for value_node in value_nodes {
                let mut fail = false;
                let mut message = String::new();

                if target_datatype_iri == rdf::LANG_STRING {
                    match value_node.as_ref() {
                        TermRef::Literal(lit) => {
                            if lit.language().is_none() {
                                fail = true;
                                message = format!(
                                    "Value {:?} is not a language-tagged string for datatype rdf:langString",
                                    value_node
                                );
                            }
                        }
                        _ => {
                            fail = true;
                            message = format!(
                                "Value {:?} is not a literal for datatype rdf:langString",
                                value_node
                            );
                        }
                    }
                } else {
                    match value_node.as_ref() {
                        TermRef::Literal(lit) => {
                            let lit_datatype = lit.datatype();
                            let mut datatype_matches = lit_datatype == target_datatype_iri;

                            // Exception for xsd:integer being valid for xsd:decimal
                            if !datatype_matches
                                && target_datatype_iri == xsd::DECIMAL
                                && lit_datatype == xsd::INTEGER
                            {
                                datatype_matches = true;
                            }

                            if datatype_matches {
                                let literal_value = lit.value();
                                let is_valid = if target_datatype_iri == xsd::STRING {
                                    true
                                } else if target_datatype_iri == xsd::BOOLEAN {
                                    Boolean::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::DECIMAL {
                                    Decimal::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::INTEGER {
                                    Integer::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::DOUBLE {
                                    Double::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::FLOAT {
                                    Float::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::DATE {
                                    Date::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::TIME {
                                    Time::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::DATE_TIME {
                                    DateTime::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::G_YEAR {
                                    GYear::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::G_MONTH {
                                    GMonth::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::G_DAY {
                                    GDay::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::G_YEAR_MONTH {
                                    GYearMonth::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::G_MONTH_DAY {
                                    GMonthDay::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::DURATION {
                                    Duration::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::YEAR_MONTH_DURATION {
                                    YearMonthDuration::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::DAY_TIME_DURATION {
                                    DayTimeDuration::from_str(literal_value).is_ok()
                                } else {
                                    // For unknown or unsupported datatypes, we assume the lexical form is valid
                                    // as we can't check it. This preserves the old behavior of only checking the datatype IRI.
                                    true
                                };

                                if !is_valid {
                                    fail = true;
                                    message = format!(
                                        "Value {:?} has an invalid lexical form for datatype {}",
                                        value_node, self.datatype
                                    );
                                }
                            } else {
                                fail = true;
                                message = format!(
                                    "Value {:?} does not have datatype {}",
                                    value_node, self.datatype
                                );
                            }
                        }
                        _ => {
                            // Not a literal, so it cannot conform to a datatype constraint
                            fail = true;
                            message = format!(
                                "Value {:?} is not a literal, expected datatype {}",
                                value_node, self.datatype
                            );
                        }
                    }
                }

                if fail {
                    let mut error_context = c.clone();
                    error_context.with_value(value_node.clone());
                    let failure = ValidationFailure {
                        component_id,
                        failed_value_node: Some(value_node.clone()),
                        message,
                        result_path: None,
                    };
                    results.push(ComponentValidationResult::Fail(error_context, failure));
                }
            }
        }

        Ok(results)
    }
}

impl GraphvizOutput for DatatypeConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#DatatypeConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let datatype_name = format_term_for_label(&self.datatype);
        format!(
            "{} [label=\"Datatype: {}\"];",
            component_id.to_graphviz_id(),
            datatype_name
        )
    }
}

#[derive(Debug)]
pub struct NodeKindConstraintComponent {
    node_kind: Term,
}

impl NodeKindConstraintComponent {
    pub fn new(node_kind: Term) -> Self {
        NodeKindConstraintComponent { node_kind }
    }
}

impl ValidateComponent for NodeKindConstraintComponent {
    fn validate(
        &self,
        component_id: ComponentID,
        c: &mut Context,
        _context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let sh = SHACL::new();
        let expected_node_kind_term = self.node_kind.as_ref();
        let mut results = Vec::new();

        if let Some(value_nodes) = c.value_nodes().cloned() {
            for value_node in value_nodes {
                let matches = match value_node.as_ref() {
                    TermRef::NamedNode(_) => {
                        expected_node_kind_term == sh.iri.into()
                            || expected_node_kind_term == sh.blank_node_or_iri.into()
                            || expected_node_kind_term == sh.iri_or_literal.into()
                    }
                    TermRef::BlankNode(_) => {
                        expected_node_kind_term == sh.blank_node.into()
                            || expected_node_kind_term == sh.blank_node_or_iri.into()
                            || expected_node_kind_term == sh.blank_node_or_literal.into()
                    }
                    TermRef::Literal(_) => {
                        expected_node_kind_term == sh.literal.into()
                            || expected_node_kind_term == sh.blank_node_or_literal.into()
                            || expected_node_kind_term == sh.iri_or_literal.into()
                    }
                    _ => false, // Triple, GraphName - should not occur as value nodes
                };

                if !matches {
                    let mut error_context = c.clone();
                    error_context.with_value(value_node.clone());
                    let message = format!(
                        "Value {:?} does not match nodeKind {}",
                        value_node, self.node_kind
                    );
                    let failure = ValidationFailure {
                        component_id,
                        failed_value_node: Some(value_node.clone()),
                        message,
                        result_path: None,
                    };
                    results.push(ComponentValidationResult::Fail(error_context, failure));
                }
            }
        }

        Ok(results)
    }
}

impl GraphvizOutput for NodeKindConstraintComponent {
    fn component_type(&self) -> NamedNode {
        NamedNode::new_unchecked("http://www.w3.org/ns/shacl#NodeKindConstraintComponent")
    }

    fn to_graphviz_string(
        &self,
        component_id: ComponentID,
        _context: &ValidationContext,
    ) -> String {
        let node_kind_name = format_term_for_label(&self.node_kind);
        format!(
            "{} [label=\"NodeKind: {}\"];",
            component_id.to_graphviz_id(),
            node_kind_name
        )
    }
}
