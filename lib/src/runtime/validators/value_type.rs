use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::named_nodes::SHACL;
use crate::runtime::ToSubjectRef;
use crate::types::{ComponentID, TraceItem};
use oxigraph::model::vocab::{rdf, xsd};
use oxigraph::model::{NamedNode, Term, TermRef};
use oxigraph::sparql::{QueryResults, Variable};
use oxsdatatypes::*;
use std::str::FromStr;

use crate::runtime::{
    ComponentValidationResult, GraphvizOutput, ValidateComponent, ValidationFailure,
};

// value type
#[derive(Debug)]
pub struct ClassConstraintComponent {
    class: Term,
    query: String,
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
            class_term
        );
        ClassConstraintComponent {
            class,
            query: query_str,
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
        let prepared = context
            .prepare_query(&self.query)
            .map_err(|e| format!("Failed to prepare class constraint query: {}", e))?;

        for vn in vns.iter() {
            match context.execute_prepared(
                &self.query,
                &prepared,
                &[(cc_var.clone(), vn.clone())],
                false,
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
                            source_constraint: None,

                            severity: None,

                            message_terms: Vec::new(),
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
                                } else if target_datatype_iri == xsd::BYTE {
                                    Integer::from_str(literal_value)
                                        .map(|v| {
                                            let value: i64 = v.into();
                                            value >= i64::from(i8::MIN)
                                                && value <= i64::from(i8::MAX)
                                        })
                                        .unwrap_or(false)
                                } else if target_datatype_iri == xsd::SHORT {
                                    Integer::from_str(literal_value)
                                        .map(|v| {
                                            let value: i64 = v.into();
                                            value >= i64::from(i16::MIN)
                                                && value <= i64::from(i16::MAX)
                                        })
                                        .unwrap_or(false)
                                } else if target_datatype_iri == xsd::INT {
                                    Integer::from_str(literal_value)
                                        .map(|v| {
                                            let value: i64 = v.into();
                                            value >= i64::from(i32::MIN)
                                                && value <= i64::from(i32::MAX)
                                        })
                                        .unwrap_or(false)
                                } else if target_datatype_iri == xsd::LONG {
                                    Integer::from_str(literal_value).is_ok()
                                } else if target_datatype_iri == xsd::UNSIGNED_BYTE {
                                    Integer::from_str(literal_value)
                                        .map(|v| {
                                            let value: i64 = v.into();
                                            value >= 0 && value <= i64::from(u8::MAX)
                                        })
                                        .unwrap_or(false)
                                } else if target_datatype_iri == xsd::UNSIGNED_SHORT {
                                    Integer::from_str(literal_value)
                                        .map(|v| {
                                            let value: i64 = v.into();
                                            value >= 0 && value <= i64::from(u16::MAX)
                                        })
                                        .unwrap_or(false)
                                } else if target_datatype_iri == xsd::UNSIGNED_INT {
                                    Integer::from_str(literal_value)
                                        .map(|v| {
                                            let value: i64 = v.into();
                                            value >= 0 && value <= i64::from(u32::MAX)
                                        })
                                        .unwrap_or(false)
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
        context: &ValidationContext,
        _trace: &mut Vec<TraceItem>,
    ) -> Result<Vec<ComponentValidationResult>, String> {
        let sh = SHACL::new();
        let expected_node_kind_term = self.node_kind.as_ref();
        let mut results = Vec::new();

        if let Some(value_nodes) = c.value_nodes().cloned() {
            for value_node in value_nodes {
                enum ValueCategory {
                    Named,
                    Blank,
                    Literal,
                    #[allow(dead_code)]
                    Unsupported,
                }

                let category = match value_node.as_ref() {
                    TermRef::NamedNode(nn) => {
                        // Skolem IRIs stand in for blank nodes during validation; treat them accordingly.
                        if context.is_data_skolem_iri(nn) || context.is_shape_skolem_iri(nn) {
                            ValueCategory::Blank
                        } else {
                            ValueCategory::Named
                        }
                    }
                    TermRef::BlankNode(_) => ValueCategory::Blank,
                    TermRef::Literal(_) => ValueCategory::Literal,
                };

                let matches = match category {
                    ValueCategory::Named => {
                        expected_node_kind_term == sh.iri.into()
                            || expected_node_kind_term == sh.blank_node_or_iri.into()
                            || expected_node_kind_term == sh.iri_or_literal.into()
                    }
                    ValueCategory::Blank => {
                        expected_node_kind_term == sh.blank_node.into()
                            || expected_node_kind_term == sh.blank_node_or_iri.into()
                            || expected_node_kind_term == sh.blank_node_or_literal.into()
                    }
                    ValueCategory::Literal => {
                        expected_node_kind_term == sh.literal.into()
                            || expected_node_kind_term == sh.blank_node_or_literal.into()
                            || expected_node_kind_term == sh.iri_or_literal.into()
                    }
                    ValueCategory::Unsupported => false,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{Context, IDLookupTable, ShapesModel, SourceShape, ValidationContext};
    use crate::ir;
    use crate::model::components::ComponentDescriptor;
    use crate::sparql::SparqlServices;
    use crate::types::{ComponentID, PropShapeID};
    use ontoenv::api::OntoEnv;
    use ontoenv::config::Config;
    use oxigraph::model::{Literal, NamedNode, Term};
    use oxigraph::store::Store;
    use shacl_ir::FeatureToggles;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    fn build_empty_validation_context() -> ValidationContext {
        let store = Store::new().expect("failed to create in-memory store");
        let shape_graph_iri = NamedNode::new("urn:shape").expect("invalid shape IRI");
        let data_graph_iri = NamedNode::new("urn:data").expect("invalid data IRI");

        let root = std::env::temp_dir();
        let config = Config::builder()
            .root(root)
            .locations(Vec::new())
            .offline(true)
            .temporary(true)
            .build()
            .expect("failed to build OntoEnv config");
        let env = OntoEnv::init(config, false).expect("failed to initialise OntoEnv");

        let model = ShapesModel {
            nodeshape_id_lookup: RwLock::new(IDLookupTable::new()),
            propshape_id_lookup: RwLock::new(IDLookupTable::new()),
            component_id_lookup: RwLock::new(IDLookupTable::new()),
            rule_id_lookup: RwLock::new(IDLookupTable::new()),
            store,
            shape_graph_iri: shape_graph_iri.clone(),
            node_shapes: HashMap::new(),
            prop_shapes: HashMap::new(),
            component_descriptors: HashMap::<ComponentID, ComponentDescriptor>::new(),
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            env,
            sparql: Arc::new(SparqlServices::new()),
            features: FeatureToggles::default(),
            original_values: None,
        };

        let shape_ir = ir::build_shape_ir(&model, None, std::slice::from_ref(&shape_graph_iri))
            .expect("failed to build SHACL-IR for empty context");
        ValidationContext::new(
            Arc::new(model),
            data_graph_iri,
            false,
            true,
            Arc::new(shape_ir),
        )
    }

    #[test]
    fn node_kind_rejects_skolemised_blank_nodes_for_iri_or_literal() {
        let focus = NamedNode::new("urn:focus").unwrap();
        let skolem_value = NamedNode::new("urn:data/.sk/b1").unwrap();

        let mut context = Context::new(
            Term::NamedNode(focus.clone()),
            None,
            Some(vec![Term::NamedNode(skolem_value.clone())]),
            SourceShape::PropertyShape(PropShapeID(0)),
            0,
        );

        let validation_context = build_empty_validation_context();

        let iri_or_literal = Term::NamedNode(SHACL::new().iri_or_literal.into_owned());
        let component = NodeKindConstraintComponent::new(iri_or_literal);

        let mut trace = Vec::new();
        let results = component
            .validate(
                ComponentID(0),
                &mut context,
                &validation_context,
                &mut trace,
            )
            .expect("validation should succeed");

        assert_eq!(results.len(), 1, "expected a single violation");
        match &results[0] {
            ComponentValidationResult::Fail(_, failure) => {
                let value = failure
                    .failed_value_node
                    .as_ref()
                    .expect("missing failed value");
                assert_eq!(value, &Term::NamedNode(skolem_value));
            }
            other => panic!("expected failure result, got {:?}", other),
        }
    }

    #[test]
    fn datatype_constraint_rejects_invalid_byte_lexical_form() {
        let focus = NamedNode::new("urn:focus").unwrap();
        let byte_datatype = NamedNode::new(xsd::BYTE.as_str()).unwrap();
        let ill_formed = Literal::new_typed_literal("c", byte_datatype.clone());

        let mut context = Context::new(
            Term::NamedNode(focus.clone()),
            None,
            Some(vec![Term::Literal(ill_formed.clone())]),
            SourceShape::PropertyShape(PropShapeID(0)),
            0,
        );

        let validation_context = build_empty_validation_context();
        let datatype_component = DatatypeConstraintComponent::new(Term::NamedNode(byte_datatype));

        let mut trace = Vec::new();
        let results = datatype_component
            .validate(
                ComponentID(0),
                &mut context,
                &validation_context,
                &mut trace,
            )
            .expect("validation should succeed");

        assert_eq!(results.len(), 1, "c^^xsd:byte should trigger a violation");
        let failure = match &results[0] {
            ComponentValidationResult::Fail(_, failure) => failure,
            other => panic!("expected failure result, got {:?}", other),
        };
        assert_eq!(
            failure.failed_value_node.as_ref(),
            Some(&Term::Literal(ill_formed))
        );
    }
}
