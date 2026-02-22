use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::named_nodes::SHACL;
use crate::runtime::ToSubjectRef;
use crate::types::{ComponentID, TraceItem};
use oxigraph::model::vocab::{rdf, xsd};
use oxigraph::model::{NamedNode, NamedNodeRef, Term, TermRef};
use oxigraph::sparql::{PreparedSparqlQuery, QueryResults, Variable};
use oxsdatatypes::*;
use std::str::FromStr;

use crate::runtime::{
    ComponentValidationResult, GraphvizOutput, ValidateComponent, ValidationFailure,
};
use std::cmp::Ordering;

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
        let mut prepared: Option<PreparedSparqlQuery> = None;

        for vn in vns.iter() {
            let is_valid =
                if let Some(result) = context.class_constraint_matches_fast(vn, &self.class)? {
                    result
                } else {
                    let prepared_query = if let Some(prepared) = prepared.as_ref() {
                        prepared
                    } else {
                        prepared = Some(context.prepare_query(&self.query).map_err(|e| {
                            format!("Failed to prepare class constraint query: {}", e)
                        })?);
                        prepared.as_ref().unwrap()
                    };
                    match context.execute_prepared(
                        &self.query,
                        prepared_query,
                        &[(cc_var.clone(), vn.clone())],
                        false,
                    ) {
                        Ok(QueryResults::Boolean(result)) => result,
                        Ok(_) => {
                            return Err(
                                "Expected a boolean result for class constraint query".to_string()
                            );
                        }
                        Err(e) => {
                            return Err(format!("Failed to execute class constraint query: {}", e));
                        }
                    }
                };

            if !is_valid {
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

fn trim_leading_zeros(digits: &str) -> &str {
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() {
        "0"
    } else {
        trimmed
    }
}

fn compare_unsigned_decimal(lhs: &str, rhs: &str) -> Ordering {
    let lhs = trim_leading_zeros(lhs);
    let rhs = trim_leading_zeros(rhs);
    lhs.len().cmp(&rhs.len()).then_with(|| lhs.cmp(rhs))
}

#[derive(Debug, Clone, Copy)]
struct ParsedInteger<'a> {
    negative: bool,
    digits: &'a str,
}

impl ParsedInteger<'_> {
    fn normalized_digits(&self) -> &str {
        trim_leading_zeros(self.digits)
    }

    fn is_zero(&self) -> bool {
        self.normalized_digits() == "0"
    }

    fn is_negative_non_zero(&self) -> bool {
        self.negative && !self.is_zero()
    }
}

fn parse_integer_lexical(input: &str) -> Option<ParsedInteger<'_>> {
    if input.is_empty() {
        return None;
    }

    let (negative, digits) = match input.as_bytes().first() {
        Some(b'+') => (false, &input[1..]),
        Some(b'-') => (true, &input[1..]),
        _ => (false, input),
    };

    if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }

    Some(ParsedInteger { negative, digits })
}

fn is_valid_integer_lexical(input: &str) -> bool {
    parse_integer_lexical(input).is_some()
}

fn is_integer_within_signed_bounds(input: &str, min_abs: &str, max: &str) -> bool {
    let Some(parsed) = parse_integer_lexical(input) else {
        return false;
    };

    if parsed.is_negative_non_zero() {
        compare_unsigned_decimal(parsed.normalized_digits(), min_abs) != Ordering::Greater
    } else {
        compare_unsigned_decimal(parsed.normalized_digits(), max) != Ordering::Greater
    }
}

fn is_integer_within_unsigned_bound(input: &str, max: &str) -> bool {
    let Some(parsed) = parse_integer_lexical(input) else {
        return false;
    };
    if parsed.is_negative_non_zero() {
        return false;
    }
    compare_unsigned_decimal(parsed.normalized_digits(), max) != Ordering::Greater
}

fn is_valid_decimal_lexical(input: &str) -> bool {
    if input.is_empty() {
        return false;
    }

    let mut chars = input.chars().peekable();
    if matches!(chars.peek(), Some('+') | Some('-')) {
        chars.next();
    }

    let mut digits_before = 0usize;
    while matches!(chars.peek(), Some(c) if c.is_ascii_digit()) {
        digits_before += 1;
        chars.next();
    }

    if matches!(chars.peek(), Some('.')) {
        chars.next();

        let mut digits_after = 0usize;
        while matches!(chars.peek(), Some(c) if c.is_ascii_digit()) {
            digits_after += 1;
            chars.next();
        }

        if chars.peek().is_some() {
            return false;
        }

        digits_before > 0 || digits_after > 0
    } else {
        chars.peek().is_none() && digits_before > 0
    }
}

fn is_valid_lexical_form_for_datatype(
    literal_value: &str,
    target_datatype_iri: NamedNodeRef<'_>,
) -> bool {
    if target_datatype_iri == xsd::STRING {
        true
    } else if target_datatype_iri == xsd::BOOLEAN {
        Boolean::from_str(literal_value).is_ok()
    } else if target_datatype_iri == xsd::DECIMAL {
        is_valid_decimal_lexical(literal_value)
    } else if target_datatype_iri == xsd::INTEGER {
        is_valid_integer_lexical(literal_value)
    } else if target_datatype_iri == xsd::LONG {
        is_integer_within_signed_bounds(literal_value, "9223372036854775808", "9223372036854775807")
    } else if target_datatype_iri == xsd::INT {
        is_integer_within_signed_bounds(literal_value, "2147483648", "2147483647")
    } else if target_datatype_iri == xsd::SHORT {
        is_integer_within_signed_bounds(literal_value, "32768", "32767")
    } else if target_datatype_iri == xsd::BYTE {
        is_integer_within_signed_bounds(literal_value, "128", "127")
    } else if target_datatype_iri == xsd::UNSIGNED_LONG {
        is_integer_within_unsigned_bound(literal_value, "18446744073709551615")
    } else if target_datatype_iri == xsd::UNSIGNED_INT {
        is_integer_within_unsigned_bound(literal_value, "4294967295")
    } else if target_datatype_iri == xsd::UNSIGNED_SHORT {
        is_integer_within_unsigned_bound(literal_value, "65535")
    } else if target_datatype_iri == xsd::UNSIGNED_BYTE {
        is_integer_within_unsigned_bound(literal_value, "255")
    } else if target_datatype_iri == xsd::NON_NEGATIVE_INTEGER {
        parse_integer_lexical(literal_value)
            .map(|parsed| !parsed.is_negative_non_zero())
            .unwrap_or(false)
    } else if target_datatype_iri == xsd::POSITIVE_INTEGER {
        parse_integer_lexical(literal_value)
            .map(|parsed| !parsed.is_negative_non_zero() && !parsed.is_zero())
            .unwrap_or(false)
    } else if target_datatype_iri == xsd::NON_POSITIVE_INTEGER {
        parse_integer_lexical(literal_value)
            .map(|parsed| parsed.is_negative_non_zero() || parsed.is_zero())
            .unwrap_or(false)
    } else if target_datatype_iri == xsd::NEGATIVE_INTEGER {
        parse_integer_lexical(literal_value)
            .map(|parsed| parsed.is_negative_non_zero())
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
                                let is_valid = is_valid_lexical_form_for_datatype(
                                    literal_value,
                                    target_datatype_iri,
                                );

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
    use crate::shacl_ir::FeatureToggles;
    use crate::sparql::SparqlServices;
    use crate::types::{ComponentID, PropShapeID};
    use ontoenv::api::OntoEnv;
    use ontoenv::config::Config;
    use oxigraph::model::{Literal, NamedNode, Term};
    use oxigraph::store::Store;
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
            shape_graph_id: None,
            node_shapes: HashMap::new(),
            prop_shapes: HashMap::new(),
            component_descriptors: HashMap::<ComponentID, ComponentDescriptor>::new(),
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            env: Arc::new(RwLock::new(env)),
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

    fn run_datatype_constraint(
        target_datatype: NamedNode,
        lexical_form: &str,
        value_datatype: NamedNode,
    ) -> Vec<ComponentValidationResult> {
        let focus = NamedNode::new("urn:focus").unwrap();
        let value_literal = Literal::new_typed_literal(lexical_form, value_datatype);

        let mut context = Context::new(
            Term::NamedNode(focus),
            None,
            Some(vec![Term::Literal(value_literal)]),
            SourceShape::PropertyShape(PropShapeID(0)),
            0,
        );

        let validation_context = build_empty_validation_context();
        let datatype_component = DatatypeConstraintComponent::new(Term::NamedNode(target_datatype));

        let mut trace = Vec::new();
        datatype_component
            .validate(
                ComponentID(0),
                &mut context,
                &validation_context,
                &mut trace,
            )
            .expect("validation should succeed")
    }

    #[test]
    fn datatype_constraint_accepts_high_precision_decimal_lexical_form() {
        let datatype = NamedNode::new(xsd::DECIMAL.as_str()).unwrap();
        let results = run_datatype_constraint(datatype.clone(), "1.35581800916358032544", datatype);
        assert!(
            results.is_empty(),
            "high-precision xsd:decimal lexical forms should be accepted"
        );
    }

    #[test]
    fn datatype_constraint_accepts_large_integer_lexical_form() {
        let datatype = NamedNode::new(xsd::INTEGER.as_str()).unwrap();
        let results = run_datatype_constraint(
            datatype.clone(),
            "1234567890123456789012345678901234567890",
            datatype,
        );
        assert!(
            results.is_empty(),
            "xsd:integer lexical forms are unbounded and should not be limited to i64"
        );
    }

    #[test]
    fn datatype_constraint_enforces_long_bounds_without_i64_parser_limits() {
        let datatype = NamedNode::new(xsd::LONG.as_str()).unwrap();

        let valid_max =
            run_datatype_constraint(datatype.clone(), "9223372036854775807", datatype.clone());
        assert!(valid_max.is_empty(), "xsd:long max must be accepted");

        let overflow =
            run_datatype_constraint(datatype.clone(), "9223372036854775808", datatype.clone());
        assert_eq!(overflow.len(), 1, "xsd:long overflow must fail");

        let valid_min =
            run_datatype_constraint(datatype.clone(), "-9223372036854775808", datatype.clone());
        assert!(valid_min.is_empty(), "xsd:long min must be accepted");

        let underflow = run_datatype_constraint(
            datatype,
            "-9223372036854775809",
            NamedNode::new(xsd::LONG.as_str()).unwrap(),
        );
        assert_eq!(underflow.len(), 1, "xsd:long underflow must fail");
    }

    #[test]
    fn datatype_constraint_handles_integer_derived_xsd_datatypes() {
        let unsigned_long = NamedNode::new(xsd::UNSIGNED_LONG.as_str()).unwrap();
        let max_unsigned = run_datatype_constraint(
            unsigned_long.clone(),
            "18446744073709551615",
            unsigned_long.clone(),
        );
        assert!(
            max_unsigned.is_empty(),
            "xsd:unsignedLong max must be accepted"
        );

        let overflow_unsigned = run_datatype_constraint(
            unsigned_long.clone(),
            "18446744073709551616",
            unsigned_long.clone(),
        );
        assert_eq!(
            overflow_unsigned.len(),
            1,
            "xsd:unsignedLong overflow must fail"
        );

        let negative_integer = NamedNode::new(xsd::NEGATIVE_INTEGER.as_str()).unwrap();
        let valid_negative =
            run_datatype_constraint(negative_integer.clone(), "-42", negative_integer.clone());
        assert!(
            valid_negative.is_empty(),
            "xsd:negativeInteger must accept negatives"
        );

        let invalid_negative =
            run_datatype_constraint(negative_integer.clone(), "-0", negative_integer.clone());
        assert_eq!(
            invalid_negative.len(),
            1,
            "xsd:negativeInteger must reject zero"
        );

        let positive_integer = NamedNode::new(xsd::POSITIVE_INTEGER.as_str()).unwrap();
        let valid_positive =
            run_datatype_constraint(positive_integer.clone(), "0001", positive_integer.clone());
        assert!(
            valid_positive.is_empty(),
            "xsd:positiveInteger must accept positive values"
        );

        let invalid_positive =
            run_datatype_constraint(positive_integer.clone(), "0", positive_integer);
        assert_eq!(
            invalid_positive.len(),
            1,
            "xsd:positiveInteger must reject zero"
        );
    }
}
