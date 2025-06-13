use crate::components::ToSubjectRef;
use crate::context::{format_term_for_label, Context, ValidationContext};
use crate::named_nodes::SHACL;
use crate::types::ComponentID;
use oxigraph::model::{NamedNode, Term, TermRef};
use oxigraph::sparql::{Query, QueryOptions, QueryResults, Variable};

use super::{ComponentValidationResult, GraphvizOutput, ValidateComponent};

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
        SHACL::new().class_constraint_component
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
        c: &mut Context, // Changed to &mut Context
        context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let cc_var = Variable::new("value_node").unwrap();
        if c.value_nodes().is_none() {
            return Ok(ComponentValidationResult::Pass(component_id)); // No value nodes to validate
        }
        let vns = c.value_nodes().unwrap();
        for vn in vns.iter() {
            match context.store().query_opt_with_substituted_variables(
                self.query.clone(),
                QueryOptions::default(),
                [(cc_var.clone(), vn.clone())],
            ) {
                Ok(QueryResults::Boolean(result)) => {
                    if !result {
                        return Err(format!(
                            "Value {:?} does not conform to class constraint: {}",
                            vn, self.class
                        ));
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
        Ok(ComponentValidationResult::Pass(component_id))
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
        c: &mut Context, // Changed to &mut Context
        _context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let target_datatype_iri = match self.datatype.as_ref() {
            TermRef::NamedNode(nn) => nn,
            _ => return Err("sh:datatype must be an IRI".to_string()),
        };

        if let Some(value_nodes) = c.value_nodes() {
            for value_node in value_nodes {
                match value_node.as_ref() {
                    TermRef::Literal(lit) => {
                        if lit.datatype() != target_datatype_iri {
                            // TODO: Consider ill-typed literals if required by spec for specific datatypes
                            return Err(format!(
                                "Value {:?} does not have datatype {}",
                                value_node, self.datatype
                            ));
                        }
                    }
                    _ => {
                        // Not a literal, so it cannot conform to a datatype constraint
                        return Err(format!(
                            "Value {:?} is not a literal, expected datatype {}",
                            value_node, self.datatype
                        ));
                    }
                }
            }
        }
        Ok(ComponentValidationResult::Pass(component_id))
    }
}

impl GraphvizOutput for DatatypeConstraintComponent {
    fn component_type(&self) -> NamedNode {
        SHACL::new().datatype_constraint_component
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
        c: &mut Context, // Changed to &mut Context
        _context: &ValidationContext,
    ) -> Result<ComponentValidationResult, String> {
        let sh = SHACL::new();
        let expected_node_kind_term = self.node_kind.as_ref();

        if let Some(value_nodes) = c.value_nodes() {
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
                    return Err(format!(
                        "Value {:?} does not match nodeKind {}",
                        value_node, self.node_kind
                    ));
                }
            }
        }
        Ok(ComponentValidationResult::Pass(component_id))
    }
}

impl GraphvizOutput for NodeKindConstraintComponent {
    fn component_type(&self) -> NamedNode {
        SHACL::new().node_kind_constraint_component
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
