use crate::plan::{ComponentKind, ComponentParams, PlanIR, PlanPath, PlanShapeKind};
use sha2::{Digest, Sha256};
use shifty::compiled_runtime::program::{
    CompiledProgram, ComponentRow, IdTermRow, PathRow, ProgramHeader, RuleRow, ShapeKind, ShapeRow,
    StaticHints, TargetRow, TripleRow, KERNEL_VERSION, PROGRAM_SCHEMA_VERSION,
};
use std::collections::{BTreeMap, HashMap};

pub fn emit_compiled_program(plan: &PlanIR) -> Result<CompiledProgram, String> {
    let mut terms = plan.terms.clone();

    let shape_graph_iri = term_iri(&terms, plan.shape_graph)?;
    let default_data_graph_iri = format!("{}-compiled-data", shape_graph_iri);
    let default_data_graph_iri_term = intern_named_node_term(&mut terms, &default_data_graph_iri)?;

    let mut target_rows = Vec::<TargetRow>::new();
    let mut target_index = HashMap::<String, u64>::new();

    let shape_rows = plan
        .shapes
        .iter()
        .map(|shape| {
            let target_ids = shape
                .targets
                .iter()
                .map(|target| {
                    let key = serde_json::to_string(target)
                        .map_err(|err| format!("failed to encode plan target key: {err}"))?;
                    if let Some(id) = target_index.get(&key).copied() {
                        return Ok(id);
                    }
                    let id = target_rows.len() as u64;
                    target_rows.push(TargetRow {
                        id,
                        spec: serde_json::to_value(target)
                            .map_err(|err| format!("failed to encode plan target: {err}"))?,
                    });
                    target_index.insert(key, id);
                    Ok(id)
                })
                .collect::<Result<Vec<_>, String>>()?;

            let kind = match shape.kind {
                PlanShapeKind::Node => ShapeKind::Node,
                PlanShapeKind::Property => ShapeKind::Property,
            };

            Ok(ShapeRow {
                id: shape.id,
                kind,
                term: shape.term,
                target_ids,
                component_ids: shape.constraints.clone(),
                path_id: shape.path,
                deactivated: shape.deactivated,
                severity: format!("{:?}", shape.severity),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let component_rows = plan
        .components
        .iter()
        .map(|component| {
            let kind = component_kind_name(component.kind).to_string();
            let params = serde_json::to_value(&component.params)
                .map_err(|err| format!("failed to encode plan component params: {err}"))?;
            let source_constraint_component_term = component_source_constraint_component_term(
                component.kind,
                &component.params,
                &terms,
            )
            .map(|iri| intern_named_node_term(&mut terms, &iri))
            .transpose()?;

            Ok(ComponentRow {
                id: component.id,
                kind,
                params,
                source_constraint_component_term,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let path_rows = plan
        .paths
        .iter()
        .enumerate()
        .map(|(idx, path)| PathRow {
            id: idx as u64,
            spec: serde_json::to_value(path).unwrap_or_else(|_| serde_json::Value::Null),
            term: match path {
                PlanPath::Simple(term) => Some(*term),
                _ => None,
            },
        })
        .collect::<Vec<_>>();

    let rule_rows = plan
        .rules
        .iter()
        .map(|rule| RuleRow {
            id: rule.id,
            kind: serde_json::to_value(&rule.kind).unwrap_or_else(|_| serde_json::Value::Null),
            conditions: rule.conditions.clone(),
        })
        .collect::<Vec<_>>();

    let shape_graph_triples = plan
        .shape_triples
        .iter()
        .map(|triple| TripleRow {
            subject: triple.subject,
            predicate: triple.predicate,
            object: triple.object,
        })
        .collect::<Vec<_>>();

    let shape_id_to_term = shape_rows
        .iter()
        .map(|shape| IdTermRow {
            id: shape.id,
            term: shape.term,
        })
        .collect::<Vec<_>>();

    let component_id_to_iri_term = component_rows
        .iter()
        .filter_map(|component| {
            component
                .source_constraint_component_term
                .map(|term| IdTermRow {
                    id: component.id,
                    term,
                })
        })
        .collect::<Vec<_>>();

    let path_id_to_term = path_rows
        .iter()
        .filter_map(|path| path.term.map(|term| IdTermRow { id: path.id, term }))
        .collect::<Vec<_>>();

    let static_hints = StaticHints {
        shape_graph_iri_term: plan.shape_graph,
        default_data_graph_iri_term: Some(default_data_graph_iri_term),
        shape_id_to_term,
        component_id_to_iri_term,
        path_id_to_term,
    };

    let mut program = CompiledProgram {
        header: ProgramHeader {
            schema_version: PROGRAM_SCHEMA_VERSION,
            min_kernel_version: KERNEL_VERSION,
            program_hash: [0u8; 32],
            feature_bits: 0,
        },
        terms,
        shapes: shape_rows,
        components: component_rows,
        paths: path_rows,
        targets: target_rows,
        rules: rule_rows,
        shape_graph_triples,
        static_hints,
        ext: BTreeMap::new(),
    };

    program.header.program_hash = program_hash(&program)?;

    Ok(program)
}

pub fn serialize_program_deterministic(program: &CompiledProgram) -> Result<Vec<u8>, String> {
    serde_json::to_vec(program)
        .map_err(|err| format!("failed to serialize compiled program: {err}"))
}

fn program_hash(program: &CompiledProgram) -> Result<[u8; 32], String> {
    let mut normalized = program.clone();
    normalized.header.program_hash = [0u8; 32];
    let bytes = serialize_program_deterministic(&normalized)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&digest);
    Ok(hash)
}

fn term_iri(terms: &[oxigraph::model::Term], term_id: u64) -> Result<String, String> {
    match terms.get(term_id as usize) {
        Some(oxigraph::model::Term::NamedNode(node)) => Ok(node.as_str().to_string()),
        Some(_) => Err(format!("term {} is not an IRI", term_id)),
        None => Err(format!("missing term {}", term_id)),
    }
}

fn intern_named_node_term(
    terms: &mut Vec<oxigraph::model::Term>,
    iri: &str,
) -> Result<u64, String> {
    let term = oxigraph::model::Term::NamedNode(
        oxigraph::model::NamedNode::new(iri)
            .map_err(|err| format!("invalid IRI {} in compiled program: {err}", iri))?,
    );

    if let Some(pos) = terms.iter().position(|candidate| candidate == &term) {
        return Ok(pos as u64);
    }

    terms.push(term);
    Ok((terms.len() - 1) as u64)
}

fn component_kind_name(kind: ComponentKind) -> &'static str {
    match kind {
        ComponentKind::Node => "Node",
        ComponentKind::Property => "Property",
        ComponentKind::QualifiedValueShape => "QualifiedValueShape",
        ComponentKind::Class => "Class",
        ComponentKind::Datatype => "Datatype",
        ComponentKind::NodeKind => "NodeKind",
        ComponentKind::MinCount => "MinCount",
        ComponentKind::MaxCount => "MaxCount",
        ComponentKind::MinExclusive => "MinExclusive",
        ComponentKind::MinInclusive => "MinInclusive",
        ComponentKind::MaxExclusive => "MaxExclusive",
        ComponentKind::MaxInclusive => "MaxInclusive",
        ComponentKind::MinLength => "MinLength",
        ComponentKind::MaxLength => "MaxLength",
        ComponentKind::Pattern => "Pattern",
        ComponentKind::LanguageIn => "LanguageIn",
        ComponentKind::UniqueLang => "UniqueLang",
        ComponentKind::Equals => "Equals",
        ComponentKind::Disjoint => "Disjoint",
        ComponentKind::LessThan => "LessThan",
        ComponentKind::LessThanOrEquals => "LessThanOrEquals",
        ComponentKind::Not => "Not",
        ComponentKind::And => "And",
        ComponentKind::Or => "Or",
        ComponentKind::Xone => "Xone",
        ComponentKind::Closed => "Closed",
        ComponentKind::HasValue => "HasValue",
        ComponentKind::In => "In",
        ComponentKind::Sparql => "Sparql",
        ComponentKind::Custom => "Custom",
    }
}

fn component_source_constraint_component_term(
    kind: ComponentKind,
    params: &ComponentParams,
    terms: &[oxigraph::model::Term],
) -> Option<String> {
    match (kind, params) {
        (ComponentKind::Node, _) => {
            Some("http://www.w3.org/ns/shacl#NodeConstraintComponent".to_string())
        }
        (ComponentKind::Property, _) => {
            Some("http://www.w3.org/ns/shacl#PropertyShapeComponent".to_string())
        }
        (
            ComponentKind::QualifiedValueShape,
            ComponentParams::QualifiedValueShape { min_count, .. },
        ) => {
            if min_count.is_some() {
                Some("http://www.w3.org/ns/shacl#QualifiedMinCountConstraintComponent".to_string())
            } else {
                Some("http://www.w3.org/ns/shacl#QualifiedMaxCountConstraintComponent".to_string())
            }
        }
        (ComponentKind::Class, _) => {
            Some("http://www.w3.org/ns/shacl#ClassConstraintComponent".to_string())
        }
        (ComponentKind::Datatype, _) => {
            Some("http://www.w3.org/ns/shacl#DatatypeConstraintComponent".to_string())
        }
        (ComponentKind::NodeKind, _) => {
            Some("http://www.w3.org/ns/shacl#NodeKindConstraintComponent".to_string())
        }
        (ComponentKind::MinCount, _) => {
            Some("http://www.w3.org/ns/shacl#MinCountConstraintComponent".to_string())
        }
        (ComponentKind::MaxCount, _) => {
            Some("http://www.w3.org/ns/shacl#MaxCountConstraintComponent".to_string())
        }
        (ComponentKind::MinExclusive, _) => {
            Some("http://www.w3.org/ns/shacl#MinExclusiveConstraintComponent".to_string())
        }
        (ComponentKind::MinInclusive, _) => {
            Some("http://www.w3.org/ns/shacl#MinInclusiveConstraintComponent".to_string())
        }
        (ComponentKind::MaxExclusive, _) => {
            Some("http://www.w3.org/ns/shacl#MaxExclusiveConstraintComponent".to_string())
        }
        (ComponentKind::MaxInclusive, _) => {
            Some("http://www.w3.org/ns/shacl#MaxInclusiveConstraintComponent".to_string())
        }
        (ComponentKind::MinLength, _) => {
            Some("http://www.w3.org/ns/shacl#MinLengthConstraintComponent".to_string())
        }
        (ComponentKind::MaxLength, _) => {
            Some("http://www.w3.org/ns/shacl#MaxLengthConstraintComponent".to_string())
        }
        (ComponentKind::Pattern, _) => {
            Some("http://www.w3.org/ns/shacl#PatternConstraintComponent".to_string())
        }
        (ComponentKind::LanguageIn, _) => {
            Some("http://www.w3.org/ns/shacl#LanguageInConstraintComponent".to_string())
        }
        (ComponentKind::UniqueLang, _) => {
            Some("http://www.w3.org/ns/shacl#UniqueLangConstraintComponent".to_string())
        }
        (ComponentKind::Equals, _) => {
            Some("http://www.w3.org/ns/shacl#EqualsConstraintComponent".to_string())
        }
        (ComponentKind::Disjoint, _) => {
            Some("http://www.w3.org/ns/shacl#DisjointConstraintComponent".to_string())
        }
        (ComponentKind::LessThan, _) => {
            Some("http://www.w3.org/ns/shacl#LessThanConstraintComponent".to_string())
        }
        (ComponentKind::LessThanOrEquals, _) => {
            Some("http://www.w3.org/ns/shacl#LessThanOrEqualsConstraintComponent".to_string())
        }
        (ComponentKind::Not, _) => {
            Some("http://www.w3.org/ns/shacl#NotConstraintComponent".to_string())
        }
        (ComponentKind::And, _) => {
            Some("http://www.w3.org/ns/shacl#AndConstraintComponent".to_string())
        }
        (ComponentKind::Or, _) => {
            Some("http://www.w3.org/ns/shacl#OrConstraintComponent".to_string())
        }
        (ComponentKind::Xone, _) => {
            Some("http://www.w3.org/ns/shacl#XoneConstraintComponent".to_string())
        }
        (ComponentKind::Closed, _) => {
            Some("http://www.w3.org/ns/shacl#ClosedConstraintComponent".to_string())
        }
        (ComponentKind::HasValue, _) => {
            Some("http://www.w3.org/ns/shacl#HasValueConstraintComponent".to_string())
        }
        (ComponentKind::In, _) => {
            Some("http://www.w3.org/ns/shacl#InConstraintComponent".to_string())
        }
        (ComponentKind::Sparql, _) => {
            Some("http://www.w3.org/ns/shacl#SPARQLConstraintComponent".to_string())
        }
        (ComponentKind::Custom, ComponentParams::Custom { iri, .. }) => {
            match terms.get(*iri as usize) {
                Some(oxigraph::model::Term::NamedNode(node)) => Some(node.as_str().to_string()),
                _ => Some("http://www.w3.org/ns/shacl#ConstraintComponent".to_string()),
            }
        }
        _ => Some("http://www.w3.org/ns/shacl#ConstraintComponent".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{PlanComponent, PlanOrder, PlanShape, PlanTarget};
    use shifty::shacl_ir::Severity;

    fn sample_plan() -> PlanIR {
        let shape_graph = oxigraph::model::Term::NamedNode(
            oxigraph::model::NamedNode::new("http://example.com/shapes").unwrap(),
        );
        let node_shape = oxigraph::model::Term::NamedNode(
            oxigraph::model::NamedNode::new("http://example.com/shapes#S").unwrap(),
        );
        let class = oxigraph::model::Term::NamedNode(
            oxigraph::model::NamedNode::new("http://example.com/ns#Thing").unwrap(),
        );

        PlanIR {
            terms: vec![shape_graph, node_shape, class],
            paths: vec![],
            components: vec![PlanComponent {
                id: 0,
                kind: ComponentKind::Class,
                params: ComponentParams::Class { class: 2 },
            }],
            shapes: vec![PlanShape {
                id: 0,
                kind: PlanShapeKind::Node,
                targets: vec![PlanTarget::Class(2)],
                constraints: vec![0],
                path: None,
                severity: Severity::Violation,
                deactivated: false,
                term: 1,
            }],
            shape_triples: vec![],
            shape_graph: 0,
            order: PlanOrder {
                node_shapes: vec![0],
                property_shapes: vec![],
            },
            rules: vec![],
            node_shape_rules: HashMap::new(),
            property_shape_rules: HashMap::new(),
        }
    }

    #[test]
    fn deterministic_program_serialization() {
        let plan = sample_plan();
        let program_a = emit_compiled_program(&plan).expect("emit program A");
        let program_b = emit_compiled_program(&plan).expect("emit program B");

        let bytes_a = serialize_program_deterministic(&program_a).expect("serialize A");
        let bytes_b = serialize_program_deterministic(&program_b).expect("serialize B");

        assert_eq!(bytes_a, bytes_b);
        assert_eq!(program_a.header.program_hash, program_b.header.program_hash);
    }

    #[test]
    fn source_constraint_component_is_set_for_known_components() {
        let plan = sample_plan();
        let program = emit_compiled_program(&plan).expect("emit program");
        assert_eq!(program.components.len(), 1);
        assert!(program.components[0]
            .source_constraint_component_term
            .is_some());
    }
}
