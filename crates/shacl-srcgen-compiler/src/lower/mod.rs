use crate::ir::{
    FallbackAnnotation, SrcGenComponent, SrcGenComponentKind, SrcGenIR, SrcGenMeta,
    SrcGenNodeShape, SrcGenPropertyShape,
};
use oxigraph::model::Term;
use shifty::shacl_ir::{ComponentDescriptor, Path, PropShapeID, Severity, ShapeIR, Target, ID};
use std::collections::{BTreeMap, HashMap, HashSet};

pub fn lower_shape_ir(shape_ir: &ShapeIR) -> Result<SrcGenIR, String> {
    let mut component_ids: Vec<u64> = shape_ir.components.keys().map(|id| id.0).collect();
    component_ids.sort_unstable();

    let mut components = Vec::with_capacity(component_ids.len());
    let mut component_kinds: HashMap<u64, SrcGenComponentKind> =
        HashMap::with_capacity(component_ids.len());
    let mut fallback_annotations = Vec::new();

    for component_id in component_ids {
        let descriptor = shape_ir
            .components
            .get(&shifty::shacl_ir::ComponentID(component_id))
            .ok_or_else(|| format!("missing component {component_id} in ShapeIR"))?;

        let (kind, fallback_reason) = component_kind(shape_ir, descriptor);
        let fallback_only = fallback_reason.is_some();
        let iri = component_constraint_component_iri(descriptor).to_string();
        component_kinds.insert(component_id, kind.clone());
        components.push(SrcGenComponent {
            id: component_id,
            iri,
            kind,
            fallback_only,
        });

        if let Some(reason) = fallback_reason {
            fallback_annotations.push(FallbackAnnotation {
                component_id,
                reason,
            });
        }
    }

    let mut pending_property_shapes: Vec<PendingPropertyShape> = shape_ir
        .property_shapes
        .iter()
        .map(|shape| {
            let iri = shape_ir
                .property_shape_terms
                .get(&shape.id)
                .map(term_to_stable_string)
                .unwrap_or_else(|| format!("_:property-shape-{}", shape.id.0));
            let path_predicate = simple_named_path_predicate(&shape.path);
            let mut constraints: Vec<u64> = shape.constraints.iter().map(|id| id.0).collect();
            constraints.sort_unstable();

            let base_supported = !shape.deactivated
                && matches!(shape.severity, Severity::Violation)
                && shape.targets.is_empty()
                && path_predicate.is_some();
            let mut supported_constraints = Vec::new();
            let mut fallback_constraints = Vec::new();
            for component_id in &constraints {
                let supported_component = component_kinds
                    .get(component_id)
                    .map(property_constraint_supported)
                    .unwrap_or(false);
                if base_supported && supported_component {
                    supported_constraints.push(*component_id);
                } else {
                    fallback_constraints.push(*component_id);
                }
            }

            PendingPropertyShape {
                original_id: shape.id,
                iri,
                path_predicate,
                constraints,
                supported_constraints,
                fallback_constraints,
                supported: base_supported,
            }
        })
        .collect();

    pending_property_shapes.sort_by(|a, b| {
        a.iri
            .cmp(&b.iri)
            .then_with(|| a.original_id.0.cmp(&b.original_id.0))
    });

    let property_path_by_original: HashMap<PropShapeID, Option<String>> = pending_property_shapes
        .iter()
        .map(|shape| (shape.original_id, shape.path_predicate.clone()))
        .collect();

    let mut pending_node_shapes: Vec<PendingNodeShape> = shape_ir
        .node_shapes
        .iter()
        .map(|shape| {
            let iri = shape_ir
                .node_shape_terms
                .get(&shape.id)
                .map(term_to_stable_string)
                .unwrap_or_else(|| format!("_:node-shape-{}", shape.id.0));
            let mut constraints: Vec<u64> = shape.constraints.iter().map(|id| id.0).collect();
            constraints.sort_unstable();
            let mut property_shapes = shape.property_shapes.clone();
            property_shapes.sort_by_key(|id| id.0);

            let target_classes = class_targets_only(&shape.targets).unwrap_or_default();
            let targets_supported = !target_classes.is_empty();
            let base_supported = !shape.deactivated
                && matches!(shape.severity, Severity::Violation)
                && targets_supported;
            let linked_property_paths_supported = property_shapes.iter().all(|property_id| {
                property_path_by_original
                    .get(property_id)
                    .and_then(|path| path.as_ref())
                    .is_some()
            });

            let mut supported_constraints = Vec::new();
            let mut fallback_constraints = Vec::new();
            for component_id in &constraints {
                let Some(kind) = component_kinds.get(component_id) else {
                    fallback_constraints.push(*component_id);
                    continue;
                };
                let kind_supported = node_constraint_supported(kind);
                let context_supported = match kind {
                    SrcGenComponentKind::Closed { .. } => linked_property_paths_supported,
                    SrcGenComponentKind::Sparql { requires_path, .. } => !requires_path,
                    _ => true,
                };
                if base_supported && kind_supported && context_supported {
                    supported_constraints.push(*component_id);
                } else {
                    fallback_constraints.push(*component_id);
                }
            }

            PendingNodeShape {
                original_id: shape.id,
                iri,
                target_classes,
                constraints,
                supported_constraints,
                fallback_constraints,
                property_shapes,
                supported: base_supported,
            }
        })
        .collect();

    pending_node_shapes.sort_by(|a, b| {
        a.iri
            .cmp(&b.iri)
            .then_with(|| a.original_id.0.cmp(&b.original_id.0))
    });

    let mut node_id_by_original: HashMap<ID, u64> =
        HashMap::with_capacity(pending_node_shapes.len());
    let mut property_id_by_original: HashMap<PropShapeID, u64> =
        HashMap::with_capacity(pending_property_shapes.len());

    for (index, node) in pending_node_shapes.iter().enumerate() {
        node_id_by_original.insert(node.original_id, (index + 1) as u64);
    }
    let property_offset = pending_node_shapes.len() as u64;
    for (index, property) in pending_property_shapes.iter().enumerate() {
        property_id_by_original.insert(property.original_id, property_offset + (index as u64) + 1);
    }

    let property_shapes: Vec<SrcGenPropertyShape> = pending_property_shapes
        .into_iter()
        .filter_map(|shape| {
            property_id_by_original
                .get(&shape.original_id)
                .copied()
                .map(|id| SrcGenPropertyShape {
                    id,
                    iri: shape.iri,
                    path_predicate: shape.path_predicate,
                    constraints: shape.constraints,
                    supported_constraints: shape.supported_constraints,
                    fallback_constraints: shape.fallback_constraints,
                    supported: shape.supported,
                })
        })
        .collect();

    let property_support_by_id: HashMap<u64, bool> = property_shapes
        .iter()
        .map(|shape| (shape.id, shape.supported))
        .collect();

    let node_shapes: Vec<SrcGenNodeShape> = pending_node_shapes
        .into_iter()
        .filter_map(|shape| {
            let id = node_id_by_original.get(&shape.original_id).copied()?;
            let mut property_shapes: Vec<u64> = shape
                .property_shapes
                .iter()
                .filter_map(|prop_id| property_id_by_original.get(prop_id).copied())
                .collect();
            property_shapes.sort_unstable();

            let any_linked_properties_supported = property_shapes.iter().any(|prop_id| {
                property_support_by_id
                    .get(prop_id)
                    .copied()
                    .unwrap_or(false)
            });
            let is_noop_shape = shape.constraints.is_empty() && property_shapes.is_empty();

            let supported = shape.supported
                && (is_noop_shape
                    || !shape.supported_constraints.is_empty()
                    || any_linked_properties_supported);

            Some(SrcGenNodeShape {
                id,
                iri: shape.iri,
                target_classes: shape.target_classes,
                constraints: shape.constraints,
                supported_constraints: shape.supported_constraints,
                fallback_constraints: shape.fallback_constraints,
                property_shapes,
                supported,
            })
        })
        .collect();

    let specialization_ready = shape_ir.rules.is_empty()
        && components.iter().all(|component| !component.fallback_only)
        && !node_shapes.is_empty()
        && node_shapes.iter().all(|shape| {
            shape.supported
                && shape.fallback_constraints.is_empty()
                && shape.property_shapes.iter().all(|property_id| {
                    property_support_by_id
                        .get(property_id)
                        .copied()
                        .unwrap_or(false)
                })
        })
        && property_shapes
            .iter()
            .all(|shape| shape.supported && shape.fallback_constraints.is_empty());

    let data_graph_iri = shape_ir
        .data_graph
        .as_ref()
        .map(|iri| iri.as_str().to_string())
        .unwrap_or_default();

    Ok(SrcGenIR {
        meta: SrcGenMeta {
            compiler_track: "srcgen".to_string(),
            schema_version: 1,
            shape_graph_iri: shape_ir.shape_graph.as_str().to_string(),
            data_graph_iri,
            rule_count: shape_ir.rules.len(),
            specialization_ready,
        },
        node_shapes,
        property_shapes,
        components,
        fallback_annotations,
    })
}

#[derive(Debug)]
struct PendingNodeShape {
    original_id: ID,
    iri: String,
    target_classes: Vec<String>,
    constraints: Vec<u64>,
    supported_constraints: Vec<u64>,
    fallback_constraints: Vec<u64>,
    property_shapes: Vec<PropShapeID>,
    supported: bool,
}

#[derive(Debug)]
struct PendingPropertyShape {
    original_id: PropShapeID,
    iri: String,
    path_predicate: Option<String>,
    constraints: Vec<u64>,
    supported_constraints: Vec<u64>,
    fallback_constraints: Vec<u64>,
    supported: bool,
}

fn class_targets_only(targets: &[Target]) -> Option<Vec<String>> {
    let mut classes = Vec::new();
    for target in targets {
        match target {
            Target::Class(Term::NamedNode(class)) => classes.push(class.as_str().to_string()),
            _ => return None,
        }
    }
    classes.sort();
    classes.dedup();
    Some(classes)
}

fn simple_named_path_predicate(path: &Path) -> Option<String> {
    match path {
        Path::Simple(Term::NamedNode(predicate)) => Some(predicate.as_str().to_string()),
        _ => None,
    }
}

fn node_constraint_supported(kind: &SrcGenComponentKind) -> bool {
    matches!(
        kind,
        SrcGenComponentKind::Class { .. }
            | SrcGenComponentKind::PropertyLink
            | SrcGenComponentKind::Closed { .. }
            | SrcGenComponentKind::Datatype { .. }
            | SrcGenComponentKind::Node { .. }
            | SrcGenComponentKind::Not { .. }
            | SrcGenComponentKind::And { .. }
            | SrcGenComponentKind::Or { .. }
            | SrcGenComponentKind::Xone { .. }
            | SrcGenComponentKind::NodeKind { .. }
            | SrcGenComponentKind::MinLength { .. }
            | SrcGenComponentKind::MaxLength { .. }
            | SrcGenComponentKind::Pattern { .. }
            | SrcGenComponentKind::HasValue { .. }
            | SrcGenComponentKind::In { .. }
            | SrcGenComponentKind::LanguageIn { .. }
            | SrcGenComponentKind::Sparql { .. }
    )
}

fn property_constraint_supported(kind: &SrcGenComponentKind) -> bool {
    matches!(
        kind,
        SrcGenComponentKind::Class { .. }
            | SrcGenComponentKind::Datatype { .. }
            | SrcGenComponentKind::QualifiedValueShape { .. }
            | SrcGenComponentKind::Node { .. }
            | SrcGenComponentKind::Not { .. }
            | SrcGenComponentKind::And { .. }
            | SrcGenComponentKind::Or { .. }
            | SrcGenComponentKind::Xone { .. }
            | SrcGenComponentKind::NodeKind { .. }
            | SrcGenComponentKind::MinCount { .. }
            | SrcGenComponentKind::MaxCount { .. }
            | SrcGenComponentKind::MinLength { .. }
            | SrcGenComponentKind::MaxLength { .. }
            | SrcGenComponentKind::MinExclusive { .. }
            | SrcGenComponentKind::MinInclusive { .. }
            | SrcGenComponentKind::MaxExclusive { .. }
            | SrcGenComponentKind::MaxInclusive { .. }
            | SrcGenComponentKind::Pattern { .. }
            | SrcGenComponentKind::HasValue { .. }
            | SrcGenComponentKind::In { .. }
            | SrcGenComponentKind::LanguageIn { .. }
            | SrcGenComponentKind::UniqueLang { .. }
            | SrcGenComponentKind::Equals { .. }
            | SrcGenComponentKind::Disjoint { .. }
            | SrcGenComponentKind::LessThan { .. }
            | SrcGenComponentKind::LessThanOrEquals { .. }
            | SrcGenComponentKind::Sparql { .. }
    )
}

fn term_to_stable_string(term: &Term) -> String {
    match term {
        Term::NamedNode(node) => node.as_str().to_string(),
        _ => term.to_string(),
    }
}

fn escape_sparql_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
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

fn term_to_sparql(term: &Term) -> String {
    match term {
        Term::NamedNode(node) => format!("<{}>", node.as_str()),
        Term::BlankNode(node) => format!("_:{}", node.as_str()),
        Term::Literal(literal) => {
            if let Some(language) = literal.language() {
                format!("\"{}\"@{}", escape_sparql_string(literal.value()), language)
            } else {
                format!(
                    "\"{}\"^^<{}>",
                    escape_sparql_string(literal.value()),
                    literal.datatype().as_str(),
                )
            }
        }
    }
}

fn sparql_query_for(shape_ir: &ShapeIR, constraint_node: &Term) -> Result<String, String> {
    let select_predicate = "http://www.w3.org/ns/shacl#select";
    for quad in &shape_ir.shape_quads {
        if quad.predicate.as_str() != select_predicate {
            continue;
        }
        let subject = Term::from(quad.subject.clone());
        if &subject != constraint_node {
            continue;
        }
        return match &quad.object {
            Term::Literal(literal) => Ok(literal.value().to_string()),
            _ => Err("sh:select value must be a literal string".to_string()),
        };
    }
    Err("SPARQL constraint is missing sh:select".to_string())
}

fn sparql_prefixes_for(shape_ir: &ShapeIR, constraint_node: &Term) -> Result<String, String> {
    let sh_prefixes = "http://www.w3.org/ns/shacl#prefixes";
    let sh_declare = "http://www.w3.org/ns/shacl#declare";
    let sh_prefix = "http://www.w3.org/ns/shacl#prefix";
    let sh_namespace = "http://www.w3.org/ns/shacl#namespace";

    let mut prefix_subjects: Vec<Term> = shape_ir
        .shape_quads
        .iter()
        .filter_map(|quad| {
            if quad.predicate.as_str() != sh_prefixes {
                return None;
            }
            let subject = Term::from(quad.subject.clone());
            if &subject == constraint_node {
                Some(quad.object.clone())
            } else {
                None
            }
        })
        .collect();

    let mut global_declare_subjects: HashSet<Term> = shape_ir
        .shape_quads
        .iter()
        .filter_map(|quad| {
            if quad.predicate.as_str() == sh_declare {
                Some(Term::from(quad.subject.clone()))
            } else {
                None
            }
        })
        .collect();
    prefix_subjects.extend(global_declare_subjects.drain());
    prefix_subjects.sort_by_key(|term| term.to_string());
    prefix_subjects.dedup();

    let mut collected: BTreeMap<String, String> = BTreeMap::new();
    for prefixes_subject in prefix_subjects {
        let declarations: Vec<Term> = shape_ir
            .shape_quads
            .iter()
            .filter_map(|quad| {
                if quad.predicate.as_str() != sh_declare {
                    return None;
                }
                let subject = Term::from(quad.subject.clone());
                if subject == prefixes_subject {
                    Some(quad.object.clone())
                } else {
                    None
                }
            })
            .collect();

        for declaration in declarations {
            let prefix_val = shape_ir.shape_quads.iter().find_map(|quad| {
                if quad.predicate.as_str() != sh_prefix {
                    return None;
                }
                let subject = Term::from(quad.subject.clone());
                if subject == declaration {
                    Some(quad.object.clone())
                } else {
                    None
                }
            });
            let namespace_val = shape_ir.shape_quads.iter().find_map(|quad| {
                if quad.predicate.as_str() != sh_namespace {
                    return None;
                }
                let subject = Term::from(quad.subject.clone());
                if subject == declaration {
                    Some(quad.object.clone())
                } else {
                    None
                }
            });

            let (Some(Term::Literal(prefix_lit)), Some(Term::Literal(namespace_lit))) =
                (prefix_val, namespace_val)
            else {
                return Err(format!(
                    "Ill-formed prefix declaration for {}: missing sh:prefix or sh:namespace literal",
                    declaration
                ));
            };

            let prefix = prefix_lit.value().to_string();
            let namespace = namespace_lit.value().to_string();
            if let Some(existing_namespace) = collected.get(&prefix) {
                if existing_namespace != &namespace {
                    return Err(format!(
                        "Duplicate prefix '{}' with different namespaces: '{}' and '{}'",
                        prefix, existing_namespace, namespace
                    ));
                }
            } else {
                collected.insert(prefix, namespace);
            }
        }
    }

    if collected.is_empty() {
        return Ok(String::new());
    }

    let mut prefixes = String::new();
    for (prefix, namespace) in collected {
        prefixes.push_str("PREFIX ");
        prefixes.push_str(prefix.as_str());
        prefixes.push_str(": <");
        prefixes.push_str(namespace.as_str());
        prefixes.push_str(">\n");
    }
    Ok(prefixes.trim_end().to_string())
}

fn contains_variable_token(query: &str, sigil: char, var: &str) -> bool {
    let pattern = format!("{sigil}{var}");
    let query_bytes = query.as_bytes();
    let pattern_len = pattern.len();
    for (idx, _) in query.match_indices(pattern.as_str()) {
        let boundary_before = if idx == 0 {
            true
        } else {
            let prev = query_bytes[idx - 1];
            !(prev.is_ascii_alphanumeric() || prev == b'_')
        };
        let boundary_after = if idx + pattern_len >= query_bytes.len() {
            true
        } else {
            let next = query_bytes[idx + pattern_len];
            !(next.is_ascii_alphanumeric() || next == b'_')
        };
        if boundary_before && boundary_after {
            return true;
        }
    }
    false
}

fn sparql_query_mentions_var(query: &str, var: &str) -> bool {
    contains_variable_token(query, '?', var) || contains_variable_token(query, '$', var)
}

fn sparql_query_requires_path(query: &str) -> bool {
    query.contains("$PATH")
}

fn sparql_query_is_select(query: &str) -> bool {
    for line in query.lines() {
        let trimmed = line.trim_start();
        if trimmed.is_empty() {
            continue;
        }
        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("PREFIX ") || upper.starts_with("BASE ") {
            continue;
        }
        return upper.starts_with("SELECT");
    }
    false
}

fn sparql_query_uses_relation_projection_pattern(query: &str) -> bool {
    (query.contains("$this ?p ?o") || query.contains("?this ?p ?o"))
        && query.contains("?p a/rdfs:subClassOf* s223:Relation")
}

fn resolve_shape_iri(shape_ir: &ShapeIR, shape_id: ID) -> Option<String> {
    shape_ir
        .node_shape_terms
        .get(&shape_id)
        .map(term_to_stable_string)
        .or_else(|| {
            shape_ir
                .node_shapes
                .iter()
                .find(|shape| shape.id == shape_id)
                .map(|_| format!("_:node-shape-{}", shape_id.0))
        })
}

fn component_kind(
    shape_ir: &ShapeIR,
    descriptor: &ComponentDescriptor,
) -> (SrcGenComponentKind, Option<String>) {
    match descriptor {
        ComponentDescriptor::Property { .. } => (SrcGenComponentKind::PropertyLink, None),
        ComponentDescriptor::QualifiedValueShape {
            shape,
            min_count,
            max_count,
            disjoint,
        } => {
            if let Some(shape_iri) = resolve_shape_iri(shape_ir, *shape) {
                (
                    SrcGenComponentKind::QualifiedValueShape {
                        shape_iri,
                        min_count: *min_count,
                        max_count: *max_count,
                        disjoint: disjoint.unwrap_or(false),
                    },
                    None,
                )
            } else {
                (
                    SrcGenComponentKind::Unsupported {
                        kind: "QualifiedValueShape(missing-shape)".to_string(),
                    },
                    Some(format!(
                        "QualifiedValueShape constraint references unknown shape id {}",
                        shape.0
                    )),
                )
            }
        }
        ComponentDescriptor::Closed {
            closed,
            ignored_properties,
        } => {
            let ignored_property_iris = ignored_properties
                .iter()
                .filter_map(|term| match term {
                    Term::NamedNode(node) => Some(node.as_str().to_string()),
                    _ => None,
                })
                .collect();
            (
                SrcGenComponentKind::Closed {
                    closed: *closed,
                    ignored_property_iris,
                },
                None,
            )
        }
        ComponentDescriptor::Node { shape } => {
            if let Some(shape_iri) = resolve_shape_iri(shape_ir, *shape) {
                (SrcGenComponentKind::Node { shape_iri }, None)
            } else {
                (
                    SrcGenComponentKind::Unsupported {
                        kind: "Node(missing-shape)".to_string(),
                    },
                    Some(format!(
                        "Node constraint references unknown shape id {}",
                        shape.0
                    )),
                )
            }
        }
        ComponentDescriptor::Not { shape } => {
            if let Some(shape_iri) = resolve_shape_iri(shape_ir, *shape) {
                (SrcGenComponentKind::Not { shape_iri }, None)
            } else {
                (
                    SrcGenComponentKind::Unsupported {
                        kind: "Not(missing-shape)".to_string(),
                    },
                    Some(format!(
                        "Not constraint references unknown shape id {}",
                        shape.0
                    )),
                )
            }
        }
        ComponentDescriptor::And { shapes } => {
            let mut shape_iris = Vec::with_capacity(shapes.len());
            for shape_id in shapes {
                let Some(shape_iri) = resolve_shape_iri(shape_ir, *shape_id) else {
                    return (
                        SrcGenComponentKind::Unsupported {
                            kind: "And(missing-shape)".to_string(),
                        },
                        Some(format!(
                            "And constraint references unknown shape id {}",
                            shape_id.0
                        )),
                    );
                };
                shape_iris.push(shape_iri);
            }
            (SrcGenComponentKind::And { shape_iris }, None)
        }
        ComponentDescriptor::Or { shapes } => {
            let mut shape_iris = Vec::with_capacity(shapes.len());
            for shape_id in shapes {
                let Some(shape_iri) = resolve_shape_iri(shape_ir, *shape_id) else {
                    return (
                        SrcGenComponentKind::Unsupported {
                            kind: "Or(missing-shape)".to_string(),
                        },
                        Some(format!(
                            "Or constraint references unknown shape id {}",
                            shape_id.0
                        )),
                    );
                };
                shape_iris.push(shape_iri);
            }
            (SrcGenComponentKind::Or { shape_iris }, None)
        }
        ComponentDescriptor::Xone { shapes } => {
            let mut shape_iris = Vec::with_capacity(shapes.len());
            for shape_id in shapes {
                let Some(shape_iri) = resolve_shape_iri(shape_ir, *shape_id) else {
                    return (
                        SrcGenComponentKind::Unsupported {
                            kind: "Xone(missing-shape)".to_string(),
                        },
                        Some(format!(
                            "Xone constraint references unknown shape id {}",
                            shape_id.0
                        )),
                    );
                };
                shape_iris.push(shape_iri);
            }
            (SrcGenComponentKind::Xone { shape_iris }, None)
        }
        ComponentDescriptor::Class {
            class: Term::NamedNode(class),
        } => (
            SrcGenComponentKind::Class {
                class_iri: class.as_str().to_string(),
            },
            None,
        ),
        ComponentDescriptor::Datatype {
            datatype: Term::NamedNode(datatype),
        } => (
            SrcGenComponentKind::Datatype {
                datatype_iri: datatype.as_str().to_string(),
            },
            None,
        ),
        ComponentDescriptor::NodeKind {
            node_kind: Term::NamedNode(node_kind),
        } => (
            SrcGenComponentKind::NodeKind {
                node_kind_iri: node_kind.as_str().to_string(),
            },
            None,
        ),
        ComponentDescriptor::MinCount { min_count } => (
            SrcGenComponentKind::MinCount {
                min_count: *min_count,
            },
            None,
        ),
        ComponentDescriptor::MaxCount { max_count } => (
            SrcGenComponentKind::MaxCount {
                max_count: *max_count,
            },
            None,
        ),
        ComponentDescriptor::MinLength { length } => (
            SrcGenComponentKind::MinLength {
                min_length: *length,
            },
            None,
        ),
        ComponentDescriptor::MaxLength { length } => (
            SrcGenComponentKind::MaxLength {
                max_length: *length,
            },
            None,
        ),
        ComponentDescriptor::MinExclusive { value } => (
            SrcGenComponentKind::MinExclusive {
                value_sparql: term_to_sparql(value),
            },
            None,
        ),
        ComponentDescriptor::MinInclusive { value } => (
            SrcGenComponentKind::MinInclusive {
                value_sparql: term_to_sparql(value),
            },
            None,
        ),
        ComponentDescriptor::MaxExclusive { value } => (
            SrcGenComponentKind::MaxExclusive {
                value_sparql: term_to_sparql(value),
            },
            None,
        ),
        ComponentDescriptor::MaxInclusive { value } => (
            SrcGenComponentKind::MaxInclusive {
                value_sparql: term_to_sparql(value),
            },
            None,
        ),
        ComponentDescriptor::Pattern { pattern, flags } => (
            SrcGenComponentKind::Pattern {
                pattern: pattern.clone(),
                flags: flags.clone(),
            },
            None,
        ),
        ComponentDescriptor::HasValue { value } => (
            SrcGenComponentKind::HasValue {
                value_sparql: term_to_sparql(value),
            },
            None,
        ),
        ComponentDescriptor::In { values } => (
            SrcGenComponentKind::In {
                values_sparql: values.iter().map(term_to_sparql).collect(),
            },
            None,
        ),
        ComponentDescriptor::Sparql { constraint_node } => {
            let query = match sparql_query_for(shape_ir, constraint_node) {
                Ok(query) => query,
                Err(err) => {
                    return (
                        SrcGenComponentKind::Unsupported {
                            kind: "Sparql(missing-select)".to_string(),
                        },
                        Some(err),
                    );
                }
            };
            if !sparql_query_is_select(&query) {
                return (
                    SrcGenComponentKind::Unsupported {
                        kind: "Sparql(non-select)".to_string(),
                    },
                    Some(
                        "SPARQL constraint specialization currently supports SELECT queries only"
                            .to_string(),
                    ),
                );
            }
            if !sparql_query_mentions_var(&query, "this") {
                return (
                    SrcGenComponentKind::Unsupported {
                        kind: "Sparql(missing-this)".to_string(),
                    },
                    Some(
                        "SPARQL constraint does not reference pre-bound variable ?this".to_string(),
                    ),
                );
            }
            if sparql_query_uses_relation_projection_pattern(&query) {
                return (
                    SrcGenComponentKind::Unsupported {
                        kind: "Sparql(relation-projection)".to_string(),
                    },
                    Some("SPARQL relation-projection query pattern is not yet specialized (guarded component fallback)".to_string()),
                );
            }
            let prefixes = match sparql_prefixes_for(shape_ir, constraint_node) {
                Ok(prefixes) => prefixes,
                Err(err) => {
                    return (
                        SrcGenComponentKind::Unsupported {
                            kind: "Sparql(prefixes)".to_string(),
                        },
                        Some(format!(
                            "SPARQL constraint prefix resolution failed for {}: {}",
                            constraint_node, err
                        )),
                    );
                }
            };
            let requires_path = sparql_query_requires_path(&query);
            (
                SrcGenComponentKind::Sparql {
                    query,
                    prefixes,
                    requires_path,
                    constraint_term: constraint_node.to_string(),
                },
                None,
            )
        }
        ComponentDescriptor::LanguageIn { languages } => (
            SrcGenComponentKind::LanguageIn {
                languages: languages.clone(),
            },
            None,
        ),
        ComponentDescriptor::UniqueLang { enabled } => {
            (SrcGenComponentKind::UniqueLang { enabled: *enabled }, None)
        }
        ComponentDescriptor::Equals {
            property: Term::NamedNode(property),
        } => (
            SrcGenComponentKind::Equals {
                property_iri: property.as_str().to_string(),
            },
            None,
        ),
        ComponentDescriptor::Disjoint {
            property: Term::NamedNode(property),
        } => (
            SrcGenComponentKind::Disjoint {
                property_iri: property.as_str().to_string(),
            },
            None,
        ),
        ComponentDescriptor::LessThan {
            property: Term::NamedNode(property),
        } => (
            SrcGenComponentKind::LessThan {
                property_iri: property.as_str().to_string(),
            },
            None,
        ),
        ComponentDescriptor::LessThanOrEquals {
            property: Term::NamedNode(property),
        } => (
            SrcGenComponentKind::LessThanOrEquals {
                property_iri: property.as_str().to_string(),
            },
            None,
        ),
        ComponentDescriptor::Equals { .. } => (
            SrcGenComponentKind::Unsupported {
                kind: "Equals(non-iri)".to_string(),
            },
            Some("Equals constraint uses a non-IRI property term".to_string()),
        ),
        ComponentDescriptor::Disjoint { .. } => (
            SrcGenComponentKind::Unsupported {
                kind: "Disjoint(non-iri)".to_string(),
            },
            Some("Disjoint constraint uses a non-IRI property term".to_string()),
        ),
        ComponentDescriptor::LessThan { .. } => (
            SrcGenComponentKind::Unsupported {
                kind: "LessThan(non-iri)".to_string(),
            },
            Some("LessThan constraint uses a non-IRI property term".to_string()),
        ),
        ComponentDescriptor::LessThanOrEquals { .. } => (
            SrcGenComponentKind::Unsupported {
                kind: "LessThanOrEquals(non-iri)".to_string(),
            },
            Some("LessThanOrEquals constraint uses a non-IRI property term".to_string()),
        ),
        ComponentDescriptor::Class { .. } => (
            SrcGenComponentKind::Unsupported {
                kind: "Class(non-iri)".to_string(),
            },
            Some("Class constraint uses a non-IRI class term".to_string()),
        ),
        ComponentDescriptor::Datatype { .. } => (
            SrcGenComponentKind::Unsupported {
                kind: "Datatype(non-iri)".to_string(),
            },
            Some("Datatype constraint uses a non-IRI datatype term".to_string()),
        ),
        ComponentDescriptor::NodeKind { .. } => (
            SrcGenComponentKind::Unsupported {
                kind: "NodeKind(non-iri)".to_string(),
            },
            Some("NodeKind constraint uses a non-IRI node kind term".to_string()),
        ),
        other => (
            SrcGenComponentKind::Unsupported {
                kind: component_kind_name(other).to_string(),
            },
            Some(format!(
                "component kind {} is not specialized in phase-2",
                component_kind_name(other)
            )),
        ),
    }
}

fn component_kind_name(component: &ComponentDescriptor) -> &'static str {
    match component {
        ComponentDescriptor::Node { .. } => "Node",
        ComponentDescriptor::Property { .. } => "Property",
        ComponentDescriptor::QualifiedValueShape { .. } => "QualifiedValueShape",
        ComponentDescriptor::Class { .. } => "Class",
        ComponentDescriptor::Datatype { .. } => "Datatype",
        ComponentDescriptor::NodeKind { .. } => "NodeKind",
        ComponentDescriptor::MinCount { .. } => "MinCount",
        ComponentDescriptor::MaxCount { .. } => "MaxCount",
        ComponentDescriptor::MinExclusive { .. } => "MinExclusive",
        ComponentDescriptor::MinInclusive { .. } => "MinInclusive",
        ComponentDescriptor::MaxExclusive { .. } => "MaxExclusive",
        ComponentDescriptor::MaxInclusive { .. } => "MaxInclusive",
        ComponentDescriptor::MinLength { .. } => "MinLength",
        ComponentDescriptor::MaxLength { .. } => "MaxLength",
        ComponentDescriptor::Pattern { .. } => "Pattern",
        ComponentDescriptor::LanguageIn { .. } => "LanguageIn",
        ComponentDescriptor::UniqueLang { .. } => "UniqueLang",
        ComponentDescriptor::Equals { .. } => "Equals",
        ComponentDescriptor::Disjoint { .. } => "Disjoint",
        ComponentDescriptor::LessThan { .. } => "LessThan",
        ComponentDescriptor::LessThanOrEquals { .. } => "LessThanOrEquals",
        ComponentDescriptor::Not { .. } => "Not",
        ComponentDescriptor::And { .. } => "And",
        ComponentDescriptor::Or { .. } => "Or",
        ComponentDescriptor::Xone { .. } => "Xone",
        ComponentDescriptor::Closed { .. } => "Closed",
        ComponentDescriptor::HasValue { .. } => "HasValue",
        ComponentDescriptor::In { .. } => "In",
        ComponentDescriptor::Sparql { .. } => "Sparql",
        ComponentDescriptor::Custom { .. } => "Custom",
    }
}

fn component_constraint_component_iri(component: &ComponentDescriptor) -> &str {
    match component {
        ComponentDescriptor::Node { .. } => "http://www.w3.org/ns/shacl#NodeConstraintComponent",
        ComponentDescriptor::Property { .. } => "http://www.w3.org/ns/shacl#PropertyShapeComponent",
        ComponentDescriptor::QualifiedValueShape { min_count, .. } => {
            if min_count.is_some() {
                "http://www.w3.org/ns/shacl#QualifiedMinCountConstraintComponent"
            } else {
                "http://www.w3.org/ns/shacl#QualifiedMaxCountConstraintComponent"
            }
        }
        ComponentDescriptor::Class { .. } => "http://www.w3.org/ns/shacl#ClassConstraintComponent",
        ComponentDescriptor::Datatype { .. } => {
            "http://www.w3.org/ns/shacl#DatatypeConstraintComponent"
        }
        ComponentDescriptor::NodeKind { .. } => {
            "http://www.w3.org/ns/shacl#NodeKindConstraintComponent"
        }
        ComponentDescriptor::MinCount { .. } => {
            "http://www.w3.org/ns/shacl#MinCountConstraintComponent"
        }
        ComponentDescriptor::MaxCount { .. } => {
            "http://www.w3.org/ns/shacl#MaxCountConstraintComponent"
        }
        ComponentDescriptor::MinExclusive { .. } => {
            "http://www.w3.org/ns/shacl#MinExclusiveConstraintComponent"
        }
        ComponentDescriptor::MinInclusive { .. } => {
            "http://www.w3.org/ns/shacl#MinInclusiveConstraintComponent"
        }
        ComponentDescriptor::MaxExclusive { .. } => {
            "http://www.w3.org/ns/shacl#MaxExclusiveConstraintComponent"
        }
        ComponentDescriptor::MaxInclusive { .. } => {
            "http://www.w3.org/ns/shacl#MaxInclusiveConstraintComponent"
        }
        ComponentDescriptor::MinLength { .. } => {
            "http://www.w3.org/ns/shacl#MinLengthConstraintComponent"
        }
        ComponentDescriptor::MaxLength { .. } => {
            "http://www.w3.org/ns/shacl#MaxLengthConstraintComponent"
        }
        ComponentDescriptor::Pattern { .. } => {
            "http://www.w3.org/ns/shacl#PatternConstraintComponent"
        }
        ComponentDescriptor::LanguageIn { .. } => {
            "http://www.w3.org/ns/shacl#LanguageInConstraintComponent"
        }
        ComponentDescriptor::UniqueLang { .. } => {
            "http://www.w3.org/ns/shacl#UniqueLangConstraintComponent"
        }
        ComponentDescriptor::Equals { .. } => {
            "http://www.w3.org/ns/shacl#EqualsConstraintComponent"
        }
        ComponentDescriptor::Disjoint { .. } => {
            "http://www.w3.org/ns/shacl#DisjointConstraintComponent"
        }
        ComponentDescriptor::LessThan { .. } => {
            "http://www.w3.org/ns/shacl#LessThanConstraintComponent"
        }
        ComponentDescriptor::LessThanOrEquals { .. } => {
            "http://www.w3.org/ns/shacl#LessThanOrEqualsConstraintComponent"
        }
        ComponentDescriptor::Not { .. } => "http://www.w3.org/ns/shacl#NotConstraintComponent",
        ComponentDescriptor::And { .. } => "http://www.w3.org/ns/shacl#AndConstraintComponent",
        ComponentDescriptor::Or { .. } => "http://www.w3.org/ns/shacl#OrConstraintComponent",
        ComponentDescriptor::Xone { .. } => "http://www.w3.org/ns/shacl#XoneConstraintComponent",
        ComponentDescriptor::Closed { .. } => {
            "http://www.w3.org/ns/shacl#ClosedConstraintComponent"
        }
        ComponentDescriptor::HasValue { .. } => {
            "http://www.w3.org/ns/shacl#HasValueConstraintComponent"
        }
        ComponentDescriptor::In { .. } => "http://www.w3.org/ns/shacl#InConstraintComponent",
        ComponentDescriptor::Sparql { .. } => {
            "http://www.w3.org/ns/shacl#SPARQLConstraintComponent"
        }
        ComponentDescriptor::Custom { definition, .. } => definition.iri.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shifty::shacl_ir::{
        ComponentDescriptor, ComponentID, FeatureToggles, NodeShapeIR, PropShapeID,
        PropertyShapeIR, Severity, ShapeIR, Target, ID,
    };
    use std::collections::HashMap;

    #[test]
    fn lower_is_deterministic_with_unsorted_maps() {
        let mut components = HashMap::new();
        components.insert(
            shifty::shacl_ir::ComponentID(9),
            ComponentDescriptor::MaxCount { max_count: 2 },
        );
        components.insert(
            shifty::shacl_ir::ComponentID(3),
            ComponentDescriptor::MinCount { min_count: 1 },
        );

        let mut node_shape_terms = HashMap::new();
        node_shape_terms.insert(
            ID(8),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:z-node",
            )),
        );
        node_shape_terms.insert(
            ID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:a-node",
            )),
        );

        let mut property_shape_terms = HashMap::new();
        property_shape_terms.insert(
            PropShapeID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:b-prop",
            )),
        );

        let shape_ir = ShapeIR {
            shape_graph: oxigraph::model::NamedNode::new_unchecked("urn:shape-graph"),
            data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:data-graph")),
            node_shapes: vec![NodeShapeIR {
                id: ID(1),
                targets: Vec::new(),
                constraints: Vec::new(),
                property_shapes: Vec::new(),
                severity: Severity::Violation,
                deactivated: false,
            }],
            property_shapes: vec![PropertyShapeIR {
                id: PropShapeID(1),
                targets: Vec::new(),
                path: Path::Simple(Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:p",
                ))),
                path_term: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:p")),
                constraints: Vec::new(),
                severity: Severity::Violation,
                deactivated: false,
            }],
            components,
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            node_shape_terms,
            property_shape_terms,
            shape_quads: Vec::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            features: FeatureToggles::default(),
        };

        let first = lower_shape_ir(&shape_ir).unwrap();
        let second = lower_shape_ir(&shape_ir).unwrap();

        assert_eq!(
            first.to_json_pretty().unwrap(),
            second.to_json_pretty().unwrap()
        );
        assert_eq!(first.node_shapes[0].iri, "urn:shape:a-node");
        assert_eq!(first.components[0].id, 3);
        assert!(!first.meta.specialization_ready);
    }

    #[test]
    fn specialization_ready_for_phase1_supported_subset() {
        let mut components = HashMap::new();
        components.insert(
            ComponentID(1),
            ComponentDescriptor::Class {
                class: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:Person")),
            },
        );
        components.insert(
            ComponentID(2),
            ComponentDescriptor::Property {
                shape: PropShapeID(7),
            },
        );
        components.insert(
            ComponentID(3),
            ComponentDescriptor::Datatype {
                datatype: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "http://www.w3.org/2001/XMLSchema#integer",
                )),
            },
        );
        components.insert(
            ComponentID(4),
            ComponentDescriptor::MinCount { min_count: 1 },
        );
        components.insert(
            ComponentID(5),
            ComponentDescriptor::MaxCount { max_count: 1 },
        );

        let mut node_shape_terms = HashMap::new();
        node_shape_terms.insert(
            ID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person",
            )),
        );
        let mut property_shape_terms = HashMap::new();
        property_shape_terms.insert(
            PropShapeID(7),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person-age",
            )),
        );

        let shape_ir = ShapeIR {
            shape_graph: oxigraph::model::NamedNode::new_unchecked("urn:shape-graph"),
            data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:data-graph")),
            node_shapes: vec![NodeShapeIR {
                id: ID(1),
                targets: vec![Target::Class(Term::NamedNode(
                    oxigraph::model::NamedNode::new_unchecked("urn:Person"),
                ))],
                constraints: vec![ComponentID(1), ComponentID(2)],
                property_shapes: vec![PropShapeID(7)],
                severity: Severity::Violation,
                deactivated: false,
            }],
            property_shapes: vec![PropertyShapeIR {
                id: PropShapeID(7),
                targets: Vec::new(),
                path: Path::Simple(Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:age",
                ))),
                path_term: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:age")),
                constraints: vec![ComponentID(3), ComponentID(4), ComponentID(5)],
                severity: Severity::Violation,
                deactivated: false,
            }],
            components,
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            node_shape_terms,
            property_shape_terms,
            shape_quads: Vec::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            features: FeatureToggles::default(),
        };

        let lowered = lower_shape_ir(&shape_ir).unwrap();
        assert!(lowered.meta.specialization_ready);
        assert!(lowered.fallback_annotations.is_empty());
        assert!(lowered
            .components
            .iter()
            .all(|component| !component.fallback_only));
        assert_eq!(lowered.node_shapes.len(), 1);
        assert_eq!(lowered.property_shapes.len(), 1);
        assert!(lowered.node_shapes[0].supported);
        assert!(lowered.property_shapes[0].supported);
    }

    #[test]
    fn unsupported_component_disables_specialization_and_sets_fallback_annotation() {
        let mut components = HashMap::new();
        components.insert(
            ComponentID(1),
            ComponentDescriptor::Sparql {
                constraint_node: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:sparql:constraint",
                )),
            },
        );

        let mut node_shape_terms = HashMap::new();
        node_shape_terms.insert(
            ID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person",
            )),
        );

        let shape_ir = ShapeIR {
            shape_graph: oxigraph::model::NamedNode::new_unchecked("urn:shape-graph"),
            data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:data-graph")),
            node_shapes: vec![NodeShapeIR {
                id: ID(1),
                targets: vec![Target::Class(Term::NamedNode(
                    oxigraph::model::NamedNode::new_unchecked("urn:Person"),
                ))],
                constraints: vec![ComponentID(1)],
                property_shapes: Vec::new(),
                severity: Severity::Violation,
                deactivated: false,
            }],
            property_shapes: Vec::new(),
            components,
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            node_shape_terms,
            property_shape_terms: HashMap::new(),
            shape_quads: Vec::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            features: FeatureToggles::default(),
        };

        let lowered = lower_shape_ir(&shape_ir).unwrap();
        assert!(!lowered.meta.specialization_ready);
        assert_eq!(lowered.fallback_annotations.len(), 1);
        assert_eq!(lowered.fallback_annotations[0].component_id, 1);
        assert!(lowered.components[0].fallback_only);
        assert!(!lowered.node_shapes[0].supported);
    }

    #[test]
    fn lowering_tracks_supported_and_fallback_constraints_per_shape() {
        let mut components = HashMap::new();
        components.insert(
            ComponentID(1),
            ComponentDescriptor::Class {
                class: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:Person")),
            },
        );
        components.insert(
            ComponentID(2),
            ComponentDescriptor::Property {
                shape: PropShapeID(7),
            },
        );
        components.insert(
            ComponentID(3),
            ComponentDescriptor::MinCount { min_count: 1 },
        );
        components.insert(
            ComponentID(4),
            ComponentDescriptor::Sparql {
                constraint_node: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:sparql:constraint",
                )),
            },
        );

        let mut node_shape_terms = HashMap::new();
        node_shape_terms.insert(
            ID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person",
            )),
        );
        let mut property_shape_terms = HashMap::new();
        property_shape_terms.insert(
            PropShapeID(7),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person-age",
            )),
        );

        let shape_ir = ShapeIR {
            shape_graph: oxigraph::model::NamedNode::new_unchecked("urn:shape-graph"),
            data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:data-graph")),
            node_shapes: vec![NodeShapeIR {
                id: ID(1),
                targets: vec![Target::Class(Term::NamedNode(
                    oxigraph::model::NamedNode::new_unchecked("urn:Person"),
                ))],
                constraints: vec![ComponentID(1), ComponentID(2), ComponentID(4)],
                property_shapes: vec![PropShapeID(7)],
                severity: Severity::Violation,
                deactivated: false,
            }],
            property_shapes: vec![PropertyShapeIR {
                id: PropShapeID(7),
                targets: Vec::new(),
                path: Path::Simple(Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:age",
                ))),
                path_term: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:age")),
                constraints: vec![ComponentID(3), ComponentID(4)],
                severity: Severity::Violation,
                deactivated: false,
            }],
            components,
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            node_shape_terms,
            property_shape_terms,
            shape_quads: Vec::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            features: FeatureToggles::default(),
        };

        let lowered = lower_shape_ir(&shape_ir).unwrap();
        assert!(!lowered.meta.specialization_ready);

        assert_eq!(lowered.fallback_annotations.len(), 1);
        assert_eq!(lowered.fallback_annotations[0].component_id, 4);

        let node_shape = lowered
            .node_shapes
            .iter()
            .find(|shape| shape.iri == "urn:shape:person")
            .expect("node shape must be present");
        assert!(node_shape.supported);
        assert_eq!(node_shape.supported_constraints, vec![1, 2]);
        assert_eq!(node_shape.fallback_constraints, vec![4]);

        let property_shape = lowered
            .property_shapes
            .iter()
            .find(|shape| shape.iri == "urn:shape:person-age")
            .expect("property shape must be present");
        assert!(property_shape.supported);
        assert_eq!(property_shape.supported_constraints, vec![3]);
        assert_eq!(property_shape.fallback_constraints, vec![4]);
    }

    #[test]
    fn sparql_component_is_specialized_when_select_query_is_available() {
        let mut components = HashMap::new();
        components.insert(
            ComponentID(1),
            ComponentDescriptor::Sparql {
                constraint_node: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:sparql:constraint",
                )),
            },
        );

        let mut node_shape_terms = HashMap::new();
        node_shape_terms.insert(
            ID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person",
            )),
        );

        let shape_graph = oxigraph::model::NamedNode::new_unchecked("urn:shape-graph");
        let shape_ir = ShapeIR {
            shape_graph: shape_graph.clone(),
            data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:data-graph")),
            node_shapes: vec![NodeShapeIR {
                id: ID(1),
                targets: vec![Target::Class(Term::NamedNode(
                    oxigraph::model::NamedNode::new_unchecked("urn:Person"),
                ))],
                constraints: vec![ComponentID(1)],
                property_shapes: Vec::new(),
                severity: Severity::Violation,
                deactivated: false,
            }],
            property_shapes: Vec::new(),
            components,
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            node_shape_terms,
            property_shape_terms: HashMap::new(),
            shape_quads: vec![oxigraph::model::Quad::new(
                oxigraph::model::NamedNode::new_unchecked("urn:sparql:constraint"),
                oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#select"),
                oxigraph::model::Literal::new_typed_literal(
                    "SELECT $this WHERE { FILTER(false) }",
                    oxigraph::model::vocab::xsd::STRING,
                ),
                oxigraph::model::GraphName::NamedNode(shape_graph),
            )],
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            features: FeatureToggles::default(),
        };

        let lowered = lower_shape_ir(&shape_ir).unwrap();
        assert!(lowered.meta.specialization_ready);
        assert!(lowered.fallback_annotations.is_empty());
        assert!(matches!(
            lowered.components[0].kind,
            SrcGenComponentKind::Sparql { .. }
        ));
        assert!(matches!(
            lowered.components[0].kind,
            SrcGenComponentKind::Sparql { ref constraint_term, .. }
                if constraint_term.contains("urn:sparql:constraint")
        ));
        assert!(lowered.node_shapes[0].supported);
        assert_eq!(lowered.node_shapes[0].supported_constraints, vec![1]);
        assert!(lowered.node_shapes[0].fallback_constraints.is_empty());
    }

    #[test]
    fn sparql_relation_projection_pattern_is_guarded_for_fallback() {
        let mut components = HashMap::new();
        components.insert(
            ComponentID(1),
            ComponentDescriptor::Sparql {
                constraint_node: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:closed-world:sparql",
                )),
            },
        );

        let mut node_shape_terms = HashMap::new();
        node_shape_terms.insert(
            ID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:closed-world",
            )),
        );

        let shape_graph = oxigraph::model::NamedNode::new_unchecked("urn:shape-graph");
        let shape_ir = ShapeIR {
            shape_graph: shape_graph.clone(),
            data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:data-graph")),
            node_shapes: vec![NodeShapeIR {
                id: ID(1),
                targets: vec![Target::Class(Term::NamedNode(
                    oxigraph::model::NamedNode::new_unchecked("urn:Equipment"),
                ))],
                constraints: vec![ComponentID(1)],
                property_shapes: Vec::new(),
                severity: Severity::Violation,
                deactivated: false,
            }],
            property_shapes: Vec::new(),
            components,
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            node_shape_terms,
            property_shape_terms: HashMap::new(),
            shape_quads: vec![oxigraph::model::Quad::new(
                oxigraph::model::NamedNode::new_unchecked("urn:closed-world:sparql"),
                oxigraph::model::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#select"),
                oxigraph::model::Literal::new_typed_literal(
                    "SELECT $this WHERE { $this ?p ?o . ?p a/rdfs:subClassOf* s223:Relation . }",
                    oxigraph::model::vocab::xsd::STRING,
                ),
                oxigraph::model::GraphName::NamedNode(shape_graph),
            )],
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            features: FeatureToggles::default(),
        };

        let lowered = lower_shape_ir(&shape_ir).unwrap();
        assert!(!lowered.meta.specialization_ready);
        assert_eq!(lowered.fallback_annotations.len(), 1);
        assert_eq!(
            lowered.fallback_annotations[0].reason,
            "SPARQL relation-projection query pattern is not yet specialized (guarded component fallback)"
        );
        assert!(matches!(
            lowered.components[0].kind,
            SrcGenComponentKind::Unsupported { ref kind }
                if kind == "Sparql(relation-projection)"
        ));
        assert_eq!(lowered.node_shapes[0].fallback_constraints, vec![1]);
    }

    #[test]
    fn specialization_ready_for_phase2_language_and_property_pair_constraints() {
        let mut components = HashMap::new();
        components.insert(
            ComponentID(1),
            ComponentDescriptor::Class {
                class: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:Person")),
            },
        );
        components.insert(
            ComponentID(2),
            ComponentDescriptor::Property {
                shape: PropShapeID(7),
            },
        );
        components.insert(
            ComponentID(3),
            ComponentDescriptor::LanguageIn {
                languages: vec!["en".to_string(), "fr".to_string()],
            },
        );
        components.insert(
            ComponentID(4),
            ComponentDescriptor::UniqueLang { enabled: true },
        );
        components.insert(
            ComponentID(5),
            ComponentDescriptor::Equals {
                property: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:altName")),
            },
        );
        components.insert(
            ComponentID(6),
            ComponentDescriptor::Disjoint {
                property: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:banned")),
            },
        );
        components.insert(
            ComponentID(7),
            ComponentDescriptor::LessThan {
                property: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:maxValue",
                )),
            },
        );
        components.insert(
            ComponentID(8),
            ComponentDescriptor::LessThanOrEquals {
                property: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:maxInclusive",
                )),
            },
        );

        let mut node_shape_terms = HashMap::new();
        node_shape_terms.insert(
            ID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person",
            )),
        );
        let mut property_shape_terms = HashMap::new();
        property_shape_terms.insert(
            PropShapeID(7),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person-name",
            )),
        );

        let shape_ir = ShapeIR {
            shape_graph: oxigraph::model::NamedNode::new_unchecked("urn:shape-graph"),
            data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:data-graph")),
            node_shapes: vec![NodeShapeIR {
                id: ID(1),
                targets: vec![Target::Class(Term::NamedNode(
                    oxigraph::model::NamedNode::new_unchecked("urn:Person"),
                ))],
                constraints: vec![ComponentID(1), ComponentID(2)],
                property_shapes: vec![PropShapeID(7)],
                severity: Severity::Violation,
                deactivated: false,
            }],
            property_shapes: vec![PropertyShapeIR {
                id: PropShapeID(7),
                targets: Vec::new(),
                path: Path::Simple(Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:name",
                ))),
                path_term: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:name")),
                constraints: vec![
                    ComponentID(3),
                    ComponentID(4),
                    ComponentID(5),
                    ComponentID(6),
                    ComponentID(7),
                    ComponentID(8),
                ],
                severity: Severity::Violation,
                deactivated: false,
            }],
            components,
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            node_shape_terms,
            property_shape_terms,
            shape_quads: Vec::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            features: FeatureToggles::default(),
        };

        let lowered = lower_shape_ir(&shape_ir).unwrap();
        assert!(lowered.meta.specialization_ready);
        assert!(lowered.fallback_annotations.is_empty());
        assert!(lowered
            .components
            .iter()
            .all(|component| !component.fallback_only));
        assert!(lowered.node_shapes[0].supported);
        assert!(lowered.property_shapes[0].supported);
    }

    #[test]
    fn specialization_ready_for_phase2_value_range_and_membership_constraints() {
        let mut components = HashMap::new();
        components.insert(
            ComponentID(1),
            ComponentDescriptor::Class {
                class: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:Person")),
            },
        );
        components.insert(
            ComponentID(2),
            ComponentDescriptor::Property {
                shape: PropShapeID(7),
            },
        );
        components.insert(
            ComponentID(3),
            ComponentDescriptor::MinInclusive {
                value: Term::Literal(oxigraph::model::Literal::from(10)),
            },
        );
        components.insert(
            ComponentID(4),
            ComponentDescriptor::MaxExclusive {
                value: Term::Literal(oxigraph::model::Literal::from(20)),
            },
        );
        components.insert(
            ComponentID(5),
            ComponentDescriptor::HasValue {
                value: Term::Literal(oxigraph::model::Literal::from(15)),
            },
        );
        components.insert(
            ComponentID(6),
            ComponentDescriptor::In {
                values: vec![
                    Term::Literal(oxigraph::model::Literal::from(14)),
                    Term::Literal(oxigraph::model::Literal::from(15)),
                    Term::Literal(oxigraph::model::Literal::from(16)),
                ],
            },
        );

        let mut node_shape_terms = HashMap::new();
        node_shape_terms.insert(
            ID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person",
            )),
        );
        let mut property_shape_terms = HashMap::new();
        property_shape_terms.insert(
            PropShapeID(7),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person-age",
            )),
        );

        let shape_ir = ShapeIR {
            shape_graph: oxigraph::model::NamedNode::new_unchecked("urn:shape-graph"),
            data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:data-graph")),
            node_shapes: vec![NodeShapeIR {
                id: ID(1),
                targets: vec![Target::Class(Term::NamedNode(
                    oxigraph::model::NamedNode::new_unchecked("urn:Person"),
                ))],
                constraints: vec![ComponentID(1), ComponentID(2)],
                property_shapes: vec![PropShapeID(7)],
                severity: Severity::Violation,
                deactivated: false,
            }],
            property_shapes: vec![PropertyShapeIR {
                id: PropShapeID(7),
                targets: Vec::new(),
                path: Path::Simple(Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:age",
                ))),
                path_term: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:age")),
                constraints: vec![
                    ComponentID(3),
                    ComponentID(4),
                    ComponentID(5),
                    ComponentID(6),
                ],
                severity: Severity::Violation,
                deactivated: false,
            }],
            components,
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            node_shape_terms,
            property_shape_terms,
            shape_quads: Vec::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            features: FeatureToggles::default(),
        };

        let lowered = lower_shape_ir(&shape_ir).unwrap();
        assert!(lowered.meta.specialization_ready);
        assert!(lowered.fallback_annotations.is_empty());
        assert!(lowered
            .components
            .iter()
            .all(|component| !component.fallback_only));
        assert!(lowered.node_shapes[0].supported);
        assert!(lowered.property_shapes[0].supported);
    }

    #[test]
    fn specialization_ready_for_phase2_closed_and_qualified_value_shape_constraints() {
        let mut components = HashMap::new();
        components.insert(
            ComponentID(1),
            ComponentDescriptor::Class {
                class: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:Person")),
            },
        );
        components.insert(
            ComponentID(2),
            ComponentDescriptor::Property {
                shape: PropShapeID(7),
            },
        );
        components.insert(
            ComponentID(3),
            ComponentDescriptor::Closed {
                closed: true,
                ignored_properties: vec![
                    Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:ignored")),
                    Term::BlankNode(oxigraph::model::BlankNode::new_unchecked("ignored-bnode")),
                ],
            },
        );
        components.insert(
            ComponentID(4),
            ComponentDescriptor::QualifiedValueShape {
                shape: ID(2),
                min_count: Some(1),
                max_count: Some(2),
                disjoint: Some(true),
            },
        );
        components.insert(
            ComponentID(5),
            ComponentDescriptor::QualifiedValueShape {
                shape: ID(3),
                min_count: None,
                max_count: Some(1),
                disjoint: Some(true),
            },
        );

        let mut node_shape_terms = HashMap::new();
        node_shape_terms.insert(
            ID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person",
            )),
        );
        node_shape_terms.insert(
            ID(2),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:shape:child")),
        );
        node_shape_terms.insert(
            ID(3),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:alt-child",
            )),
        );
        let mut property_shape_terms = HashMap::new();
        property_shape_terms.insert(
            PropShapeID(7),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person-child",
            )),
        );

        let shape_ir = ShapeIR {
            shape_graph: oxigraph::model::NamedNode::new_unchecked("urn:shape-graph"),
            data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:data-graph")),
            node_shapes: vec![
                NodeShapeIR {
                    id: ID(1),
                    targets: vec![Target::Class(Term::NamedNode(
                        oxigraph::model::NamedNode::new_unchecked("urn:Person"),
                    ))],
                    constraints: vec![ComponentID(1), ComponentID(2), ComponentID(3)],
                    property_shapes: vec![PropShapeID(7)],
                    severity: Severity::Violation,
                    deactivated: false,
                },
                NodeShapeIR {
                    id: ID(2),
                    targets: vec![Target::Class(Term::NamedNode(
                        oxigraph::model::NamedNode::new_unchecked("urn:Child"),
                    ))],
                    constraints: Vec::new(),
                    property_shapes: Vec::new(),
                    severity: Severity::Violation,
                    deactivated: false,
                },
                NodeShapeIR {
                    id: ID(3),
                    targets: vec![Target::Class(Term::NamedNode(
                        oxigraph::model::NamedNode::new_unchecked("urn:AltChild"),
                    ))],
                    constraints: Vec::new(),
                    property_shapes: Vec::new(),
                    severity: Severity::Violation,
                    deactivated: false,
                },
            ],
            property_shapes: vec![PropertyShapeIR {
                id: PropShapeID(7),
                targets: Vec::new(),
                path: Path::Simple(Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:child",
                ))),
                path_term: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:child")),
                constraints: vec![ComponentID(4), ComponentID(5)],
                severity: Severity::Violation,
                deactivated: false,
            }],
            components,
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            node_shape_terms,
            property_shape_terms,
            shape_quads: Vec::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            features: FeatureToggles::default(),
        };

        let lowered = lower_shape_ir(&shape_ir).unwrap();
        assert!(lowered.meta.specialization_ready);
        assert!(lowered.fallback_annotations.is_empty());
        assert!(lowered
            .components
            .iter()
            .all(|component| !component.fallback_only));
        assert!(lowered.node_shapes.iter().all(|shape| shape.supported));
        assert!(lowered.property_shapes[0].supported);
    }

    #[test]
    fn specialization_ready_for_phase2_logical_and_node_constraints() {
        let mut components = HashMap::new();
        components.insert(ComponentID(1), ComponentDescriptor::Node { shape: ID(2) });
        components.insert(ComponentID(2), ComponentDescriptor::Not { shape: ID(2) });
        components.insert(
            ComponentID(3),
            ComponentDescriptor::And {
                shapes: vec![ID(2), ID(3)],
            },
        );
        components.insert(
            ComponentID(4),
            ComponentDescriptor::Or {
                shapes: vec![ID(2), ID(3)],
            },
        );
        components.insert(
            ComponentID(5),
            ComponentDescriptor::Xone {
                shapes: vec![ID(2), ID(3)],
            },
        );

        let mut node_shape_terms = HashMap::new();
        node_shape_terms.insert(
            ID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person",
            )),
        );
        node_shape_terms.insert(
            ID(2),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:shape:child")),
        );
        node_shape_terms.insert(
            ID(3),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:alt-child",
            )),
        );
        let mut property_shape_terms = HashMap::new();
        property_shape_terms.insert(
            PropShapeID(7),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person-child",
            )),
        );

        let shape_ir = ShapeIR {
            shape_graph: oxigraph::model::NamedNode::new_unchecked("urn:shape-graph"),
            data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:data-graph")),
            node_shapes: vec![
                NodeShapeIR {
                    id: ID(1),
                    targets: vec![Target::Class(Term::NamedNode(
                        oxigraph::model::NamedNode::new_unchecked("urn:Person"),
                    ))],
                    constraints: Vec::new(),
                    property_shapes: vec![PropShapeID(7)],
                    severity: Severity::Violation,
                    deactivated: false,
                },
                NodeShapeIR {
                    id: ID(2),
                    targets: vec![Target::Class(Term::NamedNode(
                        oxigraph::model::NamedNode::new_unchecked("urn:Child"),
                    ))],
                    constraints: Vec::new(),
                    property_shapes: Vec::new(),
                    severity: Severity::Violation,
                    deactivated: false,
                },
                NodeShapeIR {
                    id: ID(3),
                    targets: vec![Target::Class(Term::NamedNode(
                        oxigraph::model::NamedNode::new_unchecked("urn:AltChild"),
                    ))],
                    constraints: Vec::new(),
                    property_shapes: Vec::new(),
                    severity: Severity::Violation,
                    deactivated: false,
                },
            ],
            property_shapes: vec![PropertyShapeIR {
                id: PropShapeID(7),
                targets: Vec::new(),
                path: Path::Simple(Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:child",
                ))),
                path_term: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:child")),
                constraints: vec![
                    ComponentID(1),
                    ComponentID(2),
                    ComponentID(3),
                    ComponentID(4),
                    ComponentID(5),
                ],
                severity: Severity::Violation,
                deactivated: false,
            }],
            components,
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            node_shape_terms,
            property_shape_terms,
            shape_quads: Vec::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            features: FeatureToggles::default(),
        };

        let lowered = lower_shape_ir(&shape_ir).unwrap();
        assert!(lowered.meta.specialization_ready);
        assert!(lowered.fallback_annotations.is_empty());
        assert!(lowered
            .components
            .iter()
            .all(|component| !component.fallback_only));
        assert!(lowered.property_shapes[0].supported);
    }

    #[test]
    fn specialization_ready_for_phase2_nodekind_and_text_constraints() {
        let mut components = HashMap::new();
        components.insert(
            ComponentID(1),
            ComponentDescriptor::Class {
                class: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:Person")),
            },
        );
        components.insert(
            ComponentID(2),
            ComponentDescriptor::Property {
                shape: PropShapeID(7),
            },
        );
        components.insert(
            ComponentID(3),
            ComponentDescriptor::NodeKind {
                node_kind: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "http://www.w3.org/ns/shacl#Literal",
                )),
            },
        );
        components.insert(ComponentID(4), ComponentDescriptor::MinLength { length: 3 });
        components.insert(
            ComponentID(5),
            ComponentDescriptor::MaxLength { length: 16 },
        );
        components.insert(
            ComponentID(6),
            ComponentDescriptor::Pattern {
                pattern: "^[A-Za-z]+$".to_string(),
                flags: Some("i".to_string()),
            },
        );

        let mut node_shape_terms = HashMap::new();
        node_shape_terms.insert(
            ID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person",
            )),
        );
        let mut property_shape_terms = HashMap::new();
        property_shape_terms.insert(
            PropShapeID(7),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                "urn:shape:person-name",
            )),
        );

        let shape_ir = ShapeIR {
            shape_graph: oxigraph::model::NamedNode::new_unchecked("urn:shape-graph"),
            data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:data-graph")),
            node_shapes: vec![NodeShapeIR {
                id: ID(1),
                targets: vec![Target::Class(Term::NamedNode(
                    oxigraph::model::NamedNode::new_unchecked("urn:Person"),
                ))],
                constraints: vec![ComponentID(1), ComponentID(2)],
                property_shapes: vec![PropShapeID(7)],
                severity: Severity::Violation,
                deactivated: false,
            }],
            property_shapes: vec![PropertyShapeIR {
                id: PropShapeID(7),
                targets: Vec::new(),
                path: Path::Simple(Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "urn:name",
                ))),
                path_term: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:name")),
                constraints: vec![
                    ComponentID(3),
                    ComponentID(4),
                    ComponentID(5),
                    ComponentID(6),
                ],
                severity: Severity::Violation,
                deactivated: false,
            }],
            components,
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            node_shape_terms,
            property_shape_terms,
            shape_quads: Vec::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            features: FeatureToggles::default(),
        };

        let lowered = lower_shape_ir(&shape_ir).unwrap();
        assert!(lowered.meta.specialization_ready);
        assert!(lowered.fallback_annotations.is_empty());
        assert!(lowered
            .components
            .iter()
            .all(|component| !component.fallback_only));
        assert!(lowered.node_shapes[0].supported);
        assert!(lowered.property_shapes[0].supported);
    }

    #[test]
    fn specialization_ready_for_phase2_node_datatype_and_membership_constraints() {
        let mut components = HashMap::new();
        components.insert(
            ComponentID(1),
            ComponentDescriptor::Datatype {
                datatype: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked(
                    "http://www.w3.org/2001/XMLSchema#string",
                )),
            },
        );
        components.insert(
            ComponentID(2),
            ComponentDescriptor::HasValue {
                value: Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:required")),
            },
        );
        components.insert(
            ComponentID(3),
            ComponentDescriptor::In {
                values: vec![
                    Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:required")),
                    Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:alternate")),
                ],
            },
        );

        let mut node_shape_terms = HashMap::new();
        node_shape_terms.insert(
            ID(1),
            Term::NamedNode(oxigraph::model::NamedNode::new_unchecked("urn:shape:item")),
        );

        let shape_ir = ShapeIR {
            shape_graph: oxigraph::model::NamedNode::new_unchecked("urn:shape-graph"),
            data_graph: Some(oxigraph::model::NamedNode::new_unchecked("urn:data-graph")),
            node_shapes: vec![NodeShapeIR {
                id: ID(1),
                targets: vec![Target::Class(Term::NamedNode(
                    oxigraph::model::NamedNode::new_unchecked("urn:Item"),
                ))],
                constraints: vec![ComponentID(1), ComponentID(2), ComponentID(3)],
                property_shapes: Vec::new(),
                severity: Severity::Violation,
                deactivated: false,
            }],
            property_shapes: Vec::new(),
            components,
            component_templates: HashMap::new(),
            shape_templates: HashMap::new(),
            shape_template_cache: HashMap::new(),
            node_shape_terms,
            property_shape_terms: HashMap::new(),
            shape_quads: Vec::new(),
            rules: HashMap::new(),
            node_shape_rules: HashMap::new(),
            prop_shape_rules: HashMap::new(),
            features: FeatureToggles::default(),
        };

        let lowered = lower_shape_ir(&shape_ir).unwrap();
        assert!(lowered.meta.specialization_ready);
        assert!(lowered.fallback_annotations.is_empty());
        assert!(lowered
            .components
            .iter()
            .all(|component| !component.fallback_only));
        assert!(lowered.node_shapes[0].supported);
    }
}
