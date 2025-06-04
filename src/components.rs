use oxigraph::model::{Term, NamedNode, BlankNode, Literal};
use oxigraph::model::{Dataset, Graph, GraphName, Quad, Triple};
use crate::types::ID;

pub fn parse_components(start: Term, shape_graph: &Graph) -> Vec<Component> {
    // SHACL predicate IRIs
    let sh_class = NamedNode::new("http://www.w3.org/ns/shacl#class").unwrap();
    let sh_node = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let sh_property = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let sh_qvs = NamedNode::new("http://www.w3.org/ns/shacl#qualifiedValueShape").unwrap();
    let sh_min_count = NamedNode::new("http://www.w3.org/ns/shacl#minCount").unwrap();
    let sh_max_count = NamedNode::new("http://www.w3.org/ns/shacl#maxCount").unwrap();
    let sh_disjoint = NamedNode::new("http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint").unwrap();

    let mut components = Vec::new();

    // ClassConstraint components
    for t in shape_graph.triples_with_subject_predicate(&start, &sh_class) {
        if let Ok(triple) = t {
            components.push(Component::ClassConstraint(
                ClassConstraintComponent { class: triple.object.clone() }
            ));
        }
    }

    // NodeConstraint components
    for t in shape_graph.triples_with_subject_predicate(&start, &sh_node) {
        if let Ok(triple) = t {
            if let Term::NamedNode(n) | Term::BlankNode(n) = &triple.object {
                components.push(Component::NodeConstraint(
                    NodeConstraintComponent { shape: ID::from(n.clone()) }
                ));
            }
        }
    }

    // PropertyConstraint components
    for t in shape_graph.triples_with_subject_predicate(&start, &sh_property) {
        if let Ok(triple) = t {
            if let Term::NamedNode(n) | Term::BlankNode(n) = &triple.object {
                components.push(Component::PropertyConstraint(
                    PropertyConstraintComponent { shape: ID::from(n.clone()) }
                ));
            }
        }
    }

    // QualifiedValueShape components
    for t in shape_graph.triples_with_subject_predicate(&start, &sh_qvs) {
        if let Ok(triple) = t {
            if let Term::NamedNode(n) | Term::BlankNode(n) = &triple.object {
                let shape_id = ID::from(n.clone());
                // Optional parameters
                let min_count = shape_graph
                    .triples_with_subject_predicate(&triple.object, &sh_min_count)
                    .filter_map(|r| r.ok())
                    .filter_map(|tr| if let Term::Literal(lit) = tr.object { lit.value().parse().ok() } else { None })
                    .next();
                let max_count = shape_graph
                    .triples_with_subject_predicate(&triple.object, &sh_max_count)
                    .filter_map(|r| r.ok())
                    .filter_map(|tr| if let Term::Literal(lit) = tr.object { lit.value().parse().ok() } else { None })
                    .next();
                let disjoint = shape_graph
                    .triples_with_subject_predicate(&triple.object, &sh_disjoint)
                    .filter_map(|r| r.ok())
                    .filter_map(|tr| if let Term::Literal(lit) = tr.object { lit.value().parse().ok() } else { None })
                    .next();
                components.push(Component::QualifiedValueShape(
                    QualifiedValueShapeComponent {
                        shape: shape_id,
                        min_count,
                        max_count,
                        disjoint,
                    }
                ));
            }
        }
    }

    components
}

pub enum Component {
    ClassConstraint(ClassConstraintComponent),
    NodeConstraint(NodeConstraintComponent),
    PropertyConstraint(PropertyConstraintComponent),
    QualifiedValueShape(QualifiedValueShapeComponent),
}

pub struct ClassConstraintComponent {
    class: Term,
}

pub struct NodeConstraintComponent {
    shape: ID,
}

pub struct PropertyConstraintComponent {
    shape: ID,
}

pub struct QualifiedValueShapeComponent {
    shape: ID,
    min_count: Option<u64>,
    max_count: Option<u64>,
    disjoint: Option<bool>,
}
