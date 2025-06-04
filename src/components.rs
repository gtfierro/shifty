use oxigraph::model::{Term, NamedNodeRef, TermRef, Literal};
use oxigraph::model::{Dataset, Graph, GraphName, Quad, Triple};
use crate::types::ID;


pub fn parse_components(start: Term, shape_graph: &Graph) -> Vec<Component> {
    let mut components = Vec::new();
    // Class constraints
    if let Some(class_term) = shape_graph.object_for_subject_predicate(
        &start,
        NamedNodeRef::new("http://www.w3.org/ns/shacl#class").unwrap(),
    ) {
        components.push(Component::ClassConstraint(ClassConstraintComponent {
            class: class_term.to_owned(),
        }));
    }
    // Node constraints
    if let Some(shape_term) = shape_graph.object_for_subject_predicate(
        &start,
        NamedNodeRef::new("http://www.w3.org/ns/shacl#node").unwrap(),
    ) {
        components.push(Component::NodeConstraint(NodeConstraintComponent {
            shape: match shape_term {
                TermRef::Literal(lit) => lit.value().parse().unwrap(),
                _ => panic!("Invalid shape term: {:?}", shape_term),
            },
        }));
    }
    // Property constraints
    for prop in shape_graph.objects_for_subject_predicate(
        &start,
        NamedNodeRef::new("http://www.w3.org/ns/shacl#property").unwrap(),
    ) {
        components.push(Component::PropertyConstraint(PropertyConstraintComponent {
            shape: match prop {
                TermRef::Literal(lit) => lit.value().parse().unwrap(),
                _ => panic!("Invalid property shape term: {:?}", prop),
            },
        }));
    }
    // Qualified value shape constraints
    for qvs in shape_graph.objects_for_subject_predicate(
        &start,
        NamedNodeRef::new("http://www.w3.org/ns/shacl#qualifiedValueShape").unwrap(),
    ) {
        let min_count = shape_graph
            .object_for_subject_predicate(
                &qvs.to_owned(),
                NamedNodeRef::new("http://www.w3.org/ns/shacl#minCount").unwrap(),
            )
            .and_then(|t| if let TermRef::Literal(lit) = t { lit.value().parse().ok() } else { None });
        let max_count = shape_graph
            .object_for_subject_predicate(
                &qvs.to_owned(),
                NamedNodeRef::new("http://www.w3.org/ns/shacl#maxCount").unwrap(),
            )
            .and_then(|t| if let TermRef::Literal(lit) = t { lit.value().parse().ok() } else { None });
        let disjoint = shape_graph
            .object_for_subject_predicate(
                &qvs.to_owned(),
                NamedNodeRef::new("http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint")
                    .unwrap(),
            )
            .and_then(|t| if let TermRef::Literal(lit) = t { lit.value().parse().ok() } else { None });
        components.push(Component::QualifiedValueShape(
            QualifiedValueShapeComponent {
                shape: match qvs {
                    TermRef::Literal(lit) => lit.value().parse().unwrap(),
                    _ => panic!("Invalid qualified value shape term: {:?}", qvs),
                },
                min_count,
                max_count,
                disjoint,
            },
        ));
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
