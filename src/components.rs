use oxigraph::model::{Term, NamedNode, BlankNode, Literal, SubjectRef};
use oxigraph::model::{Dataset, Graph, GraphName, Quad, Triple};
use crate::types::ID;

impl<'a> From<&'a Term> for SubjectRef<'a> {
    fn from(term: &'a Term) -> SubjectRef<'a> {
        match term {
            Term::NamedNode(n) => SubjectRef::NamedNodeRef(n.into()),
            Term::BlankNode(b)   => SubjectRef::BlankNodeRef(b.into()),
            _ => panic!("Invalid subject term: {:?}", term),
        }
    }
}

pub fn parse_components(start: Term, shape_graph: &Graph) -> Vec<Component> {
    let mut components = Vec::new();
    // Class constraints
    if let Some(class_term) = shape_graph.object_for_subject_predicate(
        &start,
        NamedNode::new("http://www.w3.org/ns/shacl#class").unwrap(),
    ) {
        components.push(Component::ClassConstraint(ClassConstraintComponent {
            class: class_term.into(),
        }));
    }
    // Node constraints
    if let Some(shape_term) = shape_graph.object_for_subject_predicate(
        &start,
        NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap(),
    ) {
        components.push(Component::NodeConstraint(NodeConstraintComponent {
            shape: shape_term.into(),
        }));
    }
    // Property constraints
    for prop in shape_graph.objects_for_subject_predicate(
        &start,
        NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap(),
    ) {
        components.push(Component::PropertyConstraint(PropertyConstraintComponent {
            shape: prop.into(),
        }));
    }
    // Qualified value shape constraints
    for qvs in shape_graph.objects_for_subject_predicate(
        &start,
        NamedNode::new("http://www.w3.org/ns/shacl#qualifiedValueShape").unwrap(),
    ) {
        let min_count = shape_graph
            .object_for_subject_predicate(
                &qvs,
                NamedNode::new("http://www.w3.org/ns/shacl#minCount").unwrap(),
            )
            .and_then(|t| t.into_literal().and_then(|lit| lit.value().parse().ok()));
        let max_count = shape_graph
            .object_for_subject_predicate(
                &qvs,
                NamedNode::new("http://www.w3.org/ns/shacl#maxCount").unwrap(),
            )
            .and_then(|t| t.into_literal().and_then(|lit| lit.value().parse().ok()));
        let disjoint = shape_graph
            .object_for_subject_predicate(
                &qvs,
                NamedNode::new("http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint")
                    .unwrap(),
            )
            .and_then(|t| t.into_literal().and_then(|lit| lit.value().parse().ok()));
        components.push(Component::QualifiedValueShape(
            QualifiedValueShapeComponent {
                shape: qvs.into(),
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
