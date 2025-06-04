use oxigraph::model::{Term, NamedNode, BlankNode, Literal};
use oxigraph::model::{Dataset, Graph, GraphName, Quad, Triple};
use crate::types::ID;

pub fn parse_components(start: Term, shape_graph: &Graph) -> Vec<Component> {
    use oxigraph::model::{NamedNode, Literal};
    let mut components = Vec::new();
    let class_pred = NamedNode::new("http://www.w3.org/ns/shacl#class").unwrap();
    let node_pred = NamedNode::new("http://www.w3.org/ns/shacl#node").unwrap();
    let prop_pred = NamedNode::new("http://www.w3.org/ns/shacl#property").unwrap();
    let qv_pred = NamedNode::new("http://www.w3.org/ns/shacl#qualifiedValueShape").unwrap();
    let min_count_pred = NamedNode::new("http://www.w3.org/ns/shacl#minCount").unwrap();
    let max_count_pred = NamedNode::new("http://www.w3.org/ns/shacl#maxCount").unwrap();
    let disjoint_pred = NamedNode::new("http://www.w3.org/ns/shacl#defaultDisjoint").unwrap();

    for triple in shape_graph.triples_for_subject(&start) {
        let pred = &triple.predicate;
        let obj = triple.object.clone();
        if pred == &class_pred {
            components.push(Component::ClassConstraint(ClassConstraintComponent {
                identifier: start.clone().into(),
                class: obj,
            }));
        } else if pred == &node_pred {
            components.push(Component::NodeConstraint(NodeConstraintComponent {
                identifier: start.clone().into(),
                shape: obj.clone().into(),
            }));
        } else if pred == &prop_pred {
            components.push(Component::PropertyConstraint(PropertyConstraintComponent {
                identifier: start.clone().into(),
                shape: obj.clone().into(),
            }));
        } else if pred == &qv_pred {
            let mut min_count = None;
            let mut max_count = None;
            let mut disjoint = None;
            for nested in shape_graph.triples_for_subject(&obj) {
                let np = &nested.predicate;
                let no = nested.object.clone();
                if np == &min_count_pred {
                    if let Literal { value, .. } = no {
                        if let Ok(n) = value.parse::<u64>() {
                            min_count = Some(n);
                        }
                    }
                } else if np == &max_count_pred {
                    if let Literal { value, .. } = no {
                        if let Ok(n) = value.parse::<u64>() {
                            max_count = Some(n);
                        }
                    }
                } else if np == &disjoint_pred {
                    if let Literal { value, .. } = no {
                        if let Ok(b) = value.parse::<bool>() {
                            disjoint = Some(b);
                        }
                    }
                }
            }
            components.push(Component::QualifiedValueShape(
                QualifiedValueShapeComponent {
                    identifier: start.clone().into(),
                    shape: obj.clone().into(),
                    min_count,
                    max_count,
                    disjoint,
                },
            ));
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
    identifier: ID,
    class: Term,
}

pub struct NodeConstraintComponent {
    identifier: ID,
    shape: ID,
}

pub struct PropertyConstraintComponent {
    identifier: ID,
    shape: ID,
}

pub struct QualifiedValueShapeComponent {
    identifier: ID,
    shape: ID,
    min_count: Option<u64>,
    max_count: Option<u64>,
    disjoint: Option<bool>,
}
