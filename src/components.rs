use oxigraph::model::{Term, TermRef, SubjectRef, TripleRef};
use std::collections::HashMap;
use crate::context::ValidationContext;
use crate::types::ID;
use crate::named_nodes::SHACL;

trait ToSubjectRef {
    fn to_subject_ref(&self) -> SubjectRef;
}

impl ToSubjectRef for Term {
    fn to_subject_ref(&self) -> SubjectRef {
        match self {
            Term::NamedNode(n) => n.into(),
            Term::BlankNode(b) => b.into(),
            _ => panic!("Invalid subject term: {:?}", self),
        }
    }
}

impl<'a> ToSubjectRef for TermRef<'a> {
    fn to_subject_ref(&self) -> SubjectRef<'a> {
        match self {
            TermRef::NamedNode(nr) => nr.clone().into(),
            TermRef::BlankNode(br) => br.clone().into(),
            _ => panic!("Invalid subject term ref: {:?}", self),
        }
    }
}


pub fn parse_components(start: Term, context: &mut ValidationContext) -> Vec<Component> {
    let mut components = Vec::new();
    let shacl = SHACL::new();

    // make a list of all the predicate-object pairs for the given start term, as a dictionary
    let pred_obj_pairs: HashMap<TermRef, Vec<TermRef>> = context.shape_graph()
        .triples_for_subject(start.to_subject_ref())
        .fold(
            HashMap::new(),
            |mut acc, TripleRef{subject, predicate, object}| {
                let predicate: TermRef = predicate.into();
                let object: TermRef = object.into();
                if let Some(objects) = acc.get_mut(&predicate) {
                    objects.push(object);
                } else {
                    acc.insert(predicate.clone(), vec![object]);
                }
                acc
            },
        );

    // class constraint component
    if let Some(class_terms) = pred_obj_pairs.get(&shacl.class.into()) {
        for class_term in class_terms {
            components.push(Component::ClassConstraint(ClassConstraintComponent {
                class: class_term.clone().into(),
            }));
        }
    }

    // node constraint component
    if let Some(node_terms) = pred_obj_pairs.get(&shacl.node.into()) {
        for node_term in node_terms {
            components.push(Component::NodeConstraint(NodeConstraintComponent {
                shape: context.get_or_create_id(node_term.clone().into()),
            }));
        }
    }


    // Class constraints
    //if let Some(class_term) = context.shape_graph().object_for_subject_predicate(
    //    start.to_subject_ref(),
    //    shacl.class,
    //) {
    //    components.push(Component::ClassConstraint(ClassConstraintComponent {
    //        class: class_term.into(),
    //    }));
    //}
    //// Node constraints
    //if let Some(shape_term) = context.shape_graph().object_for_subject_predicate(
    //    start.to_subject_ref(),
    //    shacl.node,
    //) {
    //    components.push(Component::NodeConstraint(NodeConstraintComponent {
    //        shape: context.get_or_create_id(shape_term.clone().into()),
    //    }));
    //}
    //// Property constraints
    //for prop in context.shape_graph().objects_for_subject_predicate(
    //    start.to_subject_ref(),
    //    shacl.property,
    //) {
    //    components.push(Component::PropertyConstraint(PropertyConstraintComponent {
    //        shape: context.get_or_create_id(prop.clone().into()),
    //    }));
    //}
    //// Qualified value shape constraints
    //for qvs in context.shape_graph().objects_for_subject_predicate(
    //    start.to_subject_ref(),
    //    shacl.qualified_value_shape,
    //) {
    //    let min_count = context.shape_graph()
    //        .object_for_subject_predicate(
    //            qvs.to_subject_ref(),
    //            shacl.min_count,
    //        )
    //        .and_then(|t| if let TermRef::Literal(lit) = t { lit.value().parse().ok() } else { None });
    //    let max_count = context.shape_graph()
    //        .object_for_subject_predicate(
    //            qvs.to_subject_ref(),
    //            shacl.max_count,
    //        )
    //        .and_then(|t| if let TermRef::Literal(lit) = t { lit.value().parse().ok() } else { None });
    //    let disjoint = context.shape_graph()
    //        .object_for_subject_predicate(
    //            qvs.to_subject_ref(),
    //            shacl.qualified_value_shapes_disjoint,
    //        )
    //        .and_then(|t| if let TermRef::Literal(lit) = t { lit.value().parse().ok() } else { None });
    //    components.push(Component::QualifiedValueShape(
    //        QualifiedValueShapeComponent {
    //            shape: context.get_or_create_id(qvs.clone().into()),
    //            min_count,
    //            max_count,
    //            disjoint,
    //        },
    //    ));
    //}
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
