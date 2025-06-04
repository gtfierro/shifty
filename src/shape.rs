use crate::types::ID;
use crate::types::Path;
use crate::named_nodes::SHACL;
use oxigraph::model::{Term, NamedNode, TermRef};

pub enum Shape {
    NodeShape(NodeShape),
    PropertyShape(PropertyShape),
}

pub struct NodeShape {
    identifier: ID,
    targets: Vec<ID>,
    property_shapes: Vec<ID>,
    constraints: Vec<ID>,
    // TODO severity
    // TODO message
}

pub struct PropertyShape {
    identifier: ID,
    path: Path,
    constraints: Vec<ID>,
    // TODO severity
    // TODO message
}

pub enum Target {
    Class(Term),
    Node(Term),
    SubjectsOf(NamedNode),
    ObjectsOf(NamedNode),
}

impl Target {
    pub fn from_predicate_object(predicate: TermRef, object: TermRef) -> Self {
        let shacl = SHACL::new();
        match predicate {
            shacl.target_class => Target::Class(object.into_owned()),
            shacl.target_node => Target::Node(object.into_owned()),
            shacl.target_subjects_of => {
                if let TermRef::NamedNode(nn) = object {
                    Target::SubjectsOf(nn.into_owned())
                } else {
                    panic!("Expected NamedNode for target_subjects_of")
                }
            },
            shacl.target_objects_of => {
                if let TermRef::NamedNode(nn) = object {
                    Target::ObjectsOf(nn.into_owned())
                } else {
                    panic!("Expected NamedNode for target_objects_of")
                }
            },
            _ => panic!("Unknown predicate for target: {:?}", predicate),
        }
    }
}
