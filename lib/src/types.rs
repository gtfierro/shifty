use crate::named_nodes::SHACL;
use oxigraph::model::{NamedNodeRef, Term, TermRef}; // Removed NamedNode
use std::fmt; // Added for Display trait
use std::hash::Hash; // Added Hash for derived traits

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct ID(pub u64);

impl From<u64> for ID {
    fn from(item: u64) -> Self {
        ID(item)
    }
}

impl fmt::Display for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ID {
    pub fn to_graphviz_id(&self) -> String {
        format!("n{}", self.0)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct ComponentID(pub u64);

impl From<u64> for ComponentID {
    fn from(item: u64) -> Self {
        ComponentID(item)
    }
}

impl fmt::Display for ComponentID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ComponentID {
    pub fn to_graphviz_id(&self) -> String {
        format!("c{}", self.0)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct PropShapeID(pub u64);

impl From<u64> for PropShapeID {
    fn from(item: u64) -> Self {
        PropShapeID(item)
    }
}

impl fmt::Display for PropShapeID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PropShapeID {
    pub fn to_graphviz_id(&self) -> String {
        format!("p{}", self.0)
    }
}

#[derive(Debug)]
pub enum Path {
    Simple(Term),
}

#[derive(Debug)]
pub enum Target {
    Class(Term),
    Node(Term),
    SubjectsOf(Term),
    ObjectsOf(Term),
}

impl Target {
    pub fn from_predicate_object(predicate: NamedNodeRef, object: TermRef) -> Option<Self> {
        let shacl = SHACL::new();
        if predicate == shacl.target_class {
            Some(Target::Class(object.into_owned()))
        } else if predicate == shacl.target_node {
            Some(Target::Node(object.into_owned()))
        } else if predicate == shacl.target_subjects_of {
            Some(Target::SubjectsOf(object.into_owned()))
        } else if predicate == shacl.target_objects_of {
            Some(Target::ObjectsOf(object.into_owned()))
        } else {
            None
        }
    }
}
