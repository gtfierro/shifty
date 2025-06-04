use oxigraph::model::{Term, NamedNode};

pub type ID = u64;

pub enum Path {
    Simple(Term),
}

pub enum Target {
    Class(Term),
    Node(Term),
    SubjectsOf(NamedNode),
    ObjectsOf(NamedNode),
}

