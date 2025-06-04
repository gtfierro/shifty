use oxigraph::model::NamedNodeRef;

pub struct SHACL {
    pub class: NamedNodeRef<'static>,
    pub node: NamedNodeRef<'static>,
    pub property: NamedNodeRef<'static>,
    pub qualified_value_shape: NamedNodeRef<'static>,
    pub min_count: NamedNodeRef<'static>,
    pub max_count: NamedNodeRef<'static>,
    pub qualified_value_shapes_disjoint: NamedNodeRef<'static>,
    pub node_kind: NamedNodeRef<'static>,
    pub datatype: NamedNodeRef<'static>,
    pub min_exclusive: NamedNodeRef<'static>,
    pub min_inclusive: NamedNodeRef<'static>,
    pub max_exclusive: NamedNodeRef<'static>,
    pub max_inclusive: NamedNodeRef<'static>,
    pub min_length: NamedNodeRef<'static>,
    pub max_length: NamedNodeRef<'static>,
    pub pattern: NamedNodeRef<'static>,
    pub flags: NamedNodeRef<'static>,
    pub language_in: NamedNodeRef<'static>,
    pub unique_lang: NamedNodeRef<'static>,
    pub rdf_first: NamedNodeRef<'static>,
    pub rdf_rest: NamedNodeRef<'static>,
    pub rdf_nil: NamedNodeRef<'static>,
    pub node_shape: NamedNodeRef<'static>,
}

impl SHACL {
    pub fn new() -> Self {
        SHACL {
            class: NamedNodeRef::new("http://www.w3.org/ns/shacl#class").unwrap(),
            node: NamedNodeRef::new("http://www.w3.org/ns/shacl#node").unwrap(),
            property: NamedNodeRef::new("http://www.w3.org/ns/shacl#property").unwrap(),
            qualified_value_shape: NamedNodeRef::new("http://www.w3.org/ns/shacl#qualifiedValueShape").unwrap(),
            min_count: NamedNodeRef::new("http://www.w3.org/ns/shacl#minCount").unwrap(),
            max_count: NamedNodeRef::new("http://www.w3.org/ns/shacl#maxCount").unwrap(),
            qualified_value_shapes_disjoint: NamedNodeRef::new("http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint").unwrap(),
            node_kind: NamedNodeRef::new("http://www.w3.org/ns/shacl#nodeKind").unwrap(),
            datatype: NamedNodeRef::new("http://www.w3.org/ns/shacl#datatype").unwrap(),
            min_exclusive: NamedNodeRef::new("http://www.w3.org/ns/shacl#minExclusive").unwrap(),
            min_inclusive: NamedNodeRef::new("http://www.w3.org/ns/shacl#minInclusive").unwrap(),
            max_exclusive: NamedNodeRef::new("http://www.w3.org/ns/shacl#maxExclusive").unwrap(),
            max_inclusive: NamedNodeRef::new("http://www.w3.org/ns/shacl#maxInclusive").unwrap(),
            min_length: NamedNodeRef::new("http://www.w3.org/ns/shacl#minLength").unwrap(),
            max_length: NamedNodeRef::new("http://www.w3.org/ns/shacl#maxLength").unwrap(),
            pattern: NamedNodeRef::new("http://www.w3.org/ns/shacl#pattern").unwrap(),
            flags: NamedNodeRef::new("http://www.w3.org/ns/shacl#flags").unwrap(),
            language_in: NamedNodeRef::new("http://www.w3.org/ns/shacl#languageIn").unwrap(),
            unique_lang: NamedNodeRef::new("http://www.w3.org/ns/shacl#uniqueLang").unwrap(),
            rdf_first: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#first").unwrap(),
            rdf_rest: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest").unwrap(),
            rdf_nil: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil").unwrap(),
            node_shape: NamedNodeRef::new("http://www.w3.org/ns/shacl#NodeShape").unwrap(),
        }
    }
}

pub struct RDF {
    pub type_: NamedNodeRef<'static>,
    pub subject: NamedNodeRef<'static>,
    pub predicate: NamedNodeRef<'static>,
    pub object: NamedNodeRef<'static>,
    pub first: NamedNodeRef<'static>,
    pub rest: NamedNodeRef<'static>,
    pub nil: NamedNodeRef<'static>,
}

impl RDF {
    pub fn new() -> Self {
        RDF {
            type_: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").unwrap(),
            subject: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#subject").unwrap(),
            predicate: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate").unwrap(),
            object: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#object").unwrap(),
            first: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#first").unwrap(),
            rest: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest").unwrap(),
            nil: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil").unwrap(),
        }
    }
}
