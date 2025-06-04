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
        }
    }
}
