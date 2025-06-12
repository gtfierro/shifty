use oxigraph::model::NamedNodeRef;

pub struct SHACL {
    pub class: NamedNodeRef<'static>,
    pub node: NamedNodeRef<'static>,
    pub property: NamedNodeRef<'static>,
    pub qualified_value_shape: NamedNodeRef<'static>,
    pub qualified_min_count: NamedNodeRef<'static>,
    pub qualified_max_count: NamedNodeRef<'static>,
    pub min_count: NamedNodeRef<'static>,
    pub max_count: NamedNodeRef<'static>,
    pub qualified_value_shapes_disjoint: NamedNodeRef<'static>,
    pub not: NamedNodeRef<'static>,
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
    pub node_shape: NamedNodeRef<'static>,
    pub property_shape: NamedNodeRef<'static>,
    pub and_: NamedNodeRef<'static>,
    pub or_: NamedNodeRef<'static>,
    pub xone: NamedNodeRef<'static>,
    pub path: NamedNodeRef<'static>,
    pub inverse_path: NamedNodeRef<'static>,
    pub alternative_path: NamedNodeRef<'static>,
    pub sequence_path: NamedNodeRef<'static>,
    pub zero_or_more_path: NamedNodeRef<'static>,
    pub one_or_more_path: NamedNodeRef<'static>,
    pub zero_or_one_path: NamedNodeRef<'static>,

    pub target_class: NamedNodeRef<'static>,
    pub target_node: NamedNodeRef<'static>,
    pub target_objects_of: NamedNodeRef<'static>,
    pub target_subjects_of: NamedNodeRef<'static>,

    pub equals: NamedNodeRef<'static>,
    pub disjoint: NamedNodeRef<'static>,
    pub less_than: NamedNodeRef<'static>,
    pub less_than_or_equals: NamedNodeRef<'static>,

    pub closed: NamedNodeRef<'static>,
    pub ignored_properties: NamedNodeRef<'static>,
    pub has_value: NamedNodeRef<'static>,
    pub in_: NamedNodeRef<'static>, // `in` is a reserved keyword in Rust

    // NodeKind instances
    pub iri: NamedNodeRef<'static>,
    pub literal: NamedNodeRef<'static>,
    pub blank_node: NamedNodeRef<'static>,
    pub blank_node_or_iri: NamedNodeRef<'static>,
    pub blank_node_or_literal: NamedNodeRef<'static>,
    pub iri_or_literal: NamedNodeRef<'static>,

    // Severities
    pub severity: NamedNodeRef<'static>,
    pub info: NamedNodeRef<'static>,
    pub warning: NamedNodeRef<'static>,
    pub violation: NamedNodeRef<'static>,

    // SPARQL
    pub select: NamedNodeRef<'static>,
    pub deactivated: NamedNodeRef<'static>,
    pub message: NamedNodeRef<'static>,
    pub sparql: NamedNodeRef<'static>,

    // Validation Report
    pub validation_report: NamedNodeRef<'static>,
    pub conforms: NamedNodeRef<'static>,
    pub result: NamedNodeRef<'static>,
    pub validation_result: NamedNodeRef<'static>,
    pub focus_node: NamedNodeRef<'static>,
    pub result_path: NamedNodeRef<'static>,
    pub source_shape: NamedNodeRef<'static>,
    pub source_constraint_component: NamedNodeRef<'static>,
    pub result_message: NamedNodeRef<'static>,
    pub result_severity: NamedNodeRef<'static>,
}

impl SHACL {
    pub fn new() -> Self {
        SHACL {
            class: NamedNodeRef::new("http://www.w3.org/ns/shacl#class").unwrap(),
            node: NamedNodeRef::new("http://www.w3.org/ns/shacl#node").unwrap(),
            property: NamedNodeRef::new("http://www.w3.org/ns/shacl#property").unwrap(),
            qualified_value_shape: NamedNodeRef::new(
                "http://www.w3.org/ns/shacl#qualifiedValueShape",
            )
            .unwrap(),
            qualified_min_count: NamedNodeRef::new("http://www.w3.org/ns/shacl#qualifiedMinCount")
                .unwrap(),
            qualified_max_count: NamedNodeRef::new("http://www.w3.org/ns/shacl#qualifiedMaxCount")
                .unwrap(),
            min_count: NamedNodeRef::new("http://www.w3.org/ns/shacl#minCount").unwrap(),
            max_count: NamedNodeRef::new("http://www.w3.org/ns/shacl#maxCount").unwrap(),
            qualified_value_shapes_disjoint: NamedNodeRef::new(
                "http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint",
            )
            .unwrap(),
            not: NamedNodeRef::new("http://www.w3.org/ns/shacl#not").unwrap(),
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
            node_shape: NamedNodeRef::new("http://www.w3.org/ns/shacl#NodeShape").unwrap(),
            property_shape: NamedNodeRef::new("http://www.w3.org/ns/shacl#PropertyShape").unwrap(),
            and_: NamedNodeRef::new("http://www.w3.org/ns/shacl#and").unwrap(),
            or_: NamedNodeRef::new("http://www.w3.org/ns/shacl#or").unwrap(),
            xone: NamedNodeRef::new("http://www.w3.org/ns/shacl#xone").unwrap(),
            path: NamedNodeRef::new("http://www.w3.org/ns/shacl#path").unwrap(),
            inverse_path: NamedNodeRef::new("http://www.w3.org/ns/shacl#inversePath").unwrap(),
            alternative_path: NamedNodeRef::new("http://www.w3.org/ns/shacl#alternativePath")
                .unwrap(),
            sequence_path: NamedNodeRef::new("http://www.w3.org/ns/shacl#sequencePath").unwrap(),
            zero_or_more_path: NamedNodeRef::new("http://www.w3.org/ns/shacl#zeroOrMorePath")
                .unwrap(),
            one_or_more_path: NamedNodeRef::new("http://www.w3.org/ns/shacl#oneOrMorePath")
                .unwrap(),
            zero_or_one_path: NamedNodeRef::new("http://www.w3.org/ns/shacl#zeroOrOnePath")
                .unwrap(),

            target_class: NamedNodeRef::new("http://www.w3.org/ns/shacl#targetClass").unwrap(),
            target_node: NamedNodeRef::new("http://www.w3.org/ns/shacl#targetNode").unwrap(),
            target_objects_of: NamedNodeRef::new("http://www.w3.org/ns/shacl#targetObjectsOf")
                .unwrap(),
            target_subjects_of: NamedNodeRef::new("http://www.w3.org/ns/shacl#targetSubjectsOf")
                .unwrap(),

            equals: NamedNodeRef::new("http://www.w3.org/ns/shacl#equals").unwrap(),
            disjoint: NamedNodeRef::new("http://www.w3.org/ns/shacl#disjoint").unwrap(),
            less_than: NamedNodeRef::new("http://www.w3.org/ns/shacl#lessThan").unwrap(),
            less_than_or_equals: NamedNodeRef::new("http://www.w3.org/ns/shacl#lessThanOrEquals")
                .unwrap(),

            closed: NamedNodeRef::new("http://www.w3.org/ns/shacl#closed").unwrap(),
            ignored_properties: NamedNodeRef::new("http://www.w3.org/ns/shacl#ignoredProperties")
                .unwrap(),
            has_value: NamedNodeRef::new("http://www.w3.org/ns/shacl#hasValue").unwrap(),
            in_: NamedNodeRef::new("http://www.w3.org/ns/shacl#in").unwrap(),

            // NodeKind instances
            iri: NamedNodeRef::new("http://www.w3.org/ns/shacl#IRI").unwrap(),
            literal: NamedNodeRef::new("http://www.w3.org/ns/shacl#Literal").unwrap(),
            blank_node: NamedNodeRef::new("http://www.w3.org/ns/shacl#BlankNode").unwrap(),
            blank_node_or_iri: NamedNodeRef::new("http://www.w3.org/ns/shacl#BlankNodeOrIRI")
                .unwrap(),
            blank_node_or_literal: NamedNodeRef::new(
                "http://www.w3.org/ns/shacl#BlankNodeOrLiteral",
            )
            .unwrap(),
            iri_or_literal: NamedNodeRef::new("http://www.w3.org/ns/shacl#IRIOrLiteral").unwrap(),

            // Severities
            severity: NamedNodeRef::new("http://www.w3.org/ns/shacl#severity").unwrap(),
            info: NamedNodeRef::new("http://www.w3.org/ns/shacl#Info").unwrap(),
            warning: NamedNodeRef::new("http://www.w3.org/ns/shacl#Warning").unwrap(),
            violation: NamedNodeRef::new("http://www.w3.org/ns/shacl#Violation").unwrap(),

            // SPARQL
            select: NamedNodeRef::new("http://www.w3.org/ns/shacl#select").unwrap(),
            deactivated: NamedNodeRef::new("http://www.w3.org/ns/shacl#deactivated").unwrap(),
            message: NamedNodeRef::new("http://www.w3.org/ns/shacl#message").unwrap(),
            sparql: NamedNodeRef::new("http://www.w3.org/ns/shacl#sparql").unwrap(),

            // Validation Report
            validation_report: NamedNodeRef::new("http://www.w3.org/ns/shacl#ValidationReport")
                .unwrap(),
            conforms: NamedNodeRef::new("http://www.w3.org/ns/shacl#conforms").unwrap(),
            result: NamedNodeRef::new("http://www.w3.org/ns/shacl#result").unwrap(),
            validation_result: NamedNodeRef::new("http://www.w3.org/ns/shacl#ValidationResult")
                .unwrap(),
            focus_node: NamedNodeRef::new("http://www.w3.org/ns/shacl#focusNode").unwrap(),
            result_path: NamedNodeRef::new("http://www.w3.org/ns/shacl#resultPath").unwrap(),
            source_shape: NamedNodeRef::new("http://www.w3.org/ns/shacl#sourceShape").unwrap(),
            source_constraint_component: NamedNodeRef::new(
                "http://www.w3.org/ns/shacl#sourceConstraintComponent",
            )
            .unwrap(),
            result_message: NamedNodeRef::new("http://www.w3.org/ns/shacl#resultMessage").unwrap(),
            result_severity: NamedNodeRef::new("http://www.w3.org/ns/shacl#resultSeverity")
                .unwrap(),
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
            subject: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#subject")
                .unwrap(),
            predicate: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate")
                .unwrap(),
            object: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#object").unwrap(),
            first: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#first").unwrap(),
            rest: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest").unwrap(),
            nil: NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil").unwrap(),
        }
    }
}

pub struct RDFS {
    pub sub_class_of: NamedNodeRef<'static>,
    pub label: NamedNodeRef<'static>,
    pub comment: NamedNodeRef<'static>,
    pub class: NamedNodeRef<'static>,
}

impl RDFS {
    pub fn new() -> Self {
        RDFS {
            sub_class_of: NamedNodeRef::new("http://www.w3.org/2000/01/rdf-schema#subClassOf")
                .unwrap(),
            label: NamedNodeRef::new("http://www.w3.org/2000/01/rdf-schema#label").unwrap(),
            comment: NamedNodeRef::new("http://www.w3.org/2000/01/rdf-schema#comment").unwrap(),
            class: NamedNodeRef::new("http://www.w3.org/2000/01/rdf-schema#Class").unwrap(),
        }
    }
}

pub struct OWL {
    pub class: NamedNodeRef<'static>,
}

impl OWL {
    pub fn new() -> Self {
        OWL {
            class: NamedNodeRef::new("http://www.w3.org/2002/07/owl#Class").unwrap(),
        }
    }
}

pub struct MF {
    pub manifest: NamedNodeRef<'static>,
    pub entries: NamedNodeRef<'static>,
    pub action: NamedNodeRef<'static>,
    pub result: NamedNodeRef<'static>,
    pub status: NamedNodeRef<'static>,
    pub include: NamedNodeRef<'static>,
}

impl MF {
    pub fn new() -> Self {
        MF {
            manifest: NamedNodeRef::new("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#Manifest").unwrap(),
            entries: NamedNodeRef::new("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#entries").unwrap(),
            action: NamedNodeRef::new("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action").unwrap(),
            result: NamedNodeRef::new("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#result").unwrap(),
            status: NamedNodeRef::new("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#status").unwrap(),
            include: NamedNodeRef::new("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#include").unwrap(),
        }
    }
}

pub struct SHT {
    pub validate: NamedNodeRef<'static>,
    pub data_graph: NamedNodeRef<'static>,
    pub shapes_graph: NamedNodeRef<'static>,
}

impl SHT {
    pub fn new() -> Self {
        SHT {
            validate: NamedNodeRef::new("http://www.w3.org/ns/shacl-test#Validate").unwrap(),
            data_graph: NamedNodeRef::new("http://www.w3.org/ns/shacl-test#dataGraph").unwrap(),
            shapes_graph: NamedNodeRef::new("http://www.w3.org/ns/shacl-test#shapesGraph").unwrap(),
        }
    }
}
