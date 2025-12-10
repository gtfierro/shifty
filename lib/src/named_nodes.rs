#![allow(clippy::upper_case_acronyms)]

use oxigraph::model::NamedNodeRef;

/// A struct holding static `NamedNodeRef`s for all SHACL vocabulary terms.
///
/// This provides a convenient and performant way to access SHACL IRIs.
pub(crate) struct SHACL {
    pub(crate) class: NamedNodeRef<'static>,
    pub(crate) node: NamedNodeRef<'static>,
    pub(crate) property: NamedNodeRef<'static>,
    pub(crate) qualified_value_shape: NamedNodeRef<'static>,
    pub(crate) qualified_min_count: NamedNodeRef<'static>,
    pub(crate) qualified_max_count: NamedNodeRef<'static>,
    pub(crate) min_count: NamedNodeRef<'static>,
    pub(crate) max_count: NamedNodeRef<'static>,
    pub(crate) qualified_value_shapes_disjoint: NamedNodeRef<'static>,
    pub(crate) not: NamedNodeRef<'static>,
    pub(crate) node_kind: NamedNodeRef<'static>,
    pub(crate) datatype: NamedNodeRef<'static>,
    pub(crate) min_exclusive: NamedNodeRef<'static>,
    pub(crate) min_inclusive: NamedNodeRef<'static>,
    pub(crate) max_exclusive: NamedNodeRef<'static>,
    pub(crate) max_inclusive: NamedNodeRef<'static>,
    pub(crate) min_length: NamedNodeRef<'static>,
    pub(crate) max_length: NamedNodeRef<'static>,
    pub(crate) pattern: NamedNodeRef<'static>,
    pub(crate) flags: NamedNodeRef<'static>,
    pub(crate) language_in: NamedNodeRef<'static>,
    pub(crate) unique_lang: NamedNodeRef<'static>,
    pub(crate) node_shape: NamedNodeRef<'static>,
    pub(crate) property_shape: NamedNodeRef<'static>,
    pub(crate) and_: NamedNodeRef<'static>,
    pub(crate) or_: NamedNodeRef<'static>,
    pub(crate) xone: NamedNodeRef<'static>,
    pub(crate) path: NamedNodeRef<'static>,
    pub(crate) inverse_path: NamedNodeRef<'static>,
    pub(crate) alternative_path: NamedNodeRef<'static>,
    pub(crate) sequence_path: NamedNodeRef<'static>,
    pub(crate) zero_or_more_path: NamedNodeRef<'static>,
    pub(crate) one_or_more_path: NamedNodeRef<'static>,
    pub(crate) zero_or_one_path: NamedNodeRef<'static>,

    pub(crate) target_class: NamedNodeRef<'static>,
    pub(crate) target_node: NamedNodeRef<'static>,
    pub(crate) target_objects_of: NamedNodeRef<'static>,
    pub(crate) target_subjects_of: NamedNodeRef<'static>,
    pub(crate) target: NamedNodeRef<'static>,
    pub(crate) target_validator: NamedNodeRef<'static>,

    pub(crate) equals: NamedNodeRef<'static>,
    pub(crate) disjoint: NamedNodeRef<'static>,
    pub(crate) less_than: NamedNodeRef<'static>,
    pub(crate) less_than_or_equals: NamedNodeRef<'static>,

    pub(crate) closed: NamedNodeRef<'static>,
    pub(crate) ignored_properties: NamedNodeRef<'static>,
    pub(crate) has_value: NamedNodeRef<'static>,
    pub(crate) in_: NamedNodeRef<'static>, // `in` is a reserved keyword in Rust

    // NodeKind instances
    pub(crate) iri: NamedNodeRef<'static>,
    pub(crate) literal: NamedNodeRef<'static>,
    pub(crate) blank_node: NamedNodeRef<'static>,
    pub(crate) blank_node_or_iri: NamedNodeRef<'static>,
    pub(crate) blank_node_or_literal: NamedNodeRef<'static>,
    pub(crate) iri_or_literal: NamedNodeRef<'static>,

    // Severities
    pub(crate) severity: NamedNodeRef<'static>,
    pub(crate) info: NamedNodeRef<'static>,
    pub(crate) warning: NamedNodeRef<'static>,
    pub(crate) violation: NamedNodeRef<'static>,

    // SPARQL
    pub(crate) select: NamedNodeRef<'static>,
    pub(crate) deactivated: NamedNodeRef<'static>,
    pub(crate) message: NamedNodeRef<'static>,
    pub(crate) sparql: NamedNodeRef<'static>,
    pub(crate) prefixes: NamedNodeRef<'static>,
    pub(crate) declare: NamedNodeRef<'static>,
    pub(crate) prefix: NamedNodeRef<'static>,
    pub(crate) namespace: NamedNodeRef<'static>,
    pub(crate) default_value: NamedNodeRef<'static>,
    pub(crate) name: NamedNodeRef<'static>,
    pub(crate) description: NamedNodeRef<'static>,
    pub(crate) parameter: NamedNodeRef<'static>,
    pub(crate) optional: NamedNodeRef<'static>,
    pub(crate) var_name: NamedNodeRef<'static>,
    pub(crate) validator: NamedNodeRef<'static>,
    pub(crate) node_validator: NamedNodeRef<'static>,
    pub(crate) property_validator: NamedNodeRef<'static>,
    pub(crate) shape_class: NamedNodeRef<'static>,
    pub(crate) shape_prop: NamedNodeRef<'static>,
    pub(crate) rule: NamedNodeRef<'static>,
    pub(crate) triple_rule: NamedNodeRef<'static>,
    pub(crate) sparql_rule: NamedNodeRef<'static>,
    pub(crate) construct: NamedNodeRef<'static>,
    pub(crate) condition: NamedNodeRef<'static>,
    pub(crate) rule_subject: NamedNodeRef<'static>,
    pub(crate) rule_predicate: NamedNodeRef<'static>,
    pub(crate) rule_object: NamedNodeRef<'static>,
    pub(crate) order: NamedNodeRef<'static>,
    pub(crate) this: NamedNodeRef<'static>,

    // Validation Report
    pub(crate) validation_report: NamedNodeRef<'static>,
    pub(crate) conforms: NamedNodeRef<'static>,
    pub(crate) result: NamedNodeRef<'static>,
    pub(crate) validation_result: NamedNodeRef<'static>,
    pub(crate) focus_node: NamedNodeRef<'static>,
    pub(crate) value: NamedNodeRef<'static>,
    pub(crate) result_path: NamedNodeRef<'static>,
    pub(crate) source_shape: NamedNodeRef<'static>,
    pub(crate) source_constraint: NamedNodeRef<'static>,
    pub(crate) source_constraint_component: NamedNodeRef<'static>,
    pub(crate) result_message: NamedNodeRef<'static>,
    pub(crate) result_severity: NamedNodeRef<'static>,
}

impl SHACL {
    /// Creates a new `SHACL` instance with all terms initialized.
    pub(crate) fn new() -> Self {
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
            target: NamedNodeRef::new("http://www.w3.org/ns/shacl#target").unwrap(),
            target_validator: NamedNodeRef::new("http://www.w3.org/ns/shacl#targetValidator")
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
            prefixes: NamedNodeRef::new("http://www.w3.org/ns/shacl#prefixes").unwrap(),
            declare: NamedNodeRef::new("http://www.w3.org/ns/shacl#declare").unwrap(),
            prefix: NamedNodeRef::new("http://www.w3.org/ns/shacl#prefix").unwrap(),
            namespace: NamedNodeRef::new("http://www.w3.org/ns/shacl#namespace").unwrap(),
            default_value: NamedNodeRef::new("http://www.w3.org/ns/shacl#defaultValue").unwrap(),
            name: NamedNodeRef::new("http://www.w3.org/ns/shacl#name").unwrap(),
            description: NamedNodeRef::new("http://www.w3.org/ns/shacl#description").unwrap(),
            parameter: NamedNodeRef::new("http://www.w3.org/ns/shacl#parameter").unwrap(),
            optional: NamedNodeRef::new("http://www.w3.org/ns/shacl#optional").unwrap(),
            var_name: NamedNodeRef::new("http://www.w3.org/ns/shacl#varName").unwrap(),
            validator: NamedNodeRef::new("http://www.w3.org/ns/shacl#validator").unwrap(),
            node_validator: NamedNodeRef::new("http://www.w3.org/ns/shacl#nodeValidator").unwrap(),
            property_validator: NamedNodeRef::new("http://www.w3.org/ns/shacl#propertyValidator")
                .unwrap(),
            shape_class: NamedNodeRef::new("http://www.w3.org/ns/shacl#Shape").unwrap(),
            shape_prop: NamedNodeRef::new("http://www.w3.org/ns/shacl#shape").unwrap(),
            rule: NamedNodeRef::new("http://www.w3.org/ns/shacl#rule").unwrap(),
            triple_rule: NamedNodeRef::new("http://www.w3.org/ns/shacl#TripleRule").unwrap(),
            sparql_rule: NamedNodeRef::new("http://www.w3.org/ns/shacl#SPARQLRule").unwrap(),
            construct: NamedNodeRef::new("http://www.w3.org/ns/shacl#construct").unwrap(),
            condition: NamedNodeRef::new("http://www.w3.org/ns/shacl#condition").unwrap(),
            rule_subject: NamedNodeRef::new("http://www.w3.org/ns/shacl#subject").unwrap(),
            rule_predicate: NamedNodeRef::new("http://www.w3.org/ns/shacl#predicate").unwrap(),
            rule_object: NamedNodeRef::new("http://www.w3.org/ns/shacl#object").unwrap(),
            order: NamedNodeRef::new("http://www.w3.org/ns/shacl#order").unwrap(),
            this: NamedNodeRef::new("http://www.w3.org/ns/shacl#this").unwrap(),

            // Validation Report
            validation_report: NamedNodeRef::new("http://www.w3.org/ns/shacl#ValidationReport")
                .unwrap(),
            conforms: NamedNodeRef::new("http://www.w3.org/ns/shacl#conforms").unwrap(),
            result: NamedNodeRef::new("http://www.w3.org/ns/shacl#result").unwrap(),
            validation_result: NamedNodeRef::new("http://www.w3.org/ns/shacl#ValidationResult")
                .unwrap(),
            focus_node: NamedNodeRef::new("http://www.w3.org/ns/shacl#focusNode").unwrap(),
            value: NamedNodeRef::new("http://www.w3.org/ns/shacl#value").unwrap(),
            result_path: NamedNodeRef::new("http://www.w3.org/ns/shacl#resultPath").unwrap(),
            source_shape: NamedNodeRef::new("http://www.w3.org/ns/shacl#sourceShape").unwrap(),
            source_constraint: NamedNodeRef::new("http://www.w3.org/ns/shacl#sourceConstraint")
                .unwrap(),
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

/// A struct holding static `NamedNodeRef`s for RDF vocabulary terms.
#[allow(dead_code)]
pub(crate) struct RDF {
    pub(crate) type_: NamedNodeRef<'static>,
    pub(crate) subject: NamedNodeRef<'static>,
    pub(crate) predicate: NamedNodeRef<'static>,
    pub(crate) object: NamedNodeRef<'static>,
    pub(crate) first: NamedNodeRef<'static>,
    pub(crate) rest: NamedNodeRef<'static>,
    pub(crate) nil: NamedNodeRef<'static>,
}

impl RDF {
    /// Creates a new `RDF` instance.
    pub(crate) fn new() -> Self {
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

/// A struct holding static `NamedNodeRef`s for RDFS vocabulary terms.
#[allow(dead_code)]
pub(crate) struct RDFS {
    pub(crate) sub_class_of: NamedNodeRef<'static>,
    pub(crate) label: NamedNodeRef<'static>,
    pub(crate) comment: NamedNodeRef<'static>,
    pub(crate) class: NamedNodeRef<'static>,
}

impl RDFS {
    /// Creates a new `RDFS` instance.
    pub(crate) fn new() -> Self {
        RDFS {
            sub_class_of: NamedNodeRef::new("http://www.w3.org/2000/01/rdf-schema#subClassOf")
                .unwrap(),
            label: NamedNodeRef::new("http://www.w3.org/2000/01/rdf-schema#label").unwrap(),
            comment: NamedNodeRef::new("http://www.w3.org/2000/01/rdf-schema#comment").unwrap(),
            class: NamedNodeRef::new("http://www.w3.org/2000/01/rdf-schema#Class").unwrap(),
        }
    }
}

/// A struct holding static `NamedNodeRef`s for OWL vocabulary terms.
pub(crate) struct OWL {
    pub(crate) class: NamedNodeRef<'static>,
}

impl OWL {
    /// Creates a new `OWL` instance.
    pub(crate) fn new() -> Self {
        OWL {
            class: NamedNodeRef::new("http://www.w3.org/2002/07/owl#Class").unwrap(),
        }
    }
}

/// A struct holding static `NamedNodeRef`s for W3C Test Manifest vocabulary terms.
#[allow(dead_code)]
pub(crate) struct MF {
    pub(crate) manifest: NamedNodeRef<'static>,
    pub(crate) entries: NamedNodeRef<'static>,
    pub(crate) action: NamedNodeRef<'static>,
    pub(crate) result: NamedNodeRef<'static>,
    pub(crate) status: NamedNodeRef<'static>,
    pub(crate) include: NamedNodeRef<'static>,
}

impl MF {
    /// Creates a new `MF` instance.
    pub(crate) fn new() -> Self {
        MF {
            manifest: NamedNodeRef::new(
                "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#Manifest",
            )
            .unwrap(),
            entries: NamedNodeRef::new(
                "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#entries",
            )
            .unwrap(),
            action: NamedNodeRef::new(
                "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action",
            )
            .unwrap(),
            result: NamedNodeRef::new(
                "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#result",
            )
            .unwrap(),
            status: NamedNodeRef::new(
                "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#status",
            )
            .unwrap(),
            include: NamedNodeRef::new(
                "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#include",
            )
            .unwrap(),
        }
    }
}

/// A struct holding static `NamedNodeRef`s for SHACL Test Suite vocabulary terms.
pub(crate) struct SHT {
    pub(crate) validate: NamedNodeRef<'static>,
    pub(crate) data_graph: NamedNodeRef<'static>,
    pub(crate) shapes_graph: NamedNodeRef<'static>,
}

impl SHT {
    /// Creates a new `SHT` instance.
    pub(crate) fn new() -> Self {
        SHT {
            validate: NamedNodeRef::new("http://www.w3.org/ns/shacl-test#Validate").unwrap(),
            data_graph: NamedNodeRef::new("http://www.w3.org/ns/shacl-test#dataGraph").unwrap(),
            shapes_graph: NamedNodeRef::new("http://www.w3.org/ns/shacl-test#shapesGraph").unwrap(),
        }
    }
}
