//! IRI constants for the SHACL, RDF, and RDFS vocabularies.
//!
//! `*_REF` constants are `NamedNodeRef` for graph lookups; the owned helpers are
//! for building paths in the IR.

use oxrdf::{NamedNode, NamedNodeRef};

pub const SH: &str = "http://www.w3.org/ns/shacl#";
pub const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
pub const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
pub const OWL: &str = "http://www.w3.org/2002/07/owl#";

macro_rules! iri {
    ($name:ident, $iri:expr) => {
        pub const $name: NamedNodeRef<'static> = NamedNodeRef::new_unchecked($iri);
    };
}

// rdf / rdfs
iri!(RDF_TYPE, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
iri!(RDF_FIRST, "http://www.w3.org/1999/02/22-rdf-syntax-ns#first");
iri!(RDF_REST, "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest");
iri!(RDF_NIL, "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil");
iri!(RDFS_CLASS, "http://www.w3.org/2000/01/rdf-schema#Class");
iri!(RDFS_SUBCLASSOF, "http://www.w3.org/2000/01/rdf-schema#subClassOf");
iri!(OWL_CLASS, "http://www.w3.org/2002/07/owl#Class");

// shape types
iri!(SH_NODE_SHAPE, "http://www.w3.org/ns/shacl#NodeShape");
iri!(SH_PROPERTY_SHAPE, "http://www.w3.org/ns/shacl#PropertyShape");

// targets
iri!(SH_TARGET_NODE, "http://www.w3.org/ns/shacl#targetNode");
iri!(SH_TARGET_CLASS, "http://www.w3.org/ns/shacl#targetClass");
iri!(SH_TARGET_SUBJECTS_OF, "http://www.w3.org/ns/shacl#targetSubjectsOf");
iri!(SH_TARGET_OBJECTS_OF, "http://www.w3.org/ns/shacl#targetObjectsOf");
iri!(SH_TARGET, "http://www.w3.org/ns/shacl#target");

// paths
iri!(SH_PATH, "http://www.w3.org/ns/shacl#path");
iri!(SH_INVERSE_PATH, "http://www.w3.org/ns/shacl#inversePath");
iri!(SH_ALTERNATIVE_PATH, "http://www.w3.org/ns/shacl#alternativePath");
iri!(SH_ZERO_OR_MORE_PATH, "http://www.w3.org/ns/shacl#zeroOrMorePath");
iri!(SH_ONE_OR_MORE_PATH, "http://www.w3.org/ns/shacl#oneOrMorePath");
iri!(SH_ZERO_OR_ONE_PATH, "http://www.w3.org/ns/shacl#zeroOrOnePath");

// value-type / facet constraints
iri!(SH_CLASS, "http://www.w3.org/ns/shacl#class");
iri!(SH_DATATYPE, "http://www.w3.org/ns/shacl#datatype");
iri!(SH_NODE_KIND, "http://www.w3.org/ns/shacl#nodeKind");
iri!(SH_MIN_EXCLUSIVE, "http://www.w3.org/ns/shacl#minExclusive");
iri!(SH_MIN_INCLUSIVE, "http://www.w3.org/ns/shacl#minInclusive");
iri!(SH_MAX_EXCLUSIVE, "http://www.w3.org/ns/shacl#maxExclusive");
iri!(SH_MAX_INCLUSIVE, "http://www.w3.org/ns/shacl#maxInclusive");
iri!(SH_MIN_LENGTH, "http://www.w3.org/ns/shacl#minLength");
iri!(SH_MAX_LENGTH, "http://www.w3.org/ns/shacl#maxLength");
iri!(SH_PATTERN, "http://www.w3.org/ns/shacl#pattern");
iri!(SH_FLAGS, "http://www.w3.org/ns/shacl#flags");
iri!(SH_LANGUAGE_IN, "http://www.w3.org/ns/shacl#languageIn");
iri!(SH_UNIQUE_LANG, "http://www.w3.org/ns/shacl#uniqueLang");

// cardinality
iri!(SH_MIN_COUNT, "http://www.w3.org/ns/shacl#minCount");
iri!(SH_MAX_COUNT, "http://www.w3.org/ns/shacl#maxCount");

// property pairs
iri!(SH_EQUALS, "http://www.w3.org/ns/shacl#equals");
iri!(SH_DISJOINT, "http://www.w3.org/ns/shacl#disjoint");
iri!(SH_LESS_THAN, "http://www.w3.org/ns/shacl#lessThan");
iri!(SH_LESS_THAN_OR_EQUALS, "http://www.w3.org/ns/shacl#lessThanOrEquals");

// logical
iri!(SH_NOT, "http://www.w3.org/ns/shacl#not");
iri!(SH_AND, "http://www.w3.org/ns/shacl#and");
iri!(SH_OR, "http://www.w3.org/ns/shacl#or");
iri!(SH_XONE, "http://www.w3.org/ns/shacl#xone");

// shape-based
iri!(SH_NODE, "http://www.w3.org/ns/shacl#node");
iri!(SH_PROPERTY, "http://www.w3.org/ns/shacl#property");
iri!(SH_QUALIFIED_VALUE_SHAPE, "http://www.w3.org/ns/shacl#qualifiedValueShape");
iri!(SH_QUALIFIED_MIN_COUNT, "http://www.w3.org/ns/shacl#qualifiedMinCount");
iri!(SH_QUALIFIED_MAX_COUNT, "http://www.w3.org/ns/shacl#qualifiedMaxCount");

// other
iri!(SH_CLOSED, "http://www.w3.org/ns/shacl#closed");
iri!(SH_IGNORED_PROPERTIES, "http://www.w3.org/ns/shacl#ignoredProperties");
iri!(SH_HAS_VALUE, "http://www.w3.org/ns/shacl#hasValue");
iri!(SH_IN, "http://www.w3.org/ns/shacl#in");
iri!(SH_DEACTIVATED, "http://www.w3.org/ns/shacl#deactivated");

// node-kind values
iri!(SH_IRI, "http://www.w3.org/ns/shacl#IRI");
iri!(SH_BLANK_NODE, "http://www.w3.org/ns/shacl#BlankNode");
iri!(SH_LITERAL, "http://www.w3.org/ns/shacl#Literal");
iri!(SH_BLANK_NODE_OR_IRI, "http://www.w3.org/ns/shacl#BlankNodeOrIRI");
iri!(SH_BLANK_NODE_OR_LITERAL, "http://www.w3.org/ns/shacl#BlankNodeOrLiteral");
iri!(SH_IRI_OR_LITERAL, "http://www.w3.org/ns/shacl#IRIOrLiteral");

// AF (recognized for diagnostics)
iri!(SH_SPARQL, "http://www.w3.org/ns/shacl#sparql");
iri!(SH_RULE, "http://www.w3.org/ns/shacl#rule");

// AF rules (SHACL-AF §4)
iri!(SH_THIS, "http://www.w3.org/ns/shacl#this");
iri!(SH_SUBJECT, "http://www.w3.org/ns/shacl#subject");
iri!(SH_PREDICATE, "http://www.w3.org/ns/shacl#predicate");
iri!(SH_OBJECT, "http://www.w3.org/ns/shacl#object");
iri!(SH_CONDITION, "http://www.w3.org/ns/shacl#condition");
iri!(SH_ORDER, "http://www.w3.org/ns/shacl#order");
iri!(SH_CONSTRUCT, "http://www.w3.org/ns/shacl#construct");

/// `rdf:type` as an owned node (for path construction).
pub fn rdf_type() -> NamedNode {
    NamedNode::new_unchecked(format!("{RDF}type"))
}

/// `rdfs:subClassOf` as an owned node (for path construction).
pub fn rdfs_subclassof() -> NamedNode {
    NamedNode::new_unchecked(format!("{RDFS}subClassOf"))
}
