//! Vocabulary index built from imported ontologies, for build-time checks.
//!
//! An unknown name in a namespace that a loaded ontology defines is almost
//! always a typo (`brick:hasPoints`), and SHACL would only reveal it at
//! validation time as a mysteriously failing or vacuous constraint. The index
//! records which classes and properties exist, per namespace.

use oxrdf::{Graph, NamedNodeRef, Term, TermRef};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const RDFS_SUBPROPERTY_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const OWL_INVERSE_OF: &str = "http://www.w3.org/2002/07/owl#inverseOf";

const CLASS_TYPES: &[&str] = &[
    "http://www.w3.org/2002/07/owl#Class",
    "http://www.w3.org/2000/01/rdf-schema#Class",
];
const PROPERTY_TYPES: &[&str] = &[
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property",
    "http://www.w3.org/2002/07/owl#ObjectProperty",
    "http://www.w3.org/2002/07/owl#DatatypeProperty",
    "http://www.w3.org/2002/07/owl#AnnotationProperty",
    "http://www.w3.org/2002/07/owl#SymmetricProperty",
    "http://www.w3.org/2002/07/owl#TransitiveProperty",
    "http://www.w3.org/2002/07/owl#FunctionalProperty",
    "http://www.w3.org/2002/07/owl#InverseFunctionalProperty",
    "http://www.w3.org/2002/07/owl#AsymmetricProperty",
    "http://www.w3.org/2002/07/owl#IrreflexiveProperty",
];

/// Namespaces that are never checked: their terms are standard and a loaded
/// ontology rarely redefines them.
pub const WELL_KNOWN_NAMESPACES: &[&str] = &[
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "http://www.w3.org/2000/01/rdf-schema#",
    "http://www.w3.org/2001/XMLSchema#",
    "http://www.w3.org/2002/07/owl#",
    "http://www.w3.org/ns/shacl#",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermKind {
    Class,
    Property,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vocabulary {
    pub classes: BTreeSet<String>,
    pub properties: BTreeSet<String>,
}

/// Split an IRI into `(namespace, local name)` at the last `#` or `/`.
pub fn split_iri(iri: &str) -> (&str, &str) {
    match iri.rfind(['#', '/']) {
        Some(i) => (&iri[..=i], &iri[i + 1..]),
        None => ("", iri),
    }
}

impl Vocabulary {
    pub fn is_empty(&self) -> bool {
        self.classes.is_empty() && self.properties.is_empty()
    }

    pub fn extend(&mut self, other: &Vocabulary) {
        self.classes.extend(other.classes.iter().cloned());
        self.properties.extend(other.properties.iter().cloned());
    }

    /// Index the classes and properties a graph declares or uses
    /// schematically.
    pub fn from_graph(g: &Graph) -> Vocabulary {
        let mut v = Vocabulary::default();
        let named = |t: TermRef<'_>| match t {
            TermRef::NamedNode(n) => Some(n.as_str().to_string()),
            _ => None,
        };
        for t in g.iter() {
            let p = t.predicate.as_str();
            let subject = match t.subject {
                oxrdf::NamedOrBlankNodeRef::NamedNode(n) => Some(n.as_str().to_string()),
                _ => None,
            };
            if p == RDF_TYPE {
                if let TermRef::NamedNode(ty) = t.object {
                    if CLASS_TYPES.contains(&ty.as_str()) {
                        v.classes.extend(subject);
                    } else if PROPERTY_TYPES.contains(&ty.as_str()) {
                        v.properties.extend(subject);
                    }
                }
            } else if p == RDFS_SUBCLASS_OF {
                v.classes.extend(subject);
                v.classes.extend(named(t.object));
            } else if p == RDFS_SUBPROPERTY_OF || p == OWL_INVERSE_OF {
                v.properties.extend(subject);
                v.properties.extend(named(t.object));
            } else if p == RDFS_DOMAIN || p == RDFS_RANGE {
                v.properties.extend(subject);
            }
        }
        v
    }

    /// Does any loaded term live in `namespace`?
    pub fn covers(&self, namespace: &str) -> bool {
        let in_ns = |s: &String| split_iri(s).0 == namespace;
        self.classes.iter().any(in_ns) || self.properties.iter().any(in_ns)
    }

    pub fn contains(&self, iri: &str, kind: TermKind) -> bool {
        match kind {
            TermKind::Class => self.classes.contains(iri),
            TermKind::Property => self.properties.contains(iri),
        }
    }

    /// Terms of `kind` in `namespace`, by local name.
    pub fn locals_in(&self, namespace: &str, kind: TermKind) -> Vec<&str> {
        let set = match kind {
            TermKind::Class => &self.classes,
            TermKind::Property => &self.properties,
        };
        set.iter()
            .filter_map(|s| {
                let (ns, local) = split_iri(s);
                (ns == namespace).then_some(local)
            })
            .collect()
    }

    /// Does the graph-level class hierarchy know `class` has superclasses?
    pub fn has_class(&self, class: &str) -> bool {
        self.classes.contains(class)
    }
}

/// Is `t` an IRI equal to `iri`?
pub fn is_iri(t: &Term, iri: NamedNodeRef<'_>) -> bool {
    matches!(t, Term::NamedNode(n) if n.as_ref() == iri)
}
