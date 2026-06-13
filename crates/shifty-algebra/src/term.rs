//! RDF term aliases and node-kind sets (doc 00 §1).
//!
//! We reuse `oxrdf`'s term types rather than reinvent them: `NamedNode` (IRIs),
//! `BlankNode`, `Literal`, and the `Term` sum. In the formalism's vocabulary,
//! nodes `N = IRI ∪ Blank`, values `V = Literal`, and predicates `P = K = IRI`.

pub use oxrdf::{BlankNode, Literal, NamedNode, Term};
use serde::{Deserialize, Serialize};

/// The set of RDF node kinds a term may have, for `sh:nodeKind`
/// (gap-analysis **K1** — the paper drops node-kind because it hides node
/// identity). The six W3C node-kind values are the non-empty combinations
/// exposed as associated constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeKindSet {
    pub iri: bool,
    pub blank: bool,
    pub literal: bool,
}

impl NodeKindSet {
    pub const IRI: Self = Self {
        iri: true,
        blank: false,
        literal: false,
    };
    pub const BLANK_NODE: Self = Self {
        iri: false,
        blank: true,
        literal: false,
    };
    pub const LITERAL: Self = Self {
        iri: false,
        blank: false,
        literal: true,
    };
    pub const BLANK_NODE_OR_IRI: Self = Self {
        iri: true,
        blank: true,
        literal: false,
    };
    pub const BLANK_NODE_OR_LITERAL: Self = Self {
        iri: false,
        blank: true,
        literal: true,
    };
    pub const IRI_OR_LITERAL: Self = Self {
        iri: true,
        blank: false,
        literal: true,
    };

    /// Does `term` have one of the kinds in this set?
    pub fn matches(&self, term: &Term) -> bool {
        match term {
            Term::NamedNode(_) => self.iri,
            Term::BlankNode(_) => self.blank,
            Term::Literal(_) => self.literal,
        }
    }

    pub fn complement(self) -> NodeKindSet {
        NodeKindSet {
            iri: !self.iri,
            blank: !self.blank,
            literal: !self.literal,
        }
    }

    pub fn is_empty(self) -> bool {
        !self.iri && !self.blank && !self.literal
    }
}
