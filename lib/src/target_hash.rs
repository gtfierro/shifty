//! Target hashing for deduplication of identical target expressions.
//!
//! Multiple shapes often use identical target expressions (e.g., `sh:targetClass s223:Equipment`).
//! By hashing target expressions and deduplicating them, we can compute target nodes once
//! and share results across all shapes with the same targets.

use crate::types::Target;
use oxigraph::model::Term;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Compute a hash for a target expression.
///
/// Identical target expressions (same type and parameters) will produce the same hash,
/// enabling deduplication and sharing of target node computations.
pub fn hash_target(target: &Target) -> u64 {
    let mut hasher = DefaultHasher::new();

    match target {
        Target::Class(class_term) => {
            "class".hash(&mut hasher);
            hash_term(class_term, &mut hasher);
        }
        Target::Node(node_term) => {
            "node".hash(&mut hasher);
            hash_term(node_term, &mut hasher);
        }
        Target::SubjectsOf(predicate) => {
            "subjects_of".hash(&mut hasher);
            hash_term(predicate, &mut hasher);
        }
        Target::ObjectsOf(predicate) => {
            "objects_of".hash(&mut hasher);
            hash_term(predicate, &mut hasher);
        }
        Target::Advanced { .. } => {
            // Advanced targets include SPARQL queries which are more complex
            // For now, hash as a unique type to avoid false deduplication
            // TODO: Consider hashing SPARQL query text for better sharing
            "advanced".hash(&mut hasher);
            // Use a unique identifier to prevent accidental collisions
            std::ptr::addr_of!(target).hash(&mut hasher);
        }
    }

    hasher.finish()
}

/// Hash a Term for use in target hashing.
fn hash_term(term: &Term, hasher: &mut DefaultHasher) {
    // Hash the term's string representation for simplicity
    // This ensures identical IRIs/literals produce the same hash
    term.to_string().hash(hasher);
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxigraph::model::NamedNode;

    #[test]
    fn test_identical_class_targets_same_hash() {
        let class1 = NamedNode::new("http://example.org/Class1").unwrap();
        let class2 = NamedNode::new("http://example.org/Class1").unwrap();

        let target1 = Target::Class(class1.into());
        let target2 = Target::Class(class2.into());

        assert_eq!(hash_target(&target1), hash_target(&target2));
    }

    #[test]
    fn test_different_class_targets_different_hash() {
        let class1 = NamedNode::new("http://example.org/Class1").unwrap();
        let class2 = NamedNode::new("http://example.org/Class2").unwrap();

        let target1 = Target::Class(class1.into());
        let target2 = Target::Class(class2.into());

        assert_ne!(hash_target(&target1), hash_target(&target2));
    }

    #[test]
    fn test_different_target_types_different_hash() {
        let term = NamedNode::new("http://example.org/Term").unwrap();

        let target_class = Target::Class(term.clone().into());
        let target_node = Target::Node(term.into());

        assert_ne!(hash_target(&target_class), hash_target(&target_node));
    }
}
