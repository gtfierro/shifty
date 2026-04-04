//! Component result memoization for deduplicating identical constraint validations.
//!
//! When multiple shapes have identical constraints (e.g., `sh:class s223:Equipment`, `sh:minCount 1`),
//! we recompute identical validation logic. By memoizing component results based on
//! (component_descriptor, focus_node, value_nodes) tuples, we skip redundant work.

use crate::runtime::ValidationFailure;
use crate::types::ComponentID;
use oxigraph::model::Term;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Cache key for component memoization.
///
/// Uniquely identifies a component validation by:
/// - Component ID (which constraint)
/// - Focus node (what we're validating)
/// - Value nodes hash (what values we're checking)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComponentMemoKey {
    pub component_id: ComponentID,
    pub focus_node: Term,
    pub value_nodes_hash: u64,
}

impl ComponentMemoKey {
    /// Create a new memoization key.
    pub fn new(component_id: ComponentID, focus_node: Term, value_nodes: &Option<Vec<Term>>) -> Self {
        let value_nodes_hash = hash_value_nodes(value_nodes);
        Self {
            component_id,
            focus_node,
            value_nodes_hash,
        }
    }
}

/// Cached component validation result.
#[derive(Debug, Clone)]
pub enum ComponentMemoValue {
    /// Validation passed with no failures
    Pass,
    /// Validation failed with specific failures
    Fail(Vec<ValidationFailure>),
}

/// Hash value nodes for use in memoization key.
fn hash_value_nodes(value_nodes: &Option<Vec<Term>>) -> u64 {
    let mut hasher = DefaultHasher::new();

    match value_nodes {
        None => {
            "none".hash(&mut hasher);
        }
        Some(nodes) => {
            // Hash the count first
            nodes.len().hash(&mut hasher);

            // Hash each node's string representation
            // Sort first to ensure consistent hashing regardless of order
            let mut sorted_nodes: Vec<String> = nodes.iter().map(|t| t.to_string()).collect();
            sorted_nodes.sort();

            for node_str in sorted_nodes {
                node_str.hash(&mut hasher);
            }
        }
    }

    hasher.finish()
}

/// Determine if a component type is safe to memoize.
///
/// Components are memoizable if they:
/// 1. Have deterministic results (same inputs → same outputs)
/// 2. Have no side effects
/// 3. Don't depend on external state beyond the data graph
pub fn is_memoizable_component(component_id: ComponentID) -> bool {
    // For now, we'll use a conservative approach and only memoize
    // components we know are safe. This can be expanded as we verify
    // more component types.
    //
    // Memoizable components (deterministic, no side effects):
    // - Class, Datatype, NodeKind (type checking)
    // - MinCount, MaxCount (cardinality)
    // - Pattern, MinLength, MaxLength (string constraints)
    // - MinInclusive, MaxInclusive, etc. (numeric constraints)
    //
    // NOT memoizable:
    // - SPARQL constraints (may have side effects, complex queries)
    // - Custom constraints (unknown behavior)
    // - JSConstraint (may have side effects)

    // TODO: Implement component type detection based on ComponentID
    // For now, we'll default to NOT memoizing until we can properly
    // identify component types from the ComponentID

    // This will be implemented in the actual validation code where we
    // have access to the Component descriptor and can check its type
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxigraph::model::NamedNode;

    #[test]
    fn test_identical_keys_same_hash() {
        let component_id = ComponentID(1);
        let focus = NamedNode::new("http://example.org/node1").unwrap();
        let values = Some(vec![
            NamedNode::new("http://example.org/val1").unwrap().into(),
            NamedNode::new("http://example.org/val2").unwrap().into(),
        ]);

        let key1 = ComponentMemoKey::new(component_id, focus.clone().into(), &values);
        let key2 = ComponentMemoKey::new(component_id, focus.into(), &values);

        assert_eq!(key1, key2);
        assert_eq!(key1.value_nodes_hash, key2.value_nodes_hash);
    }

    #[test]
    fn test_different_values_different_hash() {
        let component_id = ComponentID(1);
        let focus = NamedNode::new("http://example.org/node1").unwrap();

        let values1 = Some(vec![
            NamedNode::new("http://example.org/val1").unwrap().into(),
        ]);
        let values2 = Some(vec![
            NamedNode::new("http://example.org/val2").unwrap().into(),
        ]);

        let key1 = ComponentMemoKey::new(component_id, focus.clone().into(), &values1);
        let key2 = ComponentMemoKey::new(component_id, focus.into(), &values2);

        assert_ne!(key1.value_nodes_hash, key2.value_nodes_hash);
    }

    #[test]
    fn test_value_order_doesnt_matter() {
        let component_id = ComponentID(1);
        let focus = NamedNode::new("http://example.org/node1").unwrap();

        let values1 = Some(vec![
            NamedNode::new("http://example.org/val1").unwrap().into(),
            NamedNode::new("http://example.org/val2").unwrap().into(),
        ]);
        let values2 = Some(vec![
            NamedNode::new("http://example.org/val2").unwrap().into(),
            NamedNode::new("http://example.org/val1").unwrap().into(),
        ]);

        let key1 = ComponentMemoKey::new(component_id, focus.clone().into(), &values1);
        let key2 = ComponentMemoKey::new(component_id, focus.into(), &values2);

        // Order shouldn't matter for hashing
        assert_eq!(key1.value_nodes_hash, key2.value_nodes_hash);
    }

    #[test]
    fn test_none_values_hash() {
        let component_id = ComponentID(1);
        let focus = NamedNode::new("http://example.org/node1").unwrap();

        let key1 = ComponentMemoKey::new(component_id, focus.clone().into(), &None);
        let key2 = ComponentMemoKey::new(component_id, focus.into(), &None);

        assert_eq!(key1.value_nodes_hash, key2.value_nodes_hash);
    }
}
