//! Component result memoization for deduplicating identical constraint validations.
//!
//! When multiple shapes have identical constraints (e.g., `sh:class s223:Equipment`, `sh:minCount 1`),
//! we recompute identical validation logic. By memoizing component results based on
//! (component_descriptor, focus_node, value_nodes) tuples, we skip redundant work.

use crate::context::{Context, ValidationContext};
use crate::runtime::{ComponentValidationResult, ValidationFailure};
use crate::shacl_ir::ComponentDescriptor;
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
/// - Value count (for cardinality constraints where only count matters)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComponentMemoKey {
    pub component_id: ComponentID,
    pub focus_node: Term,
    pub value_nodes_hash: u64,
    pub value_count: usize,
}

impl ComponentMemoKey {
    /// Create a new memoization key.
    pub fn new(
        component_id: ComponentID,
        focus_node: Term,
        value_nodes: &Option<Vec<Term>>,
        value_count: usize,
    ) -> Self {
        let value_nodes_hash = hash_value_nodes(value_nodes);
        Self {
            component_id,
            focus_node,
            value_nodes_hash,
            value_count,
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
pub fn is_memoizable_component(component_id: ComponentID, context: &ValidationContext) -> bool {
    // Get component descriptor from ShapeIR
    let descriptor = context.shape_ir.components.get(&component_id);

    match descriptor {
        // SAFE: Deterministic type checks
        Some(ComponentDescriptor::Class { .. }) => true,
        Some(ComponentDescriptor::Datatype { .. }) => true,
        Some(ComponentDescriptor::NodeKind { .. }) => true,

        // SAFE: Simple cardinality checks
        Some(ComponentDescriptor::MinCount { .. }) => true,
        Some(ComponentDescriptor::MaxCount { .. }) => true,

        // SAFE: String constraints
        Some(ComponentDescriptor::Pattern { .. }) => true,
        Some(ComponentDescriptor::MinLength { .. }) => true,
        Some(ComponentDescriptor::MaxLength { .. }) => true,
        Some(ComponentDescriptor::LanguageIn { .. }) => true,
        Some(ComponentDescriptor::UniqueLang { .. }) => true,

        // SAFE: Numeric constraints
        Some(ComponentDescriptor::MinInclusive { .. }) => true,
        Some(ComponentDescriptor::MaxInclusive { .. }) => true,
        Some(ComponentDescriptor::MinExclusive { .. }) => true,
        Some(ComponentDescriptor::MaxExclusive { .. }) => true,

        // SAFE: Value constraints
        Some(ComponentDescriptor::HasValue { .. }) => true,
        Some(ComponentDescriptor::In { .. }) => true,
        Some(ComponentDescriptor::Equals { .. }) => true,
        Some(ComponentDescriptor::Disjoint { .. }) => true,
        Some(ComponentDescriptor::LessThan { .. }) => true,
        Some(ComponentDescriptor::LessThanOrEquals { .. }) => true,

        // NOT SAFE: May have side effects or complex state
        Some(ComponentDescriptor::Sparql { .. }) => false,
        Some(ComponentDescriptor::Custom { .. }) => false,

        // NOT SAFE: Depend on nested shape conformance
        Some(ComponentDescriptor::Node { .. }) => false,
        Some(ComponentDescriptor::Property { .. }) => false,
        Some(ComponentDescriptor::QualifiedValueShape { .. }) => false,
        Some(ComponentDescriptor::Not { .. }) => false,
        Some(ComponentDescriptor::And { .. }) => false,
        Some(ComponentDescriptor::Or { .. }) => false,
        Some(ComponentDescriptor::Xone { .. }) => false,
        Some(ComponentDescriptor::Closed { .. }) => false,

        None => false,
    }
}

/// Convert ComponentMemoValue back to ComponentValidationResult.
pub fn memo_value_to_results(
    value: &ComponentMemoValue,
    context: &Context,
) -> Vec<ComponentValidationResult> {
    match value {
        ComponentMemoValue::Pass => {
            vec![ComponentValidationResult::Pass(context.clone())]
        }
        ComponentMemoValue::Fail(failures) => failures
            .iter()
            .map(|f| ComponentValidationResult::Fail(context.clone(), f.clone()))
            .collect(),
    }
}

/// Convert ComponentValidationResult to ComponentMemoValue.
pub fn results_to_memo_value(results: &[ComponentValidationResult]) -> ComponentMemoValue {
    let failures: Vec<ValidationFailure> = results
        .iter()
        .filter_map(|r| match r {
            ComponentValidationResult::Fail(_, failure) => Some(failure.clone()),
            _ => None,
        })
        .collect();

    if failures.is_empty() {
        ComponentMemoValue::Pass
    } else {
        ComponentMemoValue::Fail(failures)
    }
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

        let key1 = ComponentMemoKey::new(
            component_id,
            focus.clone().into(),
            &values,
            values.as_ref().map_or(0, |v| v.len()),
        );
        let key2 = ComponentMemoKey::new(
            component_id,
            focus.into(),
            &values,
            values.as_ref().map_or(0, |v| v.len()),
        );

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

        let key1 = ComponentMemoKey::new(component_id, focus.clone().into(), &values1, 1);
        let key2 = ComponentMemoKey::new(component_id, focus.into(), &values2, 1);

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

        let key1 = ComponentMemoKey::new(component_id, focus.clone().into(), &values1, 2);
        let key2 = ComponentMemoKey::new(component_id, focus.into(), &values2, 2);

        // Order shouldn't matter for hashing
        assert_eq!(key1.value_nodes_hash, key2.value_nodes_hash);
    }

    #[test]
    fn test_none_values_hash() {
        let component_id = ComponentID(1);
        let focus = NamedNode::new("http://example.org/node1").unwrap();

        let key1 = ComponentMemoKey::new(component_id, focus.clone().into(), &None, 0);
        let key2 = ComponentMemoKey::new(component_id, focus.into(), &None, 0);

        assert_eq!(key1.value_nodes_hash, key2.value_nodes_hash);
    }
}
