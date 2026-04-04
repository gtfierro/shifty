//! Cost model for estimating shape validation complexity.
//!
//! This module provides cost estimation for shapes and components to enable
//! cost-based scheduling. By estimating the computational cost of validating
//! each shape, we can schedule expensive shapes first to maximize parallel
//! utilization and minimize tail latency.

use crate::shacl_ir::ComponentDescriptor;

/// Estimated cost for validating a shape.
#[derive(Debug, Clone, Copy)]
pub struct ShapeCost {
    /// Unique identifier for the shape
    pub shape_id: u64,
    /// Estimated number of target/focus nodes
    pub estimated_target_count: usize,
    /// Sum of component complexities for this shape
    pub component_complexity: u64,
    /// Total cost (target_count * component_complexity)
    pub total_cost: u64,
}

impl ShapeCost {
    /// Create a new shape cost estimate.
    pub fn new(shape_id: u64, target_count: usize, component_complexity: u64) -> Self {
        let total_cost = (target_count as u64).saturating_mul(component_complexity);
        Self {
            shape_id,
            estimated_target_count: target_count,
            component_complexity,
            total_cost,
        }
    }

    /// Update the target count estimate and recalculate total cost.
    pub fn with_target_count(mut self, target_count: usize) -> Self {
        self.estimated_target_count = target_count;
        self.total_cost = (target_count as u64).saturating_mul(self.component_complexity);
        self
    }
}

/// Estimate the computational complexity of a component.
///
/// Returns a relative cost score where:
/// - 1 = trivial (type checks, simple predicates)
/// - 5-10 = moderate (string patterns, path navigation)
/// - 20-50 = expensive (recursive validation, qualified shapes)
/// - 100+ = very expensive (SPARQL queries, complex custom logic)
pub fn estimate_component_complexity(desc: &ComponentDescriptor) -> u64 {
    match desc {
        // Simple type checks - O(1) operations
        ComponentDescriptor::Class { .. } => 1,
        ComponentDescriptor::Datatype { .. } => 1,
        ComponentDescriptor::NodeKind { .. } => 1,

        // Simple cardinality checks - O(1) with cached counts
        ComponentDescriptor::MinCount { .. } => 1,
        ComponentDescriptor::MaxCount { .. } => 1,

        // Simple value comparisons - O(1)
        ComponentDescriptor::HasValue { .. } => 1,
        ComponentDescriptor::In { values } => values.len().max(1) as u64,

        // String operations - O(n) where n is string length
        ComponentDescriptor::Pattern { .. } => 5,
        ComponentDescriptor::MinLength { .. } => 2,
        ComponentDescriptor::MaxLength { .. } => 2,
        ComponentDescriptor::LanguageIn { .. } => 3,
        ComponentDescriptor::UniqueLang { .. } => 5,

        // Numeric comparisons - O(1) but may involve parsing
        ComponentDescriptor::MinExclusive { .. } => 2,
        ComponentDescriptor::MinInclusive { .. } => 2,
        ComponentDescriptor::MaxExclusive { .. } => 2,
        ComponentDescriptor::MaxInclusive { .. } => 2,

        // Property pair constraints - requires path navigation
        ComponentDescriptor::Equals { .. } => 10,
        ComponentDescriptor::Disjoint { .. } => 10,
        ComponentDescriptor::LessThan { .. } => 10,
        ComponentDescriptor::LessThanOrEquals { .. } => 10,

        // Logical operators - cost depends on operands
        ComponentDescriptor::Not { .. } => 15,
        ComponentDescriptor::And { shapes } => 10 + (shapes.len() as u64 * 5),
        ComponentDescriptor::Or { shapes } => 10 + (shapes.len() as u64 * 5),
        ComponentDescriptor::Xone { shapes } => 10 + (shapes.len() as u64 * 5),

        // Closed shape - requires checking all predicates
        ComponentDescriptor::Closed { .. } => 15,

        // Recursive shape validation - expensive
        ComponentDescriptor::Node { .. } => 20,
        ComponentDescriptor::Property { .. } => 20,

        // Qualified value shapes - multiple validations per value
        ComponentDescriptor::QualifiedValueShape { .. } => 30,

        // SPARQL - very expensive (query execution, result processing)
        ComponentDescriptor::Sparql { .. } => 100,

        // Custom constraints - unknown complexity, assume expensive
        ComponentDescriptor::Custom { .. } => 50,
    }
}

/// Estimate the total complexity for a set of components.
pub fn estimate_shape_component_complexity<'a, I>(components: I) -> u64
where
    I: IntoIterator<Item = &'a ComponentDescriptor>,
{
    components
        .into_iter()
        .map(estimate_component_complexity)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shacl_ir::ID;
    use oxigraph::model::NamedNode;

    #[test]
    fn test_simple_components_low_cost() {
        let class_component = ComponentDescriptor::Class {
            class: NamedNode::new("http://example.org/Class").unwrap().into(),
        };
        assert_eq!(estimate_component_complexity(&class_component), 1);

        let min_count = ComponentDescriptor::MinCount { min_count: 1 };
        assert_eq!(estimate_component_complexity(&min_count), 1);
    }

    #[test]
    fn test_moderate_components() {
        let pattern = ComponentDescriptor::Pattern {
            pattern: "^[A-Z]+$".to_string(),
            flags: None,
        };
        assert_eq!(estimate_component_complexity(&pattern), 5);

        let equals = ComponentDescriptor::Equals {
            property: NamedNode::new("http://example.org/prop").unwrap().into(),
        };
        assert_eq!(estimate_component_complexity(&equals), 10);
    }

    #[test]
    fn test_expensive_components() {
        let node = ComponentDescriptor::Node {
            shape: ID(1),
        };
        assert_eq!(estimate_component_complexity(&node), 20);

        let qualified = ComponentDescriptor::QualifiedValueShape {
            shape: ID(1),
            min_count: None,
            max_count: None,
            disjoint: Some(false),
        };
        assert_eq!(estimate_component_complexity(&qualified), 30);

        // SPARQL is very expensive
        let sparql = ComponentDescriptor::Sparql {
            constraint_node: NamedNode::new("http://example.org/constraint").unwrap().into(),
        };
        assert_eq!(estimate_component_complexity(&sparql), 100);
    }

    #[test]
    fn test_logical_operators_scale_with_operands() {
        let and_2 = ComponentDescriptor::And {
            shapes: vec![ID(1), ID(2)],
        };
        assert_eq!(estimate_component_complexity(&and_2), 20); // 10 + 2*5

        let and_5 = ComponentDescriptor::And {
            shapes: vec![ID(1), ID(2), ID(3), ID(4), ID(5)],
        };
        assert_eq!(estimate_component_complexity(&and_5), 35); // 10 + 5*5
    }

    #[test]
    fn test_shape_cost_calculation() {
        let cost = ShapeCost::new(1, 100, 50);
        assert_eq!(cost.shape_id, 1);
        assert_eq!(cost.estimated_target_count, 100);
        assert_eq!(cost.component_complexity, 50);
        assert_eq!(cost.total_cost, 5000); // 100 * 50
    }

    #[test]
    fn test_shape_cost_update_target_count() {
        let cost = ShapeCost::new(1, 10, 50);
        assert_eq!(cost.total_cost, 500);

        let updated = cost.with_target_count(200);
        assert_eq!(updated.estimated_target_count, 200);
        assert_eq!(updated.total_cost, 10000); // 200 * 50
    }

    #[test]
    fn test_estimate_shape_component_complexity() {
        let components = vec![
            ComponentDescriptor::Class {
                class: NamedNode::new("http://example.org/Class").unwrap().into(),
            },
            ComponentDescriptor::MinCount { min_count: 1 },
            ComponentDescriptor::Pattern {
                pattern: "^[A-Z]+$".to_string(),
                flags: None,
            },
        ];

        let total = estimate_shape_component_complexity(&components);
        assert_eq!(total, 7); // 1 + 1 + 5
    }

    #[test]
    fn test_saturating_multiplication_prevents_overflow() {
        let cost = ShapeCost::new(1, usize::MAX, u64::MAX);
        // Should not panic, uses saturating_mul
        assert_eq!(cost.total_cost, u64::MAX);
    }
}
