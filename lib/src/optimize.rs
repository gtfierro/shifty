use crate::context::ParsingContext;
use crate::types::{ComponentDescriptor, Target};
use oxigraph::model::{NamedOrBlankNode, Term, vocab::rdf, vocab::rdfs};
use std::collections::{HashMap, HashSet};

/// Configuration for inference-time optimization passes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InferenceOptimizationConfig {
    /// Build and use an explicit rule dependency graph derived from rule read/write signatures.
    pub explicit_rule_dependency_graph: bool,
    /// Skip disconnected rule families whose read/target signatures cannot match the current dataset.
    pub prune_rule_families_by_dataset: bool,
}

impl Default for InferenceOptimizationConfig {
    fn default() -> Self {
        Self {
            explicit_rule_dependency_graph: true,
            prune_rule_families_by_dataset: true,
        }
    }
}

/// A struct to hold statistics about the optimizations performed.
#[derive(Default, Debug)]
pub(crate) struct OptimizerStats {
    /// The number of `sh:targetClass` targets removed because the class has no instances in the data graph.
    pub(crate) unreachable_targets_removed: u64,
}

impl OptimizerStats {
    /// Creates a new `OptimizerStats` with all counters at zero.
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

/// A struct that holds the `ParsingContext` and performs optimizations on it.
pub(crate) struct Optimizer {
    /// The `ParsingContext` to be optimized.
    pub(crate) ctx: ParsingContext,
    /// Statistics collected during optimization.
    pub(crate) stats: OptimizerStats,
}

impl Optimizer {
    /// Creates a new `Optimizer` for a given `ParsingContext`.
    pub(crate) fn new(ctx: ParsingContext) -> Self {
        Optimizer {
            ctx,
            stats: OptimizerStats::new(),
        }
    }

    /// Runs all optimization passes.
    pub(crate) fn optimize(&mut self) -> Result<(), String> {
        self.optimize_shape_only()?;
        self.optimize_data_dependent()?;
        Ok(())
    }

    /// Runs shape-only optimization passes that do not depend on data graph contents.
    pub(crate) fn optimize_shape_only(&mut self) -> Result<(), String> {
        // Placeholder for shape-graph-only passes.
        Ok(())
    }

    /// Runs optimization passes that depend on data graph contents.
    pub(crate) fn optimize_data_dependent(&mut self) -> Result<(), String> {
        // Remove unreachable targets from node shapes based on observed data graph types.
        self.remove_unreachable_targets()?;
        Ok(())
    }

    /// Consumes the optimizer and returns the optimized `ParsingContext`.
    pub(crate) fn finish(self) -> ParsingContext {
        self.ctx
    }

    // Add methods for optimization logic here
    fn remove_unreachable_targets(&mut self) -> Result<(), String> {
        // Collect direct rdf:types from the data graph, then walk the subclass hierarchy across
        // all graphs to retain implicit target classes such as s223:ObservableProperty.
        let mut parents: HashMap<Term, Vec<Term>> = HashMap::new();
        for quad in self
            .ctx
            .store
            .quads_for_pattern(None, Some(rdfs::SUB_CLASS_OF), None, None)
            .flatten()
        {
            let child = match quad.subject {
                NamedOrBlankNode::NamedNode(node) => Term::NamedNode(node),
                NamedOrBlankNode::BlankNode(node) => Term::BlankNode(node),
            };
            parents.entry(child).or_default().push(quad.object);
        }

        let present_predicates = self.present_data_graph_predicates();

        let mut types = HashSet::<Term>::new();
        for quad in self
            .ctx
            .store
            .quads_for_pattern(
                None,
                Some(rdf::TYPE),
                None,
                Some(self.ctx.data_graph_iri.as_ref().into()),
            )
            .flatten()
        {
            let mut stack: Vec<Term> = vec![quad.object];
            let mut visited = HashSet::new();
            while let Some(ty) = stack.pop() {
                if !visited.insert(ty.clone()) {
                    continue;
                }
                types.insert(ty.clone());
                if let Some(super_types) = parents.get(&ty) {
                    for super_type in super_types {
                        stack.push(super_type.clone());
                    }
                }
            }
        }

        for shape in self.ctx.node_shapes.values_mut() {
            let targets_before = shape.targets.len();
            shape.targets.retain(|target| match target {
                Target::Class(class_term) => types.contains(class_term),
                Target::SubjectsOf(predicate) | Target::ObjectsOf(predicate) => {
                    present_predicates.contains(predicate)
                }
                _ => true, // Keep other target types
            });
            let targets_after = shape.targets.len();
            self.stats.unreachable_targets_removed += (targets_before - targets_after) as u64;
        }

        let component_descriptors = &self.ctx.component_descriptors;
        for shape in self.ctx.prop_shapes.values_mut() {
            let targets_before = shape.targets.len();
            shape.targets.retain(|target| match target {
                Target::Class(class_term) => types.contains(class_term),
                Target::SubjectsOf(predicate) | Target::ObjectsOf(predicate) => {
                    present_predicates.contains(predicate)
                }
                _ => true,
            });

            if !shape.targets.is_empty()
                && shape.path().is_simple_predicate()
                && !present_predicates.contains(shape.path_term())
                && property_shape_is_safe_when_path_absent(shape, component_descriptors)
            {
                shape.targets.clear();
            }

            let targets_after = shape.targets.len();
            self.stats.unreachable_targets_removed += (targets_before - targets_after) as u64;
        }

        Ok(())
    }

    fn present_data_graph_predicates(&self) -> HashSet<Term> {
        self.ctx
            .store
            .quads_for_pattern(
                None,
                None,
                None,
                Some(self.ctx.data_graph_iri.as_ref().into()),
            )
            .flatten()
            .map(|quad| Term::NamedNode(quad.predicate))
            .collect()
    }
}

fn property_shape_is_safe_when_path_absent(
    shape: &crate::model::shapes::PropertyShape,
    component_descriptors: &HashMap<crate::types::ComponentID, ComponentDescriptor>,
) -> bool {
    shape.constraints().iter().all(|component_id| {
        let Some(descriptor) = component_descriptors.get(component_id) else {
            return false;
        };
        component_is_safe_when_value_set_is_empty(descriptor)
    })
}

fn component_is_safe_when_value_set_is_empty(descriptor: &ComponentDescriptor) -> bool {
    match descriptor {
        ComponentDescriptor::MinCount { min_count } => *min_count == 0,
        ComponentDescriptor::HasValue { .. } => false,
        ComponentDescriptor::QualifiedValueShape { min_count, .. } => {
            min_count.is_none_or(|min| min == 0)
        }
        ComponentDescriptor::Equals { .. } => false,
        ComponentDescriptor::Sparql { .. } | ComponentDescriptor::Custom { .. } => false,
        _ => true,
    }
}
