use crate::context::ParsingContext;
use crate::types::Target;
use oxigraph::model::{vocab::rdf, vocab::rdfs, NamedOrBlankNode, Term};
use std::collections::{HashMap, HashSet};

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
            parents.entry(child).or_default().push(quad.object.into());
        }

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
            let mut stack: Vec<Term> = vec![quad.object.into()];
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
                _ => true, // Keep other target types
            });
            let targets_after = shape.targets.len();
            self.stats.unreachable_targets_removed += (targets_before - targets_after) as u64;
        }

        Ok(())
    }
}
